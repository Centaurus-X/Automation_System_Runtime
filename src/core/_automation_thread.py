# -*- coding: utf-8 -*-

# src/core/_automation_thread.py

"""
Automation Thread — v5.0 (Synchronous, job-based, ThreadPool worker)
----------------------------------------------------------------------

Responsibility
~~~~~~~~~~~~~~
Stateless automation cycle triggered by a sensor event.
Called by TM ThreadPool as a job — NOT a long-running thread.

Each call to ``run_automation_cycle()`` performs one complete cycle:

1. Build automation context from config snapshot + sensor data
2. Find all process_states linked to the sensor
3. For each active state:
   a. Apply sensor filters
   b. Extract setpoint (from parameters or control_method)
   c. Run controller (PID / P / ONOFF)
   d. Emit actuator task (collected, not queued)
4. Evaluate automation_rule_sets
5. Return collected actuator_tasks[]

TM dispatches the returned tasks to the correct controller threads.

Architecture
~~~~~~~~~~~~
- No OOP — dict-based context only.
- functools.partial instead of lambda.
- Pure function: config_snapshot in, actuator_tasks out.
- No shared mutable state between calls.
- Controller state (PID integral, filter state) persisted
  via thread_info["persistent_state"] across calls.
- C/C11 migration friendly.
"""

import copy
import logging
import threading
import time

from functools import partial

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Automation System Interface (sync, all business logic)
# ---------------------------------------------------------------------------

from src.library._automation_engine import (
    cfg_snapshot,
    get_automation_settings,
    get_automation_rules,
    get_active_value_ids,
    find_sensor_by_id,
    find_actuator_by_id,
    find_process_state_by_id,
    find_states_for_sensor,
    is_state_active,
    get_state_value_id,
    seed_runtime_state_values,
    resolve_allowed_controller_ids,
    apply_sensor_filters,
    get_filter_chain_for_sensor,
    ensure_controller_for_state,
    controller_update,
    extract_setpoint_from_state,
    evaluate_control_method,
    evaluate_automation_rules,
    emit_actuator_task,
    apply_safety_fail_safe,
    raw_to_scaled,
)


# =============================================================================
# Helper
# =============================================================================


def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


def _now_ts():
    return time.time()


def _list_from_cfg(cfg, key):
    value = cfg.get(key) or []
    if isinstance(value, list):
        return value
    return []


def _index_records(records, id_key):
    indexed = {}
    for record in records or []:
        if not isinstance(record, dict):
            continue
        rid = _safe_int(record.get(id_key))
        if rid is None:
            continue
        indexed[rid] = record
    return indexed


def _active_value_ids_from_cfg(cfg):
    active_ids = set()
    labels_active = {"activated", "active", "on", "enabled", "running"}

    for value_rec in _list_from_cfg(cfg, "process_values"):
        if not isinstance(value_rec, dict):
            continue
        value_id = _safe_int(value_rec.get("value_id"))
        if value_id is None:
            continue
        label = str(value_rec.get("state_value") or "").strip().lower()
        if label in labels_active:
            active_ids.add(value_id)

    if not active_ids:
        active_ids.add(1)
    return active_ids


def _sensor_to_states_from_cfg(cfg):
    mapping = {}
    for state in _list_from_cfg(cfg, "process_states"):
        if not isinstance(state, dict):
            continue
        sensor_ids = state.get("sensor_ids") or []
        if not isinstance(sensor_ids, list):
            sensor_ids = [sensor_ids]
        for sensor_id in sensor_ids:
            sid = _safe_int(sensor_id)
            if sid is None:
                continue
            mapping.setdefault(sid, []).append(state)
    return mapping


def _sensor_to_filter_chain_from_cfg(cfg):
    mapping = {}
    for filter_rec in _list_from_cfg(cfg, "automation_filters"):
        if not isinstance(filter_rec, dict):
            continue
        apply_to = filter_rec.get("apply_to") or {}
        sensor_ids = filter_rec.get("sensors")
        if sensor_ids is None and isinstance(apply_to, dict):
            sensor_ids = apply_to.get("sensor_ids")
        if sensor_ids is None:
            sensor_ids = []
        if not isinstance(sensor_ids, list):
            sensor_ids = [sensor_ids]
        chain = filter_rec.get("chain") or []
        if not isinstance(chain, list):
            chain = []
        for sensor_id in sensor_ids:
            sid = _safe_int(sensor_id)
            if sid is None:
                continue
            mapping[sid] = list(chain)
    return mapping


# =============================================================================
# Automation context builder (per-job, stateless)
# =============================================================================


def _build_automation_context(config_snapshot, sensor_data, thread_info):
    """Build the automation context dict for one evaluation cycle.

    This context is local to the ThreadPool worker — no shared state.
    Persistent state (PID integrals, filter buffers) can be passed in
    via thread_info["persistent_state"] and returned for next cycle.

    Args:
        config_snapshot: Frozen config dict (read-only)
        sensor_data:     {"sensor_id": int, "value": float, "controller_id": int, ...}
        thread_info:     {"job_id": str, "sensor_id": int, "persistent_state": dict, ...}

    Returns:
        Automation context dict compatible with _automation_system_interface functions.
    """
    cfg = config_snapshot if isinstance(config_snapshot, dict) else {}

    # Persistent state from previous cycle (PID integrals, filter buffers)
    persistent = thread_info.get("persistent_state") or {}

    sensors = _list_from_cfg(cfg, "sensors")
    actuators = _list_from_cfg(cfg, "actuators")
    process_states = _list_from_cfg(cfg, "process_states")
    pid_configs = _list_from_cfg(cfg, "pid_configs")
    scaling_profiles = _list_from_cfg(cfg, "scaling_profiles")
    automation_rules = _list_from_cfg(cfg, "automation_rule_sets")
    automation_filters = _list_from_cfg(cfg, "automation_filters")

    controller_scope_explicit = bool(
        thread_info.get("controller_scope_explicit")
        or "allowed_controller_ids" in thread_info
    )
    supplied_controller_ids = thread_info.get("allowed_controller_ids")
    normalized_controller_ids = set()
    if controller_scope_explicit:
        for controller_id in supplied_controller_ids or ():
            try:
                normalized_controller_ids.add(int(controller_id))
            except Exception:
                continue

    ctx = {
        # Config snapshot (read-only)
        "config_data": cfg,
        "config_snapshot": cfg,
        "config_lock": None,
        "node_id": thread_info.get("node_id"),

        # Thread identity
        "thread_index": thread_info.get("job_id", "auto"),
        "run_epoch": thread_info.get("job_id"),

        # Sensor data from this cycle
        "sensor_data": dict(sensor_data or {}),

        # Collected actuator tasks (filled by emit_actuator_task)
        "_collected_tasks": [],

        # Event queue (None in job-based mode — tasks collected, not emitted)
        "queue_event_send": None,

        # Controller instances (PID, P, ONOFF) — restored from persistent state
        "controllers": persistent.get("controllers") or {},

        # Sensor filter states — restored from persistent state
        # Compatibility: keep both keys on the same dict object.
        "sensor_filter_states": (
            persistent.get("sensor_filter_states")
            or persistent.get("filter_states")
            or {}
        ),

        # Runtime state values (value_id tracking per state)
        "runtime_state_values": persistent.get("runtime_state_values") or {},

        # Method states (control_method tracking)
        "method_states": persistent.get("method_states") or {},

        # Setpoint overrides
        "setpoint_overrides": persistent.get("setpoint_overrides") or {},

        # Actuator duplicate suppression
        "actuator_last_cmd": persistent.get("actuator_last_cmd") or {},

        # One-shot tracking (local per-cycle)
        "_one_shot_fired_local": set(),

        # Explicit scope keeps an empty set fail-closed. Legacy direct callers
        # without a supplied scope retain their historical resolver behaviour.
        "allowed_controller_ids": (
            normalized_controller_ids if controller_scope_explicit else None
        ),
        "controller_scope_explicit": controller_scope_explicit,

        # Shared automation states (stub — no TM shared store in job mode)
        "shared_automation_states": persistent.get("shared_automation_states") or {},
        "shared_automation_states_lock": threading.RLock(),

        # Changed-key tracking for safe persistent-state merge.
        # IMPORTANT:
        # Jobs are submitted concurrently with a snapshot copy of the shared
        # automation state. If a job returns the full copied store, later jobs
        # can overwrite newer controller/filter state from other jobs with
        # stale data from their older snapshot. Therefore only touched keys are
        # merged back into TM.
        "_touched_sensor_ids": set(),
        "_touched_state_ids": set(),
        "_touched_actuator_ids": set(),

        # Prebuilt caches for _automation_system_interface
        "_cache_sensors": _index_records(sensors, "sensor_id"),
        "_cache_actuators": _index_records(actuators, "actuator_id"),
        "_cache_process_states": _index_records(process_states, "state_id"),
        "_cache_sensor_to_states": _sensor_to_states_from_cfg(cfg),
        "_cache_pid_configs": _index_records(pid_configs, "pid_config_id"),
        "_cache_scaling_profiles": _index_records(scaling_profiles, "profile_id"),
        "_cache_automation_rules": list(automation_rules),
        "_cache_automation_settings": dict(cfg.get("automation_settings") or {}),
        "_cache_sensor_filters": list(automation_filters),
        "_cache_sensor_to_filter_chain": _sensor_to_filter_chain_from_cfg(cfg),
        "_cache_active_value_ids": _active_value_ids_from_cfg(cfg),
    }

    # Backward-compatible alias used by older call sites.
    ctx["filter_states"] = ctx["sensor_filter_states"]

    return ctx


def _copy_subset_by_keys(mapping, keys):
    """Return a deep-copied subset of a dict for the given keys."""
    if not isinstance(mapping, dict):
        return {}

    key_set = set(keys or [])
    if not key_set:
        return {}

    subset = {}
    for key, value in mapping.items():
        if key in key_set:
            subset[key] = copy.deepcopy(value)
            continue
        try:
            if str(key) in key_set or int(key) in key_set:
                subset[key] = copy.deepcopy(value)
        except Exception:
            continue
    return subset


def _copy_shared_state_subset(shared_store, state_ids):
    """Return only the touched per-state shared-store sections."""
    if not isinstance(shared_store, dict):
        return {}

    state_key_set = set(state_ids or [])
    if not state_key_set:
        return {}

    out = {}

    states_map = shared_store.get('states')
    states_subset = _copy_subset_by_keys(states_map, state_key_set)
    if states_subset:
        out['states'] = states_subset

    runtime_map = shared_store.get('runtime_state_values')
    runtime_subset = _copy_subset_by_keys(runtime_map, state_key_set)
    if runtime_subset:
        out['runtime_state_values'] = runtime_subset

    method_map = shared_store.get('method_states')
    method_subset = _copy_subset_by_keys(method_map, state_key_set)
    if method_subset:
        out['method_states'] = method_subset

    override_map = shared_store.get('setpoint_overrides')
    override_subset = _copy_subset_by_keys(override_map, state_key_set)
    if override_subset:
        out['setpoint_overrides'] = override_subset

    return out


def _extract_persistent_state(ctx):
    """Extract only touched persistent state for safe TM merge.

    Why this is necessary:
    TM submits multiple automation jobs in parallel. Each job receives a copy
    of the shared persistent store at submission time. Returning the full copied
    store from every job would allow later-finishing jobs to overwrite newer
    controller/filter state from other jobs with stale snapshot data.

    Therefore we only return the entries that were actually touched in the job.
    TM's recursive merge then updates those keys in-place without regressing
    unrelated sensors/states.
    """
    sensor_filter_states = (
        ctx.get("sensor_filter_states")
        or ctx.get("filter_states")
        or {}
    )

    touched_sensor_ids = set(ctx.get("_touched_sensor_ids") or [])
    touched_state_ids = set(ctx.get("_touched_state_ids") or [])
    touched_actuator_ids = set(ctx.get("_touched_actuator_ids") or [])

    controllers = _copy_subset_by_keys(ctx.get("controllers") or {}, touched_state_ids)
    filter_subset = _copy_subset_by_keys(sensor_filter_states, touched_sensor_ids)
    runtime_subset = _copy_subset_by_keys(ctx.get("runtime_state_values") or {}, touched_state_ids)
    method_subset = _copy_subset_by_keys(ctx.get("method_states") or {}, touched_state_ids)
    override_subset = _copy_subset_by_keys(ctx.get("setpoint_overrides") or {}, touched_state_ids)
    actuator_cmd_subset = _copy_subset_by_keys(ctx.get("actuator_last_cmd") or {}, touched_actuator_ids)
    shared_subset = _copy_shared_state_subset(ctx.get("shared_automation_states") or {}, touched_state_ids)

    return {
        "controllers": controllers,
        "sensor_filter_states": filter_subset,
        "filter_states": copy.deepcopy(filter_subset),
        "runtime_state_values": runtime_subset,
        "method_states": method_subset,
        "setpoint_overrides": override_subset,
        "actuator_last_cmd": actuator_cmd_subset,
        "shared_automation_states": shared_subset,
    }


# =============================================================================
# Sensor value reader (for rule conditions that need other sensor values)
# =============================================================================


def _make_read_sensor_fn(ctx, primary_sensor_id, primary_value):
    """Create a sensor-read function for rule condition evaluation.

    Returns the primary sensor value directly. For other sensors,
    returns None (not available in job-based mode without shared store).
    """
    def _read(sensor_id):
        sid = _safe_int(sensor_id)
        if sid == _safe_int(primary_sensor_id):
            return primary_value
        # Other sensors: check recent_sensor_values in ctx
        recent = ctx.get("_recent_sensor_values") or {}
        return recent.get(sid, recent.get(str(sid)))

    return _read


# =============================================================================
# Core automation cycle
# =============================================================================


def _process_state_for_sensor(ctx, state, sensor_id, filtered_value, now_ts):
    """Process one state for one sensor reading.

    Steps:
        1. Check if state is active
        2. Extract or compute setpoint
        3. Run controller (PID/P/ONOFF/control_method)
        4. Emit actuator task with controller output

    Returns:
        Number of actuator tasks emitted.
    """
    state_id = _safe_int(state.get("state_id"))
    if state_id is None:
        return 0

    # Check active
    if not is_state_active(ctx, state):
        return 0

    ctx.setdefault("_touched_state_ids", set()).add(int(state_id))

    tasks_before = len(ctx.get("_collected_tasks") or [])

    try:
        # Control method evaluation (ramp, square, sine, etc.)
        cs = state.get("control_strategy") or {}
        cs_type = str(cs.get("type") or "").strip().lower()

        if cs_type == "control_method":
            try:
                method_result = evaluate_control_method(ctx, state, now_ts)
                if method_result is not None:
                    # Control method produces output directly
                    act_ids = state.get("actuator_ids") or []
                    for aid in act_ids:
                        try:
                            ctx.setdefault("_touched_actuator_ids", set()).add(int(aid))
                        except Exception:
                            pass
                        emit_actuator_task(
                            ctx, int(aid), method_result,
                            meta={"state_id": state_id, "source": "control_method"}
                        )
                    return len(ctx.get("_collected_tasks") or []) - tasks_before
            except Exception as exc:
                logger.warning("Control method error (state=%s): %s", state_id, exc)

        # Standard controller path (PID / P / ONOFF)
        controller = ensure_controller_for_state(ctx, state)
        if controller is None:
            return 0

        # Setpoint
        setpoint = extract_setpoint_from_state(state)
        if setpoint is None:
            return 0

        # Controller update
        output = controller_update(controller, setpoint, filtered_value, now_ts)
        if output is None:
            return 0

        # Emit actuator tasks
        act_ids = state.get("actuator_ids") or []
        for aid in act_ids:
            try:
                ctx.setdefault("_touched_actuator_ids", set()).add(int(aid))
            except Exception:
                pass
            emit_actuator_task(
                ctx, int(aid), output,
                meta={"state_id": state_id, "setpoint": setpoint, "measurement": filtered_value}
            )

    except Exception as exc:
        logger.error("State processing error (state=%s, sensor=%s): %s",
                     state_id, sensor_id, exc, exc_info=True)

    return len(ctx.get("_collected_tasks") or []) - tasks_before


# =============================================================================
# Main entry point — called by TM ThreadPool
# =============================================================================


def run_automation_cycle(config_snapshot, sensor_data, thread_info):
    """Execute one automation cycle for a sensor event.

    This is the function called by TM's ThreadPool workers.

    Args:
        config_snapshot: Dict with all config tables (frozen, read-only)
        sensor_data:     {"sensor_id": int, "value": float, "controller_id": int, "timestamp": str}
        thread_info:     {"job_id": str, "sensor_id": int, "persistent_state": dict}

    Returns:
        List of actuator task dicts, or empty list.
        Each task has: actuator_id, controller_id, function_type, address, control_value, ...
    """
    try:
        sensor_id = _safe_int(sensor_data.get("sensor_id"))
        raw_value = _safe_float(sensor_data.get("value"))
        now_ts = _now_ts()

        if sensor_id is None or raw_value is None:
            return []

        # Build automation context
        ctx = _build_automation_context(config_snapshot, sensor_data, thread_info)

        # Seed runtime state values from config (value_id tracking)
        try:
            seed_runtime_state_values(ctx)
        except Exception:
            pass

        # Resolve legacy ownership only when Thread Management did not provide
        # an explicit, immutable controller scope for this job.
        if not ctx.get("controller_scope_explicit"):
            try:
                resolved_controller_ids = resolve_allowed_controller_ids(ctx)
                ctx["allowed_controller_ids"] = (
                    resolved_controller_ids if resolved_controller_ids else None
                )
            except Exception:
                ctx["allowed_controller_ids"] = None

        # Find sensor config
        sensor_dict = find_sensor_by_id(ctx, sensor_id)

        # Apply sensor filters (median, lowpass, etc.)
        filtered_value = raw_value
        if sensor_dict is not None:
            try:
                filtered_value = apply_sensor_filters(ctx, sensor_dict, raw_value)
            except Exception:
                filtered_value = raw_value

        # Track touched primary sensor for persistent-state extraction
        ctx.setdefault("_touched_sensor_ids", set()).add(int(sensor_id))

        # Store for rule condition evaluation
        ctx["_recent_sensor_values"] = {sensor_id: filtered_value, str(sensor_id): filtered_value}

        # Find all states linked to this sensor
        states = find_states_for_sensor(ctx, sensor_id)
        if not states:
            states = []

        # Process each state
        total_tasks = 0
        for state in states:
            total_tasks += _process_state_for_sensor(ctx, state, sensor_id, filtered_value, now_ts)

        # Evaluate automation rules (when_all / do / else_do)
        try:
            read_fn = _make_read_sensor_fn(ctx, sensor_id, filtered_value)
            evaluate_automation_rules(ctx, {sensor_id: filtered_value}, read_fn)
        except Exception as exc:
            logger.error("Rule evaluation error (sensor=%s): %s", sensor_id, exc, exc_info=True)

        # Extract collected tasks
        collected = ctx.get("_collected_tasks") or []

        # Persist state for next cycle (PID integrals, filter buffers)
        if isinstance(thread_info, dict):
            thread_info["persistent_state"] = _extract_persistent_state(ctx)

        if collected:
            logger.debug(
                "Automation cycle: sensor=%s value=%.2f -> %d tasks",
                sensor_id, filtered_value, len(collected),
            )

        return collected

    except Exception as exc:
        logger.error("run_automation_cycle fatal: %s", exc, exc_info=True)
        return []
