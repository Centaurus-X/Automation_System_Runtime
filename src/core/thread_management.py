# -*- coding: utf-8 -*-

# src/core/thread_management.py


"""
 =============================================================================
 THREAD_MANAGEMENT
 =============================================================================

 What it is:
     The runtime owner for the central GenericThreadPool, isolated controller
     workers, connection threads and event-to-job dispatch.

 Why it is needed:
     Long-lived controller ownership and short CPU/business jobs have different
     concurrency rules. Controller I/O stays UUID-local and ordered; stateless
     automation, FSM and RuleEngine work is submitted to one central pool.

 How it works:
     Routed events enter one management queue. Sensor updates create pool jobs.
     A dedicated result dispatcher routes actuator tasks back to the owning
     UUID worker and sends RuleEngine action plans to the SEM single writer.
     Config rollout atomically refreshes pool, FSM, rules and affected workers.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy

import json
import logging
import threading
import time

from functools import partial
from queue import Empty

from src.core._automation_thread import run_automation_cycle
from src.library._automation_engine import scaled_to_raw
from src.core._worker_thread import (
    apply_controller_config,
    create_controller_worker,
    dispatch_controller_event,
    get_controller_worker_snapshot,
    start_controller_worker,
    stop_controller_worker,
)

from src.library import _generic_rule_engine as rule_engine
from src.library import _generic_type_library as type_library
from src.library._config_safety_policy import (
    create_safety_context,
    evaluate_actuator_task,
    get_safety_snapshot,
)
from src.library._event_interface import make_event, sync_queue_get, sync_queue_put
from src.library._generic_finite_state_machine import (
    fsm_machines_from_config,
    fsm_restore,
    fsm_send,
    fsm_snapshot,
)

from src.library._generic_thread_pool import create_thread_pool
from src.library._state_shard_manager import (
    build_automation_projection,
    build_worker_shard,
    get_node_controller_uuids,
)
from src.library._generic_system_contract_library.config_sharding_contracts import build_local_execution_shard


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "thread_management",
    "library_version": "2.5.0",
    "description": "Central pool and isolated controller lifecycle owner",
    "options": {
        "thread_name": "ThreadManagement",
        "ingress_timeout_s": 0.25,
        "join_timeout_s": 5.0,
        "pool_min_workers": 2,
        "pool_max_workers": 8,
        "pool_default_job_timeout_s": 30.0,
        "pool_queue_timeout_s": 10.0,
        "pool_max_detached_jobs": 32,
        "pool_priority_fsm": 300.0,
        "pool_priority_automation": 200.0,
        "pool_priority_rule": 100.0,
        "pool_priority_cross": 150.0,
        "controllers_enabled": False,
        "connections_enabled": False,
        "rule_engine_enabled": True,
        "rule_scope": "all",
        "fsm_scope": "all",
        "controller_allowlist": (),
        "controller_timing_defaults": {},
        "rejection_log_interval_s": 5.0,
        "node_id": "",
    },
}


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _safe_float(value, default):
    try:
        result = float(value)
    except Exception:
        return float(default)
    return result if result > 0.0 else float(default)


def _bounded_priority(value, default=0.0):
    """Return one finite pool priority inside the public pool bounds."""
    try:
        result = float(value)
    except Exception:
        result = float(default)
    if result != result:
        result = float(default)
    return max(-1000000.0, min(1000000.0, result))


def _pool_priority(context, option_key, extra=0.0):
    base = _bounded_priority(context["options"].get(option_key), 0.0)
    offset = _bounded_priority(extra, 0.0)
    return _bounded_priority(base + offset, base)


def _safe_int(value, default, minimum=0):
    try:
        result = int(value)
    except Exception:
        return int(default)
    return result if result >= minimum else int(default)


def _safe_bool(value, default):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(default)


def _normalize_controller_timing_defaults(value):
    if value is None:
        return {}
    if not _is_dict(value):
        raise ValueError("controller_timing_defaults must be a mapping")
    normalized = {}
    for controller_uuid, defaults in value.items():
        identity = str(controller_uuid or "").strip()
        if not identity or not _is_dict(defaults):
            raise ValueError(
                "controller_timing_defaults entries require UUID keys and mappings"
            )
        normalized[identity] = copy.deepcopy(defaults)
    return normalized


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _event_type(event):
    if not _is_dict(event):
        return ""
    return str(event.get("event_type") or event.get("type") or "").upper()


def _event_payload(event):
    if not _is_dict(event):
        return {}
    payload = event.get("payload")
    return payload if _is_dict(payload) else {}


def _stop_event(target):
    return make_event(
        "THREAD_MANAGEMENT_STOP",
        payload={"reason": "component_shutdown"},
        source="thread_management",
        target=target,
        _type="__STOP__",
        _sentinel=True,
    )


def _is_stop(event):
    if not _is_dict(event):
        return False
    return (
        event.get("_type") == "__STOP__"
        and event.get("_sentinel") is True
    ) or _event_type(event) in ("__STOP__", "THREAD_MANAGEMENT_STOP")


def _options_from(kwargs):
    options = dict(CONTEXT["options"])
    supplied = kwargs.get("options")
    if _is_dict(supplied):
        options.update(supplied)
    for key in CONTEXT["options"]:
        if key in kwargs:
            options[key] = kwargs[key]
    options["ingress_timeout_s"] = _safe_float(
        options.get("ingress_timeout_s"), CONTEXT["options"]["ingress_timeout_s"])
    options["join_timeout_s"] = _safe_float(
        options.get("join_timeout_s"), CONTEXT["options"]["join_timeout_s"])
    options["pool_min_workers"] = _safe_int(
        options.get("pool_min_workers"), CONTEXT["options"]["pool_min_workers"])
    options["pool_max_workers"] = _safe_int(
        options.get("pool_max_workers"), CONTEXT["options"]["pool_max_workers"], 1)
    if options["pool_max_workers"] < options["pool_min_workers"]:
        options["pool_max_workers"] = options["pool_min_workers"]
    for key in (
        "pool_default_job_timeout_s",
        "pool_queue_timeout_s",
        "pool_priority_fsm",
        "pool_priority_automation",
        "pool_priority_rule",
        "pool_priority_cross",
    ):
        options[key] = _safe_float(
            options.get(key), CONTEXT["options"][key])
    options["pool_max_detached_jobs"] = _safe_int(
        options.get("pool_max_detached_jobs"),
        CONTEXT["options"]["pool_max_detached_jobs"], 1)
    options["controllers_enabled"] = _safe_bool(
        options.get("controllers_enabled"), False)
    options["connections_enabled"] = _safe_bool(
        options.get("connections_enabled"), False)
    options["rule_engine_enabled"] = _safe_bool(
        options.get("rule_engine_enabled"), True)
    options["rule_scope"] = str(
        options.get("rule_scope") or "all").strip().lower()
    if options["rule_scope"] not in ("all", "local_controllers"):
        raise ValueError("rule_scope must be all or local_controllers")
    options["fsm_scope"] = str(
        options.get("fsm_scope") or "all").strip().lower()
    if options["fsm_scope"] not in ("all", "local_controllers"):
        raise ValueError("fsm_scope must be all or local_controllers")
    configured_allowlist = options.get("controller_allowlist") or ()
    if isinstance(configured_allowlist, str):
        configured_allowlist = configured_allowlist.split(",")
    options["controller_allowlist"] = tuple(sorted({
        str(identity).strip()
        for identity in configured_allowlist
        if str(identity).strip()
    }))
    options["controller_timing_defaults"] = (
        _normalize_controller_timing_defaults(
            options.get("controller_timing_defaults")
        )
    )
    options["rejection_log_interval_s"] = _safe_float(
        options.get("rejection_log_interval_s"),
        CONTEXT["options"]["rejection_log_interval_s"],
    )
    options["node_id"] = str(options.get("node_id") or "")
    return options


def _increment_stat(context, name, amount=1):
    lock = context.get("statistics_lock")
    if lock is None:
        context["statistics"][name] = context["statistics"].get(name, 0) + amount
        return
    with lock:
        context["statistics"][name] = context["statistics"].get(name, 0) + amount


def _rejection_counter_name(code):
    mapping = {
        "task_limit": "actuator_rejected_task_limit",
        "stale_revision": "actuator_rejected_stale_revision",
        "interlock": "actuator_rejected_interlock",
        "ownership": "actuator_rejected_ownership",
    }
    return mapping.get(str(code or ""), "actuator_rejected_other")


def _log_actuator_rejection(context, task, report, **kwargs):
    metadata = task.get("meta") if _is_dict(task.get("meta")) else {}
    code = str(report.get("code") or "rejected")
    fields = (
        code,
        str(task.get("controller_uuid") or ""),
        str(task.get("actuator_id") or ""),
        str(task.get("address") or ""),
        str(task.get("source") or metadata.get("source") or ""),
        str(metadata.get("state_id") or ""),
    )
    now = time.monotonic()
    interval = context["options"]["rejection_log_interval_s"]
    with context["rejection_log_lock"]:
        previous = context["rejection_log_state"].get(fields, 0.0)
        if now - previous < interval:
            _increment_stat(context, "actuator_rejection_logs_suppressed")
            return
        context["rejection_log_state"][fields] = now
    LOGGER.warning(
        "Actuator task rejected: code=%s message=%s controller_uuid=%s "
        "actuator_id=%s address=%s config_revision=%s source=%s state_id=%s",
        code,
        str(report.get("message") or "rejected"),
        fields[1],
        fields[2],
        fields[3],
        kwargs.get("config_revision"),
        fields[4],
        fields[5],
    )


def _record_actuator_rejection(context, task, report, **kwargs):
    code = str(report.get("code") or "rejected")
    details = copy.deepcopy(report)
    details.update({
        "controller_uuid": str(task.get("controller_uuid") or ""),
        "actuator_id": task.get("actuator_id"),
        "address": task.get("address"),
        "config_revision": kwargs.get("config_revision"),
    })
    with context["statistics_lock"]:
        statistics = context["statistics"]
        statistics["actuator_rejected"] += 1
        counter = _rejection_counter_name(code)
        statistics[counter] = statistics.get(counter, 0) + 1
        if kwargs.get("safety"):
            statistics["actuator_safety_rejected"] += 1
        if code == "ownership":
            statistics["ownership_rejected"] += 1
        context["last_error"] = details
    _log_actuator_rejection(
        context,
        task,
        report,
        config_revision=kwargs.get("config_revision"),
    )
    return False


# =============================================================================
# RULE ENGINE, FSM AND FACT SNAPSHOTS
# =============================================================================

def _collect_sensor_controllers(node, result):
    if isinstance(node, list):
        for item in node:
            _collect_sensor_controllers(item, result)
        return
    if not _is_dict(node):
        return
    for gate in ("all", "any", "none", "xor"):
        if gate in node:
            _collect_sensor_controllers(node.get(gate), result)
            return
    if "not" in node:
        _collect_sensor_controllers(node.get("not"), result)
        return
    if str(node.get("src") or "").lower() != "sensor":
        return
    controller_uuid = str(node.get("controller_uuid") or "").strip()
    if controller_uuid:
        result.add(controller_uuid)


def _rule_target_rows(config, rule):
    states = [row for row in config.get("process_states") or [] if _is_dict(row)]
    result = []
    seen = set()
    for branch in ("do", "else_do"):
        for item in rule.get(branch) or []:
            action = item.get("action") if _is_dict(item) else None
            action = action if _is_dict(action) else item
            if not _is_dict(action):
                continue
            state_ids = {str(value) for value in action.get("state_ids") or []}
            process_ids = {str(value) for value in action.get("p_ids") or []}
            for row in states:
                selected = (
                    (state_ids and str(row.get("state_id")) in state_ids)
                    or (process_ids and str(row.get("p_id")) in process_ids)
                )
                if not selected:
                    continue
                identity = str(row.get("state_id"))
                if identity not in seen:
                    seen.add(identity)
                    result.append(row)
    return result


def _rule_target_controllers(config, rule):
    result = set()
    for row in _rule_target_rows(config, rule):
        refs = row.get("io_refs") if _is_dict(row.get("io_refs")) else {}
        for reference in refs.get("actuators") or []:
            if not _is_dict(reference):
                continue
            identity = str(reference.get("controller_uuid") or "").strip()
            if identity:
                result.add(identity)
    return result


def _rule_modes_allowed(config, rule):
    engine_id = str(rule.get("engine_id") or "")
    engine = None
    for row in config.get("rule_engine") or []:
        if not _is_dict(row) or row.get("enabled") is False:
            continue
        if not engine_id or str(row.get("engine_id") or "") == engine_id:
            engine = row
            break
    required_mode = engine.get("required_mode_id") if _is_dict(engine) else None
    if required_mode is None:
        return True
    return all(
        str(row.get("mode_id")) == str(required_mode)
        for row in _rule_target_rows(config, rule)
    )


def _local_rule_projection(config, options):
    if options.get("rule_scope") != "local_controllers":
        return copy.deepcopy(config), {
            "mode": "all", "selected": len(config.get("rules") or []),
            "skipped": 0, "controller_uuids": (),
        }
    routed = set(get_node_controller_uuids(config, options.get("node_id")))
    allowed = set(options.get("controller_allowlist") or ())
    local = routed & allowed if allowed else routed
    selected = []
    skipped = []
    for rule in config.get("rules") or []:
        if not _is_dict(rule):
            continue
        explicit_owner = str(rule.get("owner_node_id") or "").strip()
        sensor_controllers = set()
        _collect_sensor_controllers(rule.get("when"), sensor_controllers)
        _collect_sensor_controllers(rule.get("until"), sensor_controllers)
        targets = _rule_target_controllers(config, rule)
        owned = bool(local)
        if explicit_owner:
            owned = owned and explicit_owner == str(options.get("node_id") or "")
        elif not sensor_controllers and not targets:
            owned = False
        owned = owned and sensor_controllers.issubset(local)
        owned = owned and targets.issubset(local)
        # Scope and mode are separate gates: the strict shard decides whether
        # the rule is local; the RuleEngine's configured mode decides whether
        # it is executable in the current engine instance.
        owned = owned and _rule_modes_allowed(config, rule)
        if owned:
            selected.append(copy.deepcopy(rule))
        else:
            skipped.append(rule.get("rule_id"))
    projection = copy.deepcopy(config)
    projection["rules"] = selected
    return projection, {
        "mode": "local_controllers",
        "selected": len(selected),
        "skipped": len(skipped),
        "skipped_rule_ids": tuple(skipped),
        "controller_uuids": tuple(sorted(local)),
    }


def _build_rule_context(config, options=None):
    effective_options = options if _is_dict(options) else {"rule_scope": "all"}
    projected, scope = _local_rule_projection(config, effective_options)
    context = rule_engine.create_engine_context()
    dataset = rule_engine.load_dataset(projected)
    binding = rule_engine.engine_bind_dataset(context, dataset)
    binding["scope"] = scope
    return context, binding


def _process_controller_uuids(config, process_id):
    result = set()
    for row in config.get("process_states") or []:
        if not _is_dict(row) or str(row.get("p_id")) != str(process_id):
            continue
        refs = row.get("io_refs") if _is_dict(row.get("io_refs")) else {}
        for section in ("sensors", "actuators"):
            for reference in refs.get(section) or []:
                if not _is_dict(reference):
                    continue
                identity = str(
                    reference.get("controller_uuid") or "").strip()
                if identity:
                    result.add(identity)
    return result


def _local_fsm_projection(config, options):
    if options.get("fsm_scope") != "local_controllers":
        return copy.deepcopy(config), {
            "mode": "all", "selected": len(config.get("processes") or []),
            "skipped": 0, "controller_uuids": (),
        }
    # FSM ownership is node-scoped, not restricted to the currently opened
    # hardware subset. A simulator allowlist may select one controller while
    # the same Worker still owns the process instance. A genuinely
    # cross-Worker process is excluded because its controllers are not a
    # subset of one node's routed controllers.
    local = set(get_node_controller_uuids(config, options.get("node_id")))
    selected = []
    skipped = []
    for process in config.get("processes") or []:
        if not _is_dict(process):
            continue
        explicit_owner = str(process.get("owner_node_id") or "").strip()
        controllers = _process_controller_uuids(config, process.get("p_id"))
        owned = bool(controllers) and controllers.issubset(local)
        if explicit_owner:
            owned = owned and explicit_owner == str(options.get("node_id") or "")
        if owned:
            selected.append(copy.deepcopy(process))
        else:
            skipped.append(process.get("p_id"))
    projection = copy.deepcopy(config)
    projection["processes"] = selected
    return projection, {
        "mode": "local_controllers", "selected": len(selected),
        "skipped": len(skipped), "skipped_process_ids": tuple(skipped),
        "controller_uuids": tuple(sorted(local)),
    }


def _build_fsm_context(config, options=None):
    try:
        effective = options if _is_dict(options) else {"fsm_scope": "all"}
        projected, scope = _local_fsm_projection(config, effective)
        machines = fsm_machines_from_config(projected)
        return {
            "ok": True, "machines": machines, "error": None, "scope": scope}
    except Exception as exc:
        LOGGER.exception("FSM config projection failed")
        return {
            "ok": False, "machines": {}, "error": str(exc), "scope": {}}


def _restore_fsm_context(new_context, previous_context):
    if not new_context.get("ok") or not previous_context.get("ok"):
        return new_context

    previous = previous_context.get("machines") or {}

    for name, machine in tuple((new_context.get("machines") or {}).items()):
        old_machine = previous.get(name)

        if not _is_dict(old_machine):
            continue

        try:
            snapshot = fsm_snapshot(old_machine)
            new_context["machines"][name] = fsm_restore(
                machine["spec"], snapshot)
        except Exception:
            LOGGER.warning("FSM runtime restore skipped for %s", name)

    return new_context


def _states_facts(config):
    result = {}
    for row in config.get("process_states") or []:
        if _is_dict(row) and row.get("state_id") is not None:
            result[row["state_id"]] = copy.deepcopy(row)
    return result


def _sensor_facts(context):
    capabilities = context["resources"].get("sensor_value_store") or {}
    reader = capabilities.get("get_all_snapshot")
    snapshot = reader() if callable(reader) else {"controllers": {}}
    result = {}
    for controller_uuid, shard in (snapshot.get("controllers") or {}).items():
        values = {}
        for sensor_key, entry in (shard.get("values") or {}).items():
            sensor_id = entry.get("sensor_id", sensor_key)
            values[sensor_id] = {
                "value": copy.deepcopy(entry.get("value")),
                "ts": entry.get("timestamp_s"),
                "quality": entry.get("quality"),
                "sequence": entry.get("sequence"),
            }
        result[controller_uuid] = values
    return result


def _rule_facts(context):
    with context["config_lock"]:
        config = copy.deepcopy(context["execution_config_snapshot"])
    return {
        "sensors": _sensor_facts(context),
        "filters": {},
        "states": _states_facts(config),
        "now": time.time(),
    }


def _evaluate_rules_job(context):
    with context["engine_lock"]:
        engine = context["rule_engine"]
    return rule_engine.evaluate_rules(engine, _rule_facts(context))


def _run_fsm_event_job(machine, event_name, payload, **kwargs):
    return fsm_send(
        machine,
        event_name,
        payload,
        priority=int(kwargs.get("priority", 0)),
    )


def _submit_fsm_job(context, item):
    process_name = item["process_name"]

    with context["engine_lock"]:
        machine = copy.deepcopy(
            (context["fsm"].get("machines") or {}).get(process_name))

    with context["config_lock"]:
        config_revision = context["config_revision"]

    if not _is_dict(machine):
        context["statistics"]["fsm_errors"] += 1
        return False

    job_id = context["pool"]["submit_job"]({
        "fn": _run_fsm_event_job,
        "priority": _pool_priority(
            context,
            "pool_priority_fsm",
            item.get("priority", 0),
        ),
        "timeout_s": context["options"]["pool_default_job_timeout_s"],
        "queue_timeout_s": context["options"]["pool_queue_timeout_s"],
        "args": (
            machine,
            item["event_name"],
            copy.deepcopy(item.get("data") or {}),
        ),
        "kwargs": {"priority": item.get("priority", 0)},
        "meta": {
            "kind": "fsm",
            "process_name": process_name,
            "config_revision": config_revision,
            "source_event_id": item.get("source_event_id"),
            "fsm_item": copy.deepcopy(item),
            "retry_count": int(item.get("retry_count", 0)),
        },
    })

    if job_id:
        context["statistics"]["fsm_jobs"] += 1

    return bool(job_id)


def _schedule_next_fsm_event(context, process_name):
    with context["fsm_dispatch_lock"]:
        pending = context["fsm_pending"].get(process_name) or []

        if process_name in context["fsm_active"] or not pending:
            return False

        item = pending.pop(0)
        context["fsm_active"].add(process_name)

    submitted = _submit_fsm_job(context, item)

    if submitted:
        return True

    with context["fsm_dispatch_lock"]:
        context["fsm_active"].discard(process_name)

    return False


def _finish_fsm_event(context, process_name):
    with context["fsm_dispatch_lock"]:
        context["fsm_active"].discard(process_name)

    _schedule_next_fsm_event(context, process_name)


def _retry_fsm_event(context, item):
    process_name = item["process_name"]

    with context["fsm_dispatch_lock"]:
        context["fsm_active"].discard(process_name)
        context["fsm_pending"].setdefault(process_name, []).insert(0, item)

    _schedule_next_fsm_event(context, process_name)


def _submit_fsm_event(context, event):
    payload = _event_payload(event)
    process_name = str(payload.get("process_name") or payload.get("instance") or "")
    event_name = str(payload.get("event_name") or payload.get("event") or "")

    if not process_name or not event_name:
        context["statistics"]["fsm_errors"] += 1
        return False

    item = {
        "process_name": process_name,
        "event_name": event_name,
        "data": copy.deepcopy(payload.get("data") or {}),
        "priority": payload.get("priority", 0),
        "source_event_id": event.get("event_id"),
        "retry_count": 0,
    }

    with context["fsm_dispatch_lock"]:
        context["fsm_pending"].setdefault(process_name, []).append(item)

    _schedule_next_fsm_event(context, process_name)
    return True


# =============================================================================
# AUTOMATION JOBS
# =============================================================================

def _automation_identity(payload):
    return "{}:{}".format(
        str(payload.get("controller_uuid") or ""),
        str(payload.get("sensor_id")),
    )


def _automation_projection_fingerprint(projection):
    """Hash only the deterministic inputs used by one automation job."""
    try:
        return type_library.fingerprint(projection)
    except Exception:
        return ""

def _controller_ids_for_uuids(config, controller_uuids):
    allowed_uuids = set(controller_uuids or ())
    controller_ids = set()
    for controller in config.get("controllers") or []:
        if not _is_dict(controller):
            continue
        identity = str(controller.get("controller_uuid") or "")
        if identity not in allowed_uuids:
            continue
        try:
            controller_ids.add(int(controller.get("controller_id")))
        except Exception:
            continue
    return tuple(sorted(controller_ids))


def _pipeline_sort_key(item):
    return int(item.get("sequence", 0))


def _schedule_next_pipeline(context):
    selected = None

    with context["pipeline_lock"]:
        if context["pipeline_active"] or not context["pipeline_pending"]:
            return False

        selected = min(
            context["pipeline_pending"].values(),
            key=_pipeline_sort_key,
        )
        identity = selected["identity"]
        context["pipeline_pending"].pop(identity, None)
        context["pipeline_active"] = True
        context["pipeline_current"] = copy.deepcopy(selected)

    if context["options"]["rule_engine_enabled"]:
        job_id = _submit_rule_job(
            context,
            selected["payload"],
            source_event_id=selected.get("source_event_id"),
            pipeline_sequence=selected["sequence"],
        )
    else:
        job_id = _submit_automation_job(
            context,
            selected["payload"],
            source_event_id=selected.get("source_event_id"),
            pipeline_sequence=selected["sequence"],
        )

    if job_id:
        return True

    with context["pipeline_lock"]:
        context["pipeline_active"] = False
        context["pipeline_current"] = None

    return False


def _finish_pipeline(context):
    with context["pipeline_lock"]:
        context["pipeline_active"] = False
        context["pipeline_current"] = None
        context["statistics"]["pipelines_completed"] += 1

    _schedule_next_pipeline(context)


def _enqueue_sensor_pipeline(context, event):
    payload = _event_payload(event)
    identity = _automation_identity(payload)

    with context["pipeline_lock"]:
        context["pipeline_sequence"] += 1
        replaced = identity in context["pipeline_pending"]
        context["pipeline_pending"][identity] = {
            "identity": identity,
            "payload": copy.deepcopy(payload),
            "source_event_id": event.get("event_id"),
            "sequence": context["pipeline_sequence"],
        }

        if replaced:
            context["statistics"]["sensor_events_coalesced"] += 1

    _schedule_next_pipeline(context)
    return True


def _get_automation_slot(context, identity):
    with context["automation_lock"]:
        slot = context["automation_slots"].get(identity)
        if slot is None:
            slot = {"lock": threading.Lock(), "persistent_state": {}}
            context["automation_slots"][identity] = slot
        return slot


def _prune_automation_slots(context, config):
    valid = set()

    for group in config.get("sensors") or []:
        if not _is_dict(group):
            continue
        controller_uuid = str(group.get("controller_uuid") or "")

        for row in group.get("rows") or []:
            if _is_dict(row) and row.get("sensor_id") is not None:
                valid.add("{}:{}".format(controller_uuid, row["sensor_id"]))

    with context["automation_lock"]:
        stale = set(context["automation_slots"]) - valid

        for identity in stale:
            context["automation_slots"].pop(identity, None)

    return len(stale)


def _run_automation_job(context, projection, sensor_data, **kwargs):
    identity = kwargs.get("identity") or _automation_identity(sensor_data)
    slot = _get_automation_slot(context, identity)
    thread_info = {
        "job_id": kwargs.get("job_id") or identity,
        "sensor_id": projection["sensor_id"],
        "persistent_state": slot["persistent_state"],
        "node_id": context["options"]["node_id"],
        "allowed_controller_ids": tuple(
            kwargs.get("allowed_controller_ids") or ()
        ),
        "controller_scope_explicit": True,
    }
    projected_sensor = copy.deepcopy(sensor_data)
    projected_sensor["sensor_id"] = projection["sensor_id"]
    with slot["lock"]:
        tasks = run_automation_cycle(
            projection["config"], projected_sensor, thread_info)
        slot["persistent_state"] = copy.deepcopy(
            thread_info.get("persistent_state") or {})
    return tasks


def _submit_automation_job(context, payload, **kwargs):
    controller_uuid = str(payload.get("controller_uuid") or "")
    sensor_id = payload.get("sensor_id")

    with context["config_lock"]:
        config = copy.deepcopy(context["execution_config_snapshot"])
        config_revision = context["config_revision"]

    local_controller_uuids = _local_controller_uuids(context, config)
    if controller_uuid not in local_controller_uuids:
        _increment_stat(context, "automation_rejected")
        _increment_stat(context, "ownership_rejected")
        return ""

    projection = build_automation_projection(config, controller_uuid, sensor_id)

    if not projection:
        context["statistics"]["automation_rejected"] += 1
        return ""

    identity = _automation_identity(payload)
    allowed_controller_ids = _controller_ids_for_uuids(
        config,
        local_controller_uuids,
    )
    automation_id = context["pool"]["submit_job"]({
        "fn": _run_automation_job,
        "priority": _pool_priority(
            context,
            "pool_priority_automation",
            payload.get("priority", 0),
        ),
        "timeout_s": context["options"]["pool_default_job_timeout_s"],
        "queue_timeout_s": context["options"]["pool_queue_timeout_s"],
        "args": (context, projection, copy.deepcopy(payload)),
        "kwargs": {
            "identity": identity,
            "allowed_controller_ids": allowed_controller_ids,
        },
        "meta": {
            "kind": "automation",
            "identity": identity,
            "source_event_id": kwargs.get("source_event_id"),
            "config_revision": config_revision,
            "projection_fingerprint": _automation_projection_fingerprint(
                projection),
            "sensor_payload": copy.deepcopy(payload),
            "retry_count": int(kwargs.get("retry_count", 0)),
            "pipeline_sequence": kwargs.get("pipeline_sequence"),
        },
    })

    if automation_id:
        context["statistics"]["automation_jobs"] += 1

    return automation_id


def _submit_rule_job(context, payload, **kwargs):
    identity = _automation_identity(payload)

    with context["config_lock"]:
        config_revision = context["config_revision"]

    rule_id = context["pool"]["submit_job"]({
        "fn": _evaluate_rules_job,
        "priority": _pool_priority(context, "pool_priority_rule"),
        "timeout_s": context["options"]["pool_default_job_timeout_s"],
        "queue_timeout_s": context["options"]["pool_queue_timeout_s"],
        "args": (context,),
        "meta": {
            "kind": "rule_engine",
            "identity": identity,
            "source_event_id": kwargs.get("source_event_id"),
            "config_revision": config_revision,
            "sensor_payload": copy.deepcopy(payload),
            "retry_count": int(kwargs.get("retry_count", 0)),
            "pipeline_sequence": kwargs.get("pipeline_sequence"),
        },
    })

    if rule_id:
        context["statistics"]["rule_jobs"] += 1

    return rule_id


def _submit_sensor_jobs(context, event):
    return _enqueue_sensor_pipeline(context, event)


# =============================================================================
# CONTROLLER AND CONNECTION OWNERSHIP
# =============================================================================

def _register_default_adapters():
    from src.adapter._connection_registry import register_adapter_factory
    from src.adapter.fieldbus._ethernet_ip_adapter import create_ethernet_ip_adapter
    from src.adapter.fieldbus._knx_ip_adapter import create_knx_ip_adapter
    from src.adapter.fieldbus._modbus_tcp_adapter import create_modbus_tcp_adapter
    from src.adapter.fieldbus._profibus_gw_adapter import create_profibus_gw_adapter
    from src.adapter.fieldbus._profinet_rt_adapter import create_profinet_rt_adapter
    from src.adapter.plc._plc_modbus_adapter import create_plc_modbus_adapter
    from src.adapter.plc._plc_opcua_adapter import create_plc_opcua_adapter
    from src.adapter.plc._tia_plc_adapter import create_tia_plc_adapter
    factories = {
        "modbus_tcp": create_modbus_tcp_adapter,
        "ethernet_ip": create_ethernet_ip_adapter,
        "knx_ip": create_knx_ip_adapter,
        "profibus_gw": create_profibus_gw_adapter,
        "profinet_rt": create_profinet_rt_adapter,
        "plc_modbus": create_plc_modbus_adapter,
        "plc_opcua": create_plc_opcua_adapter,
        "plc_s7": create_tia_plc_adapter,
    }
    for name, factory in factories.items():
        register_adapter_factory(name, factory)


def _start_connections(context):
    if not context["options"]["connections_enabled"]:
        return {}
    from src.adapter._connection_registry import create_connection_registry
    from src.adapter._connection_thread import start_connections_from_config
    _register_default_adapters()
    registry = create_connection_registry(name="runtime")
    with context["config_lock"]:
        config = copy.deepcopy(context["config_snapshot"])
    node_config = build_worker_shard(config, context["options"]["node_id"])
    selected = _local_controller_uuids(context, config)
    node_config["controllers"] = [
        row for row in node_config.get("controllers") or []
        if str(row.get("controller_uuid") or "") in selected
    ]
    node_config["virtual_controllers"] = [
        row for row in node_config.get("virtual_controllers") or []
        if str(row.get("controller_uuid") or "") in selected
    ]
    LOGGER.info(
        "Starting %d connection owner(s) for node %s",
        len(node_config["controllers"]),
        context["options"]["node_id"],
    )
    queues = start_connections_from_config(
        registry, node_config, event_queue=context.get("queue_event_send"))
    context["connection_registry"] = registry
    context["connection_queues"] = queues
    return queues


def _request_queue_for_controller(context, controller):
    if not context.get("connection_registry"):
        return None
    try:
        from src.adapter._connection_registry import resolve_conn_key_for_controller
        with context["config_lock"]:
            config = context["config_snapshot"]
        key = resolve_conn_key_for_controller(controller, config)
        return context["connection_queues"].get(key)
    except Exception:
        LOGGER.exception("Connection queue resolution failed")
        return None


def _local_controller_uuids(context, config):
    routed = set(get_node_controller_uuids(
        config, context["options"]["node_id"]))
    configured = set(context["options"].get("controller_allowlist") or ())
    return routed & configured if configured else routed


def _build_local_execution_config(config, options):
    configured = options.get("controller_allowlist")
    allowlist = configured if configured else None
    try:
        return build_local_execution_shard(
            config,
            options.get("node_id"),
            controller_allowlist=allowlist,
        )
    except TypeError:
        # Compatibility during rolling upgrades of the pure projection
        # library. The implementation accepts the allowlist.
        return build_local_execution_shard(config, options.get("node_id"))


def _canonicalize_local_actuator_task(config, task):
    """Resolve a UUID intent to owner-local immutable bus coordinates.

    Remote and Cross producers are not trusted to provide address, function,
    unit or controller id.  The hardware owner overwrites those fields from
    its own node shard before safety evaluation and dispatch.
    """
    controller_uuid = str(task.get("controller_uuid") or "").strip()
    actuator_id = task.get("actuator_id")
    selected = None
    for group in config.get("actuators") or []:
        if not _is_dict(group):
            continue
        if str(group.get("controller_uuid") or "") != controller_uuid:
            continue
        for row in group.get("rows") or []:
            if not _is_dict(row):
                continue
            if str(row.get("actuator_id")) == str(actuator_id):
                selected = (group, row)
                break
        if selected is not None:
            break
    if selected is None:
        return None
    group, row = selected
    prepared = copy.deepcopy(task)
    for key in ("address", "function_type", "bus_type", "unit"):
        value = row.get(key, group.get(key))
        if value is not None:
            prepared[key] = copy.deepcopy(value)
    prepared["actuator_id"] = row.get("actuator_id")
    for controller in config.get("controllers") or []:
        if not _is_dict(controller):
            continue
        if str(controller.get("controller_uuid") or "") == controller_uuid:
            prepared["controller_id"] = controller.get("controller_id")
            break
    metadata = prepared.get("metadata")
    metadata = metadata if _is_dict(metadata) else {}
    cross_meta = metadata.get("cross_runtime")
    cross_intent = (
        str(prepared.get("execution_scope") or "") == "cross"
        or _is_dict(cross_meta)
    )
    if cross_intent:
        engineering_value = prepared.get(
            "engineering_value", prepared.get("control_value"))
        prepared["engineering_value"] = copy.deepcopy(engineering_value)
        function_type = str(prepared.get("function_type") or "")
        if function_type == "write_coil":
            try:
                prepared["control_value"] = (
                    1 if float(engineering_value) > 0.5 else 0)
            except Exception:
                prepared["control_value"] = 0
        elif function_type in (
            "write_register", "write_holding_register",
            "write_single_register",
        ):
            scaling = {
                row_value.get("profile_id"): row_value
                for row_value in config.get("scaling_profiles") or []
                if _is_dict(row_value) and row_value.get("profile_id") is not None
            }
            prepared["control_value"] = scaled_to_raw(
                {"_cache_scaling_profiles": scaling},
                row,
                engineering_value,
            )
    return prepared


def _controller_timing_defaults(context, controller_uuid):
    defaults = context["options"].get("controller_timing_defaults") or {}
    selected = defaults.get(str(controller_uuid))
    return copy.deepcopy(selected) if _is_dict(selected) else {}


def _controller_is_local(context, config, controller_uuid):
    return str(controller_uuid) in _local_controller_uuids(context, config)


def _start_one_controller(context, controller_uuid, package=None):
    if controller_uuid in context["controller_workers"]:
        return context["controller_workers"][controller_uuid]
    with context["config_lock"]:
        current_config = copy.deepcopy(context["config_snapshot"])
    if not _controller_is_local(context, current_config, controller_uuid):
        _increment_stat(context, "ownership_rejected")
        return None
    source = package
    if source is None:
        with context["config_lock"]:
            source = copy.deepcopy(context["config_snapshot"])
    projection = source.get("sections") if _is_dict(source) else None
    projection = projection if _is_dict(projection) else source
    controller = {}
    for row in projection.get("controllers") or []:
        if str(row.get("controller_uuid")) == str(controller_uuid):
            controller = row
            break
    worker = create_controller_worker(
        controller_uuid=controller_uuid,
        config_package=source,
        request_queue=_request_queue_for_controller(context, controller),
        queue_event_send=context.get("queue_event_send"),
        runtime_resources=context["resources"],
        controller_target=context.get("controller_target"),
        join_timeout_s=context["options"]["join_timeout_s"],
        controller_timing_defaults=_controller_timing_defaults(
            context,
            controller_uuid,
        ),
    )
    context["controller_workers"][controller_uuid] = worker
    start_controller_worker(worker)
    return worker


def _start_initial_controllers(context):
    if not context["options"]["controllers_enabled"]:
        return
    with context["config_lock"]:
        config = copy.deepcopy(context["config_snapshot"])
    for identity in sorted(_local_controller_uuids(context, config)):
        _start_one_controller(context, identity)


def _connection_transport_signature(controller):
    """Hash only settings that require rebuilding the physical connection."""
    comm = controller.get("comm") if _is_dict(controller) else None
    comm = comm if _is_dict(comm) else {}
    selected = {}
    for key in (
        "area_rids",
        "connection_type",
        "optimized_block_access",
        "rack",
        "s7_max_parallel",
        "s7_multi_read_max_gap",
        "s7_multi_var_limit",
        "s7_read_optimizer",
        "security",
        "slot",
        "string_encoding",
    ):
        if key in controller:
            selected[key] = controller.get(key)
        if key in comm:
            selected[key] = comm.get(key)
    return type_library.fingerprint(selected)

def _connection_signature(context, config):
    node_id = context["options"]["node_id"]
    selected = _local_controller_uuids(context, config)
    shard = build_worker_shard(config, node_id)
    rows = []

    for controller in shard.get("controllers") or []:
        if not _is_dict(controller):
            continue
        if str(controller.get("controller_uuid") or "") not in selected:
            continue
        rows.append((
            str(controller.get("controller_uuid") or ""),
            str(controller.get("protocol_type") or controller.get("bus_type") or ""),
            str(controller.get("host") or controller.get("ip_address") or ""),
            str(controller.get("port") or ""),
            str(controller.get("unit") or ""),
            _connection_transport_signature(controller),
        ))

    virtual = []

    for endpoint in shard.get("virtual_controllers") or []:
        if not _is_dict(endpoint):
            continue
        if str(endpoint.get("controller_uuid") or "") not in selected:
            continue
        virtual.append((
            str(endpoint.get("controller_uuid") or ""),
            str(endpoint.get("host") or endpoint.get("ip_address") or ""),
            str(endpoint.get("port") or ""),
            bool(endpoint.get("virt_active", endpoint.get("enabled", False))),
        ))

    return tuple(sorted(rows)), tuple(sorted(virtual))


def _restart_connection_owners(context):
    for worker in tuple(context["controller_workers"].values()):
        stop_controller_worker(
            worker, join_timeout_s=context["options"]["join_timeout_s"])

    context["controller_workers"].clear()
    registry = context.get("connection_registry")

    if registry is not None:
        from src.adapter._connection_registry import registry_stop_all
        registry_stop_all(
            registry, join_timeout_s=context["options"]["join_timeout_s"])

    context["connection_registry"] = None
    context["connection_queues"] = {}
    _start_connections(context)


def _dispatch_actuator_task(context, task):
    with context["config_lock"]:
        config = copy.deepcopy(context["config_snapshot"])
        config_revision = context["config_revision"]
    controller_uuid = str(task.get("controller_uuid") or "")
    if not _controller_is_local(context, config, controller_uuid):
        return _record_actuator_rejection(
            context,
            task,
            {
                "ok": False,
                "code": "ownership",
                "message": "controller is outside the active local ownership scope",
            },
            config_revision=config_revision,
        )
    canonical = _canonicalize_local_actuator_task(config, task)
    if canonical is None:
        return _record_actuator_rejection(
            context,
            task,
            {
                "ok": False,
                "code": "actuator_identity",
                "message": "controller_uuid + actuator_id is not in the local shard",
            },
            config_revision=config_revision,
        )
    task = canonical
    safety = evaluate_actuator_task(
        context["safety"], config, task, current_revision=config_revision)
    if not safety.get("ok"):
        return _record_actuator_rejection(
            context,
            task,
            safety,
            config_revision=config_revision,
            safety=True,
        )
    worker = context["controller_workers"].get(controller_uuid)
    if worker is None:
        remote = context["resources"].get("remote_actuator_dispatcher")
        if callable(remote) and not task.get("_local_only"):
            try:
                accepted = bool(remote(copy.deepcopy(task)))
            except Exception:
                LOGGER.exception("Remote actuator dispatch failed")
                accepted = False
            key = (
                "remote_actuator_dispatched"
                if accepted else "remote_actuator_rejected"
            )
            context["statistics"][key] += 1
            return accepted
        return _record_actuator_rejection(
            context,
            task,
            {
                "ok": False,
                "code": "controller_worker_unavailable",
                "message": "local controller worker is unavailable",
            },
            config_revision=config_revision,
        )
    event = make_event(
        "ACTUATOR_TASK",
        payload={"task": copy.deepcopy(task), "source": "automation"},
        source="thread_management",
        target="controller",
    )
    written = dispatch_controller_event(worker, event)
    if written:
        context["statistics"]["actuator_dispatched"] += 1
    return written


def dispatch_local_actuator_task(context, task):
    if not _is_dict(context) or not _is_dict(task):
        return False
    prepared = copy.deepcopy(task)
    prepared["_local_only"] = True
    return _dispatch_actuator_task(context, prepared)


def _dispatch_controller_event(context, event):
    payload = _event_payload(event)
    task = payload.get("task") if _is_dict(payload.get("task")) else payload
    if _event_type(event) == "ACTUATOR_TASK":
        return _dispatch_actuator_task(context, task)
    controller_uuid = str(task.get("controller_uuid") or payload.get("controller_uuid") or "")
    worker = context["controller_workers"].get(controller_uuid)
    if worker is None:
        return False
    return dispatch_controller_event(worker, event)


# =============================================================================
# POOL RESULT DISPATCH
# =============================================================================

def _handle_automation_result(context, result):
    metadata = result.get("meta") or {}
    result_revision = metadata.get("config_revision")

    with context["config_lock"]:
        current_revision = context["config_revision"]

    if result_revision != current_revision:
        context["statistics"]["stale_automation_results"] += 1

        payload = metadata.get("sensor_payload") or {}
        with context["config_lock"]:
            current_config = copy.deepcopy(
                context.get(
                    "execution_config_snapshot",
                    context.get("config_snapshot") or {},
                ))
        current_projection = build_automation_projection(
            current_config,
            payload.get("controller_uuid"),
            payload.get("sensor_id"),
        )
        compatible = bool(
            current_projection
            and metadata.get("projection_fingerprint")
            and metadata.get("projection_fingerprint")
            == _automation_projection_fingerprint(current_projection)
        )
        if compatible:
            context["statistics"]["stale_automation_compatible"] += 1
            tasks = result.get("result") or []
            for task in tasks if isinstance(tasks, list) else []:
                if _is_dict(task):
                    prepared = copy.deepcopy(task)
                    prepared["config_revision"] = current_revision
                    _dispatch_actuator_task(context, prepared)
            _finish_pipeline(context)
            return

        if int(metadata.get("retry_count", 0)) < 2:
            retry_id = _submit_automation_job(
                context,
                metadata.get("sensor_payload") or {},
                source_event_id=metadata.get("source_event_id"),
                retry_count=int(metadata.get("retry_count", 0)) + 1,
                pipeline_sequence=metadata.get("pipeline_sequence"),
            )

            if not retry_id:
                _finish_pipeline(context)

        else:
            _finish_pipeline(context)

        return

    tasks = result.get("result") or []
    if not isinstance(tasks, list):
        _finish_pipeline(context)
        return
    for task in tasks:
        if _is_dict(task):
            prepared = copy.deepcopy(task)
            prepared["config_revision"] = result_revision
            _dispatch_actuator_task(context, prepared)

    _finish_pipeline(context)


def _request_rule_actions(context, actions, metadata, result_revision):
    writer = context["resources"].get("config_writer") or {}
    request = writer.get("request_action_plan")
    if not callable(request):
        _increment_stat(context, "rule_action_errors")
        _increment_stat(context, "rule_action_control_fallbacks")
        LOGGER.error(
            "Rule action writer is unavailable; local automation continues "
            "with the last committed Worker snapshot")
        return "control_fallback"
    operation_id = "rule:{}:{}:{}".format(
        metadata.get("source_event_id"),
        result_revision,
        metadata.get("pipeline_sequence"),
    )
    try:
        response = request(
            actions,
            source="generic_rule_engine",
            base_revision=result_revision,
            operation_id=operation_id,
        )
    except Exception:
        _increment_stat(context, "rule_action_errors")
        _increment_stat(context, "rule_action_control_fallbacks")
        LOGGER.exception(
            "Rule action commit failed unexpectedly; local automation "
            "continues with the last committed Worker snapshot")
        return "control_fallback"
    if not _is_dict(response):
        _increment_stat(context, "rule_action_errors")
        _increment_stat(context, "rule_action_control_fallbacks")
        LOGGER.error(
            "Rule action writer returned an invalid response; local "
            "automation continues with the last committed Worker snapshot")
        return "control_fallback"
    if response.get("ok"):
        return "ok"
    _increment_stat(context, "rule_action_errors")
    safety = response.get("safety") or {}
    code = str(response.get("code") or safety.get("code") or "")
    if response.get("local_committed") is True:
        _increment_stat(context, "rule_action_sync_degraded")
        LOGGER.warning(
            "Local RuleEngine commit succeeded but asynchronous Master sync "
            "is degraded: code=%s; actuator control continues locally",
            code or "unknown",
        )
        return "ok"
    if code not in ("stale_revision", "stale_authority_revision"):
        if code in (
            "capability", "outbox_conflict", "proposal_journal",
            "proposal_timeout", "replication_timeout",
            "transport_unavailable",
        ):
            _increment_stat(context, "rule_action_control_fallbacks")
            LOGGER.warning(
                "RuleEngine synchronization is unavailable: code=%s; "
                "local automation continues with the last committed Worker "
                "snapshot",
                code or "unknown",
            )
            return "control_fallback"
        return "error"
    if int(metadata.get("retry_count", 0)) >= 3:
        return "stale_exhausted"
    retry_id = _submit_rule_job(
        context,
        metadata.get("sensor_payload") or {},
        source_event_id=metadata.get("source_event_id"),
        retry_count=int(metadata.get("retry_count", 0)) + 1,
        pipeline_sequence=metadata.get("pipeline_sequence"),
    )
    return "retry" if retry_id else "error"


def _submit_latest_automation(context, metadata):
    automation_id = _submit_automation_job(
        context,
        metadata.get("sensor_payload") or {},
        source_event_id=metadata.get("source_event_id"),
        pipeline_sequence=metadata.get("pipeline_sequence"),
    )
    if automation_id:
        context["statistics"]["rule_stale_automation_fallbacks"] += 1
        return True
    _finish_pipeline(context)
    return False


def _handle_rule_result(context, result):
    metadata = result.get("meta") or {}
    result_revision = metadata.get("config_revision")

    with context["config_lock"]:
        current_revision = context["config_revision"]

    if result_revision != current_revision:
        context["statistics"]["stale_rule_results"] += 1

        if int(metadata.get("retry_count", 0)) < 3:
            retry_id = _submit_rule_job(
                context,
                metadata.get("sensor_payload") or {},
                source_event_id=metadata.get("source_event_id"),
                retry_count=int(metadata.get("retry_count", 0)) + 1,
                pipeline_sequence=metadata.get("pipeline_sequence"),
            )

            if not retry_id:
                _finish_pipeline(context)

        else:
            _submit_latest_automation(context, metadata)

        return

    decision = result.get("result")
    if not _is_dict(decision):
        _finish_pipeline(context)
        return
    actions = decision.get("actions") or []

    if actions:
        action_status = _request_rule_actions(
            context, actions, metadata, result_revision)
        if action_status == "retry":
            return
        if action_status == "stale_exhausted":
            _submit_latest_automation(context, metadata)
            return
        if action_status == "control_fallback":
            _submit_latest_automation(context, metadata)
            return
        if action_status != "ok":
            _finish_pipeline(context)
            return

    automation_id = _submit_automation_job(
        context,
        metadata.get("sensor_payload") or {},
        source_event_id=metadata.get("source_event_id"),
        pipeline_sequence=metadata.get("pipeline_sequence"),
    )

    if not automation_id:
        _finish_pipeline(context)


def _handle_fsm_result(context, result):
    metadata = result.get("meta") or {}
    process_name = str(metadata.get("process_name") or "")

    with context["config_lock"]:
        current_revision = context["config_revision"]

    if metadata.get("config_revision") != current_revision:
        item = metadata.get("fsm_item") or {}

        if int(metadata.get("retry_count", 0)) < 2 and _is_dict(item):
            item = copy.deepcopy(item)
            item["retry_count"] = int(metadata.get("retry_count", 0)) + 1
            _retry_fsm_event(context, item)
        else:
            context["statistics"]["fsm_errors"] += 1
            _finish_fsm_event(context, process_name)

        return

    machine = result.get("result")

    if not _is_dict(machine):
        context["statistics"]["fsm_errors"] += 1
        _finish_fsm_event(context, process_name)
        return

    with context["engine_lock"]:
        machines = context["fsm"].get("machines") or {}

        if process_name not in machines:
            context["statistics"]["fsm_errors"] += 1
            _finish_fsm_event(context, process_name)
            return

        machines[process_name] = copy.deepcopy(machine)

    context["statistics"]["fsm_updates"] += 1
    _mirror_external_event(context, make_event(
        "FSM_STATE_UPDATE",
        payload={
            "node_id": context["options"]["node_id"],
            "process_name": process_name,
            "config_revision": current_revision,
            "snapshot": fsm_snapshot(machine),
        },
        source="worker_local_fsm",
        target="external",
    ))
    _finish_fsm_event(context, process_name)


def _handle_failed_pool_result(context, result):
    context["statistics"]["pool_job_errors"] += 1
    context["last_error"] = copy.deepcopy(result)
    metadata = result.get("meta") or {}
    kind = metadata.get("kind")

    if kind in ("automation", "rule_engine"):
        _finish_pipeline(context)

    if kind == "fsm":
        process_name = str(metadata.get("process_name") or "")
        context["statistics"]["fsm_errors"] += 1
        _finish_fsm_event(context, process_name)


def _dispatch_pool_result(context, result):
    metadata = result.get("meta") if _is_dict(result.get("meta")) else {}
    if metadata.get("kind") in ("cross_pipeline", "cross_fsm"):
        dispatcher = context["resources"].get("cross_result_dispatcher")
        accepted = False
        if callable(dispatcher):
            try:
                accepted = bool(dispatcher(copy.deepcopy(result)))
            except Exception:
                LOGGER.exception("Cross pool result dispatch failed")
        key = "cross_results_dispatched" if accepted else "cross_results_rejected"
        _increment_stat(context, key)
        return
    handlers = {
        "automation": _handle_automation_result,
        "rule_engine": _handle_rule_result,
        "fsm": _handle_fsm_result,
    }
    kind = (result.get("meta") or {}).get("kind")
    handler = handlers.get(kind)

    if callable(handler):
        handler(context, result)


def _handle_pool_result(context, result):
    if not _is_dict(result):
        return

    if not result.get("ok"):
        metadata = (
            result.get("meta")
            if _is_dict(result.get("meta"))
            else {}
        )
        if metadata.get("kind") in ("cross_pipeline", "cross_fsm"):
            _dispatch_pool_result(context, result)
        _handle_failed_pool_result(context, result)
        return

    _dispatch_pool_result(context, result)


def _result_loop(context):
    result_queue = context["pool"]["_out_q"]
    while not context["stop_event"].is_set():
        try:
            result = result_queue.get(timeout=context["options"]["ingress_timeout_s"])
        except Empty:
            continue
        try:
            _handle_pool_result(context, result)
        finally:
            try:
                result_queue.task_done()
            except Exception:
                pass


# =============================================================================
# CONFIG ROLLOUT
# =============================================================================

def _local_rollout_scope(context, previous_config, config, affected, removed):
    previous_local = _local_controller_uuids(context, previous_config)
    current_local = _local_controller_uuids(context, config)
    existing = set(context["controller_workers"])
    moved_away = previous_local - current_local
    removed_local = set(removed) | moved_away | (existing - current_local)
    affected_local = (set(affected) & (previous_local | current_local))
    affected_local.update(removed_local)
    return tuple(sorted(affected_local)), removed_local


def apply_config_rollout(context, rollout):
    if not _is_dict(context) or not _is_dict(rollout):
        return {"ok": False, "error": "invalid rollout arguments"}
    config = rollout.get("config_snapshot")
    if not _is_dict(config):
        return {"ok": False, "error": "rollout has no config snapshot"}
    with context["config_lock"]:
        previous_config = copy.deepcopy(context["config_snapshot"])
        context["config_snapshot"] = copy.deepcopy(config)
        execution_config = _build_local_execution_config(
            config, context["options"])
        context["execution_config_snapshot"] = execution_config
        context["config_revision"] = rollout.get("revision", 0)
        context["config_fingerprint"] = rollout.get("fingerprint", "")
    context["pool"]["config_replace"](execution_config)
    _prune_automation_slots(context, execution_config)
    with context["engine_lock"]:
        old_runtime = rule_engine.engine_export_runtime(context["rule_engine"])
        old_rule_stats = rule_engine.engine_stats(context["rule_engine"])
        previous_fsm = copy.deepcopy(context["fsm"])
    new_engine, binding = _build_rule_context(
        execution_config, context["options"])
    rule_engine.engine_import_runtime(new_engine, old_runtime)
    with new_engine["lock"]:
        new_engine["stats"].update(old_rule_stats)
    new_fsm = _restore_fsm_context(
        _build_fsm_context(execution_config, context["options"]), previous_fsm)
    with context["engine_lock"]:
        context["rule_engine"] = new_engine
        context["rule_binding"] = binding
        context["fsm"] = new_fsm
    results = {}
    affected = tuple(rollout.get("affected_controller_uuids") or ())
    removed = set(rollout.get("removed_controller_uuids") or ())

    previous_signature = _connection_signature(context, previous_config)
    current_signature = _connection_signature(context, config)
    connection_restart = (
        context["options"]["connections_enabled"]
        and (
            context.get("connection_registry") is None
            or previous_signature != current_signature
        )
    )

    if connection_restart:
        previous_workers = tuple(context["controller_workers"])
        _restart_connection_owners(context)
        current_workers = _local_controller_uuids(context, config)
        affected = tuple(sorted(set(affected) | set(previous_workers) | set(current_workers)))

    affected, removed = _local_rollout_scope(
        context, previous_config, config, affected, removed)

    for controller_uuid in affected:
        worker = context["controller_workers"].get(controller_uuid)
        if controller_uuid in removed:
            if worker is not None:
                results[controller_uuid] = stop_controller_worker(worker)
                context["controller_workers"].pop(controller_uuid, None)
            continue
        package = (rollout.get("packages") or {}).get(controller_uuid)
        if worker is None and context["options"]["controllers_enabled"]:
            worker = _start_one_controller(context, controller_uuid, package)
        if worker is not None and package is not None:
            results[controller_uuid] = apply_controller_config(worker, package)
    failures = [value for value in results.values()
                if _is_dict(value) and value.get("ok") is False]
    context["statistics"]["config_rollouts"] += 1
    return {
        "ok": not failures,
        "revision": context["config_revision"],
        "affected_controller_uuids": tuple(affected),
        "removed_controller_uuids": tuple(sorted(removed)),
        "workers": results,
        "rule_binding": copy.deepcopy(binding),
        "fsm_ok": new_fsm.get("ok", False),
        "connections_restarted": connection_restart,
    }


# =============================================================================
# CONTEXT AND EVENT LOOPS
# =============================================================================

def _mirror_external_event(context, event):
    if _is_dict(event) and event.get("_external_mirror_suppressed"):
        return False
    mirror = context["resources"].get("external_event_mirror")
    if not callable(mirror):
        return False
    try:
        written = mirror(copy.deepcopy(event))
    except Exception:
        LOGGER.exception("External runtime event mirror failed")
        written = False
    if written is None:
        return False
    key = "external_events_published" if written else "external_events_rejected"
    context["statistics"][key] += 1
    return bool(written)


def _tm_statistics():
    return {
        "events_received": 0, "automation_jobs": 0,
        "automation_rejected": 0, "rule_jobs": 0,
        "rule_action_errors": 0, "pool_job_errors": 0,
        "rule_action_sync_degraded": 0,
        "rule_action_control_fallbacks": 0,
        "actuator_dispatched": 0, "actuator_rejected": 0,
        "remote_actuator_dispatched": 0, "remote_actuator_rejected": 0,
        "actuator_safety_rejected": 0, "ownership_rejected": 0,
        "actuator_rejected_task_limit": 0,
        "actuator_rejected_stale_revision": 0,
        "actuator_rejected_interlock": 0,
        "actuator_rejected_ownership": 0,
        "actuator_rejected_other": 0,
        "actuator_rejection_logs_suppressed": 0,
        "external_events_published": 0, "external_events_rejected": 0,
        "sensor_events_forward_only": 0,
        "cross_events_dispatched": 0, "cross_events_rejected": 0,
        "cross_results_dispatched": 0, "cross_results_rejected": 0,
        "execution_events_rejected": 0,
        "config_rollouts": 0, "stale_automation_results": 0,
        "stale_automation_compatible": 0,
        "stale_rule_results": 0, "sensor_events_coalesced": 0,
        "rule_stale_automation_fallbacks": 0,
        "pipelines_completed": 0, "fsm_jobs": 0, "fsm_updates": 0,
        "fsm_errors": 0,
    }


def create_tm_context(**kwargs):
    queues = kwargs.get("queues") if _is_dict(kwargs.get("queues")) else {}
    resources = kwargs.get("resources") if _is_dict(kwargs.get("resources")) else {}
    config = copy.deepcopy(kwargs.get("config") or {})
    options = _options_from(kwargs)
    execution_config = _build_local_execution_config(config, options)
    sensor_values = resources.get("sensor_value_store") or {}
    rule_context, binding = _build_rule_context(execution_config, options)
    pool = create_thread_pool(
        name="RuntimeAutomationPool",
        min_workers=options["pool_min_workers"],
        max_workers=options["pool_max_workers"],
        default_job_timeout_s=options["pool_default_job_timeout_s"],
        max_detached_jobs=options["pool_max_detached_jobs"],
        json_config=execution_config,
        join_timeout_s=options["join_timeout_s"],
    )
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "state": "created",
        "stop_event": kwargs.get("stop_event") or threading.Event(),
        "system_shutdown_event": kwargs.get("system_shutdown_event"),
        "options": options,
        "resources": resources,
        "safety": create_safety_context(
            options.get("safety"),
            sensor_reader=sensor_values.get("get_sensor_snapshot"),
        ),
        "queue_event_pc": queues.get("queue_event_pc"),
        "queue_event_mbc": queues.get("queue_event_mbc"),
        "queue_event_send": queues.get("queue_event_send"),
        "config_lock": threading.RLock(),
        "engine_lock": threading.RLock(),
        "config_snapshot": config,
        "execution_config_snapshot": execution_config,
        "config_revision": 0,
        "config_fingerprint": "",
        "rule_engine": rule_context,
        "rule_binding": binding,
        "fsm": _build_fsm_context(execution_config, options),
        "pool": pool,
        "controller_workers": {},
        "controller_target": kwargs.get("controller_target"),
        "connection_registry": None,
        "connection_queues": {},
        "automation_lock": threading.RLock(),
        "automation_slots": {},
        "pipeline_lock": threading.RLock(),
        "pipeline_active": False,
        "pipeline_current": None,
        "pipeline_pending": {},
        "pipeline_sequence": 0,
        "fsm_dispatch_lock": threading.RLock(),
        "fsm_pending": {},
        "fsm_active": set(),
        "thread": None,
        "result_thread": None,
        "controller_ingress_thread": None,
        "statistics": _tm_statistics(),
        "statistics_lock": threading.Lock(),
        "rejection_log_lock": threading.Lock(),
        "rejection_log_state": {},
        "last_error": None,
    }
    if context["queue_event_pc"] is None:
        raise ValueError("queue_event_pc is required")
    return context


def _management_loop(context):
    context["state"] = "running"
    forward_only_warned = False
    while not context["stop_event"].is_set():
        event = sync_queue_get(
            context["queue_event_pc"], context["options"]["ingress_timeout_s"])
        if event is None:
            continue
        if _is_stop(event):
            break
        context["statistics"]["events_received"] += 1
        _mirror_external_event(context, event)
        event_name = _event_type(event)
        if event_name == "SENSOR_VALUE_UPDATE":
            cross_dispatcher = context["resources"].get(
                "cross_event_dispatcher")
            if callable(cross_dispatcher):
                try:
                    accepted = bool(cross_dispatcher(copy.deepcopy(event)))
                except Exception:
                    LOGGER.exception("Local fact dispatch to Cross Worker failed")
                    accepted = False
                key = (
                    "cross_events_dispatched"
                    if accepted else "cross_events_rejected"
                )
                _increment_stat(context, key)
            if context["resources"].get("sensor_pipeline_enabled", True):
                _submit_sensor_jobs(context, event)
            else:
                _increment_stat(context, "sensor_events_forward_only")
                if not forward_only_warned:
                    forward_only_warned = True
                    LOGGER.error(
                        "Local sensor pipeline is disabled (forward-only); "
                        "RuleEngine, FSM and Automation cannot create local "
                        "actuator tasks")
        elif event_name == "FSM_EVENT":
            if context["resources"].get("automation_pipeline_enabled", True):
                _submit_fsm_event(context, event)
            else:
                _increment_stat(context, "execution_events_rejected")
        elif event_name == "ACTUATOR_TASK":
            if context["resources"].get("automation_pipeline_enabled", True):
                _dispatch_controller_event(context, event)
            else:
                _increment_stat(context, "execution_events_rejected")
    context["state"] = "stopped"


def _controller_ingress_loop(context):
    queue_object = context.get("queue_event_mbc")
    if queue_object is None:
        return
    while not context["stop_event"].is_set():
        event = sync_queue_get(queue_object, context["options"]["ingress_timeout_s"])
        if event is None:
            continue
        if _is_stop(event):
            break
        if context["resources"].get("automation_pipeline_enabled", True):
            _dispatch_controller_event(context, event)
        else:
            _increment_stat(context, "execution_events_rejected")


# =============================================================================
# PUBLIC LIFECYCLE
# =============================================================================

def start_thread_management(**kwargs):
    runtime = kwargs.get("runtime") if _is_dict(kwargs.get("runtime")) else {}
    context = create_tm_context(**kwargs)
    selected = _local_controller_uuids(context, context["config_snapshot"])
    LOGGER.info(
        "Worker hardware mode: node=%s controllers_enabled=%s "
        "connections_enabled=%s rule_engine_enabled=%s selected_controllers=%d",
        context["options"]["node_id"],
        context["options"]["controllers_enabled"],
        context["options"]["connections_enabled"],
        context["options"]["rule_engine_enabled"],
        len(selected),
    )
    if not context["options"]["controllers_enabled"]:
        LOGGER.warning(
            "Controller workers are disabled; no fieldbus I/O will be performed. "
            "Select an explicit worker profile to enable hardware ownership."
        )
    elif not context["options"]["connections_enabled"]:
        LOGGER.warning(
            "Controller workers are enabled without connection owners; "
            "hardware polling requires an injected test target or external queues."
        )
    context["pool"]["start"]()
    result_thread = threading.Thread(
        target=partial(_result_loop, context),
        name="ThreadManagement-PoolResults",
        daemon=False,
    )
    controller_ingress = threading.Thread(
        target=partial(_controller_ingress_loop, context),
        name="ThreadManagement-ControllerIngress",
        daemon=False,
    )
    thread = threading.Thread(
        target=partial(_management_loop, context),
        name=context["options"]["thread_name"],
        daemon=False,
    )
    context["result_thread"] = result_thread
    context["controller_ingress_thread"] = controller_ingress
    context["thread"] = thread
    runtime["context"] = context
    result_thread.start()
    controller_ingress.start()
    thread.start()
    return thread


def stop_thread_management(**kwargs):
    context = kwargs.get("context") or kwargs.get("ctx")
    if not _is_dict(context):
        return {"state": "missing"}
    context["state"] = "stopping"
    context["stop_event"].set()
    sync_queue_put(context.get("queue_event_pc"), _stop_event("thread_management"))
    if context.get("queue_event_mbc") is not None:
        sync_queue_put(context["queue_event_mbc"], _stop_event("controller"))
    for worker in tuple(context["controller_workers"].values()):
        stop_controller_worker(worker, join_timeout_s=context["options"]["join_timeout_s"])
    context["pool"]["shutdown"](
        join_timeout_s=context["options"]["join_timeout_s"])
    registry = context.get("connection_registry")
    if registry is not None:
        from src.adapter._connection_registry import registry_stop_all
        registry_stop_all(registry, join_timeout_s=context["options"]["join_timeout_s"])
    deadline = time.monotonic() + context["options"]["join_timeout_s"]
    for thread in (
        context.get("thread"),
        context.get("controller_ingress_thread"),
        context.get("result_thread"),
    ):
        if thread is None or thread is threading.current_thread():
            continue
        thread.join(timeout=max(0.0, deadline - time.monotonic()))
    live = any(_thread_alive(thread) for thread in (
        context.get("thread"), context.get("controller_ingress_thread"),
        context.get("result_thread")))
    context["state"] = "stuck" if live else "stopped"
    return get_thread_management_snapshot(context)


def get_thread_management_snapshot(context):
    if not _is_dict(context):
        return {"state": "missing"}
    workers = {
        identity: get_controller_worker_snapshot(worker)
        for identity, worker in sorted(context["controller_workers"].items())
    }
    with context["engine_lock"]:
        rule_stats = rule_engine.engine_stats(context["rule_engine"])
        fsm_machines = context["fsm"].get("machines") or {}
        fsm = {
            "ok": context["fsm"].get("ok"),
            "error": context["fsm"].get("error"),
            "scope": copy.deepcopy(context["fsm"].get("scope") or {}),
            "machine_count": len(fsm_machines),
            "states": {
                name: machine.get("state")
                for name, machine in sorted(fsm_machines.items())
            },
        }
    with context["fsm_dispatch_lock"]:
        fsm["active_instances"] = tuple(sorted(context["fsm_active"]))
        fsm["pending_events"] = sum(
            len(events) for events in context["fsm_pending"].values())
    with context["pipeline_lock"]:
        pipeline = {
            "active": context["pipeline_active"],
            "current": copy.deepcopy(context["pipeline_current"]),
            "pending_count": len(context["pipeline_pending"]),
            "sequence": context["pipeline_sequence"],
        }
    return {
        "module": context["module"],
        "version": context["version"],
        "state": context["state"],
        "thread_alive": _thread_alive(context.get("thread")),
        "result_thread_alive": _thread_alive(context.get("result_thread")),
        "controller_ingress_alive": _thread_alive(
            context.get("controller_ingress_thread")),
        "config_revision": context["config_revision"],
        "config_fingerprint": context["config_fingerprint"],
        "execution_scope": {
            "controller_count": len(
                context["execution_config_snapshot"].get("controllers") or []),
            "rule_count": len(
                context["execution_config_snapshot"].get("rules") or []),
            "state_count": len(
                context["execution_config_snapshot"].get("process_states") or []),
        },
        "pool": context["pool"]["get_status_snapshot"](),
        "rules": {"binding": copy.deepcopy(context["rule_binding"]),
                  "statistics": rule_stats},
        "fsm": fsm,
        "sensor_pipeline": pipeline,
        "safety": get_safety_snapshot(context["safety"]),
        "controller_workers": workers,
        "statistics": dict(context["statistics"]),
        "last_error": copy.deepcopy(context["last_error"]),
    }


# Compatibility helpers used by earlier runtime bootstrap revisions.
run_thread_management = _management_loop


def set_cross_dispatchers(context, event_dispatcher=None, result_dispatcher=None):
    if not _is_dict(context):
        return False
    if event_dispatcher is not None and not callable(event_dispatcher):
        raise TypeError("cross event dispatcher must be callable")
    if result_dispatcher is not None and not callable(result_dispatcher):
        raise TypeError("cross result dispatcher must be callable")
    context["resources"]["cross_event_dispatcher"] = event_dispatcher
    context["resources"]["cross_result_dispatcher"] = result_dispatcher
    return True


def create_thread(name, target_func, **kwargs):
    thread = threading.Thread(
        target=partial(target_func, **kwargs), name=str(name), daemon=False)
    thread.start()
    return thread
