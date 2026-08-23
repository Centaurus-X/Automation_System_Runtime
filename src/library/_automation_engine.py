# -*- coding: utf-8 -*-

# src/library/_automation_engine.py


"""
 =============================================================================
 _AUTOMATION_ENGINE
 =============================================================================

 Pure functional business logic for the automation system:
 - PID / P / ONOFF controller algorithms
 - Sensor filter chains (median, lowpass, highpass, scaler, expr, sampler)
 - Scaling profiles (raw <-> engineering units)
 - Rule engine (conditions, actions, one-shot logic)
 - Control method evaluation (ramp, square, sine, impulse, expr)
 - Controller management & factory
 - State management (active checks, value_id resolution)
 - Setpoint extraction & override handling
 - Fail-safe logic
 - Actuator task emission
 - Event emission helper

 IMPORTANT: This module is designed for Python 3.14+ with GIL=disabled!
            All config access operates on thread-local snapshots via ctx.

 Standards:
 - No OOP / No classes / No decorators
 - functools.partial instead of lambda
 - PEP 8 compliant
 - Full type hinting
 - Every code block wrapped in try/except
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import json
import time
import uuid
import math
import logging
import ast
import threading

from functools import partial
from datetime import datetime, timezone
from collections import deque


# -------------------------------------------------------
# Event Interface (sync only — no asyncio)
# -------------------------------------------------------
try:
    from src.library._event_interface import (
        make_event,
    )
except Exception:
    try:
        from _event_interface import (
            make_event,
        )
    except Exception:
        make_event = None

# -------------------------------------------------------
# Logger
# -------------------------------------------------------
logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Active value_ids for process_states (from process_values)
DEFAULT_ACTIVE_VALUE_IDS = {1}  # value_id=1 -> "activated"

# Default PID parameters
DEFAULT_PID_KP = 1.0
DEFAULT_PID_KI = 0.1
DEFAULT_PID_KD = 0.01
DEFAULT_SAMPLE_TIME = 0.1


# =============================================================================
# MATH / UTILITY
# =============================================================================

def _clamp(v, vmin, vmax):
    """Clamp value between min and max."""
    try:
        fv = float(v)
    except Exception:
        return float(vmin)
    if fv < float(vmin):
        return float(vmin)
    if fv > float(vmax):
        return float(vmax)
    return fv


def _map_linear(x, in_min, in_max, out_min, out_max):
    """Linear mapping from one range to another."""
    in_min_f = float(in_min)
    in_max_f = float(in_max)
    out_min_f = float(out_min)
    out_max_f = float(out_max)

    if in_max_f == in_min_f:
        return out_min_f

    xf = float(x)
    t = (xf - in_min_f) / (in_max_f - in_min_f)
    return out_min_f + t * (out_max_f - out_min_f)


def _as_int_or_none(val):
    """
    Robust int conversion:
    - accepts int/bool/float/str/numpy-ints (best effort)
    - returns None if conversion is not sensible
    """
    if val is None:
        return None

    # bool is subclass of int -> explicitly allow
    if isinstance(val, bool):
        return int(val)

    if isinstance(val, int):
        return int(val)

    if isinstance(val, float):
        try:
            if math.isfinite(val):
                return int(val)
        except Exception:
            return None
        return None

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None

        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            try:
                return int(s)
            except Exception:
                return None

        # Float-Strings like "1.0"
        try:
            f = float(s)
            if math.isfinite(f):
                return int(f)
        except Exception:
            return None
        return None

    # Fallback: generic int(...) attempt
    try:
        return int(val)
    except Exception:
        return None


def _normalize_int_list(values):
    """Robust int list (unique, order-preserving)."""
    if not isinstance(values, (list, tuple, set)):
        return []
    tmp = []
    for v in values:
        try:
            tmp.append(int(v))
        except Exception:
            continue
    seen = set()
    out = []
    for v in tmp:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


# =============================================================================
# EVENT EMISSION HELPER
# =============================================================================

def _queue_put_safe(q, item):
    """Non-blocking queue put with fallback."""
    if q is None:
        return
    try:
        q.put_nowait(item)
    except Exception:
        try:
            q.put(item, timeout=0.2)
        except Exception:
            pass


def emit_event(ctx, event_type, payload, **kwargs):
    """Emit an event via the queue (sync, thread-safe)."""
    try:
        evt = make_event(event_type, payload, **kwargs)
        q = ctx.get("queue_event_send")
        _queue_put_safe(q, evt)
    except Exception as e:
        logger.error("emit_event failed: %s", e)


# =============================================================================
# PATCH QUEUE HELPERS (shared state write-via-patch)
# =============================================================================

def _patch_key_local(patch):
    """Compute dedup key for a state patch."""
    if not isinstance(patch, dict):
        return ("invalid", 0)

    op = str(patch.get("op") or "merge_state").strip().lower()
    if op == "one_shot_reset":
        rid = patch.get("rule_id")
        try:
            rid_i = int(rid) if rid is not None else 0
        except Exception:
            rid_i = 0
        return (op, rid_i)

    if op == "set_runtime_map":
        return (op, 0)

    sid = patch.get("state_id")
    try:
        sid_i = int(sid) if sid is not None else 0
    except Exception:
        sid_i = 0
    return (op, sid_i)


def queue_automation_state_patch(ctx, patch):
    """Queue a state patch for batched flush."""
    if not isinstance(ctx, dict) or not isinstance(patch, dict):
        return False
    try:
        pending = ctx.setdefault("_automation_state_patch_pending", {})
        key = _patch_key_local(patch)
        pending[key] = patch
        return True
    except Exception:
        return False


def _patch_throttle_allow(ctx, key, now_ts, min_interval_s):
    """Throttle check for patch emissions."""
    if not isinstance(ctx, dict):
        return True
    try:
        now = float(now_ts)
    except Exception:
        now = time.time()
    try:
        min_dt = float(min_interval_s)
    except Exception:
        min_dt = 0.0

    th = ctx.setdefault("_patch_throttle", {})
    last = th.get(key)
    if last is not None:
        try:
            if now - float(last) < min_dt:
                return False
        except Exception:
            pass
    th[key] = now
    return True


# =============================================================================
# CONFIG SNAPSHOT ACCESS (thread-local, lock-free read)
# =============================================================================

def cfg_snapshot(ctx):
    """
    Return the thread-local config snapshot.
    Falls back to creating one if missing.
    """
    try:
        snapshot = ctx.get("config_snapshot")
        if snapshot is not None:
            return snapshot
    except Exception:
        pass

    logger.warning(
        "Automation %s: cfg_snapshot called without snapshot",
        ctx.get("thread_index", "?")
    )
    return ctx.get("config_snapshot") or {}


def get_automation_settings(ctx):
    """Return automation general_settings (from cache)."""
    try:
        return ctx.get("_cache_automation_settings") or {}
    except Exception:
        return {}


def get_automation_rules(ctx):
    """Return automation_rules (from cache)."""
    try:
        return ctx.get("_cache_automation_rules") or []
    except Exception:
        return []


def get_sensor_filter_configs(ctx):
    """Return sensor_filters (from cache)."""
    try:
        return ctx.get("_cache_sensor_filters") or []
    except Exception:
        return []


def get_active_value_ids(ctx):
    """Return active value_ids (from cache)."""
    try:
        return ctx.get("_cache_active_value_ids") or DEFAULT_ACTIVE_VALUE_IDS
    except Exception:
        return DEFAULT_ACTIVE_VALUE_IDS


def find_sensor_by_id(ctx, sensor_id):
    """Find a sensor record (from cache)."""
    try:
        cache = ctx.get("_cache_sensors") or {}
        return cache.get(int(sensor_id))
    except Exception:
        return None


def find_actuator_by_id(ctx, actuator_id):
    """Find an actuator record (from cache)."""
    try:
        cache = ctx.get("_cache_actuators") or {}
        return cache.get(int(actuator_id))
    except Exception:
        return None


def find_process_state_by_id(ctx, state_id):
    """Find a process_state record (from cache)."""
    try:
        cache = ctx.get("_cache_process_states") or {}
        return cache.get(int(state_id))
    except Exception:
        return None


def find_states_for_sensor(ctx, sensor_id):
    """Find all state_ids using a given sensor (from cache)."""
    try:
        cache = ctx.get("_cache_sensor_to_states") or {}
        return cache.get(int(sensor_id)) or []
    except Exception:
        return []


def find_pid_config(ctx, pid_config_id):
    """Find a PID configuration (from cache)."""
    try:
        cache = ctx.get("_cache_pid_configs") or {}
        return cache.get(int(pid_config_id)) or {}
    except Exception:
        return {}


def get_value_label(ctx, value_id):
    """Return the label for a value_id."""
    try:
        if value_id is None:
            return None
        cfg = cfg_snapshot(ctx)
        process_values = cfg.get("process_values") or []
        for pv in process_values:
            if int(pv.get("value_id", -1)) == int(value_id):
                return pv.get("state_value")
    except Exception:
        pass
    return None


def find_control_method(ctx, method_id):
    """Find a control_method configuration."""
    try:
        cfg = cfg_snapshot(ctx)
        methods = cfg.get("control_methods") or []
        for m in methods:
            if int(m.get("method_id", -1)) == int(method_id):
                return m
    except Exception:
        pass
    return None


def find_control_method_template(ctx, template_id):
    """Find a control_method_template."""
    try:
        cfg = cfg_snapshot(ctx)
        templates = cfg.get("control_method_templates") or []
        for tmpl in templates:
            if int(tmpl.get("template_id", -1)) == int(template_id):
                return tmpl
    except Exception:
        pass
    return None


# =============================================================================
# SHARED STATE READ FUNCTIONS (lock-protected reads)
# =============================================================================

def _shared_store_get(ctx):
    """Get shared automation states store and lock reference."""
    try:
        store = ctx.get("shared_automation_states")
        lock = ctx.get("shared_automation_states_lock")
        if isinstance(store, dict) and lock is not None:
            return store, lock
    except Exception:
        pass
    return None, None


def shared_state_get(ctx, state_id):
    """Read a state record from shared store (deep copy)."""
    store, lock = _shared_store_get(ctx)
    if store is None or lock is None:
        return None
    try:
        sid = int(state_id)
    except Exception:
        return None
    try:
        with lock:
            rec = (store.get("states") or {}).get(sid)
            if not isinstance(rec, dict):
                return None
            return copy.deepcopy(rec)
    except Exception:
        return None


def shared_get_runtime_value_id(ctx, state_id):
    """Read runtime value_id from shared store."""
    store, lock = _shared_store_get(ctx)
    if store is None or lock is None:
        return None
    try:
        sid = int(state_id)
    except Exception:
        return None
    try:
        with lock:
            rsv = store.get("runtime_state_values") or {}
            if not isinstance(rsv, dict):
                return None
            return rsv.get(sid)
    except Exception:
        return None


def shared_setpoint_override_get(ctx, state_id, now_ts):
    """Read setpoint override from shared store (with expiry check)."""
    store, lock = _shared_store_get(ctx)
    if store is None or lock is None:
        return None
    try:
        sid = int(state_id)
    except Exception:
        return None

    try:
        with lock:
            ovs = store.get("setpoint_overrides") or {}
            if not isinstance(ovs, dict):
                return None
            ov = ovs.get(sid)
            if not isinstance(ov, dict):
                return None
            ov_copy = copy.deepcopy(ov)
    except Exception:
        return None

    until = ov_copy.get("until")
    if until is None:
        return ov_copy

    try:
        if float(now_ts) <= float(until):
            return ov_copy
    except Exception:
        return ov_copy

    # expired -> request TM removal (throttled)
    try:
        if _patch_throttle_allow(ctx, ("expired_override", int(sid)), now_ts, 1.0):
            queue_automation_state_patch(ctx, {"op": "set_setpoint_override", "state_id": int(sid), "override": None})
    except Exception:
        pass

    return None


def shared_method_state_get(ctx, state_id):
    """Read method state from shared store."""
    store, lock = _shared_store_get(ctx)
    if store is None or lock is None:
        return None
    try:
        sid = int(state_id)
    except Exception:
        return None
    try:
        with lock:
            ms = (store.get("method_states") or {}).get(sid)
            if not isinstance(ms, dict):
                return None
            return copy.deepcopy(ms)
    except Exception:
        return None


# =============================================================================
# SHARED STATE WRITE FUNCTIONS (via patch queue)
# =============================================================================

def shared_state_update(ctx, state_id, update):
    """Queue a state merge update for shared store."""
    try:
        sid = int(state_id)
    except Exception:
        return False
    if not isinstance(update, dict):
        return False
    try:
        patch = {"op": "merge_state", "state_id": int(sid), "update": copy.deepcopy(update)}
        return bool(queue_automation_state_patch(ctx, patch))
    except Exception:
        return False


def shared_state_clear(ctx, state_id):
    """Queue a state clear for shared store."""
    try:
        sid = int(state_id)
    except Exception:
        return False
    try:
        patch = {"op": "clear_state_all", "state_id": int(sid)}
        return bool(queue_automation_state_patch(ctx, patch))
    except Exception:
        return False


def shared_runtime_state_values_set(ctx, state_map):
    """Queue a runtime state values map update."""
    try:
        snap = {}
        if isinstance(state_map, dict):
            for k, v in state_map.items():
                try:
                    sid = int(k)
                except Exception:
                    continue
                if v is None:
                    snap[sid] = None
                else:
                    try:
                        snap[sid] = int(v)
                    except Exception:
                        snap[sid] = None

        patch = {"op": "set_runtime_map", "map": snap}
        return bool(queue_automation_state_patch(ctx, patch))
    except Exception:
        return False


def shared_setpoint_override_set(ctx, state_id, override):
    """Queue a setpoint override update."""
    try:
        sid = int(state_id)
    except Exception:
        return False

    ov = None
    if override is not None:
        if not isinstance(override, dict):
            return False
        ov = copy.deepcopy(override)

    try:
        patch = {"op": "set_setpoint_override", "state_id": int(sid), "override": ov}
        return bool(queue_automation_state_patch(ctx, patch))
    except Exception:
        return False


def shared_method_state_set(ctx, state_id, method_state):
    """Queue a method state update."""
    try:
        sid = int(state_id)
    except Exception:
        return False

    ms = None
    if method_state is not None:
        if not isinstance(method_state, dict):
            return False
        ms = copy.deepcopy(method_state)

    try:
        patch = {"op": "set_method_state", "state_id": int(sid), "method_state": ms}
        return bool(queue_automation_state_patch(ctx, patch))
    except Exception:
        return False


def shared_clear_state_memory(ctx, state_id):
    """Queue a state memory clear."""
    try:
        sid = int(state_id)
    except Exception:
        return False
    try:
        patch = {"op": "clear_state_memory", "state_id": int(sid)}
        return bool(queue_automation_state_patch(ctx, patch))
    except Exception:
        return False


# =============================================================================
# ONE-SHOT SHARED STATE
# =============================================================================

def _one_shot_rule_id(rule):
    """Extract rule_id from a rule dict."""
    try:
        return int(rule.get("rule_id"))
    except Exception:
        return None


def _one_shot_scope(rule):
    """Extract one_shot_scope from rule flags."""
    try:
        flags = rule.get("flags") or {}
        return str(flags.get("one_shot_scope") or "global").strip().lower()
    except Exception:
        return "global"


def shared_one_shot_is_fired(ctx, rule):
    """
    READ-ONLY (strict single-writer):
    Check if a one-shot rule has already been fired.
    """
    try:
        flags = rule.get("flags") or {}
        if not bool(flags.get("one_shot", False)):
            return False

        rule_id = _one_shot_rule_id(rule)
        if rule_id is None:
            return False

        scope = _one_shot_scope(rule)

        store, lock = _shared_store_get(ctx)
        if store is None or lock is None:
            return False

        with lock:
            if scope != "per_process":
                fired = store.get("one_shot_fired")
                if not isinstance(fired, set):
                    return False
                return int(rule_id) in fired

            p_ids = _rule_active_process_ids(ctx, rule)
            if not p_ids:
                fired = store.get("one_shot_fired")
                if not isinstance(fired, set):
                    return False
                return int(rule_id) in fired

            fired_pp = store.get("one_shot_fired_per_process")
            if not isinstance(fired_pp, set):
                return False

            for p_id in p_ids:
                try:
                    pid_int = int(p_id)
                except Exception:
                    return False
                if (int(rule_id), pid_int) not in fired_pp:
                    return False
            return True
    except Exception:
        return False


def shared_one_shot_reset(ctx, rule):
    """Queue a one-shot reset via patch."""
    try:
        flags = rule.get("flags") or {}
        if not bool(flags.get("one_shot", False)):
            return False

        rid = _one_shot_rule_id(rule)
        if rid is None:
            return False

        scope = _one_shot_scope(rule)
        patch = {"op": "one_shot_reset", "rule_id": int(rid), "scope": str(scope or "")}
        return bool(queue_automation_state_patch(ctx, patch))
    except Exception:
        return False


def shared_one_shot_claim(ctx, rule):
    """Claim a one-shot (sync, local tracking).

    In TM v5.0 the automation runs as a stateless job. One-shot tracking
    is handled locally per job context. For cross-thread one-shot coordination,
    TM would need to persist the one-shot state externally.
    """
    try:
        flags = rule.get("flags") or {}
        if not bool(flags.get("one_shot", False)):
            return True

        rid = _one_shot_rule_id(rule)
        if rid is None:
            return False

        # Local one-shot tracking (thread-local within job)
        fired = ctx.setdefault("_one_shot_fired_local", set())
        scope = _one_shot_scope(rule)

        if str(scope) == "global":
            key = str(rid)
        elif str(scope) == "per_process":
            pids = _rule_active_process_ids(ctx, rule) or []
            key = "{}:{}".format(rid, ",".join(str(p) for p in pids))
        else:
            key = str(rid)

        if key in fired:
            return False

        fired.add(key)
        return True
    except Exception:
        return False


# =============================================================================
# LEGACY ONE-SHOT (thread-local, kept for reference)
# =============================================================================

def _legacy_check_one_shot_fired_local(ctx, rule):
    """LEGACY: thread-local one-shot check."""
    if not isinstance(ctx, dict) or not isinstance(rule, dict):
        return False

    flags = rule.get("flags") or {}
    if not flags.get("one_shot", False):
        return False

    try:
        rule_id = int(rule.get("rule_id"))
    except Exception:
        return False

    scope = str(flags.get("one_shot_scope") or "global").strip().lower()

    if scope != "per_process":
        fired = ctx.get("one_shot_fired")
        if not isinstance(fired, set):
            return False
        return rule_id in fired

    p_ids = _rule_active_process_ids(ctx, rule)
    if not p_ids:
        fired = ctx.get("one_shot_fired")
        if not isinstance(fired, set):
            return False
        return rule_id in fired

    fired_pp = ctx.get("one_shot_fired_per_process")
    if not isinstance(fired_pp, set):
        return False

    for p_id in p_ids:
        if (rule_id, int(p_id)) not in fired_pp:
            return False

    return True


def _legacy_mark_one_shot_fired_local(ctx, rule):
    """LEGACY: thread-local one-shot mark."""
    if not isinstance(ctx, dict) or not isinstance(rule, dict):
        return

    flags = rule.get("flags") or {}
    if not flags.get("one_shot", False):
        return

    try:
        rule_id = int(rule.get("rule_id"))
    except Exception:
        return

    scope = str(flags.get("one_shot_scope") or "global").strip().lower()

    if scope != "per_process":
        fired = ctx.setdefault("one_shot_fired", set())
        if not isinstance(fired, set):
            fired = set()
            ctx["one_shot_fired"] = fired
        fired.add(rule_id)
        return

    p_ids = _rule_active_process_ids(ctx, rule)
    if not p_ids:
        fired = ctx.setdefault("one_shot_fired", set())
        if not isinstance(fired, set):
            fired = set()
            ctx["one_shot_fired"] = fired
        fired.add(rule_id)
        return

    fired_pp = ctx.setdefault("one_shot_fired_per_process", set())
    if not isinstance(fired_pp, set):
        fired_pp = set()
        ctx["one_shot_fired_per_process"] = fired_pp

    for p_id in p_ids:
        try:
            pid_int = int(p_id)
        except Exception:
            continue
        fired_pp.add((rule_id, pid_int))


def _legacy_reset_one_shot_local(ctx, rule):
    """LEGACY: thread-local one-shot reset."""
    if not isinstance(ctx, dict) or not isinstance(rule, dict):
        return

    flags = rule.get("flags") or {}
    scope = str(flags.get("one_shot_scope") or "global").strip().lower()

    try:
        rule_id = int(rule.get("rule_id"))
    except Exception:
        return

    if scope != "per_process":
        fired = ctx.get("one_shot_fired")
        if isinstance(fired, set):
            fired.discard(rule_id)
        return

    fired_pp = ctx.get("one_shot_fired_per_process")
    if not isinstance(fired_pp, set):
        return

    p_ids = _rule_scope_process_ids(ctx, rule)

    if p_ids:
        for p_id in p_ids:
            try:
                pid_int = int(p_id)
            except Exception:
                continue
            fired_pp.discard((rule_id, pid_int))
        return

    to_remove = []
    for item in fired_pp:
        try:
            rid, _pid = item
        except Exception:
            continue
        if int(rid) == int(rule_id):
            to_remove.append(item)

    for item in to_remove:
        fired_pp.discard(item)


# One-shot wrappers (final shared-based)

def check_one_shot_fired(ctx, rule):
    """FINAL: shared-based one-shot check."""
    if not isinstance(rule, dict):
        return False
    try:
        flags = rule.get("flags") or {}
        if not bool(flags.get("one_shot", False)):
            return False
        return bool(shared_one_shot_is_fired(ctx, rule))
    except Exception:
        return False


def mark_one_shot_fired(ctx, rule):
    """FINAL: shared-based one-shot mark."""
    if not isinstance(rule, dict):
        return
    try:
        flags = rule.get("flags") or {}
        if not bool(flags.get("one_shot", False)):
            return
        shared_one_shot_claim(ctx, rule)
    except Exception:
        pass


def reset_one_shot(ctx, rule):
    """FINAL: shared-based one-shot reset."""
    if not isinstance(rule, dict):
        return
    try:
        flags = rule.get("flags") or {}
        if not bool(flags.get("one_shot", False)):
            return
        shared_one_shot_reset(ctx, rule)
    except Exception:
        pass


def check_one_shot_reset(ctx, rule, recent_sensor_values, read_sensor_fn):
    """Check if a one-shot rule should be reset."""
    try:
        flags = rule.get("flags") or {}
        reset_mode = flags.get("reset_mode", "never")

        if reset_mode == "never":
            return False

        if reset_mode == "on_conditions_true":
            reset_when = flags.get("reset_when") or {}
            when_all = reset_when.get("when_all") or []
            when_any = reset_when.get("when_any") or []
            when_none = reset_when.get("when_none") or []

            all_ok = evaluate_condition_list(ctx, when_all, recent_sensor_values, read_sensor_fn, "all")
            any_ok = evaluate_condition_list(ctx, when_any, recent_sensor_values, read_sensor_fn, "any") if when_any else True
            none_ok = evaluate_condition_list(ctx, when_none, recent_sensor_values, read_sensor_fn, "none")

            return all_ok and any_ok and none_ok
    except Exception:
        pass
    return False


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

def get_state_value_id(ctx, state):
    """Resolve the current value_id for a state."""
    try:
        sid = int(state.get("state_id", -1))
    except Exception:
        return None

    # 1) Shared truth (deterministic for RR)
    try:
        shared_vid = shared_get_runtime_value_id(ctx, sid)
        if shared_vid is not None:
            return int(shared_vid)
    except Exception:
        pass

    # 2) Local fallback
    rsv = ctx.get("runtime_state_values") or {}
    current = rsv.get(int(sid))
    if current is not None:
        try:
            return int(current)
        except Exception:
            return None

    vid = state.get("value_id") or state.get("initial_value_id")
    try:
        return int(vid) if vid is not None else None
    except Exception:
        return None


def set_state_value_id(ctx, state_id, value_id, source="runtime"):
    """Set the runtime value_id for a state and propagate via patch."""
    try:
        rsv = ctx.setdefault("runtime_state_values", {})
        src = ctx.setdefault("runtime_state_values_source", {})

        sid = int(state_id)
        vid = int(value_id)

        prev = rsv.get(sid)
        rsv[sid] = vid
        src[sid] = str(source)

        # Propagate via patch to TM
        queue_automation_state_patch(ctx, {
            "op": "set_runtime_value",
            "state_id": int(sid),
            "value_id": int(vid),
            "source": str(source),
        })

        return prev
    except Exception:
        return None


def is_state_active(ctx, state):
    """Check if a state is considered active."""
    try:
        current_vid = get_state_value_id(ctx, state)
        active_ids = get_active_value_ids(ctx)

        if current_vid is None:
            return True  # Active when in doubt

        return int(current_vid) in active_ids
    except Exception:
        return True


def seed_runtime_state_values(ctx):
    """Initialize runtime_state_values from config snapshot."""
    try:
        cfg = cfg_snapshot(ctx)
        states = cfg.get("process_states") or []

        rsv = ctx.setdefault("runtime_state_values", {})
        source = ctx.setdefault("runtime_state_values_source", {})

        if not isinstance(states, list):
            return rsv

        for st in states:
            if not isinstance(st, dict):
                continue

            sid = _as_int_or_none(st.get("state_id"))
            if sid is None:
                continue

            current = _as_int_or_none(st.get("value_id"))
            initial = _as_int_or_none(st.get("initial_value_id"))

            if current is not None:
                rsv[int(sid)] = int(current)
            elif initial is not None:
                rsv[int(sid)] = int(initial)
            else:
                rsv[int(sid)] = 2

            source.setdefault(int(sid), "config")

        return rsv
    except Exception:
        return ctx.get("runtime_state_values") or {}


def _build_state_value_map_from_cfg(cfg):
    """Extract {state_id:int -> value_id:int|None} from cfg['process_states']."""
    out = {}
    if not isinstance(cfg, dict):
        return out

    try:
        states = cfg.get("process_states") or []
        if not isinstance(states, list):
            return out

        for st in states:
            if not isinstance(st, dict):
                continue

            sid = _as_int_or_none(st.get("state_id"))
            if sid is None:
                continue

            vid = _as_int_or_none(st.get("value_id"))
            if vid is None:
                vid = _as_int_or_none(st.get("initial_value_id"))

            out[int(sid)] = int(vid) if vid is not None else None
    except Exception:
        pass

    return out


def _sync_runtime_state_values_from_cfg(ctx, cfg, source_tag):
    """Sync ctx['runtime_state_values'] with the config snapshot."""
    try:
        state_map = _build_state_value_map_from_cfg(cfg)

        rsv = ctx.setdefault("runtime_state_values", {})
        src = ctx.setdefault("runtime_state_values_source", {})

        # Set/Update
        for sid, vid in state_map.items():
            if vid is None:
                rsv.pop(int(sid), None)
                src.pop(int(sid), None)
                continue

            rsv[int(sid)] = int(vid)
            src[int(sid)] = str(source_tag)

        # Remove states that no longer exist
        for sid in list(rsv.keys()):
            if int(sid) not in state_map:
                rsv.pop(int(sid), None)
                src.pop(int(sid), None)

        return state_map
    except Exception:
        return {}


def _reset_runtime_for_state(ctx, state_id):
    """Reset thread-local controller/override artifacts for a state."""
    try:
        sid = int(state_id)
    except Exception:
        return

    try:
        ctrls = ctx.get("controllers")
        if isinstance(ctrls, dict):
            ctrls.pop(sid, None)
    except Exception:
        pass

    try:
        kinds = ctx.get("controller_kinds")
        if isinstance(kinds, dict):
            kinds.pop(sid, None)
    except Exception:
        pass

    try:
        cstates = ctx.get("controller_states")
        if isinstance(cstates, dict):
            cstates.pop(sid, None)
    except Exception:
        pass

    try:
        overrides = ctx.get("setpoint_overrides")
        if isinstance(overrides, dict):
            overrides.pop(sid, None)
    except Exception:
        pass

    try:
        mstates = ctx.get("method_states")
        if isinstance(mstates, dict):
            mstates.pop(sid, None)
    except Exception:
        pass

    try:
        shared_clear_state_memory(ctx, sid)
    except Exception:
        pass


# =============================================================================
# CONTROLLER BINDINGS
# =============================================================================

def resolve_allowed_controller_ids(ctx):
    """
    Determine allowed controller IDs for this node.

    Config v10: reads from local_node[].active_controllers.
    If active_controllers is empty -> all controllers[] are allowed (limiter logic).
    Legacy fallback: reads from controller_node_bindings (backward compatibility).
    """
    try:
        cfg = cfg_snapshot(ctx)
        node_id = ctx.get("node_id")
        allowed = set()

        if not isinstance(cfg, dict) or not node_id:
            return allowed

        # --- Config v10: local_node[].active_controllers ---
        local_nodes = cfg.get("local_node") or []
        if isinstance(local_nodes, list):
            for ln in local_nodes:
                if not isinstance(ln, dict):
                    continue
                ln_id = str(ln.get("node_id") or "")
                if ln_id and ln_id != str(node_id):
                    continue
                ac = ln.get("active_controllers") or []
                if isinstance(ac, list) and ac:
                    for cid in ac:
                        try:
                            allowed.add(int(cid))
                        except Exception:
                            pass
                    return allowed
                # active_controllers empty -> all controllers allowed
                for c in cfg.get("controllers") or []:
                    if isinstance(c, dict):
                        try:
                            allowed.add(int(c.get("controller_id")))
                        except Exception:
                            pass
                return allowed

        # --- Legacy fallback: controller_node_bindings ---
        try:
            registrar_list = cfg.get("node_registrar") or []
            registrar = registrar_list[0] if registrar_list else {}
            policy = str(registrar.get("policy_default") or "primary_only").lower()
        except Exception:
            policy = "primary_only"

        bindings = cfg.get("controller_node_bindings") or []

        for b in bindings:
            try:
                primary = str(b.get("primary") or "")
                fallbacks = [str(x) for x in (b.get("fallbacks") or [])]
                controller_ids = []
                for cid in (b.get("controller_ids") or []):
                    try:
                        controller_ids.append(int(cid))
                    except Exception:
                        pass

                if policy == "primary_or_fallback":
                    if node_id == primary or node_id in fallbacks:
                        allowed.update(controller_ids)
                    continue

                # primary_only or primary_then_fallback
                if node_id == primary:
                    allowed.update(controller_ids)

            except Exception:
                continue

        return allowed
    except Exception:
        return set()


def is_controller_allowed(ctx, controller_id):
    """Check if a controller is allowed for this node."""
    try:
        allowed = ctx.get("allowed_controller_ids")
        if ctx.get("controller_scope_explicit"):
            return int(controller_id) in set(allowed or ())
        if not allowed:
            return True
        return int(controller_id) in allowed
    except Exception:
        return False


# =============================================================================
# SCALING PROFILES
# =============================================================================

def _profile_id_from_record(rec):
    """Extract the profile ID from a sensor/actuator record."""
    if not isinstance(rec, dict):
        return None

    for key in ("profile_id", "scaling_profile_id", "scaling_profile", "scale_profile_id", "scale_id"):
        v = rec.get(key)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            continue
    return None


def _get_scaling_profile(ctx, rec):
    """Return the scaling profile dict for a record or None."""
    try:
        pid = _profile_id_from_record(rec)
        if pid is None:
            return None

        profiles = ctx.get("_cache_scaling_profiles") or {}
        prof = profiles.get(pid)
        if not isinstance(prof, dict):
            return None

        # Minimum requirements
        for k in ("raw_min", "raw_max", "eng_min", "eng_max"):
            if k not in prof:
                return None

        return prof
    except Exception:
        return None


def _record_has_scaling_profile(ctx, rec):
    """Check if a record has an associated scaling profile."""
    return _get_scaling_profile(ctx, rec) is not None


def raw_to_scaled(ctx, rec, raw_value):
    """Map a raw value (e.g. register/ADC) to the scaled engineering range."""
    try:
        prof = _get_scaling_profile(ctx, rec)
        if not prof:
            return float(raw_value)

        raw_min = float(prof.get("raw_min"))
        raw_max = float(prof.get("raw_max"))
        out_min = float(prof.get("eng_min"))
        out_max = float(prof.get("eng_max"))
        rv = float(raw_value)

        lo = min(raw_min, raw_max)
        hi = max(raw_min, raw_max)
        rv = _clamp(rv, lo, hi)

        return float(_map_linear(rv, raw_min, raw_max, out_min, out_max))
    except Exception:
        try:
            return float(raw_value)
        except Exception:
            return 0.0


def scaled_to_raw(ctx, rec, scaled_value):
    """Map a scaled value to the raw register/ADC range."""
    try:
        prof = _get_scaling_profile(ctx, rec)
        if not prof:
            return float(scaled_value)

        raw_min = float(prof.get("raw_min"))
        raw_max = float(prof.get("raw_max"))
        in_min = float(prof.get("eng_min"))
        in_max = float(prof.get("eng_max"))
        sv = float(scaled_value)

        lo = min(in_min, in_max)
        hi = max(in_min, in_max)
        sv = _clamp(sv, lo, hi)

        return float(_map_linear(sv, in_min, in_max, raw_min, raw_max))
    except Exception:
        try:
            return float(scaled_value)
        except Exception:
            return 0.0


# =============================================================================
# SENSOR FILTERS (signal processing) - thread-local
# =============================================================================

def create_filter_state():
    """Create an empty filter state."""
    return {
        "history": deque(maxlen=100),
        "last_value": None,
        "last_ts": None,
        "lowpass_state": None,
        "highpass_state": None,
    }


def apply_median_filter(value, state, window):
    """Sliding median filter."""
    try:
        hist = state["history"]
        hist.append(value)

        if len(hist) < window:
            return value

        recent = list(hist)[-window:]
        sorted_vals = sorted(recent)
        mid = len(sorted_vals) // 2

        if len(sorted_vals) % 2 == 0:
            return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0
        return sorted_vals[mid]
    except Exception:
        return value


def apply_moving_average_filter(value, state, window):
    """Sliding moving average filter."""
    try:
        hist = state["history"]
        hist.append(value)

        if len(hist) < window:
            return value

        recent = list(hist)[-window:]
        return sum(recent) / len(recent)
    except Exception:
        return value


def apply_lowpass_filter(value, state, alpha):
    """Exponential low-pass filter."""
    try:
        prev = state.get("lowpass_state")
        if prev is None:
            state["lowpass_state"] = value
            return value

        filtered = alpha * value + (1.0 - alpha) * prev
        state["lowpass_state"] = filtered
        return filtered
    except Exception:
        return value


def apply_highpass_filter(value, state, alpha):
    """High-pass filter."""
    try:
        prev_value = state.get("last_value")
        prev_filtered = state.get("highpass_state")

        if prev_value is None or prev_filtered is None:
            state["last_value"] = value
            state["highpass_state"] = 0.0
            return 0.0

        filtered = alpha * (prev_filtered + value - prev_value)
        state["last_value"] = value
        state["highpass_state"] = filtered
        return filtered
    except Exception:
        return 0.0


def apply_scaler_filter(value, in_min, in_max, out_min, out_max):
    """Linear scaling filter."""
    try:
        if in_max == in_min:
            return out_min

        normalized = (value - in_min) / (in_max - in_min)
        return out_min + normalized * (out_max - out_min)
    except Exception:
        return value


def apply_sampler_filter(value, state, period_s, now_ts):
    """Time-based sampling filter."""
    try:
        last_ts = state.get("last_ts")

        if last_ts is None or (now_ts - last_ts) >= period_s:
            state["last_ts"] = now_ts
            state["last_value"] = value
            return value

        return state.get("last_value", value)
    except Exception:
        return value


_EXPR_MAX_AST_NODES = 96
_EXPR_MAX_AST_DEPTH = 24
_EXPR_MAX_ABS_NUMBER = 1000000000.0
_EXPR_MAX_POWER_EXPONENT = 12.0
_EXPR_MAX_CALL_ARGS = 16


def _expr_ast_depth(node):
    child_depth = 0
    for child in ast.iter_child_nodes(node):
        current_depth = _expr_ast_depth(child)
        if current_depth > child_depth:
            child_depth = current_depth
    return child_depth + 1


def _expr_ast_node_count(tree):
    count = 0
    for _node in ast.walk(tree):
        count += 1
    return count


def _expr_is_finite_number(value):
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except Exception:
        return False


def _expr_guard_number(value):
    if isinstance(value, bool):
        return value
    if not _expr_is_finite_number(value):
        raise ValueError("Expr-Filter erlaubt nur endliche numerische Werte")
    numeric_value = float(value)
    if abs(numeric_value) > _EXPR_MAX_ABS_NUMBER:
        raise ValueError("Expr-Filter Zahlenlimit überschritten")
    return value


def _expr_is_safe_ast(tree):
    """Restrictive AST check for safe expression evaluation."""
    allowed_nodes = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Constant,
        ast.Num,
        ast.Name,
        ast.Load,
        ast.Call,
        ast.IfExp,
        ast.Compare,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.Not,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Pow,
        ast.Mod,
        ast.USub,
        ast.UAdd,
        ast.Eq,
        ast.NotEq,
        ast.Lt,
        ast.LtE,
        ast.Gt,
        ast.GtE,
    )

    try:
        if _expr_ast_node_count(tree) > _EXPR_MAX_AST_NODES:
            return False
        if _expr_ast_depth(tree) > _EXPR_MAX_AST_DEPTH:
            return False

        for node in ast.walk(tree):
            if not isinstance(node, allowed_nodes):
                return False
            if isinstance(node, (ast.Attribute, ast.Subscript, ast.Lambda, ast.List, ast.Dict, ast.Set, ast.Tuple)):
                return False
            if isinstance(node, ast.Call):
                if not isinstance(node.func, ast.Name):
                    return False
                if node.keywords:
                    return False
                if len(node.args) > _EXPR_MAX_CALL_ARGS:
                    return False
            if isinstance(node, ast.Name):
                if str(node.id).startswith("__"):
                    return False
            if isinstance(node, ast.Constant):
                try:
                    _expr_guard_number(node.value)
                except Exception:
                    return False
        return True
    except Exception:
        return False


def _expr_parse_cached(expr_str, state):
    """Parse an expression once and cache the validated AST in state."""
    if not expr_str:
        return None

    try:
        cache = state.get("_expr_cache")
        if isinstance(cache, dict) and cache.get("expr") == expr_str and cache.get("tree") is not None:
            return cache.get("tree")

        tree = ast.parse(expr_str, mode="eval")

        if not _expr_is_safe_ast(tree):
            state["_expr_cache"] = {"expr": expr_str, "tree": None}
            return None

        state["_expr_cache"] = {"expr": expr_str, "tree": tree}
        return tree
    except Exception:
        state["_expr_cache"] = {"expr": expr_str, "tree": None}
        return None


def _expr_apply_binop(left, op, right):
    if isinstance(op, ast.Add):
        return _expr_guard_number(left + right)
    if isinstance(op, ast.Sub):
        return _expr_guard_number(left - right)
    if isinstance(op, ast.Mult):
        return _expr_guard_number(left * right)
    if isinstance(op, ast.Div):
        if right == 0:
            raise ValueError("Division durch null im Expr-Filter")
        return _expr_guard_number(left / right)
    if isinstance(op, ast.Mod):
        if right == 0:
            raise ValueError("Modulo durch null im Expr-Filter")
        return _expr_guard_number(left % right)
    if isinstance(op, ast.Pow):
        if abs(float(right)) > _EXPR_MAX_POWER_EXPONENT:
            raise ValueError("Potenz-Exponent überschreitet Expr-Filter Limit")
        return _expr_guard_number(left ** right)
    raise ValueError("Nicht unterstützter Expr-Operator")


def _expr_apply_compare(left, op, right):
    if isinstance(op, ast.Eq):
        return left == right
    if isinstance(op, ast.NotEq):
        return left != right
    if isinstance(op, ast.Lt):
        return left < right
    if isinstance(op, ast.LtE):
        return left <= right
    if isinstance(op, ast.Gt):
        return left > right
    if isinstance(op, ast.GtE):
        return left >= right
    raise ValueError("Nicht unterstützter Expr-Vergleich")


def _expr_eval_node(node, env):
    if isinstance(node, ast.Expression):
        return _expr_eval_node(node.body, env)

    if isinstance(node, ast.Constant):
        return _expr_guard_number(node.value)

    if isinstance(node, ast.Num):
        return _expr_guard_number(node.n)

    if isinstance(node, ast.Name):
        if node.id not in env:
            raise ValueError("Unbekannter Expr-Name: " + str(node.id))
        return env[node.id]

    if isinstance(node, ast.BinOp):
        left = _expr_eval_node(node.left, env)
        right = _expr_eval_node(node.right, env)
        return _expr_apply_binop(left, node.op, right)

    if isinstance(node, ast.UnaryOp):
        operand = _expr_eval_node(node.operand, env)
        if isinstance(node.op, ast.UAdd):
            return _expr_guard_number(+operand)
        if isinstance(node.op, ast.USub):
            return _expr_guard_number(-operand)
        if isinstance(node.op, ast.Not):
            return not bool(operand)
        raise ValueError("Nicht unterstützter Expr-Unary-Operator")

    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            result = True
            for value_node in node.values:
                result = _expr_eval_node(value_node, env)
                if not result:
                    return result
            return result
        if isinstance(node.op, ast.Or):
            result = False
            for value_node in node.values:
                result = _expr_eval_node(value_node, env)
                if result:
                    return result
            return result
        raise ValueError("Nicht unterstützter Expr-Bool-Operator")

    if isinstance(node, ast.Compare):
        left = _expr_eval_node(node.left, env)
        for index, op in enumerate(node.ops):
            right = _expr_eval_node(node.comparators[index], env)
            if not _expr_apply_compare(left, op, right):
                return False
            left = right
        return True

    if isinstance(node, ast.IfExp):
        test_result = _expr_eval_node(node.test, env)
        if test_result:
            return _expr_eval_node(node.body, env)
        return _expr_eval_node(node.orelse, env)

    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ValueError("Nur benannte Expr-Funktionen sind erlaubt")
        fn = env.get(node.func.id)
        if not callable(fn):
            raise ValueError("Expr-Funktion nicht erlaubt: " + str(node.func.id))
        if node.keywords:
            raise ValueError("Expr-Funktionen erlauben keine Keyword-Argumente")
        args = []
        for arg_node in node.args:
            args.append(_expr_eval_node(arg_node, env))
        return _expr_guard_number(fn(*args))

    raise ValueError("Nicht unterstützter Expr-AST-Knoten")


def _expr_env(x, params):
    """Build allowed names/functions for expr evaluation."""
    env = {
        "x": float(x),
        "pi": math.pi,
        "e": math.e,

        # Math functions
        "sin": math.sin,
        "cos": math.cos,
        "tan": math.tan,
        "asin": math.asin,
        "acos": math.acos,
        "atan": math.atan,
        "sqrt": math.sqrt,
        "log": math.log,
        "log10": math.log10,
        "exp": math.exp,
        "fabs": math.fabs,
        "floor": math.floor,
        "ceil": math.ceil,

        # Helper functions
        "min": min,
        "max": max,
        "clamp": _clamp,
    }

    if isinstance(params, dict):
        for k, v in params.items():
            if not isinstance(k, str):
                continue
            if k.startswith("__"):
                continue
            try:
                env[k] = float(v)
            except Exception:
                continue

    return env


def apply_piecewise_linear(x, points, state=None):
    """Piecewise linear characteristic curve via control points."""
    try:
        xv = float(x)
    except Exception:
        return 0.0

    if not isinstance(points, list) or len(points) < 2:
        return float(xv)

    # Cache sorted points in state if possible
    pts = None
    if isinstance(state, dict):
        cache = state.get("_lut_cache")
        if (
            isinstance(cache, dict)
            and cache.get("raw") == points
            and isinstance(cache.get("sorted"), list)
            and len(cache.get("sorted") or []) >= 2
        ):
            pts = cache.get("sorted")

    if pts is None:
        tmp = []
        for p in points:
            if not isinstance(p, (list, tuple)) or len(p) < 2:
                continue
            try:
                xi = float(p[0])
                yi = float(p[1])
            except Exception:
                continue

            if math.isnan(xi) or math.isinf(xi) or math.isnan(yi) or math.isinf(yi):
                continue

            tmp.append((xi, yi))

        if len(tmp) < 2:
            return float(xv)

        def _key_first(t):
            return t[0]

        tmp.sort(key=_key_first)

        # Deduplicate x values (last wins)
        pts2 = []
        last_x = None
        for xi, yi in tmp:
            if last_x is not None and xi == last_x:
                pts2[-1] = (xi, yi)
            else:
                pts2.append((xi, yi))
                last_x = xi

        if len(pts2) < 2:
            return float(pts2[0][1]) if pts2 else float(xv)

        pts = pts2

        if isinstance(state, dict):
            state["_lut_cache"] = {"raw": points, "sorted": pts}

    # Clamp left/right
    if xv <= pts[0][0]:
        return float(pts[0][1])
    if xv >= pts[-1][0]:
        return float(pts[-1][1])

    # Find segment (linear interpolation)
    for i in range(len(pts) - 1):
        x0, y0 = pts[i]
        x1, y1 = pts[i + 1]

        if x0 <= xv <= x1:
            dx = x1 - x0
            if dx == 0.0:
                return float(y0)
            t = (xv - x0) / dx
            return float(y0 + t * (y1 - y0))

    return float(pts[-1][1])


def apply_expr_filter(value, state, fc):
    """Expression filter with optional LUT and clamping."""
    try:
        x = float(value)

        # Optional input clamp
        if "in_min" in fc or "in_max" in fc:
            x = _clamp(x, float(fc.get("in_min", x)), float(fc.get("in_max", x)))

        # LUT?
        points = fc.get("points")
        if isinstance(points, list):
            y = apply_piecewise_linear(x, points, state)
        else:
            expr_str = fc.get("expression") or fc.get("expr") or ""
            tree = _expr_parse_cached(str(expr_str), state)
            if tree is None:
                return float(x)

            params = fc.get("params") or fc.get("parameters") or {}
            env = _expr_env(x, params)

            try:
                y = _expr_eval_node(tree, env)
            except Exception:
                return float(x)

            try:
                y = float(y)
            except Exception:
                return float(x)

        # Optional output clamp
        if "out_min" in fc or "out_max" in fc:
            y = _clamp(y, float(fc.get("out_min", y)), float(fc.get("out_max", y)))

        return float(y)
    except Exception:
        return float(value)


def apply_filter_chain(value, filter_configs, filter_states, now_ts):
    """
    Apply a filter chain to a value.
    Supported: median, moving_average, lowpass, highpass, sampler, scaler, expr
    """
    try:
        result = float(value)
    except Exception:
        return 0.0

    if not isinstance(filter_configs, list):
        return result
    if not isinstance(filter_states, dict):
        return result

    for idx, fc in enumerate(filter_configs):
        if not isinstance(fc, dict):
            continue

        ftype = str(fc.get("type") or "").strip().lower()
        if not ftype:
            continue

        state_key = "filter_{}".format(idx)
        if state_key not in filter_states:
            filter_states[state_key] = create_filter_state()

        state = filter_states[state_key]

        try:
            if ftype == "median":
                window = int(fc.get("window", 5))
                result = apply_median_filter(result, state, window)

            elif ftype == "moving_average":
                window = int(fc.get("window", 5))
                result = apply_moving_average_filter(result, state, window)

            elif ftype == "lowpass":
                alpha = float(fc.get("alpha", 0.2))
                result = apply_lowpass_filter(result, state, alpha)

            elif ftype == "highpass":
                alpha = float(fc.get("alpha", 0.05))
                result = apply_highpass_filter(result, state, alpha)

            elif ftype in ("scaler", "scaler_sensor_profile", "scaler_profile"):
                try:
                    in_min = fc.get("in_min")
                    in_max = fc.get("in_max")
                    out_min = fc.get("out_min")
                    out_max = fc.get("out_max")
                    if None in (in_min, in_max, out_min, out_max):
                        continue
                    result = apply_scaler_filter(
                        result, float(in_min), float(in_max), float(out_min), float(out_max),
                    )
                except Exception:
                    continue

            elif ftype == "sampler":
                period_s = float(fc.get("period_s", 1.0))
                result = apply_sampler_filter(result, state, period_s, now_ts)

            elif ftype == "expr":
                result = apply_expr_filter(result, state, fc)

            else:
                # Unknown filter: skip (tolerant by design)
                continue
        except Exception:
            continue

    return result


def get_filter_chain_for_sensor(ctx, sensor_id):
    """Find the filter chain for a sensor."""
    try:
        sid_i = int(sensor_id)
    except Exception:
        return []

    try:
        cache_map = ctx.get("_cache_sensor_to_filter_chain") or {}
        if isinstance(cache_map, dict):
            chain = cache_map.get(sid_i)
            if isinstance(chain, list):
                return chain
    except Exception:
        pass

    try:
        filter_configs = get_sensor_filter_configs(ctx)
        if not isinstance(filter_configs, list):
            return []

        for fc in filter_configs:
            if not isinstance(fc, dict):
                continue

            sensors = fc.get("sensors")
            if sensors is None:
                apply_to = fc.get("apply_to") or {}
                sensors = apply_to.get("sensor_ids") or []

            if not isinstance(sensors, list):
                sensors = [sensors]

            try:
                sid_list = [int(s) for s in sensors if s is not None]
            except Exception:
                sid_list = []

            if sid_i in sid_list:
                chain = fc.get("chain") or []
                return chain if isinstance(chain, list) else []
    except Exception:
        pass

    return []


def apply_sensor_filters(ctx, sensor_dict, raw_value):
    """Apply configured sensor filters and return an engineering value.

    Filter chains operate on raw sensor counts first. When a scaling profile is
    configured, profile-based scaler markers are treated as a request to convert
    the post-filtered raw value into engineering units exactly once.
    """
    if not isinstance(ctx, dict) or not isinstance(sensor_dict, dict):
        return float(raw_value)

    try:
        sid = sensor_dict.get("sensor_id")
        if sid is None:
            return float(raw_value)

        sid_int = int(sid)
        chain = get_filter_chain_for_sensor(ctx, sid_int)
        has_profile = _record_has_scaling_profile(ctx, sensor_dict)

        filter_states = ctx.setdefault("sensor_filter_states", {})
        sensor_filter_state = filter_states.setdefault(sid_int, {})

        if not isinstance(chain, list):
            chain = []

        if has_profile:
            filtered_chain = []
            for fc in chain:
                if not isinstance(fc, dict):
                    continue
                ftype = str(fc.get("type") or "").strip().lower()
                if ftype in ("scaler", "scaler_sensor_profile", "scaler_profile"):
                    continue
                filtered_chain.append(fc)
            chain = filtered_chain

        now_ts = time.time()
        filtered_raw = float(apply_filter_chain(raw_value, chain, sensor_filter_state, now_ts))

        if has_profile:
            return float(raw_to_scaled(ctx, sensor_dict, filtered_raw))

        return filtered_raw
    except Exception:
        return float(raw_value)


# =============================================================================
# CONTROLLER CREATION
# =============================================================================

def _normalize_pid_params(pid_cfg):
    """
    Normalize PID parameters from config.
    Supports: {kp:..., ki:..., kd:...} and {pid_parameters:{kp:..., ki:..., kd:...}}
    """
    if not isinstance(pid_cfg, dict):
        return {}

    try:
        # Format 1: Gains at root
        if any((k in pid_cfg) for k in ("kp", "ki", "kd")):
            return pid_cfg

        # Format 2: Gains in sub-structure
        nested = pid_cfg.get("pid_parameters")
        if isinstance(nested, dict):
            return nested
    except Exception:
        pass

    return {}


def create_pid_controller(kp, ki, kd, out_min, out_max, sample_time, anti_windup=True, error_scale=1.0, dt_max_s=None, anti_windup_mode="clamp"):
    """Create a PID controller state dict."""
    dt_max_val = None
    if dt_max_s is not None:
        try:
            dt_max_val = float(dt_max_s)
        except Exception:
            dt_max_val = None

    return {
        "type": "PID",
        "kp": float(kp),
        "ki": float(ki),
        "kd": float(kd),
        "out_min": float(out_min),
        "out_max": float(out_max),
        "sample_time": float(sample_time),
        "anti_windup": anti_windup,
        "anti_windup_mode": str(anti_windup_mode or "clamp"),
        "error_scale": float(error_scale),
        "dt_max_s": dt_max_val,
        "integral": 0.0,
        "last_error": None,
        "last_time": None,
    }


def create_p_controller(kp, out_min, out_max):
    """Create a P controller state dict."""
    return {
        "type": "P",
        "kp": float(kp),
        "out_min": float(out_min),
        "out_max": float(out_max),
    }


def create_onoff_controller(hysteresis=0.0):
    """Create an ONOFF controller state dict."""
    return {
        "type": "ONOFF",
        "hysteresis": float(hysteresis),
        "last_output": None,
    }


# =============================================================================
# CONTROLLER UPDATE ALGORITHMS
# =============================================================================

def pid_update(controller, setpoint, measurement, now_ts):
    """
    Compute PID output. Clamped to [out_min, out_max].
    Includes anti-windup, error scaling, dt clamping, and debug tracing.
    """
    try:
        # Error scaling
        error_scale = float(controller.get("error_scale", 1.0))
        error = (setpoint - measurement) * error_scale

        kp = controller["kp"]
        ki = controller["ki"]
        kd = controller["kd"]
        out_min = controller["out_min"]
        out_max = controller["out_max"]
        sample_time = controller["sample_time"]

        # Time delta (incl. clamp)
        last_time = controller.get("last_time")
        if last_time is None:
            dt_raw = float(sample_time)
        else:
            try:
                dt_raw = float(now_ts - last_time)
            except Exception:
                dt_raw = float(sample_time)

        dt = float(dt_raw)
        dt_sample_clamped = False
        if dt < float(sample_time):
            dt = float(sample_time)
            dt_sample_clamped = True

        dt_max_applied = False
        dt_max_s = controller.get("dt_max_s")
        dt_max_f = None
        if dt_max_s is not None:
            try:
                dt_max_f = float(dt_max_s)
                if dt > dt_max_f:
                    dt = dt_max_f
                    dt_max_applied = True
            except Exception:
                dt_max_f = None

        controller["last_time"] = now_ts

        # Proportional
        p_term = kp * error

        # Integral
        integral_before = controller.get("integral", 0.0)
        try:
            integral_before_f = float(integral_before)
        except Exception:
            integral_before_f = 0.0

        integral_after_add = integral_before_f + (error * dt)
        controller["integral"] = integral_after_add

        # Anti-windup
        integral_clamped = False
        integral_rejected = False
        max_integral = None
        anti_windup_enabled = bool(controller.get("anti_windup", True))
        anti_windup_mode = str(controller.get("anti_windup_mode") or "clamp").strip().lower()

        if anti_windup_enabled and anti_windup_mode in (
                "conditional", "conditional_integration", "freeze", "freeze_on_saturation"):
            candidate_i_term = ki * float(controller.get("integral", 0.0))
            candidate_output_pre = p_term + candidate_i_term

            if candidate_output_pre < out_min and error < 0.0:
                controller["integral"] = integral_before_f
                integral_rejected = True
            elif candidate_output_pre > out_max and error > 0.0:
                controller["integral"] = integral_before_f
                integral_rejected = True

        if anti_windup_enabled:
            denom = ki
            if abs(denom) < 0.0001:
                denom = 0.0001
            max_integral = (out_max - out_min) / abs(denom)

            if controller["integral"] > max_integral:
                controller["integral"] = max_integral
                integral_clamped = True
            elif controller["integral"] < -max_integral:
                controller["integral"] = -max_integral
                integral_clamped = True

        i_term = ki * controller["integral"]

        # Derivative
        last_error = controller.get("last_error")
        if last_error is None:
            d_term = 0.0
        else:
            d_term = kd * (error - last_error) / dt

        controller["last_error"] = error

        # Output (pre-clamping)
        output_pre = p_term + i_term + d_term
        output = output_pre

        clamp_state = None
        if output < out_min:
            output = out_min
            clamp_state = "min"
        elif output > out_max:
            output = out_max
            clamp_state = "max"

        # Debug/Tracing (only when enabled)
        if bool(controller.get("debug_pid", False)):
            state_id = controller.get("state_id")
            thread_index = controller.get("thread_index")

            # Snapshot for post-analysis
            controller["_dbg_pid_last"] = {
                "ts": float(now_ts),
                "state_id": state_id,
                "thread_index": thread_index,
                "setpoint": float(setpoint),
                "measurement": float(measurement),
                "error": float(error),
                "error_scale": float(error_scale),
                "dt_raw": float(dt_raw),
                "dt": float(dt),
                "dt_sample_clamped": bool(dt_sample_clamped),
                "dt_max_s": dt_max_f,
                "dt_max_applied": bool(dt_max_applied),
                "kp": float(kp),
                "ki": float(ki),
                "kd": float(kd),
                "p_term": float(p_term),
                "i_term": float(i_term),
                "d_term": float(d_term),
                "integral_before": float(integral_before_f),
                "integral_after_add": float(integral_after_add),
                "integral": float(controller.get("integral", 0.0)),
                "max_integral": max_integral,
                "integral_clamped": bool(integral_clamped),
                "integral_rejected": bool(integral_rejected),
                "anti_windup_mode": str(anti_windup_mode),
                "output_pre": float(output_pre),
                "output": float(output),
                "clamp_state": clamp_state,
                "out_min": float(out_min),
                "out_max": float(out_max),
            }

            # (A) Periodic update log
            interval_s = controller.get("debug_pid_interval_s", 1.0)
            try:
                interval_f = float(interval_s)
            except Exception:
                interval_f = 1.0
            if interval_f < 0.0:
                interval_f = 0.0

            last_log_ts = controller.get("_dbg_pid_last_log_ts")
            do_periodic = False
            if last_log_ts is None:
                do_periodic = True
            else:
                try:
                    do_periodic = (float(now_ts) - float(last_log_ts)) >= interval_f
                except Exception:
                    do_periodic = True

            if do_periodic:
                controller["_dbg_pid_last_log_ts"] = float(now_ts)
                logger.info(
                    "Automation %s: PID_DBG state=%s sp=%.3f meas=%.3f err=%.3f dt=%.3f(dt_raw=%.3f) P=%.6f I=%.6f D=%.6f int=%.6f out_pre=%.6f out=%.6f clamp=%s dt_max=%s int_clamp=%s int_reject=%s aw_mode=%s",
                    thread_index, state_id,
                    float(setpoint), float(measurement), float(error),
                    float(dt), float(dt_raw),
                    float(p_term), float(i_term), float(d_term),
                    float(controller.get("integral", 0.0)),
                    float(output_pre), float(output),
                    clamp_state, str(dt_max_applied), str(integral_clamped),
                    str(integral_rejected), str(anti_windup_mode),
                )

            # (B) Clamp logs
            if bool(controller.get("debug_pid_log_clamps", True)):
                if dt_max_applied:
                    logger.info(
                        "Automation %s: PID_DT_CLAMP state=%s dt_raw=%.3f -> dt=%.3f (dt_max_s=%.3f)",
                        thread_index, state_id, float(dt_raw), float(dt), float(dt_max_f) if dt_max_f is not None else -1.0
                    )
                if integral_clamped:
                    logger.info(
                        "Automation %s: PID_I_CLAMP state=%s int_before=%.6f int_after=%.6f max_int=%.6f",
                        thread_index, state_id,
                        float(integral_after_add), float(controller.get("integral", 0.0)),
                        float(max_integral) if max_integral is not None else 0.0,
                    )
                if integral_rejected:
                    logger.info(
                        "Automation %s: PID_I_REJECT state=%s mode=%s int_before=%.6f int_candidate=%.6f",
                        thread_index, state_id, str(anti_windup_mode),
                        float(integral_before_f), float(integral_after_add),
                    )

            # (C) Saturation / transitions
            prev_sat = controller.get("_dbg_pid_sat_state")
            prev_sat_since = controller.get("_dbg_pid_sat_since")
            if prev_sat_since is None:
                prev_sat_since = float(now_ts)

            if clamp_state != prev_sat:
                if bool(controller.get("debug_pid_log_transitions", True)):
                    if prev_sat is not None and clamp_state is None:
                        try:
                            dur = float(now_ts) - float(prev_sat_since)
                        except Exception:
                            dur = 0.0
                        logger.info(
                            "Automation %s: PID_TRANSITION state=%s LEAVE_SAT sat=%s duration=%.3fs out_pre=%.6f out=%.6f",
                            thread_index, state_id, prev_sat, dur, float(output_pre), float(output)
                        )
                    if clamp_state is not None:
                        logger.info(
                            "Automation %s: PID_TRANSITION state=%s ENTER_SAT sat=%s out_pre=%.6f out=%.6f",
                            thread_index, state_id, clamp_state, float(output_pre), float(output)
                        )

                controller["_dbg_pid_sat_state"] = clamp_state
                controller["_dbg_pid_sat_since"] = float(now_ts)
                controller["_dbg_pid_sat_last_log_ts"] = float(now_ts)

            if clamp_state is not None and bool(controller.get("debug_pid_log_saturation", True)):
                sat_interval = controller.get("debug_pid_saturation_interval_s", 10.0)
                try:
                    sat_interval_f = float(sat_interval)
                except Exception:
                    sat_interval_f = 10.0
                if sat_interval_f < 0.1:
                    sat_interval_f = 0.1

                sat_last_log = controller.get("_dbg_pid_sat_last_log_ts")
                if sat_last_log is None:
                    sat_last_log = 0.0
                try:
                    need_sat_log = (float(now_ts) - float(sat_last_log)) >= sat_interval_f
                except Exception:
                    need_sat_log = True

                if need_sat_log:
                    controller["_dbg_pid_sat_last_log_ts"] = float(now_ts)
                    try:
                        sat_since = float(controller.get("_dbg_pid_sat_since", now_ts))
                        sat_dur = float(now_ts) - sat_since
                    except Exception:
                        sat_dur = 0.0
                    logger.info(
                        "Automation %s: PID_SAT state=%s sat=%s duration=%.1fs sp=%.3f meas=%.3f err=%.3f int=%.6f out=%.6f",
                        thread_index, state_id, clamp_state, sat_dur,
                        float(setpoint), float(measurement), float(error),
                        float(controller.get("integral", 0.0)), float(output),
                    )

            # Output rise detection
            last_out = controller.get("_dbg_pid_last_output")
            try:
                last_out_f = float(last_out) if last_out is not None else None
            except Exception:
                last_out_f = None

            eps = 1e-9
            if bool(controller.get("debug_pid_log_transitions", True)) and last_out_f is not None:
                if (last_out_f <= (out_min + eps)) and (float(output) > (out_min + eps)):
                    logger.info(
                        "Automation %s: PID_OUTPUT_RISE state=%s out_prev=%.6f -> out=%.6f",
                        thread_index, state_id, float(last_out_f), float(output)
                    )

            controller["_dbg_pid_last_output"] = float(output)

        return output
    except Exception as e:
        logger.error("pid_update error: %s", e)
        return float(controller.get("out_min", 0.0))


def p_update(controller, setpoint, measurement):
    """Compute P controller output."""
    try:
        error = setpoint - measurement
        output = controller["kp"] * error

        out_min = controller["out_min"]
        out_max = controller["out_max"]

        return max(out_min, min(out_max, output))
    except Exception:
        return 0.0


def onoff_update(controller, setpoint, measurement):
    """Compute ONOFF controller output with hysteresis."""
    try:
        hysteresis = controller.get("hysteresis", 0.0)
        last_output = controller.get("last_output")

        if last_output is None or last_output == 0:
            if measurement < (setpoint - hysteresis):
                controller["last_output"] = 1
                return 1
            controller["last_output"] = 0
            return 0
        else:
            if measurement > (setpoint + hysteresis):
                controller["last_output"] = 0
                return 0
            controller["last_output"] = 1
            return 1
    except Exception:
        return 0


def controller_update(controller, setpoint, measurement, now_ts=None):
    """Universal controller update dispatch."""
    try:
        ctype = str(controller.get("type", "P")).upper()

        if now_ts is None:
            now_ts = time.time()

        if ctype in ("PID", "PI", "PD"):
            return pid_update(controller, setpoint, measurement, now_ts)

        if ctype == "P" or ctype == "CONTROL_METHOD":
            return p_update(controller, setpoint, measurement)

        if ctype == "ONOFF":
            return onoff_update(controller, setpoint, measurement)

        # Fallback
        return p_update(controller, setpoint, measurement)
    except Exception:
        return 0.0


def controller_reset_integral(controller):
    """Reset PID integral accumulator (anti-windup on state reactivation)."""
    try:
        if not isinstance(controller, dict):
            return
        ctype = str(controller.get("type") or "").upper()
        if ctype == "PID":
            controller["integral"] = 0.0
            controller["last_error"] = 0.0
            controller["last_time"] = None
            controller["saturated_since"] = None
    except Exception:
        pass


# =============================================================================
# CONTROLLER MANAGEMENT
# =============================================================================

def _pid_debug_apply_settings(ctx, controller, state_id):
    """Apply debug flags to controller from automation general_settings."""
    try:
        settings = get_automation_settings(ctx) or {}
        enabled = bool(settings.get("debug_pid", False))

        state_ids = settings.get("debug_pid_state_ids")
        if state_ids is None:
            state_ids = settings.get("debug_pid_states")

        if isinstance(state_ids, (list, tuple, set)):
            allowed = set()
            for x in state_ids:
                try:
                    allowed.add(int(x))
                except Exception:
                    continue
            if allowed and int(state_id) not in allowed:
                enabled = False

        ctype = str(controller.get("type") or "").upper()
        if ctype not in ("PID", "PI", "PD"):
            controller["debug_pid"] = False
            return

        controller["debug_pid"] = enabled

        try:
            controller["debug_pid_interval_s"] = float(settings.get("debug_pid_interval_s", 1.0))
        except Exception:
            controller["debug_pid_interval_s"] = 1.0

        try:
            controller["debug_pid_saturation_interval_s"] = float(settings.get("debug_pid_saturation_interval_s", 10.0))
        except Exception:
            controller["debug_pid_saturation_interval_s"] = 10.0

        controller["debug_pid_log_transitions"] = bool(settings.get("debug_pid_log_transitions", True))
        controller["debug_pid_log_saturation"] = bool(settings.get("debug_pid_log_saturation", True))
        controller["debug_pid_log_clamps"] = bool(settings.get("debug_pid_log_clamps", True))
    except Exception:
        pass


def sync_controller_from_shared(ctx, state_id, controller):
    """
    Sync a thread-local controller with shared state (for round-robin recovery).
    """
    try:
        sid = int(state_id)
    except Exception:
        return

    if not isinstance(controller, dict):
        return

    rec = None
    try:
        rec = shared_state_get(ctx, sid)
    except Exception:
        rec = None

    if not isinstance(rec, dict):
        return

    try:
        rec_v = int(rec.get("v", 0))
    except Exception:
        rec_v = 0

    seen = ctx.setdefault("_shared_state_seen_v", {}).get(sid, 0)
    try:
        seen_i = int(seen)
    except Exception:
        seen_i = 0

    if rec_v <= seen_i:
        return

    ctx.setdefault("_shared_state_seen_v", {})[sid] = rec_v

    prev_ctrl = rec.get("controller")
    if isinstance(prev_ctrl, dict):
        old_t = str(prev_ctrl.get("type") or "").upper()
        new_t = str(controller.get("type") or "").upper()
        old_signature = str(
            prev_ctrl.get("_config_signature") or "")
        new_signature = str(
            controller.get("_config_signature") or "")
        same_configuration = bool(old_signature) and (
            old_signature == new_signature)

        if old_t == new_t and same_configuration:
            if new_t in ("PID", "PI", "PD"):
                for k in ("integral", "last_error", "last_time"):
                    if k in prev_ctrl:
                        controller[k] = prev_ctrl.get(k)
            elif new_t == "ONOFF":
                if "last_output" in prev_ctrl:
                    controller["last_output"] = prev_ctrl.get("last_output")

    prev_cs = rec.get("controller_state")
    if isinstance(prev_cs, dict):
        ctx.setdefault("controller_states", {})[sid] = copy.deepcopy(prev_cs)

    ck = rec.get("controller_kind")
    if ck is not None:
        ctx.setdefault("controller_kinds", {})[sid] = str(ck)


def find_pid_config_by_id(ctx, pid_config_id):
    """Compatibility lookup for PID configs with normalized gains."""
    if pid_config_id is None:
        return {}

    try:
        base = find_pid_config(ctx, pid_config_id)
    except Exception:
        base = {}

    if not isinstance(base, dict):
        return {}

    try:
        gains = _normalize_pid_params(base)
    except Exception:
        gains = {}

    if not isinstance(gains, dict):
        gains = {}

    out = dict(base)
    for k in ("kp", "ki", "kd"):
        v = gains.get(k)
        if v is not None:
            out[k] = v

    return out


def extract_setpoint_from_state(state):
    """Extract setpoint from a state's parameters."""
    try:
        params = state.get("parameters") or {}

        for key in ("target_temp_c", "target_temp", "target_bar", "target_pressure_bar",
                     "target", "setpoint", "value", "power", "rpm", "flow_rate"):
            val = params.get(key)
            if isinstance(val, (int, float)):
                return float(val)
    except Exception:
        pass

    return 0.0


def _controller_configuration_signature(ctx, state):
    """Return the deterministic controller-construction identity."""
    control_strategy = state.get("control_strategy") or {}
    if not isinstance(control_strategy, dict):
        control_strategy = {}
    parameters = state.get("parameters") or {}
    if not isinstance(parameters, dict):
        parameters = {}
    controller_type = str(
        control_strategy.get("type") or "").strip().upper()
    signature_data = {
        "control_strategy": copy.deepcopy(control_strategy),
    }

    pid_config_id = control_strategy.get("pid_config_id")
    if pid_config_id is not None:
        signature_data["pid_config"] = copy.deepcopy(
            find_pid_config_by_id(ctx, pid_config_id))

    if controller_type == "ONOFF" or not controller_type:
        hysteresis_parameters = {}
        for key in ("hysteresis", "hysteresis_bar"):
            if key in parameters:
                hysteresis_parameters[key] = copy.deepcopy(parameters[key])
        signature_data["hysteresis_parameters"] = hysteresis_parameters

    if not controller_type:
        actuator_ids = state.get("actuator_ids") or ()
        if not isinstance(actuator_ids, (list, tuple, set)):
            actuator_ids = (actuator_ids,)
        actuator_features = []
        for actuator_id in actuator_ids:
            actuator = find_actuator_by_id(ctx, actuator_id) or {}
            actuator_features.append((
                str(actuator_id),
                str(actuator.get("function_type") or ""),
            ))
        actuator_features.sort()
        signature_data["auto_detect_actuators"] = actuator_features

    return json.dumps(
        signature_data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def ensure_controller_for_state(ctx, state):
    """
    Ensure a controller exists for a state.
    Supports: PID / PI / PD / P / ONOFF / CONTROL_METHOD / Auto-detect.
    """
    if not isinstance(ctx, dict) or not isinstance(state, dict):
        return None

    state_id = state.get("state_id")
    if state_id is None:
        return None

    try:
        sid = int(state_id)
    except Exception:
        return None

    cs = state.get("control_strategy") or {}
    if not isinstance(cs, dict):
        cs = {}

    params = state.get("parameters") or {}
    if not isinstance(params, dict):
        params = {}

    ctype = str(cs.get("type") or "").strip().upper()
    configuration_signature = _controller_configuration_signature(
        ctx, state)
    controllers = ctx.setdefault("controllers", {})

    # Reuse existing controller
    existing = controllers.get(sid)
    if (isinstance(existing, dict)
            and existing.get("_config_signature") == (
                configuration_signature)):
        try:
            existing["state_id"] = sid
            existing["thread_index"] = ctx.get("thread_index")
        except Exception:
            pass

        try:
            sync_controller_from_shared(ctx, sid, existing)
        except Exception:
            pass

        try:
            _pid_debug_apply_settings(ctx, existing, sid)
        except Exception:
            pass

        return existing

    if isinstance(existing, dict):
        controllers.pop(sid, None)
        controller_states = ctx.get("controller_states")
        if isinstance(controller_states, dict):
            controller_states.pop(sid, None)

    # Defaults
    out_min = float(cs.get("out_min", 0.0))
    out_max = float(cs.get("out_max", 10.0))
    sample_time = float(cs.get("sample_time", 0.1))
    anti_windup = bool(cs.get("anti_windup", True))
    error_scale = float(cs.get("error_scale", 1.0))
    dt_max_s = cs.get("dt_max_s", None)
    anti_windup_mode = str(cs.get("anti_windup_mode") or cs.get("anti_windup_strategy") or "clamp")

    # PID-Config optional via pid_config_id
    pid_config_id = cs.get("pid_config_id")
    pid_cfg = None
    if pid_config_id is not None:
        try:
            pid_cfg = find_pid_config_by_id(ctx, int(pid_config_id))
        except Exception:
            pid_cfg = None

    controller = None

    if ctype in ("PID", "PI", "PD"):
        kp = None
        ki = None
        kd = None

        if isinstance(pid_cfg, dict):
            kp = pid_cfg.get("kp")
            ki = pid_cfg.get("ki")
            kd = pid_cfg.get("kd")

            if pid_cfg.get("out_min") is not None:
                out_min = float(pid_cfg.get("out_min"))
            if pid_cfg.get("out_max") is not None:
                out_max = float(pid_cfg.get("out_max"))
            if pid_cfg.get("sample_time") is not None:
                sample_time = float(pid_cfg.get("sample_time"))
            if pid_cfg.get("anti_windup") is not None:
                anti_windup = bool(pid_cfg.get("anti_windup"))
            if pid_cfg.get("error_scale") is not None:
                error_scale = float(pid_cfg.get("error_scale"))
            if pid_cfg.get("dt_max_s") is not None:
                dt_max_s = pid_cfg.get("dt_max_s")
            if pid_cfg.get("anti_windup_mode") is not None:
                anti_windup_mode = str(pid_cfg.get("anti_windup_mode"))
            elif pid_cfg.get("anti_windup_strategy") is not None:
                anti_windup_mode = str(pid_cfg.get("anti_windup_strategy"))

        if kp is None:
            kp = float(cs.get("kp", DEFAULT_PID_KP))
        if ki is None:
            ki = float(cs.get("ki", DEFAULT_PID_KI))
        if kd is None:
            kd = float(cs.get("kd", DEFAULT_PID_KD))

        if ctype == "PI":
            kd = 0.0
        elif ctype == "PD":
            ki = 0.0

        controller = create_pid_controller(
            float(kp), float(ki), float(kd),
            float(out_min), float(out_max),
            float(sample_time),
            anti_windup,
            float(error_scale),
            dt_max_s,
            anti_windup_mode
        )
        controller["type"] = ctype

    elif ctype == "P":
        kp = float(cs.get("kp", DEFAULT_PID_KP))
        controller = create_p_controller(kp, out_min, out_max)

    elif ctype == "ONOFF":
        hysteresis = float(params.get("hysteresis_bar", params.get("hysteresis", cs.get("hysteresis", 0.0))))
        controller = create_onoff_controller(hysteresis)

    elif ctype == "CONTROL_METHOD":
        kp = float(cs.get("kp", 1.0))
        controller = create_p_controller(kp, out_min, out_max)
        controller["type"] = "CONTROL_METHOD"
        controller["method_id"] = cs.get("method_id")

    else:
        # Auto-detect: if write_coil actuator present -> ONOFF, else P
        has_coil = False
        act_ids = state.get("actuator_ids") or []
        if not isinstance(act_ids, list):
            act_ids = [act_ids]

        actuators = list((ctx.get("_cache_actuators") or {}).values())
        for aid in act_ids:
            try:
                act = next((a for a in actuators if int(a.get("actuator_id", -1)) == int(aid)), None)
            except Exception:
                act = None
            if act and str(act.get("function_type")) == "write_coil":
                has_coil = True
                break

        if has_coil or params.get("hysteresis") is not None or params.get("hysteresis_bar") is not None:
            hysteresis = float(params.get("hysteresis_bar", params.get("hysteresis", 0.0)))
            controller = create_onoff_controller(hysteresis)
        else:
            controller = create_p_controller(1.0, out_min, out_max)

    # Meta for debug/tracing
    try:
        controller["state_id"] = sid
        controller["thread_index"] = ctx.get("thread_index")
        controller["_config_signature"] = configuration_signature
    except Exception:
        pass

    # Register in local cache
    controllers[sid] = controller

    # Shared-sync: restore PID memory if state migrated between threads
    try:
        sync_controller_from_shared(ctx, sid, controller)
    except Exception:
        pass
    ctx.setdefault("controller_kinds", {})[sid] = controller.get(
        "type", "UNKNOWN")

    # Apply debug PID settings
    try:
        _pid_debug_apply_settings(ctx, controller, sid)
    except Exception:
        pass

    logger.info(
        "Automation %s: Controller erstellt für state=%d type=%s",
        ctx.get("thread_index"), sid, controller.get("type")
    )

    return controller


# =============================================================================
# CONTROL METHOD EVALUATION
# =============================================================================

def evaluate_control_method(ctx, state, now_ts):
    """Evaluate a control method (ramp, square, sine, impulse, expr)."""
    try:
        cs = state.get("control_strategy") or {}
        method_id = cs.get("method_id")

        if method_id is None:
            return None

        method = find_control_method(ctx, method_id)
        if method is None:
            return None

        template_id = method.get("template_id")
        template = find_control_method_template(ctx, template_id)
        if template is None:
            return None

        template_type = str(template.get("type", "")).lower()
        args = method.get("args") or {}

        state_id = int(state.get("state_id"))

        ms = shared_method_state_get(ctx, state_id)
        if not isinstance(ms, dict):
            ms = (ctx.get("method_states") or {}).get(state_id)

        if not isinstance(ms, dict):
            ms = {"start_ts": float(now_ts), "method_id": method_id}
            shared_method_state_set(ctx, state_id, ms)
        else:
            if ms.get("method_id") != method_id:
                ms["start_ts"] = float(now_ts)
                ms["method_id"] = method_id
                shared_method_state_set(ctx, state_id, ms)

        ctx.setdefault("method_states", {})[state_id] = dict(ms)

        t_elapsed = float(now_ts - float(ms.get("start_ts", now_ts)))

        if template_type == "ramp":
            v0 = float(args.get("v0", 0))
            v1 = float(args.get("v1", 100))
            t_ramp = float(args.get("t_ramp", 600))
            if t_ramp <= 0.0:
                return v1
            if t_elapsed >= t_ramp:
                return v1
            return v0 + (v1 - v0) * (t_elapsed / t_ramp)

        if template_type == "square":
            low = float(args.get("low", 0))
            high = float(args.get("high", 100))
            period_s = float(args.get("period_s", 60))
            duty = float(args.get("duty", 0.5))
            duration_s = args.get("duration_s")
            if duration_s is not None and t_elapsed >= float(duration_s):
                return low
            if period_s <= 0.0:
                return high
            phase = (t_elapsed % period_s) / period_s
            return high if phase < duty else low

        if template_type == "sine":
            amp = float(args.get("amp", 10))
            offset = float(args.get("offset", 50))
            period_s = float(args.get("period_s", 60))
            phase_deg = float(args.get("phase_deg", 0))
            duration_s = args.get("duration_s")
            if duration_s is not None and t_elapsed >= float(duration_s):
                return offset
            if period_s <= 0.0:
                return offset
            phase_rad = math.radians(phase_deg)
            return offset + amp * math.sin(2.0 * math.pi * t_elapsed / period_s + phase_rad)

        if template_type == "impulse":
            amp = float(args.get("amp", 100))
            duration_s = float(args.get("duration_s", 0.25))
            return amp if t_elapsed < duration_s else 0.0

        if template_type == "expr":
            expr_id = str(template.get("expr_id", "")).lower()

            t0 = float(args.get("t0", 0.0))
            duration_s = args.get("duration_s")

            tau = t_elapsed - t0
            if tau < 0.0:
                tau = 0.0

            if duration_s is not None:
                dur = float(duration_s)
                if dur < 0.0:
                    dur = 0.0
                if tau > dur:
                    tau = dur

            if expr_id == "poly2":
                A = float(args.get("A", 0.0))
                B = float(args.get("B", 0.0))
                return A * (tau ** 2.0) + B

            if expr_id == "inv_sin":
                A = float(args.get("A", 0.0))
                period_s_val = float(args.get("period_s", 60.0))
                eps = float(args.get("eps", 0.01))
                offset = float(args.get("offset", 0.0))
                if period_s_val <= 0.0:
                    return offset
                denom = tau + eps
                if denom == 0.0:
                    denom = eps
                return (A / denom) * math.sin(2.0 * math.pi * tau / period_s_val) + offset

            if expr_id == "log_n":
                A = float(args.get("A", 0.0))
                k = float(args.get("k", 0.0))
                B = float(args.get("B", 0.0))
                x = k * tau + 1.0
                if x <= 0.0:
                    x = 1e-9
                return A * math.log(x) + B

            if expr_id == "exp":
                A = float(args.get("A", 0.0))
                k = float(args.get("k", 0.0))
                sign = float(args.get("sign", 1.0))
                B = float(args.get("B", 0.0))
                return A * math.exp(sign * k * tau) + B

            return None

        return None
    except Exception as e:
        logger.error("evaluate_control_method error: %s", e)
        return None


# =============================================================================
# RULE ENGINE - CONDITION EVALUATION
# =============================================================================

def evaluate_condition(ctx, condition, recent_sensor_values, read_sensor_fn):
    """
    Evaluate a single condition.
    Supported types: sensor_cmp, sensor_between, state_value_is,
                     process_value_is, actuator_value_is
    Aliases: sensor_threshold, state_value, process_value
    """
    if not isinstance(condition, dict):
        return False

    try:
        raw_type = condition.get("type", "")
        ctype = str(raw_type or "").strip()
        ctype_l = ctype.lower()

        def _warn_unknown(t):
            try:
                s = ctx.setdefault("_warned_unknown_condition_types", set())
                if not isinstance(s, set):
                    s = set()
                    ctx["_warned_unknown_condition_types"] = s
                if t not in s:
                    s.add(t)
                    logger.warning("Automation: Unknown condition type '%s' -> False", t)
            except Exception:
                pass

        # Alias: sensor_threshold -> sensor_cmp
        if ctype_l == "sensor_threshold":
            op = None
            threshold = None
            if condition.get("ge") is not None:
                op = ">="
                threshold = condition.get("ge")
            elif condition.get("gt") is not None:
                op = ">"
                threshold = condition.get("gt")
            elif condition.get("le") is not None:
                op = "<="
                threshold = condition.get("le")
            elif condition.get("lt") is not None:
                op = "<"
                threshold = condition.get("lt")
            elif condition.get("eq") is not None:
                op = "=="
                threshold = condition.get("eq")
            elif condition.get("ne") is not None:
                op = "!="
                threshold = condition.get("ne")

            if op is None:
                op = condition.get("op", "==")
                threshold = condition.get("value", 0)

            mapped = {
                "type": "sensor_cmp",
                "sensor_id": condition.get("sensor_id"),
                "op": op,
                "value": threshold,
                "hysteresis": condition.get("hysteresis", 0),
            }
            return evaluate_condition(ctx, mapped, recent_sensor_values, read_sensor_fn)

        # Alias: state_value -> state_value_is
        if ctype_l == "state_value":
            mapped = {
                "type": "state_value_is",
                "state_id": condition.get("state_id"),
                "value_id": condition.get("value_id", condition.get("equals", -1)),
            }
            return evaluate_condition(ctx, mapped, recent_sensor_values, read_sensor_fn)

        # Alias: process_value -> process_value_is
        if ctype_l == "process_value":
            mapped = {
                "type": "process_value_is",
                "p_id": condition.get("p_id"),
                "value_id": condition.get("value_id", condition.get("equals", -1)),
            }
            return evaluate_condition(ctx, mapped, recent_sensor_values, read_sensor_fn)

        # Canonical: sensor_cmp
        if ctype_l == "sensor_cmp":
            try:
                sensor_id = int(condition.get("sensor_id", -1))
            except Exception:
                return False

            op = condition.get("op", "==")
            try:
                threshold = float(condition.get("value", 0))
            except Exception:
                return False

            value = recent_sensor_values.get(sensor_id)
            if value is None and read_sensor_fn is not None:
                value = read_sensor_fn(sensor_id)
            if value is None:
                return False

            try:
                value = float(value)
            except Exception:
                return False

            try:
                hysteresis = float(condition.get("hysteresis", 0))
            except Exception:
                hysteresis = 0.0

            if op == ">=":
                return value >= (threshold - hysteresis)
            if op == ">":
                return value > threshold
            if op == "<=":
                return value <= (threshold + hysteresis)
            if op == "<":
                return value < threshold
            if op == "==":
                return abs(value - threshold) <= hysteresis
            if op == "!=":
                return abs(value - threshold) > hysteresis
            return False

        # Canonical: sensor_between
        if ctype_l == "sensor_between":
            try:
                sensor_id = int(condition.get("sensor_id", -1))
                min_val = float(condition.get("min", 0))
                max_val = float(condition.get("max", 100))
            except Exception:
                return False

            inclusive = bool(condition.get("inclusive", True))

            value = recent_sensor_values.get(sensor_id)
            if value is None and read_sensor_fn is not None:
                value = read_sensor_fn(sensor_id)
            if value is None:
                return False

            try:
                value = float(value)
            except Exception:
                return False

            if inclusive:
                return min_val <= value <= max_val
            return min_val < value < max_val

        # Canonical: state_value_is
        if ctype_l == "state_value_is":
            try:
                state_id = int(condition.get("state_id", -1))
                expected_value_id = int(condition.get("value_id", -1))
            except Exception:
                return False

            rsv = ctx.get("runtime_state_values") or {}
            current = rsv.get(state_id)

            if current is None:
                state = find_process_state_by_id(ctx, state_id)
                if state:
                    current = get_state_value_id(ctx, state)

            if current is None:
                return False

            try:
                return int(current) == expected_value_id
            except Exception:
                return False

        # Canonical: process_value_is
        if ctype_l == "process_value_is":
            try:
                p_id = int(condition.get("p_id", -1))
                expected_value_id = int(condition.get("value_id", -1))
            except Exception:
                return False

            rpv = ctx.get("runtime_process_values") or {}
            current = rpv.get(p_id)
            return current is not None and int(current) == expected_value_id

        # Canonical: actuator_value_is
        if ctype_l == "actuator_value_is":
            try:
                actuator_id = int(condition.get("actuator_id", -1))
            except Exception:
                return False

            expected_value = condition.get("value")

            lock = ctx.get("local_actuator_lock")
            lav = ctx.get("local_actuator_values") or {}

            if lock:
                with lock:
                    current = lav.get(str(actuator_id)) or lav.get(int(actuator_id))
            else:
                current = lav.get(str(actuator_id)) or lav.get(int(actuator_id))

            if current is None:
                return False

            if isinstance(current, dict):
                current = current.get("value")

            if expected_value is None:
                return current is not None

            return current == expected_value

        if not ctype_l:
            return False

        _warn_unknown(ctype or "<empty>")
        return False
    except Exception:
        return False


def evaluate_condition_list(ctx, conditions, recent_sensor_values, read_sensor_fn, mode="all"):
    """Evaluate a list of conditions. mode: 'all'(AND), 'any'(OR), 'none'."""
    try:
        if not conditions:
            return True if mode == "all" else (True if mode == "none" else False)

        results = []
        for cond in conditions:
            result = evaluate_condition(ctx, cond, recent_sensor_values, read_sensor_fn)
            results.append(result)

        if mode == "all":
            return all(results)
        elif mode == "any":
            return any(results)
        elif mode == "none":
            return not any(results)
    except Exception:
        pass
    return False


def evaluate_rule_conditions(ctx, rule, recent_sensor_values, read_sensor_fn):
    """Evaluate all conditions of a rule. Returns (is_true, is_else)."""
    try:
        when_all = rule.get("when_all") or []
        when_any = rule.get("when_any") or []
        when_none = rule.get("when_none") or []

        all_ok = evaluate_condition_list(ctx, when_all, recent_sensor_values, read_sensor_fn, "all")
        any_ok = evaluate_condition_list(ctx, when_any, recent_sensor_values, read_sensor_fn, "any") if when_any else True
        none_ok = evaluate_condition_list(ctx, when_none, recent_sensor_values, read_sensor_fn, "none")

        is_true = all_ok and any_ok and none_ok
        return is_true, not is_true
    except Exception:
        return False, True


# =============================================================================
# RULE ENGINE - SCOPE / APPLICABILITY
# =============================================================================

def _rule_scope_process_ids(ctx, rule):
    """Get p_ids from a rule's process_scope."""
    if not isinstance(rule, dict):
        return []
    try:
        scope = rule.get("process_scope") or {}

        p_ids = _normalize_int_list(scope.get("p_ids") or [])
        if p_ids:
            return p_ids

        state_ids = _normalize_int_list(scope.get("state_ids") or [])
        if not state_ids:
            return []

        out = []
        for sid in state_ids:
            st = find_process_state_by_id(ctx, sid)
            if not isinstance(st, dict):
                continue
            pid = st.get("p_id")
            try:
                pid_int = int(pid)
            except Exception:
                continue
            if pid_int not in out:
                out.append(pid_int)

        return out
    except Exception:
        return []


def _rule_active_process_ids(ctx, rule):
    """Get *active* p_ids from a rule's process_scope."""
    if not isinstance(rule, dict):
        return []
    try:
        scope = rule.get("process_scope") or {}
        scope_p_ids = _normalize_int_list(scope.get("p_ids") or [])
        scope_p_set = set(scope_p_ids) if scope_p_ids else None

        state_ids = _normalize_int_list(scope.get("state_ids") or [])
        active = []

        for sid in state_ids:
            st = find_process_state_by_id(ctx, sid)
            if not isinstance(st, dict):
                continue
            if not is_state_active(ctx, st):
                continue

            pid = st.get("p_id")
            try:
                pid_int = int(pid)
            except Exception:
                continue

            if scope_p_set is not None and pid_int not in scope_p_set:
                continue

            if pid_int not in active:
                active.append(pid_int)

        if active:
            return active

        return _rule_scope_process_ids(ctx, rule)
    except Exception:
        return []


def is_rule_applicable(ctx, rule):
    """Check if a rule is applicable (scope, flags, etc.)."""
    if not isinstance(rule, dict):
        return False

    try:
        flags = rule.get("flags") or {}

        # Enabled check
        if not flags.get("enabled", True):
            return False

        scope = rule.get("process_scope") or {}
        p_ids_raw = scope.get("p_ids")
        state_ids_raw = scope.get("state_ids")
        modes_raw = scope.get("modes")

        def _norm_int_list_local(v):
            if not isinstance(v, (list, tuple, set)):
                return []
            tmp = []
            for x in v:
                try:
                    tmp.append(int(x))
                except Exception:
                    continue
            seen = set()
            out = []
            for x in tmp:
                if x in seen:
                    continue
                seen.add(x)
                out.append(x)
            return out

        def _norm_str_set(v):
            if not isinstance(v, (list, tuple, set)):
                return set()
            out = set()
            for x in v:
                if x is None:
                    continue
                sx = str(x).strip().lower()
                if sx:
                    out.add(sx)
            return out

        p_ids = _norm_int_list_local(p_ids_raw)
        p_id_set = set(p_ids) if p_ids else None

        state_ids = _norm_int_list_local(state_ids_raw)
        modes = _norm_str_set(modes_raw)

        # State-Scope
        scoped_states = []
        if state_ids:
            for sid in state_ids:
                st = find_process_state_by_id(ctx, sid)
                if not isinstance(st, dict):
                    continue
                if p_id_set is not None:
                    try:
                        st_pid = int(st.get("p_id", -1))
                    except Exception:
                        continue
                    if st_pid not in p_id_set:
                        continue
                scoped_states.append(st)

            any_active = False
            for st in scoped_states:
                if is_state_active(ctx, st):
                    any_active = True
                    break

            if not any_active:
                return False

        # Mode-Scope
        if modes:
            cfg = cfg_snapshot(ctx)

            mode_map = {}
            for pm in (cfg.get("process_modes") or []):
                try:
                    mid = int(pm.get("mode_id", -1))
                except Exception:
                    continue
                mode_map[mid] = str(pm.get("mode") or "").strip().lower()

            if scoped_states:
                mode_states = scoped_states
            else:
                mode_states = cfg.get("process_states") or []
                if p_id_set is not None:
                    filtered = []
                    for ps in mode_states:
                        if not isinstance(ps, dict):
                            continue
                        try:
                            ps_pid = int(ps.get("p_id", -1))
                        except Exception:
                            continue
                        if ps_pid in p_id_set:
                            filtered.append(ps)
                    mode_states = filtered

            current_modes = set()
            for ps in mode_states:
                if not isinstance(ps, dict):
                    continue
                if not is_state_active(ctx, ps):
                    continue

                mode_id = ps.get("mode_id")
                if mode_id is None:
                    continue
                try:
                    mode_id_int = int(mode_id)
                except Exception:
                    continue

                m = mode_map.get(mode_id_int)
                if m:
                    current_modes.add(m)

            if not current_modes:
                return False

            if not (current_modes & modes):
                return False

        return True
    except Exception:
        return False


# =============================================================================
# RULE ENGINE - ACTION EXECUTION
# =============================================================================

def execute_action(ctx, action, rule_id):
    """Execute a single rule action (sync)."""
    try:
        action_type = action.get("action", "")

        if action_type == "actuator_set":
            actuator_id = int(action.get("actuator_id", -1))
            value = action.get("value", 0)
            emit_actuator_task(ctx, actuator_id, value, meta={"origin": "rule_engine", "rule_id": rule_id})
            return

        if action_type == "process_state_set_value":
            state_id = int(action.get("state_id", -1))
            value_id = int(action.get("value_id", 2))

            prev = set_state_value_id(ctx, state_id, value_id, source="rule_engine")

            emit_event(
                ctx, "PROCESS_STATE_VALUE_CHANGED",
                {"state_id": state_id, "from_value_id": prev, "to_value_id": value_id, "source": "rule_engine", "rule_id": rule_id}
            )

            active_ids = get_active_value_ids(ctx)
            was_active = prev in active_ids if prev is not None else False
            now_active = value_id in active_ids

            if was_active and not now_active:
                state = find_process_state_by_id(ctx, state_id)
                if state:
                    apply_deactivation_fail_safe(ctx, state, prev, value_id, "rule_action")
            return

        if action_type == "process_set_value":
            p_id = int(action.get("p_id", -1))
            value_id = int(action.get("value_id", 2))
            ctx.setdefault("runtime_process_values", {})[p_id] = value_id
            return

        if action_type == "setpoint_override":
            state_id = int(action.get("state_id", -1))
            value = float(action.get("value", 0))
            duration_s = action.get("duration_s")

            until = None
            if duration_s is not None:
                try:
                    until = time.time() + float(duration_s)
                except Exception:
                    until = None

            override = {"value": float(value), "until": until, "ts": time.time(), "rule_id": rule_id}

            ctx.setdefault("setpoint_overrides", {})[int(state_id)] = dict(override)

            try:
                shared_setpoint_override_set(ctx, int(state_id), override)
            except Exception:
                pass
            return

        if action_type == "emit_event":
            event_id = action.get("event_id")
            fields = action.get("fields") or {}

            cfg = cfg_snapshot(ctx)
            event_types = cfg.get("event_types") or []
            event_name = None
            for et in event_types:
                try:
                    if int(et.get("event_id", -1)) == int(event_id):
                        event_name = et.get("name")
                        break
                except Exception:
                    continue
            if event_name is None:
                event_name = "CUSTOM_EVENT_{}".format(event_id)

            emit_event(ctx, event_name, fields)
            return

        if action_type == "delay":
            duration_s = float(action.get("duration_s", 1.0))
            time.sleep(duration_s)
            return

        if action_type == "start_control_method":
            state_id = int(action.get("state_id", -1))
            method_id = int(action.get("method_id", -1))

            ms = {"start_ts": time.time(), "method_id": method_id, "rule_id": rule_id}
            ctx.setdefault("method_states", {})[int(state_id)] = dict(ms)

            try:
                shared_method_state_set(ctx, int(state_id), ms)
            except Exception:
                pass
            return

    except Exception as e:
        logger.error("execute_action error: %s", e)


def execute_action_list(ctx, actions, rule_id):
    """Execute a list of actions (sync)."""
    for action in actions:
        try:
            execute_action(ctx, action, rule_id)
        except Exception as e:
            logger.error(
                "Automation %s: Action error %s (rule=%s): %s",
                ctx.get("thread_index"), action.get("action"), rule_id, e
            )


# =============================================================================
# RULE ENGINE - MAIN EVALUATION
# =============================================================================

def evaluate_automation_rules(ctx, recent_sensor_values, read_sensor_fn):
    """Evaluate all automation rules (sync)."""
    try:
        rules = get_automation_rules(ctx)

        for rule in rules:
            try:
                rule_id = rule.get("rule_id", "unknown")

                if not is_rule_applicable(ctx, rule):
                    continue

                flags = rule.get("flags") or {}
                is_one_shot = bool(flags.get("one_shot", False))

                if is_one_shot:
                    if shared_one_shot_is_fired(ctx, rule):
                        if check_one_shot_reset(ctx, rule, recent_sensor_values, read_sensor_fn):
                            shared_one_shot_reset(ctx, rule)
                            logger.info("Automation %s: One-Shot Reset for rule %s", ctx.get("thread_index"), rule_id)
                        else:
                            continue

                is_true, is_else = evaluate_rule_conditions(ctx, rule, recent_sensor_values, read_sensor_fn)

                if is_true:
                    if is_one_shot:
                        if not shared_one_shot_claim(ctx, rule):
                            continue

                    do_actions = rule.get("do") or []
                    if do_actions:
                        logger.info(
                            "Automation %s: Rule %s (true) -> %d actions",
                            ctx.get("thread_index"), rule_id, len(do_actions)
                        )
                        execute_action_list(ctx, do_actions, rule_id)

                elif is_else:
                    else_actions = rule.get("else_do") or []
                    if else_actions:
                        logger.debug(
                            "Automation %s: Rule %s (else) -> %d actions",
                            ctx.get("thread_index"), rule_id, len(else_actions)
                        )
                        execute_action_list(ctx, else_actions, rule_id)

            except Exception as e:
                logger.error("Automation %s: Rule error %s: %s", ctx.get("thread_index"), rule.get("rule_id", "?"), e, exc_info=True)
    except Exception as e:
        logger.error("evaluate_automation_rules error: %s", e, exc_info=True)


# =============================================================================
# FAIL-SAFE
# =============================================================================

def apply_safety_fail_safe(ctx, state, measurement, reason="safety"):
    """Apply fail-safe for a state — all actuators to 0 (sync)."""
    try:
        act_ids = state.get("actuator_ids") or []

        for aid in act_ids:
            emit_actuator_task(
                ctx, int(aid), 0,
                meta={"fail_safe": reason, "state_id": int(state.get("state_id"))}
            )
    except Exception as e:
        logger.error("apply_safety_fail_safe error: %s", e)


def apply_deactivation_fail_safe(ctx, state, from_vid, to_vid, reason="deactivated"):
    """Apply fail-safe on state deactivation (sync)."""
    try:
        sid = int(state.get("state_id"))
        apply_safety_fail_safe(ctx, state, None, reason)

        emit_event(
            ctx, "STATE_DEACTIVATED_FAILSAFE",
            {
                "state_id": sid,
                "from_value_id": from_vid,
                "to_value_id": to_vid,
                "reason": reason,
            }
        )

        logger.info(
            "Automation %s: FAIL-SAFE for state=%s (reason=%s, %s -> %s)",
            ctx.get("thread_index"), sid, reason, from_vid, to_vid
        )
    except Exception as e:
        logger.error("apply_deactivation_fail_safe error: %s", e)


# =============================================================================
# ACTUATOR TASK EMISSION
# =============================================================================

def emit_actuator_task(ctx, actuator_id, value, meta=None):
    """Create and emit an actuator task (sync).

    In TM v5.0 job-based mode: collects tasks in ctx["_collected_tasks"]
    for return to TM. Also optionally sends event to queue_event_send
    if available (for backward compatibility).
    """
    try:
        actuator = find_actuator_by_id(ctx, actuator_id)

        if actuator is None:
            logger.warning(
                "Automation %s: emit_actuator_task: unknown actuator_id=%s",
                ctx.get("thread_index"), actuator_id
            )
            return False

        controller_id = int(actuator.get("controller_id", -1))

        # Controller permission check
        if not is_controller_allowed(ctx, controller_id):
            logger.info(
                "Automation %s: emit_actuator_task: controller_id=%s not allowed",
                ctx.get("thread_index"), controller_id
            )
            return False

        ftype = str(actuator.get("function_type") or "")
        bus = actuator.get("bus_type")
        addr = actuator.get("address")
        unit = actuator.get("unit", "0x1")

        # Value conversion
        try:
            raw_value = float(value)
        except Exception:
            raw_value = value

        # Function type specific conversion
        if ftype == "write_coil":
            command_value = 1 if float(raw_value) > 0.5 else 0

        elif ftype in ("write_register", "write_holding_register", "write_single_register"):
            command_value = scaled_to_raw(ctx, actuator, raw_value)

        else:
            command_value = raw_value

        # Duplicate suppression (thread-local)
        settings = get_automation_settings(ctx)
        suppress_s = float(settings.get("suppress_duplicates_s", 0.0) or 0.0)

        if suppress_s > 0.0:
            last_cmd = ctx.setdefault("actuator_last_cmd", {})
            last = last_cmd.get(int(actuator_id))
            now_ts = time.time()

            if last and last.get("val") == command_value:
                if (now_ts - float(last.get("ts", 0.0))) < suppress_s:
                    logger.debug(
                        "Automation %s: SUPPRESS duplicate cmd (aid=%s val=%s)",
                        ctx.get("thread_index"), actuator_id, command_value
                    )
                    return True

            last_cmd[int(actuator_id)] = {"val": command_value, "ts": now_ts}

        # Build task
        meta_in = dict(meta or {})
        meta_in["run_epoch"] = ctx.get("run_epoch")
        local_actuator_id = actuator.get(
            "local_actuator_id",
            actuator_id,
        )

        task = {
            "task_uuid": str(uuid.uuid4()),
            "task_type": "actuator",
            "controller_id": controller_id,
            "controller_uuid": actuator.get("controller_uuid"),
            "actuator_id": int(local_actuator_id),
            "projected_actuator_id": int(actuator_id),
            "actuator_uuid": actuator.get("actuator_uuid") or actuator.get("signal_uuid"),
            "device_type": "actuator",
            "bus_type": bus,
            "function_type": ftype,
            "address": addr,
            "unit": unit,
            "control_value": command_value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "priority": 1,
            "status": "pending",
            "source": "automation_{}".format(ctx.get("thread_index")),
            "meta": meta_in,
        }

        logger.info(
            "Automation %s: ACTUATOR_TASK -> ctrl=%s aid=%s addr=%s f=%s cmd=%s",
            ctx.get("thread_index"), controller_id, local_actuator_id,
            addr, ftype, command_value
        )

        # v5.0: Collect task for return to TM (job-based mode)
        collected = ctx.get("_collected_tasks")
        if isinstance(collected, list):
            collected.append(task)

        # Also emit event if queue available (backward compat / direct mode)
        q = ctx.get("queue_event_send")
        if q is not None:
            evt = make_event(
                "ACTUATOR_TASK",
                payload={"task": task, "run_epoch": ctx.get("run_epoch")},
                controller_id=controller_id,
                thread_index=ctx.get("thread_index"),
                target="controller",
            )
            _queue_put_safe(q, evt)

        return True
    except Exception as e:
        logger.error("emit_actuator_task error: %s", e, exc_info=True)
        return False
