# -*- coding: utf-8 -*-

# src/library/_generic_finite_state_machine.py


"""
 =============================================================================
 _GENERIC_FINITE_STATE_MACHINE
 =============================================================================

 WHAT IT IS :
   A generic, deterministic, purely functional Finite State Machine (FSM)
   library built exclusively on the Python standard library. FSM definitions
   ("spec") and FSM instances ("machine") are plain dict/list structures.
   Every public operation is a pure function: it never mutates its inputs
   and always returns new, independent, deep-copied values.

 WHY IT IS NEEDED :
   Event driven automation systems (controller threads, worker threads,
   rule engines) need a reproducible, auditable state core. This library
   provides deterministic state control that is decoupled from real time,
   free of hidden global state and safe under multi-threading, because
   shared mutation simply does not exist at the API boundary. Thread
   orchestration is intentionally done ABOVE this library: threads exchange
   immutable machine values, they never share a mutating machine object.

 HOW IT WORKS :
   1. Build a spec   : fsm_create_spec / fsm_add_state / fsm_add_transition
   2. Register logic : fsm_register_action / fsm_register_guard /
                       fsm_register_normalizer (callables referenced by name)
   3. Validate       : fsm_validate_spec
   4. Instantiate    : fsm_init  -> machine (independent immutable value)
   5. Drive          : fsm_send / fsm_post / fsm_process   (event queue)
                       fsm_advance_by / fsm_advance_to     (clock + timers)
   6. Observe        : fsm_snapshot / fsm_get_* / history / trace
   7. Persist        : fsm_snapshot / fsm_restore / fsm_spec_to_json /
                       fsm_spec_from_json / config dataset adapter

 DETERMINISM CONTRACT :
   - Events      : processed by (priority DESC, enqueue sequence ASC)
   - Timers      : fired by (fire_at ASC, priority DESC, sequence ASC)
   - Transitions : resolved by (priority DESC, registration index ASC);
                   the first candidate whose guard passes wins
   - Run to completion: a fired timer event is fully processed (the queue
     is drained) before the next due timer fires
   - The logical clock never moves backwards
   - halt: applied immediately when a result requests it; a transition that
     is already executing completes structurally, afterwards no further
     events or timers are processed until fsm_resume

 ACTION RESULT MODEL (declarative side effects) :
   Actions return an "fsm_result" dict. Supported keys, applied in this
   fixed order: trace -> data_replace -> data_patch -> cancel_timers
   -> schedule -> emit -> halt

 CONFIG COMPATIBILITY :
   fsm_spec_from_config / fsm_machines_from_config consume normalized
   "device_state_norm" JSON datasets (process_values, process_modes,
   processes, site_controller_map, ...) through a fully generic, mapping
   driven adapter. Nothing in the FSM core is coupled to that schema.

 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import json
import logging

from copy import deepcopy
from functools import partial

# =============================================================================
# CENTRAL MODULE "CONTEXT MANAGEMENT" DEFINITION & INITIALISATION
# =============================================================================
# FSM_CONTEXT holds read-only library defaults and constants. It is never
# mutated at runtime; per machine overrides live inside each machine value
# under the "config" key, so there is no global mutable state at all.

FSM_CONTEXT = {
    "library_name": "_generic_finite_state_machine",
    "library_version": "1.0.0",
    "snapshot_version": 1,
    "wildcard": "*",
    "timer_kinds": ("delay", "interval"),
    "timer_scopes": ("machine", "state"),
    "condition_policies": ("skip", "cancel"),
    "event_sources": ("external", "timer", "action", "system"),
    "result_keys": (
        "trace", "data_replace", "data_patch",
        "cancel_timers", "schedule", "emit", "halt",
    ),
    "log_levels": {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40},
    "defaults": {
        "history_limit": 1000,
        "trace_limit": 2000,
        "queue_limit": 10000,
        "timer_limit": 10000,
        "max_steps_per_call": 10000,
        "trace_enabled": True,
        "strict_unhandled": False,
        "log_enabled": True,
        "log_level": "INFO",
    },
}

_FSM_LOGGER = logging.getLogger(FSM_CONTEXT["library_name"])


# =============================================================================
# HELP-FUNCTIONS (internal, prefixed with "_fsm_")
# =============================================================================


def _fsm_log(container, level, message):
    """Emit one log line honoring machine config and an optional custom hook."""
    try:
        spec = None
        config = FSM_CONTEXT["defaults"]
        if isinstance(container, dict):
            if container.get("spec_type") == "fsm_spec":
                spec = container
            elif isinstance(container.get("spec"), dict):
                spec = container["spec"]
                if isinstance(container.get("config"), dict):
                    config = container["config"]
        if not config.get("log_enabled", True):
            return None
        levels = FSM_CONTEXT["log_levels"]
        wanted = levels.get(str(level).upper(), levels["INFO"])
        threshold = levels.get(str(config.get("log_level", "INFO")).upper(), levels["INFO"])
        if wanted < threshold:
            return None
        hook = None
        if spec is not None:
            hook = spec.get("registry", {}).get("hooks", {}).get("log")
        if callable(hook):
            hook(str(level).upper(), str(message))
            return None
        _FSM_LOGGER.log(wanted, str(message))
        return None
    except Exception:
        return None


def _fsm_config_value(machine, key):
    """Read one effective config value of a machine with library fallback."""
    config = machine.get("config") if isinstance(machine, dict) else None
    if isinstance(config, dict) and key in config:
        return config[key]
    return FSM_CONTEXT["defaults"].get(key)


def _fsm_next_seq(machine):
    """Return the next monotonic sequence number of the working machine copy."""
    machine["seq"] = int(machine.get("seq", 0)) + 1
    return machine["seq"]


def _fsm_match_token(pattern, value):
    """Match one token against a value where '*' is the universal wildcard."""
    return pattern == FSM_CONTEXT["wildcard"] or pattern == value


def _fsm_event_sort_key(event):
    """Deterministic event order: priority DESC, then enqueue sequence ASC."""
    return (-int(event.get("priority", 0)), int(event.get("seq", 0)))


def _fsm_timer_sort_key(timer):
    """Deterministic timer order: fire_at ASC, priority DESC, sequence ASC."""
    return (
        float(timer.get("fire_at", 0.0)),
        -int(timer.get("priority", 0)),
        int(timer.get("seq", 0)),
    )


def _fsm_transition_sort_key(transition):
    """Deterministic transition order: priority DESC, registration index ASC."""
    return (-int(transition.get("priority", 0)), int(transition.get("index", 0)))


def _fsm_row_order_value(row, order_key):
    """Sort helper: numeric order value of one config row, stable fallback."""
    value = row.get(order_key) if isinstance(row, dict) else None
    try:
        return float(value)
    except Exception:
        return float("inf")


def _fsm_deep_merge(base, patch):
    """Recursively merge dict 'patch' into dict 'base' returning a new dict.

    Nested dicts merge key by key; every other value type (including lists)
    is replaced by a deep copy of the patch value.
    """
    merged = deepcopy(base) if isinstance(base, dict) else {}
    if not isinstance(patch, dict):
        return merged
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _fsm_deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _fsm_bounded_append(target_list, entry, limit):
    """Append one entry and trim the list front so it never exceeds limit."""
    target_list.append(entry)
    excess = len(target_list) - int(limit)
    if excess > 0:
        del target_list[0:excess]
    return target_list


def _fsm_trace(machine, tag, detail):
    """Append one trace entry when tracing is enabled for the machine."""
    if not _fsm_config_value(machine, "trace_enabled"):
        return None
    entry = {
        "clock": machine.get("clock", 0.0),
        "step": machine.get("step", 0),
        "tag": str(tag),
        "detail": deepcopy(detail),
    }
    _fsm_bounded_append(machine["trace_log"], entry, _fsm_config_value(machine, "trace_limit"))
    return None


def _fsm_history(machine, entry):
    """Append one bounded history entry (audit log of the machine)."""
    _fsm_bounded_append(machine["history"], deepcopy(entry), _fsm_config_value(machine, "history_limit"))
    return None


def _fsm_make_view(machine):
    """Build the read-only, deep-copied view handed to user callables."""
    spec = machine.get("spec", {})
    return {
        "spec_name": spec.get("name"),
        "spec_version": spec.get("version"),
        "spec_meta": deepcopy(spec.get("meta", {})),
        "state": machine.get("state"),
        "data": deepcopy(machine.get("data", {})),
        "clock": machine.get("clock", 0.0),
        "step": machine.get("step", 0),
        "halted": bool(machine.get("halted", False)),
        "queue_size": len(machine.get("queue", [])),
        "timer_names": [
            timer.get("name")
            for timer in sorted(machine.get("timers", []), key=_fsm_timer_sort_key)
        ],
    }


def _fsm_registry_get(spec, kind, name):
    """Look up one registered callable by kind and name (None when absent)."""
    return spec.get("registry", {}).get(kind, {}).get(name)


def _fsm_call_guard(machine, guard_name, event):
    """Evaluate one guard by name; exceptions and unknown guards yield False."""
    if guard_name is None:
        return True
    fn = _fsm_registry_get(machine["spec"], "guards", guard_name)
    if not callable(fn):
        _fsm_trace(machine, "guard_missing", {"guard": guard_name})
        _fsm_log(machine, "WARNING", "guard not registered: " + str(guard_name))
        return False
    try:
        return bool(fn(_fsm_make_view(machine), deepcopy(event)))
    except Exception as exc:
        _fsm_trace(machine, "guard_error", {"guard": guard_name, "error": repr(exc)})
        _fsm_log(machine, "ERROR", "guard raised: " + str(guard_name) + " -> " + repr(exc))
        return False


def _fsm_apply_result(machine, result, phase):
    """Apply one declarative fsm_result dict in the fixed documented order.

    Order: trace -> data_replace -> data_patch -> cancel_timers -> schedule
    -> emit -> halt. Unknown keys are ignored on purpose (forward compatible).
    """
    if not isinstance(result, dict):
        return None
    for line in (result.get("trace") or []):
        _fsm_trace(machine, "action_trace", {"phase": phase, "message": deepcopy(line)})
    if result.get("data_replace") is not None:
        machine["data"] = deepcopy(result["data_replace"])
    elif isinstance(result.get("data_patch"), dict):
        machine["data"] = _fsm_deep_merge(machine["data"], result["data_patch"])
    if result.get("cancel_timers") is not None:
        _fsm_cancel_timers(machine, result["cancel_timers"], None)
    for timer_def in (result.get("schedule") or []):
        _fsm_schedule_timer(machine, timer_def, machine.get("state"))
    for item in (result.get("emit") or []):
        if isinstance(item, dict):
            _fsm_enqueue_event(
                machine, item.get("name"), item.get("payload"),
                item.get("priority", 0), "action", None,
            )
        else:
            _fsm_enqueue_event(machine, item, None, 0, "action", None)
    if result.get("halt"):
        machine["halted"] = True
        if isinstance(result.get("halt"), bool):
            machine["halt_reason"] = str(phase)
        else:
            machine["halt_reason"] = deepcopy(result.get("halt"))
        _fsm_trace(machine, "halt", {"phase": phase, "reason": machine["halt_reason"]})
        _fsm_history(machine, {
            "type": "halt",
            "clock": machine["clock"],
            "step": machine["step"],
            "reason": machine["halt_reason"],
        })
    return None


def _fsm_call_action(machine, action_name, event, phase):
    """Execute one action by name and apply its declarative result."""
    fn = _fsm_registry_get(machine["spec"], "actions", action_name)
    if not callable(fn):
        _fsm_trace(machine, "action_missing", {"action": action_name, "phase": phase})
        _fsm_log(machine, "WARNING", "action not registered: " + str(action_name))
        return None
    try:
        result = fn(_fsm_make_view(machine), deepcopy(event))
    except Exception as exc:
        _fsm_trace(machine, "action_error", {"action": action_name, "phase": phase, "error": repr(exc)})
        _fsm_log(machine, "ERROR", "action raised: " + str(action_name) + " -> " + repr(exc))
        return None
    _fsm_apply_result(machine, result, phase + ":" + str(action_name))
    return None


def _fsm_run_actions(machine, action_names, event, phase):
    """Run one ordered action list; the pipeline always completes fully."""
    for action_name in (action_names or []):
        _fsm_call_action(machine, action_name, event, phase)
    return None


def _fsm_normalize_event(machine, event):
    """Run the ordered normalizer chain; a normalizer returning None drops it.

    Returned dicts are merged over the current event, while the original
    enqueue sequence number is always preserved to keep the total order.
    """
    current = event
    for name in machine["spec"].get("normalizer_order", []):
        fn = _fsm_registry_get(machine["spec"], "normalizers", name)
        if not callable(fn):
            _fsm_trace(machine, "normalizer_missing", {"normalizer": name})
            continue
        try:
            outcome = fn(deepcopy(current))
        except Exception as exc:
            _fsm_trace(machine, "normalizer_error", {"normalizer": name, "error": repr(exc)})
            continue
        if outcome is None:
            _fsm_trace(machine, "event_dropped", {"normalizer": name, "event": deepcopy(current)})
            return None
        if isinstance(outcome, dict):
            merged = deepcopy(current)
            merged.update(deepcopy(outcome))
            merged["seq"] = current["seq"]
            current = merged
    return current


def _fsm_enqueue_event(machine, name, payload, priority, source, timer_name):
    """Enqueue one canonical event honoring the configured queue limit."""
    if name is None or str(name) == "":
        _fsm_trace(machine, "enqueue_rejected", {"reason": "empty event name"})
        return None
    if len(machine["queue"]) >= int(_fsm_config_value(machine, "queue_limit")):
        _fsm_trace(machine, "queue_overflow", {"event": str(name)})
        _fsm_log(machine, "WARNING", "event queue overflow, dropped: " + str(name))
        return None
    event = {
        "name": str(name),
        "payload": deepcopy(payload) if payload is not None else {},
        "priority": int(priority) if priority is not None else 0,
        "seq": _fsm_next_seq(machine),
        "enqueued_at": machine["clock"],
        "source": source if source in FSM_CONTEXT["event_sources"] else "external",
    }
    if timer_name is not None:
        event["timer"] = str(timer_name)
    machine["queue"].append(event)
    return event


def _fsm_pop_next_event(machine):
    """Pop the next due event using the deterministic event order."""
    if not machine["queue"]:
        return None
    machine["queue"].sort(key=_fsm_event_sort_key)
    return machine["queue"].pop(0)


def _fsm_prepare_timer_def(timer_def, fallback_scope):
    """Normalize one raw timer definition into the canonical internal shape.

    Returns a tuple (normalized_def_or_None, issue_or_None).
    """
    if not isinstance(timer_def, dict):
        return (None, "timer definition must be a dict")
    kind = timer_def.get("kind")
    if kind is None:
        kind = "interval" if timer_def.get("interval") is not None else "delay"
    if kind not in FSM_CONTEXT["timer_kinds"]:
        return (None, "invalid timer kind: " + str(kind))
    event_name = timer_def.get("event")
    if event_name is None or str(event_name) == "":
        return (None, "timer requires an event name")
    interval = timer_def.get("interval")
    delay = timer_def.get("delay")
    if kind == "interval":
        try:
            interval = float(interval)
        except Exception:
            return (None, "interval timer requires a numeric interval")
        if interval <= 0.0:
            return (None, "interval must be > 0")
    else:
        interval = None
    if delay is None:
        if kind == "interval":
            delay = interval
        else:
            delay = 0.0
    try:
        delay = float(delay)
    except Exception:
        return (None, "timer delay must be numeric")
    if delay < 0.0:
        return (None, "timer delay must be >= 0")
    repeat = timer_def.get("repeat")
    if repeat is not None:
        try:
            repeat = int(repeat)
        except Exception:
            return (None, "timer repeat must be an integer")
        if repeat < 0:
            return (None, "timer repeat must be >= 0")
        if repeat == 0:
            repeat = None
    scope = timer_def.get("scope") or fallback_scope or "machine"
    if scope not in FSM_CONTEXT["timer_scopes"]:
        return (None, "invalid timer scope: " + str(scope))
    policy = timer_def.get("condition_policy") or "skip"
    if policy not in FSM_CONTEXT["condition_policies"]:
        return (None, "invalid condition_policy: " + str(policy))
    normalized = {
        "name": str(timer_def.get("name")) if timer_def.get("name") else None,
        "kind": kind,
        "event": str(event_name),
        "payload": deepcopy(timer_def.get("payload")) if timer_def.get("payload") is not None else {},
        "delay": delay,
        "interval": interval,
        "repeat": repeat if kind == "interval" else 1,
        "condition": timer_def.get("condition"),
        "condition_policy": policy,
        "scope": scope,
        "priority": int(timer_def.get("priority", 0)),
        "meta": deepcopy(timer_def.get("meta")) if isinstance(timer_def.get("meta"), dict) else {},
    }
    return (normalized, None)


def _fsm_schedule_timer(machine, timer_def, owner_state):
    """Instantiate one active timer on the working machine copy."""
    normalized, issue = _fsm_prepare_timer_def(timer_def, None)
    if issue is not None:
        _fsm_trace(machine, "timer_rejected", {"issue": issue, "definition": deepcopy(timer_def)})
        _fsm_log(machine, "WARNING", "timer rejected: " + issue)
        return None
    if len(machine["timers"]) >= int(_fsm_config_value(machine, "timer_limit")):
        _fsm_trace(machine, "timer_overflow", {"timer": normalized.get("name")})
        _fsm_log(machine, "WARNING", "timer limit reached, rejected: " + str(normalized.get("name")))
        return None
    seq = _fsm_next_seq(machine)
    instance = deepcopy(normalized)
    if instance["name"] is None:
        instance["name"] = "timer_" + str(seq)
    instance["seq"] = seq
    instance["fire_at"] = float(machine["clock"]) + instance["delay"]
    instance["remaining"] = instance["repeat"]
    instance["owner_state"] = owner_state if instance["scope"] == "state" else None
    machine["timers"].append(instance)
    _fsm_trace(machine, "timer_scheduled", {
        "timer": instance["name"],
        "fire_at": instance["fire_at"],
        "scope": instance["scope"],
    })
    return instance


def _fsm_cancel_timers(machine, names, scope_state):
    """Cancel timers by name(s) or '*', optionally limited to one owner state."""
    if names is None:
        return 0
    if isinstance(names, str):
        wanted = None if names == FSM_CONTEXT["wildcard"] else [names]
    else:
        wanted = [str(name) for name in names]
    kept = []
    cancelled = 0
    for timer in machine["timers"]:
        name_hit = wanted is None or timer.get("name") in wanted
        scope_hit = scope_state is None or (
            timer.get("scope") == "state" and timer.get("owner_state") == scope_state
        )
        if name_hit and scope_hit:
            cancelled += 1
            _fsm_trace(machine, "timer_cancelled", {"timer": timer.get("name")})
        else:
            kept.append(timer)
    machine["timers"] = kept
    return cancelled


def _fsm_start_state_timers(machine, state_name):
    """Schedule all timers declared on one state (default scope 'state')."""
    state_def = machine["spec"]["states"].get(state_name, {})
    for timer_def in state_def.get("timers", []):
        prepared = deepcopy(timer_def)
        if not prepared.get("scope"):
            prepared["scope"] = "state"
        _fsm_schedule_timer(machine, prepared, state_name)
    return None


def _fsm_resolve_transition(machine, event):
    """Resolve one event deterministically to the first matching transition."""
    state = machine["state"]
    candidates = []
    for transition in machine["spec"]["transitions"]:
        source_hit = _fsm_match_token(transition.get("source"), state)
        event_hit = _fsm_match_token(transition.get("event"), event["name"])
        if source_hit and event_hit:
            candidates.append(transition)
    candidates.sort(key=_fsm_transition_sort_key)
    for transition in candidates:
        if _fsm_call_guard(machine, transition.get("guard"), event):
            return transition
    return None


def _fsm_exit_state(machine, event):
    """Run exit actions of the current state and cancel its scoped timers."""
    state_name = machine["state"]
    state_def = machine["spec"]["states"].get(state_name, {})
    _fsm_run_actions(machine, state_def.get("on_exit"), event, "exit:" + str(state_name))
    _fsm_cancel_timers(machine, FSM_CONTEXT["wildcard"], state_name)
    return None


def _fsm_enter_state(machine, state_name, event):
    """Set the new state, start its scoped timers and run its enter actions."""
    machine["state"] = state_name
    _fsm_start_state_timers(machine, state_name)
    state_def = machine["spec"]["states"].get(state_name, {})
    _fsm_run_actions(machine, state_def.get("on_enter"), event, "enter:" + str(state_name))
    return None


def _fsm_execute_transition(machine, transition, event):
    """Execute one resolved transition (external or internal) atomically."""
    source_state = machine["state"]
    internal = bool(transition.get("internal")) or transition.get("target") is None
    target_state = source_state if internal else transition.get("target")
    if not internal:
        _fsm_exit_state(machine, event)
    _fsm_run_actions(machine, transition.get("actions"), event, "transition:" + str(transition.get("index")))
    if not internal:
        _fsm_enter_state(machine, target_state, event)
    machine["step"] = int(machine["step"]) + 1
    _fsm_history(machine, {
        "type": "transition",
        "step": machine["step"],
        "clock": machine["clock"],
        "event": event["name"],
        "event_seq": event["seq"],
        "source": source_state,
        "target": machine["state"],
        "transition_index": transition.get("index"),
        "internal": internal,
    })
    return None


def _fsm_process_queue(machine, max_steps):
    """Drain the event queue deterministically within the step budget."""
    if max_steps is None:
        budget = int(_fsm_config_value(machine, "max_steps_per_call"))
    else:
        budget = int(max_steps)
    steps = 0
    while machine["queue"] and not machine["halted"] and steps < budget:
        raw_event = _fsm_pop_next_event(machine)
        if raw_event is None:
            break
        steps = steps + 1
        event = _fsm_normalize_event(machine, raw_event)
        if event is None:
            _fsm_history(machine, {
                "type": "dropped",
                "clock": machine["clock"],
                "step": machine["step"],
                "event": raw_event["name"],
                "event_seq": raw_event["seq"],
            })
            continue
        transition = _fsm_resolve_transition(machine, event)
        if transition is None:
            _fsm_history(machine, {
                "type": "unhandled",
                "clock": machine["clock"],
                "step": machine["step"],
                "event": event["name"],
                "event_seq": event["seq"],
                "state": machine["state"],
            })
            _fsm_trace(machine, "event_unhandled", {"event": event["name"], "state": machine["state"]})
            if _fsm_config_value(machine, "strict_unhandled"):
                _fsm_log(machine, "WARNING", "unhandled event: " + str(event["name"]) + " in state " + str(machine["state"]))
            continue
        _fsm_execute_transition(machine, transition, event)
    if machine["queue"] and steps >= budget:
        _fsm_trace(machine, "step_budget_exhausted", {"pending": len(machine["queue"])})
        _fsm_log(machine, "WARNING", "step budget exhausted, pending events: " + str(len(machine["queue"])))
    return None


def _fsm_fire_one_timer(machine, timer):
    """Fire (or skip/cancel) one due timer and reschedule interval timers."""
    survivors = []
    for existing in machine["timers"]:
        if existing.get("seq") != timer.get("seq"):
            survivors.append(existing)
    machine["timers"] = survivors
    fired = True
    if timer.get("condition") is not None:
        probe_event = {
            "name": timer["event"],
            "payload": deepcopy(timer.get("payload", {})),
            "priority": timer.get("priority", 0),
            "seq": timer.get("seq"),
            "enqueued_at": machine["clock"],
            "source": "timer",
            "timer": timer.get("name"),
        }
        fired = _fsm_call_guard(machine, timer["condition"], probe_event)
        if not fired and timer.get("condition_policy") == "cancel":
            _fsm_trace(machine, "timer_condition_cancel", {"timer": timer.get("name")})
            return None
        if not fired:
            _fsm_trace(machine, "timer_condition_skip", {"timer": timer.get("name")})
    if fired:
        _fsm_enqueue_event(
            machine, timer["event"], timer.get("payload"),
            timer.get("priority", 0), "timer", timer.get("name"),
        )
        if timer.get("remaining") is not None:
            timer["remaining"] = int(timer["remaining"]) - 1
    if timer.get("kind") == "interval":
        finished = timer.get("remaining") is not None and int(timer["remaining"]) <= 0
        if not finished:
            timer["fire_at"] = float(timer["fire_at"]) + float(timer["interval"])
            machine["timers"].append(timer)
    return None


def _fsm_fire_due_timers(machine, target_time):
    """Advance the logical clock to target_time firing due timers in order.

    Run to completion semantics: after every single timer fire the event
    queue is fully drained before the next due timer is selected.
    """
    budget = int(_fsm_config_value(machine, "max_steps_per_call"))
    fires = 0
    while not machine["halted"] and fires < budget:
        due = []
        for timer in machine["timers"]:
            if float(timer.get("fire_at", 0.0)) <= float(target_time):
                due.append(timer)
        if not due:
            break
        due.sort(key=_fsm_timer_sort_key)
        current = due[0]
        machine["clock"] = max(float(machine["clock"]), float(current["fire_at"]))
        _fsm_fire_one_timer(machine, current)
        _fsm_process_queue(machine, None)
        fires = fires + 1
    if fires >= budget:
        _fsm_trace(machine, "timer_budget_exhausted", {"target": target_time})
        _fsm_log(machine, "WARNING", "timer fire budget exhausted while advancing")
    machine["clock"] = max(float(machine["clock"]), float(target_time))
    return None


def _fsm_new_machine_shell(spec, data, clock, config):
    """Create one fresh machine value with independent deep-copied members."""
    machine_config = _fsm_deep_merge(FSM_CONTEXT["defaults"], config if isinstance(config, dict) else {})
    return {
        "machine_type": "fsm_machine",
        "library": FSM_CONTEXT["library_name"],
        "library_version": FSM_CONTEXT["library_version"],
        "spec": deepcopy(spec),
        "state": None,
        "data": deepcopy(data) if isinstance(data, dict) else {},
        "clock": float(clock),
        "queue": [],
        "timers": [],
        "history": [],
        "trace_log": [],
        "halted": False,
        "halt_reason": None,
        "seq": 0,
        "step": 0,
        "config": machine_config,
    }


# =============================================================================
# CODEBASE - FUNCTIONS (public API, prefixed with "fsm_")
# =============================================================================
# --- Spec construction (functional builders, inputs never mutated) ----------


def fsm_create_spec(name, initial_state, version="1.0.0", meta=None):
    """Create one empty FSM spec value (definition only, no runtime parts)."""
    return {
        "spec_type": "fsm_spec",
        "name": str(name),
        "version": str(version),
        "initial_state": str(initial_state),
        "states": {},
        "transitions": [],
        "normalizer_order": [],
        "meta": deepcopy(meta) if isinstance(meta, dict) else {},
        "registry": {"actions": {}, "guards": {}, "normalizers": {}, "hooks": {"log": None}},
    }


def fsm_add_state(spec, state_name, on_enter=None, on_exit=None, timers=None, meta=None):
    """Return a new spec with one state added (an existing state is replaced).

    Timers attached to a state default to scope 'state': they start on entry
    and are cancelled automatically on exit of exactly this state.
    """
    new_spec = deepcopy(spec)
    prepared_timers = []
    for timer_def in (timers or []):
        normalized, issue = _fsm_prepare_timer_def(timer_def, "state")
        if issue is not None:
            raise ValueError("state '" + str(state_name) + "' timer invalid: " + issue)
        if normalized["name"] is None:
            normalized["name"] = str(state_name) + "_" + normalized["event"] + "_" + str(len(prepared_timers) + 1)
        prepared_timers.append(normalized)
    new_spec["states"][str(state_name)] = {
        "name": str(state_name),
        "on_enter": [str(name) for name in (on_enter or [])],
        "on_exit": [str(name) for name in (on_exit or [])],
        "timers": prepared_timers,
        "meta": deepcopy(meta) if isinstance(meta, dict) else {},
    }
    return new_spec


def fsm_add_transition(spec, source, event, target=None, guard=None, actions=None,
                       priority=0, internal=False, meta=None):
    """Return a new spec with one transition appended (registration ordered).

    A transition with target None is always internal (no state change, no
    exit/enter actions, state scoped timers keep running).
    """
    new_spec = deepcopy(spec)
    transition = {
        "index": len(new_spec["transitions"]),
        "source": str(source),
        "event": str(event),
        "target": str(target) if target is not None else None,
        "guard": str(guard) if guard is not None else None,
        "actions": [str(name) for name in (actions or [])],
        "priority": int(priority),
        "internal": bool(internal) or target is None,
        "meta": deepcopy(meta) if isinstance(meta, dict) else {},
    }
    new_spec["transitions"].append(transition)
    return new_spec


def fsm_register_action(spec, name, function):
    """Return a new spec with one named action callable registered.

    Action signature: function(view, event) -> fsm_result dict or None.
    """
    new_spec = deepcopy(spec)
    new_spec["registry"]["actions"][str(name)] = function
    return new_spec


def fsm_register_guard(spec, name, function):
    """Return a new spec with one named guard callable registered.

    Guard signature: function(view, event) -> truthy/falsy.
    """
    new_spec = deepcopy(spec)
    new_spec["registry"]["guards"][str(name)] = function
    return new_spec


def fsm_register_normalizer(spec, name, function, position=None):
    """Return a new spec with one named normalizer registered in chain order.

    Normalizer signature: function(event) -> event dict, or None to drop it.
    """
    new_spec = deepcopy(spec)
    new_spec["registry"]["normalizers"][str(name)] = function
    if str(name) in new_spec["normalizer_order"]:
        new_spec["normalizer_order"].remove(str(name))
    if position is None:
        new_spec["normalizer_order"].append(str(name))
    else:
        new_spec["normalizer_order"].insert(int(position), str(name))
    return new_spec


def fsm_set_log_function(spec, function):
    """Return a new spec using one custom log callable: fn(level, message)."""
    new_spec = deepcopy(spec)
    new_spec["registry"]["hooks"]["log"] = function
    return new_spec


# --- Declarative builders ----------------------------------------------------


def fsm_event(name, payload=None, priority=0):
    """Build one plain event request dict (seq is assigned at enqueue time)."""
    return {
        "name": str(name),
        "payload": deepcopy(payload) if payload is not None else {},
        "priority": int(priority),
    }


def fsm_timer(name, event, kind="delay", delay=None, interval=None, repeat=None,
              condition=None, condition_policy="skip", payload=None, scope=None,
              priority=0, meta=None):
    """Build one timer definition dict for states, results or fsm_schedule.

    kind 'delay'    : fires once at clock + delay (delay default 0.0)
    kind 'interval' : first fire at clock + (delay or interval), then every
                      interval; repeat limits total emissions (None = endless)
    condition       : guard name evaluated right before firing
    condition_policy: 'skip' keeps interval timers alive, 'cancel' removes
    scope           : 'machine' survives state changes, 'state' is cancelled
                      automatically when its owning state is exited
    """
    return {
        "name": str(name) if name is not None else None,
        "event": str(event),
        "kind": str(kind),
        "delay": delay,
        "interval": interval,
        "repeat": repeat,
        "condition": str(condition) if condition is not None else None,
        "condition_policy": str(condition_policy),
        "payload": deepcopy(payload) if payload is not None else {},
        "scope": str(scope) if scope is not None else None,
        "priority": int(priority),
        "meta": deepcopy(meta) if isinstance(meta, dict) else {},
    }


def fsm_result(data_patch=None, data_replace=None, emit=None, schedule=None,
               cancel_timers=None, halt=None, trace=None):
    """Build one declarative action result dict (only used keys are set)."""
    result = {}
    if data_patch is not None:
        result["data_patch"] = deepcopy(data_patch)
    if data_replace is not None:
        result["data_replace"] = deepcopy(data_replace)
    if emit is not None:
        result["emit"] = deepcopy(emit) if isinstance(emit, list) else [deepcopy(emit)]
    if schedule is not None:
        result["schedule"] = deepcopy(schedule) if isinstance(schedule, list) else [deepcopy(schedule)]
    if cancel_timers is not None:
        result["cancel_timers"] = deepcopy(cancel_timers)
    if halt is not None:
        result["halt"] = deepcopy(halt)
    if trace is not None:
        result["trace"] = deepcopy(trace) if isinstance(trace, list) else [deepcopy(trace)]
    return result


# --- Validation ---------------------------------------------------------------


def fsm_validate_spec(spec):
    """Validate one spec deeply; returns {'ok', 'errors', 'warnings'}.

    Errors block fsm_init; warnings document design smells (shadowed
    transitions, unreachable states, terminal states) without blocking.
    """
    errors = []
    warnings = []
    try:
        if not isinstance(spec, dict) or spec.get("spec_type") != "fsm_spec":
            return {"ok": False, "errors": ["spec must be a dict created by fsm_create_spec"], "warnings": []}
        if not spec.get("name"):
            errors.append("spec name is missing")
        states = spec.get("states", {})
        if not isinstance(states, dict) or len(states) == 0:
            errors.append("spec has no states")
            return {"ok": False, "errors": errors, "warnings": warnings}
        if spec.get("initial_state") not in states:
            errors.append("initial_state '" + str(spec.get("initial_state")) + "' is not a defined state")
        registry = spec.get("registry", {})
        actions = registry.get("actions", {})
        guards = registry.get("guards", {})
        normalizers = registry.get("normalizers", {})
        for state_name, state_def in states.items():
            for phase in ("on_enter", "on_exit"):
                for action_name in state_def.get(phase, []):
                    if action_name not in actions:
                        errors.append("state '" + str(state_name) + "' " + phase + " action not registered: " + str(action_name))
            for timer_def in state_def.get("timers", []):
                normalized, issue = _fsm_prepare_timer_def(timer_def, "state")
                if issue is not None:
                    errors.append("state '" + str(state_name) + "' timer invalid: " + issue)
                    continue
                if normalized.get("name") is None:
                    errors.append("state '" + str(state_name) + "' timer requires a name")
                if normalized.get("condition") is not None and normalized["condition"] not in guards:
                    errors.append("state '" + str(state_name) + "' timer condition not registered: " + str(normalized["condition"]))
        wildcard = FSM_CONTEXT["wildcard"]
        seen_signatures = {}
        incoming = {}
        for transition in spec.get("transitions", []):
            source = transition.get("source")
            target = transition.get("target")
            internal = bool(transition.get("internal"))
            if source != wildcard and source not in states:
                errors.append("transition " + str(transition.get("index")) + " source unknown: " + str(source))
            if not internal and (target is None or target not in states):
                errors.append("transition " + str(transition.get("index")) + " target unknown: " + str(target))
            if not transition.get("event"):
                errors.append("transition " + str(transition.get("index")) + " event is empty")
            guard_name = transition.get("guard")
            if guard_name is not None and guard_name not in guards:
                errors.append("transition " + str(transition.get("index")) + " guard not registered: " + str(guard_name))
            for action_name in transition.get("actions", []):
                if action_name not in actions:
                    errors.append("transition " + str(transition.get("index")) + " action not registered: " + str(action_name))
            signature = (str(source), str(transition.get("event")), int(transition.get("priority", 0)))
            if signature in seen_signatures:
                warnings.append("transition " + str(transition.get("index")) + " shadows transition " + str(seen_signatures[signature]) + " (same source/event/priority)")
            else:
                seen_signatures[signature] = transition.get("index")
            if target is not None and not internal:
                incoming[str(target)] = True
        for name in spec.get("normalizer_order", []):
            if name not in normalizers:
                errors.append("normalizer in chain not registered: " + str(name))
        for state_name in states:
            if state_name != spec.get("initial_state") and not incoming.get(state_name):
                warnings.append("state '" + str(state_name) + "' has no incoming transition (unreachable unless restored)")
        outgoing = {}
        for transition in spec.get("transitions", []):
            if transition.get("source") == wildcard:
                for state_name in states:
                    outgoing[state_name] = True
            else:
                outgoing[str(transition.get("source"))] = True
        for state_name in states:
            if not outgoing.get(state_name):
                warnings.append("state '" + str(state_name) + "' has no outgoing transition (terminal state)")
        return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}
    except Exception as exc:
        return {"ok": False, "errors": ["validation crashed: " + repr(exc)], "warnings": warnings}


# --- Runtime lifecycle ---------------------------------------------------------


def fsm_init(spec, data=None, clock=0.0, config=None):
    """Create one new machine, enter the initial state and settle the queue."""
    try:
        validation = fsm_validate_spec(spec)
        if not validation["ok"]:
            raise ValueError("invalid FSM spec: " + "; ".join(validation["errors"]))
        machine = _fsm_new_machine_shell(spec, data, clock, config)
        for warning in validation["warnings"]:
            _fsm_trace(machine, "spec_warning", {"warning": warning})
        init_event = {
            "name": "__init__",
            "payload": {},
            "priority": 0,
            "seq": 0,
            "enqueued_at": machine["clock"],
            "source": "system",
        }
        _fsm_enter_state(machine, machine["spec"]["initial_state"], init_event)
        _fsm_history(machine, {
            "type": "init",
            "clock": machine["clock"],
            "step": machine["step"],
            "state": machine["state"],
        })
        _fsm_process_queue(machine, None)
        return machine
    except Exception as exc:
        _FSM_LOGGER.error("fsm_init failed: " + repr(exc))
        raise


def fsm_send(machine, event_name, payload=None, priority=0, process=True, max_steps=None):
    """Return a new machine with one event enqueued and (optionally) processed."""
    try:
        working = deepcopy(machine)
        if working["halted"]:
            _fsm_trace(working, "send_ignored_halted", {"event": str(event_name)})
            return working
        _fsm_enqueue_event(working, event_name, payload, priority, "external", None)
        if process:
            _fsm_process_queue(working, max_steps)
        return working
    except Exception as exc:
        _fsm_log(machine, "ERROR", "fsm_send failed: " + repr(exc))
        raise


def fsm_post(machine, event_name, payload=None, priority=0):
    """Return a new machine with one event enqueued but not yet processed."""
    return fsm_send(machine, event_name, payload, priority, False, None)


def fsm_process(machine, max_steps=None):
    """Return a new machine with the pending event queue drained."""
    try:
        working = deepcopy(machine)
        if working["halted"]:
            _fsm_trace(working, "process_ignored_halted", {})
            return working
        _fsm_process_queue(working, max_steps)
        return working
    except Exception as exc:
        _fsm_log(machine, "ERROR", "fsm_process failed: " + repr(exc))
        raise


def fsm_advance_to(machine, target_time):
    """Return a new machine advanced to target_time with due timers fired.

    The logical clock never moves backwards: a target in the past yields an
    unchanged machine (plus one trace entry). While halted only the clock
    moves; no timers fire and no events are processed.
    """
    try:
        working = deepcopy(machine)
        target = float(target_time)
        if target < float(working["clock"]):
            _fsm_trace(working, "advance_rejected", {"target": target, "clock": working["clock"]})
            return working
        if working["halted"]:
            working["clock"] = target
            _fsm_trace(working, "advance_while_halted", {"target": target})
            return working
        _fsm_fire_due_timers(working, target)
        return working
    except Exception as exc:
        _fsm_log(machine, "ERROR", "fsm_advance_to failed: " + repr(exc))
        raise


def fsm_advance_by(machine, delta):
    """Return a new machine advanced by one non-negative clock delta."""
    step = float(delta)
    if step < 0.0:
        step = 0.0
    return fsm_advance_to(machine, float(machine.get("clock", 0.0)) + step)


def fsm_schedule(machine, timer_def):
    """Return a new machine with one additional timer scheduled now."""
    working = deepcopy(machine)
    _fsm_schedule_timer(working, timer_def, working.get("state"))
    return working


def fsm_cancel(machine, timer_names):
    """Return a new machine with the given timer name(s) or '*' cancelled."""
    working = deepcopy(machine)
    _fsm_cancel_timers(working, timer_names, None)
    return working


def fsm_halt(machine, reason=None):
    """Return a new halted machine (no further events or timers execute)."""
    working = deepcopy(machine)
    working["halted"] = True
    working["halt_reason"] = deepcopy(reason) if reason is not None else "manual"
    _fsm_trace(working, "halt", {"reason": working["halt_reason"]})
    _fsm_history(working, {
        "type": "halt",
        "clock": working["clock"],
        "step": working["step"],
        "reason": working["halt_reason"],
    })
    return working


def fsm_resume(machine):
    """Return a new machine with the halted flag cleared."""
    working = deepcopy(machine)
    working["halted"] = False
    working["halt_reason"] = None
    _fsm_trace(working, "resume", {})
    return working


def fsm_patch_data(machine, patch):
    """Return a new machine with one deep merged data patch applied."""
    working = deepcopy(machine)
    working["data"] = _fsm_deep_merge(working["data"], patch if isinstance(patch, dict) else {})
    return working


def fsm_replace_data(machine, data):
    """Return a new machine with the data payload fully replaced."""
    working = deepcopy(machine)
    working["data"] = deepcopy(data) if isinstance(data, dict) else {}
    return working


# --- Pure read access ----------------------------------------------------------


def fsm_get_state(machine):
    """Return the current state name."""
    return machine.get("state")


def fsm_get_data(machine):
    """Return a deep copy of the machine data payload."""
    return deepcopy(machine.get("data", {}))


def fsm_get_clock(machine):
    """Return the current logical clock value."""
    return float(machine.get("clock", 0.0))


def fsm_is_halted(machine):
    """Return True when the machine is halted."""
    return bool(machine.get("halted", False))


def fsm_get_pending_events(machine):
    """Return a deep copy of pending events in deterministic order."""
    return deepcopy(sorted(machine.get("queue", []), key=_fsm_event_sort_key))


def fsm_get_active_timers(machine):
    """Return a deep copy of active timers in deterministic firing order."""
    return deepcopy(sorted(machine.get("timers", []), key=_fsm_timer_sort_key))


def fsm_get_history(machine, limit=None):
    """Return a deep copy of the newest history entries (all when limit None)."""
    history = machine.get("history", [])
    if limit is not None:
        history = history[-int(limit):]
    return deepcopy(history)


def fsm_get_trace(machine, limit=None):
    """Return a deep copy of the newest trace entries (all when limit None)."""
    trace_log = machine.get("trace_log", [])
    if limit is not None:
        trace_log = trace_log[-int(limit):]
    return deepcopy(trace_log)


# --- Snapshot / Restore ----------------------------------------------------------


def fsm_snapshot(machine):
    """Return one JSON-serializable deep snapshot of the full machine state.

    The snapshot never contains callables: actions, guards, normalizers and
    timer conditions are referenced by name only, so it can be persisted to
    file or database and restored later against the same spec.
    """
    try:
        return {
            "snapshot_type": "fsm_snapshot",
            "snapshot_version": FSM_CONTEXT["snapshot_version"],
            "library": FSM_CONTEXT["library_name"],
            "library_version": FSM_CONTEXT["library_version"],
            "spec_name": machine.get("spec", {}).get("name"),
            "spec_version": machine.get("spec", {}).get("version"),
            "state": machine.get("state"),
            "data": deepcopy(machine.get("data", {})),
            "clock": float(machine.get("clock", 0.0)),
            "step": int(machine.get("step", 0)),
            "seq": int(machine.get("seq", 0)),
            "halted": bool(machine.get("halted", False)),
            "halt_reason": deepcopy(machine.get("halt_reason")),
            "queue": deepcopy(machine.get("queue", [])),
            "timers": deepcopy(machine.get("timers", [])),
            "history": deepcopy(machine.get("history", [])),
            "trace_log": deepcopy(machine.get("trace_log", [])),
            "config": deepcopy(machine.get("config", {})),
        }
    except Exception as exc:
        _fsm_log(machine, "ERROR", "fsm_snapshot failed: " + repr(exc))
        raise


def fsm_restore(spec, snapshot, config=None):
    """Rebuild one machine from a snapshot without re-running enter actions."""
    try:
        if not isinstance(snapshot, dict) or snapshot.get("snapshot_type") != "fsm_snapshot":
            raise ValueError("snapshot must be created by fsm_snapshot")
        validation = fsm_validate_spec(spec)
        if not validation["ok"]:
            raise ValueError("invalid FSM spec: " + "; ".join(validation["errors"]))
        if snapshot.get("spec_name") != spec.get("name"):
            raise ValueError("snapshot spec name mismatch: " + str(snapshot.get("spec_name")) + " != " + str(spec.get("name")))
        if snapshot.get("state") not in spec.get("states", {}):
            raise ValueError("snapshot state unknown in spec: " + str(snapshot.get("state")))
        machine = _fsm_new_machine_shell(spec, snapshot.get("data"), snapshot.get("clock", 0.0), snapshot.get("config"))
        if isinstance(config, dict):
            machine["config"] = _fsm_deep_merge(machine["config"], config)
        machine["state"] = snapshot.get("state")
        machine["step"] = int(snapshot.get("step", 0))
        machine["seq"] = int(snapshot.get("seq", 0))
        machine["halted"] = bool(snapshot.get("halted", False))
        machine["halt_reason"] = deepcopy(snapshot.get("halt_reason"))
        machine["queue"] = deepcopy(snapshot.get("queue", []))
        machine["timers"] = deepcopy(snapshot.get("timers", []))
        machine["history"] = deepcopy(snapshot.get("history", []))
        machine["trace_log"] = deepcopy(snapshot.get("trace_log", []))
        _fsm_history(machine, {
            "type": "restore",
            "clock": machine["clock"],
            "step": machine["step"],
            "state": machine["state"],
        })
        return machine
    except Exception as exc:
        _FSM_LOGGER.error("fsm_restore failed: " + repr(exc))
        raise


# --- Spec JSON export / import ---------------------------------------------------


def fsm_spec_to_json(spec, indent=2):
    """Serialize the JSON-safe spec definition (registry callables excluded)."""
    definition = deepcopy(spec)
    definition.pop("registry", None)
    return json.dumps(definition, indent=indent, sort_keys=True, ensure_ascii=False)


def fsm_spec_from_json(source, actions=None, guards=None, normalizers=None, log_function=None):
    """Rebuild one full spec from JSON text/dict plus callable registries."""
    try:
        definition = json.loads(source) if isinstance(source, str) else deepcopy(source)
        if not isinstance(definition, dict) or definition.get("spec_type") != "fsm_spec":
            raise ValueError("source is not an exported fsm spec definition")
        spec = deepcopy(definition)
        spec["registry"] = {
            "actions": dict(actions) if isinstance(actions, dict) else {},
            "guards": dict(guards) if isinstance(guards, dict) else {},
            "normalizers": dict(normalizers) if isinstance(normalizers, dict) else {},
            "hooks": {"log": log_function if callable(log_function) else None},
        }
        return spec
    except Exception as exc:
        _FSM_LOGGER.error("fsm_spec_from_json failed: " + repr(exc))
        raise


def fsm_describe(spec):
    """Return one compact JSON-safe overview of a spec for humans and tools."""
    transitions = []
    for transition in spec.get("transitions", []):
        transitions.append({
            "index": transition.get("index"),
            "source": transition.get("source"),
            "event": transition.get("event"),
            "target": transition.get("target"),
            "priority": transition.get("priority"),
            "internal": transition.get("internal"),
            "guard": transition.get("guard"),
        })
    return {
        "name": spec.get("name"),
        "version": spec.get("version"),
        "initial_state": spec.get("initial_state"),
        "states": sorted(spec.get("states", {}).keys()),
        "transition_count": len(transitions),
        "transitions": transitions,
        "normalizer_order": deepcopy(spec.get("normalizer_order", [])),
        "registered_actions": sorted(spec.get("registry", {}).get("actions", {}).keys()),
        "registered_guards": sorted(spec.get("registry", {}).get("guards", {}).keys()),
    }


# --- Generic config dataset adapter ("device_state_norm" compatible) --------------


def fsm_config_default_mapping():
    """Return the default mapping matching the 'device_state_norm' V5 schema.

    Every key can be overridden, so the very same adapter builds machines
    from any comparable normalized dataset (states table + optional modes
    table + instances table) without touching the FSM core.
    """
    return {
        "spec_name": "device_state_process",
        "spec_version": "5",
        "states_table": "process_values",
        "state_name_key": "state_value",
        "state_order_key": "value_id",
        "modes_table": "process_modes",
        "mode_name_key": "mode",
        "mode_order_key": "mode_id",
        "instances_table": "processes",
        "instance_name_key": "process_name",
        "instance_order_key": "p_id",
        "initial_state": "deactivated",
        "default_mode": "manual",
        "state_event_prefix": "set_state:",
        "allow_self_transition": False,
        "mode_event": "set_mode",
        "mode_data_key": "mode",
        "error_state": "error",
        "lifecycle_transitions": [
            {"source": "*", "event": "fault", "target": "error", "priority": 100},
            {"source": "deactivated", "event": "activate", "target": "activated", "priority": 10},
            {"source": "activated", "event": "deactivate", "target": "deactivated", "priority": 10},
            {"source": "error", "event": "recover", "target": "deactivated", "priority": 10},
            {"source": "deactivated", "event": "maintenance_start", "target": "maintenance", "priority": 10},
            {"source": "maintenance", "event": "maintenance_end", "target": "deactivated", "priority": 10},
        ],
    }


def fsm_builtin_guard_mode_valid(view, event):
    """Guard: accept a mode change only when the mode is configured."""
    wanted = event.get("payload", {}).get("mode")
    allowed = view.get("spec_meta", {}).get("modes", [])
    return wanted in allowed


def fsm_builtin_action_apply_mode(view, event):
    """Action: persist the requested operating mode into the machine data."""
    mode_key = view.get("spec_meta", {}).get("mode_data_key", "mode")
    wanted = event.get("payload", {}).get("mode")
    return fsm_result(data_patch={mode_key: wanted}, trace=["mode -> " + str(wanted)])


def fsm_builtin_action_record_event(view, event):
    """Action: record the last handled event name and clock into the data."""
    return fsm_result(data_patch={
        "last_event": event.get("name"),
        "last_event_clock": view.get("clock"),
    })


def fsm_builtin_action_record_error(view, event):
    """Action: record the last error payload into the machine data."""
    return fsm_result(
        data_patch={"last_error": {
            "event": event.get("name"),
            "payload": deepcopy(event.get("payload", {})),
            "clock": view.get("clock"),
        }},
        trace=["error recorded"],
    )


def fsm_spec_from_config(config, mapping=None, actions=None, guards=None, normalizers=None):
    """Build one generic FSM spec from a normalized JSON config dataset.

    The adapter is fully mapping driven. With the default mapping it targets
    the 'device_state_norm' V5 schema: the state space comes from
    'process_values', operating modes from 'process_modes'. It generates:
      1. one state per state row (deterministic order via state_order_key)
      2. a complete deterministic state graph via 'set_state:<target>' events
      3. filtered lifecycle transitions (activate/deactivate/fault/recover/
         maintenance_start/maintenance_end) - only where states really exist
      4. one internal wildcard 'set_mode' transition guarded by the mode list
    Nothing here couples the FSM core to those key names.
    """
    try:
        merged_mapping = _fsm_deep_merge(fsm_config_default_mapping(), mapping if isinstance(mapping, dict) else {})
        if not isinstance(config, dict):
            raise ValueError("config must be a dict (parsed JSON)")
        state_rows = config.get(merged_mapping["states_table"], [])
        if not isinstance(state_rows, list) or len(state_rows) == 0:
            raise ValueError("config states table missing or empty: " + str(merged_mapping["states_table"]))
        ordered_rows = sorted(state_rows, key=partial(_fsm_row_order_value, order_key=merged_mapping["state_order_key"]))
        state_names = []
        state_meta = {}
        for row in ordered_rows:
            state_name = row.get(merged_mapping["state_name_key"]) if isinstance(row, dict) else None
            if state_name is None:
                continue
            state_name = str(state_name)
            if state_name in state_names:
                continue
            state_names.append(state_name)
            state_meta[state_name] = deepcopy(row)
        if len(state_names) == 0:
            raise ValueError("no state names resolved from config states table")
        initial_state = merged_mapping.get("initial_state")
        if initial_state not in state_names:
            initial_state = state_names[0]
        mode_names = []
        mode_rows = config.get(merged_mapping["modes_table"], [])
        if isinstance(mode_rows, list):
            for row in sorted(mode_rows, key=partial(_fsm_row_order_value, order_key=merged_mapping["mode_order_key"])):
                mode_name = row.get(merged_mapping["mode_name_key"]) if isinstance(row, dict) else None
                if mode_name is not None and str(mode_name) not in mode_names:
                    mode_names.append(str(mode_name))
        config_meta = {}
        meta_rows = config.get("meta", [])
        if isinstance(meta_rows, list) and len(meta_rows) > 0 and isinstance(meta_rows[0], dict):
            config_meta = deepcopy(meta_rows[0])
        spec_meta = {
            "adapter": "fsm_spec_from_config",
            "config_schema": config_meta,
            "mapping": deepcopy(merged_mapping),
            "modes": mode_names,
            "mode_data_key": merged_mapping.get("mode_data_key", "mode"),
        }
        spec = fsm_create_spec(merged_mapping["spec_name"], initial_state, merged_mapping["spec_version"], spec_meta)
        error_state = merged_mapping.get("error_state")
        for state_name in state_names:
            on_enter = ["record_event"]
            if error_state is not None and state_name == str(error_state):
                on_enter = ["record_event", "record_error"]
            spec = fsm_add_state(spec, state_name, on_enter, None, None, {"config_row": state_meta[state_name]})
        prefix = str(merged_mapping.get("state_event_prefix", "set_state:"))
        allow_self = bool(merged_mapping.get("allow_self_transition", False))
        for target_name in state_names:
            for source_name in state_names:
                if source_name == target_name and not allow_self:
                    continue
                spec = fsm_add_transition(
                    spec, source_name, prefix + target_name, target_name,
                    None, None, 0, False, {"generated": "state_graph"},
                )
        for entry in merged_mapping.get("lifecycle_transitions", []):
            source = str(entry.get("source"))
            target = str(entry.get("target"))
            if target not in state_names:
                continue
            if source != FSM_CONTEXT["wildcard"] and source not in state_names:
                continue
            spec = fsm_add_transition(
                spec, source, str(entry.get("event")), target,
                entry.get("guard"), entry.get("actions"),
                int(entry.get("priority", 10)), False, {"generated": "lifecycle"},
            )
        if len(mode_names) > 0:
            spec = fsm_add_transition(
                spec, FSM_CONTEXT["wildcard"], str(merged_mapping.get("mode_event", "set_mode")),
                None, "mode_valid", ["apply_mode"], 50, True, {"generated": "mode"},
            )
            spec = fsm_register_guard(spec, "mode_valid", fsm_builtin_guard_mode_valid)
            spec = fsm_register_action(spec, "apply_mode", fsm_builtin_action_apply_mode)
        spec = fsm_register_action(spec, "record_event", fsm_builtin_action_record_event)
        if error_state is not None and str(error_state) in state_names:
            spec = fsm_register_action(spec, "record_error", fsm_builtin_action_record_error)
        for name, fn in (actions or {}).items():
            spec = fsm_register_action(spec, name, fn)
        for name, fn in (guards or {}).items():
            spec = fsm_register_guard(spec, name, fn)
        for name, fn in (normalizers or {}).items():
            spec = fsm_register_normalizer(spec, name, fn)
        return spec
    except Exception as exc:
        _FSM_LOGGER.error("fsm_spec_from_config failed: " + repr(exc))
        raise


def fsm_machines_from_config(config, spec=None, mapping=None, data_defaults=None,
                             clock=0.0, config_overrides=None):
    """Build one deterministic machine per configured instance row.

    Returns a dict {instance_name: machine} in stable instance order. The
    instances table is mapping driven, so the very same call can create
    machines per process ('processes'), per controller route
    ('site_controller_map' with instance_name_key 'route_key') or per any
    other config table - true multi-worker instantiation from one dataset.
    """
    try:
        merged_mapping = _fsm_deep_merge(fsm_config_default_mapping(), mapping if isinstance(mapping, dict) else {})
        machine_spec = spec if spec is not None else fsm_spec_from_config(config, merged_mapping)
        instance_rows = config.get(merged_mapping["instances_table"], [])
        if not isinstance(instance_rows, list):
            instance_rows = []
        machines = {}
        ordered = sorted(instance_rows, key=partial(_fsm_row_order_value, order_key=merged_mapping["instance_order_key"]))
        mode_key = machine_spec.get("meta", {}).get("mode_data_key", "mode")
        for row in ordered:
            instance_name = row.get(merged_mapping["instance_name_key"]) if isinstance(row, dict) else None
            if instance_name is None:
                continue
            instance_name = str(instance_name)
            if instance_name in machines:
                continue
            data = _fsm_deep_merge(data_defaults if isinstance(data_defaults, dict) else {}, {
                "instance": deepcopy(row),
                mode_key: merged_mapping.get("default_mode"),
            })
            machines[instance_name] = fsm_init(machine_spec, data, clock, config_overrides)
        return machines
    except Exception as exc:
        _FSM_LOGGER.error("fsm_machines_from_config failed: " + repr(exc))
        raise


# --- Selftest support (pure module level callables, reusable via partial) ---------


def _selftest_action_patch(view, event, key, value):
    """Selftest action: patch one data key with one fixed value."""
    return fsm_result(data_patch={key: deepcopy(value)})


def _selftest_action_count(view, event, key):
    """Selftest action: increment one numeric data counter."""
    return fsm_result(data_patch={key: int(view["data"].get(key, 0)) + 1})


def _selftest_action_record_clock(view, event, key):
    """Selftest action: store the current logical clock under one data key."""
    return fsm_result(data_patch={key: view["clock"]})


def _selftest_action_append_token(view, event, key, token):
    """Selftest action: append one token to a string under one data key."""
    return fsm_result(data_patch={key: str(view["data"].get(key, "")) + str(token)})


def _selftest_action_emit(view, event, emit_name):
    """Selftest action: emit one follow-up event."""
    return fsm_result(emit=[emit_name])


def _selftest_action_halt(view, event):
    """Selftest action: request an immediate machine halt."""
    return fsm_result(halt="selftest_halt", trace=["halt requested"])


def _selftest_guard_equals(view, event, key, expected):
    """Selftest guard: pass when one data key equals the expected value."""
    return view["data"].get(key) == expected


def _selftest_normalizer_rename(event, source_name, target_name):
    """Selftest normalizer: rename one specific event name."""
    if event.get("name") == source_name:
        renamed = deepcopy(event)
        renamed["name"] = target_name
        return renamed
    return event


def _selftest_normalizer_drop(event, drop_name):
    """Selftest normalizer: drop one specific event name."""
    if event.get("name") == drop_name:
        return None
    return event


def _selftest_config_sample():
    """Selftest helper: minimal embedded 'device_state_norm' style dataset."""
    return {
        "meta": [{"schema": "device_state_norm", "version": 5}],
        "process_values": [
            {"value_id": 1, "state_value": "activated"},
            {"value_id": 2, "state_value": "deactivated"},
            {"value_id": 3, "state_value": "error"},
            {"value_id": 4, "state_value": "maintenance"},
        ],
        "process_modes": [
            {"mode_id": 1, "mode": "manual"},
            {"mode_id": 2, "mode": "automatic"},
            {"mode_id": 3, "mode": "AI"},
        ],
        "processes": [
            {"p_id": 1, "process_name": "alpha_process"},
            {"p_id": 2, "process_name": "beta_process"},
        ],
        "site_controller_map": [
            {"local_controller_id": 2, "route_key": "site-main.fn-02.ctrl_1"},
            {"local_controller_id": 1, "route_key": "site-main.fn-01.ctrl_1"},
        ],
    }


def _selftest_build_basic_spec():
    """Selftest helper: one small deterministic spec used by many checks."""
    spec = fsm_create_spec("selftest_machine", "idle", "1.0.0", {"purpose": "selftest"})
    spec = fsm_add_state(spec, "idle", ["mark_idle"], None, None, None)
    spec = fsm_add_state(spec, "running", ["count_runs"], ["mark_left_running"], [
        fsm_timer("run_watchdog", "watchdog_fired", kind="delay", delay=5.0, scope="state", priority=5),
    ], None)
    spec = fsm_add_state(spec, "done", None, None, None, None)
    spec = fsm_register_action(spec, "mark_idle", partial(_selftest_action_patch, key="phase", value="idle"))
    spec = fsm_register_action(spec, "count_runs", partial(_selftest_action_count, key="runs"))
    spec = fsm_register_action(spec, "mark_left_running", partial(_selftest_action_patch, key="left_running", value=True))
    spec = fsm_register_action(spec, "emit_finish", partial(_selftest_action_emit, emit_name="finish"))
    spec = fsm_register_action(spec, "halt_now", _selftest_action_halt)
    spec = fsm_register_action(spec, "count_ticks", partial(_selftest_action_count, key="ticks"))
    spec = fsm_register_action(spec, "record_tick_clock", partial(_selftest_action_record_clock, key="tick_clock"))
    spec = fsm_register_action(spec, "append_H", partial(_selftest_action_append_token, key="order", token="H"))
    spec = fsm_register_action(spec, "append_L", partial(_selftest_action_append_token, key="order", token="L"))
    spec = fsm_register_guard(spec, "is_armed", partial(_selftest_guard_equals, key="armed", expected=True))
    spec = fsm_add_transition(spec, "idle", "start", "running", "is_armed", None, 10, False, None)
    spec = fsm_add_transition(spec, "idle", "start", None, None, None, 0, True, None)
    spec = fsm_add_transition(spec, "running", "stop", "idle", None, None, 0, False, None)
    spec = fsm_add_transition(spec, "running", "auto_finish", None, None, ["emit_finish"], 0, True, None)
    spec = fsm_add_transition(spec, "running", "finish", "done", None, None, 0, False, None)
    spec = fsm_add_transition(spec, "running", "watchdog_fired", "idle", None, None, 0, False, None)
    spec = fsm_add_transition(spec, "*", "emergency", "done", None, ["halt_now"], 100, False, None)
    spec = fsm_add_transition(spec, "*", "tick_event", None, None, ["count_ticks", "record_tick_clock"], 0, True, None)
    spec = fsm_add_transition(spec, "*", "order_hi", None, None, ["append_H"], 0, True, None)
    spec = fsm_add_transition(spec, "*", "order_lo", None, None, ["append_L"], 0, True, None)
    return spec


def _selftest_scenario_fingerprint():
    """Selftest helper: run one fixed mixed scenario, fingerprint the snapshot."""
    spec = _selftest_build_basic_spec()
    machine = fsm_init(spec, {"armed": True})
    machine = fsm_send(machine, "start")
    machine = fsm_schedule(machine, fsm_timer("tick", "tick_event", kind="interval", interval=2.0, repeat=3, scope="machine"))
    machine = fsm_advance_by(machine, 4.9)
    machine = fsm_send(machine, "stop")
    machine = fsm_advance_by(machine, 5.0)
    return json.dumps(fsm_snapshot(machine), sort_keys=True)


def _selftest_check(results, name, ok, detail):
    """Selftest helper: record one check outcome."""
    results.append({"check": str(name), "ok": bool(ok), "detail": str(detail)})
    return bool(ok)


def fsm_selftest(verbose=False):
    """Run the built-in deterministic selftest suite of the library.

    Covers: validation, purity/immutability, guard priorities, emit chains,
    delay/interval/repeat timers, state scoped timer cancellation, timer
    conditions (skip/cancel), simultaneous timer ordering, double run
    determinism, snapshot/restore continuation, spec JSON round trip,
    normalizers (rename/drop), halt/resume and the generic config adapter
    including alternate instance/route mapping.
    Returns {'ok': bool, 'passed': int, 'failed': int, 'checks': list}.
    """
    results = []

    # 01 - spec validation
    try:
        spec = _selftest_build_basic_spec()
        report = fsm_validate_spec(spec)
        _selftest_check(results, "validate_spec", report["ok"] and len(report["errors"]) == 0,
                        "errors=" + str(len(report["errors"])) + " warnings=" + str(len(report["warnings"])))
    except Exception as exc:
        _selftest_check(results, "validate_spec", False, repr(exc))

    # 02 - purity / immutability of inputs
    try:
        spec = _selftest_build_basic_spec()
        origin = fsm_init(spec, {"armed": True})
        before = json.dumps(fsm_snapshot(origin), sort_keys=True)
        moved = fsm_send(origin, "start")
        after = json.dumps(fsm_snapshot(origin), sort_keys=True)
        ok = (before == after) and fsm_get_state(moved) == "running" and fsm_get_state(origin) == "idle"
        _selftest_check(results, "immutability", ok, "origin untouched, copy state=" + str(fsm_get_state(moved)))
    except Exception as exc:
        _selftest_check(results, "immutability", False, repr(exc))

    # 03 - guard evaluation and priority fallback
    try:
        spec = _selftest_build_basic_spec()
        blocked = fsm_send(fsm_init(spec, {"armed": False}), "start")
        blocked_last = fsm_get_history(blocked, 1)[0]
        armed = fsm_send(fsm_init(spec, {"armed": True}), "start")
        ok = (fsm_get_state(blocked) == "idle" and blocked_last.get("type") == "transition"
              and bool(blocked_last.get("internal")) and fsm_get_state(armed) == "running")
        _selftest_check(results, "guard_priority", ok, "blocked=internal-fallback, armed=running")
    except Exception as exc:
        _selftest_check(results, "guard_priority", False, repr(exc))

    # 04 - emit chain processed run-to-completion inside one call
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_send(fsm_init(spec, {"armed": True}), "start")
        machine = fsm_send(machine, "auto_finish")
        snap = fsm_snapshot(machine)
        ok = fsm_get_state(machine) == "done" and snap["step"] == 3 and len(snap["queue"]) == 0
        _selftest_check(results, "emit_chain", ok, "state=" + str(fsm_get_state(machine)) + " step=" + str(snap["step"]))
    except Exception as exc:
        _selftest_check(results, "emit_chain", False, repr(exc))

    # 05 - delay timer fires exactly at its logical due time
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_send(fsm_init(spec, {"armed": True}), "start")
        near = fsm_advance_by(machine, 4.9)
        fired = fsm_advance_by(near, 0.2)
        ok = (fsm_get_state(near) == "running" and fsm_get_state(fired) == "idle"
              and abs(fsm_get_clock(fired) - 5.1) < 1e-9)
        _selftest_check(results, "delay_timer", ok, "4.9=running, 5.1=idle")
    except Exception as exc:
        _selftest_check(results, "delay_timer", False, repr(exc))

    # 06 - state scoped timers are cancelled on state exit
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_send(fsm_init(spec, {"armed": True}), "start")
        machine = fsm_send(machine, "stop")
        machine = fsm_advance_by(machine, 10.0)
        fired_events = []
        for entry in fsm_get_history(machine):
            fired_events.append(entry.get("event"))
        ok = (fsm_get_state(machine) == "idle" and "watchdog_fired" not in fired_events
              and len(fsm_get_active_timers(machine)) == 0
              and fsm_get_data(machine).get("left_running") is True)
        _selftest_check(results, "state_scope_cancel", ok, "watchdog cancelled on exit")
    except Exception as exc:
        _selftest_check(results, "state_scope_cancel", False, repr(exc))

    # 07 - interval timer: repeat limit and drift free schedule
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_init(spec, {"armed": False})
        machine = fsm_schedule(machine, fsm_timer("tick", "tick_event", kind="interval", interval=10.0, repeat=3, scope="machine"))
        machine = fsm_advance_by(machine, 35.0)
        data = fsm_get_data(machine)
        names = []
        for timer in fsm_get_active_timers(machine):
            names.append(timer.get("name"))
        ok = (data.get("ticks") == 3 and abs(float(data.get("tick_clock", -1.0)) - 30.0) < 1e-9
              and "tick" not in names)
        _selftest_check(results, "interval_repeat", ok, "ticks=" + str(data.get("ticks")) + " last_fire=" + str(data.get("tick_clock")))
    except Exception as exc:
        _selftest_check(results, "interval_repeat", False, repr(exc))

    # 08 - timer condition policy 'skip' keeps the interval alive
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_init(spec, {"armed": False})
        machine = fsm_schedule(machine, fsm_timer("cond_tick", "tick_event", kind="interval", interval=5.0, condition="is_armed", condition_policy="skip", scope="machine"))
        machine = fsm_advance_by(machine, 20.0)
        names = []
        for timer in fsm_get_active_timers(machine):
            names.append(timer.get("name"))
        survived = "cond_tick" in names
        silent = fsm_get_data(machine).get("ticks") is None
        machine = fsm_patch_data(machine, {"armed": True})
        machine = fsm_advance_to(machine, 30.0)
        ok = survived and silent and fsm_get_data(machine).get("ticks") == 2
        _selftest_check(results, "condition_skip", ok, "skipped while unarmed, fired twice after arming")
    except Exception as exc:
        _selftest_check(results, "condition_skip", False, repr(exc))

    # 09 - timer condition policy 'cancel' removes the timer
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_init(spec, {"armed": False})
        machine = fsm_schedule(machine, fsm_timer("cond_once", "tick_event", kind="delay", delay=5.0, condition="is_armed", condition_policy="cancel", scope="machine"))
        machine = fsm_advance_by(machine, 10.0)
        ok = fsm_get_data(machine).get("ticks") is None and len(fsm_get_active_timers(machine)) == 0
        _selftest_check(results, "condition_cancel", ok, "timer removed without firing")
    except Exception as exc:
        _selftest_check(results, "condition_cancel", False, repr(exc))

    # 10 - simultaneous timers fire in deterministic priority order
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_init(spec, {"armed": False})
        machine = fsm_schedule(machine, fsm_timer("lo", "order_lo", kind="delay", delay=5.0, priority=0, scope="machine"))
        machine = fsm_schedule(machine, fsm_timer("hi", "order_hi", kind="delay", delay=5.0, priority=10, scope="machine"))
        machine = fsm_advance_by(machine, 5.0)
        ok = fsm_get_data(machine).get("order") == "HL"
        _selftest_check(results, "timer_order", ok, "order=" + str(fsm_get_data(machine).get("order")))
    except Exception as exc:
        _selftest_check(results, "timer_order", False, repr(exc))

    # 11 - full double run determinism (byte identical snapshots)
    try:
        first = _selftest_scenario_fingerprint()
        second = _selftest_scenario_fingerprint()
        _selftest_check(results, "determinism_double_run", first == second, "fingerprints equal=" + str(first == second))
    except Exception as exc:
        _selftest_check(results, "determinism_double_run", False, repr(exc))

    # 12 - snapshot / restore continuation equals uninterrupted run
    try:
        spec = _selftest_build_basic_spec()
        straight = fsm_send(fsm_init(spec, {"armed": True}), "start")
        straight = fsm_advance_by(straight, 2.0)
        parted = fsm_restore(spec, fsm_snapshot(straight))
        straight = fsm_advance_by(straight, 10.0)
        parted = fsm_advance_by(parted, 10.0)
        compare_keys = ("state", "data", "clock", "step", "halted", "timers", "queue", "seq")
        left_snap = fsm_snapshot(straight)
        right_snap = fsm_snapshot(parted)
        left = {}
        right = {}
        for key in compare_keys:
            left[key] = left_snap.get(key)
            right[key] = right_snap.get(key)
        ok = json.dumps(left, sort_keys=True) == json.dumps(right, sort_keys=True) and fsm_get_state(parted) == "idle"
        _selftest_check(results, "snapshot_restore", ok, "continuation equivalent, state=" + str(fsm_get_state(parted)))
    except Exception as exc:
        _selftest_check(results, "snapshot_restore", False, repr(exc))

    # 13 - spec JSON round trip with re-attached registries
    try:
        spec = _selftest_build_basic_spec()
        text = fsm_spec_to_json(spec)
        registry = spec.get("registry", {})
        clone = fsm_spec_from_json(text, registry.get("actions"), registry.get("guards"), registry.get("normalizers"))
        report = fsm_validate_spec(clone)
        machine = fsm_send(fsm_init(clone, {"armed": True}), "start")
        same_shape = json.dumps(fsm_describe(spec), sort_keys=True) == json.dumps(fsm_describe(clone), sort_keys=True)
        ok = report["ok"] and same_shape and fsm_get_state(machine) == "running"
        _selftest_check(results, "spec_json_roundtrip", ok, "validated and behaviour preserved")
    except Exception as exc:
        _selftest_check(results, "spec_json_roundtrip", False, repr(exc))

    # 14 - normalizers: rename and drop
    try:
        spec = _selftest_build_basic_spec()
        spec = fsm_register_normalizer(spec, "rename_raw", partial(_selftest_normalizer_rename, source_name="raw_start", target_name="start"))
        spec = fsm_register_normalizer(spec, "drop_noise", partial(_selftest_normalizer_drop, drop_name="noise"))
        machine = fsm_send(fsm_init(spec, {"armed": True}), "raw_start")
        renamed_ok = fsm_get_state(machine) == "running"
        machine = fsm_send(machine, "noise")
        last = fsm_get_history(machine, 1)[0]
        ok = renamed_ok and last.get("type") == "dropped" and fsm_get_state(machine) == "running"
        _selftest_check(results, "normalizers", ok, "rename applied, noise dropped")
    except Exception as exc:
        _selftest_check(results, "normalizers", False, repr(exc))

    # 15 - halt via action result, ignore while halted, resume
    try:
        spec = _selftest_build_basic_spec()
        machine = fsm_send(fsm_init(spec, {"armed": True}), "start")
        machine = fsm_send(machine, "emergency")
        halted_state = fsm_get_state(machine)
        halted_flag = fsm_is_halted(machine)
        ignored = fsm_send(machine, "start")
        resumed = fsm_resume(machine)
        ok = (halted_state == "done" and halted_flag and fsm_get_state(ignored) == "done"
              and fsm_is_halted(ignored) and not fsm_is_halted(resumed))
        _selftest_check(results, "halt_resume", ok, "halted in done, send ignored, resume ok")
    except Exception as exc:
        _selftest_check(results, "halt_resume", False, repr(exc))

    # 16 - generic config adapter: full lifecycle on sample dataset
    try:
        sample = _selftest_config_sample()
        machines = fsm_machines_from_config(sample)
        order_ok = list(machines.keys()) == ["alpha_process", "beta_process"]
        machine = machines["alpha_process"]
        step_checks = []
        step_checks.append(fsm_get_state(machine) == "deactivated")
        machine = fsm_send(machine, "activate")
        step_checks.append(fsm_get_state(machine) == "activated")
        machine = fsm_send(machine, "set_mode", {"mode": "turbo"})
        step_checks.append(fsm_get_data(machine).get("mode") == "manual")
        step_checks.append(fsm_get_history(machine, 1)[0].get("type") == "unhandled")
        machine = fsm_send(machine, "set_mode", {"mode": "automatic"})
        step_checks.append(fsm_get_data(machine).get("mode") == "automatic")
        step_checks.append(fsm_get_state(machine) == "activated")
        machine = fsm_send(machine, "fault", {"code": 42})
        step_checks.append(fsm_get_state(machine) == "error")
        step_checks.append(fsm_get_data(machine).get("last_error", {}).get("payload", {}).get("code") == 42)
        machine = fsm_send(machine, "recover")
        step_checks.append(fsm_get_state(machine) == "deactivated")
        machine = fsm_send(machine, "set_state:maintenance")
        step_checks.append(fsm_get_state(machine) == "maintenance")
        machine = fsm_send(machine, "maintenance_end")
        step_checks.append(fsm_get_state(machine) == "deactivated")
        ok = order_ok and all(step_checks)
        _selftest_check(results, "config_adapter", ok, "lifecycle steps ok=" + str(sum(step_checks)) + "/" + str(len(step_checks)))
    except Exception as exc:
        _selftest_check(results, "config_adapter", False, repr(exc))

    # 17 - alternate mapping: one machine per controller route (multi worker)
    try:
        sample = _selftest_config_sample()
        routes = fsm_machines_from_config(sample, None, {
            "instances_table": "site_controller_map",
            "instance_name_key": "route_key",
            "instance_order_key": "local_controller_id",
        })
        keys = list(routes.keys())
        ok = (keys == ["site-main.fn-01.ctrl_1", "site-main.fn-02.ctrl_1"]
              and fsm_get_state(routes[keys[0]]) == "deactivated")
        _selftest_check(results, "route_mapping", ok, "routes=" + str(keys))
    except Exception as exc:
        _selftest_check(results, "route_mapping", False, repr(exc))

    passed = 0
    failed = 0
    for entry in results:
        if entry["ok"]:
            passed = passed + 1
        else:
            failed = failed + 1
        if verbose:
            marker = "PASS" if entry["ok"] else "FAIL"
            print("[" + marker + "] " + entry["check"] + " :: " + entry["detail"])
    if verbose:
        print("selftest result: " + str(passed) + " passed, " + str(failed) + " failed")
    return {"ok": failed == 0, "passed": passed, "failed": failed, "checks": results}


def fsm_demo():
    """Run one small deterministic traffic light demo and return a summary.

    Three states with one state scoped delay timer each: red (30s), green
    (25s), yellow (5s). Advancing the logical clock to 120 cycles the light
    exactly twice and ends on red - fully reproducible on every run.
    """
    spec = fsm_create_spec("demo_traffic_light", "red", "1.0.0", {"purpose": "demo"})
    spec = fsm_add_state(spec, "red", None, None, [fsm_timer("red_phase", "go_green", kind="delay", delay=30.0)], None)
    spec = fsm_add_state(spec, "green", None, None, [fsm_timer("green_phase", "go_yellow", kind="delay", delay=25.0)], None)
    spec = fsm_add_state(spec, "yellow", None, None, [fsm_timer("yellow_phase", "go_red", kind="delay", delay=5.0)], None)
    spec = fsm_add_transition(spec, "red", "go_green", "green")
    spec = fsm_add_transition(spec, "green", "go_yellow", "yellow")
    spec = fsm_add_transition(spec, "yellow", "go_red", "red")
    machine = fsm_init(spec)
    machine = fsm_advance_to(machine, 120.0)
    switches = []
    for entry in fsm_get_history(machine):
        if entry.get("type") == "transition":
            switches.append(str(entry.get("clock")) + " -> " + str(entry.get("target")))
    return {
        "state": fsm_get_state(machine),
        "clock": fsm_get_clock(machine),
        "switch_count": len(switches),
        "switches": switches,
    }


# =============================================================================
# INTERFACE - EXTERNAL CALLABLE
# =============================================================================
# Central dispatch registry: enables fully dynamic, name based access from
# higher layers (rule engines, controller threads, config driven wiring)
# without importing individual symbols - one dict, every capability.

FSM_INTERFACE = {
    "create_spec": fsm_create_spec,
    "add_state": fsm_add_state,
    "add_transition": fsm_add_transition,
    "register_action": fsm_register_action,
    "register_guard": fsm_register_guard,
    "register_normalizer": fsm_register_normalizer,
    "set_log_function": fsm_set_log_function,
    "event": fsm_event,
    "timer": fsm_timer,
    "result": fsm_result,
    "validate_spec": fsm_validate_spec,
    "init": fsm_init,
    "send": fsm_send,
    "post": fsm_post,
    "process": fsm_process,
    "advance_by": fsm_advance_by,
    "advance_to": fsm_advance_to,
    "schedule": fsm_schedule,
    "cancel": fsm_cancel,
    "halt": fsm_halt,
    "resume": fsm_resume,
    "patch_data": fsm_patch_data,
    "replace_data": fsm_replace_data,
    "get_state": fsm_get_state,
    "get_data": fsm_get_data,
    "get_clock": fsm_get_clock,
    "is_halted": fsm_is_halted,
    "get_pending_events": fsm_get_pending_events,
    "get_active_timers": fsm_get_active_timers,
    "get_history": fsm_get_history,
    "get_trace": fsm_get_trace,
    "snapshot": fsm_snapshot,
    "restore": fsm_restore,
    "spec_to_json": fsm_spec_to_json,
    "spec_from_json": fsm_spec_from_json,
    "describe": fsm_describe,
    "config_default_mapping": fsm_config_default_mapping,
    "spec_from_config": fsm_spec_from_config,
    "machines_from_config": fsm_machines_from_config,
    "selftest": fsm_selftest,
    "demo": fsm_demo,
}


if __name__ == "__main__":
    _selftest_report = fsm_selftest(True)
    _demo_report = fsm_demo()
    print(json.dumps({
        "selftest": {
            "ok": _selftest_report["ok"],
            "passed": _selftest_report["passed"],
            "failed": _selftest_report["failed"],
        },
        "demo": _demo_report,
    }, indent=2))
