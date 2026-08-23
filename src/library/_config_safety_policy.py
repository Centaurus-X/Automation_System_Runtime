# -*- coding: utf-8 -*-

# src/library/_config_safety_policy.py


"""
 =============================================================================
 CONFIG_SAFETY_POLICY
 =============================================================================

 Optional, functional safety gates for RuleEngine action plans, external
 configuration commits and actuator tasks. The module never mutates runtime
 data. It validates detached snapshots immediately before the owning boundary
 (SEM or Thread Management) performs the operation.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import threading
import time

from src.library._generic_system_contract_library import config_sharding_contracts


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

CONTEXT = {
    "library_name": "config_safety_policy",
    "library_version": "1.0.0",
    "options": {
        "enabled": False,
        "dedupe_limit": 2048,
    },
}


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _safe_bool(value, default):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
    return bool(default)


def _safe_int(value, default, minimum=0):
    try:
        result = int(value)
    except Exception:
        return int(default)
    return result if result >= minimum else int(default)


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _result(ok, code, message, **kwargs):
    result = {"ok": bool(ok), "code": str(code), "message": str(message)}
    result.update(copy.deepcopy(kwargs))
    return result


def _gate_options(context, name):
    options = context.get("options") or {}
    gate = options.get(name)
    return gate if _is_dict(gate) else {}


def safety_gate_enabled(context, name):
    options = context.get("options") or {}
    if not _safe_bool(options.get("enabled"), False):
        return False
    gate = _gate_options(context, name)
    return _safe_bool(gate.get("enabled"), True)


def _path_tokens(path):
    if isinstance(path, (list, tuple)):
        return [str(item) for item in path]
    text = str(path or "").replace("/", ".")
    for character in "[]":
        text = text.replace(character, ".")
    return [part for part in text.split(".") if part]


def _read_path(data, path):
    current = data
    for token in _path_tokens(path):
        if _is_dict(current):
            if token not in current:
                return False, None
            current = current[token]
            continue
        if isinstance(current, list) and token.lstrip("+-").isdigit():
            index = int(token)
            if -len(current) <= index < len(current):
                current = current[index]
                continue
        return False, None
    return True, copy.deepcopy(current)


# =============================================================================
# CONTEXT, DEDUPLICATION AND DIAGNOSTICS
# =============================================================================

def create_safety_context(options=None, **kwargs):
    supplied = copy.deepcopy(options) if _is_dict(options) else {}
    supplied.setdefault("enabled", CONTEXT["options"]["enabled"])
    supplied.setdefault("dedupe_limit", CONTEXT["options"]["dedupe_limit"])
    return {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "options": supplied,
        "lock": threading.RLock(),
        "sensor_reader": kwargs.get("sensor_reader"),
        "dedupe": {},
        "dedupe_order": [],
        "statistics": {
            "allowed": 0,
            "rejected": 0,
            "duplicates": 0,
        },
        "last_rejection": None,
    }


def lookup_safety_result(context, operation_id):
    identity = str(operation_id or "")
    if not identity:
        return None
    with context["lock"]:
        result = context["dedupe"].get(identity)
        if result is not None:
            context["statistics"]["duplicates"] += 1
        return copy.deepcopy(result)


def remember_safety_result(context, operation_id, result):
    identity = str(operation_id or "")
    if not identity:
        return False
    limit = _safe_int(
        context["options"].get("dedupe_limit"),
        CONTEXT["options"]["dedupe_limit"],
        1,
    )
    with context["lock"]:
        if identity not in context["dedupe"]:
            context["dedupe_order"].append(identity)
        context["dedupe"][identity] = copy.deepcopy(result)
        while len(context["dedupe_order"]) > limit:
            expired = context["dedupe_order"].pop(0)
            context["dedupe"].pop(expired, None)
    return True


def _record(context, result):
    with context["lock"]:
        key = "allowed" if result.get("ok") else "rejected"
        context["statistics"][key] += 1
        if not result.get("ok"):
            context["last_rejection"] = copy.deepcopy(result)
    return result


def get_safety_snapshot(context):
    with context["lock"]:
        return {
            "module": context["module"],
            "version": context["version"],
            "enabled": _safe_bool(context["options"].get("enabled"), False),
            "gates": {
                name: safety_gate_enabled(context, name)
                for name in ("rule_actions", "external_commits", "actuator_tasks")
            },
            "dedupe_entries": len(context["dedupe"]),
            "statistics": dict(context["statistics"]),
            "last_rejection": copy.deepcopy(context["last_rejection"]),
        }


# =============================================================================
# REVISION AND EXTERNAL COMMIT GATE
# =============================================================================

def validate_base_revision(context, gate_name, current_revision, supplied):
    if not safety_gate_enabled(context, gate_name):
        return _result(True, "gate_disabled", "safety gate is disabled")
    gate = _gate_options(context, gate_name)
    required = _safe_bool(gate.get("require_base_revision"), True)
    if supplied is None and required:
        return _result(False, "base_revision_required", "base_revision is required")
    if supplied is None:
        return _result(True, "revision_optional", "revision check is optional")
    if _safe_int(supplied, -1) != _safe_int(current_revision, -2):
        return _result(False, "stale_revision", "configuration revision is stale")
    return _result(True, "revision_current", "configuration revision is current")


def _prefix_matches(path, prefix):
    path_tokens = _path_tokens(path)
    prefix_tokens = _path_tokens(prefix)
    if not prefix_tokens or len(prefix_tokens) > len(path_tokens):
        return False
    return path_tokens[:len(prefix_tokens)] == prefix_tokens


def _external_path_allowed(gate, path):
    for prefix in gate.get("denied_path_prefixes") or []:
        if _prefix_matches(path, prefix):
            return False
    if _safe_bool(gate.get("allow_arbitrary_paths"), False):
        return True
    return any(
        _prefix_matches(path, prefix)
        for prefix in gate.get("allowed_path_prefixes") or []
    )


def authorize_external_commit(context, kind, payload, current_revision):
    """Authorize external changes fail-closed through the configured gate."""
    if not safety_gate_enabled(context, "external_commits"):
        return _record(
            context,
            _result(False, "principal_denied", "external commits are disabled"),
        )
    gate = _gate_options(context, "external_commits")
    operation = str(kind or "").lower()
    allowed = [str(item).lower() for item in gate.get("allowed_operations") or []]
    if operation not in allowed:
        return _record(context, _result(False, "operation_denied", operation))
    principal = str(payload.get("principal") or "")
    principals = [str(item) for item in gate.get("allowed_principals") or []]
    if not principal or (principals and principal not in principals):
        return _record(context, _result(False, "principal_denied", principal))
    revision = validate_base_revision(
        context, "external_commits", current_revision, payload.get("base_revision"))
    if not revision.get("ok"):
        return _record(context, revision)
    if operation == "scope_operations":
        try:
            operations = config_sharding_contracts.validate_scope_operations(
                payload.get("operations"),
                operation_format=payload.get("operation_format"),
            )
        except (TypeError, ValueError) as exc:
            return _record(
                context,
                _result(False, "unsupported_operation_format", str(exc)),
            )
        for item in operations:
            section = str(item.get("section") or "")
            path = section + str(item.get("json_pointer") or "")
            if not _external_path_allowed(gate, path):
                return _record(
                    context,
                    _result(False, "path_denied", path),
                )
        return _record(
            context,
            _result(True, "external_commit_allowed", "scope operations allowed"),
        )
    if operation == "replace":
        return _record(context, _result(True, "replace_allowed", "replace allowed"))
    mode = str(payload.get("mode") or "set").lower()
    modes = [str(item).lower() for item in gate.get("allowed_patch_modes") or []]
    if mode not in modes:
        return _record(context, _result(False, "patch_mode_denied", mode))
    if not _external_path_allowed(gate, payload.get("path")):
        return _record(context, _result(False, "path_denied", str(payload.get("path"))))
    return _record(context, _result(True, "external_commit_allowed", "commit allowed"))


# =============================================================================
# RULE ACTION PLAN GATE
# =============================================================================

def _engine_config(snapshot, item):
    source = snapshot if _is_dict(snapshot) else {}
    requested = item.get("engine_id") if _is_dict(item) else None
    if requested is None and _is_dict(item):
        rule_id = item.get("rule_id")
        for rule in source.get("rules") or []:
            if _is_dict(rule) and str(rule.get("rule_id")) == str(rule_id):
                requested = rule.get("engine_id")
                break
    rows = source.get("rule_engine")
    rows = rows if isinstance(rows, list) else []
    for row in rows:
        if not _is_dict(row):
            continue
        if requested is not None and str(row.get("engine_id")) != str(requested):
            continue
        if row.get("enabled", True):
            return row
    return {}


def _matching_states(snapshot, action):
    rows = snapshot.get("process_states") if _is_dict(snapshot) else None
    rows = rows if isinstance(rows, list) else []
    state_ids = {str(item) for item in action.get("state_ids") or []}
    process_ids = {str(item) for item in action.get("p_ids") or []}
    return [
        row for row in rows if _is_dict(row) and (
            (state_ids and str(row.get("state_id")) in state_ids)
            or (process_ids and str(row.get("p_id")) in process_ids)
        )
    ]


def _valid_value_id(snapshot, value_id):
    rows = snapshot.get("process_values") if _is_dict(snapshot) else None
    return any(
        _is_dict(row) and str(row.get("value_id")) == str(value_id)
        for row in (rows if isinstance(rows, list) else [])
    )


def _limit_valid(value, specification):
    if not _is_dict(specification):
        return True
    if specification.get("allowed") is not None:
        if value not in specification.get("allowed"):
            return False
    if specification.get("min") is not None:
        if not _number(value) or value < specification.get("min"):
            return False
    if specification.get("max") is not None:
        if not _number(value) or value > specification.get("max"):
            return False
    return True


def _parameters_valid(gate, parameters):
    if not _is_dict(parameters):
        return False, "parameters must be a dict"
    limits = gate.get("parameter_limits") or {}
    require_limits = _safe_bool(gate.get("require_parameter_limits"), False)
    for key, value in parameters.items():
        specification = limits.get(key) if _is_dict(limits) else None
        if require_limits and not _is_dict(specification):
            return False, "no configured limit for parameter " + str(key)
        if not _limit_valid(value, specification):
            return False, "parameter outside safety limit: " + str(key)
    return True, ""


def _action_fields_valid(gate, engine, action):
    controls = {"state_ids", "p_ids", "notify"}
    mutation_fields = [key for key in action if key not in controls]
    configured = engine.get("writable_fields")
    allowed = configured if isinstance(configured, list) else []
    if _safe_bool(gate.get("enforce_writable_fields"), True):
        denied = [field for field in mutation_fields if field not in allowed]
        if denied:
            return False, "writable_fields", (
                "non-writable action fields: " + ",".join(sorted(denied)))
    if "parameters" in action:
        valid, message = _parameters_valid(gate, action.get("parameters"))
        return valid, "parameter_limit", message
    return True, "action_fields", ""


def _action_modes_valid(gate, engine, rows):
    if not _safe_bool(gate.get("enforce_required_mode"), True):
        return True
    required = engine.get("required_mode_id")
    if required is None:
        return True
    return all(str(row.get("mode_id")) == str(required) for row in rows)


def _compare_ordered(actual, name, expected):
    try:
        if name in ("lt", "<"):
            return actual < expected
        if name in ("lte", "<="):
            return actual <= expected
        if name in ("gt", ">"):
            return actual > expected
        if name in ("gte", ">="):
            return actual >= expected
    except Exception:
        return False
    return False


def _compare(actual, operator, expected):
    name = str(operator or "eq").lower()
    if name in ("eq", "=="):
        return actual == expected
    if name in ("ne", "!="):
        return actual != expected
    if name == "in":
        return actual in expected
    if name == "not_in":
        return actual not in expected
    if name == "truthy":
        return bool(actual)
    if name == "falsy":
        return not bool(actual)
    return _compare_ordered(actual, name, expected)


def _state_condition(snapshot, condition):
    state_id = str(condition.get("state_id"))
    for row in snapshot.get("process_states") or []:
        if _is_dict(row) and str(row.get("state_id")) == state_id:
            return _read_path(row, condition.get("path") or condition.get("field"))
    return False, None


def _sensor_condition(context, condition):
    reader = context.get("sensor_reader")
    if not callable(reader):
        return False, None
    entry = reader(condition.get("controller_uuid"), condition.get("sensor_id"))
    if not _is_dict(entry):
        return False, None
    return _read_path(entry, condition.get("path") or condition.get("field") or "value")


def _condition_value(context, snapshot, condition):
    source = str(condition.get("source") or "config").lower()
    if source == "state":
        return _state_condition(snapshot, condition)
    if source == "sensor":
        return _sensor_condition(context, condition)
    return _read_path(snapshot, condition.get("path"))


def _condition_passes(context, snapshot, condition):
    found, actual = _condition_value(context, snapshot, condition)
    if not found:
        return False
    return _compare(actual, condition.get("operator"), condition.get("value"))


def _interlock_applies(interlock, action, rows):
    applies = interlock.get("applies_to") or {}
    if not _is_dict(applies) or not applies:
        return True
    action_states = {str(item) for item in action.get("state_ids") or []}
    row_processes = {str(row.get("p_id")) for row in rows}
    configured_states = {str(item) for item in applies.get("state_ids") or []}
    configured_processes = {str(item) for item in applies.get("p_ids") or []}
    if configured_states and action_states & configured_states:
        return True
    if configured_processes and row_processes & configured_processes:
        return True
    return not configured_states and not configured_processes


def _interlocks_valid(context, gate, snapshot, action, rows):
    for interlock in gate.get("interlocks") or []:
        if not _is_dict(interlock) or not _safe_bool(interlock.get("enabled"), True):
            continue
        if not _interlock_applies(interlock, action, rows):
            continue
        conditions = interlock.get("conditions") or []
        passed = [_condition_passes(context, snapshot, item)
                  for item in conditions if _is_dict(item)]
        match = str(interlock.get("match") or "all").lower()
        allowed = any(passed) if match == "any" else bool(passed) and all(passed)
        if not allowed:
            return False, str(interlock.get("name") or "unnamed_interlock")
    return True, ""


def _validate_action(context, gate, snapshot, item):
    action = item.get("action") if _is_dict(item.get("action")) else item
    if not _is_dict(action):
        return _result(False, "invalid_action", "action must be a dict")
    engine = _engine_config(snapshot, item)
    if not engine:
        return _result(False, "engine_missing", "enabled RuleEngine config missing")
    fields_ok, field_code, field_error = _action_fields_valid(gate, engine, action)
    if not fields_ok:
        return _result(False, field_code, field_error)
    mutation = "value_id" in action or "parameters" in action
    rows = _matching_states(snapshot, action)
    if mutation and not rows:
        return _result(False, "target_missing", "action has no matching state")
    if mutation and not _action_modes_valid(gate, engine, rows):
        return _result(False, "required_mode", "action target is in a forbidden mode")
    if "value_id" in action and not _valid_value_id(snapshot, action.get("value_id")):
        return _result(False, "value_id", "action references an unknown value_id")
    interlocks_ok, name = _interlocks_valid(context, gate, snapshot, action, rows)
    if not interlocks_ok:
        return _result(False, "interlock", "interlock blocked action: " + name)
    return _result(True, "action_allowed", "action allowed")


def prepare_rule_action_plan(context, snapshot, actions, **kwargs):
    if not safety_gate_enabled(context, "rule_actions"):
        return _record(context, _result(
            True, "gate_disabled", "rule action gate disabled",
            actions=copy.deepcopy(actions or []),
        ))
    gate = _gate_options(context, "rule_actions")
    plan = [item for item in (actions or []) if _is_dict(item)]
    maximum = _safe_int(gate.get("max_actions"), 128, 1)
    if len(plan) > maximum:
        return _record(context, _result(False, "plan_size", "action plan is too large"))
    revision = validate_base_revision(
        context, "rule_actions", kwargs.get("current_revision"),
        kwargs.get("base_revision"),
    )
    if not revision.get("ok"):
        return _record(context, revision)
    for item in sorted(plan, key=_sequence):
        validation = _validate_action(context, gate, snapshot, item)
        if not validation.get("ok"):
            return _record(context, validation)
    return _record(context, _result(
        True, "plan_allowed", "action plan allowed", actions=copy.deepcopy(plan)))


def _sequence(item):
    return _safe_int(item.get("seq"), 0)


# =============================================================================
# ACTUATOR TASK GATE
# =============================================================================

def _task_limit_matches(limit, task):
    for key in ("controller_uuid", "actuator_uuid", "actuator_id"):
        if limit.get(key) is not None and str(limit.get(key)) != str(task.get(key)):
            return False
    return True


def _task_value_allowed(gate, task):
    value = task.get("control_value")
    for limit in gate.get("limits") or []:
        if _is_dict(limit) and _task_limit_matches(limit, task):
            return _limit_valid(value, limit)
    return not _safe_bool(gate.get("require_limits"), False)


def evaluate_actuator_task(context, snapshot, task, **kwargs):
    if not safety_gate_enabled(context, "actuator_tasks"):
        return _record(context, _result(True, "gate_disabled", "actuator gate disabled"))
    gate = _gate_options(context, "actuator_tasks")
    if not _is_dict(task) or not str(task.get("task_uuid") or ""):
        return _record(context, _result(False, "task_identity", "task_uuid is required"))
    if _safe_bool(gate.get("require_config_revision"), True):
        metadata = task.get("meta") if _is_dict(task.get("meta")) else {}
        supplied = task.get("config_revision", metadata.get("config_revision"))
        current = kwargs.get("current_revision")
        if supplied is None or str(supplied) != str(current):
            return _record(context, _result(
                False, "stale_revision", "actuator task revision is stale"))
    if not _task_value_allowed(gate, task):
        return _record(context, _result(False, "task_limit", "control value is unsafe"))
    action = {
        "state_ids": (task.get("meta") or {}).get("state_ids") or [],
        "p_ids": (task.get("meta") or {}).get("p_ids") or [],
    }
    valid, name = _interlocks_valid(context, gate, snapshot, action, [])
    if not valid:
        return _record(context, _result(False, "interlock", "interlock blocked task: " + name))
    return _record(context, _result(True, "task_allowed", "actuator task allowed"))


