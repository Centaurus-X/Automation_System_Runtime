# -*- coding: utf-8 -*-

# src/core/state_event_management.py


"""
 =============================================================================
 STATE_EVENT_MANAGEMENT
 =============================================================================

 What it is:
     The runtime single-writer boundary for configuration data. It accepts
     JSON-compatible commands, commits them through Generic Data Center and
     completes the downstream rollout before acknowledging the caller.

 Why it is needed:
     A shared configuration dict cannot be updated safely by controller,
     automation and API threads. One explicit writer preserves revision order,
     atomic commits and deterministic worker synchronisation.

 How it works:
     1. Producers enqueue a CONFIG command and optionally register a waiter.
     2. SEM snapshots the old heap and performs exactly one central commit.
     3. Changed UUID shards are rebuilt and passed to the registered rollout
        handler synchronously.
     4. Only after rollout returns is the request acknowledged.
     5. Persistence is coalesced on a separate thread outside the commit path.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import logging
import threading
import time
import uuid

from functools import partial
from queue import Empty, Queue

from src.library import _generic_data_center as data_center
from src.library import _generic_type_library as config_compatibility
from src.library._generic_system_contract_library import config_sharding_contracts
from src.library._config_safety_policy import (
    authorize_external_commit,
    create_safety_context,
    get_safety_snapshot,
    lookup_safety_result,
    prepare_rule_action_plan,
    remember_safety_result,
)
from src.library._event_interface import make_event, sync_queue_get, sync_queue_put
from src.library._state_shard_manager import (
    build_controller_projection,
    get_node_controller_uuids,
)


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "state_event_management",
    "library_version": "3.0.0",
    "description": "Synchronous central configuration writer",
    "options": {
        "thread_name": "StateEventManagement",
        "ingress_timeout_s": 0.25,
        "request_timeout_s": 10.0,
        "join_timeout_s": 5.0,
        "persistence_enabled": True,
        "persistence_coalesce_s": 0.2,
        "persist_json": True,
        "persist_sqlite": False,
    },
}

CONFIG_PATCH = "CONFIG_PATCH"
CONFIG_REPLACE = "CONFIG_REPLACE"
CONFIG_SCOPE_OPERATIONS = "CONFIG_SCOPE_OPERATIONS"
CONFIG_ACTION_PLAN = "CONFIG_ACTION_PLAN"
CONFIG_SYNC_REQUEST = "CONFIG_SYNC_REQUEST"
INITIAL_FULL_SYNC = "INITIAL_FULL_SYNC"
STOP_EVENT = "STATE_EVENT_MANAGEMENT_STOP"


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
    if result <= 0.0:
        return float(default)
    return result


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


def _event_type(event):
    if not _is_dict(event):
        return ""
    return str(event.get("event_type") or event.get("type") or "").upper()


def _payload(event):
    if not _is_dict(event):
        return {}
    payload = event.get("payload")
    return payload if _is_dict(payload) else {}


def _make_stop_event():
    return make_event(
        STOP_EVENT,
        payload={"reason": "component_shutdown"},
        source="state_event_management",
        target="state_event_management",
        _type="__STOP__",
        _sentinel=True,
    )


def _is_stop_event(event):
    if not _is_dict(event):
        return False
    if event.get("_type") == "__STOP__" and event.get("_sentinel") is True:
        return True
    return _event_type(event) in (STOP_EVENT, "__STOP__")


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _options_from(kwargs):
    defaults = CONTEXT["options"]
    options = dict(defaults)
    supplied = kwargs.get("options")
    if _is_dict(supplied):
        options.update(supplied)
    for key in defaults:
        if key in kwargs:
            options[key] = kwargs[key]
    options["ingress_timeout_s"] = _safe_float(
        options.get("ingress_timeout_s"), defaults["ingress_timeout_s"])
    options["request_timeout_s"] = _safe_float(
        options.get("request_timeout_s"), defaults["request_timeout_s"])
    options["join_timeout_s"] = _safe_float(
        options.get("join_timeout_s"), defaults["join_timeout_s"])
    options["persistence_coalesce_s"] = _safe_float(
        options.get("persistence_coalesce_s"),
        defaults["persistence_coalesce_s"],
    )
    for key in ("persistence_enabled", "persist_json", "persist_sqlite"):
        options[key] = _safe_bool(options.get(key), defaults[key])
    return options


# =============================================================================
# CONTEXT AND CAPABILITIES
# =============================================================================

def create_sem_context(**kwargs):
    center = kwargs.get("data_center")
    if not _is_dict(center):
        raise TypeError("data_center must be a Generic Data Center context")
    stop_event = kwargs.get("stop_event") or threading.Event()
    queues = kwargs.get("queues") if _is_dict(kwargs.get("queues")) else {}
    options = _options_from(kwargs)
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "state": "created",
        "lock": threading.RLock(),
        "writer_lock": threading.Lock(),
        "stop_event": stop_event,
        "system_shutdown_event": kwargs.get("system_shutdown_event"),
        "options": options,
        "data_center": center,
        "queue_event_state": queues.get("queue_event_state"),
        "queue_event_send": queues.get("queue_event_send"),
        "thread": None,
        "persistence_thread": None,
        "persistence_queue": Queue(),
        "rollout_handler": kwargs.get("rollout_handler"),
        "post_commit_handler": kwargs.get("post_commit_handler"),
        "write_authorizer": kwargs.get("write_authorizer"),
        "external_publisher": kwargs.get("external_publisher"),
        "safety": create_safety_context(
            options.get("safety"),
            sensor_reader=kwargs.get("sensor_value_reader"),
        ),
        "waiters": {},
        "statistics": {
            "events_received": 0,
            "commits_ok": 0,
            "commits_error": 0,
            "rollouts_ok": 0,
            "rollouts_error": 0,
            "persistence_runs": 0,
            "persistence_errors": 0,
            "external_events_published": 0,
            "external_events_rejected": 0,
        },
        "last_result": None,
        "last_error": None,
    }
    if context["queue_event_state"] is None:
        raise ValueError("queue_event_state is required")
    return context


def register_rollout_handler(context, handler):
    if handler is not None and not callable(handler):
        raise TypeError("rollout handler must be callable")
    with context["lock"]:
        context["rollout_handler"] = handler
    return True


def register_external_publisher(context, publisher):
    if publisher is not None and not callable(publisher):
        raise TypeError("external publisher must be callable")
    with context["lock"]:
        context["external_publisher"] = publisher
    return True


def register_post_commit_handler(context, handler):
    if handler is not None and not callable(handler):
        raise TypeError("post commit handler must be callable")
    with context["lock"]:
        context["post_commit_handler"] = handler
    return True


def register_write_authorizer(context, handler):
    if handler is not None and not callable(handler):
        raise TypeError("write authorizer must be callable")
    with context["lock"]:
        context["write_authorizer"] = handler
    return True


def create_config_capabilities(context):
    return {
        "request_patch": partial(request_config_patch, context),
        "request_replace": partial(request_config_replace, context),
        "request_scope_operations": partial(
            request_config_scope_operations, context),
        "request_action_plan": partial(request_config_action_plan, context),
        "request_sync": partial(request_config_sync, context),
        "get_snapshot": partial(_get_heap_snapshot, context),
        "get_revision": partial(data_center.get_revision, context["data_center"]),
        "get_fingerprint": partial(data_center.get_fingerprint, context["data_center"]),
        "persist_json": partial(
            data_center.save_heap_to_json, context["data_center"]),
        "get_shard": partial(data_center.get_shard_snapshot, context["data_center"]),
        "get_safety_snapshot": partial(get_safety_snapshot, context["safety"]),
        "authorize_external_scope": partial(
            authorize_external_scope_operations, context),
    }


def _get_heap_snapshot(context):
    found, snapshot = data_center.heap_snapshot(context["data_center"])
    return snapshot if found else {}


def authorize_external_scope_operations(context, operations, **kwargs):
    """Run the identical SEM external gate for a detached owner scope."""
    payload = {
        "operations": copy.deepcopy(operations or []),
        "operation_format": kwargs.get(
            "operation_format", config_sharding_contracts.OPERATION_FORMAT),
        "principal": kwargs.get("principal"),
        "base_revision": kwargs.get("base_revision"),
    }
    return authorize_external_commit(
        context["safety"],
        "scope_operations",
        payload,
        kwargs.get("current_revision"),
    )


# =============================================================================
# CONFIG DIFF AND ACTION APPLICATION
# =============================================================================

def _controller_map(snapshot):
    result = {}
    for controller_uuid in get_node_controller_uuids(snapshot, None):
        result[controller_uuid] = build_controller_projection(
            snapshot, controller_uuid)
    return result


def _affected_controllers(before, after):
    before_map = _controller_map(before)
    after_map = _controller_map(after)
    identities = sorted(set(before_map) | set(after_map))
    return tuple(
        identity for identity in identities
        if before_map.get(identity) != after_map.get(identity)
    )


def _matching_state_rows(config, action):
    states = config.get("process_states")
    if not isinstance(states, list):
        return []
    state_ids = {str(item) for item in action.get("state_ids", [])}
    process_ids = {str(item) for item in action.get("p_ids", [])}
    matched = []
    for row in states:
        if not _is_dict(row):
            continue
        if state_ids and str(row.get("state_id")) in state_ids:
            matched.append(row)
            continue
        if process_ids and str(row.get("p_id")) in process_ids:
            matched.append(row)
    return matched


def _apply_action_to_row(row, action):
    changed = 0

    if "value_id" in action and row.get("value_id") != action["value_id"]:
        row["value_id"] = copy.deepcopy(action["value_id"])
        changed += 1

    patch = action.get("parameters")

    if not _is_dict(patch):
        return changed

    parameters = row.setdefault("parameters", {})

    for key, value in patch.items():
        if parameters.get(key) == value:
            continue
        parameters[key] = copy.deepcopy(value)
        changed += 1

    return changed


def _apply_action_plan(snapshot, plan):
    working = copy.deepcopy(snapshot)
    changed = 0
    notifications = []
    ordered = sorted(
        (item for item in plan or [] if _is_dict(item)),
        key=_item_sequence,
    )
    for item in ordered:
        action = item.get("action") if _is_dict(item.get("action")) else item
        if "notify" in action:
            notifications.append(copy.deepcopy(action.get("notify")))
        rows = _matching_state_rows(working, action)
        for row in rows:
            changed += _apply_action_to_row(row, action)
    return working, changed, notifications


def _item_sequence(item):
    try:
        return int(item.get("seq", 0))
    except Exception:
        return 0


def _safety_rejection(context, event, report, before_revision):
    commit = {
        "status": "ERROR",
        "message": report.get("message", "safety policy rejected command"),
        "details": {"code": report.get("code")},
    }
    return _build_result(
        context,
        event,
        commit,
        rollout={},
        extra={
            "safety": copy.deepcopy(report),
            "previous_revision": before_revision,
            "changed": False,
        },
    )


def _authorize_external(context, event_name, payload, before_revision):
    if not payload.get("external"):
        return None
    if event_name == CONFIG_REPLACE:
        kind = "replace"
    elif event_name == CONFIG_SCOPE_OPERATIONS:
        kind = "scope_operations"
    else:
        kind = "patch"
    return authorize_external_commit(
        context["safety"], kind, payload, before_revision)


def _authorize_writer(context, event_name, payload):
    with context["lock"]:
        authorizer = context.get("write_authorizer")
    if not callable(authorizer):
        return None
    report = authorizer(event_name, copy.deepcopy(payload))
    if not _is_dict(report):
        report = {"ok": bool(report)}
    return report


def _prepare_actions(context, before, payload, before_revision):
    return prepare_rule_action_plan(
        context["safety"],
        before,
        payload.get("actions") or [],
        current_revision=before_revision,
        base_revision=payload.get("base_revision"),
    )


def _selector_operation_kind(selector, payload):
    return str(
        payload.get("operation_kind")
        or selector.get("operation_kind")
        or selector.get("operation")
        or ""
    ).strip().lower()


def _selector_identity_profile(section_name):
    profile = config_compatibility.IO_SECTIONS.get(section_name)
    if profile is not None:
        return profile["id_field"], True
    profile = config_compatibility.ENTITY_SECTIONS.get(section_name)
    if profile is not None:
        return profile["id_field"], False
    return "", False


def _validate_selector_operation(selector, payload):
    """Fence row updates, structural operations and identity migrations."""
    section_name = str(selector.get("section") or "").strip()
    id_field, is_io = _selector_identity_profile(section_name)
    if not id_field:
        raise ValueError("unsupported stable selector section: " + section_name)
    operation = _selector_operation_kind(selector, payload)
    mode = str(payload.get("mode") or "set").strip().lower()
    field = str(selector.get("field") or "").strip()
    value = payload.get("value")

    if operation == "identity_migration":
        raise ValueError(
            "identity migration requires the dedicated atomic migration API"
        )
    if field:
        expected = "controller_update" if section_name == "controllers" else "field_update"
        if operation not in ("", expected):
            raise ValueError(expected + " selector has an incompatible operation_kind")
        payload["operation_kind"] = expected
        if mode not in ("set", "update"):
            raise ValueError(expected + " requires set or update mode")
        return
    if operation not in ("entity_add", "entity_delete", "entity_replace"):
        raise ValueError(
            "whole-entity selectors require entity_add, entity_delete or entity_replace"
        )
    required_mode = {
        "entity_add": "append",
        "entity_delete": "delete",
        "entity_replace": "set",
    }[operation]
    payload["operation_kind"] = operation
    if mode != required_mode:
        raise ValueError(operation + " requires mode='" + required_mode + "'")
    if operation == "entity_delete":
        return
    if not _is_dict(value):
        raise ValueError(operation + " requires an object value")
    selector_identity = selector.get(id_field)
    value_identity = value.get(id_field)
    if operation == "entity_replace" and str(value_identity) != str(selector_identity):
        raise ValueError("entity_replace must preserve " + id_field)
    if operation == "entity_add" and value_identity is None:
        raise ValueError("entity_add value requires " + id_field)
    if is_io:
        selector_uuid = str(selector.get("controller_uuid") or "")
        value_uuid = value.get("controller_uuid")
        if value_uuid is not None and str(value_uuid) != selector_uuid:
            raise ValueError("I/O entity operation must preserve controller_uuid")


def _resolve_patch_payload(event_name, before, payload):
    if event_name != CONFIG_PATCH:
        return payload
    resolved = copy.deepcopy(payload)
    selector = resolved.get("path")
    if _is_dict(selector):
        _validate_selector_operation(selector, resolved)
    resolved_path = config_compatibility.normalize_patch_selector(
        before,
        selector,
        external=bool(resolved.get("external")),
        operation_kind=resolved.get("operation_kind"),
    )
    resolved["path"] = resolved_path
    if (
        _is_dict(selector)
        and resolved.get("operation_kind") == "entity_replace"
    ):
        found, current = data_center.walk_path(
            before, data_center.parse_data_path(resolved_path))
        replacement = resolved.get("value")
        if not found or not _is_dict(current) or not _is_dict(replacement):
            raise ValueError("entity_replace target is unavailable")
        for field in sorted(config_compatibility.OWNERSHIP_FIELDS):
            if current.get(field) != replacement.get(field):
                raise ValueError(
                    "entity_replace must preserve ownership field '"
                    + field
                    + "'"
                )
    return resolved


# =============================================================================
# COMMIT AND SYNCHRONOUS ROLLOUT
# =============================================================================

def _commit_patch(center, payload):
    return data_center.request_write_commit(
        center,
        payload.get("path"),
        payload.get("value"),
        mode=payload.get("mode", "set"),
        source=payload.get("source", "state_event_management"),
        verify=payload.get("verify", True),
    ), {}


def _commit_replace(center, payload):
    source = payload.get("source", "state_event_management")
    representation = str(payload.get("representation") or "").strip().lower()
    if not representation:
        representation = (
            "storage"
            if payload.get("external") or source == "coordinator_replication"
            else "runtime"
        )
    return data_center.commit_full_config(
        center,
        payload.get("config") or {},
        source=source,
        representation=representation,
    ), {}


def _commit_action_plan(context, center, payload, before, before_revision):
    safety = _prepare_actions(context, before, payload, before_revision)
    extra = {"safety": copy.deepcopy(safety)}
    if not safety.get("ok"):
        return {
            "status": "ERROR",
            "message": safety.get("message", "safety policy rejected action plan"),
            "details": {"code": safety.get("code")},
        }, extra
    working, changed, notifications = _apply_action_plan(
        before, safety.get("actions") or [])
    extra.update({"changed_fields": changed, "notifications": notifications})
    if changed:
        return data_center.commit_full_config(
            center,
            working,
            source=payload.get("source", "generic_rule_engine"),
            representation="runtime",
        ), extra
    return {
        "status": "OK",
        "message": "action plan contained no config mutations",
        "details": {"revision": data_center.get_revision(center)},
    }, extra


def _commit_scope_operations(center, payload, before):
    """Apply canonical entity operations and commit one verified candidate."""
    operations = config_sharding_contracts.validate_scope_operations(
        payload.get("operations"),
        operation_format=payload.get("operation_format"),
    )
    candidate = config_sharding_contracts.apply_operations(
        before,
        operations,
        representation="runtime",
    )
    report = config_compatibility.verify_runtime_config_data(candidate)
    if report.get("status") != "OK":
        return {
            "status": "ERROR",
            "message": "scope operation candidate failed Type Authority",
            "validation": report,
        }, {"operation_count": len(operations)}
    return data_center.commit_full_config(
        center,
        candidate,
        source=payload.get("source", "external_proposal"),
        representation="runtime",
    ), {"operation_count": len(operations)}


def _commit_sync(center):
    return {
        "status": "OK",
        "message": "synchronisation requested",
        "details": {"revision": data_center.get_revision(center)},
    }, {}


def _perform_commit(context, event_name, payload, before, before_revision):
    center = context["data_center"]
    if event_name == CONFIG_PATCH:
        return _commit_patch(center, payload)
    if event_name == CONFIG_REPLACE:
        return _commit_replace(center, payload)
    if event_name == CONFIG_SCOPE_OPERATIONS:
        return _commit_scope_operations(center, payload, before)
    if event_name == CONFIG_ACTION_PLAN:
        return _commit_action_plan(
            context, center, payload, before, before_revision)
    return _commit_sync(center)


def _failed_commit_result(context, event, result, extra, before_revision):
    extra.update({"previous_revision": before_revision, "changed": False})
    return _build_result(
        context, event, result, rollout={}, extra=extra)


def _commit_command(context, event):
    event_name = _event_type(event)
    payload = _payload(event)
    center = context["data_center"]
    before_revision = data_center.get_revision(center)
    found, before = data_center.heap_snapshot(center)
    before = before if found else {}
    payload = _resolve_patch_payload(event_name, before, payload)
    writer_authorization = _authorize_writer(context, event_name, payload)
    if writer_authorization is not None and not writer_authorization.get("ok"):
        return _safety_rejection(
            context, event, writer_authorization, before_revision)
    authorization = _authorize_external(
        context, event_name, payload, before_revision)
    if authorization is not None and not authorization.get("ok"):
        return _safety_rejection(
            context, event, authorization, before_revision)
    result, extra = _perform_commit(
        context, event_name, payload, before, before_revision)
    if result.get("status") != "OK":
        return _failed_commit_result(
            context, event, result, extra, before_revision)

    found, after = data_center.heap_snapshot(center)
    after = after if found else {}
    affected = _affected_controllers(before, after)
    if event_name in (CONFIG_SYNC_REQUEST, INITIAL_FULL_SYNC):
        affected = tuple(get_node_controller_uuids(after, None))
    rollout = _rollout_config(context, after, affected, event_name)
    if not rollout.get("ok", False):
        failed_rollout = copy.deepcopy(rollout)
        compensation = data_center.commit_full_config(
            center, before, source="rollout_compensation",
            representation="runtime")
        rollback_rollout = {"ok": False, "status": "compensation_commit_failed"}
        if compensation.get("status") == "OK":
            rollback_rollout = _rollout_config(
                context,
                before,
                _affected_controllers(after, before),
                event_name + "_ROLLBACK",
            )
        result = dict(result)
        result["status"] = "ERROR"
        result["message"] = (
            "config rollout failed; previous runtime bundle restored"
            if rollback_rollout.get("ok")
            else "config rollout failed and runtime compensation is incomplete"
        )
        extra["failed_rollout"] = failed_rollout
        extra["compensation"] = {
            "commit": copy.deepcopy(compensation),
            "rollout": copy.deepcopy(rollback_rollout),
            "restored": bool(
                compensation.get("status") == "OK"
                and rollback_rollout.get("ok")),
        }
        rollout = rollback_rollout
    current_revision = data_center.get_revision(center)
    extra.update({
        "previous_revision": before_revision,
        "changed": current_revision > before_revision,
    })

    if current_revision > before_revision:
        _schedule_persistence(context, current_revision)

    return _build_result(
        context,
        event,
        result,
        rollout=rollout,
        extra=extra,
    )


def _execute_commit_command(context, event):
    payload = _payload(event)
    operation_id = payload.get("operation_id")
    cached = lookup_safety_result(context["safety"], operation_id)
    if cached is not None:
        cached["duplicate"] = True
        return cached
    result = _commit_command(context, event)
    notification = _notify_post_commit(context, event, result)
    if notification is not None:
        result["post_commit"] = copy.deepcopy(notification)
    remember_safety_result(context["safety"], operation_id, result)
    return result


def _notify_post_commit(context, event, result):
    with context["lock"]:
        handler = context.get("post_commit_handler")
    if not callable(handler):
        return None
    try:
        response = handler(copy.deepcopy(event), copy.deepcopy(result))
        return response if _is_dict(response) else {"ok": bool(response)}
    except Exception as exc:
        LOGGER.exception("Post-commit handler failed")
        return {"ok": False, "error": str(exc)}


def _rollout_config(context, snapshot, affected, reason):
    with context["lock"]:
        handler = context.get("rollout_handler")
    revision = data_center.get_revision(context["data_center"])
    packages = {}
    for controller_uuid in affected:
        packages[controller_uuid] = data_center.get_shard_snapshot(
            context["data_center"], controller_uuid)
    request = {
        "reason": reason,
        "revision": revision,
        "fingerprint": data_center.get_fingerprint(context["data_center"]),
        "affected_controller_uuids": tuple(affected),
        "removed_controller_uuids": tuple(
            identity for identity in affected
            if not _projection_has_controller(snapshot, identity)
        ),
        "packages": packages,
        "config_snapshot": copy.deepcopy(snapshot),
    }
    if not callable(handler):
        context["statistics"]["rollouts_ok"] += 1
        return {"ok": True, "status": "no_consumer", "revision": revision}
    try:
        response = handler(request)
        if not _is_dict(response):
            response = {"ok": bool(response)}
        if response.get("ok", False):
            context["statistics"]["rollouts_ok"] += 1
        else:
            context["statistics"]["rollouts_error"] += 1
        return response
    except Exception as exc:
        context["statistics"]["rollouts_error"] += 1
        LOGGER.exception("Configuration rollout failed")
        return {"ok": False, "error": str(exc), "revision": revision}


def _projection_has_controller(snapshot, identity):
    projection = build_controller_projection(snapshot, identity)
    physical = projection.get("controllers") or []
    virtual = projection.get("virtual_controllers") or []
    return bool(physical or virtual)


def _build_result(context, event, commit, **kwargs):
    rollout = kwargs.get("rollout") or {}
    extra = kwargs.get("extra") or {}
    ok = commit.get("status") == "OK" and rollout.get("ok", True)
    if ok:
        context["statistics"]["commits_ok"] += 1
    else:
        context["statistics"]["commits_error"] += 1
    result = {
        "ok": ok,
        "request_id": event.get("request_id"),
        "event_type": _event_type(event),
        "commit": copy.deepcopy(commit),
        "rollout": copy.deepcopy(rollout),
        "revision": data_center.get_revision(context["data_center"]),
        "fingerprint": data_center.get_fingerprint(context["data_center"]),
        "completed_at": time.time(),
    }
    result.update(copy.deepcopy(extra))
    context["last_result"] = copy.deepcopy(result)
    if not ok:
        context["last_error"] = copy.deepcopy(result)
    return result


# =============================================================================
# REQUEST / ACK PROTOCOL
# =============================================================================

def _complete_waiter(context, request_id, result):
    if not request_id:
        return
    with context["lock"]:
        waiter = context["waiters"].get(str(request_id))
    if not _is_dict(waiter):
        return
    waiter["result"] = copy.deepcopy(result)
    waiter["event"].set()


def _publish_result(context, result):
    output = context.get("queue_event_send")
    if output is None:
        return False
    event = make_event(
        "CONFIG_COMMAND_COMPLETED",
        payload=copy.deepcopy(result),
        source="state_event_management",
        target="thread_management",
    )
    return sync_queue_put(output, event)


def _external_event_name(result):
    if result.get("duplicate"):
        return ""
    if result.get("changed") and result.get("ok"):
        return "CONFIG_COMMIT_APPLIED"
    if result.get("changed"):
        return "CONFIG_COMMIT_ROLLOUT_FAILED"
    return ""


def _publish_external_result(context, event, result):
    event_name = _external_event_name(result)
    if not event_name:
        return False
    with context["lock"]:
        publisher = context.get("external_publisher")
    if not callable(publisher):
        return False
    payload = copy.deepcopy(result)
    payload["source_event_type"] = _event_type(event)
    payload["source"] = _payload(event).get("source")
    try:
        written = publisher(event_name, payload)
    except Exception:
        LOGGER.exception("External config event publication failed")
        written = False
    key = "external_events_published" if written else "external_events_rejected"
    context["statistics"][key] += 1
    return bool(written)


def _submit_request(context, event_type, payload, **kwargs):
    request_id = str(uuid.uuid4())
    waiter = {"event": threading.Event(), "result": None}
    with context["lock"]:
        context["waiters"][request_id] = waiter
    event = make_event(
        event_type,
        payload=copy.deepcopy(payload),
        source=payload.get("source", "runtime_api"),
        target="state_event_management",
        request_id=request_id,
    )
    if not sync_queue_put(context["queue_event_state"], event):
        with context["lock"]:
            context["waiters"].pop(request_id, None)
        return {"ok": False, "error": "state queue rejected request"}
    timeout = _safe_float(
        kwargs.get("timeout_s"), context["options"]["request_timeout_s"])
    completed = waiter["event"].wait(timeout)
    with context["lock"]:
        context["waiters"].pop(request_id, None)
    if not completed:
        return {"ok": False, "error": "config request timed out", "request_id": request_id}
    return copy.deepcopy(waiter["result"])


def request_config_patch(context, path, value=None, **kwargs):
    payload = {
        "path": path,
        "value": copy.deepcopy(value),
        "mode": kwargs.get("mode", "set"),
        "source": kwargs.get("source", "runtime_api"),
        "verify": kwargs.get("verify", True),
        "external": kwargs.get("external", False),
        "principal": kwargs.get("principal"),
        "base_revision": kwargs.get("base_revision"),
        "operation_id": kwargs.get("operation_id"),
        "operation_kind": kwargs.get("operation_kind"),
    }
    return _submit_request(context, CONFIG_PATCH, payload, **kwargs)


def request_config_replace(context, config, **kwargs):
    payload = {
        "config": copy.deepcopy(config),
        "source": kwargs.get("source", "runtime_api"),
        "external": kwargs.get("external", False),
        "principal": kwargs.get("principal"),
        "base_revision": kwargs.get("base_revision"),
        "operation_id": kwargs.get("operation_id"),
        "restore_id": kwargs.get("restore_id"),
        "transfer_id": kwargs.get("transfer_id"),
        "authority_epoch": kwargs.get("authority_epoch"),
        "fingerprint": kwargs.get("fingerprint"),
        "representation": kwargs.get("representation"),
    }
    return _submit_request(context, CONFIG_REPLACE, payload, **kwargs)

def request_config_scope_operations(context, operations, **kwargs):
    """Submit canonical scope operations through the SEM single writer."""
    payload = {
        "operations": copy.deepcopy(operations or []),
        "operation_format": kwargs.get(
            "operation_format",
            config_sharding_contracts.OPERATION_FORMAT,
        ),
        "scope": kwargs.get("scope"),
        "source": kwargs.get("source", "external_proposal"),
        "external": kwargs.get("external", True),
        "principal": kwargs.get("principal"),
        "base_revision": kwargs.get("base_revision"),
        "base_authority_epoch": kwargs.get("base_authority_epoch"),
        "base_scope_revision": kwargs.get("base_scope_revision"),
        "operation_id": kwargs.get("operation_id"),
        "relay_identity": copy.deepcopy(kwargs.get("relay_identity")),
    }
    return _submit_request(context, CONFIG_SCOPE_OPERATIONS, payload, **kwargs)


def request_config_action_plan(context, actions, **kwargs):
    payload = {
        "actions": copy.deepcopy(actions or []),
        "source": kwargs.get("source", "generic_rule_engine"),
        "base_revision": kwargs.get("base_revision"),
        "operation_id": kwargs.get("operation_id"),
    }
    return _submit_request(context, CONFIG_ACTION_PLAN, payload, **kwargs)


def request_config_sync(context, **kwargs):
    payload = {"source": kwargs.get("source", "runtime_api")}
    return _submit_request(context, CONFIG_SYNC_REQUEST, payload, **kwargs)


# =============================================================================
# ASYNCHRONOUS PERSISTENCE
# =============================================================================

def _schedule_persistence(context, revision):
    if not context["options"]["persistence_enabled"]:
        return
    context["persistence_queue"].put({"revision": revision})


def _persist_once(context):
    center = context["data_center"]
    options = context["options"]
    reports = []
    if options["persist_json"]:
        reports.append(data_center.save_heap_to_json(center))
    if options["persist_sqlite"]:
        reports.append(data_center.sync_heap_to_sqlite(center))
    failed = [report for report in reports if report.get("status") != "OK"]
    context["statistics"]["persistence_runs"] += 1
    if failed:
        context["statistics"]["persistence_errors"] += 1
        context["last_error"] = copy.deepcopy(failed[-1])
        return False
    return True


def _persistence_loop(context):
    interval = context["options"]["persistence_coalesce_s"]

    if not context["options"]["persistence_enabled"]:
        context["stop_event"].wait()
        return

    pending = False
    while not context["stop_event"].is_set():
        try:
            item = context["persistence_queue"].get(timeout=interval)
            pending = item is not None
        except Empty:
            if pending:
                _persist_once(context)
                pending = False
            continue
        while True:
            try:
                context["persistence_queue"].get_nowait()
                pending = True
            except Empty:
                break
    if pending or data_center.describe_data_center(context["data_center"])["dirty"]:
        _persist_once(context)


# =============================================================================
# COMPONENT LIFECYCLE
# =============================================================================

def _sem_loop(context):
    context["state"] = "running"
    while not context["stop_event"].is_set():
        event = sync_queue_get(
            context["queue_event_state"],
            context["options"]["ingress_timeout_s"],
        )
        if event is None:
            continue
        if _is_stop_event(event):
            break
        context["statistics"]["events_received"] += 1
        if _event_type(event) not in (
            CONFIG_PATCH,
            CONFIG_REPLACE,
            CONFIG_SCOPE_OPERATIONS,
            CONFIG_ACTION_PLAN,
            CONFIG_SYNC_REQUEST,
            INITIAL_FULL_SYNC,
        ):
            continue
        with context["writer_lock"]:
            try:
                result = _execute_commit_command(context, event)
            except Exception as exc:
                LOGGER.exception("SEM command failed")
                result = {
                    "ok": False,
                    "request_id": event.get("request_id"),
                    "error": str(exc),
                    "completed_at": time.time(),
                }
                context["statistics"]["commits_error"] += 1
                context["last_error"] = copy.deepcopy(result)
        _publish_external_result(context, event, result)
        _complete_waiter(context, event.get("request_id"), result)
        _publish_result(context, result)
    context["state"] = "stopped"


def start_state_event_management(**kwargs):
    runtime = kwargs.get("runtime") if _is_dict(kwargs.get("runtime")) else {}
    context = create_sem_context(**kwargs)
    persistence_thread = threading.Thread(
        target=partial(_persistence_loop, context),
        name=context["options"]["thread_name"] + "-persistence",
        daemon=False,
    )
    thread = threading.Thread(
        target=partial(_sem_loop, context),
        name=context["options"]["thread_name"],
        daemon=False,
    )
    context["persistence_thread"] = persistence_thread
    context["thread"] = thread
    runtime["context"] = context
    persistence_thread.start()
    thread.start()
    return thread


def stop_state_event_management(context, **kwargs):
    if not _is_dict(context):
        return {"state": "missing"}
    context["stop_event"].set()
    sync_queue_put(context.get("queue_event_state"), _make_stop_event())
    timeout = _safe_float(
        kwargs.get("join_timeout_s"), context["options"]["join_timeout_s"])
    deadline = time.monotonic() + timeout
    for thread in (context.get("thread"), context.get("persistence_thread")):
        if thread is None or thread is threading.current_thread():
            continue
        thread.join(timeout=max(0.0, deadline - time.monotonic()))
    return get_state_event_management_snapshot(context)


def get_state_event_management_snapshot(context):
    if not _is_dict(context):
        return {"state": "missing"}
    with context["lock"]:
        return {
            "module": context["module"],
            "version": context["version"],
            "state": context["state"],
            "thread_alive": _thread_alive(context.get("thread")),
            "persistence_thread_alive": _thread_alive(
                context.get("persistence_thread")),
            "revision": data_center.get_revision(context["data_center"]),
            "fingerprint": data_center.get_fingerprint(context["data_center"]),
            "pending_waiters": len(context["waiters"]),
            "statistics": dict(context["statistics"]),
            "safety": get_safety_snapshot(context["safety"]),
            "last_error": copy.deepcopy(context["last_error"]),
        }


# Compatibility aliases for older bootstrap revisions.
start_sem = start_state_event_management
stop_sem = stop_state_event_management




