# -*- coding: utf-8 -*-

# src/core/distributed_coordinator.py


"""
 =============================================================================
 DISTRIBUTED_COORDINATOR
 =============================================================================

 Functional site coordination over the existing MQTT boundary. Authority is
 selected per persisted scope: legacy Master authority or Worker node authority.
 Durable journals provide idempotent delivery and crash-visible actuator
 states. Hardware writes are never executed in this component.
 =============================================================================
"""


import copy
import logging
import threading
import time
import uuid

from functools import partial
from queue import Empty, Full, Queue

from src.library._generic_system_contract_library import authority_flow_contracts
from src.library._generic_system_contract_library import persistence_contracts
from src.library._generic_system_contract_library import protocol_contracts
from src.library._generic_system_contract_library.config_sharding_contracts import (
    apply_operations,
    build_cross_execution_shard,
    classify_config_scopes,
    build_local_execution_shard,
    normalize_legacy_ingress,
    validate_authority_config,
    validate_cross_execution_shard,
    validate_scope_operations,
    validate_scope_partition,
)

from src.core.authority_transition_service import create_authority_transition_service
from src.library._authority_transition_store import (
    get_registry_view,
    persist_registry_view,
)
from src.library import _generic_type_library as type_library
from src.library._distributed_operation_store import (
    close_operation_store,
    complete_inbox,
    complete_outbox,
    commit_scope_state,
    converge_config_proposals,
    create_operation_store,
    enqueue_outbox,
    get_actuator_operation,
    get_due_outbox,
    get_due_mirrors,
    get_metadata,
    get_mirror_record,
    get_outbox,
    get_operation_store_snapshot,
    get_scope_head,
    increment_metadata,
    ensure_scope_head,
    list_actuator_operations,
    list_nodes,
    mark_outbox_attempt,
    mark_mirror_attempt,
    rebase_config_proposals,
    recover_actuator_operations,
    register_actuator_operation,
    register_inbox,
    acknowledge_mirror,
    set_metadata,
    stage_mirror_record,
    transition_actuator_operation,
    update_unattempted_outbox_payload,
    upsert_node,
)
from src.library._state_shard_manager import (
    get_controller_owner_node,
    get_enabled_node_ids,
    get_node_controller_uuids,
)

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "distributed_coordinator",
    "library_version": "2.0.0",
    "protocol": "automation.distributed.v2",
    "options": {
        "enabled": False,
        "role": "standalone",
        "node_id": "node-unknown",
        "site_id": "site-main",
        "coordinator_node_id": "",
        "journal_path": "_runtime/distributed_operations.sqlite3",
        "registration_interval_s": 10.0,
        "node_timeout_s": 45.0,
        "retry_interval_s": 2.0,
        "command_ttl_s": 60.0,
        "outbox_batch_size": 64,
        "control_queue_maxsize": 512,
        "result_queue_maxsize": 512,
        "join_timeout_s": 5.0,
        "proposal_timeout_s": 10.0,
        "local_automation_enabled": False,
        "config_authority": "node_local",
        "config_sync_mode": "local_first",
        "conflict_policy": "worker_authoritative",
        "require_hardware_readback": True,
        "allow_retry_after_uncertain": False,
        "replication_targets": [],
        "controller_allowlist": None,
        "authority_validation_profile": "production_v2",
    },
}

COMMAND_NODE_REGISTER = "distributed.node.register"
COMMAND_NODE_HEARTBEAT = "distributed.node.heartbeat"
COMMAND_CONFIG_APPLY = "distributed.config.apply"
COMMAND_CONFIG_PATCH = "distributed.config.patch"
COMMAND_CROSS_CONFIG_APPLY = "distributed.cross.apply"
COMMAND_CONFIG_PROPOSE = protocol_contracts.CONFIG_PROPOSE
COMMAND_MIRROR_ANNOUNCE = protocol_contracts.MIRROR_ANNOUNCE
COMMAND_MIRROR_REQUEST = protocol_contracts.MIRROR_REQUEST
COMMAND_MIRROR_DELTA = protocol_contracts.MIRROR_DELTA
COMMAND_MIRROR_SNAPSHOT = protocol_contracts.MIRROR_SNAPSHOT
COMMAND_MIRROR_CHUNK = protocol_contracts.MIRROR_CHUNK
COMMAND_MIRROR_ACK = protocol_contracts.MIRROR_ACK
COMMAND_AUTHORITY_TRANSFER = protocol_contracts.AUTHORITY_TRANSFER
COMMAND_RESTORE_REQUEST = protocol_contracts.RESTORE_REQUEST
COMMAND_RESTORE_ACTIVATE = protocol_contracts.RESTORE_ACTIVATE
COMMAND_ACTUATOR_ROUTE = "distributed.actuator.route"
COMMAND_ACTUATOR_EXECUTE = "distributed.actuator.execute"
COMMAND_ACTUATOR_RESULT = "distributed.actuator.result"

CONFIG_MUTATION_COMMANDS = (
    "config.patch", "config.update", "config.append",
    "config.delete", "config.replace", "config.sync",
)

TERMINAL_ACTUATOR_STATES = (
    "CONFIRMED", "WRITE_UNVERIFIED", "FAILED", "REJECTED", "UNCERTAIN",
)


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


def _safe_float(value, default, minimum=0.0):
    try:
        result = float(value)
    except Exception:
        return float(default)
    return result if result >= minimum else float(default)


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _increment_stat(context, key, amount=1):
    with context["lock"]:
        context["statistics"][key] += int(amount)


def _authority_state(context):
    with context["lock"]:
        return {
            "coordinator_term": context["coordinator_term"],
            "authority_revision": context["authority_revision"],
            "authority_fingerprint": context["authority_fingerprint"],
            "authority_epoch": context.get("authority_epoch", 0),
            "scope_revision": context.get("node_scope_revision", 0),
            "registered": context["registered"],
        }

def _set_state(context, state):
    with context["lock"]:
        context["state"] = str(state)


def _error(code, message, **kwargs):
    result = {"ok": False, "code": str(code), "error": str(message)}
    result.update(copy.deepcopy(kwargs))
    return result


def _accepted(result):
    if _is_dict(result):
        if "ok" in result:
            return bool(result.get("ok"))
        return bool(result.get("accepted"))
    return bool(result)


def _resolved_options(options):
    supplied = copy.deepcopy(options) if _is_dict(options) else {}
    result = dict(supplied)
    for key, value in CONTEXT["options"].items():
        result.setdefault(key, copy.deepcopy(value))
    result["enabled"] = _safe_bool(result.get("enabled"), False)
    result["role"] = str(result.get("role") or "standalone").strip().lower()
    result["node_id"] = str(result.get("node_id") or "node-unknown").strip()
    result["site_id"] = protocol_contracts.effective_site_id(
        result.get("site_id"))
    result["coordinator_node_id"] = str(
        result.get("coordinator_node_id") or "").strip()
    result["config_authority"] = authority_flow_contracts.validate_authority_mode(
        result.get("config_authority"))
    for key in (
        "registration_interval_s", "node_timeout_s", "retry_interval_s",
        "command_ttl_s", "join_timeout_s", "proposal_timeout_s",
    ):
        result[key] = _safe_float(
            result.get(key), CONTEXT["options"][key], 0.05)
    for key in (
        "outbox_batch_size", "control_queue_maxsize", "result_queue_maxsize",
    ):
        result[key] = _safe_int(
            result.get(key), CONTEXT["options"][key], 1)
    result["require_hardware_readback"] = _safe_bool(
        result.get("require_hardware_readback"), True)
    result["allow_retry_after_uncertain"] = _safe_bool(
        result.get("allow_retry_after_uncertain"), False)
    result["local_automation_enabled"] = _safe_bool(
        result.get("local_automation_enabled"), False)
    result["config_sync_mode"] = str(
        result.get("config_sync_mode") or "local_first").strip().lower()
    result["conflict_policy"] = str(
        result.get("conflict_policy") or "worker_authoritative").strip().lower()
    result["authority_validation_profile"] = str(
        result.get("authority_validation_profile")
        or "production_v2").strip().lower()
    result["replication_targets"] = tuple(sorted({
        str(value)
        for value in result.get("replication_targets") or []
        if str(value)
    }))
    configured_controllers = result.get("controller_allowlist")
    if configured_controllers is None:
        result["controller_allowlist"] = None
    else:
        result["controller_allowlist"] = tuple(sorted({
            str(value).strip()
            for value in configured_controllers
            if str(value).strip()
        }))
    if result["enabled"] and result["role"] == "worker":
        result["local_automation_enabled"] = True
        if result["config_authority"] == "node_local":
            result["config_sync_mode"] = "local_first"
            result["conflict_policy"] = "worker_authoritative"
    _validate_options(result)
    return result

def _validate_options(options):
    if not options["enabled"]:
        return
    if options["role"] not in ("coordinator", "worker"):
        raise ValueError("distributed role must be coordinator or worker")
    if (
        options["config_authority"] == "master_global"
        and not options["coordinator_node_id"]
    ):
        raise ValueError("master_global requires coordinator_node_id")
    if options["role"] == "coordinator":
        if options["node_id"] != options["coordinator_node_id"]:
            raise ValueError("coordinator role requires node_id=coordinator_node_id")
    if options["config_sync_mode"] not in ("global_first", "local_first"):
        raise ValueError("config_sync_mode must be global_first or local_first")
    expected_policy = (
        "master_wins"
        if options["config_authority"] == "master_global"
        else "worker_authoritative"
    )
    if options["conflict_policy"] != expected_policy:
        raise ValueError(
            "conflict_policy does not match config_authority mode")
    if options["authority_validation_profile"] not in (
        "lab_v2", "production_v2",
    ):
        raise ValueError(
            "authority_validation_profile must be lab_v2 or production_v2")

def _config_snapshot(context):
    reader = context["config_capabilities"].get("get_snapshot")
    return reader() if callable(reader) else {}


def _config_fingerprint(context):
    reader = context["config_capabilities"].get("get_fingerprint")
    return str(reader() or "") if callable(reader) else ""


def _local_config_revision(context):
    reader = context["config_capabilities"].get("get_revision")
    return _safe_int(reader(), 0) if callable(reader) else 0


def _pending_proposal_count(context):
    snapshot = get_operation_store_snapshot(context["store"])
    states = (snapshot.get("counts") or {}).get("outbox") or {}
    pending_states = (
        "PENDING", "AWAITING_ACK", "MASTER_ACKED", "REBASE_REQUIRED",
    )
    return sum(int(states.get(state) or 0) for state in pending_states)


def _remember_node_scope(context, payload, fingerprint):
    scope_revision = _safe_int(
        payload.get("target_scope_revision", payload.get("scope_revision")),
        context.get("node_scope_revision") or 0,
    )
    with context["lock"]:
        context["node_scope_revision"] = scope_revision
        context["node_shard_fingerprint"] = str(fingerprint or "")
    set_metadata(context["store"], "node_scope_revision", scope_revision)
    set_metadata(context["store"], "node_shard_fingerprint", str(fingerprint or ""))


def _ensure_node_scope_fingerprint(context, live_fingerprint=None):
    recorded = str(context.get("node_shard_fingerprint") or "")
    if recorded:
        return recorded
    live = str(
        live_fingerprint
        if live_fingerprint is not None
        else _config_fingerprint(context)
    )
    if not live:
        return ""
    initialized = False
    with context["lock"]:
        recorded = str(context.get("node_shard_fingerprint") or "")
        if not recorded:
            context["node_shard_fingerprint"] = live
            recorded = live
            initialized = True
        scope_revision = _safe_int(
            context.get("node_scope_revision"), 0)
    if initialized:
        set_metadata(context["store"], "node_scope_revision", scope_revision)
        set_metadata(context["store"], "node_shard_fingerprint", recorded)
    return recorded


def _initialize_authority(context):
    store = context["store"]
    role = context["options"]["role"]
    if role == "coordinator":
        context["coordinator_term"] = increment_metadata(
            store, "coordinator_term", 0)
        current_fp = _config_fingerprint(context)
        stored_fp = str(get_metadata(store, "authority_fingerprint", "") or "")
        stored_revision = _safe_int(
            get_metadata(store, "authority_revision", 0), 0)
        if not stored_revision:
            stored_revision = 1
        elif stored_fp and stored_fp != current_fp:
            stored_revision += 1
        context["authority_revision"] = stored_revision
        context["authority_fingerprint"] = current_fp
        set_metadata(store, "authority_revision", stored_revision)
        set_metadata(store, "authority_fingerprint", current_fp)
        return
    context["coordinator_term"] = _safe_int(
        get_metadata(store, "worker_coordinator_term", 0), 0)
    context["authority_revision"] = _safe_int(
        get_metadata(store, "worker_authority_revision", 0), 0)
    context["authority_fingerprint"] = str(
        get_metadata(store, "worker_authority_fingerprint", "") or "")


def _remember_worker_authority(context, revision, **kwargs):
    fingerprint = kwargs.get("fingerprint")
    term = kwargs.get("term")
    with context["lock"]:
        context["authority_revision"] = int(revision)
        context["authority_fingerprint"] = str(fingerprint)
        context["coordinator_term"] = int(term)
    set_metadata(context["store"], "worker_authority_revision", int(revision))
    set_metadata(context["store"], "worker_authority_fingerprint", str(fingerprint))
    set_metadata(context["store"], "worker_coordinator_term", int(term))


def _register_runtime_event_handler(context):
    with context["lock"]:
        if context.get("runtime_event_handler_registered"):
            return True
        registrar = context["mqtt_api"].get("register_event_handler")
        if not callable(registrar):
            return False
        registrar(partial(_handle_runtime_event, context))
        context["runtime_event_handler_registered"] = True
    return True


def _register_mqtt_handlers(context):
    api = context["mqtt_api"]
    registrar = api.get("register_command_handler")
    if not callable(registrar):
        raise RuntimeError("MQTT custom command handler capability is unavailable")
    handlers = {
        COMMAND_NODE_REGISTER: _handle_node_register,
        COMMAND_NODE_HEARTBEAT: _handle_node_heartbeat,
        COMMAND_CONFIG_APPLY: _handle_config_apply,
        COMMAND_CONFIG_PATCH: _handle_config_patch,
        COMMAND_CROSS_CONFIG_APPLY: _handle_cross_config_apply,
        COMMAND_CONFIG_PROPOSE: _handle_config_proposal,
        COMMAND_MIRROR_REQUEST: _handle_mirror_request,
        COMMAND_AUTHORITY_TRANSFER: _handle_authority_transfer,
        COMMAND_RESTORE_REQUEST: _handle_restore_request,
        COMMAND_RESTORE_ACTIVATE: _handle_restore_activate,
        COMMAND_ACTUATOR_ROUTE: _handle_actuator_route,
        COMMAND_ACTUATOR_EXECUTE: _handle_actuator_execute,
        COMMAND_ACTUATOR_RESULT: _handle_actuator_result,
    }
    if (
        context["options"]["role"] == "worker"
        and context["options"]["config_authority"] == "node_local"
    ):
        for name in CONFIG_MUTATION_COMMANDS:
            handlers[name] = _handle_legacy_external_mutation
    for command, handler in handlers.items():
        registrar(command, partial(handler, context))
    api["register_ack_handler"](partial(_handle_mqtt_ack, context))
    api["register_connect_handler"](partial(_handle_mqtt_connect, context))
    api["register_command_guard"](partial(_guard_mqtt_command, context))
    if (
        context["options"]["role"] == "coordinator"
        or callable(context.get("cross_event_handler"))
    ):
        _register_runtime_event_handler(context)


def _guard_mqtt_command(context, command):
    if context["options"]["role"] != "worker":
        return {"ok": True}
    if str(command.get("command") or "") not in CONFIG_MUTATION_COMMANDS:
        return {"ok": True}
    if context["options"]["config_authority"] == "node_local":
        return {"ok": True, "route": "external_proposal"}
    return _error(
        "not_config_authority",
        "worker accepts config mutation only through legacy coordinator replication",
        coordinator_node_id=context["options"]["coordinator_node_id"],
    )

def _handle_mqtt_connect(context):
    context["connected_event"].set()
    _queue_control(context, {"kind": "connected"})
    return True


def _queue_control(context, item):
    try:
        context["control_queue"].put_nowait(copy.deepcopy(item))
        return True
    except Full:
        _increment_stat(context, "control_rejected")
        return False


def _inbox_start(context, command):
    return register_inbox(
        context["store"], command.get("message_id"),
        command.get("command"), payload=command.get("payload") or {},
    )


def _inbox_duplicate_result(record):
    if record.get("status") == "conflict":
        return _error("message_id_conflict", "message ID was reused with other content")
    if record.get("status") != "duplicate":
        return None
    cached = record.get("result")
    if _is_dict(cached):
        result = copy.deepcopy(cached)
        result["duplicate"] = True
        return result
    return {"ok": True, "duplicate": True, "state": record.get("state")}


def _complete_command(context, command, result, **kwargs):
    complete_inbox(
        context["store"], command.get("message_id"), result,
        state=kwargs.get("state", "COMPLETED"))
    return result


def _require_role(context, role):
    if context["options"]["role"] == role:
        return None
    return _error("role", "command requires distributed role " + str(role))


def _validate_coordinator_command(context, command):
    source = str(command.get("source_node") or "")
    expected = context["options"]["coordinator_node_id"]
    if source != expected:
        return _error("authority", "command source is not the configured coordinator")
    payload = command.get("payload") or {}
    term = _safe_int(payload.get("coordinator_term"), 0)
    updated = False
    with context["lock"]:
        current = context["coordinator_term"]
        if term > current:
            context["coordinator_term"] = term
            updated = True
    if term < current:
        return _error("stale_fencing_term", "coordinator fencing term is stale")
    if updated:
        set_metadata(context["store"], "worker_coordinator_term", term)
    return None


def _validate_coordinator_command_deferred(context, command):
    """Authenticate a coordinator command without advancing durable fencing."""
    source = str(command.get("source_node") or "")
    expected = context["options"]["coordinator_node_id"]
    if source != expected:
        return _error("authority", "command source is not the configured coordinator")
    payload = command.get("payload") or {}
    metadata = command.get("metadata") if _is_dict(command.get("metadata")) else {}
    term = _safe_int(
        payload.get("coordinator_term", metadata.get("authority_term")), -1)
    current = _authority_state(context)["coordinator_term"]
    if term < current:
        return _error("stale_fencing_term", "coordinator fencing term is stale")
    return {"ok": True, "coordinator_term": term}

def _commit_registry_term(context, term):
    """Persist a successfully applied Registry fencing term monotonically."""
    supplied = _safe_int(term, -1)
    with context["lock"]:
        current = int(context["coordinator_term"])
        if supplied < current:
            return False
        context["coordinator_term"] = supplied
    set_metadata(context["store"], "worker_coordinator_term", supplied)
    return True

def _node_payload(context):
    config = _config_snapshot(context)
    authority = _authority_state(context)
    routed = tuple(get_node_controller_uuids(
        config, context["options"]["node_id"]))
    configured = context["options"].get("controller_allowlist")
    active_controllers = routed
    if configured is not None:
        allowed = set(configured)
        active_controllers = tuple(
            identity for identity in routed if identity in allowed)
    cross_reader = context.get("cross_status_reader")
    cross_status = cross_reader() if callable(cross_reader) else {
        "enabled": False,
        "revision": 0,
        "fingerprint": "",
        "authority_epoch": 0,
        "execution_epoch": 0,
    }
    capabilities = set(protocol_contracts.CAPABILITIES)
    if not (
        callable(cross_reader)
        and cross_status.get("enabled") is not False
    ):
        capabilities.difference_update((
            "cross_owner_accept_v1",
            "cross_runtime",
            "cross_full",
            "cross_patch",
            "cross_fact_consumer",
        ))
    live_fingerprint = _config_fingerprint(context)
    recorded_fingerprint = _ensure_node_scope_fingerprint(
        context, live_fingerprint=live_fingerprint)
    scope_revision = _safe_int(
        context.get("node_scope_revision"), 0)
    node_scope = "node:" + context["options"]["node_id"]
    return {
        "protocol": protocol_contracts.PROTOCOL_V2,
        "protocol_version": protocol_contracts.PROTOCOL_V2,
        "protocol_versions": [protocol_contracts.PROTOCOL_V2],
        "contracts_version": protocol_contracts.CONTRACTS_VERSION,
        "contracts_major": protocol_contracts.CONTRACTS_MAJOR,
        "contracts_minor": protocol_contracts.CONTRACTS_MINOR,
        "type_schema_version": type_library.TYPE_SCHEMA_VERSION,
        "config_norm_version": copy.deepcopy(type_library.CONFIG_NORM_VERSION),
        "software_version": "automation_system_v0101",
        "site_id": context["options"]["site_id"],
        "node_id": context["options"]["node_id"],
        "role": context["options"]["role"],
        "runtime_nonce": context["runtime_nonce"],
        "config_authority": context["options"]["config_authority"],
        "authority_epoch": authority["authority_epoch"],
        "authority_revision": authority["authority_revision"],
        "fingerprint": authority["authority_fingerprint"],
        "local_config_revision": _local_config_revision(context),
        "live_config_revision": _local_config_revision(context),
        "live_config_fingerprint": live_fingerprint,
        "pending_proposal_count": _pending_proposal_count(context),
        "node_scope_revision": scope_revision,
        "node_shard_fingerprint": recorded_fingerprint,
        "node_replica": {
            "scope": node_scope,
            "authority_epoch": authority["authority_epoch"],
            "revision": scope_revision,
            "fingerprint": recorded_fingerprint,
        },
        "cross_replica": {
            "scope": str(cross_status.get("scope") or ""),
            "authority_epoch": int(cross_status.get("authority_epoch") or 0),
            "revision": int(cross_status.get("revision") or 0),
            "execution_epoch": int(cross_status.get("execution_epoch") or 0),
            "fingerprint": str(cross_status.get("fingerprint") or ""),
        },
        "controller_uuids": active_controllers,
        "capabilities": tuple(sorted(capabilities)),
        "max_packet_size": protocol_contracts.HARD_LIMITS["max_packet_size"],
    }

def _publish_registration(context, heartbeat=False):
    if not context["options"]["coordinator_node_id"]:
        return False
    publisher = context["mqtt_api"].get("publish_command")
    if not callable(publisher):
        return False
    command = COMMAND_NODE_HEARTBEAT if heartbeat else COMMAND_NODE_REGISTER
    result = publisher(
        context["options"]["coordinator_node_id"], command,
        payload=_node_payload(context),
        message_id=str(uuid.uuid4()),
        expires_at_s=time.time() + context["options"]["command_ttl_s"],
    )
    if result.get("ok"):
        _increment_stat(context, "registrations_sent", int(not heartbeat))
        _increment_stat(context, "heartbeats_sent", int(heartbeat))
    with context["lock"]:
        context["last_registration_s"] = time.monotonic()
    return bool(result.get("ok"))


def _normalize_node_registration(payload):
    """Return one detached Contract-2 registration or a structured rejection."""
    try:
        normalized = protocol_contracts.normalize_registration(payload)
    except (TypeError, ValueError) as exc:
        return _error(
            "registration_contract",
            "node registration is incompatible",
            detail=str(exc),
        )
    return {
        "ok": True,
        "registration": normalized,
        "node_replica": normalized.get("node_replica") or {},
        "cross_replica": normalized.get("cross_replica") or {},
        "capabilities": normalized.get("capabilities") or {},
    }


def _handle_node_register(context, command):
    role_error = _require_role(context, "coordinator")
    if role_error is not None:
        return role_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    checked = _normalize_node_registration(payload)
    if not checked.get("ok"):
        return _complete_command(context, command, checked)
    registration = checked["registration"]
    source = str(command.get("source_node") or "")
    if source != registration["node_id"]:
        return _complete_command(
            context, command,
            _error("node_identity", "source node and payload node differ"),
        )
    upsert_node(
        context["store"], source,
        role=payload.get("role"), state="online",
        authority_revision=payload.get("authority_revision"),
        fingerprint=payload.get("fingerprint"),
        capabilities={
            "runtime_nonce": payload.get("runtime_nonce"),
            "features": checked["capabilities"],
            "controller_uuids": payload.get("controller_uuids") or [],
        },
    )
    _queue_control(context, {
        "kind": "sync_node", "node_id": source,
        "runtime_nonce": payload.get("runtime_nonce"),
        "authority_revision": payload.get("authority_revision"),
        "fingerprint": payload.get("fingerprint"),
        "node_shard_fingerprint": checked["node_replica"].get("fingerprint"),
        "cross_revision": checked["cross_replica"].get("revision"),
        "cross_fingerprint": checked["cross_replica"].get("fingerprint"),
        "cross_runtime_enabled": bool(
            checked["capabilities"].get("cross_runtime")
        ),
    })
    authority = _authority_state(context)
    result = {
        "ok": True,
        "coordinator_node_id": context["options"]["node_id"],
        "coordinator_term": authority["coordinator_term"],
        "authority_revision": authority["authority_revision"],
        "fingerprint": authority["authority_fingerprint"],
    }
    _increment_stat(context, "nodes_registered")
    return _complete_command(context, command, result)


def _handle_node_heartbeat(context, command):
    role_error = _require_role(context, "coordinator")
    if role_error is not None:
        return role_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    checked = _normalize_node_registration(payload)
    if not checked.get("ok"):
        return _complete_command(context, command, checked)
    registration = checked["registration"]
    source = str(command.get("source_node") or "")
    if source != registration["node_id"]:
        return _complete_command(
            context, command, _error("node_identity", "heartbeat identity differs"))
    upsert_node(
        context["store"], source,
        role=payload.get("role"), state="online",
        authority_revision=payload.get("authority_revision"),
        fingerprint=payload.get("fingerprint"),
        capabilities={
            "runtime_nonce": payload.get("runtime_nonce"),
            "features": checked["capabilities"],
            "controller_uuids": payload.get("controller_uuids") or [],
        },
    )
    _queue_control(context, {
        "kind": "sync_node", "node_id": source,
        "runtime_nonce": "{}:{}".format(
            payload.get("runtime_nonce") or "heartbeat",
            command.get("message_id") or uuid.uuid4(),
        ),
        "authority_revision": payload.get("authority_revision"),
        "fingerprint": payload.get("fingerprint"),
        "node_shard_fingerprint": checked["node_replica"].get("fingerprint"),
        "cross_revision": checked["cross_replica"].get("revision"),
        "cross_fingerprint": checked["cross_replica"].get("fingerprint"),
        "cross_runtime_enabled": bool(
            checked["capabilities"].get("cross_runtime")
        ),
    })
    authority = _authority_state(context)
    result = {
        "ok": True,
        "coordinator_term": authority["coordinator_term"],
        "authority_revision": authority["authority_revision"],
        "fingerprint": authority["authority_fingerprint"],
    }
    return _complete_command(context, command, result)

def _remote_sensor_payload(envelope):
    outer = envelope.get("payload")
    outer = outer if _is_dict(outer) else {}
    inner = outer.get("payload")
    return inner if _is_dict(inner) else outer


def _claim_remote_sensor_writer(context, controller_uuid, source_node):
    with context["lock"]:
        token = context["sensor_writer_tokens"].get(controller_uuid)
    if token:
        return token
    capabilities = context["sensor_value_capabilities"]
    create_token = capabilities.get("create_writer_token")
    claim = capabilities.get("claim_writer")
    if not callable(create_token) or not callable(claim):
        return None
    token = create_token("mqtt_sensor_mirror:" + str(source_node))
    report = claim({
        "controller_uuid": controller_uuid,
        "writer_label": "mqtt_sensor_mirror:" + str(source_node),
    }, token)
    if not report.get("ok"):
        return None
    with context["lock"]:
        context["sensor_writer_tokens"][controller_uuid] = token
    return token


def _write_remote_sensor_value(context, source_node, payload):
    controller_uuid = str(payload.get("controller_uuid") or "")
    owner = get_controller_owner_node(_config_snapshot(context), controller_uuid)
    if not controller_uuid or owner != str(source_node):
        return _error("sensor_owner", "sensor event source does not own controller")
    token = _claim_remote_sensor_writer(context, controller_uuid, source_node)
    writer = context["sensor_value_capabilities"].get("write_sensor_value")
    if not token or not callable(writer):
        return _error("sensor_store", "remote sensor writer is unavailable")
    return writer(
        controller_uuid=controller_uuid,
        sensor_id=payload.get("sensor_id"),
        value=payload.get("value"),
        quality=payload.get("quality", "good"),
        timestamp=payload.get("timestamp"),
        source="mqtt_node:" + str(source_node),
        writer_token=token,
        metadata={
            "remote_sequence": payload.get("sequence"),
            "source_node": source_node,
        },
    )


def _forward_remote_sensor_event(context, envelope, payload):
    sink = context.get("runtime_event_sink")
    if not callable(sink):
        return False
    event = envelope.get("payload")
    event = copy.deepcopy(event) if _is_dict(event) else {}
    if not event.get("event_type"):
        event = {
            "event_type": "SENSOR_VALUE_UPDATE",
            "payload": copy.deepcopy(payload),
        }
    event["target"] = "thread_management"
    event["source"] = "mqtt_node:" + str(envelope.get("source_node") or "")
    event["_external_mirror_suppressed"] = True
    return bool(sink(event))


def _handle_runtime_event(context, envelope):
    source = str(envelope.get("source_node") or "")
    if source == context["options"]["node_id"]:
        return True
    message = {
        "message_id": envelope.get("message_id"),
        "command": "event:" + str(envelope.get("event_type") or "").lower(),
        "payload": envelope.get("payload") or {},
    }
    record = _inbox_start(context, message)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return bool(duplicate.get("ok"))
    if str(envelope.get("event_type") or "").upper() != "SENSOR_VALUE_UPDATE":
        return bool(_complete_command(
            context, message, {"ok": True, "status": "event_not_consumed"}))
    payload = _remote_sensor_payload(envelope)
    if context["options"]["role"] == "worker":
        handler = context.get("cross_event_handler")
        if not callable(handler):
            result = {"ok": True, "status": "cross_fact_not_required"}
            _complete_command(context, message, result)
            return True
        try:
            accepted = _accepted(handler(
                copy.deepcopy(envelope),
                source_node=source,
                received_at_s=time.time(),
            ))
        except Exception:
            LOGGER.exception("Remote sensor dispatch to Cross Worker failed")
            accepted = False
        result = {"ok": accepted, "status": "cross_fact_dispatched"}
        _complete_command(context, message, result)
        key = "remote_sensor_ingested" if accepted else "remote_sensor_rejected"
        _increment_stat(context, key)
        return accepted
    written = _write_remote_sensor_value(context, source, payload)
    if not written.get("ok"):
        _increment_stat(context, "remote_sensor_rejected")
        _complete_command(context, message, written)
        return False
    # The coordinator stores remote values for monitoring only.  It never
    # forwards them to its own RuleEngine/Automation pipeline.
    result = {
        "ok": True,
        "status": "monitoring_only",
        "sensor_store": copy.deepcopy(written),
    }
    _complete_command(context, message, result)
    _increment_stat(context, "remote_sensor_ingested")
    return True


def _replication_targets(context, config):
    configured = context["options"]["replication_targets"]
    targets = configured or get_enabled_node_ids(config)
    local = context["options"]["node_id"]
    return tuple(node_id for node_id in targets if node_id != local)


def _node_cross_runtime_enabled(context, node_id):
    identity = str(node_id or "")
    for node in list_nodes(context["store"]):
        if str(node.get("node_id") or "") != identity:
            continue
        capabilities = node.get("capabilities") or {}
        features = capabilities.get("features") or {}
        return bool(features.get("cross_runtime"))
    return False


def _authority_partition_validation(context, config):
    meta_rows = config.get("meta") if _is_dict(config) else []
    meta = meta_rows[0] if isinstance(meta_rows, list) and meta_rows else {}
    if not _is_dict(meta) or str(meta.get("scope") or "") != "master_global":
        return {"ok": True, "status": "legacy_authority"}
    try:
        storage = type_library.project_config_for_storage(config)
    except (TypeError, ValueError) as exc:
        return _error(
            "config_contract",
            "global authority cannot be projected to canonical storage",
            details=str(exc),
        )
    return validate_authority_config(
        storage,
        profile=context["options"]["authority_validation_profile"],
    )


def _storage_meta(config):
    rows = config.get("meta") if _is_dict(config) else None
    if not isinstance(rows, list) or len(rows) != 1 or not _is_dict(rows[0]):
        return {}
    return rows[0]


def _is_v9_node_storage(config, node_id):
    meta = _storage_meta(config)
    return (
        int(meta.get("version") or 0) == 9
        and str(meta.get("scope") or "") == "node_local"
        and str(meta.get("node_id") or "") == str(node_id)
    )


def _is_v8_cross_storage(config, node_id):
    meta = _storage_meta(config)
    return (
        int(meta.get("version") or 0) == 8
        and str(meta.get("scope") or "") == "cross"
        and str(meta.get("owner_node_id") or "") == str(node_id)
    )


def _stage_config_for_node(context, node_id, **kwargs):
    config = _config_snapshot(context)
    if not _is_dict(config):
        return _error("config", "coordinator config snapshot is unavailable")
    authority = _authority_state(context)
    revision = authority["authority_revision"]
    authority_fingerprint = authority["authority_fingerprint"]
    runtime_config = build_local_execution_shard(config, node_id)
    node_config = type_library.project_config_for_storage(runtime_config)
    report = type_library.verify_storage_config_data(node_config)
    if report.get("status") != "OK":
        return _error(
            "node_shard_contract", "node shard storage projection is invalid",
            validation=report)
    if not _is_v9_node_storage(node_config, node_id):
        return _error(
            "node_shard_contract", "node shard transport must be V9 storage")
    fingerprint = type_library.config_fingerprint(node_config)
    nonce = str(kwargs.get("runtime_nonce") or "commit")
    identity = "config:{}:{}:{}:{}".format(
        authority["coordinator_term"], revision, str(node_id), nonce)
    payload = {
        "protocol": CONTEXT["protocol"],
        "coordinator_node_id": context["options"]["node_id"],
        "coordinator_term": authority["coordinator_term"],
        "authority_revision": revision,
        "authority_fingerprint": authority_fingerprint,
        "shard_scope": "node_local",
        "shard_node_id": str(node_id),
        "shard_fingerprint": fingerprint,
        "fingerprint": fingerprint,
        "scope": "node:" + str(node_id),
        "target_scope_revision": revision,
        "target_scope_fingerprint": fingerprint,
        "config": node_config,
    }
    staged = enqueue_outbox(
        context["store"], identity, target_node=node_id,
        command=COMMAND_CONFIG_APPLY, payload=payload)
    if staged.get("status") == "conflict":
        return _error("outbox_conflict", "config replication ID conflict")
    _increment_stat(
        context, "config_replications_staged",
        int(staged.get("status") == "new"))
    return {"ok": True, "message_id": identity, "state": staged.get("state")}


def _stage_cross_config_for_node(context, node_id, **kwargs):
    config = _config_snapshot(context)
    if not _is_dict(config):
        return _error("config", "coordinator config snapshot is unavailable")
    partition = _authority_partition_validation(context, config)
    if not partition.get("ok"):
        return _error(
            "cross_partition",
            "global authority contains unsafe or ambiguous Cross logic",
            validation=partition,
        )
    authority = _authority_state(context)
    runtime_config = build_cross_execution_shard(config, node_id)
    validation = validate_cross_execution_shard(runtime_config, node_id)
    if not validation.get("ok"):
        return _error(
            "cross_shard", "cross shard validation failed",
            validation=validation,
        )
    revision = authority["authority_revision"]
    cross_config = type_library.project_config_for_storage(runtime_config)
    report = type_library.verify_storage_config_data(cross_config)
    if report.get("status") != "OK":
        return _error(
            "cross_shard_contract", "cross shard storage projection is invalid",
            validation=report)
    if not _is_v8_cross_storage(cross_config, node_id):
        return _error(
            "cross_shard_contract", "cross shard transport must be V8 storage")
    fingerprint = type_library.config_fingerprint(cross_config)
    classification_fingerprint = type_library.fingerprint(
        classify_config_scopes(runtime_config))
    nonce = str(kwargs.get("runtime_nonce") or "commit")
    identity = "cross-config:{}:{}:{}:{}".format(
        authority["coordinator_term"], revision, str(node_id), nonce)
    payload = {
        "protocol": CONTEXT["protocol"],
        "coordinator_node_id": context["options"]["node_id"],
        "coordinator_term": authority["coordinator_term"],
        "authority_revision": authority["authority_revision"],
        "authority_fingerprint": authority["authority_fingerprint"],
        "config_authority": context["options"]["config_authority"],
        "authority_epoch": authority["authority_epoch"],
        "cross_revision": revision,
        "cross_fingerprint": fingerprint,
        "fingerprint": fingerprint,
        "classification_fingerprint": classification_fingerprint,
        "execution_epoch": 0,
        "config": cross_config,
    }
    staged = enqueue_outbox(
        context["store"], identity, target_node=node_id,
        command=COMMAND_CROSS_CONFIG_APPLY, payload=payload)
    if staged.get("status") == "conflict":
        return _error("outbox_conflict", "cross replication ID conflict")
    _increment_stat(
        context, "cross_replications_staged",
        int(staged.get("status") == "new"))
    return {"ok": True, "message_id": identity,
            "state": staged.get("state"), "fingerprint": fingerprint}


def _record_worker_authority_commit(context, event, result):
    if not _is_dict(result) or not result.get("ok"):
        return {"ok": True, "status": "local_commit_not_applied"}
    if not result.get("changed"):
        head = get_scope_head(
            context["store"],
            "node:" + context["options"]["node_id"],
        )
        return {
            "ok": True,
            "status": "unchanged",
            "authority_epoch": head["authority_epoch"],
            "scope_revision": head["scope_revision"],
            "fingerprint": head.get("fingerprint"),
        }
    scope = "node:" + context["options"]["node_id"]
    head = get_scope_head(context["store"], scope)
    if head is None:
        return _error("authority_state", "persistent scope head is unavailable")
    config = _config_snapshot(context)
    fingerprint = type_library.config_fingerprint(config)
    next_head = authority_flow_contracts.make_scope_head(
        scope,
        authority_mode="node_local",
        authority_epoch=head["authority_epoch"],
        scope_revision=head["scope_revision"] + 1,
        execution_epoch=head["execution_epoch"],
        fingerprint=fingerprint,
        locked=False,
        node_authoritative_committed=True,
    )
    payload = event.get("payload") if _is_dict(event) else {}
    payload = payload if _is_dict(payload) else {}
    journal = persistence_contracts.make_journal_record(
        "scope_commit",
        scope=scope,
        authority_epoch=next_head["authority_epoch"],
        scope_revision=next_head["scope_revision"],
        execution_epoch=next_head["execution_epoch"],
        fingerprint=fingerprint,
        payload={
            "event_type": str(event.get("event_type") or ""),
            "operation_id": payload.get("operation_id"),
            "principal": payload.get("principal"),
            "source": payload.get("source"),
            "sem_revision": result.get("revision"),
        },
    )
    snapshot = persistence_contracts.make_scope_snapshot(
        config,
        scope=scope,
        authority_epoch=next_head["authority_epoch"],
        scope_revision=next_head["scope_revision"],
        execution_epoch=next_head["execution_epoch"],
        fingerprint=fingerprint,
    )
    mirror = persistence_contracts.make_mirror_record(
        COMMAND_MIRROR_SNAPSHOT,
        snapshot,
        state="pending",
    )
    commit_scope_state(
        context["store"],
        next_head,
        journal,
        mirror_record=mirror,
    )
    with context["lock"]:
        context["authority_epoch"] = next_head["authority_epoch"]
        context["node_scope_revision"] = next_head["scope_revision"]
        context["node_shard_fingerprint"] = fingerprint
        context["authority_fingerprint"] = fingerprint
    set_metadata(
        context["store"], "node_scope_revision", next_head["scope_revision"])
    set_metadata(context["store"], "node_shard_fingerprint", fingerprint)
    _increment_stat(context, "local_authority_commits")
    _increment_stat(context, "mirrors_staged")
    return {
        "ok": True,
        "code": "accepted",
        "status": "node_authoritative_committed",
        "authority_epoch": next_head["authority_epoch"],
        "scope_revision": next_head["scope_revision"],
        "fingerprint": fingerprint,
        "mirror_id": mirror["mirror_id"],
    }


def handle_distributed_config_commit(context, event, result):
    if (
        context["options"]["role"] == "worker"
        and context["options"]["config_authority"] == "node_local"
    ):
        return _record_worker_authority_commit(context, event, result)
    if context["options"]["role"] != "coordinator":
        return {"ok": True, "status": "not_authority"}
    if not _is_dict(result) or not result.get("ok"):
        return {"ok": True, "status": "local_commit_not_applied"}
    config = _config_snapshot(context)
    partition = _authority_partition_validation(context, config)
    if not partition.get("ok"):
        return _error(
            "cross_partition",
            "global authority commit cannot be distributed safely",
            validation=partition,
        )
    changed = bool(result.get("changed"))
    fingerprint = str(result.get("fingerprint") or _config_fingerprint(context))
    authority = _authority_state(context)
    revision = authority["authority_revision"]
    if changed:
        revision = increment_metadata(
            context["store"], "authority_revision", revision)
    with context["lock"]:
        context["authority_revision"] = revision
        context["authority_fingerprint"] = fingerprint
    set_metadata(context["store"], "authority_fingerprint", fingerprint)
    staged = []
    if changed:
        for node_id in _replication_targets(context, config):
            staged.append(_stage_config_for_node(context, node_id))
            if _node_cross_runtime_enabled(context, node_id):
                staged.append(_stage_cross_config_for_node(context, node_id))
    failures = [item for item in staged if not item.get("ok")]
    return {
        "ok": not failures,
        "authority_revision": revision,
        "fingerprint": fingerprint,
        "staged": len(staged),
        "source_event": str(event.get("event_type") or "") if _is_dict(event) else "",
    }

def _validate_replica_payload(context, payload):
    config = payload.get("config")
    if not _is_dict(config):
        return _error("config", "replication payload has no config object")
    report = type_library.verify_storage_config_data(config)
    if report.get("status") != "OK":
        return _error(
            "config_contract", "replication payload is not strict storage data",
            validation=report)
    if not _is_v9_node_storage(config, context["options"]["node_id"]):
        return _error(
            "config_contract", "replicated node config must be V9 storage")
    expected = str(
        payload.get("shard_fingerprint")
        or payload.get("fingerprint") or "")
    actual = type_library.config_fingerprint(config)
    if not expected or actual != expected:
        return _error("fingerprint", "replicated config fingerprint differs")
    revision = _safe_int(payload.get("authority_revision"), 0)
    if revision <= 0:
        return _error("authority_revision", "authority revision is invalid")
    authority = _authority_state(context)
    current = authority["authority_revision"]
    authority_fingerprint = str(
        payload.get("authority_fingerprint") or expected)
    if revision < current:
        return _error("stale_replica", "replicated config is older than local authority state")
    if (
        revision == current
        and authority["authority_fingerprint"] not in ("", authority_fingerprint)
    ):
        return _error("replica_conflict", "same authority revision has another fingerprint")
    return {
        "ok": True,
        "actual_fingerprint": actual,
        "authority_fingerprint": authority_fingerprint,
        "revision": revision,
    }


def _proposal_error(result):
    error = result.get("error") if _is_dict(result) else {}
    error = error if _is_dict(error) else {}
    return _error(
        error.get("code") or result.get("code") or "invalid_proposal",
        error.get("message") or result.get("message") or "proposal rejected",
    )


def _proposal_principal(context, command):
    source = str(command.get("source_node") or "")
    coordinator = context["options"]["coordinator_node_id"]
    if coordinator and source == coordinator:
        checked = _validate_coordinator_command_deferred(context, command)
        if not checked.get("ok"):
            return checked
        return {
            "ok": True,
            "principal": "master_relay",
            "relay_identity": {
                "source_node": source,
                "coordinator_term": checked.get("coordinator_term"),
            },
        }
    return protocol_contracts.authenticated_principal(
        command.get("transport_identity"),
        site_id=context["options"]["site_id"],
    )


def _proposal_input(context, command, legacy_kind=None):
    payload = copy.deepcopy(command.get("payload") or {})
    baseline = _config_snapshot(context)
    if legacy_kind == "apply":
        operations = normalize_legacy_ingress(
            baseline, {"config": payload.get("config")})
    elif legacy_kind == "patch":
        operations = validate_scope_operations(
            payload.get("operations"),
            operation_format=payload.get(
                "operation_format",
                protocol_contracts.OPERATION_FORMAT,
            ),
        )
    elif legacy_kind == "external":
        name = str(command.get("command") or "")
        if name in ("config.replace", "config.sync"):
            request = {"config": payload.get("config")}
        else:
            request = {
                "changes": [{
                    "path": payload.get("selector", payload.get("path")),
                    "value": payload.get("value"),
                    "operation": payload.get("operation") or name.split(".")[-1],
                }],
            }
        operations = normalize_legacy_ingress(baseline, request)
    elif isinstance(payload.get("changes"), list):
        operations = normalize_legacy_ingress(
            baseline, {"changes": payload.get("changes")})
    else:
        operations = validate_scope_operations(
            payload.get("operations"),
            operation_format=payload.get("operation_format"),
        )
    return authority_flow_contracts.make_proposal(
        payload.get("scope") or (
            "node:" + context["options"]["node_id"]),
        operations,
        proposal_id=payload.get("proposal_id") or command.get("message_id"),
        operation_id=payload.get("operation_id") or command.get("message_id"),
        operation_format=protocol_contracts.OPERATION_FORMAT,
        base_authority_epoch=payload.get("base_authority_epoch"),
        base_scope_revision=payload.get("base_scope_revision"),
        issued_at_s=payload.get("issued_at_s", command.get("created_at_s")),
        expires_at_s=payload.get("expires_at_s", command.get("expires_at_s")),
        relay_identity=payload.get("relay_identity"),
        metadata=payload.get("metadata"),
    )

def _handle_node_local_proposal(context, command, legacy_kind=None):
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    principal_result = _proposal_principal(context, command)
    if not principal_result.get("ok"):
        return _complete_command(
            context, command, _proposal_error(principal_result))
    try:
        proposal = _proposal_input(context, command, legacy_kind=legacy_kind)
    except (KeyError, TypeError, ValueError) as exc:
        code = (
            "unsupported_operation_format"
            if "operation" in str(exc).lower()
            else "invalid_proposal"
        )
        return _complete_command(context, command, _error(code, str(exc)))
    expected_scope = "node:" + context["options"]["node_id"]
    if proposal["scope"] != expected_scope:
        return _complete_command(
            context, command, _error("path_denied", "proposal targets another scope"))
    head = get_scope_head(context["store"], expected_scope)
    fence = authority_flow_contracts.check_proposal_fence(proposal, head)
    if not fence.get("ok"):
        return _complete_command(context, command, _proposal_error(fence))
    writer = context["config_capabilities"].get("request_scope_operations")
    if not callable(writer):
        return _complete_command(
            context, command,
            _error("capability", "SEM scope-operation writer is unavailable"))
    response = writer(
        proposal["operations"],
        operation_format=proposal["operation_format"],
        scope=proposal["scope"],
        source="external_proposal",
        external=True,
        principal=principal_result["principal"],
        base_revision=_local_config_revision(context),
        base_authority_epoch=proposal["base_authority_epoch"],
        base_scope_revision=proposal["base_scope_revision"],
        operation_id=proposal["operation_id"],
        relay_identity=principal_result.get("relay_identity"),
    )
    if not _is_dict(response) or not response.get("ok"):
        sem_result = response if _is_dict(response) else {}
        safety = sem_result.get("safety") if _is_dict(sem_result) else {}
        safety = safety if _is_dict(safety) else {}
        commit = sem_result.get("commit") if _is_dict(sem_result) else {}
        details = commit.get("details") if _is_dict(commit) else {}
        code = (
            safety.get("code")
            or (details.get("code") if _is_dict(details) else None)
            or "proposal_rejected"
        )
        return _complete_command(
            context,
            command,
            _error(
                code,
                commit.get("message") if _is_dict(commit) else "proposal rejected",
                sem_result=sem_result,
            ),
        )
    head = get_scope_head(context["store"], expected_scope)
    result = {
        "ok": True,
        "code": "accepted",
        "status": "accepted",
        "proposal_id": proposal["proposal_id"],
        "operation_id": proposal["operation_id"],
        "principal": principal_result["principal"],
        "authority_epoch": head["authority_epoch"],
        "scope_revision": head["scope_revision"],
        "fingerprint": head["fingerprint"],
        "post_commit": copy.deepcopy(response.get("post_commit") or {}),
    }
    return _complete_command(context, command, result, state="DONE")


def _handle_legacy_external_mutation(context, command):
    return _handle_node_local_proposal(
        context, command, legacy_kind="external")


def _handle_mirror_request(context, command):
    """Restage current authoritative scope content for Master convergence."""
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    if context["options"]["config_authority"] != "node_local":
        return _error(
            "not_config_authority",
            "Mirror requests are valid only for node-authoritative Workers",
        )
    authority_result = _validate_coordinator_command_deferred(context, command)
    if not authority_result.get("ok"):
        return authority_result
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    scope = str(payload.get("scope") or (
        "node:" + context["options"]["node_id"]))
    head = get_scope_head(context["store"], scope)
    if head is None:
        return _complete_command(
            context, command, _error("not_found", "requested Mirror scope is unknown"))
    if scope == "node:" + context["options"]["node_id"]:
        config = _config_snapshot(context)
    elif scope.startswith("cross:"):
        status = _cross_status(context)
        candidate = _cross_runtime_config(status)
        if not candidate.get("ok"):
            return _complete_command(context, command, candidate)
        config = candidate["config"]
    else:
        return _complete_command(
            context, command, _error("path_denied", "requested Mirror scope is foreign"))
    fingerprint = type_library.config_fingerprint(config)
    if fingerprint != str(head.get("fingerprint") or ""):
        return _complete_command(
            context,
            command,
            _error("stale_revision", "live scope differs from its durable Mirror head"),
        )
    snapshot = persistence_contracts.make_scope_snapshot(
        config,
        scope=scope,
        authority_epoch=head["authority_epoch"],
        scope_revision=head["scope_revision"],
        execution_epoch=head["execution_epoch"],
        fingerprint=fingerprint,
    )
    mirror = persistence_contracts.make_mirror_record(
        COMMAND_MIRROR_SNAPSHOT, snapshot, state="pending")
    stage_mirror_record(context["store"], mirror)
    _increment_stat(context, "mirrors_staged")
    return _complete_command(
        context,
        command,
        {
            "ok": True,
            "request_queued": True,
            "mirror_id": mirror["mirror_id"],
            "scope": scope,
            "authority_epoch": head["authority_epoch"],
            "scope_revision": head["scope_revision"],
            "execution_epoch": head["execution_epoch"],
            "fingerprint": fingerprint,
        },
    )


def _restore_deployment_preflight(context, restore):
    """Validate the isolated replacement Worker before hardware activation."""
    selected = restore.get("selected_scope_snapshot") or {}
    config = selected.get("config") or {}
    expected_fingerprint = str(selected.get("fingerprint") or "")
    try:
        runtime = type_library.normalize_config_for_runtime(
            config, representation="auto")
        type_report = type_library.verify_runtime_config_data(runtime)
        contract_report = type_library.inspect_config_contract(
            config, representation="auto")
        ownership_report = validate_scope_partition(runtime)
        live_fingerprint = type_library.config_fingerprint(_config_snapshot(context))
    except (KeyError, TypeError, ValueError) as exc:
        return _error("preflight_failed", str(exc))
    safety_reader = context["config_capabilities"].get("get_safety_snapshot")
    safety = safety_reader() if callable(safety_reader) else {}
    gates = safety.get("gates") if _is_dict(safety.get("gates")) else {}
    proofs = {
        "type": type_report.get("status") == "OK",
        "contract": contract_report.get("status") == "OK",
        "ownership": bool(ownership_report.get("ok")),
        "fingerprint": bool(
            expected_fingerprint and live_fingerprint == expected_fingerprint),
        "quarantined": restore.get("hardware_writes_enabled") is False,
        "safety": bool(safety.get("enabled") and gates.get("actuator_tasks")),
    }
    return {
        "ok": all(proofs.values()),
        "proofs": proofs,
        "fingerprint": live_fingerprint,
    }


def _handle_authority_transfer(context, command):
    """Execute one authenticated A5 transfer on the target Worker."""
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    authority = _validate_coordinator_command_deferred(context, command)
    if not authority.get("ok"):
        return authority
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    transitions = context.get("authority_transitions") or {}
    begin = transitions.get("begin_transfer")
    acknowledge = transitions.get("ack_transfer")
    if not callable(begin) or not callable(acknowledge):
        return _complete_command(
            context, command,
            _error("capability", "authority transition service is unavailable"))
    prepared = begin(
        payload.get("target_mode"),
        transfer_id=payload.get("transfer_id"),
        operator_confirmed=payload.get("operator_confirmed"),
        source_truth=payload.get("source_truth"),
        master_snapshot=payload.get("master_snapshot"),
        node_snapshot=payload.get("node_snapshot"),
    )
    if not prepared.get("ok"):
        return _complete_command(context, command, prepared)
    transfer = prepared["transfer"]
    expected_epoch = int(transfer["target_authority_epoch"])
    expected_fingerprint = transfer["selected_fingerprint"]
    if int(payload.get("authority_epoch") or -1) != expected_epoch:
        return _complete_command(
            context, command,
            _error(
                "authority_epoch_stale", "authority transfer epoch differs",
                authority_epoch=expected_epoch,
            ),
        )
    if payload.get("fingerprint") != expected_fingerprint:
        return _complete_command(
            context, command,
            _error(
                "stale_revision", "authority transfer fingerprint differs",
                fingerprint=expected_fingerprint,
            ),
        )
    completed = acknowledge(
        transfer["transfer_id"],
        {
            "accepted": True,
            "authority_epoch": expected_epoch,
            "fingerprint": expected_fingerprint,
        },
    )
    if not completed.get("ok"):
        return _complete_command(context, command, completed)
    if not _commit_registry_term(context, authority["coordinator_term"]):
        return _complete_command(
            context, command,
            _error("stale_fencing_term", "Registry fencing term changed"))
    result = {
        "ok": True,
        "accepted": True,
        "transfer_id": transfer["transfer_id"],
        "authority_epoch": expected_epoch,
        "fingerprint": expected_fingerprint,
        "coordinator_term": authority["coordinator_term"],
        "sem_commit_id": completed["transfer"].get("sem_commit_id"),
    }
    return _complete_command(context, command, result, state="DONE")


def _handle_restore_request(context, command):
    """Quarantine, validate and SEM-activate one authenticated A6 restore."""
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    authority = _validate_coordinator_command_deferred(context, command)
    if not authority.get("ok"):
        return authority
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    expected_scope = "node:" + context["options"]["node_id"]
    if str(payload.get("scope") or "") != expected_scope:
        return _complete_command(
            context, command,
            _error("path_denied", "restore targets another Worker scope"))
    head = get_scope_head(context["store"], expected_scope)
    target_epoch = int(payload.get("authority_epoch") or -1)
    if head is None or target_epoch != int(head["authority_epoch"]) + 1:
        return _complete_command(
            context, command,
            _error(
                "authority_epoch_stale", "restore authority epoch differs",
                authority_epoch=int((head or {}).get("authority_epoch") or 0),
            ),
        )
    if (
        payload.get("operator_confirmed") is not True
        or payload.get("identity_confirmed") is not True
    ):
        return _complete_command(
            context, command,
            _error("principal_denied", "restore confirmations are incomplete"))
    transitions = context.get("authority_transitions") or {}
    required = (
        "begin_restore", "select_restore", "validate_restore", "activate_restore")
    if any(not callable(transitions.get(name)) for name in required):
        return _complete_command(
            context, command,
            _error("capability", "restore transition service is unavailable"))
    started = transitions["begin_restore"](
        restore_id=payload.get("restore_id"),
        identity=context["options"]["node_id"],
    )
    if not started.get("ok"):
        return _complete_command(context, command, started)
    restore_id = started["restore"]["restore_id"]
    selected = transitions["select_restore"](
        restore_id,
        payload.get("selected_source"),
        payload.get("snapshot"),
        operator_confirmed=True,
        identity_confirmed=True,
    )
    if not selected.get("ok"):
        return _complete_command(context, command, selected)
    validated = transitions["validate_restore"](restore_id)
    if not validated.get("ok"):
        return _complete_command(context, command, validated)
    activated = transitions["activate_restore"](restore_id)
    if not activated.get("ok"):
        return _complete_command(context, command, activated)
    scope_head = activated.get("scope_head") or {}
    fingerprint = str(
        (activated["restore"].get("selected_scope_snapshot") or {}).get(
            "fingerprint") or "")
    if (
        int(scope_head.get("authority_epoch") or -1) != target_epoch
        or scope_head.get("fingerprint") != fingerprint
    ):
        return _complete_command(
            context, command,
            _error("stale_revision", "restore SEM readback differs"))
    if not _commit_registry_term(context, authority["coordinator_term"]):
        return _complete_command(
            context, command,
            _error("stale_fencing_term", "Registry fencing term changed"))
    result = {
        "ok": True,
        "restore_id": restore_id,
        "authority_epoch": target_epoch,
        "fingerprint": fingerprint,
        "hardware_writes_enabled": False,
        "sem_commit_id": activated["restore"].get("sem_commit_id"),
        "coordinator_term": authority["coordinator_term"],
    }
    return _complete_command(context, command, result, state="DONE")


def _handle_restore_activate(context, command):
    """Run isolated deployment preflight before enabling hardware writes."""
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    authority = _validate_coordinator_command_deferred(context, command)
    if not authority.get("ok"):
        return authority
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    transitions = context.get("authority_transitions") or {}
    snapshot_reader = transitions.get("snapshot")
    preflight = transitions.get("pass_restore_preflight")
    if not callable(snapshot_reader) or not callable(preflight):
        return _complete_command(
            context, command,
            _error("capability", "restore preflight service is unavailable"))
    latest = (snapshot_reader().get("latest_restore") or {})
    selected = latest.get("selected_scope_snapshot") or {}
    exact = bool(
        latest.get("restore_id") == payload.get("restore_id")
        and latest.get("state") == "sem_activated"
        and int(latest.get("target_authority_epoch") or -1)
        == int(payload.get("authority_epoch") or -2)
        and selected.get("fingerprint") == payload.get("fingerprint")
        and payload.get("preflight_required") is True
    )
    if not exact:
        return _complete_command(
            context, command,
            _error("stale_revision", "restore activation intent differs"))
    completed = preflight(latest["restore_id"], preflight_ok=True)
    if not completed.get("ok"):
        return _complete_command(context, command, completed)
    if not _commit_registry_term(context, authority["coordinator_term"]):
        return _complete_command(
            context, command,
            _error("stale_fencing_term", "Registry fencing term changed"))
    result = {
        "ok": True,
        "restore_id": latest["restore_id"],
        "authority_epoch": latest["target_authority_epoch"],
        "fingerprint": selected["fingerprint"],
        "preflight_ok": True,
        "hardware_writes_enabled": True,
        "coordinator_term": authority["coordinator_term"],
    }
    return _complete_command(context, command, result, state="DONE")

def _handle_config_apply(context, command):
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    if context["options"]["config_authority"] == "node_local":
        return _handle_node_local_proposal(
            context, command, legacy_kind="apply")
    authority_error = _validate_coordinator_command(context, command)
    if authority_error is not None:
        return authority_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    validation = _validate_replica_payload(context, payload)
    if not validation.get("ok"):
        return _complete_command(context, command, validation)
    revision = validation["revision"]
    fingerprint = validation["actual_fingerprint"]
    authority_fingerprint = validation["authority_fingerprint"]
    authority = _authority_state(context)
    if (
        revision == authority["authority_revision"]
        and authority_fingerprint == authority["authority_fingerprint"]
        and _config_fingerprint(context) == fingerprint
    ):
        _remember_node_scope(context, payload, fingerprint)
        rebased = rebase_config_proposals(context["store"], revision)
        converged = converge_config_proposals(
            context["store"], revision, authority_fingerprint)
        _increment_stat(context, "config_proposals_rebased", rebased)
        _increment_stat(context, "config_proposals_converged", converged)
        result = {
            "ok": True, "status": "already_current",
            "authority_revision": revision, "fingerprint": fingerprint,
            "proposals_converged": converged,
            "proposals_rebased": rebased,
        }
        return _complete_command(context, command, result)
    writer = context["config_capabilities"].get("request_replace")
    if not callable(writer):
        return _complete_command(
            context, command, _error("capability", "config replace is unavailable"))
    response = writer(
        payload["config"], source="coordinator_replication",
        external=False,
        representation="storage",
        operation_id="replica:" + str(command.get("message_id")),
    )
    if not response.get("ok"):
        result = _error("replica_commit", "local replicated config rollout failed")
        result["local_result"] = copy.deepcopy(response)
        return _complete_command(context, command, result)
    local_fingerprint = _config_fingerprint(context)
    if local_fingerprint != fingerprint:
        result = _error("post_commit_fingerprint", "local config differs after rollout")
        return _complete_command(context, command, result)
    _remember_worker_authority(
        context, revision, fingerprint=authority_fingerprint,
        term=payload.get("coordinator_term"))
    _remember_node_scope(context, payload, fingerprint)
    rebased = rebase_config_proposals(context["store"], revision)
    converged = converge_config_proposals(
        context["store"], revision, authority_fingerprint)
    _increment_stat(context, "config_replications_applied")
    _increment_stat(context, "config_proposals_converged", converged)
    _increment_stat(context, "config_proposals_rebased", rebased)
    result = {
        "ok": True,
        "status": "applied",
        "authority_revision": revision,
        "fingerprint": fingerprint,
        "authority_fingerprint": authority_fingerprint,
        "local_revision": _local_config_revision(context),
        "proposals_converged": converged,
        "proposals_rebased": rebased,
    }
    return _complete_command(context, command, result)


def _validate_config_patch_payload(context, payload):
    operations = payload.get("operations")
    if not isinstance(operations, (list, tuple)) or not operations:
        return _error(
            "operations", "differential config patch requires operations")
    expected_scope = "node:" + context["options"]["node_id"]
    canonical_scope = payload.get("scope_id")
    legacy_scope = payload.get("scope")
    if canonical_scope is None:
        canonical_scope = legacy_scope
    if (
        str(canonical_scope or "") != expected_scope
        or (legacy_scope is not None and str(legacy_scope) != str(canonical_scope))
    ):
        return _error("scope", "config patch targets another node scope")

    base_revision = _safe_int(payload.get("base_scope_revision"), -1)
    target_revision = _safe_int(payload.get("target_scope_revision"), -1)
    current_revision = int(context.get("node_scope_revision") or 0)
    if base_revision != current_revision:
        return _error(
            "base_scope_revision", "config patch scope revision differs",
            current_scope_revision=current_revision)
    if target_revision <= base_revision:
        return _error(
            "target_scope_revision", "target scope revision must advance")

    baseline = _config_snapshot(context)
    baseline_report = type_library.verify_runtime_config_data(baseline)
    if baseline_report.get("status") != "OK":
        return _error(
            "runtime_contract", "live runtime baseline is invalid",
            validation=baseline_report)
    live_fingerprint = type_library.config_fingerprint(baseline)
    recorded_fingerprint = str(
        context.get("node_shard_fingerprint") or live_fingerprint)
    base_fingerprint = str(payload.get("base_scope_fingerprint") or "")
    if (
        not base_fingerprint
        or base_fingerprint != live_fingerprint
        or base_fingerprint != recorded_fingerprint
    ):
        return _error(
            "base_scope_fingerprint", "config patch baseline fingerprint differs",
            current_scope_fingerprint=recorded_fingerprint,
            live_config_fingerprint=live_fingerprint)

    try:
        target_runtime = apply_operations(
            baseline, operations, representation="runtime")
    except (KeyError, TypeError, ValueError) as exc:
        return _error("patch_operations", "config patch operations are invalid",
                      detail=str(exc))
    runtime_report = type_library.verify_runtime_config_data(target_runtime)
    if runtime_report.get("status") != "OK":
        return _error(
            "runtime_contract", "patched runtime config is invalid",
            validation=runtime_report)
    target_storage = type_library.project_config_for_storage(target_runtime)
    storage_report = type_library.verify_storage_config_data(target_storage)
    meta_rows = target_storage.get("meta") or []
    meta = meta_rows[0] if isinstance(meta_rows, list) and meta_rows else {}
    if storage_report.get("status") != "OK":
        return _error(
            "storage_contract", "patched config has no valid storage projection",
            validation=storage_report)
    if (
        not _is_dict(meta)
        or int(meta.get("version") or 0) != 9
        or str(meta.get("scope") or "") != "node_local"
        or str(meta.get("node_id") or "") != context["options"]["node_id"]
    ):
        return _error(
            "storage_contract", "differential node target must be V9 node storage")
    target_fingerprint = type_library.config_fingerprint(target_storage)
    expected_fingerprint = str(
        payload.get("target_scope_fingerprint") or "")
    if not expected_fingerprint or expected_fingerprint != target_fingerprint:
        return _error(
            "target_scope_fingerprint", "patched target fingerprint differs",
            actual_target_scope_fingerprint=target_fingerprint)

    authority_revision = _safe_int(payload.get("authority_revision"), -1)
    authority = _authority_state(context)
    authority_fingerprint = str(payload.get("authority_fingerprint") or "")
    if authority_revision < authority["authority_revision"]:
        return _error(
            "authority_revision", "config patch authority revision is stale")
    if not authority_fingerprint:
        return _error(
            "authority_fingerprint", "config patch authority fingerprint is missing")
    return {
        "ok": True,
        "operations": copy.deepcopy(list(operations)),
        "operation_count": len(operations),
        "target_runtime": target_runtime,
        "target_scope_revision": target_revision,
        "target_scope_fingerprint": target_fingerprint,
        "authority_revision": authority_revision,
        "authority_fingerprint": authority_fingerprint,
    }


def _handle_config_patch(context, command):
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    if context["options"]["config_authority"] == "node_local":
        return _handle_node_local_proposal(
            context, command, legacy_kind="patch")
    authority = _validate_coordinator_command_deferred(context, command)
    if not authority.get("ok"):
        return authority
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    validation = _validate_config_patch_payload(context, payload)
    if not validation.get("ok"):
        return _complete_command(context, command, validation)
    writer = context["config_capabilities"].get("request_replace")
    if not callable(writer):
        return _complete_command(
            context, command,
            _error("capability", "config replace is unavailable"))
    response = writer(
        validation["target_runtime"],
        source="coordinator_replication",
        external=False,
        representation="runtime",
        operation_id="replica-patch:" + str(command.get("message_id")),
    )
    if not _is_dict(response) or not response.get("ok"):
        return _complete_command(
            context, command,
            _error("replica_commit", "local differential rollout failed",
                   local_result=response if _is_dict(response) else {}))
    local_fingerprint = _config_fingerprint(context)
    target_fingerprint = validation["target_scope_fingerprint"]
    if local_fingerprint != target_fingerprint:
        return _complete_command(
            context, command,
            _error("post_commit_fingerprint", "local config differs after patch"))

    _remember_worker_authority(
        context,
        validation["authority_revision"],
        fingerprint=validation["authority_fingerprint"],
        term=authority["coordinator_term"],
    )
    _remember_node_scope(context, payload, target_fingerprint)
    rebased = rebase_config_proposals(
        context["store"], validation["authority_revision"])
    converged = converge_config_proposals(
        context["store"], validation["authority_revision"],
        validation["authority_fingerprint"])
    _increment_stat(context, "config_replications_applied")
    _increment_stat(context, "config_patches_applied")
    _increment_stat(context, "config_proposals_rebased", rebased)
    _increment_stat(context, "config_proposals_converged", converged)
    result = {
        "ok": True,
        "status": "applied",
        "transport_mode": "differential_patch",
        "command": COMMAND_CONFIG_PATCH,
        "operation_count": validation["operation_count"],
        "authority_revision": validation["authority_revision"],
        "authority_fingerprint": validation["authority_fingerprint"],
        "target_scope_revision": validation["target_scope_revision"],
        "target_scope_fingerprint": target_fingerprint,
        "fingerprint": target_fingerprint,
        "local_revision": _local_config_revision(context),
        "proposals_converged": converged,
        "proposals_rebased": rebased,
    }
    return _complete_command(context, command, result, state="DONE")


def _cross_status(context):
    reader = context.get("cross_status_reader")
    if not callable(reader):
        return _error("capability", "cross status reader is unavailable")
    status = reader()
    if not _is_dict(status):
        return _error("cross_status", "cross status reader returned no object")
    return copy.deepcopy(status)


def _cross_scope_identity(context, payload, status):
    domain_id = str(
        status.get("domain_id")
        or payload.get("domain_id")
        or payload.get("cross_domain_id")
        or "cross-default"
    ).strip()
    if not domain_id:
        return _error("cross_scope", "cross domain identity is missing")
    scope = "cross:" + domain_id
    supplied_scope = payload.get("scope") or payload.get("scope_id")
    if supplied_scope is not None and str(supplied_scope) != scope:
        return _error("path_denied", "proposal targets another Cross scope")
    return {"ok": True, "domain_id": domain_id, "scope": scope}


def _cross_runtime_config(status):
    config = status.get("authority_config")
    if not _is_dict(config):
        config = status.get("config")
    if not _is_dict(config):
        return _error("cross_status", "current Cross authority config is unavailable")
    validation = type_library.inspect_cross_v8_contract(
        config, representation="runtime")
    if validation.get("status") != "OK":
        return _error(
            "cross_contract",
            "current Cross authority config is invalid",
            validation=validation,
        )
    return {"ok": True, "config": copy.deepcopy(config)}


def _ensure_cross_scope_head(context, scope, status, current_config=None):
    revision = _safe_int(status.get("revision"), 0)
    execution_epoch = _safe_int(status.get("execution_epoch"), 0)
    fingerprint = str(status.get("fingerprint") or "")
    if not type_library.is_fingerprint(fingerprint):
        fingerprint = (
            type_library.config_fingerprint(current_config)
            if _is_dict(current_config)
            else None
        )
    head = ensure_scope_head(
        context["store"],
        scope,
        authority_mode=context["options"]["config_authority"],
        authority_epoch=_safe_int(status.get("authority_epoch"), 0),
        scope_revision=revision,
        execution_epoch=execution_epoch,
        fingerprint=fingerprint,
        node_authoritative_committed=True,
    )
    if head["authority_mode"] != context["options"]["config_authority"]:
        return _error(
            "authority_transfer_required",
            "Cross scope authority mode differs from configured mode",
        )
    if (
        head["scope_revision"] != revision
        or head["execution_epoch"] != execution_epoch
    ):
        return _error(
            "execution_epoch_stale",
            "durable Cross scope head differs from active owner state",
            authority_epoch=head["authority_epoch"],
            scope_revision=head["scope_revision"],
            execution_epoch=head["execution_epoch"],
        )
    return {"ok": True, "head": head}


def _cross_input_operations(context, payload, current_config):
    try:
        if payload.get("operations") is not None:
            operations = validate_scope_operations(
                payload.get("operations"),
                operation_format=payload.get(
                    "operation_format", protocol_contracts.OPERATION_FORMAT),
            )
            return {"ok": True, "operations": operations}

        storage_config = payload.get("config")
        if not _is_dict(storage_config):
            return _error(
                "unsupported_operation_format",
                "Cross proposal requires canonical operations or a legacy Full config",
            )
        report = type_library.verify_storage_config_data(storage_config)
        if report.get("status") != "OK":
            return _error(
                "cross_contract",
                "Cross Full payload is not strict storage data",
                validation=report,
            )
        if not _is_v8_cross_storage(
            storage_config, context["options"]["node_id"]
        ):
            return _error(
                "cross_contract",
                "Cross Full payload must be canonical Cross-V8 storage",
            )
        expected = str(
            payload.get("target_scope_fingerprint")
            or payload.get("cross_fingerprint")
            or payload.get("fingerprint")
            or ""
        )
        actual = type_library.config_fingerprint(storage_config)
        if expected and expected != actual:
            return _error(
                "cross_fingerprint",
                "Cross Full payload fingerprint differs",
            )
        runtime_config = type_library.normalize_cross_v8_for_runtime(
            storage_config, representation="storage")
        operations = normalize_legacy_ingress(
            current_config,
            {"config": runtime_config},
            allow_cross_v8=True,
            representation="runtime",
        )
        return {"ok": True, "operations": operations}
    except (KeyError, TypeError, ValueError) as exc:
        return _error("unsupported_operation_format", str(exc))


def _cross_candidate(context, current_config, operations):
    try:
        candidate = apply_operations(
            current_config,
            operations,
            allow_cross_v8=True,
            representation="runtime",
        )
        storage = type_library.project_cross_v8_for_storage(candidate)
    except (KeyError, TypeError, ValueError) as exc:
        code = (
            "stale_revision"
            if "hash differs" in str(exc).lower()
            else "unsupported_operation_format"
        )
        return _error(code, str(exc))
    report = type_library.verify_storage_config_data(storage)
    if report.get("status") != "OK":
        return _error(
            "cross_contract",
            "candidate Cross config violates the Type Authority",
            validation=report,
        )
    runtime = type_library.normalize_cross_v8_for_runtime(
        storage, representation="storage")
    validation = validate_cross_execution_shard(
        runtime, context["options"]["node_id"])
    if not validation.get("ok"):
        return _error(
            "cross_shard",
            "candidate Cross shard validation failed",
            validation=validation,
        )
    return {
        "ok": True,
        "config": runtime,
        "fingerprint": type_library.config_fingerprint(runtime),
        "classification_fingerprint": type_library.fingerprint(
            classify_config_scopes(runtime)),
    }


def _cross_execution_epoch(context, payload, head):
    received = _safe_int(payload.get("execution_epoch"), -1, -1)
    current = head["execution_epoch"]
    if received < current or received > current + 1:
        return _error(
            "execution_epoch_stale",
            "Cross execution epoch is not acceptable",
            accepted_execution_epoch=current,
        )
    if received == current + 1:
        proof = payload.get("activation_proof")
        valid = (
            _is_dict(proof)
            and proof.get("old_owner_frozen_ack") is True
            and proof.get("registry_persisted") is True
            and bool(str(proof.get("registry_intent_id") or ""))
            and str(proof.get("new_owner_node_id") or "")
            == context["options"]["node_id"]
            and _safe_int(proof.get("execution_epoch"), -1, -1) == received
        )
        if not valid:
            return _error(
                "execution_epoch_stale",
                "Cross owner activation proof is incomplete",
                accepted_execution_epoch=current,
            )
    return {
        "ok": True,
        "execution_epoch": received,
        "advanced": received == current + 1,
    }


def _authorize_cross_operations(
    context, principal, operations, head
):
    authorizer = context["config_capabilities"].get(
        "authorize_external_scope")
    if not callable(authorizer):
        return _error(
            "capability",
            "SEM external scope authorizer is unavailable",
        )
    decision = authorizer(
        operations,
        operation_format=protocol_contracts.OPERATION_FORMAT,
        principal=principal,
        base_revision=head["scope_revision"],
        current_revision=head["scope_revision"],
    )
    if not _is_dict(decision) or not decision.get("ok"):
        rejected = decision if _is_dict(decision) else {}
        return _error(
            rejected.get("code") or "proposal_rejected",
            rejected.get("message") or "Cross proposal failed the SEM safety gate",
            safety=copy.deepcopy(rejected),
        )
    return {"ok": True, "safety": copy.deepcopy(decision)}


def _commit_cross_owner_scope(
    context, proposal, principal, candidate, head, execution
):
    next_head = authority_flow_contracts.make_scope_head(
        head["scope"],
        authority_mode="node_local",
        authority_epoch=head["authority_epoch"],
        scope_revision=head["scope_revision"] + 1,
        execution_epoch=execution["execution_epoch"],
        fingerprint=candidate["fingerprint"],
        locked=False,
        node_authoritative_committed=True,
    )
    kind = (
        "cross_owner_transition"
        if execution["advanced"]
        else "scope_commit"
    )
    journal = persistence_contracts.make_journal_record(
        kind,
        scope=next_head["scope"],
        authority_epoch=next_head["authority_epoch"],
        scope_revision=next_head["scope_revision"],
        execution_epoch=next_head["execution_epoch"],
        fingerprint=next_head["fingerprint"],
        payload={
            "proposal_id": proposal["proposal_id"],
            "operation_id": proposal["operation_id"],
            "principal": principal,
            "classification_fingerprint": candidate[
                "classification_fingerprint"],
            "activation_proof": copy.deepcopy(
                proposal["metadata"].get("activation_proof")),
        },
    )
    snapshot = persistence_contracts.make_scope_snapshot(
        candidate["config"],
        scope=next_head["scope"],
        authority_epoch=next_head["authority_epoch"],
        scope_revision=next_head["scope_revision"],
        execution_epoch=next_head["execution_epoch"],
        fingerprint=next_head["fingerprint"],
    )
    mirror = persistence_contracts.make_mirror_record(
        COMMAND_MIRROR_SNAPSHOT,
        snapshot,
        state="pending",
    )
    commit_scope_state(
        context["store"],
        next_head,
        journal,
        mirror_record=mirror,
    )
    _increment_stat(context, "mirrors_staged")
    return {
        "ok": True,
        "head": next_head,
        "mirror_id": mirror["mirror_id"],
    }


def _handle_cross_freeze(context, command, payload, status, identity):
    principal = _proposal_principal(context, command)
    if not principal.get("ok") or principal.get("principal") != "master_relay":
        return _complete_command(
            context,
            command,
            _error(
                "principal_denied",
                "only the authenticated Registry relay may freeze a Cross owner",
            ),
        )
    head_result = _ensure_cross_scope_head(
        context, identity["scope"], status)
    if not head_result.get("ok"):
        return _complete_command(context, command, head_result)
    head = head_result["head"]
    if _safe_int(payload.get("base_authority_epoch"), -1, -1) != head[
        "authority_epoch"
    ]:
        return _complete_command(
            context,
            command,
            _error(
                "authority_epoch_stale",
                "base authority epoch differs",
                authority_epoch=head["authority_epoch"],
            ),
        )
    if _safe_int(payload.get("base_scope_revision"), -1, -1) != head[
        "scope_revision"
    ]:
        return _complete_command(
            context,
            command,
            _error(
                "stale_revision",
                "base Cross scope revision differs",
                scope_revision=head["scope_revision"],
            ),
        )
    freezer = context.get("cross_freezer")
    if not callable(freezer):
        return _complete_command(
            context,
            command,
            _error("capability", "Cross execution freezer is unavailable"),
        )
    result = freezer(
        expected_execution_epoch=payload.get("execution_epoch"),
        target_execution_epoch=payload.get("target_execution_epoch"),
        freeze_token=payload.get("freeze_token"),
        registry_intent_id=payload.get("registry_intent_id"),
    )
    response = copy.deepcopy(result) if _is_dict(result) else {
        "ok": bool(result)}
    if response.get("ok"):
        response.update({
            "code": "accepted",
            "scope": identity["scope"],
            "principal": principal["principal"],
            "authority_epoch": head["authority_epoch"],
            "scope_revision": head["scope_revision"],
        })
    return _complete_command(context, command, response, state="DONE")


def _handle_cross_owner_proposal(context, command, payload, status, identity):
    principal_result = _proposal_principal(context, command)
    if not principal_result.get("ok"):
        return _complete_command(
            context, command, _proposal_error(principal_result))
    current_result = _cross_runtime_config(status)
    if not current_result.get("ok"):
        return _complete_command(context, command, current_result)
    current_config = current_result["config"]
    head_result = _ensure_cross_scope_head(
        context, identity["scope"], status, current_config=current_config)
    if not head_result.get("ok"):
        return _complete_command(context, command, head_result)
    head = head_result["head"]
    operations_result = _cross_input_operations(
        context, payload, current_config)
    if not operations_result.get("ok"):
        return _complete_command(context, command, operations_result)
    try:
        proposal = authority_flow_contracts.make_proposal(
            identity["scope"],
            operations_result["operations"],
            proposal_id=payload.get("proposal_id") or command.get("message_id"),
            operation_id=payload.get("operation_id") or command.get("message_id"),
            operation_format=protocol_contracts.OPERATION_FORMAT,
            base_authority_epoch=payload.get("base_authority_epoch"),
            base_scope_revision=payload.get("base_scope_revision"),
            issued_at_s=(payload.get("issued_at_s") or command.get("created_at_s") or time.time()),
            expires_at_s=(payload.get("expires_at_s") or command.get("expires_at_s") or time.time() + 60.0),
            relay_identity=principal_result.get("relay_identity"),
            metadata={
                "activation_proof": copy.deepcopy(
                    payload.get("activation_proof")),
            },
        )
    except (KeyError, TypeError, ValueError) as exc:
        return _complete_command(
            context, command, _error("invalid_proposal", str(exc)))
    validated = authority_flow_contracts.validate_proposal(proposal)
    if not validated.get("ok"):
        return _complete_command(
            context, command, _proposal_error(validated))
    fence = authority_flow_contracts.check_proposal_fence(proposal, head)
    if not fence.get("ok"):
        return _complete_command(context, command, _proposal_error(fence))
    safety = _authorize_cross_operations(
        context,
        principal_result["principal"],
        proposal["operations"],
        head,
    )
    if not safety.get("ok"):
        return _complete_command(context, command, safety)
    candidate = _cross_candidate(
        context, current_config, proposal["operations"])
    if not candidate.get("ok"):
        return _complete_command(context, command, candidate)
    supplied_classification = str(
        payload.get("classification_fingerprint") or "")
    if (
        not supplied_classification
        or supplied_classification != candidate["classification_fingerprint"]
    ):
        return _complete_command(
            context,
            command,
            _error(
                "classification_mismatch",
                "owner classification differs from sender",
                classification_fingerprint=candidate[
                    "classification_fingerprint"],
            ),
        )
    supplied_fingerprint = str(
        payload.get("target_scope_fingerprint")
        or payload.get("cross_fingerprint")
        or payload.get("fingerprint")
        or ""
    )
    if supplied_fingerprint and supplied_fingerprint != candidate["fingerprint"]:
        return _complete_command(
            context,
            command,
            _error(
                "cross_fingerprint",
                "candidate Cross fingerprint differs from sender",
            ),
        )
    execution = _cross_execution_epoch(context, payload, head)
    if not execution.get("ok"):
        return _complete_command(context, command, execution)
    applier = context.get("cross_config_applier")
    if not callable(applier):
        return _complete_command(
            context,
            command,
            _error("capability", "Cross config applier is unavailable"),
        )
    package = {
        "config": copy.deepcopy(candidate["config"]),
        "revision": head["scope_revision"] + 1,
        "fingerprint": candidate["fingerprint"],
        "classification_fingerprint": candidate[
            "classification_fingerprint"],
        "execution_epoch": execution["execution_epoch"],
        "activation_proof": copy.deepcopy(payload.get("activation_proof")),
        "authority_epoch": head["authority_epoch"],
        "proposal_id": proposal["proposal_id"],
        "operation_id": proposal["operation_id"],
    }
    applied = applier(package)
    response = copy.deepcopy(applied) if _is_dict(applied) else {
        "ok": bool(applied)}
    if not response.get("ok"):
        return _complete_command(context, command, response)
    try:
        committed = _commit_cross_owner_scope(
            context,
            proposal,
            principal_result["principal"],
            candidate,
            head,
            execution,
        )
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        return _complete_command(
            context,
            command,
            _error(
                "authority_state",
                "Cross runtime changed but durable scope commit failed: " + str(exc),
            ),
        )
    _increment_stat(context, "cross_replications_applied")
    result = {
        "ok": True,
        "code": "accepted",
        "status": response.get("status") or "accepted",
        "proposal_id": proposal["proposal_id"],
        "operation_id": proposal["operation_id"],
        "principal": principal_result["principal"],
        "scope": identity["scope"],
        "authority_epoch": committed["head"]["authority_epoch"],
        "scope_revision": committed["head"]["scope_revision"],
        "execution_epoch": committed["head"]["execution_epoch"],
        "fingerprint": committed["head"]["fingerprint"],
        "classification_fingerprint": candidate[
            "classification_fingerprint"],
        "mirror_id": committed["mirror_id"],
    }
    return _complete_command(context, command, result, state="DONE")


def _handle_cross_config_apply_master_global(context, command):
    authority_error = _validate_coordinator_command(context, command)
    if authority_error is not None:
        return authority_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    config = payload.get("config")
    revision = _safe_int(payload.get("cross_revision"), -1)
    expected = str(
        payload.get("cross_fingerprint")
        or payload.get("fingerprint") or "")
    if not _is_dict(config) or revision < 0:
        return _complete_command(
            context, command,
            _error("cross_config", "cross replication payload is invalid"),
        )
    report = type_library.verify_storage_config_data(config)
    if report.get("status") != "OK":
        return _complete_command(
            context, command,
            _error("cross_contract", "cross payload is not strict storage data",
                   validation=report),
        )
    if not _is_v8_cross_storage(config, context["options"]["node_id"]):
        return _complete_command(
            context, command,
            _error("cross_contract", "replicated cross config must be V8 storage"),
        )
    actual = type_library.config_fingerprint(config)
    if not expected or actual != expected:
        return _complete_command(
            context, command,
            _error("cross_fingerprint", "cross shard fingerprint differs"),
        )
    runtime_config = type_library.normalize_cross_v8_for_runtime(
        config, representation="storage")
    classification_fingerprint = type_library.fingerprint(
        classify_config_scopes(runtime_config))
    supplied_classification = str(
        payload.get("classification_fingerprint") or "")
    if (
        not supplied_classification
        or supplied_classification != classification_fingerprint
    ):
        return _complete_command(
            context, command,
            _error(
                "classification_mismatch",
                "Cross classification differs from coordinator",
                classification_fingerprint=classification_fingerprint,
            ),
        )
    validation = validate_cross_execution_shard(
        runtime_config, context["options"]["node_id"])
    if not validation.get("ok"):
        return _complete_command(
            context, command,
            _error("cross_shard", "cross shard validation failed",
                   validation=validation),
        )
    applier = context.get("cross_config_applier")
    if not callable(applier):
        return _complete_command(
            context, command,
            _error("capability", "cross config applier is unavailable"),
        )
    status = _cross_status(context)
    if not status.get("ok", True):
        return _complete_command(context, command, status)
    response = applier({
        "config": copy.deepcopy(runtime_config),
        "revision": revision,
        "fingerprint": expected,
        "classification_fingerprint": classification_fingerprint,
        "execution_epoch": _safe_int(
            payload.get("execution_epoch"),
            _safe_int(status.get("execution_epoch"), 0),
        ),
        "authority_revision": payload.get("authority_revision"),
        "authority_fingerprint": payload.get("authority_fingerprint"),
    })
    result = copy.deepcopy(response) if _is_dict(response) else {
        "ok": bool(response)}
    if result.get("ok"):
        _increment_stat(context, "cross_replications_applied")
    return _complete_command(context, command, result)


def _handle_cross_config_apply(context, command):
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    if context["options"]["config_authority"] == "master_global":
        return _handle_cross_config_apply_master_global(context, command)
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    status = _cross_status(context)
    if not status.get("ok", True):
        return _complete_command(context, command, status)
    identity = _cross_scope_identity(context, payload, status)
    if not identity.get("ok"):
        return _complete_command(context, command, identity)
    if str(payload.get("activation_phase") or "") == "freeze_old_owner":
        return _handle_cross_freeze(
            context, command, payload, status, identity)
    return _handle_cross_owner_proposal(
        context, command, payload, status, identity)

def _handle_config_proposal(context, command):
    if context["options"]["role"] == "worker":
        if context["options"]["config_authority"] != "node_local":
            return _error("not_config_authority", "Worker uses legacy master authority")
        return _handle_node_local_proposal(context, command)
    role_error = _require_role(context, "coordinator")
    if role_error is not None:
        return role_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    source = str(command.get("source_node") or "")
    known = {node["node_id"] for node in list_nodes(context["store"])}
    if source not in known:
        return _complete_command(
            context, command, _error("node_not_registered", "source node is unknown"))
    payload = command.get("payload") or {}
    supplied_revision = _safe_int(payload.get("authority_revision"), 0)
    authority = _authority_state(context)
    if supplied_revision != authority["authority_revision"]:
        result = _error(
            "stale_authority_revision", "config proposal is based on stale config",
            current_authority_revision=authority["authority_revision"],
        )
        return _complete_command(context, command, result)
    operation = str(payload.get("operation") or "")
    if operation == "scope_operations":
        writer = context["config_capabilities"].get("request_scope_operations")
        if not callable(writer):
            return _complete_command(
                context, command,
                _error("capability", "scope operation writer unavailable"))
        response = writer(
            payload.get("operations") or [],
            scope=payload.get("scope"),
            source="distributed_config_file:" + source,
            base_revision=_local_config_revision(context),
            operation_id=payload.get("operation_id") or command.get("message_id"),
        )
        result = response if _is_dict(response) else {"ok": bool(response)}
        return _complete_command(context, command, result)
    if operation != "action_plan":
        return _complete_command(
            context, command,
            _error("operation", "proposal operation is unsupported"),
        )
    writer = context["config_capabilities"].get("request_action_plan")
    if not callable(writer):
        return _complete_command(
            context, command, _error("capability", "action plan writer unavailable"))
    response = writer(
        payload.get("actions") or [],
        source="distributed_rule_engine:" + source,
        base_revision=_local_config_revision(context),
        operation_id=payload.get("operation_id") or command.get("message_id"),
    )
    result = response if _is_dict(response) else {"ok": bool(response)}
    return _complete_command(context, command, result)


def authorize_sem_write(context, event_name, payload):
    if not context["options"]["enabled"]:
        return {"ok": True, "scope": "standalone"}
    if context["options"]["role"] != "worker":
        return {"ok": True}
    source_name = str(payload.get("source") or "") if _is_dict(payload) else ""
    if source_name == "authority_transfer":
        transitions = context.get("authority_transitions") or {}
        authorizer = transitions.get("authorize_transfer_sem_write")
        if not callable(authorizer):
            return {
                "ok": False,
                "code": "principal_denied",
                "message": "authority transition service is unavailable",
            }
        return authorizer(payload)
    if source_name == "disaster_restore":
        transitions = context.get("authority_transitions") or {}
        authorizer = transitions.get("authorize_restore_sem_write")
        if not callable(authorizer):
            return {
                "ok": False,
                "code": "principal_denied",
                "message": "restore transition service is unavailable",
            }
        return authorizer(payload)
    if str(event_name) in ("CONFIG_SYNC_REQUEST", "INITIAL_FULL_SYNC"):
        return {"ok": True}
    if context["options"]["config_authority"] == "master_global":
        if source_name == "coordinator_replication":
            return {"ok": True}
    elif source_name == "coordinator_replication":
        return {
            "ok": False,
            "code": "not_config_authority",
            "message": "Master decree is forbidden in node_local mode",
        }
    if source_name == "external_proposal":
        scope = "node:" + context["options"]["node_id"]
        head = get_scope_head(context["store"], scope)
        if int(payload.get("base_authority_epoch", -1)) != head["authority_epoch"]:
            return {
                "ok": False,
                "code": "authority_epoch_stale",
                "message": "base authority epoch differs",
                "authority_epoch": head["authority_epoch"],
            }
        if int(payload.get("base_scope_revision", -1)) != head["scope_revision"]:
            return {
                "ok": False,
                "code": "stale_revision",
                "message": "base scope revision differs",
                "scope_revision": head["scope_revision"],
            }
        return {"ok": True, "scope": scope}
    if (
        str(event_name) == "CONFIG_REPLACE"
        and source_name == "worker_file_rollback"
    ):
        return {"ok": True, "scope": "worker_file_rollback"}
    if (
        str(event_name) == "CONFIG_ACTION_PLAN"
        and source_name == "worker_local_automation"
        and context["options"]["local_automation_enabled"]
        and context["options"]["config_sync_mode"] == "local_first"
    ):
        return {"ok": True, "scope": "worker_local_automation"}
    return {
        "ok": False,
        "code": "not_config_authority",
        "message": "write source is not authorized for the active authority mode",
        "coordinator_node_id": context["options"]["coordinator_node_id"],
    }

def _wait_for_authority_revision(context, result, timeout_s):
    post_commit = result.get("post_commit") or {}
    expected = _safe_int(post_commit.get("authority_revision"), 0)
    fingerprint = str(post_commit.get("fingerprint") or "")
    authority = _authority_state(context)
    if expected <= authority["authority_revision"]:
        return True
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and not context["stop_event"].is_set():
        authority = _authority_state(context)
        if authority["authority_revision"] >= expected:
            if not fingerprint or authority["authority_fingerprint"] == fingerprint:
                return True
        time.sleep(0.02)
    return False


def _stage_local_action_plan(context, actions, local_result, operation_id):
    authority = _authority_state(context)
    message_id = "config-proposal-local:{}:{}".format(
        context["options"]["node_id"], operation_id)
    payload = {
        "protocol": CONTEXT["protocol"],
        "operation": "action_plan",
        "operation_id": operation_id,
        "authority_scope": "worker_local_runtime",
        "authority_revision": authority["authority_revision"],
        "local_revision": local_result.get("revision"),
        "local_fingerprint": local_result.get("fingerprint"),
        "proposal_id": message_id,
        "actions": copy.deepcopy(actions or []),
    }
    staged = enqueue_outbox(
        context["store"], message_id,
        target_node=context["options"]["coordinator_node_id"],
        command=COMMAND_CONFIG_PROPOSE, payload=payload,
    )
    if staged.get("status") == "conflict":
        return _error(
            "outbox_conflict", "local config proposal outbox conflict",
            local_committed=True, local_result=copy.deepcopy(local_result))
    _increment_stat(
        context, "local_config_proposals_staged",
        int(staged.get("status") == "new"))
    result = copy.deepcopy(local_result)
    result["local_committed"] = True
    result["sync"] = {
        "mode": "local_first",
        "state": staged.get("state"),
        "message_id": message_id,
        "authority_revision": authority["authority_revision"],
        "durable": True,
    }
    return result


def _node_local_action_result(local_result):
    """Expose the durable node-authority receipt without a Master proposal."""
    post_commit = local_result.get("post_commit")
    post_commit = post_commit if _is_dict(post_commit) else {}
    epoch = post_commit.get("authority_epoch")
    revision = post_commit.get("scope_revision")
    fingerprint = str(post_commit.get("fingerprint") or "")
    changed = bool(local_result.get("changed"))
    status = str(post_commit.get("status") or "")
    mirror_id = str(post_commit.get("mirror_id") or "")
    valid_numbers = all(
        isinstance(value, int) and not isinstance(value, bool)
        for value in (epoch, revision))
    valid_fingerprint = (
        len(fingerprint) == 64
        and all(character in "0123456789abcdef" for character in fingerprint))
    valid_status = (
        status == "node_authoritative_committed" and bool(mirror_id)
        if changed else status == "unchanged")
    if (
        not post_commit.get("ok")
        or not valid_numbers
        or epoch < 0
        or revision < 0
        or not valid_fingerprint
        or not valid_status
    ):
        return _error(
            "authority_journal",
            "local commit has no valid durable node-authority receipt",
            local_committed=True,
            local_result=copy.deepcopy(local_result),
        )
    result = copy.deepcopy(local_result)
    result["local_committed"] = True
    result["sync"] = {
        "mode": "node_local",
        "state": "COMMITTED" if changed else "NO_CHANGE",
        "durable": True,
        "authority_epoch": epoch,
        "scope_revision": revision,
        "fingerprint": fingerprint,
        "mirror_id": mirror_id,
    }
    return result


def _request_local_first_action_plan(context, actions, **kwargs):
    writer = context["config_capabilities"].get("request_action_plan")
    if not callable(writer):
        return _error("capability", "local action plan writer unavailable")
    operation_id = str(kwargs.get("operation_id") or uuid.uuid4())
    local_result = writer(
        actions,
        source="worker_local_automation",
        base_revision=kwargs.get("base_revision"),
        operation_id=operation_id,
        timeout_s=kwargs.get("timeout_s"),
    )
    if not _is_dict(local_result) or not local_result.get("ok"):
        return local_result if _is_dict(local_result) else _error(
            "local_commit", "local action plan commit failed")
    if context["options"]["config_authority"] == "node_local":
        return _node_local_action_result(local_result)
    if not local_result.get("changed"):
        result = copy.deepcopy(local_result)
        result["local_committed"] = True
        result["sync"] = {
            "mode": "local_first", "state": "NO_CHANGE", "durable": True}
        return result
    try:
        return _stage_local_action_plan(
            context, actions, local_result, operation_id)
    except Exception as exc:
        LOGGER.exception("Durable local config proposal staging failed")
        return _error(
            "proposal_journal", "local commit could not be journaled for sync",
            local_committed=True, local_result=copy.deepcopy(local_result),
            detail=str(exc))


def stage_applied_action_plan(context, actions, **kwargs):
    """Journal a startup-loaded edit only for the legacy Master path."""
    if context["options"]["role"] != "worker":
        return _error("role", "pre-applied proposals are Worker-only")
    if context["options"]["config_authority"] == "node_local":
        return _error(
            "startup_preapplied_forbidden",
            "node-local startup edits require a fresh SEM authority commit",
        )
    operation_id = str(kwargs.get("operation_id") or uuid.uuid4())
    local_result = {
        "ok": True,
        "changed": True,
        "revision": _local_config_revision(context),
        "fingerprint": _config_fingerprint(context),
        "startup_preapplied": True,
    }
    try:
        return _stage_local_action_plan(
            context, actions, local_result, operation_id)
    except Exception as exc:
        LOGGER.exception("Pre-applied config proposal staging failed")
        return _error(
            "proposal_journal", "pre-applied config could not be journaled",
            local_committed=True, local_result=local_result, detail=str(exc))


def request_authoritative_action_plan(context, actions, **kwargs):
    if context["options"]["role"] == "coordinator":
        writer = context["config_capabilities"].get("request_action_plan")
        if not callable(writer):
            return _error("capability", "action plan writer unavailable")
        return writer(actions, **kwargs)
    if (
        context["options"]["local_automation_enabled"]
        and context["options"]["config_sync_mode"] == "local_first"
    ):
        return _request_local_first_action_plan(context, actions, **kwargs)
    operation_id = str(kwargs.get("operation_id") or uuid.uuid4())
    authority = _authority_state(context)
    message_id = "config-proposal:{}:{}".format(operation_id, uuid.uuid4())
    waiter = {"event": threading.Event(), "result": None}
    with context["lock"]:
        context["ack_waiters"][message_id] = waiter
    payload = {
        "protocol": CONTEXT["protocol"],
        "operation": "action_plan",
        "operation_id": operation_id,
        "authority_revision": authority["authority_revision"],
        "actions": copy.deepcopy(actions or []),
    }
    staged = enqueue_outbox(
        context["store"], message_id,
        target_node=context["options"]["coordinator_node_id"],
        command=COMMAND_CONFIG_PROPOSE, payload=payload,
    )
    if staged.get("status") == "conflict":
        with context["lock"]:
            context["ack_waiters"].pop(message_id, None)
        return _error("outbox_conflict", "config proposal outbox conflict")
    timeout = _safe_float(
        kwargs.get("timeout_s"), context["options"]["proposal_timeout_s"], 0.1)
    completed = waiter["event"].wait(timeout)
    with context["lock"]:
        context["ack_waiters"].pop(message_id, None)
    if not completed or not _is_dict(waiter.get("result")):
        return _error("proposal_timeout", "coordinator config proposal timed out")
    result = copy.deepcopy(waiter["result"])
    if result.get("ok") and not _wait_for_authority_revision(context, result, timeout):
        return _error(
            "replication_timeout",
            "coordinator committed proposal but worker rollout timed out",
            coordinator_result=result,
        )
    return result


def _request_master_global_scope_operations(context, operations, **kwargs):
    """Durably propose one validated node-scope file edit to the Master."""
    if context["options"]["role"] != "worker":
        return _error("role", "scope proposals are Worker-only")
    scope = str(kwargs.get("scope") or "")
    expected_scope = "node:" + context["options"]["node_id"]
    if scope != expected_scope:
        return _error("scope", "scope proposal does not belong to this Worker")
    operation_id = str(kwargs.get("operation_id") or uuid.uuid4())
    message_id = "config-proposal-file:{}:{}".format(
        context["options"]["node_id"], operation_id)
    payload = {
        "protocol": "automation.distributed.v2",
        "contracts_version": protocol_contracts.CONTRACTS_VERSION,
        "operation": "scope_operations",
        "proposal_id": message_id,
        "operation_id": operation_id,
        "proposal_origin": str(kwargs.get("proposal_origin") or "worker_config_file"),
        "authority_scope": "worker_file_master_first",
        "authority_revision": _authority_state(context)["authority_revision"],
        "scope": scope,
        "base_scope_revision": _safe_int(kwargs.get("base_scope_revision"), 0),
        "base_scope_fingerprint": str(kwargs.get("base_scope_fingerprint") or ""),
        "reason": str(kwargs.get("reason") or "validated Worker scope proposal"),
        "operations": copy.deepcopy(operations or []),
    }
    waiter = {"event": threading.Event(), "result": None}
    with context["lock"]:
        context["ack_waiters"][message_id] = waiter
    staged = enqueue_outbox(
        context["store"], message_id,
        target_node=context["options"]["coordinator_node_id"],
        command=COMMAND_CONFIG_PROPOSE, payload=payload,
    )
    if staged.get("status") == "conflict":
        with context["lock"]:
            context["ack_waiters"].pop(message_id, None)
        return _error("outbox_conflict", "scope proposal outbox conflict")
    timeout = _safe_float(
        kwargs.get("timeout_s"), context["options"]["proposal_timeout_s"], 0.1)
    completed = waiter["event"].wait(timeout)
    with context["lock"]:
        context["ack_waiters"].pop(message_id, None)
    result = copy.deepcopy(waiter.get("result") or {}) if completed else {
        "ok": False,
        "code": "proposal_timeout",
        "message": "Master acknowledgement is still pending",
    }
    result["message_id"] = message_id
    result["durable"] = True
    result["sync"] = {
        "mode": "master_first", "state": staged.get("state"),
        "message_id": message_id, "durable": True,
    }
    if result.get("ok") and not _wait_for_authority_revision(context, result, timeout):
        result["replication_pending"] = True
    return result



def request_scope_operations(context, operations, **kwargs):
    """Commit locally in node_local or use the explicit legacy Master path."""
    if context["options"]["config_authority"] == "master_global":
        return _request_master_global_scope_operations(
            context, operations, **kwargs)
    if context["options"]["role"] != "worker":
        return _error("role", "node-local scope commits are Worker-only")
    scope = str(kwargs.get("scope") or "")
    expected_scope = "node:" + context["options"]["node_id"]
    if scope != expected_scope:
        return _error("path_denied", "scope proposal belongs to another Worker")
    head = get_scope_head(context["store"], scope)
    base_epoch = _safe_int(
        kwargs.get("base_authority_epoch"), head["authority_epoch"])
    base_revision = _safe_int(
        kwargs.get("base_scope_revision"), head["scope_revision"])
    proposal = authority_flow_contracts.make_proposal(
        scope,
        operations,
        operation_id=kwargs.get("operation_id"),
        operation_format=protocol_contracts.OPERATION_FORMAT,
        base_authority_epoch=base_epoch,
        base_scope_revision=base_revision,
        metadata={"origin": kwargs.get("proposal_origin", "worker_config_file")},
    )
    writer = context["config_capabilities"].get("request_scope_operations")
    if not callable(writer):
        return _error("capability", "SEM scope-operation writer is unavailable")
    principal = str(kwargs.get("principal") or "worker_file_sync")
    return writer(
        proposal["operations"],
        operation_format=proposal["operation_format"],
        scope=scope,
        source="external_proposal",
        external=True,
        principal=principal,
        base_revision=_local_config_revision(context),
        base_authority_epoch=proposal["base_authority_epoch"],
        base_scope_revision=proposal["base_scope_revision"],
        operation_id=proposal["operation_id"],
    )

def get_config_proposal(context, message_id):
    return get_outbox(context["store"], str(message_id or ""))


def _operation_id(task):
    return str(task.get("task_uuid") or task.get("operation_id") or uuid.uuid4())


def _owner_for_task(context, task):
    return get_controller_owner_node(
        _config_snapshot(context), task.get("controller_uuid"))


def _cross_task_authority(task):
    metadata = task.get("meta") if _is_dict(task.get("meta")) else {}
    source = str(task.get("source") or "").lower()
    scope = str(
        metadata.get("execution_scope")
        or task.get("execution_scope")
        or ""
    ).lower()
    if scope != "cross" and not source.startswith("cross_automation:"):
        return {"ok": True, "cross": False}
    domain_id = str(
        metadata.get("cross_domain_id")
        or task.get("cross_domain_id")
        or ""
    ).strip()
    owner_node_id = str(
        metadata.get("cross_owner_node_id")
        or task.get("cross_owner_node_id")
        or ""
    ).strip()
    top_epoch = task.get("execution_epoch")
    metadata_epoch = metadata.get("execution_epoch")
    if (
        top_epoch is not None
        and metadata_epoch is not None
        and _safe_int(top_epoch, -1, -1)
        != _safe_int(metadata_epoch, -2, -1)
    ):
        return _error(
            "execution_epoch_stale",
            "Cross task epoch aliases differ",
        )
    execution_epoch = _safe_int(
        top_epoch if top_epoch is not None else metadata_epoch,
        -1,
        -1,
    )
    if not domain_id or not owner_node_id or execution_epoch < 0:
        return _error(
            "execution_epoch_stale",
            "Cross task authority metadata is incomplete",
        )
    return {
        "ok": True,
        "cross": True,
        "domain_id": domain_id,
        "registry_id": "cross:" + domain_id,
        "owner_node_id": owner_node_id,
        "execution_epoch": execution_epoch,
        "authority_epoch": _safe_int(
            metadata.get("authority_epoch"), 0),
        "transition": copy.deepcopy(
            metadata.get("registry_transition")
            or task.get("registry_transition")),
    }


def _cross_registry_transition_valid(authority, current):
    proof = authority.get("transition")
    if not _is_dict(proof):
        return False
    received = authority["execution_epoch"]
    current_epoch = (
        int(current["execution_epoch"])
        if _is_dict(current)
        else -1
    )
    consecutive = (
        received == current_epoch + 1
        if current_epoch >= 0
        else received >= 1
    )
    return (
        consecutive
        and proof.get("old_owner_frozen_ack") is True
        and proof.get("registry_persisted") is True
        and str(proof.get("new_owner_node_id") or "")
        == authority["owner_node_id"]
        and _safe_int(proof.get("execution_epoch"), -1, -1) == received
        and bool(str(proof.get("registry_intent_id") or ""))
    )


def _accept_cross_hardware_epoch(context, task, source_node=None):
    authority = _cross_task_authority(task)
    if not authority.get("ok") or not authority.get("cross"):
        return authority
    authenticated_owner = str(
        source_node
        or task.get("distributed_request_source_node")
        or authority["owner_node_id"]
        or ""
    )
    if authenticated_owner != authority["owner_node_id"]:
        return _error(
            "principal_denied",
            "Cross task source differs from the execution owner",
        )
    current = get_registry_view(
        context["store"], authority["registry_id"])
    received = authority["execution_epoch"]
    if current is not None:
        accepted = int(current["execution_epoch"])
        current_registry = current.get("registry") or {}
        current_owner = str(
            current_registry.get("owner_node_id") or "")
        if received < accepted:
            return _error(
                "execution_epoch_stale",
                "Cross task uses an older execution epoch",
                accepted_execution_epoch=accepted,
            )
        if received == accepted:
            if current_owner and current_owner != authority["owner_node_id"]:
                return _error(
                    "execution_epoch_stale",
                    "Cross owner differs at the accepted execution epoch",
                    accepted_execution_epoch=accepted,
                )
            return {
                "ok": True,
                "cross": True,
                "registry_id": authority["registry_id"],
                "execution_epoch": accepted,
                "status": "already_accepted",
            }
        if not _cross_registry_transition_valid(authority, current):
            return _error(
                "execution_epoch_stale",
                "Cross execution epoch advance lacks a valid Registry transition",
                accepted_execution_epoch=accepted,
            )
    elif received != 0 and not _cross_registry_transition_valid(
        authority, None
    ):
        return _error(
            "execution_epoch_stale",
            "initial nonzero Cross epoch lacks a valid Registry transition",
            accepted_execution_epoch=-1,
        )
    registry = {
        "domain_id": authority["domain_id"],
        "owner_node_id": authority["owner_node_id"],
        "registry_intent_id": str(
            (authority.get("transition") or {}).get(
                "registry_intent_id") or "initial"),
        "accepted_by_node_id": context["options"]["node_id"],
    }
    try:
        persisted = persist_registry_view(
            context["store"],
            authority["registry_id"],
            registry,
            authority_epoch=authority["authority_epoch"],
            execution_epoch=received,
        )
    except (KeyError, TypeError, ValueError) as exc:
        return _error(
            "authority_state",
            "Cross Registry view could not be persisted: " + str(exc),
        )
    return {
        "ok": True,
        "cross": True,
        "registry_id": persisted["registry_id"],
        "execution_epoch": persisted["execution_epoch"],
        "status": "accepted",
    }

def _stage_execute(context, task, source_node):
    operation_id = _operation_id(task)
    prepared = copy.deepcopy(task)
    prepared["task_uuid"] = operation_id
    owner = _owner_for_task(context, prepared)
    if not owner:
        return _error("controller_owner", "controller has no unique owner node")
    authority = _authority_state(context)
    record = register_actuator_operation(
        context["store"], operation_id, prepared,
        direction="outbound", source_node=source_node,
        target_node=owner, controller_uuid=prepared.get("controller_uuid"),
        fencing_term=authority["coordinator_term"],
    )
    if record.get("status") == "conflict":
        return _error("operation_conflict", "actuator operation ID was reused")
    message_id = "actuator-execute:{}:{}".format(
        authority["coordinator_term"], operation_id)
    payload = {
        "protocol": CONTEXT["protocol"],
        "operation_id": operation_id,
        "coordinator_node_id": context["options"]["coordinator_node_id"],
        "coordinator_term": authority["coordinator_term"],
        "authority_revision": authority["authority_revision"],
        "require_hardware_readback": context["options"]["require_hardware_readback"],
        "request_source_node": source_node,
        "task": prepared,
    }
    staged = enqueue_outbox(
        context["store"], message_id, target_node=owner,
        command=COMMAND_ACTUATOR_EXECUTE, payload=payload)
    if staged.get("status") == "conflict":
        return _error("outbox_conflict", "actuator execute outbox conflict")
    transition_actuator_operation(
        context["store"], operation_id, "ROUTED",
        allowed_states=("RECEIVED", "ROUTED"))
    _increment_stat(
        context, "remote_actuators_staged",
        int(staged.get("status") == "new"))
    return {
        "ok": True, "accepted": True, "operation_id": operation_id,
        "owner_node": owner, "message_id": message_id,
    }


def dispatch_remote_actuator(context, task):
    if not _is_dict(task):
        return False
    role = context["options"]["role"]
    source = context["options"]["node_id"]
    if role == "coordinator":
        return bool(_stage_execute(context, task, source).get("ok"))
    operation_id = _operation_id(task)
    authority = _authority_state(context)
    prepared = copy.deepcopy(task)
    prepared["task_uuid"] = operation_id
    owner = _owner_for_task(context, prepared)
    record = register_actuator_operation(
        context["store"], operation_id, prepared,
        direction="outbound", source_node=source,
        target_node=owner or "", controller_uuid=prepared.get("controller_uuid"),
        fencing_term=authority["coordinator_term"],
    )
    if record.get("status") == "conflict":
        return False
    payload = {
        "protocol": CONTEXT["protocol"],
        "operation_id": operation_id,
        "authority_revision": authority["authority_revision"],
        "requested_owner": owner,
        "task": prepared,
    }
    message_id = "actuator-route:{}".format(operation_id)
    staged = enqueue_outbox(
        context["store"], message_id,
        target_node=context["options"]["coordinator_node_id"],
        command=COMMAND_ACTUATOR_ROUTE, payload=payload,
    )
    if staged.get("status") == "conflict":
        return False
    transition_actuator_operation(
        context["store"], operation_id, "ROUTED",
        allowed_states=("RECEIVED", "ROUTED"))
    _increment_stat(
        context, "remote_actuators_staged",
        int(staged.get("status") == "new"))
    return True


def _handle_actuator_route(context, command):
    role_error = _require_role(context, "coordinator")
    if role_error is not None:
        return role_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    source = str(command.get("source_node") or "")
    known = {node["node_id"] for node in list_nodes(context["store"])}
    if source not in known:
        return _complete_command(
            context, command, _error("node_not_registered", "source node is unknown"))
    payload = command.get("payload") or {}
    task = payload.get("task")
    if not _is_dict(task):
        return _complete_command(
            context, command, _error("task", "actuator route has no task"))
    result = _stage_execute(context, task, source)
    return _complete_command(context, command, result)


def _dispatch_inbound_operation(context, operation):
    dispatcher = context.get("local_actuator_dispatcher")
    if not callable(dispatcher):
        return _error("capability", "local actuator dispatcher is unavailable")
    task = copy.deepcopy(operation.get("task") or {})
    epoch_decision = _accept_cross_hardware_epoch(
        context,
        task,
        source_node=task.get("distributed_request_source_node"),
    )
    if not epoch_decision.get("ok"):
        transition_actuator_operation(
            context["store"],
            operation["operation_id"],
            "REJECTED",
            allowed_states=("RECEIVED",),
            result=epoch_decision,
        )
        return epoch_decision
    authority = _authority_state(context)
    task.update({
        "distributed_operation_id": operation["operation_id"],
        "distributed_source_node": operation["source_node"],
        "coordinator_node_id": context["options"]["coordinator_node_id"],
        "coordinator_term": operation["fencing_term"],
        "authority_revision": authority["authority_revision"],
        "config_revision": _local_config_revision(context),
        "require_hardware_readback": context["options"]["require_hardware_readback"],
    })
    moved = transition_actuator_operation(
        context["store"], operation["operation_id"], "DISPATCHED",
        allowed_states=("RECEIVED",))
    if not moved:
        return _error("operation_state", "actuator operation is not dispatchable")
    accepted = bool(dispatcher(task))
    if not accepted:
        failure = _error("local_dispatch", "local controller rejected actuator task")
        transition_actuator_operation(
            context["store"], operation["operation_id"], "FAILED",
            allowed_states=("DISPATCHED",), result=failure)
        return failure
    _increment_stat(context, "remote_actuators_dispatched")
    return {
        "ok": True, "accepted": True,
        "operation_id": operation["operation_id"], "state": "DISPATCHED",
    }


def _handle_actuator_execute(context, command):
    role_error = _require_role(context, "worker")
    if role_error is not None:
        return role_error
    authority_error = _validate_coordinator_command(context, command)
    if authority_error is not None:
        return authority_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    revision = _safe_int(payload.get("authority_revision"), 0)
    authority = _authority_state(context)
    if revision != authority["authority_revision"]:
        result = _error(
            "config_not_synchronised",
            "actuator command authority revision differs from worker",
            worker_authority_revision=authority["authority_revision"],
        )
        return _complete_command(context, command, result)
    task = payload.get("task")
    if not _is_dict(task):
        return _complete_command(
            context, command, _error("task", "actuator execute has no task"))
    request_source = str(payload.get("request_source_node") or "")
    task_source = str(task.get("source") or "").lower()
    command_origin = str(task.get("command_origin") or "").lower()
    if (
        request_source == context["options"]["coordinator_node_id"]
        and task_source.startswith(("automation", "rule", "fsm", "cross"))
        and command_origin not in ("manual", "operator", "commissioning")
    ):
        return _complete_command(
            context, command,
            _error(
                "master_execution_forbidden",
                "coordinator may orchestrate config but may not execute automation",
            ),
        )
    task = copy.deepcopy(task)
    task["distributed_request_source_node"] = request_source
    epoch_decision = _accept_cross_hardware_epoch(
        context, task, source_node=request_source)
    if not epoch_decision.get("ok"):
        return _complete_command(context, command, epoch_decision)
    owner = _owner_for_task(context, task)
    if owner != context["options"]["node_id"]:
        return _complete_command(
            context, command, _error("controller_owner", "controller is not local"))
    operation_id = str(payload.get("operation_id") or _operation_id(task))
    operation = register_actuator_operation(
        context["store"], operation_id, task,
        direction="inbound", source_node=command.get("source_node"),
        target_node=context["options"]["node_id"],
        controller_uuid=task.get("controller_uuid"),
        fencing_term=payload.get("coordinator_term"),
    )
    if operation.get("status") == "conflict":
        result = _error("operation_conflict", "actuator operation ID was reused")
        return _complete_command(context, command, result)
    stored = get_actuator_operation(context["store"], operation_id)
    if stored["state"] in TERMINAL_ACTUATOR_STATES:
        result = {
            "ok": stored["state"] == "CONFIRMED",
            "accepted": True, "duplicate": True,
            "operation_id": operation_id, "state": stored["state"],
            "result": stored.get("result"),
        }
        return _complete_command(context, command, result)
    if stored["state"] == "DISPATCHED":
        result = {
            "ok": True, "accepted": True, "duplicate": True,
            "operation_id": operation_id, "state": "DISPATCHED",
        }
        return _complete_command(context, command, result)
    result = _dispatch_inbound_operation(context, stored)
    return _complete_command(context, command, result)


def report_actuator_result(context, result):
    if not _is_dict(result) or not result.get("distributed_operation_id"):
        return False
    try:
        context["result_queue"].put_nowait(copy.deepcopy(result))
        return True
    except Full:
        _increment_stat(context, "actuator_results_rejected")
        return False


def _terminal_state(context, result):
    success = bool(result.get("success"))
    checked = bool(result.get("readback_checked"))
    verified = bool(result.get("readback_verified"))
    if success and checked and verified:
        return "CONFIRMED"
    if success and not context["options"]["require_hardware_readback"]:
        return "WRITE_UNVERIFIED"
    return "FAILED"


def _stage_actuator_result(context, operation, result, **kwargs):
    state = str(kwargs.get("state") or "FAILED")
    authority = _authority_state(context)
    payload = {
        "protocol": CONTEXT["protocol"],
        "operation_id": operation["operation_id"],
        "coordinator_term": authority["coordinator_term"],
        "authority_revision": authority["authority_revision"],
        "terminal_state": state,
        "owner_node": context["options"]["node_id"],
        "result": copy.deepcopy(result),
    }
    message_id = "actuator-result:{}:{}:{}".format(
        authority["coordinator_term"], operation["operation_id"], state.lower())
    staged = enqueue_outbox(
        context["store"], message_id,
        target_node=context["options"]["coordinator_node_id"],
        command=COMMAND_ACTUATOR_RESULT, payload=payload,
    )
    return staged.get("status") != "conflict"


def _process_local_actuator_result(context, result):
    operation_id = str(result.get("distributed_operation_id") or "")
    operation = get_actuator_operation(context["store"], operation_id)
    if not operation or operation.get("direction") != "inbound":
        _increment_stat(context, "actuator_results_rejected")
        return False
    state = _terminal_state(context, result)
    moved = transition_actuator_operation(
        context["store"], operation_id, state,
        allowed_states=("DISPATCHED",), result=result)
    if not moved:
        return operation.get("state") in TERMINAL_ACTUATOR_STATES
    _stage_actuator_result(context, operation, result, state=state)
    _increment_stat(context, "actuator_results_recorded")
    return True


def _result_source_valid(context, command, operation, **kwargs):
    payload = kwargs.get("payload") or {}
    source = str(command.get("source_node") or "")
    if context["options"]["role"] == "coordinator":
        return source == operation.get("target_node") == payload.get("owner_node")
    return source == context["options"]["coordinator_node_id"]


def _forward_result(context, operation, payload):
    source = operation.get("source_node")
    if not source or source == context["options"]["node_id"]:
        return True
    forwarded = copy.deepcopy(payload)
    authority = _authority_state(context)
    forwarded["coordinator_term"] = authority["coordinator_term"]
    message_id = "actuator-result-forward:{}:{}".format(
        authority["coordinator_term"], operation["operation_id"])
    staged = enqueue_outbox(
        context["store"], message_id, target_node=source,
        command=COMMAND_ACTUATOR_RESULT, payload=forwarded)
    return staged.get("status") != "conflict"


def _notify_result_listener(context, operation, payload):
    listener = context.get("result_listener")
    if callable(listener):
        try:
            listener({
                "operation": copy.deepcopy(operation),
                "result": copy.deepcopy(payload),
            })
        except Exception:
            LOGGER.exception("Distributed actuator result listener failed")
    publisher = context["mqtt_api"].get("publish_event")
    if callable(publisher):
        publisher("REMOTE_ACTUATOR_RESULT", copy.deepcopy(payload))


def _handle_actuator_result(context, command):
    if context["options"]["role"] == "worker":
        authority_error = _validate_coordinator_command(context, command)
        if authority_error is not None:
            return authority_error
    record = _inbox_start(context, command)
    duplicate = _inbox_duplicate_result(record)
    if duplicate is not None:
        return duplicate
    payload = command.get("payload") or {}
    operation_id = str(payload.get("operation_id") or "")
    operation = get_actuator_operation(context["store"], operation_id)
    if not operation or operation.get("direction") != "outbound":
        result = _error("operation", "outbound actuator operation is unknown")
        return _complete_command(context, command, result)
    if not _result_source_valid(
            context, command, operation, payload=payload):
        result = _error("result_source", "actuator result source is not the owner")
        return _complete_command(context, command, result)
    state = str(payload.get("terminal_state") or "FAILED")
    if state not in TERMINAL_ACTUATOR_STATES:
        state = "FAILED"
    transition_actuator_operation(
        context["store"], operation_id, state,
        allowed_states=("RECEIVED", "ROUTED", "DISPATCHED"),
        result=payload.get("result"),
    )
    current = get_actuator_operation(context["store"], operation_id)
    if context["options"]["role"] == "coordinator":
        _forward_result(context, current, payload)
    _notify_result_listener(context, current, payload)
    _increment_stat(context, "remote_actuator_results")
    result = {"ok": True, "operation_id": operation_id, "state": current["state"]}
    return _complete_command(context, command, result)


def _config_proposal_completion_state(record, result):
    post_commit = result.get("post_commit")
    if (
        result.get("ok")
        and _is_dict(post_commit)
        and post_commit.get("ok")
        and _safe_int(post_commit.get("authority_revision"), 0) > 0
        and str(post_commit.get("fingerprint") or "")
    ):
        return "MASTER_ACKED"
    if str(result.get("code") or "") in (
        "stale_revision", "stale_authority_revision", "rebase_required",
    ):
        payload = record.get("payload") if _is_dict(record) else {}
        payload = payload if _is_dict(payload) else {}
        if payload.get("operation") == "action_plan":
            return "REJECTED"
        return "REBASE_REQUIRED"
    return "REJECTED"


def _handle_mqtt_ack(context, envelope):
    message_id = str(envelope.get("message_id") or "")
    result = envelope.get("result") if _is_dict(envelope.get("result")) else {}
    command = str(envelope.get("command") or "")
    outbox_record = get_outbox(context["store"], message_id)
    completion_state = None
    if command == COMMAND_CONFIG_PROPOSE:
        completion_state = _config_proposal_completion_state(
            get_outbox(context["store"], message_id), result,
        )
    completed = complete_outbox(
        context["store"], message_id, result, state=completion_state)
    converged = 0
    rebased = 0
    if completed and completion_state == "MASTER_ACKED":
        post_commit = result.get("post_commit") or {}
        expected_revision = _safe_int(
            post_commit.get("authority_revision"), 0)
        authority = _authority_state(context)
        if expected_revision and authority["authority_revision"] >= expected_revision:
            converged = converge_config_proposals(
                context["store"], authority["authority_revision"],
                authority["authority_fingerprint"])
            _increment_stat(context, "config_proposals_converged", converged)
    if completed and completion_state == "REBASE_REQUIRED":
        current_revision = _safe_int(
            result.get(
                "current_revision",
                result.get("current_authority_revision")),
            0,
        )
        authority = _authority_state(context)
        if current_revision and authority["authority_revision"] >= current_revision:
            rebased = rebase_config_proposals(
                context["store"], authority["authority_revision"])
            _increment_stat(context, "config_proposals_rebased", rebased)
    with context["lock"]:
        waiter = context["ack_waiters"].get(message_id)
    if _is_dict(waiter):
        waiter["result"] = copy.deepcopy(result)
        waiter["event"].set()
    if command in (COMMAND_NODE_REGISTER, COMMAND_NODE_HEARTBEAT):
        if result.get("ok"):
            term = _safe_int(result.get("coordinator_term"), 0)
            authority = _authority_state(context)
            if term >= authority["coordinator_term"]:
                with context["lock"]:
                    context["coordinator_term"] = term
                    context["registered"] = True
                set_metadata(context["store"], "worker_coordinator_term", term)
            floor_writer = context["sensor_value_capabilities"].get(
                "advance_sequence_floor")
            if callable(floor_writer):
                floor_writer(result.get("sensor_sequence_floor") or 0)
        return True
    if completed and command == COMMAND_MIRROR_ANNOUNCE:
        payload = outbox_record.get("payload") if _is_dict(outbox_record) else {}
        mirror_id = str((payload or {}).get("mirror_id") or "")
        if result.get("already_current"):
            acknowledge_mirror(context["store"], mirror_id, result)
        elif result.get("request_content"):
            mirror = get_mirror_record(context["store"], mirror_id)
            if mirror is not None:
                enqueue_outbox(
                    context["store"],
                    "mirror-snapshot:" + mirror_id,
                    target_node=context["options"]["coordinator_node_id"],
                    command=COMMAND_MIRROR_SNAPSHOT,
                    payload={
                        "mirror_id": mirror_id,
                        "scope": mirror["scope"],
                        "authority_epoch": mirror["authority_epoch"],
                        "scope_revision": mirror["scope_revision"],
                        "execution_epoch": mirror["execution_epoch"],
                        "fingerprint": mirror["fingerprint"],
                        "snapshot": copy.deepcopy(mirror["snapshot"]),
                    },
                )
                mark_mirror_attempt(
                    context["store"], mirror_id, state="sending")
    if completed and command == COMMAND_MIRROR_SNAPSHOT and result.get("ok"):
        payload = outbox_record.get("payload") if _is_dict(outbox_record) else {}
        mirror_id = str((payload or {}).get("mirror_id") or "")
        acknowledge_mirror(context["store"], mirror_id, result)
        _increment_stat(context, "mirrors_acked")
    if completed:
        if completion_state == "MASTER_ACKED":
            key = "config_proposals_master_acked"
        elif completion_state == "REBASE_REQUIRED":
            key = "config_proposals_conflicted"
        else:
            key = "outbox_confirmed" if result.get("ok") else "outbox_rejected"
        _increment_stat(context, key)
    return bool(completed)


def _publish_outbox_record(context, record):
    publisher = context["mqtt_api"].get("publish_command")
    if not callable(publisher):
        return False
    if (
        record.get("command") == COMMAND_CONFIG_PROPOSE
        and (record.get("payload") or {}).get("authority_scope")
        == "worker_local_runtime"
        and int(record.get("attempts") or 0) == 0
    ):
        payload = copy.deepcopy(record.get("payload") or {})
        payload["authority_revision"] = _authority_state(context)[
            "authority_revision"]
        if update_unattempted_outbox_payload(
                context["store"], record["message_id"], payload):
            record = copy.deepcopy(record)
            record["payload"] = payload
    result = publisher(
        record["target_node"], record["command"],
        payload=record["payload"], message_id=record["message_id"],
        expires_at_s=time.time() + context["options"]["command_ttl_s"],
    )
    accepted = bool(result.get("ok"))
    mark_outbox_attempt(
        context["store"], record["message_id"], accepted=accepted,
        retry_after_s=context["options"]["retry_interval_s"],
        error=result.get("error"),
    )
    _increment_stat(context, "outbox_publish_attempts")
    return accepted


def _flush_due_mirrors(context):
    if (
        context["options"]["config_authority"] != "node_local"
        or not context["options"]["coordinator_node_id"]
    ):
        return
    records = get_due_mirrors(
        context["store"],
        limit=context["options"]["outbox_batch_size"],
    )
    for record in records:
        if record.get("state") not in ("pending", "announced"):
            continue
        mirror_id = str(record.get("mirror_id") or "")
        enqueue_outbox(
            context["store"],
            "mirror-announce:" + mirror_id,
            target_node=context["options"]["coordinator_node_id"],
            command=COMMAND_MIRROR_ANNOUNCE,
            payload={
                "mirror_id": mirror_id,
                "scope": record.get("scope"),
                "authority_epoch": record.get("authority_epoch"),
                "scope_revision": record.get("scope_revision"),
                "execution_epoch": record.get("execution_epoch"),
                "fingerprint": record.get("fingerprint"),
            },
        )
        mark_mirror_attempt(
            context["store"],
            mirror_id,
            state="announced",
            retry_after_s=context["options"]["retry_interval_s"],
        )


def _flush_due_outbox(context):
    records = get_due_outbox(
        context["store"], limit=context["options"]["outbox_batch_size"])
    for record in records:
        if context["stop_event"].is_set():
            break
        _publish_outbox_record(context, record)


def _sync_registered_node(context, item):
    if context["options"]["role"] != "coordinator":
        return
    if context["options"]["config_authority"] == "node_local":
        payload = {
            "scope": "node:" + str(item.get("node_id") or ""),
            "authority_epoch": int(item.get("authority_epoch") or 0),
            "scope_revision": int(item.get("node_scope_revision") or 0),
            "fingerprint": str(item.get("node_shard_fingerprint") or ""),
        }
        enqueue_outbox(
            context["store"],
            "mirror-request:" + str(uuid.uuid4()),
            target_node=item.get("node_id"),
            command=COMMAND_MIRROR_REQUEST,
            payload=payload,
        )
        return
    authority = _authority_state(context)
    same_authority = (
        _safe_int(item.get("authority_revision"), 0) == authority["authority_revision"]
        and str(item.get("fingerprint") or "") == authority["authority_fingerprint"]
    )
    config = _config_snapshot(context)
    expected_node = type_library.config_fingerprint(build_local_execution_shard(
        config, item.get("node_id")))
    expected_cross = type_library.config_fingerprint(build_cross_execution_shard(
        config, item.get("node_id")))
    if (
        not same_authority
        or str(item.get("node_shard_fingerprint") or "") != expected_node
    ):
        _stage_config_for_node(
            context, item.get("node_id"),
            runtime_nonce=item.get("runtime_nonce") or uuid.uuid4(),
        )
    cross_enabled = item.get("cross_runtime_enabled")
    if cross_enabled is None:
        cross_enabled = _node_cross_runtime_enabled(
            context, item.get("node_id"))
    if (
        bool(cross_enabled)
        and str(item.get("cross_fingerprint") or "") != expected_cross
    ):
        _stage_cross_config_for_node(
            context, item.get("node_id"),
            runtime_nonce=item.get("runtime_nonce") or uuid.uuid4(),
        )


def _process_control_item(context, item):
    kind = str(item.get("kind") or "")
    if kind == "sync_node":
        _sync_registered_node(context, item)
    if kind == "connected" and context["options"]["role"] == "worker":
        _publish_registration(context, heartbeat=False)


def _drain_result_queue(context):
    while True:
        try:
            result = context["result_queue"].get_nowait()
        except Empty:
            return
        try:
            _process_local_actuator_result(context, result)
        finally:
            context["result_queue"].task_done()


def _registration_due(context):
    if context["options"]["role"] != "worker":
        return False
    with context["lock"]:
        last_registration_s = context["last_registration_s"]
    elapsed = time.monotonic() - last_registration_s
    return elapsed >= context["options"]["registration_interval_s"]


def _recover_inbound_operations(context):
    recovered = recover_actuator_operations(context["store"], direction="inbound")
    for operation in recovered:
        if operation["state"] == "RECEIVED":
            _dispatch_inbound_operation(context, operation)
        elif (
            operation["state"] == "UNCERTAIN"
            and context["options"]["allow_retry_after_uncertain"]
            and (
                operation["task"].get("idempotent") is True
                or operation["task"].get("device_command_id_supported") is True
            )
        ):
            transition_actuator_operation(
                context["store"], operation["operation_id"], "RECEIVED",
                allowed_states=("UNCERTAIN",))
            current = get_actuator_operation(
                context["store"], operation["operation_id"])
            _dispatch_inbound_operation(context, current)
        else:
            result = {
                "success": False,
                "readback_checked": False,
                "readback_verified": False,
                "error": "process restarted after hardware dispatch; effect is uncertain",
                "recovered_state": "UNCERTAIN",
            }
            _stage_actuator_result(
                context, operation, result, state="UNCERTAIN")


def _coordinator_loop(context):
    _set_state(context, "running")
    _recover_inbound_operations(context)
    while not context["stop_event"].is_set():
        try:
            item = context["control_queue"].get(timeout=0.1)
        except Empty:
            item = None
        if item is not None:
            try:
                _process_control_item(context, item)
            finally:
                context["control_queue"].task_done()
        if _registration_due(context):
            authority = _authority_state(context)
            _publish_registration(context, heartbeat=authority["registered"])
        _drain_result_queue(context)
        _flush_due_mirrors(context)
        _flush_due_outbox(context)
    _drain_result_queue(context)
    _flush_due_outbox(context)
    _set_state(context, "stopped")


def _activate_authority_mode(context, target_mode):
    mode = authority_flow_contracts.validate_authority_mode(target_mode)
    scope = "node:" + context["options"]["node_id"]
    head = get_scope_head(context["store"], scope)
    if not _is_dict(head) or head.get("authority_mode") != mode:
        return False
    with context["lock"]:
        context["options"]["config_authority"] = mode
        context["authority_epoch"] = head["authority_epoch"]
        context["node_scope_revision"] = head["scope_revision"]
        context["node_shard_fingerprint"] = str(
            head.get("fingerprint") or "")
        context["authority_fingerprint"] = str(
            head.get("fingerprint") or "")
    set_metadata(
        context["store"], "node_scope_revision", head["scope_revision"])
    set_metadata(
        context["store"], "node_shard_fingerprint", head.get("fingerprint") or "")
    return True

def create_distributed_coordinator(**kwargs):
    options = _resolved_options(kwargs.get("options"))
    mqtt_api = kwargs.get("mqtt_api") or {}
    capabilities = kwargs.get("config_capabilities") or {}
    if options["enabled"] and not callable(mqtt_api.get("publish_command")):
        raise ValueError("enabled distributed runtime requires MQTT")
    store = create_operation_store(options["journal_path"])
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "state": "created",
        "options": options,
        "mqtt_api": mqtt_api,
        "config_capabilities": capabilities,
        "sensor_value_capabilities": kwargs.get("sensor_value_capabilities") or {},
        "runtime_event_sink": kwargs.get("runtime_event_sink"),
        "cross_event_handler": kwargs.get("cross_event_handler"),
        "cross_config_applier": kwargs.get("cross_config_applier"),
        "cross_freezer": kwargs.get("cross_freezer"),
        "cross_status_reader": kwargs.get("cross_status_reader"),
        "runtime_event_handler_registered": False,
        "sensor_writer_tokens": {},
        "store": store,
        "lock": threading.RLock(),
        "stop_event": threading.Event(),
        "connected_event": threading.Event(),
        "control_queue": Queue(maxsize=options["control_queue_maxsize"]),
        "result_queue": Queue(maxsize=options["result_queue_maxsize"]),
        "thread": None,
        "runtime_nonce": str(uuid.uuid4()),
        "registered": options["role"] == "coordinator",
        "last_registration_s": 0.0,
        "coordinator_term": 0,
        "authority_revision": 0,
        "authority_fingerprint": "",
        "authority_epoch": 0,
        "node_shard_fingerprint": str(
            get_metadata(store, "node_shard_fingerprint", "") or ""),
        "node_scope_revision": _safe_int(
            get_metadata(store, "node_scope_revision", 0), 0),
        "local_actuator_dispatcher": None,
        "result_listener": kwargs.get("result_listener"),
        "ack_waiters": {},
        "authority_transitions": None,
        "statistics": {
            "control_rejected": 0,
            "registrations_sent": 0,
            "heartbeats_sent": 0,
            "nodes_registered": 0,
            "config_replications_staged": 0,
            "local_authority_commits": 0,
            "mirrors_staged": 0,
            "mirrors_acked": 0,
            "config_replications_applied": 0,
            "config_patches_applied": 0,
            "cross_replications_staged": 0,
            "cross_replications_applied": 0,
            "local_config_proposals_staged": 0,
            "config_proposals_master_acked": 0,
            "config_proposals_converged": 0,
            "config_proposals_conflicted": 0,
            "config_proposals_rebased": 0,
            "remote_sensor_ingested": 0,
            "remote_sensor_rejected": 0,
            "outbox_publish_attempts": 0,
            "outbox_confirmed": 0,
            "outbox_rejected": 0,
            "remote_actuators_staged": 0,
            "remote_actuators_dispatched": 0,
            "remote_actuator_results": 0,
            "actuator_results_recorded": 0,
            "actuator_results_rejected": 0,
        },
    }
    _initialize_authority(context)
    fingerprint = _ensure_node_scope_fingerprint(context)
    scope = "node:" + options["node_id"]
    scope_head = ensure_scope_head(
        store,
        scope,
        authority_mode=options["config_authority"],
        authority_epoch=0,
        scope_revision=context["node_scope_revision"],
        execution_epoch=0,
        fingerprint=fingerprint or None,
    )
    if scope_head["authority_mode"] != options["config_authority"]:
        raise RuntimeError(
            "authority_transfer_required: configured mode differs from durable scope")
    context["authority_epoch"] = scope_head["authority_epoch"]
    context["node_scope_revision"] = scope_head["scope_revision"]
    context["authority_transitions"] = create_authority_transition_service(
        store=store,
        scope=scope,
        sem_replace_writer=capabilities.get("request_replace"),
        config_snapshot_reader=capabilities.get("get_snapshot"),
        revision_reader=capabilities.get("get_revision"),
        safety_status_reader=capabilities.get("get_safety_snapshot"),
        preflight_checker=(
            kwargs.get("restore_preflight_checker")
            or partial(_restore_deployment_preflight, context)
        ),
        authority_mode_activator=partial(_activate_authority_mode, context),
    )
    if options["enabled"]:
        _register_mqtt_handlers(context)
    return _build_api(context)


def set_local_actuator_dispatcher(api_or_context, dispatcher):
    context = _get_context(api_or_context)
    if dispatcher is not None and not callable(dispatcher):
        raise TypeError("local actuator dispatcher must be callable")
    context["local_actuator_dispatcher"] = dispatcher
    return True


def set_distributed_result_listener(api_or_context, listener):
    context = _get_context(api_or_context)
    if listener is not None and not callable(listener):
        raise TypeError("distributed result listener must be callable")
    context["result_listener"] = listener
    return True


def set_cross_runtime_callbacks(api_or_context, **kwargs):
    context = _get_context(api_or_context)
    values = {
        "cross_event_handler": kwargs.get("event_handler"),
        "cross_config_applier": kwargs.get("config_applier"),
        "cross_freezer": kwargs.get("freeze_execution"),
        "cross_status_reader": kwargs.get("status_reader"),
    }
    for name, value in values.items():
        if value is not None and not callable(value):
            raise TypeError(name + " must be callable")
    with context["lock"]:
        context.update(values)
    if callable(context.get("cross_event_handler")):
        _register_runtime_event_handler(context)
    return True


def validate_actuator_write(api_or_context, worker_uuid, task, **_kwargs):
    context = _get_context(api_or_context)
    if not _is_dict(task):
        return _error("task", "actuator task must be a dict")
    transitions = context.get("authority_transitions") or {}
    hardware_allowed = transitions.get("hardware_writes_allowed")
    if callable(hardware_allowed) and not hardware_allowed():
        return _error(
            "restore_quarantined",
            "hardware writes remain disabled until restore preflight passes")
    if str(task.get("controller_uuid") or "") != str(worker_uuid or ""):
        return _error("controller_owner", "task controller differs from worker")
    metadata = task.get("meta") if _is_dict(task.get("meta")) else {}
    epoch_decision = _accept_cross_hardware_epoch(
        context,
        task,
        source_node=metadata.get("cross_owner_node_id"),
    )
    if not epoch_decision.get("ok"):
        return epoch_decision
    operation_id = str(task.get("distributed_operation_id") or "")
    if not operation_id:
        return {"ok": True, "scope": "local"}
    term = _safe_int(task.get("coordinator_term"), 0)
    authority = _authority_state(context)
    if term != authority["coordinator_term"]:
        return _error("fencing_term", "actuator task fencing term is not current")
    owner = _owner_for_task(context, task)
    if owner != context["options"]["node_id"]:
        return _error("controller_owner", "worker node does not own controller")
    operation = get_actuator_operation(context["store"], operation_id)
    if not operation or operation.get("state") != "DISPATCHED":
        return _error("operation_state", "actuator operation is not dispatched")
    return {"ok": True, "scope": "distributed", "operation_id": operation_id}


def _get_context(api_or_context):
    if _is_dict(api_or_context) and _is_dict(api_or_context.get("context")):
        return api_or_context["context"]
    return api_or_context


def start_distributed_coordinator(api_or_context):
    context = _get_context(api_or_context)
    if not context["options"]["enabled"]:
        _set_state(context, "disabled")
        return None
    if _thread_alive(context.get("thread")):
        return context["thread"]
    context["stop_event"].clear()
    thread = threading.Thread(
        target=partial(_coordinator_loop, context),
        name="DistributedCoordinator-" + context["options"]["node_id"],
        daemon=False,
    )
    context["thread"] = thread
    _set_state(context, "starting")
    thread.start()
    if context["options"]["role"] == "worker":
        _queue_control(context, {"kind": "connected"})
    return thread


def quiesce_distributed_coordinator(api_or_context, timeout_s=5.0):
    context = _get_context(api_or_context)
    if context["store"].get("closed"):
        return True
    _set_state(context, "quiescing")
    deadline = time.monotonic() + _safe_float(timeout_s, 5.0, 0.1)
    while time.monotonic() < deadline:
        inbound = list_actuator_operations(
            context["store"], direction="inbound", states=("DISPATCHED",))
        if not inbound and context["result_queue"].unfinished_tasks == 0:
            return True
        time.sleep(0.02)
    return False


def _release_remote_sensor_writers(context):
    release = context["sensor_value_capabilities"].get("release_writer")
    if not callable(release):
        return
    with context["lock"]:
        tokens = dict(context["sensor_writer_tokens"])
        context["sensor_writer_tokens"].clear()
    for controller_uuid, token in tokens.items():
        release(controller_uuid, token)


def stop_distributed_coordinator(api_or_context, **kwargs):
    context = _get_context(api_or_context)
    with context["lock"]:
        disabled = context["state"] == "disabled"
    if disabled:
        close_operation_store(context["store"])
        return get_distributed_snapshot(context)
    context["stop_event"].set()
    thread = context.get("thread")
    timeout = _safe_float(
        kwargs.get("join_timeout_s"), context["options"]["join_timeout_s"], 0.1)
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=timeout)
    live = _thread_alive(thread)
    _set_state(context, "stuck" if live else "stopped")
    snapshot = get_distributed_snapshot(context)
    if not live:
        _release_remote_sensor_writers(context)
        close_operation_store(context["store"])
    return snapshot


def get_distributed_snapshot(api_or_context):
    context = _get_context(api_or_context)
    nodes = list_nodes(context["store"]) if not context["store"]["closed"] else []
    now = time.time()
    timeout = context["options"]["node_timeout_s"]
    node_states = []
    for node in nodes:
        item = copy.deepcopy(node)
        item["online"] = now - item["last_seen_at"] <= timeout
        node_states.append(item)
    authority = _authority_state(context)
    with context["lock"]:
        state = context["state"]
        statistics = dict(context["statistics"])
    return {
        "module": context["module"],
        "version": context["version"],
        "state": state,
        "enabled": context["options"]["enabled"],
        "role": context["options"]["role"],
        "node_id": context["options"]["node_id"],
        "coordinator_node_id": context["options"]["coordinator_node_id"],
        "coordinator_term": authority["coordinator_term"],
        "authority_revision": authority["authority_revision"],
        "authority_fingerprint": authority["authority_fingerprint"],
        "node_shard_fingerprint": context.get("node_shard_fingerprint", ""),
        "node_scope_revision": int(context.get("node_scope_revision") or 0),
        "live_config_fingerprint": _config_fingerprint(context),
        "pending_proposal_count": _pending_proposal_count(context),
        "registered": authority["registered"],
        "local_automation_enabled": context["options"][
            "local_automation_enabled"],
        "config_sync_mode": context["options"]["config_sync_mode"],
        "conflict_policy": context["options"]["conflict_policy"],
        "config_authority": context["options"]["config_authority"],
        "authority_epoch": int(context.get("authority_epoch") or 0),
        "authority_transitions": (
            context["authority_transitions"]["snapshot"]()
            if _is_dict(context.get("authority_transitions"))
            else {}
        ),
        "thread_alive": _thread_alive(context.get("thread")),
        "nodes": node_states,
        "statistics": statistics,
        "journal": (
            get_operation_store_snapshot(context["store"])
            if not context["store"]["closed"] else {"closed": True}
        ),
    }


def get_actuator_operation_snapshot(api_or_context, operation_id):
    context = _get_context(api_or_context)
    return get_actuator_operation(context["store"], operation_id)


def _build_api(context):
    return {
        "context": context,
        "start": partial(start_distributed_coordinator, context),
        "stop": partial(stop_distributed_coordinator, context),
        "quiesce": partial(quiesce_distributed_coordinator, context),
        "snapshot": partial(get_distributed_snapshot, context),
        "handle_config_commit": partial(handle_distributed_config_commit, context),
        "authorize_sem_write": partial(authorize_sem_write, context),
        "request_action_plan": partial(
            request_authoritative_action_plan, context),
        "stage_applied_action_plan": partial(stage_applied_action_plan, context),
        "request_scope_operations": partial(request_scope_operations, context),
        "get_proposal": partial(get_config_proposal, context),
        "begin_authority_transfer": context["authority_transitions"][
            "begin_transfer"],
        "ack_authority_transfer": context["authority_transitions"][
            "ack_transfer"],
        "begin_disaster_restore": context["authority_transitions"][
            "begin_restore"],
        "select_disaster_restore": context["authority_transitions"][
            "select_restore"],
        "validate_disaster_restore": context["authority_transitions"][
            "validate_restore"],
        "activate_disaster_restore": context["authority_transitions"][
            "activate_restore"],
        "pass_disaster_restore_preflight": context["authority_transitions"][
            "pass_restore_preflight"],
        "get_scope_head": partial(
            get_scope_head, context["store"]),
        "dispatch_remote_actuator": partial(dispatch_remote_actuator, context),
        "report_actuator_result": partial(report_actuator_result, context),
        "validate_actuator_write": partial(validate_actuator_write, context),
        "set_local_dispatcher": partial(set_local_actuator_dispatcher, context),
        "set_result_listener": partial(set_distributed_result_listener, context),
        "set_cross_callbacks": partial(set_cross_runtime_callbacks, context),
        "get_actuator_operation": partial(
            get_actuator_operation_snapshot, context),
    }











