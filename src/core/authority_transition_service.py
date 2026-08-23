# -*- coding: utf-8 -*-

# src/core/authority_transition_service.py

"""Functional Authority V5 transfer and disaster-restore orchestration."""

import copy
import threading
import uuid

from functools import partial

from src.library import _generic_type_library as type_authority
from src.library import _authority_transition_store as transition_store
from src.library._distributed_operation_store import get_scope_head
from src.library._generic_system_contract_library import authority_flow_contracts
from src.library._generic_system_contract_library import config_sharding_contracts
from src.library._generic_system_contract_library import persistence_contracts
from src.library._generic_system_contract_library.errors import make_error, make_success


CONTEXT = {
    "library_name": "authority_transition_service",
    "library_version": "1.0.0",
}


def _is_dict(value):
    return isinstance(value, dict)


def _call(context, name, *args, **kwargs):
    callback = context.get(name)
    if not callable(callback):
        return None
    return callback(*args, **kwargs)


def _current_head(context):
    head = get_scope_head(context["store"], context["scope"])
    if not _is_dict(head):
        raise RuntimeError("authority scope head is unavailable")
    return authority_flow_contracts.validate_scope_head(head)


def _raw_config(value):
    if not _is_dict(value):
        raise ValueError("authority snapshot is required")
    if value.get("format") == persistence_contracts.SNAPSHOT_FORMAT:
        return persistence_contracts.validate_scope_snapshot(value)["config"]
    return type_authority.deep_copy_config(value)


def _scope_snapshot(context, value, head):
    if _is_dict(value) and value.get("format") == persistence_contracts.SNAPSHOT_FORMAT:
        snapshot = persistence_contracts.validate_scope_snapshot(value)
        if snapshot["scope"] != context["scope"]:
            raise ValueError("authority snapshot scope differs")
        return snapshot
    config = _raw_config(value)
    return persistence_contracts.make_scope_snapshot(
        config,
        scope=context["scope"],
        authority_epoch=head["authority_epoch"],
        scope_revision=head["scope_revision"],
        execution_epoch=head["execution_epoch"],
    )


def _validation_proofs(context, config):
    runtime = type_authority.normalize_config_for_runtime(
        config, representation="auto")
    type_report = type_authority.verify_runtime_config_data(runtime)
    contract_report = type_authority.inspect_config_contract(
        config, representation="auto")
    ownership_report = config_sharding_contracts.validate_scope_partition(runtime)
    safety = _call(context, "safety_status_reader") or {}
    safety_gates = safety.get("gates") if _is_dict(safety.get("gates")) else {}
    proofs = {
        "type": type_report.get("status") == "OK",
        "contract": contract_report.get("status") == "OK",
        "ownership": bool(ownership_report.get("ok")),
        "safety": bool(safety.get("enabled") and safety_gates.get("actuator_tasks")),
    }
    return {
        "ok": all(proofs.values()),
        "proofs": proofs,
        "type_report": type_report,
        "contract_report": contract_report,
        "ownership_report": ownership_report,
        "safety_report": safety,
        "runtime_config": runtime,
    }


def _unlocked_head(head):
    return authority_flow_contracts.make_scope_head(
        head["scope"],
        authority_mode=head["authority_mode"],
        authority_epoch=head["authority_epoch"],
        scope_revision=head["scope_revision"],
        execution_epoch=head["execution_epoch"],
        fingerprint=head.get("fingerprint"),
        locked=False,
        node_authoritative_committed=head["node_authoritative_committed"],
    )


def _fail_transfer(context, transfer, code, message):
    failed = copy.deepcopy(transfer)
    try:
        failed["state"] = authority_flow_contracts.transition_state(
            failed.get("state"), "failed",
            authority_flow_contracts.TRANSFER_TRANSITIONS,
        )
    except ValueError:
        failed["state"] = "failed"
    failed["failure"] = {"code": str(code), "message": str(message)}
    failed["scope_head"] = _unlocked_head(failed["scope_head"])
    transition_store.persist_transfer(context["store"], failed)
    return make_error(code, message, transfer=failed)


def begin_authority_transfer(context, target_mode, **kwargs):
    """Lock, compare, validate and persist a transfer awaiting exact target ACK."""
    with context["lock"]:
        if kwargs.get("operator_confirmed") is not True:
            return make_error(
                "principal_denied", "authority transfer requires operator confirmation")
        active = transition_store.get_active_transfer(
            context["store"], context["scope"])
        if active is not None:
            return make_error("scope_locked", "another authority transfer is active")
        head = _current_head(context)
        if head["locked"]:
            return make_error("scope_locked", "authority scope is already locked")
        source_truth = str(kwargs.get("source_truth") or "")
        try:
            master_snapshot = _scope_snapshot(
                context, kwargs.get("master_snapshot"), head)
            node_snapshot = _scope_snapshot(
                context, kwargs.get("node_snapshot"), head)
            source_snapshot = (
                master_snapshot if head["authority_mode"] == "master_global"
                else node_snapshot
            )
            target_snapshot = (
                node_snapshot if str(target_mode) == "node_local"
                else master_snapshot
            )
            transfer = authority_flow_contracts.begin_authority_transfer(
                head,
                target_mode,
                source_truth,
                transfer_id=kwargs.get("transfer_id"),
                source_fingerprint=source_snapshot["fingerprint"],
                target_fingerprint=target_snapshot["fingerprint"],
            )
            transition_store.persist_transfer(context["store"], transfer)
        except (KeyError, TypeError, ValueError) as exc:
            return make_error("invalid_transfer", str(exc))
        try:
            selected = (
                master_snapshot if source_truth == "master_global" else node_snapshot)
            transfer = authority_flow_contracts.advance_authority_transfer(
                transfer, "compared",
                selected_fingerprint=selected["fingerprint"],
            )
            transfer["comparison"] = {
                "master_fingerprint": master_snapshot["fingerprint"],
                "node_fingerprint": node_snapshot["fingerprint"],
                "equal": master_snapshot["fingerprint"] == node_snapshot["fingerprint"],
            }
            transfer["selected_snapshot"] = copy.deepcopy(selected)
            validation = _validation_proofs(context, selected["config"])
            if not validation["ok"]:
                return _fail_transfer(
                    context, transfer, "invalid_candidate",
                    "selected authority truth failed validation")
            transfer = authority_flow_contracts.advance_authority_transfer(
                transfer, "validated", validation_ok=True)
            transfer["validation_proofs"] = copy.deepcopy(validation["proofs"])
            journal = persistence_contracts.make_journal_record(
                "transfer_intent",
                scope=context["scope"],
                authority_epoch=transfer["target_authority_epoch"],
                scope_revision=head["scope_revision"],
                execution_epoch=head["execution_epoch"],
                fingerprint=selected["fingerprint"],
                payload={
                    "transfer_id": transfer["transfer_id"],
                    "source_mode": transfer["source_mode"],
                    "target_mode": transfer["target_mode"],
                    "source_truth": transfer["source_truth"],
                    "comparison": transfer["comparison"],
                    "validation_proofs": transfer["validation_proofs"],
                },
            )
            transfer = authority_flow_contracts.advance_authority_transfer(
                transfer, "intent_persisted",
                journal_record_id=journal["record_id"],
            )
            transfer = authority_flow_contracts.advance_authority_transfer(
                transfer, "awaiting_target_ack")
            transition_store.persist_transfer(
                context["store"], transfer, journal_record=journal)
            return make_success(
                code="awaiting_target_ack",
                transfer=copy.deepcopy(transfer),
                target_payload={
                    "transfer_id": transfer["transfer_id"],
                    "authority_epoch": transfer["target_authority_epoch"],
                    "fingerprint": transfer["selected_fingerprint"],
                    "snapshot": copy.deepcopy(transfer["selected_snapshot"]),
                },
            )
        except (KeyError, TypeError, ValueError) as exc:
            return _fail_transfer(context, transfer, "invalid_transfer", str(exc))


def authorize_transfer_sem_write(context, payload):
    """Authorize only the exact persisted A5 transfer SEM replacement."""
    transfer_id = str((payload or {}).get("transfer_id") or "")
    operation_id = str((payload or {}).get("operation_id") or "")
    transfer = transition_store.get_transfer(context["store"], transfer_id)
    if not _is_dict(transfer) or transfer.get("state") != "awaiting_target_ack":
        return make_error("principal_denied", "authority transfer is not activatable")
    if not operation_id or operation_id != str(transfer.get("sem_operation_id") or ""):
        return make_error("principal_denied", "authority transfer operation differs")
    if (
        int((payload or {}).get("authority_epoch") or -1)
        != int(transfer["target_authority_epoch"])
    ):
        return make_error("authority_epoch_stale", "authority transfer epoch differs")
    if (payload or {}).get("fingerprint") != transfer.get("selected_fingerprint"):
        return make_error("stale_revision", "authority transfer fingerprint differs")
    return make_success(scope=context["scope"], transfer_id=transfer_id)


def acknowledge_authority_transfer(context, transfer_id, target_ack):
    """SEM-apply selected truth and activate only after an exact durable ACK."""
    with context["lock"]:
        transfer = transition_store.get_transfer(context["store"], transfer_id)
        if transfer is None:
            return make_error("not_found", "authority transfer is unknown")
        if transfer.get("state") == "completed":
            return make_success(code="already_committed", transfer=transfer)
        if transfer.get("state") != "awaiting_target_ack":
            return make_error("invalid_state", "authority transfer is not awaiting ACK")
        if not _is_dict(target_ack) or target_ack.get("accepted") is not True:
            return make_error("principal_denied", "target ACK must explicitly accept")
        try:
            authority_flow_contracts.advance_authority_transfer(
                transfer, "completed", target_ack=target_ack)
        except (KeyError, TypeError, ValueError) as exc:
            return make_error("invalid_ack", str(exc))
        selected = transfer.get("selected_snapshot") or {}
        selected_config = selected.get("config") or {}
        expected_fingerprint = str(transfer.get("selected_fingerprint") or "")
        reader = context.get("config_snapshot_reader")
        if not callable(reader):
            return make_error("capability", "config snapshot reader is unavailable")
        try:
            current_fingerprint = type_authority.config_fingerprint(
                _raw_config(reader()))
        except (TypeError, ValueError) as exc:
            return make_error("invalid_runtime_state", str(exc))
        sem_result = None
        if current_fingerprint != expected_fingerprint:
            writer = context.get("sem_replace_writer")
            if not callable(writer):
                return make_error("capability", "SEM replace writer is unavailable")
            operation_id = str(transfer.get("sem_operation_id") or uuid.uuid4())
            transfer["sem_operation_id"] = operation_id
            transfer["sem_target_fingerprint"] = expected_fingerprint
            transition_store.persist_transfer(context["store"], transfer)
            sem_result = writer(
                copy.deepcopy(selected_config),
                source="authority_transfer",
                external=False,
                transfer_id=transfer["transfer_id"],
                operation_id=operation_id,
                authority_epoch=transfer["target_authority_epoch"],
                fingerprint=expected_fingerprint,
                base_revision=_call(context, "revision_reader"),
            )
            if (
                not _is_dict(sem_result)
                or not sem_result.get("ok")
                or sem_result.get("fingerprint") != expected_fingerprint
            ):
                return make_error(
                    "sem_activation_failed",
                    "authority transfer SEM activation failed",
                    sem_result=copy.deepcopy(sem_result),
                )
            try:
                current_fingerprint = type_authority.config_fingerprint(
                    _raw_config(reader()))
            except (TypeError, ValueError) as exc:
                return make_error("invalid_runtime_state", str(exc))
            if current_fingerprint != expected_fingerprint:
                return make_error(
                    "stale_revision",
                    "authority transfer SEM readback fingerprint differs",
                    fingerprint=current_fingerprint,
                )
            transfer["sem_commit_id"] = str(
                sem_result.get("operation_id") or operation_id)
        try:
            completed = authority_flow_contracts.advance_authority_transfer(
                transfer, "completed", target_ack=target_ack)
            head = completed["scope_head"]
            journal = persistence_contracts.make_journal_record(
                "transfer_completed",
                scope=head["scope"],
                authority_epoch=head["authority_epoch"],
                scope_revision=head["scope_revision"],
                execution_epoch=head["execution_epoch"],
                fingerprint=head.get("fingerprint"),
                payload={
                    "transfer_id": completed["transfer_id"],
                    "source_mode": completed["source_mode"],
                    "target_mode": completed["target_mode"],
                    "target_ack": copy.deepcopy(target_ack),
                    "sem_commit_id": completed.get("sem_commit_id"),
                },
            )
            transition_store.persist_transfer(
                context["store"], completed, journal_record=journal)
            activation = _call(
                context, "authority_mode_activator", completed["target_mode"])
            if activation is False:
                return make_error(
                    "activation_failed",
                    "authority transfer persisted but runtime mode activation failed",
                    transfer=completed,
                )
            return make_success(
                code="accepted", transfer=completed,
                sem_result=copy.deepcopy(sem_result),
            )
        except (KeyError, TypeError, ValueError) as exc:
            return make_error("invalid_ack", str(exc))

def begin_disaster_restore(context, **kwargs):
    """Persist quarantine before any replacement-Worker configuration is selected."""
    with context["lock"]:
        latest = transition_store.get_latest_restore(
            context["store"], context["scope"])
        if _is_dict(latest) and latest.get("state") not in (
            "hardware_enabled", "failed"):
            return make_error("scope_locked", "a restore is already active")
        head = _current_head(context)
        restore = authority_flow_contracts.begin_restore(
            context["scope"],
            restore_id=kwargs.get("restore_id"),
            prior_authority_epoch=head["authority_epoch"],
        )
        restore["identity"] = str(kwargs.get("identity") or "")
        restore["base_scope_revision"] = head["scope_revision"]
        transition_store.persist_restore(context["store"], restore)
        context["hardware_writes_enabled"] = False
        return make_success(code="quarantined", restore=restore)


def select_disaster_restore_snapshot(context, restore_id, source, snapshot, **kwargs):
    """Record the operator-confirmed local-backup or Master-mirror source."""
    with context["lock"]:
        restore = transition_store.get_restore(context["store"], restore_id)
        if restore is None:
            return make_error("not_found", "restore intent is unknown")
        if kwargs.get("identity_confirmed") is not True:
            return make_error("principal_denied", "replacement identity is not confirmed")
        try:
            head = _current_head(context)
            selected = _scope_snapshot(context, snapshot, head)
            if (
                str(source) == "master_mirror"
                and selected["scope_revision"] < head["scope_revision"]
                and selected["fingerprint"] != head.get("fingerprint")
                and kwargs.get("allow_older_mirror") is not True
            ):
                return make_error(
                    "stale_revision",
                    "older Master mirror cannot replace newer local state automatically",
                    scope_revision=head["scope_revision"],
                )
            selected_restore = authority_flow_contracts.advance_restore(
                restore,
                "selected",
                operator_confirmed=kwargs.get("operator_confirmed"),
                selected_source=source,
                snapshot=selected["config"],
            )
            selected_restore["selected_scope_snapshot"] = selected
            transition_store.persist_restore(
                context["store"], selected_restore)
            return make_success(code="selected", restore=selected_restore)
        except (KeyError, TypeError, ValueError) as exc:
            return make_error("invalid_restore", str(exc))


def validate_disaster_restore(context, restore_id):
    """Prove Type, Contract, ownership and Safety before persisting intent."""
    with context["lock"]:
        restore = transition_store.get_restore(context["store"], restore_id)
        if restore is None:
            return make_error("not_found", "restore intent is unknown")
        if restore.get("state") != "selected":
            return make_error("invalid_state", "restore snapshot is not selected")
        snapshot = restore.get("selected_scope_snapshot") or {}
        validation = _validation_proofs(context, snapshot.get("config") or {})
        if not validation["ok"]:
            return make_error(
                "invalid_candidate",
                "restore snapshot failed validation",
                proofs=validation["proofs"],
            )
        try:
            validated = authority_flow_contracts.advance_restore(
                restore, "validated", proofs=validation["proofs"])
            journal = persistence_contracts.make_journal_record(
                "restore_intent",
                scope=context["scope"],
                authority_epoch=validated["target_authority_epoch"],
                scope_revision=validated["base_scope_revision"],
                execution_epoch=_current_head(context)["execution_epoch"],
                fingerprint=snapshot.get("fingerprint"),
                payload={
                    "restore_id": validated["restore_id"],
                    "selected_source": validated["selected_source"],
                    "identity": validated.get("identity"),
                    "proofs": validation["proofs"],
                },
            )
            validated = authority_flow_contracts.advance_restore(
                validated, "intent_persisted",
                journal_record_id=journal["record_id"],
            )
            transition_store.persist_restore(
                context["store"], validated, journal_record=journal)
            return make_success(code="intent_persisted", restore=validated)
        except (KeyError, TypeError, ValueError) as exc:
            return make_error("invalid_restore", str(exc))


def authorize_restore_sem_write(context, payload):
    """Authorize only the exact validated restore intent while quarantined."""
    restore_id = str((payload or {}).get("restore_id") or "")
    operation_id = str((payload or {}).get("operation_id") or "")
    restore = transition_store.get_restore(context["store"], restore_id)
    if not _is_dict(restore) or restore.get("state") != "intent_persisted":
        return make_error("principal_denied", "restore intent is not activatable")
    if operation_id != str(restore.get("sem_operation_id") or operation_id):
        return make_error("principal_denied", "restore operation identity differs")
    return make_success(scope=context["scope"], restore_id=restore_id)


def activate_disaster_restore(context, restore_id, **kwargs):
    """Atomically activate through SEM, then advance the authority epoch."""
    with context["lock"]:
        restore = transition_store.get_restore(context["store"], restore_id)
        if restore is None:
            return make_error("not_found", "restore intent is unknown")
        if restore.get("state") != "intent_persisted":
            return make_error("invalid_state", "restore intent is not activatable")
        writer = context.get("sem_replace_writer")
        if not callable(writer):
            return make_error("capability", "SEM replace writer is unavailable")
        operation_id = str(kwargs.get("operation_id") or uuid.uuid4())
        restore["sem_operation_id"] = operation_id
        transition_store.persist_restore(context["store"], restore)
        selected = restore.get("selected_scope_snapshot") or {}
        result = writer(
            copy.deepcopy(selected.get("config") or {}),
            source="disaster_restore",
            external=False,
            restore_id=restore["restore_id"],
            operation_id=operation_id,
            base_revision=_call(context, "revision_reader"),
        )
        if not _is_dict(result) or not result.get("ok"):
            return make_error(
                "sem_activation_failed",
                "restore SEM activation failed",
                sem_result=copy.deepcopy(result),
            )
        try:
            activated = authority_flow_contracts.advance_restore(
                restore, "sem_activated",
                sem_commit_id=result.get("operation_id") or operation_id,
            )
            current = _current_head(context)
            restored_head = authority_flow_contracts.make_scope_head(
                context["scope"],
                authority_mode="node_local",
                authority_epoch=activated["target_authority_epoch"],
                scope_revision=current["scope_revision"],
                execution_epoch=current["execution_epoch"],
                fingerprint=selected.get("fingerprint"),
                locked=False,
                node_authoritative_committed=True,
            )
            journal = persistence_contracts.make_journal_record(
                "restore_activated",
                scope=context["scope"],
                authority_epoch=restored_head["authority_epoch"],
                scope_revision=restored_head["scope_revision"],
                execution_epoch=restored_head["execution_epoch"],
                fingerprint=restored_head.get("fingerprint"),
                payload={
                    "restore_id": activated["restore_id"],
                    "sem_commit_id": activated["sem_commit_id"],
                    "selected_source": activated["selected_source"],
                },
            )
            transition_store.persist_restore(
                context["store"], activated,
                scope_head=restored_head,
                journal_record=journal,
            )
            return make_success(
                code="sem_activated", restore=activated,
                scope_head=restored_head, sem_result=result)
        except (KeyError, TypeError, ValueError) as exc:
            return make_error("invalid_restore", str(exc))


def pass_disaster_restore_preflight(context, restore_id, **kwargs):
    """Enable hardware only after explicit successful deployment preflight proof."""
    with context["lock"]:
        restore = transition_store.get_restore(context["store"], restore_id)
        if restore is None:
            return make_error("not_found", "restore intent is unknown")
        if restore.get("state") == "hardware_enabled":
            return make_success(code="already_committed", restore=restore)
        if restore.get("state") != "sem_activated":
            return make_error("invalid_state", "restore is not SEM-activated")
        proof = kwargs.get("preflight_ok")
        checker = context.get("preflight_checker")
        if callable(checker):
            report = checker(copy.deepcopy(restore))
            proof = bool(report.get("ok")) if _is_dict(report) else bool(report)
        if proof is not True:
            return make_error("preflight_failed", "restore hardware preflight failed")
        try:
            passed = authority_flow_contracts.advance_restore(
                restore, "preflight_passed", preflight_ok=True)
            enabled = authority_flow_contracts.advance_restore(
                passed, "hardware_enabled")
            transition_store.persist_restore(context["store"], enabled)
            context["hardware_writes_enabled"] = True
            return make_success(code="hardware_enabled", restore=enabled)
        except (KeyError, TypeError, ValueError) as exc:
            return make_error("invalid_restore", str(exc))


def hardware_writes_allowed(context):
    with context["lock"]:
        return bool(context["hardware_writes_enabled"])


def get_transition_snapshot(context):
    with context["lock"]:
        return {
            "scope": context["scope"],
            "scope_head": _current_head(context),
            "active_transfer": transition_store.get_active_transfer(
                context["store"], context["scope"]),
            "latest_restore": transition_store.get_latest_restore(
                context["store"], context["scope"]),
            "hardware_writes_enabled": bool(context["hardware_writes_enabled"]),
        }


def create_authority_transition_service(**kwargs):
    store = kwargs.get("store")
    if not _is_dict(store) or store.get("closed"):
        raise ValueError("an open distributed operation store is required")
    scope = str(kwargs.get("scope") or "").strip()
    if not scope:
        raise ValueError("authority transition scope is required")
    latest = transition_store.get_latest_restore(store, scope)
    hardware_enabled = not _is_dict(latest) or latest.get("state") == "hardware_enabled"
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "store": store,
        "scope": scope,
        "lock": threading.RLock(),
        "sem_replace_writer": kwargs.get("sem_replace_writer"),
        "config_snapshot_reader": kwargs.get("config_snapshot_reader"),
        "revision_reader": kwargs.get("revision_reader"),
        "safety_status_reader": kwargs.get("safety_status_reader"),
        "preflight_checker": kwargs.get("preflight_checker"),
        "authority_mode_activator": kwargs.get("authority_mode_activator"),
        "hardware_writes_enabled": hardware_enabled,
    }
    return {
        "context": context,
        "begin_transfer": partial(begin_authority_transfer, context),
        "ack_transfer": partial(acknowledge_authority_transfer, context),
        "begin_restore": partial(begin_disaster_restore, context),
        "select_restore": partial(select_disaster_restore_snapshot, context),
        "validate_restore": partial(validate_disaster_restore, context),
        "activate_restore": partial(activate_disaster_restore, context),
        "pass_restore_preflight": partial(
            pass_disaster_restore_preflight, context),
        "authorize_transfer_sem_write": partial(
            authorize_transfer_sem_write, context),
        "authorize_restore_sem_write": partial(
            authorize_restore_sem_write, context),
        "hardware_writes_allowed": partial(hardware_writes_allowed, context),
        "snapshot": partial(get_transition_snapshot, context),
    }


__all__ = (
    "acknowledge_authority_transfer",
    "activate_disaster_restore",
    "authorize_restore_sem_write",
    "authorize_transfer_sem_write",
    "begin_authority_transfer",
    "begin_disaster_restore",
    "create_authority_transition_service",
    "get_transition_snapshot",
    "hardware_writes_allowed",
    "pass_disaster_restore_preflight",
    "select_disaster_restore_snapshot",
    "validate_disaster_restore",
)