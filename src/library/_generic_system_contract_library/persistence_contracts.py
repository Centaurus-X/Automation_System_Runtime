# -*- coding: utf-8 -*-

# src/library/_generic_system_contract_library/persistence_contracts.py

"""Pure WAL, journal, receipt, snapshot and outbox record contracts."""

import copy
import time
import uuid

from src.library import _generic_type_library as type_authority


JOURNAL_FORMAT = "authority_journal_v1"
MIRROR_FORMAT = "worker_mirror_v1"
SNAPSHOT_FORMAT = "authority_snapshot_v1"
INBOX_FORMAT = "distributed_inbox_v2"
OUTBOX_FORMAT = "distributed_outbox_v2"

JOURNAL_KINDS = frozenset((
    "scope_commit",
    "proposal_decision",
    "mirror_pending",
    "mirror_acked",
    "transfer_intent",
    "transfer_completed",
    "restore_intent",
    "restore_activated",
    "cross_owner_transition",
))

MIRROR_STATES = frozenset((
    "pending",
    "announced",
    "requested",
    "sending",
    "persisted",
    "acked",
    "expired",
))


def _positive_or_zero(value, label):
    if isinstance(value, bool):
        raise ValueError(str(label) + " must be an integer")
    result = int(value)
    if result < 0:
        raise ValueError(str(label) + " must be non-negative")
    return result


def _required_text(value, label):
    result = str(value or "").strip()
    if not result:
        raise ValueError(str(label) + " must not be empty")
    return result


def make_journal_record(kind, **kwargs):
    """Build one forward-compatible immutable authority journal record."""
    normalized_kind = _required_text(kind, "kind")
    if normalized_kind not in JOURNAL_KINDS:
        raise ValueError("unknown authority journal record kind")
    fingerprint = kwargs.get("fingerprint")
    if fingerprint is not None and not type_authority.is_fingerprint(fingerprint):
        raise ValueError("journal fingerprint must be lowercase raw 64-hex")
    return {
        "format": JOURNAL_FORMAT,
        "record_id": str(kwargs.get("record_id") or uuid.uuid4()),
        "kind": normalized_kind,
        "scope": _required_text(kwargs.get("scope"), "scope"),
        "authority_epoch": _positive_or_zero(
            kwargs.get("authority_epoch", 0), "authority_epoch"),
        "scope_revision": _positive_or_zero(
            kwargs.get("scope_revision", 0), "scope_revision"),
        "execution_epoch": _positive_or_zero(
            kwargs.get("execution_epoch", 0), "execution_epoch"),
        "fingerprint": fingerprint,
        "created_at_s": float(kwargs.get("created_at_s", time.time())),
        "payload": copy.deepcopy(kwargs.get("payload") or {}),
    }


def validate_journal_record(record):
    """Validate a known journal record and reject unknown future formats."""
    if not isinstance(record, dict):
        raise ValueError("journal record must be an object")
    if record.get("format") != JOURNAL_FORMAT:
        raise ValueError("unknown journal format")
    expected = make_journal_record(
        record.get("kind"),
        record_id=record.get("record_id"),
        scope=record.get("scope"),
        authority_epoch=record.get("authority_epoch"),
        scope_revision=record.get("scope_revision"),
        execution_epoch=record.get("execution_epoch"),
        fingerprint=record.get("fingerprint"),
        created_at_s=record.get("created_at_s"),
        payload=record.get("payload"),
    )
    if set(record) != set(expected):
        raise ValueError("journal record fields differ")
    return copy.deepcopy(record)


def make_scope_snapshot(config, **kwargs):
    """Build a detached, self-verifying authority scope snapshot."""
    config_copy = type_authority.canonicalize_config_for_fingerprint(config)
    fingerprint = type_authority.config_fingerprint(config_copy)
    supplied = kwargs.get("fingerprint")
    if supplied is not None and supplied != fingerprint:
        raise ValueError("scope snapshot fingerprint differs")
    return {
        "format": SNAPSHOT_FORMAT,
        "scope": _required_text(kwargs.get("scope"), "scope"),
        "authority_epoch": _positive_or_zero(
            kwargs.get("authority_epoch", 0), "authority_epoch"),
        "scope_revision": _positive_or_zero(
            kwargs.get("scope_revision", 0), "scope_revision"),
        "execution_epoch": _positive_or_zero(
            kwargs.get("execution_epoch", 0), "execution_epoch"),
        "fingerprint": fingerprint,
        "config": config_copy,
    }


def validate_scope_snapshot(snapshot):
    """Verify snapshot format, epochs, config contract and fingerprint."""
    if not isinstance(snapshot, dict) or snapshot.get("format") != SNAPSHOT_FORMAT:
        raise ValueError("unknown authority snapshot format")
    candidate = make_scope_snapshot(
        snapshot.get("config"),
        scope=snapshot.get("scope"),
        authority_epoch=snapshot.get("authority_epoch"),
        scope_revision=snapshot.get("scope_revision"),
        execution_epoch=snapshot.get("execution_epoch"),
        fingerprint=snapshot.get("fingerprint"),
    )
    if set(snapshot) != set(candidate):
        raise ValueError("scope snapshot fields differ")
    return candidate


def make_mirror_record(command, snapshot, **kwargs):
    """Build one restart-safe Worker-to-Master mirror record."""
    validated = validate_scope_snapshot(snapshot)
    state = str(kwargs.get("state") or "pending")
    if state not in MIRROR_STATES:
        raise ValueError("mirror state is invalid")
    return {
        "format": MIRROR_FORMAT,
        "mirror_id": str(kwargs.get("mirror_id") or uuid.uuid4()),
        "command": _required_text(command, "command"),
        "state": state,
        "scope": validated["scope"],
        "authority_epoch": validated["authority_epoch"],
        "scope_revision": validated["scope_revision"],
        "execution_epoch": validated["execution_epoch"],
        "fingerprint": validated["fingerprint"],
        "snapshot": validated,
        "attempts": _positive_or_zero(kwargs.get("attempts", 0), "attempts"),
        "created_at_s": float(kwargs.get("created_at_s", time.time())),
        "updated_at_s": float(kwargs.get("updated_at_s", time.time())),
    }


def make_inbox_receipt(message_id, result, **kwargs):
    """Build the durable idempotent inbox decision receipt."""
    return {
        "format": INBOX_FORMAT,
        "message_id": _required_text(message_id, "message_id"),
        "source": _required_text(kwargs.get("source"), "source"),
        "command": _required_text(kwargs.get("command"), "command"),
        "received_at_s": float(kwargs.get("received_at_s", time.time())),
        "result_json": type_authority.json_ready(copy.deepcopy(result)),
    }


def make_outbox_record(command, payload, **kwargs):
    """Build one bounded persistent outbox item."""
    return {
        "format": OUTBOX_FORMAT,
        "outbox_id": str(kwargs.get("outbox_id") or uuid.uuid4()),
        "target": _required_text(kwargs.get("target"), "target"),
        "command": _required_text(command, "command"),
        "payload": type_authority.json_ready(copy.deepcopy(payload)),
        "state": str(kwargs.get("state") or "pending"),
        "attempts": _positive_or_zero(kwargs.get("attempts", 0), "attempts"),
        "created_at_s": float(kwargs.get("created_at_s", time.time())),
        "expires_at_s": float(kwargs.get("expires_at_s", time.time() + 60.0)),
    }
