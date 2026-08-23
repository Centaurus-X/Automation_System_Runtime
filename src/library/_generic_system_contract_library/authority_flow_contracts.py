# -*- coding: utf-8 -*-

# src/library/_generic_system_contract_library/authority_flow_contracts.py

"""Pure Authority Model V5 proposal, mirror, transfer and restore contracts."""

import copy
import time
import uuid

from src.library import _generic_type_library as type_authority
from src.library._generic_system_contract_library import config_sharding_contracts
from src.library._generic_system_contract_library.errors import make_error, make_success


AUTHORITY_MODES = frozenset(("master_global", "node_local"))
DEFAULT_AUTHORITY_MODE = "node_local"
LEGACY_AUTHORITY_MODE = "master_global"

PROPOSAL_STATES = frozenset((
    "received",
    "accepted",
    "already_committed",
    "rejected",
    "not_authorized",
    "expired",
))

PROPOSAL_TRANSITIONS = {
    "received": frozenset((
        "accepted",
        "already_committed",
        "rejected",
        "not_authorized",
        "expired",
    )),
    "accepted": frozenset(),
    "already_committed": frozenset(),
    "rejected": frozenset(),
    "not_authorized": frozenset(),
    "expired": frozenset(),
}

TRANSFER_STATES = frozenset((
    "requested",
    "locked",
    "compared",
    "validated",
    "intent_persisted",
    "awaiting_target_ack",
    "completed",
    "failed",
))

TRANSFER_TRANSITIONS = {
    "requested": frozenset(("locked", "failed")),
    "locked": frozenset(("compared", "failed")),
    "compared": frozenset(("validated", "failed")),
    "validated": frozenset(("intent_persisted", "failed")),
    "intent_persisted": frozenset(("awaiting_target_ack", "failed")),
    "awaiting_target_ack": frozenset(("completed", "failed")),
    "completed": frozenset(),
    "failed": frozenset(),
}

RESTORE_STATES = frozenset((
    "quarantined",
    "selected",
    "validated",
    "intent_persisted",
    "sem_activated",
    "preflight_passed",
    "hardware_enabled",
    "failed",
))

RESTORE_TRANSITIONS = {
    "quarantined": frozenset(("selected", "failed")),
    "selected": frozenset(("validated", "failed")),
    "validated": frozenset(("intent_persisted", "failed")),
    "intent_persisted": frozenset(("sem_activated", "failed")),
    "sem_activated": frozenset(("preflight_passed", "failed")),
    "preflight_passed": frozenset(("hardware_enabled", "failed")),
    "hardware_enabled": frozenset(),
    "failed": frozenset(),
}

AUTHORITY_SEMANTICS = {
    "modes": tuple(sorted(AUTHORITY_MODES)),
    "default_mode": DEFAULT_AUTHORITY_MODE,
    "operation_format": config_sharding_contracts.OPERATION_FORMAT,
    "proposal_states": tuple(sorted(PROPOSAL_STATES)),
    "conflict_codes": (
        "authority_epoch_stale",
        "stale_revision",
        "execution_epoch_stale",
    ),
    "decision_codes": (
        "accepted",
        "already_committed",
        "authority_epoch_stale",
        "stale_revision",
        "execution_epoch_stale",
        "principal_denied",
        "path_denied",
        "unsupported_operation_format",
        "classification_mismatch",
        "scope_locked",
        "expired",
    ),
}


def _non_negative(value, label):
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


def validate_authority_mode(value):
    """Return one explicit authority mode."""
    mode = str(value or "").strip().lower()
    if mode not in AUTHORITY_MODES:
        raise ValueError("config_authority must be master_global or node_local")
    return mode


def make_scope_head(scope, **kwargs):
    """Build the independent authority/revision/execution head."""
    fingerprint = kwargs.get("fingerprint")
    if fingerprint is not None and not type_authority.is_fingerprint(fingerprint):
        raise ValueError("scope fingerprint must be lowercase raw 64-hex")
    return {
        "scope": _required_text(scope, "scope"),
        "authority_mode": validate_authority_mode(
            kwargs.get("authority_mode", DEFAULT_AUTHORITY_MODE)),
        "authority_epoch": _non_negative(
            kwargs.get("authority_epoch", 0), "authority_epoch"),
        "scope_revision": _non_negative(
            kwargs.get("scope_revision", 0), "scope_revision"),
        "execution_epoch": _non_negative(
            kwargs.get("execution_epoch", 0), "execution_epoch"),
        "fingerprint": fingerprint,
        "locked": bool(kwargs.get("locked", False)),
        "node_authoritative_committed": bool(
            kwargs.get("node_authoritative_committed", False)),
    }


def validate_scope_head(value):
    """Validate and detach one scope head."""
    if not isinstance(value, dict):
        raise ValueError("scope head must be an object")
    expected = make_scope_head(
        value.get("scope"),
        authority_mode=value.get("authority_mode"),
        authority_epoch=value.get("authority_epoch"),
        scope_revision=value.get("scope_revision"),
        execution_epoch=value.get("execution_epoch"),
        fingerprint=value.get("fingerprint"),
        locked=value.get("locked"),
        node_authoritative_committed=value.get("node_authoritative_committed"),
    )
    if set(value) != set(expected):
        raise ValueError("scope head fields differ")
    return expected


def make_proposal(scope, operations, **kwargs):
    """Build one canonical external or relay proposal."""
    normalized = config_sharding_contracts.validate_scope_operations(
        operations,
        operation_format=kwargs.get(
            "operation_format",
            config_sharding_contracts.OPERATION_FORMAT),
        maximum=kwargs.get("maximum", 500),
    )
    return {
        "proposal_id": str(kwargs.get("proposal_id") or uuid.uuid4()),
        "operation_id": str(kwargs.get("operation_id") or uuid.uuid4()),
        "operation_format": config_sharding_contracts.OPERATION_FORMAT,
        "scope": _required_text(scope, "scope"),
        "base_authority_epoch": _non_negative(
            kwargs.get("base_authority_epoch"), "base_authority_epoch"),
        "base_scope_revision": _non_negative(
            kwargs.get("base_scope_revision"), "base_scope_revision"),
        "operations": tuple(normalized),
        "issued_at_s": float(kwargs.get("issued_at_s", time.time())),
        "expires_at_s": float(kwargs.get("expires_at_s", time.time() + 60.0)),
        "relay_identity": copy.deepcopy(kwargs.get("relay_identity")),
        "metadata": copy.deepcopy(kwargs.get("metadata") or {}),
    }


def validate_proposal(value, **kwargs):
    """Validate a canonical proposal without assigning a new revision."""
    if not isinstance(value, dict):
        return make_error("unsupported_operation_format", "proposal must be an object")
    try:
        proposal = make_proposal(
            value.get("scope"),
            value.get("operations"),
            proposal_id=value.get("proposal_id"),
            operation_id=value.get("operation_id"),
            operation_format=value.get("operation_format"),
            base_authority_epoch=value.get("base_authority_epoch"),
            base_scope_revision=value.get("base_scope_revision"),
            issued_at_s=value.get("issued_at_s"),
            expires_at_s=value.get("expires_at_s"),
            relay_identity=value.get("relay_identity"),
            metadata=value.get("metadata"),
            maximum=kwargs.get("maximum", 500),
        )
    except (TypeError, ValueError) as exc:
        code = (
            "unsupported_operation_format"
            if "operation" in str(exc).lower()
            else "invalid_proposal"
        )
        return make_error(code, str(exc))
    if proposal["expires_at_s"] < float(kwargs.get("now_s", time.time())):
        return make_error("expired", "proposal has expired")
    return make_success(proposal=proposal)


def check_proposal_fence(proposal, scope_head):
    """Apply config authority epoch and scope revision concurrency fences."""
    head = validate_scope_head(scope_head)
    if head["locked"]:
        return make_error("scope_locked", "scope is write-locked")
    if int(proposal.get("base_authority_epoch", -1)) != head["authority_epoch"]:
        return make_error(
            "authority_epoch_stale",
            "base authority epoch differs",
            authority_epoch=head["authority_epoch"],
        )
    if int(proposal.get("base_scope_revision", -1)) != head["scope_revision"]:
        return make_error(
            "stale_revision",
            "base scope revision differs",
            scope_revision=head["scope_revision"],
        )
    return make_success(scope_head=head)


def authorize_proposal(proposal, scope_head, policy, principal):
    """Authorize one proposal using authenticated principal and section policy."""
    fence = check_proposal_fence(proposal, scope_head)
    if not fence.get("ok"):
        return fence
    options = policy if isinstance(policy, dict) else {}
    allowed_principals = tuple(str(value) for value in options.get("allowed_principals") or ())
    authenticated = str(principal or "")
    if not authenticated or authenticated not in allowed_principals:
        return make_error("principal_denied", "principal is not allowed")
    allowed_sections = set(str(value) for value in options.get("allowed_sections") or ())
    denied_sections = set(str(value) for value in options.get("denied_sections") or ())
    for operation in proposal.get("operations") or ():
        section = str(operation.get("section") or "")
        if section in denied_sections or (allowed_sections and section not in allowed_sections):
            return make_error("path_denied", "proposal section is not allowed", section=section)
    return make_success(principal=authenticated, scope_head=fence["scope_head"])


def apply_proposal(snapshot, proposal, scope_head, policy, principal):
    """Return the detached accepted candidate and monotonically advanced head."""
    authorization = authorize_proposal(proposal, scope_head, policy, principal)
    if not authorization.get("ok"):
        return authorization
    try:
        candidate = config_sharding_contracts.apply_operations(
            snapshot,
            proposal.get("operations"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        return make_error("unsupported_operation_format", str(exc))
    report = type_authority.verify_runtime_config_data(
        type_authority.normalize_config_for_runtime(candidate, representation="auto"))
    if report.get("status") != "OK":
        return make_error(
            "invalid_candidate",
            "proposal candidate failed Type Authority",
            report=report,
        )
    head = validate_scope_head(scope_head)
    next_head = make_scope_head(
        head["scope"],
        authority_mode=head["authority_mode"],
        authority_epoch=head["authority_epoch"],
        scope_revision=head["scope_revision"] + 1,
        execution_epoch=head["execution_epoch"],
        fingerprint=type_authority.config_fingerprint(candidate),
        locked=False,
        node_authoritative_committed=(
            head["node_authoritative_committed"]
            or head["authority_mode"] == "node_local"
        ),
    )
    return make_success(
        code="accepted",
        candidate=candidate,
        scope_head=next_head,
        principal=str(principal),
    )


def relay_proposal(proposal, relay_identity):
    """Attach relay audit identity without mapping config operations."""
    if not isinstance(proposal, dict):
        raise ValueError("proposal must be an object")
    relayed = copy.deepcopy(proposal)
    relayed["relay_identity"] = copy.deepcopy(relay_identity)
    if relayed.get("operations") != proposal.get("operations"):
        raise ValueError("relay modified canonical operations")
    return relayed


def transition_state(current, target, transitions):
    """Validate one explicit finite-state transition."""
    source = str(current or "")
    destination = str(target or "")
    if destination not in transitions.get(source, frozenset()):
        raise ValueError("invalid state transition: " + source + " -> " + destination)
    return destination


def transition_proposal(current, target):
    """Validate a terminal proposal-state transition."""
    return transition_state(current, target, PROPOSAL_TRANSITIONS)


def begin_authority_transfer(scope_head, target_mode, source_truth, **kwargs):
    """Create a locked transfer intent; a flag alone can never transfer authority."""
    head = validate_scope_head(scope_head)
    mode = validate_authority_mode(target_mode)
    if mode == head["authority_mode"]:
        raise ValueError("authority transfer target already owns the scope")
    if source_truth not in ("master_global", "node_local"):
        raise ValueError("transfer requires explicit source truth")
    locked_head = copy.deepcopy(head)
    locked_head["locked"] = True
    return {
        "transfer_id": str(kwargs.get("transfer_id") or uuid.uuid4()),
        "scope": head["scope"],
        "source_mode": head["authority_mode"],
        "target_mode": mode,
        "source_truth": source_truth,
        "state": "locked",
        "base_authority_epoch": head["authority_epoch"],
        "target_authority_epoch": head["authority_epoch"] + 1,
        "source_fingerprint": kwargs.get("source_fingerprint"),
        "target_fingerprint": kwargs.get("target_fingerprint"),
        "selected_fingerprint": None,
        "target_ack": None,
        "scope_head": locked_head,
    }


def advance_authority_transfer(transfer, target, **kwargs):
    """Advance transfer only through compare, validate, journal, ACK, completion."""
    if not isinstance(transfer, dict):
        raise ValueError("transfer must be an object")
    result = copy.deepcopy(transfer)
    result["state"] = transition_state(
        result.get("state"), target, TRANSFER_TRANSITIONS)
    if target == "compared":
        selected = kwargs.get("selected_fingerprint")
        if not type_authority.is_fingerprint(selected):
            raise ValueError("transfer requires a validated selected fingerprint")
        result["selected_fingerprint"] = selected
    elif target == "validated":
        if kwargs.get("validation_ok") is not True:
            raise ValueError("transfer validation proof is required")
    elif target == "intent_persisted":
        if kwargs.get("journal_record_id") is None:
            raise ValueError("persisted transfer intent proof is required")
        result["journal_record_id"] = str(kwargs["journal_record_id"])
    elif target == "awaiting_target_ack":
        if not result.get("journal_record_id"):
            raise ValueError("transfer intent must be persisted first")
    elif target == "completed":
        ack = kwargs.get("target_ack")
        if not isinstance(ack, dict):
            raise ValueError("target ACK is required")
        if (
            int(ack.get("authority_epoch", -1)) != result["target_authority_epoch"]
            or ack.get("fingerprint") != result.get("selected_fingerprint")
        ):
            raise ValueError("target ACK differs from transfer intent")
        result["target_ack"] = copy.deepcopy(ack)
        head = validate_scope_head(result["scope_head"])
        result["scope_head"] = make_scope_head(
            head["scope"],
            authority_mode=result["target_mode"],
            authority_epoch=result["target_authority_epoch"],
            scope_revision=head["scope_revision"],
            execution_epoch=head["execution_epoch"],
            fingerprint=result["selected_fingerprint"],
            locked=False,
            node_authoritative_committed=(
                head["node_authoritative_committed"]
                or result["target_mode"] == "node_local"
            ),
        )
    return result


def begin_restore(scope, **kwargs):
    """Start a replacement Worker in quarantine with hardware writes disabled."""
    return {
        "restore_id": str(kwargs.get("restore_id") or uuid.uuid4()),
        "scope": _required_text(scope, "scope"),
        "state": "quarantined",
        "hardware_writes_enabled": False,
        "selected_source": None,
        "selected_snapshot": None,
        "target_authority_epoch": _non_negative(
            kwargs.get("prior_authority_epoch", 0), "prior_authority_epoch") + 1,
    }


def advance_restore(restore, target, **kwargs):
    """Advance restore through operator selection, validation, SEM and preflight."""
    if not isinstance(restore, dict):
        raise ValueError("restore must be an object")
    result = copy.deepcopy(restore)
    result["state"] = transition_state(
        result.get("state"), target, RESTORE_TRANSITIONS)
    if target == "selected":
        if kwargs.get("operator_confirmed") is not True:
            raise ValueError("operator confirmation is required")
        source = str(kwargs.get("selected_source") or "")
        if source not in ("local_backup", "master_mirror"):
            raise ValueError("restore source is invalid")
        snapshot = kwargs.get("snapshot")
        if not isinstance(snapshot, dict):
            raise ValueError("restore snapshot is required")
        result["selected_source"] = source
        result["selected_snapshot"] = copy.deepcopy(snapshot)
    elif target == "validated":
        required = ("type", "contract", "ownership", "safety")
        proofs = kwargs.get("proofs") if isinstance(kwargs.get("proofs"), dict) else {}
        if not all(proofs.get(key) is True for key in required):
            raise ValueError("restore validation proofs are incomplete")
        result["proofs"] = copy.deepcopy(proofs)
    elif target == "intent_persisted":
        if kwargs.get("journal_record_id") is None:
            raise ValueError("restore intent journal proof is required")
        result["journal_record_id"] = str(kwargs["journal_record_id"])
    elif target == "sem_activated":
        if kwargs.get("sem_commit_id") is None:
            raise ValueError("atomic SEM activation proof is required")
        result["sem_commit_id"] = str(kwargs["sem_commit_id"])
    elif target == "preflight_passed":
        if kwargs.get("preflight_ok") is not True:
            raise ValueError("hardware preflight proof is required")
    elif target == "hardware_enabled":
        result["hardware_writes_enabled"] = True
    return result


def check_execution_epoch(received, accepted):
    """Fence stale Cross events, intents and hardware writes."""
    if int(received) != int(accepted):
        return make_error(
            "execution_epoch_stale",
            "execution epoch differs",
            accepted_execution_epoch=int(accepted),
        )
    return make_success(execution_epoch=int(accepted))


