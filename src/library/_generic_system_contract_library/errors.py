# -*- coding: utf-8 -*-

# src/library/_generic_system_contract_library/errors.py

"""Stable functional result and error contracts."""

import copy


AUTHORITY_CONFLICT_CODES = frozenset((
    "stale_revision",
    "authority_epoch_stale",
    "execution_epoch_stale",
))

AUTHORITY_DECISION_CODES = frozenset((
    "accepted",
    "already_committed",
    "stale_revision",
    "authority_epoch_stale",
    "execution_epoch_stale",
    "principal_denied",
    "path_denied",
    "unsupported_operation_format",
    "classification_mismatch",
    "scope_locked",
    "expired",
    "restore_quarantined",
))


def make_error(code, message, **kwargs):
    """Build a detached stable error result with compatibility projections."""
    normalized_code = str(code or "").strip()
    if not normalized_code:
        raise ValueError("error code must not be empty")
    normalized_message = str(message or normalized_code)
    result = {
        "ok": False,
        "code": normalized_code,
        "message": normalized_message,
        "error": {
            "code": normalized_code,
            "message": normalized_message,
        },
    }
    for key, value in kwargs.items():
        normalized_key = str(key)
        detached = copy.deepcopy(value)
        result["error"][normalized_key] = detached
        if normalized_key not in ("ok", "code", "message", "error"):
            result[normalized_key] = copy.deepcopy(detached)
    return result

def make_success(**kwargs):
    """Build a detached stable success result."""
    result = {"ok": True}
    for key, value in kwargs.items():
        result[str(key)] = copy.deepcopy(value)
    return result


def is_error(value):
    """Return whether a value is a stable error result."""
    return (
        isinstance(value, dict)
        and value.get("ok") is False
        and isinstance(value.get("error"), dict)
        and bool(value["error"].get("code"))
    )


def error_code(value):
    """Return the stable error code or an empty string."""
    if not is_error(value):
        return ""
    return str(value["error"]["code"])


def validate_conflict_code(code):
    """Reject ambiguous or unknown epoch conflict codes."""
    normalized = str(code or "")
    if normalized not in AUTHORITY_CONFLICT_CODES:
        raise ValueError("unknown authority conflict code")
    return normalized
