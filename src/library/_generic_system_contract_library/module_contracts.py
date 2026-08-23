# -*- coding: utf-8 -*-

# src/library/_generic_system_contract_library/module_contracts.py

"""Pure adapter, event, command and result boundary contracts."""

import copy
import time
import uuid

from src.library._generic_system_contract_library.errors import make_error, make_success


ADAPTER_RESULT_FIELDS = frozenset((
    "ok",
    "code",
    "message",
    "payload",
    "metadata",
))

EVENT_FIELDS = frozenset((
    "event_id",
    "event_type",
    "source",
    "created_at_s",
    "payload",
))


def make_adapter_result(ok, **kwargs):
    """Build the single adapter result shape."""
    return {
        "ok": bool(ok),
        "code": str(kwargs.get("code") or ("ok" if ok else "failed")),
        "message": str(kwargs.get("message") or ""),
        "payload": copy.deepcopy(kwargs.get("payload")),
        "metadata": copy.deepcopy(kwargs.get("metadata") or {}),
    }


def validate_adapter_result(value):
    """Validate a result without invoking application code."""
    if not isinstance(value, dict) or set(value) != ADAPTER_RESULT_FIELDS:
        return make_error("invalid_adapter_result", "adapter result fields differ")
    if not isinstance(value.get("ok"), bool):
        return make_error("invalid_adapter_result", "adapter result ok must be boolean")
    if not str(value.get("code") or ""):
        return make_error("invalid_adapter_result", "adapter result code is required")
    if not isinstance(value.get("metadata"), dict):
        return make_error("invalid_adapter_result", "adapter metadata must be an object")
    return make_success(result=copy.deepcopy(value))


def make_event(event_type, source, payload, **kwargs):
    """Build one detached module event."""
    event = {
        "event_id": str(kwargs.get("event_id") or uuid.uuid4()),
        "event_type": str(event_type or "").strip(),
        "source": str(source or "").strip(),
        "created_at_s": float(kwargs.get("created_at_s", time.time())),
        "payload": copy.deepcopy(payload),
    }
    if not event["event_type"] or not event["source"]:
        raise ValueError("event type and source are required")
    return event


def validate_event(value):
    """Validate one exact module event boundary."""
    if not isinstance(value, dict) or set(value) != EVENT_FIELDS:
        return make_error("invalid_event", "event fields differ")
    try:
        event = make_event(
            value.get("event_type"),
            value.get("source"),
            value.get("payload"),
            event_id=value.get("event_id"),
            created_at_s=value.get("created_at_s"),
        )
    except (TypeError, ValueError) as exc:
        return make_error("invalid_event", str(exc))
    return make_success(event=event)


def require_capabilities(value, names):
    """Validate a functional capability dictionary."""
    if not isinstance(value, dict):
        return make_error("invalid_capabilities", "capabilities must be an object")
    missing = tuple(sorted(str(name) for name in names if not callable(value.get(name))))
    if missing:
        return make_error(
            "invalid_capabilities",
            "required callable capabilities are missing",
            missing=missing,
        )
    return make_success(capabilities=value)
