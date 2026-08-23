# -*- coding: utf-8 -*-

# src/library/_cross_snapshot_store.py


"""
=============================================================================
CROSS SNAPSHOT STORE
=============================================================================

What it is:
    A small atomic JSON journal for one isolated Cross execution domain.

Why it is needed:
    The owner Worker must recover its last accepted Cross shard and mutable
    Cross runtime state after a restart, even while MQTT is unavailable.

How it works:
    One canonical record is written to a same-directory temporary file,
    flushed, fsynced and atomically replaced.  Every load verifies the schema
    marker and authority fingerprint before returning an isolated copy.
=============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import json
import os
import threading
import time

from functools import partial

from src.library import _generic_type_library as type_library


# =============================================================================
# CONTEXT MANAGEMENT
# =============================================================================

CONTEXT = {
    "library_name": "cross_snapshot_store",
    "library_version": "1.0.0",
    "schema": "automation.cross.snapshot.v2",
}


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _fingerprint(config):
    return str(type_library.config_fingerprint(config or {}) or "")


def _error(code, message):
    return {"ok": False, "code": str(code), "error": str(message)}


def _record_fingerprint(record):
    material = copy.deepcopy(record) if _is_dict(record) else {}
    material.pop("record_fingerprint", None)
    return type_library.fingerprint(material)


def _validate_record(record):
    if not _is_dict(record):
        return _error("record", "cross snapshot root must be an object")
    if str(record.get("schema") or "") != CONTEXT["schema"]:
        return _error("schema", "cross snapshot schema differs")
    record_fingerprint = str(record.get("record_fingerprint") or "")
    if not record_fingerprint or _record_fingerprint(record) != record_fingerprint:
        return _error("record_fingerprint", "cross snapshot record differs")
    authority = record.get("authority_config")
    fingerprint = str(record.get("fingerprint") or "")
    if not _is_dict(authority) or not fingerprint:
        return _error("authority", "cross snapshot authority is incomplete")
    if _fingerprint(authority) != fingerprint:
        return _error("fingerprint", "cross snapshot fingerprint differs")
    try:
        revision = int(record.get("revision"))
    except Exception:
        revision = -1
    if revision < 0:
        return _error("revision", "cross snapshot revision is invalid")
    try:
        execution_epoch = int(record.get("execution_epoch"))
    except Exception:
        execution_epoch = -1
    if execution_epoch < 0:
        return _error("execution_epoch", "cross execution epoch is invalid")
    return {"ok": True, "revision": revision, "execution_epoch": execution_epoch, "fingerprint": fingerprint}


def _fsync_directory(path):
    if not hasattr(os, "O_DIRECTORY"):
        return False
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        os.fsync(descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
    return True


# =============================================================================
# CODEBASE FUNCTIONS
# =============================================================================

def create_cross_snapshot_store(path):
    normalized = os.path.abspath(os.path.expanduser(str(path or "")))
    if not normalized:
        raise ValueError("cross snapshot path is required")
    return {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "path": normalized,
        "lock": threading.RLock(),
        "writes": 0,
        "loads": 0,
        "last_error": None,
    }


def load_cross_snapshot(context):
    path = context["path"]
    with context["lock"]:
        if not os.path.exists(path):
            return {"ok": True, "status": "missing", "record": None}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                record = json.load(handle)
            validation = _validate_record(record)
            if not validation.get("ok"):
                context["last_error"] = copy.deepcopy(validation)
                return validation
            context["loads"] += 1
            return {
                "ok": True,
                "status": "loaded",
                "record": copy.deepcopy(record),
            }
        except Exception as exc:
            result = _error("load", "cross snapshot load failed: " + str(exc))
            context["last_error"] = copy.deepcopy(result)
            return result


def persist_cross_snapshot(context, record):
    prepared = copy.deepcopy(record) if _is_dict(record) else {}
    prepared["schema"] = CONTEXT["schema"]
    prepared["saved_at_s"] = time.time()
    prepared["record_fingerprint"] = _record_fingerprint(prepared)
    validation = _validate_record(prepared)
    if not validation.get("ok"):
        return validation
    path = context["path"]
    directory = os.path.dirname(path) or "."
    temporary = path + ".tmp"
    with context["lock"]:
        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)
            encoded = type_library.canonical_dumps(prepared) + "\n"
            with open(temporary, "w", encoding="utf-8") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
            _fsync_directory(directory)
            context["writes"] += 1
            context["last_error"] = None
            return {
                "ok": True,
                "status": "persisted",
                "revision": validation["revision"],
                "fingerprint": validation["fingerprint"],
            }
        except Exception as exc:
            try:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            except Exception:
                pass
            result = _error(
                "persist", "cross snapshot persistence failed: " + str(exc))
            context["last_error"] = copy.deepcopy(result)
            return result


def get_cross_snapshot_store_status(context):
    with context["lock"]:
        return {
            "path": context["path"],
            "exists": os.path.exists(context["path"]),
            "writes": context["writes"],
            "loads": context["loads"],
            "last_error": copy.deepcopy(context["last_error"]),
        }


# =============================================================================
# EXTERNAL INTERFACE
# =============================================================================

def create_cross_snapshot_capabilities(context):
    return {
        "load": partial(load_cross_snapshot, context),
        "persist": partial(persist_cross_snapshot, context),
        "status": partial(get_cross_snapshot_store_status, context),
    }


__all__ = (
    "create_cross_snapshot_capabilities",
    "create_cross_snapshot_store",
    "get_cross_snapshot_store_status",
    "load_cross_snapshot",
    "persist_cross_snapshot",
)
