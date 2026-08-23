# -*- coding: utf-8 -*-

# src/library/_runtime_value_store.py


"""
 =============================================================================
 _RUNTIME_VALUE_STORE
 =============================================================================

 What it is:
     A functional, controller-sharded in-memory store for current sensor values.
     It is independent from the configuration heap and optimized for frequent
     runtime writes plus safe external snapshot reads.

 Why it is needed:
     Configuration changes are infrequent copy-on-write transactions, while
     sensor values can change many times per second. Mixing both workloads would
     copy the complete config heap for every sensor update. This store provides
     short per-controller critical sections and preserves controller isolation.

 How it works:
     1. Each controller UUID owns one shard with its own re-entrant lock.
     2. A controller claims its shard with a unique writer token.
     3. Every write replaces one complete immutable-by-convention entry.
     4. Readers receive deep-copy snapshots and never internal references.
     5. A full snapshot acquires shard locks in sorted UUID order.
     6. Changes are copied into a non-blocking queue for later batch persistence.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import json
import logging
import os
import tempfile
import threading
import time
import uuid

from datetime import datetime, timezone
from functools import partial
from queue import Empty, Full, Queue


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "runtime_value_store",
    "library_version": "1.1.0",
    "description": "Controller-sharded current sensor-value store",
    "options": {
        "change_queue_maxsize": 4096,
        "sequence_reservation_size": 4096,
    },
}


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _safe_nonnegative_int(value, default):
    try:
        result = int(value)
    except Exception:
        return int(default)

    if result < 0:
        return int(default)

    return result


def _safe_positive_int(value, default):
    try:
        result = int(value)
    except Exception:
        return int(default)

    if result < 1:
        return int(default)

    return result


def _safe_positive_float(value, default):
    try:
        result = float(value)
    except Exception:
        return float(default)

    if result <= 0.0:
        return float(default)

    return result


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path, payload):
    """Persist one small allocator record before its sequence range is used."""
    target = os.path.abspath(str(path))
    parent = os.path.dirname(target)
    if parent:
        os.makedirs(parent, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=parent or None,
        prefix=".sensor_sequence_", suffix=".tmp", delete=False,
    )
    temporary_path = handle.name
    try:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary_path, target)
        if parent and hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except Exception:
        handle.close()
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


def _read_reserved_sequence(path):
    target = os.path.abspath(str(path))
    if not os.path.exists(target):
        return 0
    try:
        with open(target, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("sensor sequence allocator state is unreadable") from exc
    if not _is_dict(payload) or int(payload.get("version") or 0) != 1:
        raise RuntimeError("sensor sequence allocator state has an unsupported version")
    reserved_until = _safe_nonnegative_int(payload.get("reserved_until"), -1)
    if reserved_until < 0:
        raise RuntimeError("sensor sequence allocator state is invalid")
    return reserved_until


def _persist_sequence_reservation(path, reserved_until):
    _atomic_write_json(path, {
        "version": 1,
        "reserved_until": int(reserved_until),
        "updated_at": _now_iso(),
    })


def _initialize_sequence_allocator(path, reservation_size):
    if not path:
        return 0, 0
    previous_high_water = _read_reserved_sequence(path)
    next_high_water = previous_high_water + int(reservation_size)
    _persist_sequence_reservation(path, next_high_water)
    return previous_high_water, next_high_water


def _normalize_controller_uuid(value):
    normalized = str(value or "").strip()

    if not normalized:
        raise ValueError("controller_uuid is required")

    return normalized


def _normalize_sensor_id(value):
    if value is None:
        raise ValueError("sensor_id is required")

    try:
        normalized_value = int(value)
        normalized_key = str(normalized_value)
    except Exception:
        normalized_key = str(value).strip()
        normalized_value = normalized_key

    if not normalized_key:
        raise ValueError("sensor_id is required")

    return normalized_key, normalized_value


def _resolve_identity(identity):
    if _is_dict(identity):
        controller_uuid = identity.get("controller_uuid")
        writer_label = identity.get("writer_label")
    else:
        controller_uuid = identity
        writer_label = None

    return {
        "controller_uuid": _normalize_controller_uuid(controller_uuid),
        "writer_label": str(writer_label or "controller_worker"),
    }


def _make_result(ok, message, **kwargs):
    result = {
        "ok": bool(ok),
        "message": str(message),
    }
    result.update(kwargs)
    return result


def _new_controller_shard(controller_uuid):
    return {
        "controller_uuid": controller_uuid,
        "lock": threading.RLock(),
        "writer_token": None,
        "writer_label": None,
        "revision": 0,
        "writes": 0,
        "rejected_writes": 0,
        "last_update_s": None,
        "closed": False,
        "values": {},
    }


def _lookup_shard(store, controller_uuid):
    with store["registry_lock"]:
        return store["controller_shards"].get(controller_uuid)


def _get_or_create_shard(store, controller_uuid):
    with store["registry_lock"]:
        if store["closed"]:
            raise RuntimeError("runtime value store is closed")

        shard = store["controller_shards"].get(controller_uuid)

        if shard is None:
            shard = _new_controller_shard(controller_uuid)
            store["controller_shards"][controller_uuid] = shard

        return shard


def _next_global_sequence(store):
    with store["sequence_lock"]:
        next_sequence = int(store["global_sequence"]) + 1
        if store.get("sequence_state_path") and next_sequence > int(
                store.get("sequence_reserved_until") or 0):
            reserved_until = max(
                int(store.get("sequence_reserved_until") or 0),
                int(store["global_sequence"]),
            ) + int(store["sequence_reservation_size"])
            _persist_sequence_reservation(
                store["sequence_state_path"], reserved_until)
            store["sequence_reserved_until"] = reserved_until
        store["global_sequence"] += 1
        return store["global_sequence"]


def advance_global_sequence_floor(store, floor):
    """Advance to the Master's accepted floor without ever moving backwards."""
    normalized = _safe_nonnegative_int(floor, 0)
    with store["sequence_lock"]:
        current = int(store["global_sequence"])
        if normalized <= current:
            return {
                "ok": True, "changed": False,
                "global_sequence": current,
                "reserved_until": int(store.get("sequence_reserved_until") or 0),
            }
        if store.get("sequence_state_path") and normalized >= int(
                store.get("sequence_reserved_until") or 0):
            reserved_until = normalized + int(store["sequence_reservation_size"])
            _persist_sequence_reservation(
                store["sequence_state_path"], reserved_until)
            store["sequence_reserved_until"] = reserved_until
        store["global_sequence"] = normalized
        return {
            "ok": True, "changed": True,
            "global_sequence": normalized,
            "reserved_until": int(store.get("sequence_reserved_until") or 0),
        }


def _record_dropped_change(store):
    with store["sequence_lock"]:
        store["dropped_changes"] += 1


def _publish_change(store, entry):
    try:
        store["change_queue"].put_nowait(copy.deepcopy(entry))
        return True

    except Full:
        _record_dropped_change(store)
        return False

    except Exception as exc:
        _record_dropped_change(store)
        LOGGER.warning("Runtime value change enqueue failed: %s", exc)
        return False


def _writer_matches(shard, writer_token):
    expected = shard.get("writer_token")
    return expected is not None and str(expected) == str(writer_token)


def _snapshot_entry(entry):
    if not _is_dict(entry):
        return None
    return copy.deepcopy(entry)


def _change_sequence(entry):
    if not _is_dict(entry):
        return 0

    return _safe_nonnegative_int(
        entry.get("sequence"),
        0,
    )


# =============================================================================
# STORE LIFECYCLE
# =============================================================================

def create_runtime_value_store(**options):
    maxsize = _safe_nonnegative_int(
        options.get("change_queue_maxsize"),
        CONTEXT["options"]["change_queue_maxsize"],
    )

    sequence_path = options.get("sequence_state_path")
    sequence_path = os.path.abspath(str(sequence_path)) if sequence_path else None
    reservation_size = _safe_positive_int(
        options.get("sequence_reservation_size"),
        CONTEXT["options"]["sequence_reservation_size"],
    )
    sequence, reserved_until = _initialize_sequence_allocator(
        sequence_path, reservation_size)

    return {
        "name": str(options.get("name") or CONTEXT["library_name"]),
        "version": CONTEXT["library_version"],
        "created_at_s": time.time(),
        "closed": False,
        "registry_lock": threading.RLock(),
        "sequence_lock": threading.Lock(),
        "global_sequence": sequence,
        "sequence_state_path": sequence_path,
        "sequence_reservation_size": reservation_size,
        "sequence_reserved_until": reserved_until,
        "dropped_changes": 0,
        "controller_shards": {},
        "change_queue": Queue(maxsize=maxsize),
        "options": {
            "change_queue_maxsize": maxsize,
            "sequence_state_path": sequence_path,
            "sequence_reservation_size": reservation_size,
        },
    }


def close_runtime_value_store(store):
    if not _is_dict(store):
        return False

    acquired = []

    with store["registry_lock"]:
        shards = [
            store["controller_shards"][key]
            for key in sorted(store["controller_shards"])
        ]

        try:
            for shard in shards:
                shard["lock"].acquire()
                acquired.append(shard["lock"])

            store["closed"] = True

            for shard in shards:
                shard["closed"] = True

        finally:
            for lock in reversed(acquired):
                lock.release()

    return True


def create_writer_token(label=None):
    prefix = str(label or "controller_worker").strip() or "controller_worker"
    return "{}:{}".format(prefix, uuid.uuid4())


# =============================================================================
# WRITER OWNERSHIP
# =============================================================================

def claim_controller_writer(store, identity, writer_token):
    try:
        resolved = _resolve_identity(identity)
        token = str(writer_token or "").strip()

        if not token:
            return _make_result(False, "writer_token is required")

        controller_uuid = resolved["controller_uuid"]
        shard = _get_or_create_shard(store, controller_uuid)

        with shard["lock"]:
            current = shard.get("writer_token")

            if current is not None and str(current) != token:
                return _make_result(
                    False,
                    "controller shard already has a writer",
                    controller_uuid=controller_uuid,
                )

            shard["writer_token"] = token
            shard["writer_label"] = resolved["writer_label"]

        return _make_result(
            True,
            "controller writer claimed",
            controller_uuid=controller_uuid,
        )

    except Exception as exc:
        return _make_result(False, str(exc))


def release_controller_writer(store, identity, writer_token):
    try:
        resolved = _resolve_identity(identity)
        controller_uuid = resolved["controller_uuid"]
        shard = _lookup_shard(store, controller_uuid)

        if shard is None:
            return _make_result(False, "controller shard is unavailable")

        with shard["lock"]:
            if not _writer_matches(shard, writer_token):
                return _make_result(False, "writer token does not own shard")

            shard["writer_token"] = None
            shard["writer_label"] = None

        return _make_result(
            True,
            "controller writer released",
            controller_uuid=controller_uuid,
        )

    except Exception as exc:
        return _make_result(False, str(exc))


# =============================================================================
# SENSOR WRITE PATH
# =============================================================================

def _build_sensor_entry(store, shard, **kwargs):
    sensor_key, sensor_id = _normalize_sensor_id(kwargs.get("sensor_id"))
    sequence = _next_global_sequence(store)
    revision = int(shard["revision"]) + 1
    timestamp = kwargs.get("timestamp") or _now_iso()

    entry = {
        "controller_uuid": shard["controller_uuid"],
        "sensor_id": sensor_id,
        "value": copy.deepcopy(kwargs.get("value")),
        "quality": str(kwargs.get("quality") or "good"),
        "timestamp": str(timestamp),
        "timestamp_s": time.time(),
        "sequence": sequence,
        "controller_revision": revision,
        "source": str(kwargs.get("source") or "controller_worker"),
        "metadata": copy.deepcopy(kwargs.get("metadata") or {}),
    }

    return sensor_key, entry


def write_sensor_value(store, **kwargs):
    try:
        controller_uuid = _normalize_controller_uuid(
            kwargs.get("controller_uuid")
        )
        writer_token = kwargs.get("writer_token")
        shard = _lookup_shard(store, controller_uuid)

        if shard is None:
            return _make_result(False, "controller shard is unavailable")

        with shard["lock"]:
            if shard["closed"]:
                return _make_result(False, "controller shard is closed")

            if not _writer_matches(shard, writer_token):
                shard["rejected_writes"] += 1
                return _make_result(False, "writer token does not own shard")

            sensor_key, entry = _build_sensor_entry(
                store,
                shard,
                **kwargs
            )

            shard["values"][sensor_key] = entry
            shard["revision"] = entry["controller_revision"]
            shard["writes"] += 1
            shard["last_update_s"] = entry["timestamp_s"]

        change_enqueued = _publish_change(store, entry)

        return _make_result(
            True,
            "sensor value replaced",
            entry=_snapshot_entry(entry),
            change_enqueued=change_enqueued,
        )

    except Exception as exc:
        return _make_result(False, str(exc))


# =============================================================================
# SNAPSHOT READ PATH
# =============================================================================

def get_sensor_value_snapshot(store, controller_uuid, sensor_id):
    try:
        normalized_uuid = _normalize_controller_uuid(controller_uuid)
        sensor_key, _normalized_id = _normalize_sensor_id(sensor_id)
        shard = _lookup_shard(store, normalized_uuid)

        if shard is None:
            return None

        with shard["lock"]:
            return _snapshot_entry(shard["values"].get(sensor_key))

    except Exception:
        return None


def get_controller_values_snapshot(store, controller_uuid):
    try:
        normalized_uuid = _normalize_controller_uuid(controller_uuid)
        shard = _lookup_shard(store, normalized_uuid)

        if shard is None:
            return None

        with shard["lock"]:
            return {
                "controller_uuid": normalized_uuid,
                "revision": shard["revision"],
                "last_update_s": shard["last_update_s"],
                "values": copy.deepcopy(shard["values"]),
            }

    except Exception:
        return None


def _acquire_shard_locks(shards):
    acquired = []

    for shard in shards:
        shard["lock"].acquire()
        acquired.append(shard["lock"])

    return acquired


def get_all_sensor_values_snapshot(store):
    acquired = []

    with store["registry_lock"]:
        controller_uuids = sorted(store["controller_shards"])
        shards = [
            store["controller_shards"][controller_uuid]
            for controller_uuid in controller_uuids
        ]

        try:
            acquired = _acquire_shard_locks(shards)

            with store["sequence_lock"]:
                global_sequence = store["global_sequence"]
                reserved_until = store.get("sequence_reserved_until") or 0

            controllers = {}

            for shard in shards:
                controllers[shard["controller_uuid"]] = {
                    "revision": shard["revision"],
                    "last_update_s": shard["last_update_s"],
                    "values": copy.deepcopy(shard["values"]),
                }

            return {
                "store": store["name"],
                "version": store["version"],
                "snapshot_at": _now_iso(),
                "global_sequence": global_sequence,
                "sequence_reserved_until": int(reserved_until),
                "controllers": controllers,
            }

        finally:
            for lock in reversed(acquired):
                lock.release()


# =============================================================================
# CHANGE QUEUE AND DIAGNOSTICS
# =============================================================================

def drain_value_changes(store, limit=256):
    maximum = _safe_positive_int(limit, 256)
    changes = []

    while len(changes) < maximum:
        try:
            entry = store["change_queue"].get_nowait()
        except Empty:
            break

        changes.append(entry)

        try:
            store["change_queue"].task_done()
        except Exception:
            pass

    changes.sort(key=_change_sequence)

    return tuple(changes)


def get_value_store_health_snapshot(store):
    acquired = []
    sensor_count = 0
    controller_details = {}

    with store["registry_lock"]:
        controller_uuids = sorted(
            store["controller_shards"]
        )
        shards = [
            store["controller_shards"][controller_uuid]
            for controller_uuid in controller_uuids
        ]

        try:
            acquired = _acquire_shard_locks(
                shards
            )

            for shard in shards:
                controller_uuid = shard[
                    "controller_uuid"
                ]
                value_count = len(shard["values"])
                sensor_count += value_count
                controller_details[controller_uuid] = {
                    "revision": shard["revision"],
                    "sensor_count": value_count,
                    "writer_claimed": shard["writer_token"] is not None,
                    "writer_label": shard["writer_label"],
                    "writes": shard["writes"],
                    "rejected_writes": shard["rejected_writes"],
                }

            with store["sequence_lock"]:
                global_sequence = store["global_sequence"]
                dropped_changes = store["dropped_changes"]
                reserved_until = store.get("sequence_reserved_until") or 0

            closed = store["closed"]

        finally:
            for lock in reversed(acquired):
                lock.release()

    try:
        queue_depth = store["change_queue"].qsize()
    except Exception:
        queue_depth = None

    return {
        "name": store["name"],
        "version": store["version"],
        "closed": closed,
        "global_sequence": global_sequence,
        "sequence_reserved_until": int(reserved_until),
        "sequence_state_path": store.get("sequence_state_path"),
        "controller_count": len(controller_details),
        "sensor_count": sensor_count,
        "change_queue_depth": queue_depth,
        "dropped_changes": dropped_changes,
        "controllers": controller_details,
    }


def create_sensor_value_capabilities(store):
    return {
        "claim_writer": partial(claim_controller_writer, store),
        "release_writer": partial(release_controller_writer, store),
        "create_writer_token": create_writer_token,
        "write_sensor_value": partial(write_sensor_value, store),
        "get_sensor_snapshot": partial(get_sensor_value_snapshot, store),
        "get_controller_snapshot": partial(
            get_controller_values_snapshot,
            store,
        ),
        "get_all_snapshot": partial(get_all_sensor_values_snapshot, store),
        "drain_changes": partial(drain_value_changes, store),
        "health_snapshot": partial(get_value_store_health_snapshot, store),
        "advance_sequence_floor": partial(advance_global_sequence_floor, store),
    }


# =============================================================================
# ASYNCHRONOUS SNAPSHOT PERSISTENCE
# =============================================================================

def persist_sensor_values_json(store, file_path):
    snapshot = get_all_sensor_values_snapshot(
        store
    )
    target = os.path.abspath(str(file_path))
    parent = os.path.dirname(target)

    if parent:
        os.makedirs(parent, exist_ok=True)

    payload = json.dumps(
        snapshot,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=parent or None,
        prefix=".runtime_values_",
        suffix=".tmp",
        delete=False,
    )
    temporary_path = handle.name

    try:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary_path, target)

    except Exception:
        handle.close()

        try:
            os.unlink(temporary_path)
        except OSError:
            pass

        raise

    return {
        "ok": True,
        "path": target,
        "global_sequence": snapshot["global_sequence"],
        "controller_count": len(snapshot["controllers"]),
    }


def create_sensor_value_persistence(store, file_path, **options):
    return {
        "store": store,
        "file_path": os.path.abspath(str(file_path)),
        "batch_size": _safe_positive_int(options.get("batch_size"), 512),
        "flush_interval_s": _safe_positive_float(
            options.get("flush_interval_s"),
            0.5,
        ),
        "join_timeout_s": _safe_positive_float(
            options.get("join_timeout_s"),
            5.0,
        ),
        "stop_event": threading.Event(),
        "thread": None,
        "lock": threading.RLock(),
        "statistics": {
            "batches": 0,
            "changes": 0,
            "writes": 0,
            "errors": 0,
            "last_error": None,
            "last_sequence": 0,
        },
    }


def _persist_pending_batch(context, change_count):
    if not change_count:
        return False

    try:
        report = persist_sensor_values_json(
            context["store"],
            context["file_path"],
        )

        with context["lock"]:
            context["statistics"]["batches"] += 1
            context["statistics"]["changes"] += int(change_count)
            context["statistics"]["writes"] += 1
            context["statistics"]["last_sequence"] = report[
                "global_sequence"
            ]
            context["statistics"]["last_error"] = None

        return True

    except Exception as exc:
        with context["lock"]:
            context["statistics"]["errors"] += 1
            context["statistics"]["last_error"] = str(exc)

        LOGGER.error("Sensor value persistence failed: %s", exc)
        return False


def _drain_change_count(store, batch_size, **kwargs):
    max_batches = kwargs.get("max_batches")
    batches = 0
    count = 0

    while max_batches is None or batches < int(max_batches):
        changes = drain_value_changes(store, batch_size)

        if not changes:
            break

        count += len(changes)
        batches += 1

    return count


def _sensor_persistence_loop(context):
    stop_event = context["stop_event"]
    interval = max(0.01, context["flush_interval_s"])
    batch_size = context["batch_size"]

    while not stop_event.wait(interval):
        change_count = _drain_change_count(
            context["store"],
            batch_size,
            max_batches=16,
        )
        _persist_pending_batch(
            context,
            change_count,
        )

    final_change_count = _drain_change_count(
        context["store"],
        batch_size,
    )
    _persist_pending_batch(
        context,
        final_change_count,
    )


def start_sensor_value_persistence(context):
    thread = context.get("thread")

    if thread is not None and thread.is_alive():
        return thread

    context["stop_event"].clear()
    thread = threading.Thread(
        target=partial(_sensor_persistence_loop, context),
        name="SensorValuePersistence",
        daemon=False,
    )
    context["thread"] = thread
    thread.start()
    return thread


def stop_sensor_value_persistence(context, **kwargs):
    context["stop_event"].set()
    thread = context.get("thread")
    timeout = float(
        kwargs.get("timeout_s")
        or context["join_timeout_s"]
    )

    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=timeout)

    return get_sensor_value_persistence_snapshot(
        context
    )


def get_sensor_value_persistence_snapshot(context):
    thread = context.get("thread")

    with context["lock"]:
        return {
            "path": context["file_path"],
            "thread_alive": bool(thread and thread.is_alive()),
            "statistics": dict(context["statistics"]),
        }
