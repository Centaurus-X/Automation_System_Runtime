# -*- coding: utf-8 -*-

"""
=============================================================================
CROSS I/O ADAPTER
=============================================================================

What it is:
    The fieldbus-free I/O boundary of an isolated Cross Worker.

Why it is needed:
    Cross logic may consume owner-authenticated facts from several controller
    nodes, but it must never open or write a physical bus directly.

How it works:
    Declared facts are validated and stored behind one re-entrant lock.  UUID
    ownership routes action intents either to the local controller dispatcher
    or to the durable distributed actuator path.  The fact cache can be
    exported and restored by the Cross snapshot journal.
=============================================================================
"""

import copy
import threading
import time

from datetime import datetime, timezone
from functools import partial


DEFAULT_MAX_SIGNAL_AGE_S = 15.0
DEFAULT_MAX_FUTURE_SKEW_S = 5.0
DEFAULT_ACCEPTED_QUALITIES = (
    "available",
    "good",
    "ok",
    "valid",
)

CONTEXT = {
    "library_name": "cross_io_adapter",
    "library_version": "1.1.0",
}


def _is_dict(value):
    return isinstance(value, dict)


def _text(value):
    return str(value or "").strip()


def _positive_float(value, default):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if parsed <= 0.0:
        return float(default)
    return parsed


def _non_negative_float(value, default):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if parsed < 0.0:
        return float(default)
    return parsed


def _sensor_id(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = None
    if parsed is not None and str(parsed) == str(value).strip():
        return parsed
    text = _text(value)
    return text or None


def _sequence(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _timestamp_s(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _text(value)
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        pass
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return float(parsed.timestamp())


def _iso_timestamp(value):
    return datetime.fromtimestamp(float(value), timezone.utc).isoformat()


def _normalize_options(options):
    supplied = copy.deepcopy(options) if _is_dict(options) else {}
    node_id = _text(supplied.get("node_id"))
    owner_node_id = _text(supplied.get("owner_node_id")) or node_id
    domain_id = _text(
        supplied.get("cross_domain_id") or supplied.get("domain_id")
    )
    if not domain_id:
        domain_id = "cross:" + (owner_node_id or "unassigned")
    qualities = supplied.get("accepted_qualities")
    if not isinstance(qualities, (list, tuple, set, frozenset)):
        qualities = DEFAULT_ACCEPTED_QUALITIES
    qualities = tuple(sorted({
        _text(item).lower() for item in qualities if _text(item)
    }))
    return {
        "node_id": node_id,
        "owner_node_id": owner_node_id,
        "cross_domain_id": domain_id,
        "max_signal_age_s": _positive_float(
            supplied.get("max_signal_age_s"), DEFAULT_MAX_SIGNAL_AGE_S
        ),
        "max_future_skew_s": _non_negative_float(
            supplied.get("max_future_skew_s"), DEFAULT_MAX_FUTURE_SKEW_S
        ),
        "sequence_reset_after_s": _positive_float(
            supplied.get("sequence_reset_after_s"),
            supplied.get("max_signal_age_s", DEFAULT_MAX_SIGNAL_AGE_S),
        ),
        "require_timestamp": bool(supplied.get("require_timestamp", True)),
        "require_sequence": bool(supplied.get("require_sequence", False)),
        "accepted_qualities": qualities,
    }


def _ownership_row(result, controller_uuid, owner_node_id):
    controller_uuid = _text(controller_uuid)
    owner_node_id = _text(owner_node_id)
    if controller_uuid and owner_node_id:
        result[controller_uuid] = owner_node_id


def _normalize_ownership(ownership):
    result = {}
    source = ownership
    if _is_dict(ownership):
        nested = ownership.get("controller_owners")
        source = nested if _is_dict(nested) else ownership
    if _is_dict(source):
        for controller_uuid, owner in source.items():
            if _is_dict(owner):
                owner = owner.get("owner_node_id") or owner.get("node_id")
            _ownership_row(result, controller_uuid, owner)
    elif isinstance(source, (list, tuple)):
        for row in source:
            if not _is_dict(row):
                continue
            _ownership_row(
                result,
                row.get("controller_uuid"),
                row.get("owner_node_id") or row.get("node_id"),
            )
    return result


def _required_fact_row(result, controller_uuid, sensor_id, metadata=None):
    controller_uuid = _text(controller_uuid)
    sensor_id = _sensor_id(sensor_id)
    if not controller_uuid or sensor_id is None:
        return
    result[(controller_uuid, sensor_id)] = (
        copy.deepcopy(metadata) if _is_dict(metadata) else {}
    )


def _normalize_required_facts(required_facts):
    result = {}
    if isinstance(required_facts, (list, tuple)):
        for row in required_facts:
            if _is_dict(row):
                _required_fact_row(
                    result,
                    row.get("controller_uuid"),
                    row.get("sensor_id"),
                    row,
                )
        return result
    if not _is_dict(required_facts):
        return result
    for key, value in required_facts.items():
        if isinstance(key, tuple) and len(key) == 2:
            _required_fact_row(result, key[0], key[1], value)
            continue
        controller_uuid = _text(key)
        if isinstance(value, (list, tuple, set, frozenset)):
            for sensor_id in value:
                _required_fact_row(result, controller_uuid, sensor_id)
            continue
        if _is_dict(value) and value.get("sensor_id") is not None:
            _required_fact_row(
                result,
                value.get("controller_uuid") or controller_uuid,
                value.get("sensor_id"),
                value,
            )
            continue
        if _is_dict(value):
            for sensor_id, metadata in value.items():
                _required_fact_row(
                    result, controller_uuid, sensor_id, metadata
                )
    return result


def _normalize_local_controllers(local_controller_uuids):
    if isinstance(local_controller_uuids, str):
        local_controller_uuids = (local_controller_uuids,)
    if not isinstance(
        local_controller_uuids, (list, tuple, set, frozenset)
    ):
        return frozenset()
    return frozenset({
        _text(item) for item in local_controller_uuids if _text(item)
    })


def _statistics():
    return {
        "sensor_events_received": 0,
        "facts_accepted": 0,
        "facts_rejected_unknown": 0,
        "facts_rejected_owner": 0,
        "facts_rejected_quality": 0,
        "facts_rejected_timestamp": 0,
        "facts_rejected_sequence": 0,
        "local_actions_dispatched": 0,
        "remote_actions_dispatched": 0,
        "actions_rejected": 0,
        "dispatcher_errors": 0,
    }


def _error(code, message, **details):
    result = {"ok": False, "code": str(code), "message": str(message)}
    result.update(copy.deepcopy(details))
    return result


def _event_payload(event):
    current = copy.deepcopy(event) if _is_dict(event) else {}
    envelope = current
    for _index in range(3):
        if current.get("controller_uuid"):
            return envelope, current
        nested = current.get("payload")
        if not _is_dict(nested):
            break
        current = nested
    return envelope, current


def _event_source_node(envelope, payload, explicit_source, context):
    source = _text(explicit_source)
    if not source and _is_dict(envelope):
        row = envelope
        source = _text(
            row.get("source_node_id")
            or row.get("source_node")
            or row.get("node_id")
        )
        if not source:
            tagged = _text(row.get("source"))
            if tagged.startswith("mqtt_node:"):
                source = tagged.split(":", 1)[1].strip()
    controller_uuid = _text(payload.get("controller_uuid"))
    local_node = context["options"]["node_id"]
    if (
        not source
        and controller_uuid in context["local_controller_uuids"]
        and context["ownership"].get(controller_uuid) == local_node
    ):
        source = local_node
    return source


def _event_sequence(payload):
    value = payload.get("sequence")
    if value is None:
        value = payload.get("sensor_store_sequence")
    if value is None and _is_dict(payload.get("metadata")):
        value = payload["metadata"].get("remote_sequence")
    return _sequence(value)


def _event_is_newer(context, cached, sequence, timestamp_s, now_s):
    if not _is_dict(cached):
        return True
    previous_sequence = cached.get("sequence")
    previous_timestamp = float(cached.get("timestamp_s") or 0.0)
    if sequence is None or previous_sequence is None:
        return timestamp_s > previous_timestamp
    if sequence > previous_sequence:
        return True
    age = max(0.0, now_s - previous_timestamp)
    reset_after = context["options"]["sequence_reset_after_s"]
    return age > reset_after and timestamp_s > previous_timestamp


def ingest_sensor_event(context, event, source_node_id=None, **kwargs):
    """Validate and atomically store one declared cross-shard sensor fact."""
    envelope, payload = _event_payload(event)
    controller_uuid = _text(payload.get("controller_uuid"))
    sensor_id = _sensor_id(payload.get("sensor_id"))
    key = (controller_uuid, sensor_id)
    now_s = kwargs.get("now_s")
    now_s = float(now_s) if isinstance(now_s, (int, float)) else time.time()

    with context["lock"]:
        context["statistics"]["sensor_events_received"] += 1
        if key not in context["required_facts"]:
            context["statistics"]["facts_rejected_unknown"] += 1
            return _error(
                "unknown_fact",
                "sensor fact is not declared by the active cross shard",
                controller_uuid=controller_uuid,
                sensor_id=sensor_id,
            )
        expected_owner = context["ownership"].get(controller_uuid)
        source_node = _event_source_node(
            envelope, payload, source_node_id, context
        )
        if not expected_owner or source_node != expected_owner:
            context["statistics"]["facts_rejected_owner"] += 1
            return _error(
                "owner_mismatch",
                "sensor event source does not own the referenced controller",
                controller_uuid=controller_uuid,
                expected_owner_node_id=expected_owner,
                source_node_id=source_node,
            )
        quality = _text(payload.get("quality")).lower() or "unknown"
        if quality not in context["options"]["accepted_qualities"]:
            context["fact_cache"].pop(key, None)
            context["statistics"]["facts_rejected_quality"] += 1
            return _error(
                "invalid_quality",
                "sensor fact quality is not usable for cross automation",
                controller_uuid=controller_uuid,
                sensor_id=sensor_id,
                quality=quality,
            )
        timestamp_s = _timestamp_s(payload.get("timestamp"))
        if timestamp_s is None and not context["options"]["require_timestamp"]:
            timestamp_s = now_s
        future_limit = now_s + context["options"]["max_future_skew_s"]
        if timestamp_s is None or timestamp_s > future_limit:
            context["fact_cache"].pop(key, None)
            context["statistics"]["facts_rejected_timestamp"] += 1
            return _error(
                "invalid_timestamp",
                "sensor fact timestamp is missing, invalid or too far in the future",
                controller_uuid=controller_uuid,
                sensor_id=sensor_id,
            )
        sequence = _event_sequence(payload)
        if sequence is None and context["options"]["require_sequence"]:
            context["statistics"]["facts_rejected_sequence"] += 1
            return _error(
                "missing_sequence",
                "sensor fact sequence is required",
                controller_uuid=controller_uuid,
                sensor_id=sensor_id,
            )
        cached = context["fact_cache"].get(key)
        if not _event_is_newer(
            context, cached, sequence, timestamp_s, now_s
        ):
            context["statistics"]["facts_rejected_sequence"] += 1
            return _error(
                "stale_sequence",
                "sensor fact is duplicate or older than the cached fact",
                controller_uuid=controller_uuid,
                sensor_id=sensor_id,
                sequence=sequence,
            )
        context["fact_cache"][key] = {
            "controller_uuid": controller_uuid,
            "sensor_id": sensor_id,
            "value": copy.deepcopy(payload.get("value")),
            "timestamp": _iso_timestamp(timestamp_s),
            "timestamp_s": timestamp_s,
            "ts": timestamp_s,
            "quality": quality,
            "sequence": sequence,
            "source_node_id": source_node,
            "received_at_s": now_s,
        }
        context["statistics"]["facts_accepted"] += 1
        return {
            "ok": True,
            "code": "fact_accepted",
            "controller_uuid": controller_uuid,
            "sensor_id": sensor_id,
            "sequence": sequence,
        }


def _missing_fact(controller_uuid, sensor_id, reason, metadata=None):
    return {
        "controller_uuid": controller_uuid,
        "sensor_id": sensor_id,
        "reason": reason,
        "metadata": copy.deepcopy(metadata) if _is_dict(metadata) else {},
    }


def _fact_sort_key(item):
    return item[0], str(item[1])


def get_fact_snapshot(context, now_s=None):
    """Return a rule-engine facts snapshot; stale facts are omitted."""
    now_s = float(now_s) if isinstance(now_s, (int, float)) else time.time()
    sensors = {}
    missing = []
    stale = []
    max_age = context["options"]["max_signal_age_s"]
    with context["lock"]:
        cache = copy.deepcopy(context["fact_cache"])
        required = copy.deepcopy(context["required_facts"])
    for key in sorted(required, key=_fact_sort_key):
        controller_uuid, sensor_id = key
        entry = cache.get(key)
        if not _is_dict(entry):
            missing.append(_missing_fact(
                controller_uuid, sensor_id, "not_received", required[key]
            ))
            continue
        age_s = max(0.0, now_s - float(entry.get("timestamp_s") or 0.0))
        if age_s > max_age:
            item = _missing_fact(
                controller_uuid, sensor_id, "stale", required[key]
            )
            item["age_s"] = age_s
            stale.append(item)
            missing.append(copy.deepcopy(item))
            continue
        fact = copy.deepcopy(entry)
        fact["age_s"] = age_s
        sensors.setdefault(controller_uuid, {})[sensor_id] = fact
    return {
        "sensors": sensors,
        "missing": missing,
        "stale": stale,
        "complete": not missing,
        "now": now_s,
        "max_signal_age_s": max_age,
    }


def _dispatcher_accepted(result):
    if _is_dict(result):
        if "ok" in result:
            return bool(result.get("ok"))
        if "accepted" in result:
            return bool(result.get("accepted"))
    return bool(result)


def _cross_task(context, task, target_owner):
    prepared = copy.deepcopy(task)
    source_meta = prepared.get("meta")
    source_meta = copy.deepcopy(source_meta) if _is_dict(source_meta) else {}
    execution_epoch = int(
        prepared.get("execution_epoch", source_meta.get("execution_epoch", -1)))
    metadata = prepared.get("metadata")
    metadata = copy.deepcopy(metadata) if _is_dict(metadata) else {}
    cross = metadata.get("cross_runtime")
    cross = copy.deepcopy(cross) if _is_dict(cross) else {}
    cross.update({
        "execution_scope": "cross",
        "owner_node_id": context["options"]["owner_node_id"],
        "domain_id": context["options"]["cross_domain_id"],
        "target_owner_node_id": target_owner,
        "execution_epoch": execution_epoch,
    })
    metadata["cross_runtime"] = cross
    prepared["metadata"] = metadata
    prepared["execution_scope"] = "cross"
    prepared["cross_owner_node_id"] = context["options"]["owner_node_id"]
    prepared["cross_domain_id"] = context["options"]["cross_domain_id"]
    prepared["target_owner_node_id"] = target_owner
    prepared["execution_epoch"] = execution_epoch
    return prepared


def route_actuator_task(context, task):
    """Route a task locally or remotely solely by controller UUID ownership."""
    if not _is_dict(task):
        with context["lock"]:
            context["statistics"]["actions_rejected"] += 1
        return _error("invalid_task", "actuator task must be a dict")
    source_meta = task.get("meta") if _is_dict(task.get("meta")) else {}
    execution_epoch = _sequence(
        task.get("execution_epoch", source_meta.get("execution_epoch")))
    if execution_epoch is None:
        with context["lock"]:
            context["statistics"]["actions_rejected"] += 1
        return _error(
            "execution_epoch_stale", "Cross task has no accepted execution epoch")
    controller_uuid = _text(task.get("controller_uuid"))
    with context["lock"]:
        target_owner = context["ownership"].get(controller_uuid)
        local = controller_uuid in context["local_controller_uuids"]
        local_node = context["options"]["node_id"]
        invalid_local = controller_uuid in context["invalid_local_owners"]
        dispatcher = (
            context.get("local_dispatcher")
            if local else context.get("remote_dispatcher")
        )
    if not controller_uuid or not target_owner:
        with context["lock"]:
            context["statistics"]["actions_rejected"] += 1
        return _error(
            "unknown_target",
            "actuator target has no controller ownership declaration",
            controller_uuid=controller_uuid,
        )
    if invalid_local or (local and local_node and target_owner != local_node):
        with context["lock"]:
            context["statistics"]["actions_rejected"] += 1
        return _error(
            "local_owner_mismatch",
            "local controller declaration conflicts with controller ownership",
            controller_uuid=controller_uuid,
            target_owner_node_id=target_owner,
        )
    route = "local" if local else "remote"
    if not callable(dispatcher):
        with context["lock"]:
            context["statistics"]["actions_rejected"] += 1
        return _error(
            "dispatcher_unavailable",
            "{} actuator dispatcher is unavailable".format(route),
            controller_uuid=controller_uuid,
            route=route,
        )
    prepared = _cross_task(context, task, target_owner)
    try:
        result = dispatcher(copy.deepcopy(prepared))
    except Exception as exc:
        with context["lock"]:
            context["statistics"]["actions_rejected"] += 1
            context["statistics"]["dispatcher_errors"] += 1
        return _error(
            "dispatcher_error",
            "{} actuator dispatcher raised {}".format(
                route, type(exc).__name__
            ),
            controller_uuid=controller_uuid,
            route=route,
        )
    accepted = _dispatcher_accepted(result)
    with context["lock"]:
        if accepted:
            key = "{}_actions_dispatched".format(route)
            context["statistics"][key] += 1
        else:
            context["statistics"]["actions_rejected"] += 1
    if not accepted:
        return _error(
            "dispatch_rejected",
            "{} actuator dispatcher rejected the task".format(route),
            controller_uuid=controller_uuid,
            route=route,
            dispatcher_result=copy.deepcopy(result),
        )
    return {
        "ok": True,
        "code": "task_dispatched",
        "route": route,
        "controller_uuid": controller_uuid,
        "target_owner_node_id": target_owner,
        "task": copy.deepcopy(prepared),
        "dispatcher_result": copy.deepcopy(result),
    }


def snapshot(context):
    with context["lock"]:
        facts = copy.deepcopy(context["fact_cache"])
        result = {
            "state": "ready",
            "node_id": context["options"]["node_id"],
            "owner_node_id": context["options"]["owner_node_id"],
            "cross_domain_id": context["options"]["cross_domain_id"],
            "max_signal_age_s": context["options"]["max_signal_age_s"],
            "required_fact_count": len(context["required_facts"]),
            "cached_fact_count": len(facts),
            "known_target_count": len(context["ownership"]),
            "local_controller_uuids": sorted(
                context["local_controller_uuids"]
            ),
            "invalid_local_owners": sorted(
                context["invalid_local_owners"]
            ),
            "statistics": copy.deepcopy(context["statistics"]),
        }
    return result


def export_fact_cache(context):
    """Return an isolated, deterministic copy for durable Cross snapshots."""
    with context["lock"]:
        rows = [
            copy.deepcopy(value)
            for unused_key, value in sorted(
                context["fact_cache"].items(),
                key=_fact_sort_key,
            )
        ]
    return rows


def import_fact_cache(context, rows):
    """Restore only facts declared by the active shard and exact owner map."""
    restored = {}
    rejected = 0
    for row in rows if isinstance(rows, list) else []:
        if not _is_dict(row):
            rejected += 1
            continue
        controller_uuid = _text(row.get("controller_uuid"))
        sensor_id = _sensor_id(row.get("sensor_id"))
        key = (controller_uuid, sensor_id)
        timestamp_s = _timestamp_s(
            row.get("timestamp_s", row.get("timestamp")))
        source_node_id = _text(row.get("source_node_id"))
        if (
            key not in context["required_facts"]
            or context["ownership"].get(controller_uuid) != source_node_id
            or timestamp_s is None
        ):
            rejected += 1
            continue
        copied = copy.deepcopy(row)
        copied["timestamp_s"] = timestamp_s
        copied["ts"] = timestamp_s
        restored[key] = copied
    with context["lock"]:
        context["fact_cache"] = restored
    return {
        "ok": rejected == 0,
        "restored": len(restored),
        "rejected": rejected,
    }


def create_cross_io_adapter(
    options=None,
    ownership=None,
    required_facts=None,
    local_controller_uuids=None,
    local_dispatcher=None,
    remote_dispatcher=None,
):
    """Create an isolated, functional cross-I/O adapter capability dict."""
    normalized_options = _normalize_options(options)
    normalized_ownership = _normalize_ownership(ownership)
    normalized_local = _normalize_local_controllers(local_controller_uuids)
    local_node = normalized_options["node_id"]
    invalid_local = frozenset({
        controller_uuid
        for controller_uuid in normalized_local
        if (
            controller_uuid not in normalized_ownership
            or (
                local_node
                and normalized_ownership.get(controller_uuid) != local_node
            )
        )
    })
    context = {
        "options": normalized_options,
        "ownership": normalized_ownership,
        "required_facts": _normalize_required_facts(required_facts),
        "local_controller_uuids": normalized_local,
        "invalid_local_owners": invalid_local,
        "local_dispatcher": local_dispatcher,
        "remote_dispatcher": remote_dispatcher,
        "fact_cache": {},
        "statistics": _statistics(),
        "lock": threading.RLock(),
    }
    return {
        "context": context,
        "ingest_sensor_event": partial(ingest_sensor_event, context),
        "get_fact_snapshot": partial(get_fact_snapshot, context),
        "route_actuator_task": partial(route_actuator_task, context),
        "export_fact_cache": partial(export_fact_cache, context),
        "import_fact_cache": partial(import_fact_cache, context),
        "snapshot": partial(snapshot, context),
    }


__all__ = (
    "create_cross_io_adapter",
    "export_fact_cache",
    "get_fact_snapshot",
    "import_fact_cache",
    "ingest_sensor_event",
    "route_actuator_task",
    "snapshot",
)
