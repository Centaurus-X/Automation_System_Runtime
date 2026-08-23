# -*- coding: utf-8 -*-

# src/library/_state_shard_manager.py


"""
 =============================================================================
 _STATE_SHARD_MANAGER
 =============================================================================

 What it is:
     A pure projection and local-snapshot library for the normalized V5
     configuration. The Generic Data Center remains the only master authority;
     this module only derives node, controller and automation views.

 Why it is needed:
     The V5 identity is ``controller_uuid + local I/O id`` while legacy
     controller and automation functions consume flat lists. Deterministic
     projections bridge that boundary without duplicating the central config
     heap or sharing mutable dictionaries between threads.

 How it works:
     1. Node routes resolve the controller UUIDs owned by one worker node.
     2. Controller packages flatten only that UUID's sensor/actuator groups.
     3. Automation projections assign deterministic surrogate IDs so local IDs
        from different controllers never collide inside legacy calculations.
     4. Local shard cells atomically swap monotonic deep-copy snapshots.
     5. Compatibility helpers retain the old read API without becoming a
        second runtime config authority.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import logging
import threading

from functools import partial

from src.library import _generic_type_library as type_library


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "state_shard_manager",
    "library_version": "2.0.0",
    "description": "Deterministic V5 config projections and local cells",
}

CENTRAL_CONTEXT = {
    "lock": threading.RLock(),
    "full_config": None,
    "worker_shards": {},
    "fingerprint": None,
    "revision": 0,
}


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _copy(value):
    return copy.deepcopy(value)


def _source_config(source):
    if not _is_dict(source):
        return {}
    candidate = source.get("sections")
    if not _is_dict(candidate):
        candidate = source
    return type_library.normalize_config_for_runtime(
        candidate, representation="auto")


def _text(value):
    return str(value or "").strip()


def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _identity_key(controller_uuid, local_id):
    return "{}:{}".format(
        _text(controller_uuid),
        _text(local_id),
    )


def _route_sort_key(route):
    local_id = _safe_int(route.get("local_controller_id"), 2147483647)
    return (
        local_id,
        _text(route.get("controller_uuid")),
    )


def _group_sort_key(group):
    return (
        _text(group.get("controller_uuid")),
        _text(group.get("controller_key")),
    )


def _io_sort_key(row, id_field):
    return (
        _safe_int(row.get(id_field), 2147483647),
        _text(row.get(id_field)),
    )


def _controller_index(config):
    index = {}

    for row in config.get("controllers") or []:
        if _is_dict(row) and row.get("controller_uuid"):
            index[_text(row["controller_uuid"])] = row

    return index


def _controller_record(config, controller_uuid):
    return _controller_index(config).get(
        _text(controller_uuid)
    )


def _package_metadata(source, controller_uuid):
    if not _is_dict(source):
        return {}

    return {
        "controller_uuid": _text(controller_uuid),
        "revision": _safe_int(source.get("revision"), 0),
        "fingerprint": _text(source.get("fingerprint")),
        "snapshot_at": source.get("snapshot_at"),
        "shard_key": _text(source.get("shard_key") or controller_uuid),
    }


# =============================================================================
# NODE AND CONTROLLER PROJECTIONS
# =============================================================================

def get_node_controller_uuids(source, node_id):
    config = _source_config(source)
    normalized_node = _text(node_id)
    routes = []

    for route in config.get("site_controller_map") or []:
        if not _is_dict(route):
            continue
        if (
            normalized_node
            and _text(route.get("node_id")) != normalized_node
        ):
            continue
        if route.get("enabled") is False:
            continue
        if _text(route.get("routing_role") or "owner") != "owner":
            continue
        if route.get("controller_uuid"):
            routes.append(route)

    routes.sort(key=_route_sort_key)
    result = []

    for route in routes:
        controller_uuid = _text(route.get("controller_uuid"))
        if controller_uuid and controller_uuid not in result:
            result.append(controller_uuid)

    if result:
        return tuple(result)

    if not normalized_node:
        return tuple(sorted(_controller_index(config).keys()))

    return _get_local_node_uuids(config, normalized_node)


def get_controller_owner_node(source, controller_uuid):
    config = _source_config(source)
    identity = _text(controller_uuid)
    matches = []
    for route in config.get("site_controller_map") or []:
        if not _is_dict(route) or route.get("enabled") is False:
            continue
        if _text(route.get("controller_uuid")) != identity:
            continue
        if _text(route.get("routing_role") or "owner") != "owner":
            continue
        node_id = _text(route.get("node_id"))
        if node_id and node_id not in matches:
            matches.append(node_id)
    if len(matches) == 1:
        return matches[0]
    return None


def get_enabled_node_ids(source):
    config = _source_config(source)
    result = []
    for row in config.get("nodes") or []:
        if not _is_dict(row) or row.get("enabled") is False:
            continue
        node_id = _text(row.get("node_id"))
        if node_id and node_id not in result:
            result.append(node_id)
    return tuple(sorted(result))


def _get_local_node_uuids(config, node_id):
    for row in config.get("local_node") or []:
        if not _is_dict(row):
            continue
        if _text(row.get("node_id")) != node_id:
            continue
        values = row.get("active_controller_uuids") or []
        return tuple(_text(value) for value in values if _text(value))

    return tuple()


def _filter_rows(rows, key, wanted):
    return [
        _copy(row)
        for row in rows or []
        if _is_dict(row) and _text(row.get(key)) == _text(wanted)
    ]


def build_worker_shard(source, node_id):
    config = _source_config(source)
    controller_uuids = set(
        get_node_controller_uuids(config, node_id)
    )
    filtered_sections = {
        "controllers",
        "virtual_controllers",
        "site_controller_map",
        "sensors",
        "actuators",
        "local_node",
    }
    shard = {}

    for key, value in config.items():
        if key not in filtered_sections:
            shard[key] = _copy(value)

    shard["controllers"] = [
        _copy(row)
        for row in config.get("controllers") or []
        if _text(row.get("controller_uuid")) in controller_uuids
    ]
    shard["virtual_controllers"] = [
        _copy(row)
        for row in config.get("virtual_controllers") or []
        if _text(row.get("controller_uuid")) in controller_uuids
    ]
    shard["site_controller_map"] = _filter_rows(
        config.get("site_controller_map"),
        "node_id",
        node_id,
    )
    shard["sensors"] = _filter_rows(
        config.get("sensors"),
        "node_id",
        node_id,
    )
    shard["actuators"] = _filter_rows(
        config.get("actuators"),
        "node_id",
        node_id,
    )
    shard["local_node"] = _filter_rows(
        config.get("local_node"),
        "node_id",
        node_id,
    )
    return shard


def _flatten_controller_io(config, section_name, id_field, controller_uuid):
    controller = _controller_record(config, controller_uuid) or {}
    rows = []

    for group in config.get(section_name) or []:
        if not _is_dict(group):
            continue
        if _text(group.get("controller_uuid")) != _text(controller_uuid):
            continue

        group_meta = {
            key: _copy(value)
            for key, value in group.items()
            if key != "rows"
        }

        for source_row in group.get("rows") or []:
            if not _is_dict(source_row):
                continue
            row = group_meta.copy()
            row.update(_copy(source_row))
            row["controller_uuid"] = _text(controller_uuid)
            row["controller_id"] = controller.get("controller_id")
            row["local_controller_id"] = group.get("local_controller_id")
            rows.append(row)

    rows.sort(key=partial(_io_sort_key, id_field=id_field))
    return rows


def _normalize_controller_states(config, controller_uuid):
    states = []

    for source_state in config.get("process_states") or []:
        if not _is_dict(source_state):
            continue
        state = _copy(source_state)
        io_refs = state.get("io_refs") or {}
        sensor_refs = io_refs.get("sensors") or []
        actuator_refs = io_refs.get("actuators") or []
        state["sensor_ids"] = [
            ref.get("sensor_id")
            for ref in sensor_refs
            if _text(ref.get("controller_uuid")) == _text(controller_uuid)
        ]
        state["actuator_ids"] = [
            ref.get("actuator_id")
            for ref in actuator_refs
            if _text(ref.get("controller_uuid")) == _text(controller_uuid)
        ]
        states.append(state)

    return states


def build_controller_projection(source, controller_uuid):
    config = _source_config(source)
    normalized_uuid = _text(controller_uuid)
    controller = _controller_record(config, normalized_uuid)

    if controller is None:
        return {}

    projected = _copy(config)
    projected["controllers"] = [_copy(controller)]
    projected["virtual_controllers"] = _filter_rows(
        config.get("virtual_controllers"),
        "controller_uuid",
        normalized_uuid,
    )
    projected["site_controller_map"] = _filter_rows(
        config.get("site_controller_map"),
        "controller_uuid",
        normalized_uuid,
    )
    projected["sensors"] = _flatten_controller_io(
        config,
        "sensors",
        "sensor_id",
        normalized_uuid,
    )
    projected["actuators"] = _flatten_controller_io(
        config,
        "actuators",
        "actuator_id",
        normalized_uuid,
    )
    projected["process_states"] = _normalize_controller_states(
        config,
        normalized_uuid,
    )
    projected["_runtime_meta"] = _package_metadata(
        source,
        normalized_uuid,
    )
    return projected


# =============================================================================
# AUTOMATION IDENTITY PROJECTION
# =============================================================================

def _flatten_all_io(config, section_name, id_field):
    controllers = _controller_index(config)
    rows = []
    identity_to_surrogate = {}
    surrogate_to_identity = {}
    groups = [group for group in config.get(section_name) or [] if _is_dict(group)]
    groups.sort(key=_group_sort_key)
    surrogate = 0

    for group in groups:
        controller_uuid = _text(group.get("controller_uuid"))
        controller = controllers.get(controller_uuid) or {}
        source_rows = [row for row in group.get("rows") or [] if _is_dict(row)]
        source_rows.sort(key=partial(_io_sort_key, id_field=id_field))

        for source_row in source_rows:
            surrogate += 1
            local_id = source_row.get(id_field)
            identity = _identity_key(controller_uuid, local_id)
            row = _copy(source_row)
            row["local_" + id_field] = local_id
            row[id_field] = surrogate
            row["controller_uuid"] = controller_uuid
            row["controller_id"] = controller.get("controller_id")
            row["controller_key"] = group.get("controller_key")
            row["node_id"] = group.get("node_id")
            rows.append(row)
            identity_to_surrogate[identity] = surrogate
            surrogate_to_identity[str(surrogate)] = {
                "controller_uuid": controller_uuid,
                id_field: local_id,
            }

    return rows, identity_to_surrogate, surrogate_to_identity


def _project_process_states(config, sensor_map, actuator_map):
    projected = []

    for source_state in config.get("process_states") or []:
        if not _is_dict(source_state):
            continue
        state = _copy(source_state)
        io_refs = state.get("io_refs") or {}
        state["sensor_ids"] = _map_refs(
            io_refs.get("sensors"),
            "sensor_id",
            sensor_map,
        )
        state["actuator_ids"] = _map_refs(
            io_refs.get("actuators"),
            "actuator_id",
            actuator_map,
        )
        projected.append(state)

    return projected


def _map_refs(refs, id_field, identity_map):
    result = []

    for ref in refs or []:
        if not _is_dict(ref):
            continue
        identity = _identity_key(
            ref.get("controller_uuid"),
            ref.get(id_field),
        )
        surrogate = identity_map.get(identity)
        if surrogate is not None:
            result.append(surrogate)

    return result


def _project_filters(config, sensor_map):
    projected = []

    for source_filter in config.get("filters") or []:
        if not _is_dict(source_filter):
            continue
        filter_row = _copy(source_filter)
        filter_row["sensor_ids"] = _map_refs(
            source_filter.get("sensor_refs"),
            "sensor_id",
            sensor_map,
        )
        filter_row["sensors"] = list(filter_row["sensor_ids"])
        projected.append(filter_row)

    return projected


def build_automation_projection(source, controller_uuid, sensor_id):
    config = _source_config(source)
    sensors, sensor_map, reverse_sensors = _flatten_all_io(
        config,
        "sensors",
        "sensor_id",
    )
    actuators, actuator_map, reverse_actuators = _flatten_all_io(
        config,
        "actuators",
        "actuator_id",
    )
    identity = _identity_key(controller_uuid, sensor_id)
    projected_sensor_id = sensor_map.get(identity)

    if projected_sensor_id is None:
        return {}

    projected = _copy(config)
    projected["sensors"] = sensors
    projected["actuators"] = actuators
    projected["process_states"] = _project_process_states(
        config,
        sensor_map,
        actuator_map,
    )
    filters = _project_filters(config, sensor_map)
    projected["automation_filters"] = filters
    projected["sensor_filters"] = filters
    projected["automation_rule_sets"] = []
    projected["_identity_maps"] = {
        "sensors": reverse_sensors,
        "actuators": reverse_actuators,
    }

    return {
        "config": projected,
        "sensor_id": projected_sensor_id,
        "controller_uuid": _text(controller_uuid),
        "local_sensor_id": sensor_id,
        "identity_maps": _copy(projected["_identity_maps"]),
    }


# =============================================================================
# LOCAL SNAPSHOT CELL
# =============================================================================

def create_local_shard_cell(package=None):
    cell = {
        "lock": threading.RLock(),
        "revision": -1,
        "fingerprint": "",
        "package": None,
        "updated_at": None,
    }

    if package is not None:
        apply_shard_package(cell, package)

    return cell


def apply_shard_package(cell, package):
    if not _is_dict(cell) or not _is_dict(package):
        return {
            "ok": False,
            "message": "cell and package must be dicts",
        }

    revision = _safe_int(package.get("revision"), -1)

    with cell["lock"]:
        if revision < cell["revision"]:
            return {
                "ok": False,
                "message": "stale shard revision",
                "revision": cell["revision"],
            }

        cell["package"] = _copy(package)
        cell["revision"] = revision
        cell["fingerprint"] = _text(package.get("fingerprint"))
        cell["updated_at"] = package.get("snapshot_at")

    return {
        "ok": True,
        "message": "shard package applied",
        "revision": revision,
    }


def get_local_shard_snapshot(cell):
    with cell["lock"]:
        return {
            "revision": cell["revision"],
            "fingerprint": cell["fingerprint"],
            "updated_at": cell["updated_at"],
            "package": _copy(cell["package"]),
        }


# =============================================================================
# COMPATIBILITY INTERFACE
# =============================================================================

def initialize_from_config(verified_config):
    try:
        runtime_config = type_library.normalize_config_for_runtime(
            verified_config, representation="auto")
    except (TypeError, ValueError) as exc:
        return {
            "status": "FAILED",
            "verification": {"status": "FAILED", "errors": [str(exc)]},
        }
    report = type_library.verify_runtime_config_data(runtime_config)

    if report.get("status") != "OK":
        return {
            "status": "FAILED",
            "verification": report,
        }

    with CENTRAL_CONTEXT["lock"]:
        CENTRAL_CONTEXT["full_config"] = _copy(runtime_config)
        CENTRAL_CONTEXT["fingerprint"] = type_library.config_fingerprint(
            runtime_config
        )
        CENTRAL_CONTEXT["revision"] += 1
        CENTRAL_CONTEXT["worker_shards"] = {}

        for node_id in _active_worker_nodes(runtime_config):
            CENTRAL_CONTEXT["worker_shards"][node_id] = build_worker_shard(
                runtime_config,
                node_id,
            )

        return {
            "status": "OK",
            "fingerprint": CENTRAL_CONTEXT["fingerprint"],
            "worker_node_ids": tuple(sorted(CENTRAL_CONTEXT["worker_shards"])),
            "shard_count": len(CENTRAL_CONTEXT["worker_shards"]),
        }


def _active_worker_nodes(config):
    return [
        _text(row.get("node_id"))
        for row in config.get("nodes") or []
        if _is_dict(row)
        and row.get("enabled") is not False
        and _text(row.get("node_role")) == "worker"
        and _text(row.get("node_id"))
    ]


def load_and_initialize_config(config_file_path):
    config, report = type_library.load_config_file(config_file_path)

    if config is None:
        return ({"status": "FAILED"}, report)

    return (initialize_from_config(config), report)


def get_full_config_snapshot():
    with CENTRAL_CONTEXT["lock"]:
        return _copy(CENTRAL_CONTEXT["full_config"])


def get_worker_shard_snapshot(node_id):
    with CENTRAL_CONTEXT["lock"]:
        return _copy(CENTRAL_CONTEXT["worker_shards"].get(_text(node_id)))


def get_all_worker_node_ids():
    with CENTRAL_CONTEXT["lock"]:
        return tuple(sorted(CENTRAL_CONTEXT["worker_shards"]))


def get_current_fingerprint():
    with CENTRAL_CONTEXT["lock"]:
        return CENTRAL_CONTEXT["fingerprint"]


def update_full_config(new_verified_config):
    return initialize_from_config(new_verified_config)


def get_shard_stats():
    with CENTRAL_CONTEXT["lock"]:
        return {
            "initialized": CENTRAL_CONTEXT["full_config"] is not None,
            "fingerprint": CENTRAL_CONTEXT["fingerprint"],
            "revision": CENTRAL_CONTEXT["revision"],
            "num_worker_shards": len(CENTRAL_CONTEXT["worker_shards"]),
            "worker_node_ids": tuple(sorted(CENTRAL_CONTEXT["worker_shards"])),
        }


def warm_up(config_file_path):
    report, _verification = load_and_initialize_config(config_file_path)
    return report
