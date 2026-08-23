# -*- coding: utf-8 -*-

# src/library/_generic_system_contract_library/config_sharding_contracts.py

"""Pure entity registry, config graph, sharding and scope-operation contracts."""

import copy

from src.library import _generic_type_library as type_authority


OPERATION_FORMAT = "config_scope_operations_v1"
ALLOWED_OPERATION_KINDS = frozenset((
    "replace_field",
    "upsert_entity",
    "delete_entity",
))

canonical_bytes = type_authority.canonical_bytes
fingerprint = type_authority.fingerprint

_registry_REGISTRY_VERSION = "1.2"

_registry_SECTION_ORDER = (
    "meta", "local_node", "sites", "nodes", "site_controller_map",
    "modbus_functions", "scaling_profiles", "controllers",
    "virtual_controllers", "sensors", "actuators", "sensor_types",
    "actuator_types", "parameter_generic_templates", "schema_mappings",
    "processes", "process_values", "process_modes", "pid_configs",
    "control_method_templates", "control_methods", "process_states",
    "rule_engine", "filters", "timers", "rules",
)

_registry_KEY_FIELDS = {
    "sites": ("site", ("site_id",)),
    "nodes": ("node", ("node_id",)),
    "site_controller_map": ("route", ("controller_uuid", "routing_role")),
    "modbus_functions": ("modbus-function", ("modbus_function_id",)),
    "scaling_profiles": ("scaling", ("profile_id",)),
    "controllers": ("controller", ("controller_uuid",)),
    "virtual_controllers": ("virtual", ("controller_uuid",)),
    "sensor_types": ("sensor-type", ("sensor_types_id",)),
    "actuator_types": ("actuator-type", ("actuator_types_id",)),
    "parameter_generic_templates": ("parameter-template", ("schema_id",)),
    "schema_mappings": ("schema-mapping", ("mapping_id",)),
    "processes": ("process", ("p_id",)),
    "process_values": ("process-value", ("value_id",)),
    "process_modes": ("process-mode", ("mode_id",)),
    "pid_configs": ("pid-config", ("pid_config_id",)),
    "control_method_templates": ("control-template", ("template_id",)),
    "control_methods": ("control-method", ("method_id",)),
    "process_states": ("state", ("state_id",)),
    "rule_engine": ("rule-engine", ("engine_id",)),
    "filters": ("filter", ("filter_id",)),
    "timers": ("timer", ("timer_id",)),
    "rules": ("rule", ("rule_id",)),
}

_registry_GROUPED_SECTIONS = {
    "sensors": ("sensor", "sensor-group", "sensor_id"),
    "actuators": ("actuator", "actuator-group", "actuator_id"),
}


def _registry__text(value):
    if value is None or isinstance(value, bool):
        raise ValueError("entity identity is missing")
    result = str(value).strip()
    if not result or ":" in result:
        raise ValueError("entity identity is invalid")
    return result


def _registry__key(prefix, values):
    return prefix + ":" + ":".join(_registry__text(value) for value in values)


def _registry__row_key(section, row):
    if section == "meta":
        return _registry__key("meta", (row.get("schema"), row.get("version")))
    if section == "local_node":
        return _registry__key("local-node", (row.get("node_id"),))
    specification = _registry_KEY_FIELDS.get(section)
    if not specification:
        raise ValueError("unsupported config section: " + str(section))
    prefix, fields = specification
    return _registry__key(prefix, tuple(row.get(field) for field in fields))


def _registry_entity_key(section, row, **kwargs):
    """Return the exact registry key for one supported entity."""
    if section in _registry_GROUPED_SECTIONS:
        prefix, _group_prefix, id_field = _registry_GROUPED_SECTIONS[section]
        controller_uuid = kwargs.get("controller_uuid") or row.get("controller_uuid")
        return _registry__key(prefix, (controller_uuid, row.get(id_field)))
    return _registry__row_key(section, row)


def _registry__record(section, key, value, position, **kwargs):
    return {
        "section": section,
        "entity_key": key,
        "position": int(position),
        "group_position": kwargs.get("group_position"),
        "row_position": kwargs.get("row_position"),
        "payload": copy.deepcopy(value),
    }


def _registry__decompose_group(section, groups):
    prefix, group_prefix, id_field = _registry_GROUPED_SECTIONS[section]
    records = []
    for group_position, group in enumerate(groups):
        if not isinstance(group, dict):
            raise ValueError(section + " group must be an object")
        controller_uuid = group.get("controller_uuid")
        header = {key: copy.deepcopy(value) for key, value in group.items() if key != "rows"}
        group_key = _registry__key(group_prefix, (controller_uuid,))
        records.append(_registry__record(
            section, group_key, {"kind": "group", "header": header},
            group_position, group_position=group_position, row_position=-1,
        ))
        rows = group.get("rows")
        if not isinstance(rows, list):
            raise ValueError(section + " rows must be a list")
        for row_position, row in enumerate(rows):
            if not isinstance(row, dict):
                raise ValueError(section + " row must be an object")
            key = _registry__key(prefix, (controller_uuid, row.get(id_field)))
            records.append(_registry__record(
                section, key,
                {"kind": "row", "group_key": group_key, "row": row},
                group_position,
                group_position=group_position,
                row_position=row_position,
            ))
    return records


def _registry_decompose(config, **kwargs):
    """Decompose canonical storage or an explicitly declared runtime view."""
    if not isinstance(config, dict):
        raise ValueError("configuration must be an object")
    version = type_authority.get_config_version(config)
    representation = str(kwargs.get("representation") or "storage").lower()
    allow_legacy = bool(kwargs.get("allow_legacy_v5", False))
    allow_cross = bool(kwargs.get("allow_cross_v8", False))
    if version == type_authority.CROSS_VERSION:
        if not allow_cross:
            raise ValueError("Cross-V8 registry access requires allow_cross_v8")
        runtime = type_authority.normalize_cross_v8_for_runtime(
            config, representation=representation)
    elif version == type_authority.LEGACY_VERSION:
        if not allow_legacy:
            raise ValueError("legacy V5 requires explicit import_legacy_v5_config")
        runtime = type_authority.normalize_config_for_runtime(
            config, representation=representation, allow_legacy_v5=True)
    else:
        runtime = type_authority.normalize_config_for_runtime(
            config, representation=representation)
    unknown = sorted(set(runtime) - set(_registry_SECTION_ORDER))
    if unknown:
        raise ValueError("unsupported config sections: " + ",".join(unknown))
    missing = [section for section in _registry_SECTION_ORDER if section not in runtime]
    if missing:
        raise ValueError("missing config sections: " + ",".join(missing))
    records = []
    seen = set()
    for section in _registry_SECTION_ORDER:
        rows = runtime.get(section)
        if not isinstance(rows, list):
            raise ValueError(section + " must be a list")
        section_records = _registry__decompose_group(section, rows) \
            if section in _registry_GROUPED_SECTIONS else [
                _registry__record(section, _registry__row_key(section, row), row, position)
                for position, row in enumerate(rows)
                if isinstance(row, dict)
            ]
        if len(section_records) != len(rows) and section not in _registry_GROUPED_SECTIONS:
            raise ValueError(section + " row must be an object")
        for record in section_records:
            identity = (record["section"], record["entity_key"])
            if identity in seen:
                raise ValueError("duplicate entity: " + "/".join(identity))
            seen.add(identity)
            records.append(record)
    return tuple(records)


def _registry__materialize_group(section, records):
    groups = {}
    rows = []
    for record in records:
        payload = record.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("grouped entity payload must be an object")
        if payload.get("kind") == "group":
            header = copy.deepcopy(payload.get("header"))
            if not isinstance(header, dict):
                raise ValueError("group header must be an object")
            header["rows"] = []
            groups[record["entity_key"]] = {
                "position": int(record.get("group_position", record.get("position", 0))),
                "value": header,
            }
        elif payload.get("kind") == "row":
            rows.append(record)
        else:
            raise ValueError("unknown grouped entity kind")
    for record in rows:
        payload = record["payload"]
        group = groups.get(payload.get("group_key"))
        if not group:
            raise ValueError("row references an unknown group")
        group["value"]["rows"].append((
            int(record.get("row_position", 0)),
            copy.deepcopy(payload.get("row")),
        ))
    result = []
    for item in sorted(groups.values(), key=_registry_group_position_key):
        item["value"]["rows"] = [row for _position, row in sorted(
            item["value"]["rows"], key=_registry_row_position_key)]
        result.append(item["value"])
    return result


def _registry_group_position_key(item):
    """Return a deterministic group sort key for grouped entities."""
    return item["position"]


def _registry_row_position_key(item):
    """Return a deterministic grouped-row sort key."""
    return item[0]


def _registry_record_position_key(record):
    """Return a deterministic ordinary-row sort key."""
    return (int(record.get("position", 0)), str(record.get("entity_key") or ""))


def _registry_materialize(records, **kwargs):
    """Reconstruct the representation declared by the persisted meta entity."""
    if not isinstance(records, (list, tuple)):
        raise ValueError("entity records must be a list or tuple")
    by_section = {section: [] for section in _registry_SECTION_ORDER}
    seen = set()
    for supplied in records:
        if not isinstance(supplied, dict):
            raise ValueError("entity record must be an object")
        record = copy.deepcopy(supplied)
        section = record.get("section")
        key = record.get("entity_key")
        if section not in by_section:
            raise ValueError("unsupported entity section")
        identity = (section, key)
        if identity in seen:
            raise ValueError("duplicate entity record")
        seen.add(identity)
        by_section[section].append(record)
    result = {}
    for section in _registry_SECTION_ORDER:
        section_records = by_section[section]
        if section in _registry_GROUPED_SECTIONS:
            result[section] = _registry__materialize_group(section, section_records)
        else:
            ordered = sorted(section_records, key=_registry_record_position_key)
            result[section] = [copy.deepcopy(record.get("payload")) for record in ordered]
    version = type_authority.get_config_version(result)
    representation = str(kwargs.get("representation") or "storage").lower()
    if representation not in ("storage", "runtime"):
        raise ValueError("unsupported registry representation: " + representation)
    if version == type_authority.LEGACY_VERSION:
        return result
    if version == type_authority.CROSS_VERSION:
        if not bool(kwargs.get("allow_cross_v8", False)):
            raise ValueError("Cross-V8 materialization requires allow_cross_v8")
        if representation == "runtime":
            return type_authority.normalize_cross_v8_for_runtime(
                result, representation="runtime")
        return type_authority.project_cross_v8_for_storage(result)
    if version != type_authority.CANONICAL_VERSION:
        raise ValueError("entity records declare an unsupported config version")
    if representation == "runtime":
        return type_authority.normalize_config_for_runtime(
            result, representation="runtime")
    return type_authority.project_config_for_storage(result)


def _registry_verify_roundtrip(config, **kwargs):
    """Verify the binding lossless registry invariant."""
    rebuilt = _registry_materialize(_registry_decompose(config, **kwargs), **kwargs)
    return {
        "ok": canonical_bytes(rebuilt) == canonical_bytes(config),
        "source_fingerprint": fingerprint(config),
        "materialized_fingerprint": fingerprint(rebuilt),
        "entity_count": len(_registry_decompose(config, **kwargs)),
    }


_scope_MAX_PATCH_OPERATIONS = 500


def _scope__index(config, **kwargs):
    return {
        (record["section"], record["entity_key"]): record
        for record in decompose(config, **kwargs)
    }


def _scope_identity_key(identity):
    """Sort a section/entity identity deterministically."""
    return (str(identity[0]), str(identity[1]))


def _scope_diff_configs(before, after, **kwargs):
    """Create deterministic V9 operations; V5 is migration-only."""
    prior = _scope__index(before, **kwargs)
    target = _scope__index(after, **kwargs)
    operations = []
    for identity in sorted(set(prior) | set(target), key=_scope_identity_key):
        old = prior.get(identity)
        new = target.get(identity)
        section, entity_key = identity
        if new is None:
            operations.append({
                "op": "delete_entity",
                "section": section,
                "entity_key": entity_key,
                "expected_value_hash": fingerprint(old),
            })
        elif old is None or fingerprint(old) != fingerprint(new):
            operations.append({
                "op": "upsert_entity",
                "section": section,
                "entity_key": entity_key,
                "expected_value_hash": fingerprint(old) if old is not None else None,
                "value": copy.deepcopy(new),
                "value_hash": fingerprint(new),
            })
    return tuple(operations)


def _scope__decode_pointer(pointer):
    if pointer in (None, ""):
        return ()
    text = str(pointer)
    if not text.startswith("/"):
        raise ValueError("JSON pointer must start with slash")
    return tuple(token.replace("~1", "/").replace("~0", "~") for token in text[1:].split("/"))


def _scope__pointer_parent(value, tokens):
    current = value
    for token in tokens[:-1]:
        if isinstance(current, list):
            current = current[int(token)]
        elif isinstance(current, dict):
            current = current[token]
        else:
            raise ValueError("JSON pointer does not resolve")
    return current


def _scope__replace_field(record, operation):
    tokens = _scope__decode_pointer(operation.get("json_pointer"))
    if not tokens:
        raise ValueError("field patch requires a non-root pointer")
    parent = _scope__pointer_parent(record, tokens)
    token = tokens[-1]
    current = parent[int(token)] if isinstance(parent, list) else parent[token]
    expected = operation.get("expected_value_hash")
    if expected is not None and fingerprint(current) != expected:
        raise ValueError("field value hash differs")
    if isinstance(parent, list):
        parent[int(token)] = copy.deepcopy(operation.get("value"))
    elif isinstance(parent, dict):
        parent[token] = copy.deepcopy(operation.get("value"))
    else:
        raise ValueError("field patch parent is invalid")


def _scope_apply_operations(config, operations, **kwargs):
    """Apply bounded entity or field operations to a detached baseline."""
    maximum = min(_scope_MAX_PATCH_OPERATIONS, max(1, int(kwargs.get("maximum", _scope_MAX_PATCH_OPERATIONS))))
    if not isinstance(operations, (list, tuple)) or len(operations) > maximum:
        raise ValueError("patch operation count exceeds the configured limit")
    records = _scope__index(config, **kwargs)
    for operation in operations:
        if not isinstance(operation, dict):
            raise ValueError("patch operation must be an object")
        identity = (operation.get("section"), operation.get("entity_key"))
        kind = operation.get("op")
        existing = records.get(identity)
        expected = operation.get("expected_value_hash")
        if expected is not None and kind != "replace_field":
            if existing is None or fingerprint(existing) != expected:
                raise ValueError("entity value hash differs")
        if kind == "delete_entity":
            if existing is None:
                raise ValueError("cannot delete an unknown entity")
            del records[identity]
        elif kind == "upsert_entity":
            value = copy.deepcopy(operation.get("value"))
            if not isinstance(value, dict):
                raise ValueError("upsert value must be an entity record")
            if (value.get("section"), value.get("entity_key")) != identity:
                raise ValueError("upsert identity differs")
            if operation.get("value_hash") and fingerprint(value) != operation["value_hash"]:
                raise ValueError("upsert value hash differs")
            records[identity] = value
        elif kind == "replace_field":
            if existing is None:
                raise ValueError("cannot patch an unknown entity")
            _scope__replace_field(existing, operation)
        else:
            raise ValueError("unknown patch operation")
    return materialize(tuple(records.values()), **kwargs)


_shard_CONTEXT = {
    "library_name": "cross_scope_library",
    "library_version": "1.1.0",
    "description": "Deterministic local/cross automation graph projection",
}

_shard_SCOPE_LOCAL = "local_controller"
_shard_SCOPE_CROSS_CONTROLLER = "cross_controller"
_shard_SCOPE_CROSS_WORKER = "cross_worker"
_shard_SCOPE_UNBOUND = "unbound"
_shard_SCOPE_INVALID = "invalid"

_shard_EXECUTION_SCOPES = (
    _shard_SCOPE_LOCAL,
    _shard_SCOPE_CROSS_CONTROLLER,
    _shard_SCOPE_CROSS_WORKER,
)

_shard_CRITICAL_GROUPS = {
    "emergency",
    "hard_real_time",
    "hard_realtime",
    "interlock",
    "real_time",
    "realtime",
    "safety",
    "safety_critical",
    "soft_realtime",
    "time_critical",
}

_shard_RUNTIME_SECTIONS = {
    "actuators",
    "controllers",
    "filters",
    "local_node",
    "nodes",
    "process_states",
    "processes",
    "rules",
    "sensors",
    "site_controller_map",
    "virtual_controllers",
}


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def _shard__is_dict(value):
    return isinstance(value, dict)


def _shard__copy(value):
    return copy.deepcopy(value)


def _shard__text(value):
    return str(value or "").strip()


def _shard__id_key(value):
    if value is None or isinstance(value, bool):
        return ""
    return _shard__text(value)


def _shard__source_config(source):
    if not _shard__is_dict(source):
        return {}
    sections = source.get("sections")
    config = sections if _shard__is_dict(sections) else source
    representation = "runtime" \
        if type_authority.get_config_io_layout(config) == "grouped" \
        else "storage"
    if type_authority.get_config_version(config) \
            == type_authority.CROSS_VERSION:
        return type_authority.normalize_cross_v8_for_runtime(
            config, representation=representation)
    return type_authority.normalize_config_for_runtime(
        config, representation=representation)


def _shard__sorted_ids(values):
    def sort_key(value):
        key = _shard__id_key(value)
        try:
            return (0, int(key), key)
        except Exception:
            return (1, 0, key)

    return tuple(sorted(values, key=sort_key))


def _shard__new_report():
    return {
        "ok": True,
        "errors": [],
        "warnings": [],
        "stats": {},
    }


def _shard__error(report, message):
    text = _shard__text(message)
    if text and text not in report["errors"]:
        report["errors"].append(text)
    report["ok"] = False


def _shard__warning(report, message):
    text = _shard__text(message)
    if text and text not in report["warnings"]:
        report["warnings"].append(text)


def _shard__finish_report(report):
    report["errors"] = tuple(sorted(report["errors"]))
    report["warnings"] = tuple(sorted(report["warnings"]))
    report["ok"] = not report["errors"]
    return report


def _shard_canonical_fingerprint(value):
    """Delegate canonical configuration hashing to the Type Authority."""
    return type_authority.config_fingerprint(value)

def _shard__meta_row(config):
    rows = config.get("meta") or []
    if isinstance(rows, list) and rows and _shard__is_dict(rows[0]):
        return rows[0]
    if _shard__is_dict(rows):
        return rows
    return {}


def _shard__with_meta(config, scope, node_id="", owner_node_id=""):
    source = _shard__meta_row(config)
    row = _shard__copy(source)
    row["scope"] = scope
    if scope == "node_local":
        row["version"] = type_authority.CANONICAL_VERSION
    if scope == "cross":
        row["version"] = type_authority.CROSS_VERSION
        if not _shard__text(row.get("domain_id") or row.get("cross_domain_id")):
            row["domain_id"] = "cross-{}-default".format(
                _shard__text(owner_node_id) or "unassigned")
    if node_id:
        row["node_id"] = node_id
    else:
        row.pop("node_id", None)
    if owner_node_id:
        row["owner_node_id"] = owner_node_id
    else:
        row.pop("owner_node_id", None)
    return [row]


def _shard__copy_catalog(config):
    result = {}
    for key, value in config.items():
        if key not in _shard_RUNTIME_SECTIONS and key != "meta":
            result[key] = _shard__copy(value)
    return result


# =============================================================================
# ROUTING OWNERSHIP
# =============================================================================

def _shard__known_controller_uuids(config):
    result = set()
    for section in (
        "controllers",
        "virtual_controllers",
        "site_controller_map",
        "sensors",
        "actuators",
    ):
        for row in config.get(section) or []:
            if not _shard__is_dict(row):
                continue
            controller_uuid = _shard__text(row.get("controller_uuid"))
            if controller_uuid:
                result.add(controller_uuid)
    for state in config.get("process_states") or []:
        result.update(_shard__state_controller_uuids(state))
    for rule in config.get("rules") or []:
        references = _shard__new_references()
        _shard__collect_references(rule, references)
        result.update(references["controllers"])
    for row in config.get("filters") or []:
        references = _shard__new_references()
        _shard__collect_references(row, references)
        result.update(references["controllers"])
    return tuple(sorted(result))


def _shard__owner_rows(config):
    result = {}
    for route in config.get("site_controller_map") or []:
        if not _shard__is_dict(route) or route.get("enabled") is False:
            continue
        if _shard__text(route.get("routing_role")) != "owner":
            continue
        controller_uuid = _shard__text(route.get("controller_uuid"))
        if not controller_uuid:
            continue
        result.setdefault(controller_uuid, []).append(route)
    return result


def _shard__owner_analysis(config):
    rows_by_uuid = _shard__owner_rows(config)
    owners = {}
    errors = []
    for controller_uuid in _shard__known_controller_uuids(config):
        rows = rows_by_uuid.get(controller_uuid) or []
        if len(rows) != 1:
            errors.append(
                "controller {} must have exactly one enabled owner route; found {}".format(
                    controller_uuid,
                    len(rows),
                )
            )
            continue
        node_id = _shard__text(rows[0].get("node_id"))
        if not node_id:
            errors.append("controller {} owner route has no node_id".format(controller_uuid))
            continue
        owners[controller_uuid] = node_id
    return owners, tuple(sorted(errors))


def _shard_controller_owner_map(source):
    """Return only controller UUIDs with one unambiguous enabled owner."""

    config = _shard__source_config(source)
    owners, unused_errors = _shard__owner_analysis(config)
    return _shard__copy(owners)


def _shard_validate_controller_ownership(source):
    """Validate the exact-one-owner routing invariant for every used UUID."""

    config = _shard__source_config(source)
    report = _shard__new_report()
    owners, errors = _shard__owner_analysis(config)
    for message in errors:
        _shard__error(report, message)

    enabled_nodes = {
        _shard__text(row.get("node_id"))
        for row in config.get("nodes") or []
        if _shard__is_dict(row) and row.get("enabled") is not False
    }
    for controller_uuid, node_id in sorted(owners.items()):
        if enabled_nodes and node_id not in enabled_nodes:
            _shard__error(
                report,
                "controller {} owner node {} is not enabled in nodes".format(
                    controller_uuid,
                    node_id,
                ),
            )

    route_keys = set()
    local_routes = set()
    for route in config.get("site_controller_map") or []:
        if not _shard__is_dict(route) or route.get("enabled") is False:
            continue
        route_key = _shard__text(route.get("route_key"))
        if route_key:
            if route_key in route_keys:
                _shard__error(report, "duplicate enabled route_key {}".format(route_key))
            route_keys.add(route_key)
        local_key = (
            _shard__text(route.get("node_id")),
            _shard__id_key(route.get("local_controller_id")),
        )
        if local_key[0] and local_key[1]:
            if local_key in local_routes:
                _shard__error(
                    report,
                    "duplicate enabled local controller route {}:{}".format(
                        local_key[0],
                        local_key[1],
                    ),
                )
            local_routes.add(local_key)

    report["stats"] = {
        "controller_count": len(_shard__known_controller_uuids(config)),
        "resolved_owner_count": len(owners),
    }
    return _shard__finish_report(report)


# =============================================================================
# GRAPH REFERENCE COLLECTION
# =============================================================================

def _shard__new_references():
    return {
        "controllers": set(),
        "states": set(),
        "processes": set(),
        "filters": set(),
        "sensors": set(),
        "actuators": set(),
    }


def _shard__collect_references(value, references):
    if isinstance(value, list):
        for item in value:
            _shard__collect_references(item, references)
        return
    if not _shard__is_dict(value):
        return

    controller_uuid = _shard__text(value.get("controller_uuid"))
    if controller_uuid:
        references["controllers"].add(controller_uuid)
        sensor_id = _shard__id_key(value.get("sensor_id"))
        actuator_id = _shard__id_key(value.get("actuator_id"))
        if sensor_id:
            references["sensors"].add((controller_uuid, sensor_id))
        if actuator_id:
            references["actuators"].add((controller_uuid, actuator_id))

    state_id = _shard__id_key(value.get("state_id"))
    if state_id:
        references["states"].add(state_id)
    for item in value.get("state_ids") or []:
        key = _shard__id_key(item)
        if key:
            references["states"].add(key)

    parameter_state_id = _shard__id_key(value.get("param_of_state"))
    if parameter_state_id:
        references["states"].add(parameter_state_id)

    process_id = _shard__id_key(value.get("p_id"))
    if process_id:
        references["processes"].add(process_id)
    for item in value.get("p_ids") or []:
        key = _shard__id_key(item)
        if key:
            references["processes"].add(key)

    filter_id = _shard__id_key(value.get("filter_id"))
    if filter_id:
        references["filters"].add(filter_id)

    for child in value.values():
        if isinstance(child, (dict, list)):
            _shard__collect_references(child, references)


def _shard__state_controller_uuids(state):
    if not _shard__is_dict(state):
        return set()
    references = _shard__new_references()
    _shard__collect_references(state.get("io_refs") or {}, references)
    setpoint_source = state.get("setpoint_source")
    if _shard__is_dict(setpoint_source):
        _shard__collect_references(setpoint_source, references)
    return references["controllers"]


def _shard__state_index(config):
    result = {}
    for state in config.get("process_states") or []:
        if not _shard__is_dict(state):
            continue
        key = _shard__id_key(state.get("state_id"))
        if key and key not in result:
            result[key] = state
    return result


def _shard__process_state_index(config):
    result = {}
    for state in config.get("process_states") or []:
        if not _shard__is_dict(state):
            continue
        process_key = _shard__id_key(state.get("p_id"))
        state_key = _shard__id_key(state.get("state_id"))
        if process_key and state_key:
            result.setdefault(process_key, set()).add(state_key)
    return result


def _shard__filter_index(config):
    result = {}
    for row in config.get("filters") or []:
        if not _shard__is_dict(row):
            continue
        key = _shard__id_key(row.get("filter_id"))
        if key and key not in result:
            result[key] = row
    return result


def _shard__rule_references(config, rule):
    references = _shard__new_references()
    _shard__collect_references(rule, references)
    process_states = _shard__process_state_index(config)
    for process_key in tuple(references["processes"]):
        references["states"].update(process_states.get(process_key) or set())

    states = _shard__state_index(config)
    for state_key in tuple(references["states"]):
        state = states.get(state_key)
        if state is not None:
            _shard__collect_references(state.get("io_refs") or {}, references)

    filters = _shard__filter_index(config)
    for filter_key in tuple(references["filters"]):
        row = filters.get(filter_key)
        if row is not None:
            _shard__collect_references(row.get("sensor_refs") or [], references)
    return references


def _shard__collect_action_mutations(value, state_keys, process_keys):
    if isinstance(value, list):
        for item in value:
            _shard__collect_action_mutations(item, state_keys, process_keys)
        return
    if not _shard__is_dict(value):
        return
    state_id = _shard__id_key(value.get("state_id"))
    if state_id:
        state_keys.add(state_id)
    for item in value.get("state_ids") or []:
        key = _shard__id_key(item)
        if key:
            state_keys.add(key)
    process_id = _shard__id_key(value.get("p_id"))
    if process_id:
        process_keys.add(process_id)
    for item in value.get("p_ids") or []:
        key = _shard__id_key(item)
        if key:
            process_keys.add(key)
    for child in value.values():
        if isinstance(child, (dict, list)):
            _shard__collect_action_mutations(child, state_keys, process_keys)


def _shard__rule_mutated_state_keys(config, rule):
    state_keys = set()
    process_keys = set()
    for name in ("do", "else_do", "actions", "then", "on_true", "on_false"):
        _shard__collect_action_mutations(
            rule.get(name),
            state_keys,
            process_keys,
        )
    process_states = _shard__process_state_index(config)
    for process_key in process_keys:
        state_keys.update(process_states.get(process_key) or set())
    return state_keys


def _shard__contains_notify_action(value):
    if isinstance(value, list):
        return any(_shard__contains_notify_action(item) for item in value)
    if not _shard__is_dict(value):
        return False
    if "notify" in value or "notification" in value:
        return True
    return any(
        _shard__contains_notify_action(child)
        for child in value.values()
        if isinstance(child, (dict, list))
    )


def _shard__rule_has_notify_action(rule):
    for name in ("do", "else_do", "actions", "then", "on_true", "on_false"):
        if _shard__contains_notify_action(rule.get(name)):
            return True
    return False


def _shard_cross_rule_eligibility(source, rule):
    """Return the fail-closed cross-writer eligibility of one rule."""

    config = _shard__source_config(source)
    analysis = _shard_classify_config_scopes(config)
    rule_key = _shard__id_key(rule.get("rule_id")) if _shard__is_dict(rule) else ""
    record = analysis["rules"].get(rule_key) or {}
    mutated = _shard__rule_mutated_state_keys(config, rule or {})
    cross_states = {
        key
        for key, state_record in analysis["states"].items()
        if state_record["scope"] in (
            _shard_SCOPE_CROSS_CONTROLLER,
            _shard_SCOPE_CROSS_WORKER,
        )
    }
    invalid_targets = mutated.difference(cross_states)
    reasons = []
    if record.get("scope") not in (
        _shard_SCOPE_CROSS_CONTROLLER,
        _shard_SCOPE_CROSS_WORKER,
    ):
        reasons.append("rule graph is not cross-scoped")
    if _shard_is_critical_rule(rule):
        reasons.append("safety/time-critical rules may not execute cross-side")
    if invalid_targets:
        reasons.append(
            "mutated states are not cross-owned: {}".format(
                ",".join(sorted(invalid_targets))
            )
        )
    notify_only = not mutated and _shard__rule_has_notify_action(rule or {})
    if not mutated and not notify_only:
        reasons.append("cross rule has no cross-owned state target")
    return {
        "ok": not reasons,
        "rule_id": rule.get("rule_id") if _shard__is_dict(rule) else None,
        "scope": record.get("scope") or _shard_SCOPE_INVALID,
        "mutated_state_ids": _shard__sorted_ids(mutated),
        "invalid_target_state_ids": _shard__sorted_ids(invalid_targets),
        "notify_only": notify_only,
        "errors": tuple(reasons),
    }


def _shard__scope_for_controllers(controller_uuids, owners):
    identities = tuple(sorted(set(controller_uuids)))
    errors = []
    if not identities:
        return _shard_SCOPE_UNBOUND, (), (), ("graph has no controller_uuid anchor",)

    nodes = []
    for controller_uuid in identities:
        node_id = owners.get(controller_uuid)
        if not node_id:
            errors.append("controller {} has no unique owner".format(controller_uuid))
        elif node_id not in nodes:
            nodes.append(node_id)
    if errors:
        return _shard_SCOPE_INVALID, identities, tuple(sorted(nodes)), tuple(sorted(errors))
    if len(identities) == 1:
        return _shard_SCOPE_LOCAL, identities, tuple(sorted(nodes)), ()
    if len(nodes) == 1:
        return _shard_SCOPE_CROSS_CONTROLLER, identities, tuple(sorted(nodes)), ()
    return _shard_SCOPE_CROSS_WORKER, identities, tuple(sorted(nodes)), ()


def _shard__entity_record(entity_id, controller_uuids, owners):
    scope, identities, nodes, errors = _shard__scope_for_controllers(
        controller_uuids,
        owners,
    )
    return {
        "id": entity_id,
        "scope": scope,
        "controller_uuids": identities,
        "node_ids": nodes,
        "errors": errors,
    }


def _shard_classify_config_scopes(source):
    """Classify all states and rules from their complete UUID graph span."""

    config = _shard__source_config(source)
    owners, owner_errors = _shard__owner_analysis(config)
    states = {}
    rules = {}

    for state in config.get("process_states") or []:
        if not _shard__is_dict(state):
            continue
        state_id = _shard__id_key(state.get("state_id"))
        if not state_id:
            continue
        states[state_id] = _shard__entity_record(
            state.get("state_id"),
            _shard__state_controller_uuids(state),
            owners,
        )

    for rule in config.get("rules") or []:
        if not _shard__is_dict(rule):
            continue
        rule_id = _shard__id_key(rule.get("rule_id"))
        if not rule_id:
            continue
        references = _shard__rule_references(config, rule)
        record = _shard__entity_record(
            rule.get("rule_id"),
            references["controllers"],
            owners,
        )
        record["state_ids"] = _shard__sorted_ids(references["states"])
        record["process_ids"] = _shard__sorted_ids(references["processes"])
        record["filter_ids"] = _shard__sorted_ids(references["filters"])
        rules[rule_id] = record

    state_scopes = {}
    rule_scopes = {}
    for scope in (_shard_SCOPE_LOCAL, _shard_SCOPE_CROSS_CONTROLLER, _shard_SCOPE_CROSS_WORKER,
                  _shard_SCOPE_UNBOUND, _shard_SCOPE_INVALID):
        state_scopes[scope] = _shard__sorted_ids(
            record["id"] for record in states.values() if record["scope"] == scope
        )
        rule_scopes[scope] = _shard__sorted_ids(
            record["id"] for record in rules.values() if record["scope"] == scope
        )

    return {
        "controller_owners": _shard__copy(owners),
        "owner_errors": owner_errors,
        "states": states,
        "rules": rules,
        "state_scopes": state_scopes,
        "rule_scopes": rule_scopes,
    }


def _shard_is_critical_rule(rule):
    """Return True for safety, interlock or explicitly time-critical rules."""

    if not _shard__is_dict(rule):
        return False
    group = _shard__text(rule.get("group")).lower().replace("-", "_")
    if group in _shard_CRITICAL_GROUPS:
        return True
    for key in ("critical", "safety_critical", "time_critical", "hard_realtime"):
        if rule.get(key) is True:
            return True
    for key in ("deadline_ms", "max_latency_ms"):
        value = rule.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value >= 0:
                return True
    latency = _shard__text(rule.get("latency_class")).lower().replace("-", "_")
    return latency in _shard_CRITICAL_GROUPS


# =============================================================================
# LOCAL EXECUTION SHARD
# =============================================================================

def _shard__known_node(config, node_id):
    for row in config.get("nodes") or []:
        if _shard__is_dict(row) and row.get("enabled") is not False:
            if _shard__text(row.get("node_id")) == node_id:
                return True
    for route in config.get("site_controller_map") or []:
        if _shard__is_dict(route) and route.get("enabled") is not False:
            if _shard__text(route.get("node_id")) == node_id:
                return True
    return False


def _shard__rows_for_controller_uuids(rows, controller_uuids):
    return [
        _shard__copy(row)
        for row in rows or []
        if _shard__is_dict(row) and _shard__text(row.get("controller_uuid")) in controller_uuids
    ]


def _shard__normalize_controller_allowlist(controller_allowlist):
    if controller_allowlist is None:
        return None
    if isinstance(controller_allowlist, str):
        controller_allowlist = (controller_allowlist,)
    if not isinstance(controller_allowlist, (list, tuple, set, frozenset)):
        return set()
    return {
        _shard__text(item)
        for item in controller_allowlist
        if _shard__text(item)
    }


def _shard__local_node_projection(rows, node_id, controller_uuids):
    result = []
    for row in rows or []:
        if not _shard__is_dict(row) or _shard__text(row.get("node_id")) != node_id:
            continue
        copied = _shard__copy(row)
        copied["active_controller_uuids"] = [
            controller_uuid
            for controller_uuid in copied.get("active_controller_uuids") or []
            if _shard__text(controller_uuid) in controller_uuids
        ]
        copied["active_controller_routes"] = [
            route
            for route in copied.get("active_controller_routes") or []
            if _shard__is_dict(route)
            and _shard__text(route.get("controller_uuid")) in controller_uuids
        ]
        result.append(copied)
    return result


def _shard__route_projection_sort_key(row):
    value = row.get("local_controller_id")
    try:
        local_id = int(value)
    except Exception:
        local_id = 0
    return (
        local_id,
        _shard__text(row.get("controller_uuid")),
    )


def _shard__synthesise_local_node(config, node_id, controller_uuids):
    """Create the runtime ownership row for a valid hardware-only node.

    Older global authority files contain a ``local_node`` row only for the
    machine that originally exported the document.  A projected node shard
    must nevertheless be self-contained.  The owner routes are authoritative
    and therefore provide the deterministic fallback.
    """
    routes = []
    for row in config.get("site_controller_map") or []:
        if not _shard__is_dict(row) or row.get("enabled") is False:
            continue
        if _shard__text(row.get("routing_role") or "owner") != "owner":
            continue
        controller_uuid = _shard__text(row.get("controller_uuid"))
        if (
            _shard__text(row.get("node_id")) != node_id
            or controller_uuid not in controller_uuids
        ):
            continue
        routes.append({
            "local_controller_id": row.get("local_controller_id"),
            "controller_uuid": controller_uuid,
            "controller_key": row.get("controller_key"),
        })
    routes.sort(key=_shard__route_projection_sort_key)
    return [{
        "node_id": node_id,
        "active_controller_uuids": sorted(controller_uuids),
        "active_controller_routes": routes,
    }]


def _shard_build_local_execution_shard(source, node_id, controller_allowlist=None):
    """Build one strict physical-node shard; unknown nodes get empty logic."""

    config = _shard__source_config(source)
    normalized_node = _shard__text(node_id)
    result = _shard__copy_catalog(config)
    result["meta"] = _shard__with_meta(config, "node_local", node_id=normalized_node)

    owners, unused_errors = _shard__owner_analysis(config)
    known = bool(normalized_node and _shard__known_node(config, normalized_node))
    controller_uuids = {
        controller_uuid
        for controller_uuid, owner_node in owners.items()
        if known and owner_node == normalized_node
    }
    allowed = _shard__normalize_controller_allowlist(controller_allowlist)
    if allowed is not None:
        controller_uuids.intersection_update(allowed)

    result["nodes"] = [
        _shard__copy(row)
        for row in config.get("nodes") or []
        if _shard__is_dict(row) and _shard__text(row.get("node_id")) == normalized_node
    ] if known else []
    result["site_controller_map"] = [
        _shard__copy(row)
        for row in config.get("site_controller_map") or []
        if _shard__is_dict(row)
        and row.get("enabled") is not False
        and _shard__text(row.get("routing_role")) == "owner"
        and _shard__text(row.get("node_id")) == normalized_node
        and _shard__text(row.get("controller_uuid")) in controller_uuids
    ] if known else []
    result["local_node"] = _shard__local_node_projection(
        config.get("local_node"),
        normalized_node,
        controller_uuids,
    ) if known else []
    if known and not result["local_node"]:
        result["local_node"] = _shard__synthesise_local_node(
            config,
            normalized_node,
            controller_uuids,
        )
    result["controllers"] = _shard__rows_for_controller_uuids(
        config.get("controllers"),
        controller_uuids,
    )
    result["virtual_controllers"] = _shard__rows_for_controller_uuids(
        config.get("virtual_controllers"),
        controller_uuids,
    )
    result["sensors"] = _shard__rows_for_controller_uuids(
        config.get("sensors"),
        controller_uuids,
    )
    result["actuators"] = _shard__rows_for_controller_uuids(
        config.get("actuators"),
        controller_uuids,
    )

    analysis = _shard_classify_config_scopes(config)
    local_state_keys = {
        key
        for key, record in analysis["states"].items()
        if known
        and record["scope"] == _shard_SCOPE_LOCAL
        and record["node_ids"] == (normalized_node,)
        and set(record["controller_uuids"]).issubset(controller_uuids)
    }
    local_rule_keys = {
        key
        for key, record in analysis["rules"].items()
        if known
        and record["scope"] == _shard_SCOPE_LOCAL
        and record["node_ids"] == (normalized_node,)
        and set(record["controller_uuids"]).issubset(controller_uuids)
    }

    result["process_states"] = [
        _shard__copy(row)
        for row in config.get("process_states") or []
        if _shard__is_dict(row) and _shard__id_key(row.get("state_id")) in local_state_keys
    ]
    retained_processes = {
        _shard__id_key(row.get("p_id"))
        for row in result["process_states"]
        if _shard__id_key(row.get("p_id"))
    }
    result["processes"] = []
    for row in config.get("processes") or []:
        if not _shard__is_dict(row):
            continue
        if _shard__id_key(row.get("p_id")) not in retained_processes:
            continue
        process = _shard__copy(row)
        process["owner_node_id"] = normalized_node
        result["processes"].append(process)
    result["rules"] = [
        _shard__copy(row)
        for row in config.get("rules") or []
        if _shard__is_dict(row)
        and _shard__id_key(row.get("rule_id")) in local_rule_keys
        and (
            not _shard__text(row.get("owner_node_id"))
            or _shard__text(row.get("owner_node_id")) == normalized_node
        )
    ]
    result["filters"] = []
    for row in config.get("filters") or []:
        if not _shard__is_dict(row):
            continue
        references = _shard__new_references()
        _shard__collect_references(row.get("sensor_refs") or [], references)
        if references["controllers"] and references["controllers"].issubset(controller_uuids):
            result["filters"].append(_shard__copy(row))
    return type_authority.project_config_for_storage(result)


# =============================================================================
# CROSS EXECUTION SHARD
# =============================================================================

def _shard__rule_owner(rule, record):
    explicit = _shard__text(rule.get("owner_node_id"))
    if explicit:
        return explicit
    if record.get("scope") == _shard_SCOPE_CROSS_CONTROLLER:
        nodes = record.get("node_ids") or ()
        if len(nodes) == 1:
            return nodes[0]
    return ""


def _shard__filter_io_groups(groups, needed_pairs, id_field):
    result = []
    for group in groups or []:
        if not _shard__is_dict(group):
            continue
        controller_uuid = _shard__text(group.get("controller_uuid"))
        rows = []
        for row in group.get("rows") or []:
            if not _shard__is_dict(row):
                continue
            identity = (controller_uuid, _shard__id_key(row.get(id_field)))
            if identity in needed_pairs:
                rows.append(_shard__copy(row))
        if rows:
            copied = {
                key: _shard__copy(value)
                for key, value in group.items()
                if key in (
                    "controller_key",
                    "controller_uuid",
                    "enabled",
                    "local_controller_id",
                    "node_id",
                )
            }
            copied["rows"] = [
                {
                    key: _shard__copy(value)
                    for key, value in row.items()
                    if key not in (
                        "address",
                        "bus_type",
                        "function_type",
                        "unit",
                    )
                }
                for row in rows
            ]
            result.append(copied)
    return result


def _shard__cross_controller_stubs(rows, needed_uuids, owners):
    result = []
    for row in rows or []:
        if not _shard__is_dict(row):
            continue
        controller_uuid = _shard__text(row.get("controller_uuid"))
        if controller_uuid not in needed_uuids:
            continue
        result.append({
            "controller_id": row.get("controller_id"),
            "controller_uuid": controller_uuid,
            "controller_key": row.get("controller_key"),
            "owner_node_id": owners.get(controller_uuid),
        })
    return result


def _shard_build_cross_execution_shard(source, owner_node_id):
    """Build an isolated virtual-controller shard for one owner worker.

    Critical cross rules are intentionally absent.  Call
    :func:`validate_scope_partition` on the authoritative source before a
    rollout; a critical cross rule makes that validation fail closed.
    """

    config = _shard__source_config(source)
    normalized_owner = _shard__text(owner_node_id)
    result = _shard__copy_catalog(config)
    result["meta"] = _shard__with_meta(
        config,
        "cross",
        owner_node_id=normalized_owner,
    )
    analysis = _shard_classify_config_scopes(config)
    rules_by_key = {
        _shard__id_key(row.get("rule_id")): row
        for row in config.get("rules") or []
        if _shard__is_dict(row) and _shard__id_key(row.get("rule_id"))
    }
    selected_rule_keys = set()
    for key, record in analysis["rules"].items():
        rule = rules_by_key.get(key)
        if rule is None or record["scope"] not in (
            _shard_SCOPE_CROSS_CONTROLLER,
            _shard_SCOPE_CROSS_WORKER,
        ):
            continue
        if _shard__rule_owner(rule, record) != normalized_owner:
            continue
        eligibility = _shard_cross_rule_eligibility(config, rule)
        if not eligibility["ok"]:
            continue
        selected_rule_keys.add(key)

    selected_rules = [
        _shard__copy(row)
        for row in config.get("rules") or []
        if _shard__is_dict(row) and _shard__id_key(row.get("rule_id")) in selected_rule_keys
    ]
    selected_references = _shard__new_references()
    for key in selected_rule_keys:
        references = _shard__rule_references(config, rules_by_key[key])
        for name in selected_references:
            selected_references[name].update(references[name])

    selected_states = []
    for row in config.get("process_states") or []:
        if not _shard__is_dict(row):
            continue
        if _shard__id_key(row.get("state_id")) not in selected_references["states"]:
            continue
        selected_states.append(_shard__copy(row))
        _shard__collect_references(row.get("io_refs") or {}, selected_references)

    selected_process_keys = {
        _shard__id_key(row.get("p_id"))
        for row in selected_states
        if _shard__id_key(row.get("p_id"))
    }
    selected_processes = [
        _shard__copy(row)
        for row in config.get("processes") or []
        if _shard__is_dict(row)
        and _shard__id_key(row.get("p_id")) in selected_process_keys
    ]
    for process in selected_processes:
        process["owner_node_id"] = normalized_owner
    filters = _shard__filter_index(config)
    selected_filters = []
    for key in sorted(selected_references["filters"]):
        row = filters.get(key)
        if row is None:
            continue
        selected_filters.append(_shard__copy(row))
        _shard__collect_references(row.get("sensor_refs") or [], selected_references)

    needed_uuids = set(selected_references["controllers"])
    owners = analysis["controller_owners"]
    needed_nodes = {normalized_owner}
    for controller_uuid in needed_uuids:
        if owners.get(controller_uuid):
            needed_nodes.add(owners[controller_uuid])

    result["rules"] = selected_rules
    result["process_states"] = selected_states
    result["processes"] = selected_processes
    result["filters"] = selected_filters
    result["nodes"] = [
        _shard__copy(row)
        for row in config.get("nodes") or []
        if _shard__is_dict(row) and _shard__text(row.get("node_id")) in needed_nodes
    ]
    result["site_controller_map"] = [
        _shard__copy(row)
        for row in config.get("site_controller_map") or []
        if _shard__is_dict(row)
        and row.get("enabled") is not False
        and _shard__text(row.get("controller_uuid")) in needed_uuids
    ]
    result["controllers"] = _shard__cross_controller_stubs(
        config.get("controllers"),
        needed_uuids,
        owners,
    )
    result["virtual_controllers"] = []
    result["local_node"] = []
    result["sensors"] = _shard__filter_io_groups(
        config.get("sensors"),
        selected_references["sensors"],
        "sensor_id",
    )
    result["actuators"] = _shard__filter_io_groups(
        config.get("actuators"),
        selected_references["actuators"],
        "actuator_id",
    )
    return type_authority.project_cross_v8_for_storage(result)


# =============================================================================
# SCOPE AND SHARD VALIDATION
# =============================================================================

def _shard__duplicate_ids(rows, id_field):
    seen = set()
    duplicates = set()
    for row in rows or []:
        if not _shard__is_dict(row):
            continue
        key = _shard__id_key(row.get(id_field))
        if not key:
            continue
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    return tuple(sorted(duplicates))


def _shard_validate_scope_partition(source):
    """Validate disjoint graph scopes and the no-cross-safety invariant."""

    config = _shard__source_config(source)
    report = _shard__new_report()
    ownership = _shard_validate_controller_ownership(config)
    for message in ownership["errors"]:
        _shard__error(report, message)

    for section, id_field in (("process_states", "state_id"), ("rules", "rule_id")):
        for duplicate in _shard__duplicate_ids(config.get(section), id_field):
            _shard__error(report, "duplicate {} {}".format(id_field, duplicate))

    analysis = _shard_classify_config_scopes(config)
    enabled_nodes = {
        _shard__text(row.get("node_id"))
        for row in config.get("nodes") or []
        if _shard__is_dict(row) and row.get("enabled") is not False
    }
    for entity_name in ("states", "rules"):
        identities = set()
        for key, record in analysis[entity_name].items():
            if key in identities:
                _shard__error(report, "{} {} belongs to multiple scopes".format(entity_name, key))
            identities.add(key)
            if record["scope"] not in _shard_EXECUTION_SCOPES:
                _shard__error(
                    report,
                    "{} {} has non-executable scope {}".format(
                        entity_name,
                        key,
                        record["scope"],
                    ),
                )

    rule_index = {
        _shard__id_key(row.get("rule_id")): row
        for row in config.get("rules") or []
        if _shard__is_dict(row)
    }
    for key, record in analysis["rules"].items():
        rule = rule_index.get(key) or {}
        if record["scope"] in (_shard_SCOPE_CROSS_CONTROLLER, _shard_SCOPE_CROSS_WORKER):
            if _shard_is_critical_rule(rule):
                _shard__error(
                    report,
                    "critical rule {} may not execute in {} scope".format(
                        key,
                        record["scope"],
                    ),
                )
            else:
                eligibility = _shard_cross_rule_eligibility(config, rule)
                invalid_targets = eligibility["invalid_target_state_ids"]
                if invalid_targets:
                    _shard__error(
                        report,
                        "cross rule {} mutates non-cross-owned states {}; migration required".format(
                            key,
                            ",".join(str(item) for item in invalid_targets),
                        ),
                    )
                elif not eligibility["ok"]:
                    _shard__error(
                        report,
                        "cross rule {} is not executable: {}".format(
                            key,
                            "; ".join(eligibility["errors"]),
                        ),
                    )
            owner = _shard__rule_owner(rule, record)
            if not owner:
                _shard__error(report, "cross rule {} has no owner_node_id".format(key))
            elif enabled_nodes and owner not in enabled_nodes:
                _shard__error(
                    report,
                    "cross rule {} owner {} is not an enabled node".format(
                        key,
                        owner,
                    ),
                )
        explicit_owner = _shard__text(rule.get("owner_node_id"))
        if record["scope"] == _shard_SCOPE_LOCAL and explicit_owner:
            nodes = record.get("node_ids") or ()
            if nodes and explicit_owner != nodes[0]:
                _shard__error(
                    report,
                    "local rule {} owner {} differs from graph owner {}".format(
                        key,
                        explicit_owner,
                        nodes[0],
                    ),
                )
        if record["scope"] == _shard_SCOPE_CROSS_CONTROLLER and explicit_owner:
            nodes = record.get("node_ids") or ()
            if nodes and explicit_owner != nodes[0]:
                _shard__error(
                    report,
                    "cross-controller rule {} owner {} differs from controller node {}".format(
                        key,
                        explicit_owner,
                        nodes[0],
                    ),
                )

    report["stats"] = {
        "state_scopes": _shard__copy(analysis["state_scopes"]),
        "rule_scopes": _shard__copy(analysis["rule_scopes"]),
    }
    return _shard__finish_report(report)


def _shard__validate_meta(report, config, scope, node_id="", owner_node_id=""):
    rows = config.get("meta") or []
    if not isinstance(rows, list) or len(rows) != 1 or not _shard__is_dict(rows[0]):
        _shard__error(report, "meta must contain exactly one object")
        return
    row = rows[0]
    if _shard__text(row.get("scope")) != scope:
        _shard__error(report, "meta.scope must be {}".format(scope))
    if node_id and _shard__text(row.get("node_id")) != node_id:
        _shard__error(report, "meta.node_id must be {}".format(node_id))
    if owner_node_id and _shard__text(row.get("owner_node_id")) != owner_node_id:
        _shard__error(report, "meta.owner_node_id must be {}".format(owner_node_id))


def _shard_validate_local_execution_shard(source, node_id=None):
    """Validate that a shard contains only one node's physical/local scope."""

    config = _shard__source_config(source)
    normalized_node = _shard__text(node_id or _shard__meta_row(config).get("node_id"))
    report = _shard__new_report()
    if not normalized_node:
        _shard__error(report, "local shard node_id is required")
    _shard__validate_meta(report, config, "node_local", node_id=normalized_node)

    for section in ("nodes", "local_node", "site_controller_map", "sensors", "actuators"):
        for row in config.get(section) or []:
            if not _shard__is_dict(row):
                continue
            row_node = _shard__text(row.get("node_id"))
            if row_node and row_node != normalized_node:
                _shard__error(
                    report,
                    "{} contains foreign node {}".format(section, row_node),
                )
    for route in config.get("site_controller_map") or []:
        if _shard__is_dict(route) and _shard__text(route.get("routing_role")) != "owner":
            _shard__error(report, "local shard route is not routing_role=owner")

    ownership = _shard_validate_controller_ownership(config)
    for message in ownership["errors"]:
        _shard__error(report, message)
    analysis = _shard_classify_config_scopes(config)
    for key, record in analysis["states"].items():
        if record["scope"] != _shard_SCOPE_LOCAL or record["node_ids"] != (normalized_node,):
            _shard__error(report, "local shard state {} is {}".format(key, record["scope"]))
    for key, record in analysis["rules"].items():
        if record["scope"] != _shard_SCOPE_LOCAL or record["node_ids"] != (normalized_node,):
            _shard__error(report, "local shard rule {} is {}".format(key, record["scope"]))
    for row in config.get("rules") or []:
        if not _shard__is_dict(row):
            continue
        owner = _shard__text(row.get("owner_node_id"))
        if owner and owner != normalized_node:
            _shard__error(
                report,
                "local shard rule {} has foreign owner {}".format(
                    _shard__id_key(row.get("rule_id")),
                    owner,
                ),
            )
    report["stats"] = {
        "node_id": normalized_node,
        "controller_count": len(config.get("controllers") or []),
        "state_count": len(config.get("process_states") or []),
        "rule_count": len(config.get("rules") or []),
    }
    return _shard__finish_report(report)


def _shard_validate_cross_execution_shard(source, owner_node_id=None):
    """Validate an isolated cross shard and its single execution owner."""

    supplied = source.get("sections") \
        if _shard__is_dict(source) and _shard__is_dict(source.get("sections")) else source
    representation = (
        "runtime"
        if type_authority.get_config_io_layout(supplied) == "grouped"
        else "storage"
    )
    type_contract = type_authority.inspect_cross_v8_contract(
        supplied, representation=representation)
    config = _shard__source_config(source)
    normalized_owner = _shard__text(
        owner_node_id or _shard__meta_row(config).get("owner_node_id")
    )
    report = _shard__new_report()
    for row in type_contract.get("errors") or ():
        _shard__error(report, "{}: {}".format(
            row.get("path") or "Cross storage",
            row.get("error") or "invalid"))
    if not normalized_owner:
        _shard__error(report, "cross shard owner_node_id is required")
    _shard__validate_meta(
        report,
        config,
        "cross",
        owner_node_id=normalized_owner,
    )
    meta = _shard__meta_row(config)
    if type_authority.get_config_version(config) \
            != type_authority.CROSS_VERSION:
        _shard__error(report, "cross shard meta.version must be 8")
    if not _shard__text(meta.get("domain_id") or meta.get("cross_domain_id")):
        _shard__error(report, "cross shard domain identity is required")
    if config.get("local_node"):
        _shard__error(report, "cross shard may not contain local_node runtime ownership")
    enabled_nodes = {
        _shard__text(row.get("node_id"))
        for row in config.get("nodes") or []
        if _shard__is_dict(row) and row.get("enabled") is not False
    }
    if enabled_nodes and normalized_owner not in enabled_nodes:
        _shard__error(report, "cross shard owner is not an enabled node")
    forbidden_controller_fields = {
        "bus_type",
        "host",
        "port",
        "unit",
    }
    for row in config.get("controllers") or []:
        if not _shard__is_dict(row):
            continue
        leaked = forbidden_controller_fields.intersection(row)
        if leaked:
            _shard__error(
                report,
                "cross controller stub contains physical fields {}".format(
                    ",".join(sorted(leaked))
                ),
            )
    forbidden_io_fields = {
        "address",
        "bus_type",
        "function_type",
        "unit",
    }
    for section in ("sensors", "actuators"):
        for group in config.get(section) or []:
            if not _shard__is_dict(group):
                continue
            for row in group.get("rows") or []:
                leaked = forbidden_io_fields.intersection(row) if _shard__is_dict(row) else set()
                if leaked:
                    _shard__error(
                        report,
                        "cross {} catalog contains physical fields {}".format(
                            section,
                            ",".join(sorted(leaked)),
                        ),
                    )
    for row in config.get("virtual_controllers") or []:
        if _shard__is_dict(row) and row.get("virt_active") is True:
            _shard__error(report, "cross shard may not activate a hardware virtual controller")

    ownership = _shard_validate_controller_ownership(config)
    for message in ownership["errors"]:
        _shard__error(report, message)
    analysis = _shard_classify_config_scopes(config)
    for key, record in analysis["rules"].items():
        if record["scope"] not in (_shard_SCOPE_CROSS_CONTROLLER, _shard_SCOPE_CROSS_WORKER):
            _shard__error(report, "cross shard rule {} is {}".format(key, record["scope"]))
    for row in config.get("rules") or []:
        if not _shard__is_dict(row):
            continue
        rule_id = _shard__id_key(row.get("rule_id"))
        record = analysis["rules"].get(rule_id) or {}
        if _shard__rule_owner(row, record) != normalized_owner:
            _shard__error(report, "cross shard rule {} has wrong owner".format(rule_id))
        if _shard_is_critical_rule(row):
            _shard__error(report, "critical rule {} may not exist in cross shard".format(rule_id))
        eligibility = _shard_cross_rule_eligibility(config, row)
        if not eligibility["ok"]:
            _shard__error(
                report,
                "cross shard rule {} is not writer-safe: {}".format(
                    rule_id,
                    "; ".join(eligibility["errors"]),
                ),
            )
    report["stats"] = {
        "owner_node_id": normalized_owner,
        "controller_count": len(config.get("controllers") or []),
        "state_count": len(config.get("process_states") or []),
        "rule_count": len(config.get("rules") or []),
    }
    return _shard__finish_report(report)


def _shard_validate_split_shards(source, local_shard, cross_shard):
    """Validate that local and cross rule ownership is disjoint and complete."""

    config = _shard__source_config(source)
    report = _shard__new_report()
    local_rules = {
        _shard__id_key(row.get("rule_id"))
        for row in _shard__source_config(local_shard).get("rules") or []
        if _shard__is_dict(row)
    }
    cross_rules = {
        _shard__id_key(row.get("rule_id"))
        for row in _shard__source_config(cross_shard).get("rules") or []
        if _shard__is_dict(row)
    }
    overlap = local_rules.intersection(cross_rules)
    if overlap:
        _shard__error(report, "local/cross rule overlap: {}".format(",".join(sorted(overlap))))

    analysis = _shard_classify_config_scopes(config)
    eligible = {
        key
        for key, record in analysis["rules"].items()
        if record["scope"] in _shard_EXECUTION_SCOPES
    }
    missing = eligible.difference(local_rules.union(cross_rules))
    if missing:
        _shard__error(report, "rules missing from execution shards: {}".format(",".join(sorted(missing))))
    report["stats"] = {
        "local_rule_count": len(local_rules),
        "cross_rule_count": len(cross_rules),
        "overlap_count": len(overlap),
        "missing_rule_count": len(missing),
    }
    return _shard__finish_report(report)


_graph_LAB_ACTIVE_RULE_IDS = (1, 2, 3, 4, 6, 10, 11)
_graph_MIGRATION_REQUIRED_RULE_IDS = (5, 7, 8, 9, 13, 15, 17)
_graph_ACTIVE_STATUSES = ("active_node", "active_cross", "replaced_local", "monitor_only")


def _graph__rule_id(value):
    try:
        return int(value)
    except Exception:
        return -1


def _graph__owner_for_record(record, rule=None):
    declared = None
    if isinstance(rule, dict):
        declared = rule.get("execution_owner") or rule.get("owner_node_id")
    owners = tuple(record.get("node_ids") or ())
    if declared is not None and str(declared) in owners:
        return str(declared)
    return owners[0] if len(owners) == 1 else None


def _graph__migration_status(rule_id, record):
    if rule_id in _graph_MIGRATION_REQUIRED_RULE_IDS:
        return "disabled_with_reason"
    if record.get("scope") == SCOPE_LOCAL:
        return "active_node"
    if record.get("scope") in (
        SCOPE_CROSS_CONTROLLER,
        SCOPE_CROSS_WORKER,
    ):
        return "active_cross"
    return "disabled_with_reason"


def _graph_migrate_legacy_config(config, **kwargs):
    """Explicitly import V5, then annotate rules without inventing semantics."""
    profile = str(kwargs.get("profile") or "lab_v2")
    version = type_authority.get_config_version(config)
    try:
        candidate = type_authority.import_legacy_v5_config(config) \
            if version == type_authority.LEGACY_VERSION \
            else type_authority.ensure_v9_storage_config(config)
    except ValueError as exc:
        return {
            "ok": False,
            "config": copy.deepcopy(config),
            "report": {"profile": profile, "errors": (str(exc),)},
        }
    classified = classify_config_scopes(candidate)
    reports = []
    for rule in candidate.get("rules") or []:
        rule_id = _graph__rule_id(rule.get("rule_id"))
        record = classified.get("rules", {}).get(str(rule_id), {})
        status = _graph__migration_status(rule_id, record)
        owner = _graph__owner_for_record(record, rule)
        rule["deployment_status"] = status
        if owner is None:
            rule.pop("owner_node_id", None)
        else:
            rule["owner_node_id"] = owner
        if status == "active_node":
            rule["execution_scope"] = "node:" + str(owner)
            rule["execution_owner"] = owner
            rule["execution_epoch"] = 1
            rule["activation_state"] = "active"
            rule.pop("disabled_reason", None)
        elif status == "active_cross":
            domain = str(owner) + ".default"
            rule["cross_domain_id"] = domain
            rule["execution_scope"] = "cross:" + domain
            rule["execution_owner"] = owner
            rule["execution_epoch"] = 1
            rule["activation_state"] = "active"
            rule.pop("disabled_reason", None)
        else:
            rule["enabled"] = False
            rule["disabled_reason"] = "migration_required"
            rule["execution_scope"] = None
            rule["execution_owner"] = None
            rule["execution_epoch"] = 0
            rule["activation_state"] = "inactive"
        reports.append({
            "rule_id": rule_id,
            "source_scope": record.get("scope"),
            "controller_uuids": list(record.get("controller_uuids") or ()),
            "owner_node_ids": list(record.get("node_ids") or ()),
            "deployment_status": status,
            "reason": "migration_required" if status == "disabled_with_reason" else "verified_graph_projection",
        })
    safety_blocked = sorted(
        _graph__rule_id(rule.get("rule_id"))
        for rule in candidate.get("rules") or []
        if rule.get("deployment_status") == "disabled_with_reason"
        and str(rule.get("group") or "").lower() in CRITICAL_GROUPS
    )
    return {
        "ok": not (profile == "production_v2" and safety_blocked),
        "config": candidate,
        "report": {
            "profile": profile,
            "source_version": version,
            "target_version": type_authority.CANONICAL_VERSION,
            "rules": sorted(reports, key=_graph_rule_report_key),
            "active_rule_ids": sorted(
                item["rule_id"] for item in reports
                if item["deployment_status"] in ("active_node", "active_cross")
            ),
            "migration_required_rule_ids": sorted(
                item["rule_id"] for item in reports
                if item["deployment_status"] == "disabled_with_reason"
            ),
            "production_safety_blockers": safety_blocked,
        },
    }


def _graph_rule_report_key(item):
    """Sort migration rows by numeric rule identity."""
    return int(item.get("rule_id", -1))


def _graph_active_projection(config):
    """Return only explicitly executable rules for graph materialization."""
    result = copy.deepcopy(config)
    result["rules"] = [
        rule for rule in result.get("rules") or []
        if rule.get("deployment_status") in _graph_ACTIVE_STATUSES
        or "deployment_status" not in rule
    ]
    return result


def _graph_validate_authority_config(config, **kwargs):
    """Validate schema, registry roundtrip, graph safety and release profile."""
    profile = str(kwargs.get("profile") or "lab_v2")
    type_report = type_authority.verify_config_data(config)
    try:
        registry_report = verify_roundtrip(config)
    except Exception as exc:
        registry_report = {"ok": False, "error": str(exc)}
    projected = _graph_active_projection(config)
    ownership_report = validate_controller_ownership(projected)
    scope_report = validate_scope_partition(projected)
    missing_status = sorted(
        _graph__rule_id(rule.get("rule_id")) for rule in config.get("rules") or []
        if rule.get("deployment_status") not in (
            "active_node", "active_cross", "replaced_local", "monitor_only",
            "disabled_with_reason",
        )
    )
    disabled_without_reason = sorted(
        _graph__rule_id(rule.get("rule_id")) for rule in config.get("rules") or []
        if rule.get("deployment_status") == "disabled_with_reason"
        and not str(rule.get("disabled_reason") or "").strip()
    )
    production_safety = sorted(
        _graph__rule_id(rule.get("rule_id")) for rule in config.get("rules") or []
        if profile == "production_v2"
        and rule.get("deployment_status") == "disabled_with_reason"
        and str(rule.get("group") or "").lower() in CRITICAL_GROUPS
    )
    errors = []
    if type_report.get("status") != "OK":
        errors.append("generic type verification failed")
    if not registry_report.get("ok"):
        errors.append("entity registry roundtrip failed")
    errors.extend(ownership_report.get("errors") or ())
    errors.extend(scope_report.get("errors") or ())
    if missing_status:
        errors.append("rules have invalid deployment status: " + ",".join(map(str, missing_status)))
    if disabled_without_reason:
        errors.append("disabled rules lack a reason: " + ",".join(map(str, disabled_without_reason)))
    if production_safety:
        errors.append("production safety migration is incomplete: " + ",".join(map(str, production_safety)))
    return {
        "ok": not errors,
        "errors": tuple(sorted(set(errors))),
        "type_report": type_report,
        "registry_report": registry_report,
        "ownership_report": ownership_report,
        "scope_report": scope_report,
    }


def _graph_build_graph(config):
    """Expose the extracted v0082 transitive graph as the single authority."""
    return classify_config_scopes(_graph_active_projection(config))

# =============================================================================
# PUBLIC DOMAIN API
# =============================================================================

entity_key = _registry_entity_key
decompose = _registry_decompose
group_position_key = _registry_group_position_key
row_position_key = _registry_row_position_key
record_position_key = _registry_record_position_key
materialize = _registry_materialize
verify_roundtrip = _registry_verify_roundtrip
REGISTRY_VERSION = _registry_REGISTRY_VERSION
SECTION_ORDER = _registry_SECTION_ORDER
KEY_FIELDS = _registry_KEY_FIELDS
GROUPED_SECTIONS = _registry_GROUPED_SECTIONS
identity_key = _scope_identity_key
diff_configs = _scope_diff_configs
apply_operations = _scope_apply_operations
MAX_PATCH_OPERATIONS = _scope_MAX_PATCH_OPERATIONS
canonical_fingerprint = _shard_canonical_fingerprint
controller_owner_map = _shard_controller_owner_map
validate_controller_ownership = _shard_validate_controller_ownership
cross_rule_eligibility = _shard_cross_rule_eligibility
classify_config_scopes = _shard_classify_config_scopes
is_critical_rule = _shard_is_critical_rule
build_local_execution_shard = _shard_build_local_execution_shard
build_cross_execution_shard = _shard_build_cross_execution_shard
validate_scope_partition = _shard_validate_scope_partition
validate_local_execution_shard = _shard_validate_local_execution_shard
validate_cross_execution_shard = _shard_validate_cross_execution_shard
validate_split_shards = _shard_validate_split_shards
SCOPE_LOCAL = _shard_SCOPE_LOCAL
SCOPE_CROSS_CONTROLLER = _shard_SCOPE_CROSS_CONTROLLER
SCOPE_CROSS_WORKER = _shard_SCOPE_CROSS_WORKER
SCOPE_UNBOUND = _shard_SCOPE_UNBOUND
SCOPE_INVALID = _shard_SCOPE_INVALID
EXECUTION_SCOPES = _shard_EXECUTION_SCOPES
CRITICAL_GROUPS = _shard_CRITICAL_GROUPS
RUNTIME_SECTIONS = _shard_RUNTIME_SECTIONS
migrate_legacy_config = _graph_migrate_legacy_config
rule_report_key = _graph_rule_report_key
active_projection = _graph_active_projection
validate_authority_config = _graph_validate_authority_config
build_graph = _graph_build_graph
LAB_ACTIVE_RULE_IDS = _graph_LAB_ACTIVE_RULE_IDS
MIGRATION_REQUIRED_RULE_IDS = _graph_MIGRATION_REQUIRED_RULE_IDS
ACTIVE_STATUSES = _graph_ACTIVE_STATUSES

CANONICAL_VERSION = type_authority.CANONICAL_VERSION
CROSS_VERSION = type_authority.CROSS_VERSION
LEGACY_VERSION = type_authority.LEGACY_VERSION
RUNTIME_IO_LAYOUT = type_authority.RUNTIME_IO_LAYOUT
IO_SECTIONS = type_authority.IO_SECTIONS
get_config_version = type_authority.get_config_version
get_config_scope = type_authority.get_config_scope
get_io_layout = type_authority.get_io_layout
get_config_io_layout = type_authority.get_config_io_layout
inspect_config_contract = type_authority.inspect_config_contract
inspect_cross_v8_contract = type_authority.inspect_cross_v8_contract
normalize_config_for_runtime = type_authority.normalize_config_for_runtime
project_config_for_storage = type_authority.project_config_for_storage
normalize_cross_v8_for_runtime = type_authority.normalize_cross_v8_for_runtime
project_cross_v8_for_storage = type_authority.project_cross_v8_for_storage
import_legacy_v5_config = type_authority.import_legacy_v5_config
canonicalize_config_for_fingerprint = type_authority.canonicalize_config_for_fingerprint
ensure_v9_storage_config = type_authority.ensure_v9_storage_config
ensure_v8_cross_storage_config = type_authority.ensure_v8_cross_storage_config
resolve_io_entity_path = type_authority.resolve_io_entity_path
resolve_entity_path = type_authority.resolve_entity_path
normalize_patch_selector = type_authority.normalize_patch_selector



def _require_operation_identity(operation):
    section = str(operation.get("section") or "").strip()
    entity_key = str(operation.get("entity_key") or "").strip()
    if not section or not entity_key:
        raise ValueError("operation requires section and entity_key")
    return section, entity_key


def validate_scope_operations(operations, **kwargs):
    """Validate the only distributed config operation format fail-closed."""
    operation_format = str(kwargs.get("operation_format") or OPERATION_FORMAT)
    if operation_format != OPERATION_FORMAT:
        raise ValueError("unsupported_operation_format")
    maximum = min(MAX_PATCH_OPERATIONS, max(
        1, int(kwargs.get("maximum", MAX_PATCH_OPERATIONS))))
    if (
            not isinstance(operations, (list, tuple))
            or not operations
            or len(operations) > maximum):
        raise ValueError("unsupported_operation_format")
    common_fields = {
        "op", "section", "entity_key", "expected_value_hash",
    }
    operation_fields = {
        "replace_field": common_fields.union(("json_pointer", "value")),
        "upsert_entity": common_fields.union(("value", "value_hash")),
        "delete_entity": common_fields,
    }
    required_fields = {
        "replace_field": {"op", "section", "entity_key", "json_pointer", "value"},
        "upsert_entity": {
            "op", "section", "entity_key", "value", "value_hash",
        },
        "delete_entity": {"op", "section", "entity_key"},
    }
    normalized = []
    for operation in operations:
        if not isinstance(operation, dict):
            raise ValueError("unsupported_operation_format")
        kind = str(operation.get("op") or "")
        if kind not in ALLOWED_OPERATION_KINDS:
            raise ValueError("unsupported_operation_format")
        if set(operation) - operation_fields[kind]:
            raise ValueError("unsupported_operation_format")
        if required_fields[kind] - set(operation):
            raise ValueError("unsupported_operation_format")
        _require_operation_identity(operation)
        expected = operation.get("expected_value_hash")
        value_hash = operation.get("value_hash")
        if expected is not None and not type_authority.is_fingerprint(expected):
            raise ValueError("expected_value_hash must be lowercase raw 64-hex")
        if value_hash is not None and not type_authority.is_fingerprint(value_hash):
            raise ValueError("value_hash must be lowercase raw 64-hex")
        if kind == "replace_field" and not str(
                operation.get("json_pointer") or "").startswith("/"):
            raise ValueError("replace_field requires a non-root JSON pointer")
        if kind == "upsert_entity":
            if not isinstance(operation.get("value"), dict):
                raise ValueError("upsert_entity requires an entity record")
            if (
                    value_hash is None
                    or type_authority.fingerprint(operation["value"]) != value_hash):
                raise ValueError("upsert_entity value_hash differs")
        normalized.append(copy.deepcopy(operation))
    return tuple(normalized)


def _legacy_path_tokens(path):
    if isinstance(path, (list, tuple)):
        return tuple(path)
    text = str(path or "").strip()
    if not text:
        raise ValueError("legacy path is empty")
    tokens = []
    for part in text.replace("[", ".").replace("]", "").split("."):
        if not part:
            continue
        tokens.append(int(part) if part.lstrip("+-").isdigit() else part)
    return tuple(tokens)


def _legacy_path_parent(candidate, tokens):
    current = candidate
    for token in tokens[:-1]:
        current = current[token]
    return current, tokens[-1]


def normalize_legacy_ingress(snapshot, request, **kwargs):
    """Normalize legacy full/path input once against one immutable snapshot."""
    if not isinstance(snapshot, dict) or not isinstance(request, dict):
        raise ValueError("unsupported_operation_format")
    operation_format = request.get("operation_format")
    if operation_format is not None:
        return validate_scope_operations(
            request.get("operations"),
            operation_format=operation_format,
            maximum=kwargs.get("maximum", MAX_PATCH_OPERATIONS),
        )
    if isinstance(request.get("config"), dict):
        return validate_scope_operations(diff_configs(snapshot, request["config"], **kwargs))
    changes = request.get("changes")
    if not isinstance(changes, list):
        raise ValueError("unsupported_operation_format")
    candidate = copy.deepcopy(snapshot)
    for change in changes:
        if not isinstance(change, dict):
            raise ValueError("unsupported_operation_format")
        resolved = type_authority.normalize_patch_selector(
            snapshot,
            change.get("selector", change.get("path")),
            external=True,
            operation_kind=change.get("operation") or change.get("op"),
        )
        tokens = _legacy_path_tokens(resolved)
        parent, key = _legacy_path_parent(candidate, tokens)
        operation = str(change.get("operation") or change.get("op") or "set")
        if operation in ("set", "replace", "update"):
            parent[key] = copy.deepcopy(change.get("value"))
        elif operation in ("delete", "remove"):
            if isinstance(parent, list):
                del parent[int(key)]
            else:
                del parent[key]
        else:
            raise ValueError("unsupported_operation_format")
    return validate_scope_operations(diff_configs(snapshot, candidate, **kwargs))





