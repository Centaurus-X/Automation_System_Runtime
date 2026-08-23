# -*- coding: utf-8 -*-

# src/library/_generic_type_library.py


"""
 =============================================================================
 _GENERIC_TYPE_LIBRARY
 =============================================================================

 GENERIC TYPE LIBRARY - Deterministic JSON Config Type & Field Verification

 WHAT IT IS:
 A pure functional, generic and dynamic type/field verification library
 for the device_state_norm JSON configuration contracts V5, V8 and V9.
 It provides declarative type specifications, a recursive generic validator,
 referential integrity verification, canonical deterministic serialization,
 fingerprinting and safe load/save round-trips for heap, file, database and
 transport contexts.

 WHY IT IS NEEDED:
 Configuration is the source of truth selected by the current authority mode.
 Every Controller, Rule Engine, MQTT bridge, device adapter and persistence
 boundary must consume structurally identical and verified data. This module
 guarantees correct types, fields and cross-references plus byte-stable
 canonical form on every read and every write.

 HOW IT WORKS:
 1. Type specifications are plain dictionaries built by constructor functions.
 2. validate_value() recursively validates values and reports precise paths.
 3. CONTEXT holds the central schema and reference-check definitions.
 4. verify_references() builds indexes and validates every cross-reference.
 5. canonical_dumps() and fingerprint() produce deterministic output.
 6. load_config_file() and save_config_file() enforce verified round-trips.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import json
import hashlib
import logging
import copy
import os
import tempfile
import datetime
import decimal
import uuid
import re
from functools import partial


# =============================================================================
# CONFIG NORMALIZATION - CONSOLIDATED TYPE AUTHORITY
# =============================================================================

_compat_CONTEXT = {
    "library_name": "config_compatibility",
    "library_version": "2.0.0",
    "runtime_io_layout": "grouped",
    "storage_layout_by_version": {
        5: "grouped",
        8: "flat",
        9: "flat",
    },
}

_compat_CONTEXT["supported_versions"] = tuple(
    sorted(_compat_CONTEXT["storage_layout_by_version"].keys())
)
_compat_CONTEXT["flat_versions"] = tuple(
    version
    for version in _compat_CONTEXT["supported_versions"]
    if _compat_CONTEXT["storage_layout_by_version"][version] == "flat"
)

_compat_IO_SECTIONS = {
    "sensors": {
        "id_field": "sensor_id",
        "group_field": "sensor_group",
    },
    "actuators": {
        "id_field": "actuator_id",
        "group_field": "actuator_group",
    },
}

_compat_ENTITY_SECTIONS = {
    "controllers": {"id_field": "controller_uuid", "update_kind": "controller_update"},
    "rules": {"id_field": "rule_id", "update_kind": "field_update"},
    "processes": {"id_field": "p_id", "update_kind": "field_update"},
    "process_states": {"id_field": "state_id", "update_kind": "field_update"},
}

_compat_OWNERSHIP_FIELDS = {
    "execution_owner",
    "execution_scope",
    "owner_node_id",
}

_compat_ENTITY_PATH_PATTERN = re.compile(
    r"^(controllers|rules|processes|process_states|sensors|actuators)"
    r"(?:\[(?:[-+]?\d+)\]|[./](?:[-+]?\d+))(?:$|[./\[])",
)

_compat_GROUP_FIELDS = (
    "controller_key",
    "controller_uuid",
    "enabled",
    "local_controller_id",
    "node_id",
    "site_id",
)

_compat_CONTROLLER_SCOPE_FIELDS = {
    "controller_key",
    "enabled",
    "local_controller_id",
    "node_id",
    "site_id",
}

_compat_COMMON_IDENTITY_FIELDS = {
    "controller_uuid",
    "rows",
}


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _compat__is_dict(value):
    return isinstance(value, dict)


def _compat__text(value):
    return str(value or "").strip()


def _compat__meta_row(config):
    if not _compat__is_dict(config):
        return {}
    rows = config.get("meta") or []
    if isinstance(rows, list) and rows and _compat__is_dict(rows[0]):
        return rows[0]
    return {}


def _compat_get_config_version(config):
    value = _compat__meta_row(config).get("version")
    if isinstance(value, bool):
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _compat_get_config_scope(config):
    return _compat__text(_compat__meta_row(config).get("scope")).lower()


def _compat_get_expected_storage_layout(config):
    version = _compat_get_config_version(config)
    return _compat_CONTEXT["storage_layout_by_version"].get(version)


def _compat__section_layout(rows):
    if rows is None:
        return "empty"
    if not isinstance(rows, list):
        return "invalid"
    if not rows:
        return "empty"
    grouped = 0
    flat = 0
    invalid = 0
    for row in rows:
        if not _compat__is_dict(row):
            invalid += 1
            continue
        if "rows" in row:
            grouped += 1
        else:
            flat += 1
    if invalid:
        return "invalid"
    if grouped and flat:
        return "mixed"
    if grouped:
        return "grouped"
    return "flat"


def _compat_get_io_layout(config, section_name):
    if section_name not in _compat_IO_SECTIONS:
        raise ValueError("unsupported I/O section: " + str(section_name))
    rows = config.get(section_name) if _compat__is_dict(config) else None
    return _compat__section_layout(rows)


def _compat_get_config_io_layout(config):
    layouts = {
        section_name: _compat_get_io_layout(config, section_name)
        for section_name in _compat_IO_SECTIONS
    }
    active = {
        layout
        for layout in layouts.values()
        if layout not in ("empty",)
    }
    if "invalid" in active:
        return "invalid"
    if "mixed" in active or len(active) > 1:
        return "mixed"
    if active:
        return next(iter(active))
    expected = _compat_get_expected_storage_layout(config)
    if expected:
        return expected
    return _compat_CONTEXT["runtime_io_layout"]


def _compat__new_contract_report(config, representation):
    return {
        "status": "OK",
        "errors": [],
        "warnings": [],
        "version": _compat_get_config_version(config),
        "scope": _compat_get_config_scope(config),
        "representation": representation,
        "expected_io_layout": None,
        "source_io_layout": _compat_get_config_io_layout(config) if _compat__is_dict(config) else "invalid",
    }


def _compat__contract_error(report, path, message):
    report["errors"].append({"path": str(path), "error": str(message)})
    report["status"] = "FAILED"


def _compat_inspect_config_contract(config, **kwargs):
    """Inspect version/layout compatibility without mutating the document."""
    representation = _compat__text(kwargs.get("representation") or "auto").lower()
    if representation not in ("auto", "storage", "runtime"):
        raise ValueError("unsupported config representation: " + representation)

    report = _compat__new_contract_report(config, representation)
    if not _compat__is_dict(config):
        _compat__contract_error(report, "", "config root must be an object")
        return report

    version = report["version"]
    storage_layout = _compat_CONTEXT["storage_layout_by_version"].get(version)
    if representation == "storage":
        report["expected_io_layout"] = storage_layout
    elif representation == "runtime":
        report["expected_io_layout"] = _compat_CONTEXT["runtime_io_layout"]

    for section_name in _compat_IO_SECTIONS:
        layout = _compat_get_io_layout(config, section_name)
        if layout == "invalid":
            _compat__contract_error(
                report,
                section_name,
                "I/O section contains a non-object row",
            )
        elif layout == "mixed":
            _compat__contract_error(
                report,
                section_name,
                "grouped and flat I/O rows must not be mixed",
            )

    source_layout = report["source_io_layout"]
    if source_layout in ("mixed", "invalid"):
        return report

    expected = report["expected_io_layout"]
    if expected and source_layout != expected:
        _compat__contract_error(
            report,
            "sensors/actuators",
            "{} representation requires {} I/O tables, found {}".format(
                representation,
                expected,
                source_layout,
            ),
        )

    if representation == "storage" and storage_layout is None and version:
        _compat__contract_error(
            report,
            "meta[0].version",
            "no storage layout contract is registered for version " + str(version),
        )
    return report


def _compat__raise_contract_errors(report):
    if report.get("status") == "OK":
        return
    messages = [
        str(row.get("path") or "") + ": " + str(row.get("error") or "")
        for row in report.get("errors") or []
    ]
    raise ValueError("; ".join(messages))


def _compat__group_metadata(row):
    result = {}
    for field in _compat_GROUP_FIELDS:
        if field in row:
            result[field] = copy.deepcopy(row[field])
    return result


def _compat__io_payload(row):
    return {
        field: copy.deepcopy(value)
        for field, value in row.items()
        if field not in _compat_GROUP_FIELDS
    }


def _compat__metadata_conflict(left, right):
    for field in _compat_GROUP_FIELDS:
        if field in left and field in right and left.get(field) != right.get(field):
            return field
    return ""


def _compat__merge_missing_metadata(target, source):
    for field in _compat_GROUP_FIELDS:
        if field not in target and field in source:
            target[field] = copy.deepcopy(source[field])


def _compat__new_group(row):
    result = _compat__group_metadata(row)
    result["rows"] = []
    return result


def _compat__route_groups(config):
    groups = []
    seen = set()
    if _compat_get_config_scope(config) != "node_local":
        return groups
    for route in config.get("site_controller_map") or []:
        if not _compat__is_dict(route):
            continue
        identity = _compat__text(route.get("controller_uuid"))
        if not identity or identity in seen:
            continue
        groups.append(_compat__new_group(route))
        seen.add(identity)
    return groups


def _compat__group_flat_section(config, section_name):
    profile = _compat_IO_SECTIONS[section_name]
    id_field = profile["id_field"]
    groups = _compat__route_groups(config)
    by_uuid = {
        _compat__text(group.get("controller_uuid")): group
        for group in groups
        if _compat__text(group.get("controller_uuid"))
    }
    order = [
        _compat__text(group.get("controller_uuid"))
        for group in groups
        if _compat__text(group.get("controller_uuid"))
    ]

    rows = config.get(section_name) or []
    for index, row in enumerate(rows):
        if not _compat__is_dict(row):
            raise ValueError(
                section_name + "[" + str(index) + "] must be an object"
            )
        identity = _compat__text(row.get("controller_uuid"))
        if not identity:
            raise ValueError(
                section_name + "[" + str(index) + "] requires controller_uuid"
            )
        group = by_uuid.get(identity)
        if group is None:
            group = _compat__new_group(row)
            by_uuid[identity] = group
            order.append(identity)
        else:
            conflict = _compat__metadata_conflict(group, row)
            if conflict:
                raise ValueError(
                    section_name
                    + " contains inconsistent "
                    + conflict
                    + " for controller_uuid "
                    + identity
                )
            _compat__merge_missing_metadata(group, row)
        group["rows"].append(_compat__io_payload(row))

    return [by_uuid[identity] for identity in order]


def _compat__flatten_grouped_section(config, section_name):
    profile = _compat_IO_SECTIONS[section_name]
    id_field = profile["id_field"]
    result = []
    for group in config.get(section_name) or []:
        if not _compat__is_dict(group):
            continue
        metadata = {
            field: copy.deepcopy(value)
            for field, value in group.items()
            if field != "rows"
        }
        for row in group.get("rows") or []:
            if not _compat__is_dict(row):
                continue
            entity = copy.deepcopy(metadata)
            entity.update(copy.deepcopy(row))
            result.append(entity)
    return result


def _compat__identity_fields(section_name):
    profile = _compat_IO_SECTIONS[section_name]
    return set(_compat_COMMON_IDENTITY_FIELDS).union({profile["id_field"]})


def _compat__validate_selector_field(section_name, field):
    field_name = _compat__text(field)
    if not field_name:
        return ""
    if field_name in _compat__identity_fields(section_name):
        raise ValueError(
            "identity field '"
            + field_name
            + "' requires an explicit entity migration"
        )
    if field_name in _compat_CONTROLLER_SCOPE_FIELDS:
        raise ValueError(
            "controller-scope field '"
            + field_name
            + "' requires an atomic controller update"
        )
    return field_name


def _compat__field_root(field):
    text = _compat__text(field)
    if not text:
        return ""
    return re.split(r"[.\[]", text, maxsplit=1)[0]


def _compat__selector_operation(selector, default="field_update"):
    return _compat__text(
        selector.get("operation_kind")
        or selector.get("operation")
        or default
    ).lower()


def _compat__validate_generic_selector_field(section_name, profile, selector):
    field_name = _compat__text(selector.get("field"))
    if not field_name:
        return ""
    root = _compat__field_root(field_name)
    if root == profile["id_field"]:
        raise ValueError(
            "identity field '"
            + root
            + "' requires an explicit identity migration"
        )
    if root in _compat_OWNERSHIP_FIELDS:
        raise ValueError(
            "ownership field '"
            + root
            + "' requires an explicit ownership migration"
        )
    operation = _compat__selector_operation(selector)
    expected = profile.get("update_kind") or "field_update"
    if section_name == "controllers" and operation != expected:
        raise ValueError(
            "controller field updates require operation_kind='controller_update'"
        )
    return field_name


def _compat__find_table_entity_path(runtime, section_name, id_field, identity):
    for index, row in enumerate(runtime.get(section_name) or []):
        if not _compat__is_dict(row):
            continue
        if _compat__text(row.get(id_field)) == _compat__text(identity):
            return "{}[{}]".format(section_name, index)
    return ""


def _compat__find_io_group_path(runtime, section_name, controller_uuid):
    for index, group in enumerate(runtime.get(section_name) or []):
        if not _compat__is_dict(group):
            continue
        if _compat__text(group.get("controller_uuid")) == controller_uuid:
            return "{}[{}].rows".format(section_name, index)
    return ""


def _compat__find_grouped_io_path(runtime, section_name, controller_uuid, id_field, local_id):
    for group_index, group in enumerate(runtime.get(section_name) or []):
        if not _compat__is_dict(group):
            continue
        if _compat__text(group.get("controller_uuid")) != controller_uuid:
            continue
        for row_index, row in enumerate(group.get("rows") or []):
            if not _compat__is_dict(row):
                continue
            if _compat__text(row.get(id_field)) == _compat__text(local_id):
                return "{}[{}].rows[{}]".format(
                    section_name,
                    group_index,
                    row_index,
                )
    return ""


# =============================================================================
# CODEBASE FUNCTIONS
# =============================================================================

def _compat_normalize_config_for_runtime(config, **kwargs):
    """Return the canonical grouped runtime representation."""
    if not _compat__is_dict(config):
        return config
    representation = _compat__text(kwargs.get("representation") or "auto").lower()
    contract = _compat_inspect_config_contract(config, representation=representation)
    _compat__raise_contract_errors(contract)

    layout = _compat_get_config_io_layout(config)
    if layout == _compat_CONTEXT["runtime_io_layout"]:
        return config

    result = copy.deepcopy(config)
    for section_name in _compat_IO_SECTIONS:
        section_layout = _compat_get_io_layout(config, section_name)
        if section_layout == "flat":
            result[section_name] = _compat__group_flat_section(config, section_name)
        elif section_layout == "empty":
            result[section_name] = _compat__route_groups(config)
    return result


def _compat_project_config_for_storage(config, **kwargs):
    """Return the version-defined storage representation."""
    if not _compat__is_dict(config):
        return config
    runtime = _compat_normalize_config_for_runtime(config, representation="auto")
    version = _compat_get_config_version(runtime)
    target_layout = _compat_CONTEXT["storage_layout_by_version"].get(version)
    if target_layout is None:
        return runtime
    if target_layout == _compat_CONTEXT["runtime_io_layout"]:
        return runtime

    result = copy.deepcopy(runtime)
    for section_name in _compat_IO_SECTIONS:
        result[section_name] = _compat__flatten_grouped_section(runtime, section_name)
    if kwargs.get("verify_contract", True):
        contract = _compat_inspect_config_contract(result, representation="storage")
        _compat__raise_contract_errors(contract)
    return result


def _compat_canonicalize_config_for_fingerprint(config):
    """Return one valid version-defined canonical shape for fingerprinting.

    Fingerprints are protocol identities. Invalid or representation-ambiguous
    documents must therefore fail closed instead of receiving a raw fallback
    hash that another node could interpret differently.
    """
    if not _compat__is_dict(config):
        raise ValueError("config fingerprint root must be an object")
    runtime = _compat_normalize_config_for_runtime(config, representation="auto")
    return _compat_project_config_for_storage(runtime)


def _compat_resolve_io_entity_path(config, selector):
    """Resolve one safe entity-row selector to a grouped runtime path."""
    if not _compat__is_dict(selector):
        raise ValueError("I/O entity selector must be an object")

    section_name = _compat__text(selector.get("section"))
    profile = _compat_IO_SECTIONS.get(section_name)
    if profile is None:
        raise ValueError("unsupported I/O entity section: " + section_name)

    id_field = profile["id_field"]
    controller_uuid = _compat__text(selector.get("controller_uuid"))
    local_id = selector.get(id_field)
    if not controller_uuid or local_id is None:
        raise ValueError("selector requires controller_uuid and " + id_field)

    field_name = _compat__validate_selector_field(section_name, selector.get("field"))
    runtime = _compat_normalize_config_for_runtime(config, representation="auto")
    path = _compat__find_grouped_io_path(
        runtime,
        section_name,
        controller_uuid,
        id_field,
        local_id,
    )
    if not path:
        raise ValueError(
            "I/O entity not found: {} {}:{}".format(
                section_name,
                controller_uuid,
                local_id,
            )
        )
    if field_name:
        return path + "." + field_name
    return path


def _compat_resolve_entity_path(config, selector):
    """Resolve a stable Config entity selector against grouped runtime data."""
    if not _compat__is_dict(selector):
        raise ValueError("entity selector must be an object")
    section_name = _compat__text(selector.get("section"))
    operation = _compat__selector_operation(selector)
    runtime = _compat_normalize_config_for_runtime(config, representation="runtime")

    if section_name in _compat_IO_SECTIONS:
        if operation == "entity_add":
            if selector.get("field"):
                raise ValueError("entity_add selector must not contain field")
            controller_uuid = _compat__text(selector.get("controller_uuid"))
            if not controller_uuid:
                raise ValueError("I/O entity_add requires controller_uuid")
            path = _compat__find_io_group_path(runtime, section_name, controller_uuid)
            if not path:
                raise ValueError(
                    "I/O controller group not found: "
                    + section_name
                    + " "
                    + controller_uuid
                )
            return path
        return _compat_resolve_io_entity_path(runtime, selector)

    profile = _compat_ENTITY_SECTIONS.get(section_name)
    if profile is None:
        raise ValueError("unsupported entity section: " + section_name)
    if operation == "entity_add":
        if selector.get("field"):
            raise ValueError("entity_add selector must not contain field")
        return section_name

    id_field = profile["id_field"]
    identity = selector.get(id_field)
    if identity is None or not _compat__text(identity):
        raise ValueError("selector requires " + id_field)
    field_name = _compat__validate_generic_selector_field(
        section_name,
        profile,
        selector,
    )
    path = _compat__find_table_entity_path(
        runtime,
        section_name,
        id_field,
        identity,
    )
    if not path:
        raise ValueError(
            "entity not found: {} {}:{}".format(
                section_name,
                id_field,
                identity,
            )
        )
    if field_name:
        return path + "." + field_name
    return path


def _compat_is_numeric_entity_path(path):
    """Return True for index-addressed entity paths unsafe across projections."""
    if isinstance(path, (list, tuple)):
        if len(path) < 2:
            return False
        section_name = _compat__text(path[0])
        index = path[1]
        return (
            section_name in set(_compat_IO_SECTIONS).union(_compat_ENTITY_SECTIONS)
            and (
                isinstance(index, int)
                or (isinstance(index, str) and index.lstrip("+-").isdigit())
            )
        )
    if path is None:
        return False
    return bool(_compat_ENTITY_PATH_PATTERN.match(str(path).strip()))


def _compat_normalize_patch_selector(config, path, **kwargs):
    """Resolve stable selectors and reject external index-based entity paths."""
    if _compat__is_dict(path):
        selector = copy.deepcopy(path)
        if kwargs.get("operation_kind") and not (
                selector.get("operation_kind") or selector.get("operation")):
            selector["operation_kind"] = kwargs["operation_kind"]
        return _compat_resolve_entity_path(config, selector)
    if kwargs.get("external") and _compat_is_numeric_entity_path(path):
        raise ValueError(
            "external entity patches require a stable identity selector"
        )
    return path


# =============================================================================
# Logger (module level, after safe import)
# =============================================================================

LOGGER = logging.getLogger(__name__)

TYPE_SCHEMA_VERSION = "2.0.0"


# =============================================================================
# CENTRAL MODULE "CONTEXT MANAGEMENT" - definition & initialisation
# =============================================================================

CONTEXT = {
    "library_name": "generic_type_library",
    "library_version": TYPE_SCHEMA_VERSION,
    "supported_schema": "device_state_norm",
    # Scalar compatibility field now advertises the canonical primary
    # contract; legacy import support is enumerated separately below.
    "supported_schema_version": 9,
    "current_schema_version": 9,
    "supported_schema_versions": _compat_CONTEXT["supported_versions"],
    "schemas": {},            # section_name -> type spec (filled at import time)
    "reference_checks": [],   # data-driven cross-reference definitions
    "options": {
        "strict_unknown_fields": False,   # warn (False) or error (True) on unknown fields
        "max_errors": 500,                # hard cap for collected errors
        "encoding": "utf-8",
    },
}

# =============================================================================
# C_TYPE_REGISTRY - semantic Python key -> C/C11/Cython target type mapping.
# Additive optimization: single source of truth for later code generation,
# FFI bindings and fixed-width persistence layouts. Purely declarative.
# =============================================================================

C_TYPE_REGISTRY = {
    "bool": "bool",
    "int": "int64_t",
    "positive_id": "uint32_t",
    "port": "uint16_t",
    "address": "uint16_t",
    "s7_address": "char*",
    "number": "double",
    "uuid": "char[37]",
    "str": "char*",
}

# =============================================================================
# CONFIG_SCHEMA_REGISTRY - structural meta layer for the canonical grouped
# runtime representation after central storage-contract normalization.
# Additive optimization: describes section KIND (singleton | table |
# grouped_rows), whether the section is mandatory, and the minimal required
# field sets. Consumed by fast pre-checks, SQLite table generation, MQTT topic
# routing and generic tooling - complementary to the deep declarative specs in
# build_config_schema(). Storage V8/V9 flat I/O is validated before conversion
# by _generic_type_library and never interpreted directly by business logic.
# =============================================================================

CONFIG_SCHEMA_REGISTRY = {
    "meta": {"kind": "singleton", "required_table": True,
             "required": ("schema", "version")},
    "local_node": {"kind": "singleton", "required_table": False,
                   "required": ("node_id",)},
    "sites": {"kind": "table", "required_table": False,
              "required": ("site_id", "enabled")},
    "nodes": {"kind": "table", "required_table": True,
              "required": ("node_id", "node_role", "enabled")},
    "site_controller_map": {"kind": "table", "required_table": True,
                            "required": ("node_id", "local_controller_id",
                                         "controller_uuid", "controller_key", "enabled")},
    "controllers": {"kind": "table", "required_table": True,
                    "required": ("controller_uuid", "controller_key",
                                 "bus_type", "host", "port")},
    "virtual_controllers": {"kind": "table", "required_table": False,
                            "required": ("virt_controller_id", "controller_uuid",
                                         "virt_active", "host", "port")},
    "sensors": {"kind": "grouped_rows", "required_table": True,
                "group_required": ("controller_uuid", "controller_key", "rows"),
                "row_required": ("sensor_id", "bus_type", "function_type", "address"),
                "identity": "sensor_key", "grouped_by": "controller_uuid"},
    "actuators": {"kind": "grouped_rows", "required_table": True,
                  "group_required": ("controller_uuid", "controller_key", "rows"),
                  "row_required": ("actuator_id", "bus_type", "function_type", "address"),
                  "identity": "actuator_key", "grouped_by": "controller_uuid"},
    "modbus_functions": {"kind": "table", "required_table": False,
                         "required": ("modbus_function_id", "module_type",
                                      "modbus_function_type")},
    "scaling_profiles": {"kind": "table", "required_table": False,
                         "required": ("profile_id", "raw_min", "raw_max",
                                      "eng_min", "eng_max")},
    "sensor_types": {"kind": "table", "required_table": False,
                     "required": ("sensor_types_id", "sensor_type")},
    "actuator_types": {"kind": "table", "required_table": False,
                       "required": ("actuator_types_id", "actuator_type")},
    "parameter_generic_templates": {"kind": "table", "required_table": False,
                                    "required": ("schema_id", "state_type")},
    "schema_mappings": {"kind": "table", "required_table": True,
                        "required": ("mapping_id", "schema_id")},
    "processes": {"kind": "table", "required_table": True,
                  "required": ("p_id", "process_name")},
    "process_values": {"kind": "table", "required_table": True,
                       "required": ("value_id", "state_value")},
    "process_modes": {"kind": "table", "required_table": True,
                      "required": ("mode_id", "mode")},
    "pid_configs": {"kind": "table", "required_table": False,
                    "required": ("pid_config_id", "pid_parameters")},
    "control_method_templates": {"kind": "table", "required_table": False,
                                 "required": ("template_id", "type", "parameters")},
    "control_methods": {"kind": "table", "required_table": False,
                        "required": ("method_id", "template_id", "args")},
    "process_states": {"kind": "table", "required_table": True,
                       "required": ("state_id", "p_id", "value_id",
                                    "mode_id", "mapping_id", "io_refs")},
    "rule_engine": {"kind": "table", "required_table": True,
                    "required": ("engine_id", "enabled", "runtime")},
    "filters": {"kind": "table", "required_table": False,
                "required": ("filter_id", "sensor_refs", "chain")},
    "timers": {"kind": "table", "required_table": False,
               "required": ("timer_id",)},
    "rules": {"kind": "table", "required_table": True,
              "required": ("rule_id", "engine_id", "priority",
                           "enabled", "when", "do")},
}

# =============================================================================
# HELP-FUNCTIONS
# =============================================================================

def is_bool(value):
    """Strict bool check (bool must be checked before int in Python)."""
    return isinstance(value, bool)


def is_int(value):
    """Strict int check - excludes bool which is a subclass of int."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_number(value):
    """Strict numeric check - int or float, never bool."""
    return is_int(value) or isinstance(value, float)


def type_name(value):
    """Human readable type name for error messages."""
    if value is None:
        return "null"
    if is_bool(value):
        return "bool"
    return type(value).__name__


def join_path(path, key):
    """Deterministic error-path building: 'a.b' + 'c' -> 'a.b.c'."""
    if path == "":
        return str(key)
    return path + "." + str(key)


def index_path(path, index):
    """Deterministic error-path building for list items: 'a' -> 'a[3]'."""
    return path + "[" + str(index) + "]"


def add_error(report, path, message):
    """Append one error to a report, respecting the max_errors cap."""
    if len(report["errors"]) < CONTEXT["options"]["max_errors"]:
        report["errors"].append({"path": path, "error": message})


def add_warning(report, path, message):
    """Append one warning to a report."""
    report["warnings"].append({"path": path, "warning": message})


def new_report():
    """Create a fresh, empty verification report structure."""
    return {"status": "OK", "errors": [], "warnings": [], "stats": {}}


def finalize_report(report):
    """Set final status flag deterministically based on collected errors."""
    report["status"] = "OK" if len(report["errors"]) == 0 else "FAILED"
    return report

# =============================================================================
# CODEBASE - FUNCTIONS - PART 1: GENERIC TYPE SPEC CONSTRUCTORS
# =============================================================================
# Every spec is a plain dict. This keeps the whole system dynamic, generic,
# serializable, C/Cython-friendly and free of any class machinery.

def t_any():
    """Spec: accept any JSON value."""
    return {"kind": "any"}


def t_null():
    """Spec: value must be null/None."""
    return {"kind": "null"}


def t_bool():
    """Spec: strict boolean."""
    return {"kind": "bool"}


def t_str(min_len=0, non_empty=False):
    """Spec: string, optionally with minimum length / non-empty constraint."""
    return {"kind": "str", "min_len": (1 if non_empty else min_len)}


def t_int(minimum=None, maximum=None):
    """Spec: strict integer (bool excluded) with optional bounds."""
    return {"kind": "int", "min": minimum, "max": maximum}


def t_number(minimum=None, maximum=None):
    """Spec: int or float (bool excluded) with optional bounds."""
    return {"kind": "number", "min": minimum, "max": maximum}


def t_enum(values):
    """Spec: value must be one of the given literal values."""
    return {"kind": "enum", "values": list(values)}


def t_list(item_spec, min_len=0):
    """Spec: homogeneous list, every item validated against item_spec."""
    return {"kind": "list", "item": item_spec, "min_len": min_len}


def t_dict(fields, required=None, allow_unknown=False):
    """
    Spec: object with named fields.
    fields   : dict field_name -> spec
    required : list of mandatory field names (default: all declared fields)
    """
    if required is None:
        required = sorted(fields.keys())
    return {
        "kind": "dict",
        "fields": fields,
        "required": list(required),
        "allow_unknown": allow_unknown,
    }


def t_map(value_spec):
    """Spec: open string-keyed mapping, all values share one value_spec."""
    return {"kind": "map", "value": value_spec}


def t_union(specs):
    """Spec: value must satisfy at least one of the given specs."""
    return {"kind": "union", "specs": list(specs)}


def t_optional(spec):
    """Spec: value may be null OR satisfy the given spec."""
    return t_union([t_null(), spec])

# =============================================================================
# CODEBASE - FUNCTIONS - PART 2: GENERIC RECURSIVE VALIDATOR
# =============================================================================

def validate_value(spec, value, path, report):
    """
    Generic, recursive, deterministic validation of one value against one
    spec. All findings are collected into the given report. Returns True
    when the value fully satisfies the spec.
    """
    kind = spec["kind"]

    if kind == "any":
        return True

    if kind == "null":
        if value is None:
            return True
        add_error(report, path, "expected null, got " + type_name(value))
        return False

    if kind == "bool":
        if is_bool(value):
            return True
        add_error(report, path, "expected bool, got " + type_name(value))
        return False

    if kind == "str":
        if not isinstance(value, str):
            add_error(report, path, "expected str, got " + type_name(value))
            return False
        if len(value) < spec["min_len"]:
            add_error(report, path, "string shorter than min_len " + str(spec["min_len"]))
            return False
        return True

    if kind == "int":
        if not is_int(value):
            add_error(report, path, "expected int, got " + type_name(value))
            return False
        return check_bounds(spec, value, path, report)

    if kind == "number":
        if not is_number(value):
            add_error(report, path, "expected number, got " + type_name(value))
            return False
        return check_bounds(spec, value, path, report)

    if kind == "enum":
        if value in spec["values"]:
            return True
        add_error(report, path, "value " + repr(value) + " not in enum " + repr(spec["values"]))
        return False

    if kind == "list":
        if not isinstance(value, list):
            add_error(report, path, "expected list, got " + type_name(value))
            return False
        if len(value) < spec["min_len"]:
            add_error(report, path, "list shorter than min_len " + str(spec["min_len"]))
            return False
        ok = True
        for i, item in enumerate(value):
            if not validate_value(spec["item"], item, index_path(path, i), report):
                ok = False
        return ok

    if kind == "dict":
        return validate_dict(spec, value, path, report)

    if kind == "map":
        if not isinstance(value, dict):
            add_error(report, path, "expected object, got " + type_name(value))
            return False
        ok = True
        for key in sorted(value.keys()):
            if not isinstance(key, str):
                add_error(report, path, "map key must be str, got " + type_name(key))
                ok = False
                continue
            if not validate_value(spec["value"], value[key], join_path(path, key), report):
                ok = False
        return ok

    if kind == "union":
        return validate_union(spec, value, path, report)

    add_error(report, path, "unknown spec kind: " + str(kind))
    return False


def check_bounds(spec, value, path, report):
    """Shared numeric bounds verification for int/number specs."""
    if spec["min"] is not None and value < spec["min"]:
        add_error(report, path, "value " + str(value) + " below minimum " + str(spec["min"]))
        return False
    if spec["max"] is not None and value > spec["max"]:
        add_error(report, path, "value " + str(value) + " above maximum " + str(spec["max"]))
        return False
    return True


def validate_dict(spec, value, path, report):
    """Validate a fixed-field object: required fields, field types, unknowns."""
    if not isinstance(value, dict):
        add_error(report, path, "expected object, got " + type_name(value))
        return False
    ok = True
    for field in spec["required"]:
        if field not in value:
            add_error(report, path, "missing required field '" + field + "'")
            ok = False
    for key in sorted(value.keys()):
        field_path = join_path(path, key)
        if key in spec["fields"]:
            if not validate_value(spec["fields"][key], value[key], field_path, report):
                ok = False
        elif not spec["allow_unknown"]:
            if CONTEXT["options"]["strict_unknown_fields"]:
                add_error(report, field_path, "unknown field")
                ok = False
            else:
                add_warning(report, field_path, "unknown field (not in schema)")
    return ok


def validate_union(spec, value, path, report):
    """
    Validate against a union: succeed silently on first match, otherwise
    report the branch errors of the best-matching alternative (fewest errors)
    for precise diagnostics.
    """
    best_errors = None
    for sub_spec in spec["specs"]:
        trial = new_report()
        if validate_value(sub_spec, value, path, trial):
            return True
        if best_errors is None or len(trial["errors"]) < len(best_errors):
            best_errors = trial["errors"]
    add_error(report, path, "no union alternative matched (" + str(len(spec["specs"])) + " tried)")
    for err in (best_errors or [])[:3]:
        add_error(report, err["path"], "closest alternative: " + err["error"])
    return False

# =============================================================================
# FIELD NAME CONSTANTS (UPPER_SNAKE_CASE) - single source of truth for ALL
# field names of the device_state_norm V5 schema. Import these in every
# business/logic layer for "typed" field access without magic strings.
# Schema evolution (V6+): update only here + the schema definitions below.
# =============================================================================

# --- Meta ---
SCHEMA = "schema"
VERSION = "version"
DESCRIPTION = "description"
SCOPE = "scope"
OWNER_NODE_ID = "owner_node_id"
DOMAIN_ID = "domain_id"
CROSS_DOMAIN_ID = "cross_domain_id"

# Cross execution shards keep only logical UUID identities. They deliberately
# do not duplicate physical bus endpoints or Modbus coordinates from node
# shards. The canonical validator selects this profile from meta.scope.
CROSS_CONFIG_SCOPES = (
    "cross",
    "cross_controller",
    "cross_worker",
)

# --- Routing / Identity / Deployment (core of the V5 multi-worker model) ---
SITE_ID = "site_id"
NODE_ID = "node_id"
SITE_NAME = "site_name"
NODE_ROLE = "node_role"
LOCAL_CONTROLLER_ID = "local_controller_id"
CONTROLLER_UUID = "controller_uuid"
CONTROLLER_KEY = "controller_key"
ROUTE_KEY = "route_key"
ROUTING_ROLE = "routing_role"
ACTIVE_CONTROLLER_UUIDS = "active_controller_uuids"
ACTIVE_CONTROLLER_ROUTES = "active_controller_routes"

# --- Common flags ---
ENABLED = "enabled"
NAME = "name"
TAGS = "tags"

# --- Modbus / IO basics ---
BUS_TYPE = "bus_type"
FUNCTION_TYPE = "function_type"
ADDRESS = "address"
MODULE_TYPE = "module_type"
MODBUS_FUNCTION_ID = "modbus_function_id"
MODBUS_FUNCTION_TYPE = "modbus_function_type"

# --- Sensors / Actuators ---
SENSOR_ID = "sensor_id"
ACTUATOR_ID = "actuator_id"
SENSOR_GROUP = "sensor_group"
ACTUATOR_GROUP = "actuator_group"
PROFILE_ID = "profile_id"
ROWS = "rows"
SENSOR_TYPES_ID = "sensor_types_id"
SENSOR_TYPE = "sensor_type"
ACTUATOR_TYPES_ID = "actuator_types_id"
ACTUATOR_TYPE = "actuator_type"

# --- Scaling ---
RAW_MIN = "raw_min"
RAW_MAX = "raw_max"
ENG_MIN = "eng_min"
ENG_MAX = "eng_max"
UNIT = "unit"

# --- Controllers / Virtual ---
CONTROLLER_ID = "controller_id"
DEVICE_NAME = "device_name"
HOST = "host"
PORT = "port"
COMM = "comm"
AREA_RIDS = "area_rids"
CONNECTION_TYPE = "connection_type"
OPTIMIZED_BLOCK_ACCESS = "optimized_block_access"
RACK = "rack"
SLOT = "slot"
S7_BATCH_FALLBACK_SEQUENTIAL = "s7_batch_fallback_sequential"
S7_MAX_PARALLEL = "s7_max_parallel"
S7_MULTI_READ_MAX_GAP = "s7_multi_read_max_gap"
S7_MULTI_VAR_LIMIT = "s7_multi_var_limit"
S7_READ_OPTIMIZER = "s7_read_optimizer"
SECURITY = "security"
USE_TLS = "use_tls"
VERIFY_PEER = "verify_peer"
USE_SYSTEM_CA = "use_system_ca"
TLS_CA_PATH = "tls_ca_path"
TLS_CERT_PATH = "tls_cert_path"
TLS_KEY_PATH = "tls_key_path"
CREDENTIAL_REF = "credential_ref"
PASSWORD_ENV = "password_env"
STRING_ENCODING = "string_encoding"
S7_MAX_MULTI_VARS = 20
S7_INLINE_SECRET_FIELDS = frozenset((
    "api_key",
    "certificate_data",
    "key_data",
    "password",
    "private_key",
    "secret",
    "token",
))
VIRT_CONTROLLER_ID = "virt_controller_id"
VIRT_ACTIVE = "virt_active"

# --- Parameter templates / mappings ---
SCHEMA_ID = "schema_id"
STATE_TYPE = "state_type"
MAPPING_ID = "mapping_id"

# --- Process / State machine ---
P_ID = "p_id"
PROCESS_NAME = "process_name"
PROCESS_LOCATION = "process_location"
VALUE_ID = "value_id"
STATE_VALUE = "state_value"
MODE_ID = "mode_id"
MODE = "mode"
STATE_ID = "state_id"
IO_REFS = "io_refs"
ACTUATORS = "actuators"
SENSORS = "sensors"
PARAMETERS = "parameters"
CONTROL_STRATEGY = "control_strategy"
SETPOINT_SOURCE = "setpoint_source"

# --- PID / Control strategy ---
PID_CONFIG_ID = "pid_config_id"
PID_PARAMETERS = "pid_parameters"
KP = "kp"
KI = "ki"
KD = "kd"
TYPE = "type"
ANTI_WINDUP = "anti_windup"
OUT_MIN = "out_min"
OUT_MAX = "out_max"
SAMPLE_TIME = "sample_time"
ERROR_SCALE = "error_scale"
DT_MAX_S = "dt_max_s"

# --- Control methods / templates ---
TEMPLATE_ID = "template_id"
TEMPLATE_NAME = "template_name"
TEMPLATE_DESCRIPTION = "template_description"
EXPR_ID = "expr_id"
METHOD_ID = "method_id"
ARGS = "args"

# --- Rule engine ---
ENGINE_ID = "engine_id"
REQUIRED_MODE_ID = "required_mode_id"
WRITABLE_FIELDS = "writable_fields"
SP_TOLERANCE = "sp_tolerance"
SAFETY = "safety"
DEFAULTS = "defaults"
RUNTIME = "runtime"
DEBUG = "debug"

# --- Filters / Timers ---
FILTER_ID = "filter_id"
SENSOR_REFS = "sensor_refs"
CHAIN = "chain"
WINDOW = "window"
ALPHA = "alpha"
PERIOD_S = "period_s"
TIMER_ID = "timer_id"
DURATION_S = "duration_s"
INTERVAL_S = "interval_s"
AT = "at"
CRON = "cron"

# --- Rules ---
RULE_ID = "rule_id"
GROUP = "group"
PRIORITY = "priority"
LATCH = "latch"
WHEN = "when"
UNTIL = "until"
DO = "do"
ELSE_DO = "else_do"
ACTIVATION_STATE = "activation_state"
DEPLOYMENT_STATUS = "deployment_status"
EXECUTION_EPOCH = "execution_epoch"
EXECUTION_OWNER = "execution_owner"
EXECUTION_SCOPE = "execution_scope"
SRC = "src"
FIELD = "field"
OP = "op"
VALUE = "value"
COOLDOWN_S = "cooldown_s"
PARAM_OF_STATE = "param_of_state"
KEY = "key"
SCALE = "scale"
STATE_IDS = "state_ids"
P_IDS = "p_ids"
NOTIFY = "notify"

# =============================================================================
# CODEBASE - FUNCTIONS - PART 3: DEVICE_STATE_NORM V5 SCHEMA DEFINITION
# =============================================================================
# The complete declarative schema of the config - one spec per section.
# All field names are referenced EXCLUSIVELY through the UPPER_SNAKE_CASE
# constants above (single source of truth, zero magic strings).

def spec_io_sensor_ref():
    """Hard I/O identity reference: controller_uuid + local sensor_id."""
    return t_dict({CONTROLLER_UUID: t_str(non_empty=True), SENSOR_ID: t_int(minimum=1)})


def spec_io_actuator_ref():
    """Hard I/O identity reference: controller_uuid + local actuator_id."""
    return t_dict({CONTROLLER_UUID: t_str(non_empty=True), ACTUATOR_ID: t_int(minimum=1)})


def spec_param_value():
    """Generic state parameter value: number, bool or string."""
    return t_union([t_number(), t_bool(), t_str()])


def spec_rule_condition_value():
    """
    Rule condition compare value: literal number/bool OR dynamic reference
    into the parameters of a process state (param_of_state indirection).
    """
    dynamic_ref = t_dict(
        {PARAM_OF_STATE: t_int(minimum=1), KEY: t_str(non_empty=True), SCALE: t_number()},
        required=[PARAM_OF_STATE, KEY],
    )
    return t_union([t_number(), t_bool(), dynamic_ref])


def spec_rule_condition():
    """One entry of a rule 'when'/'until' block (src: state|sensor|timer)."""
    return t_dict(
        {
            SRC: t_enum(["state", "sensor", "timer"]),
            STATE_ID: t_int(minimum=1),
            FIELD: t_str(non_empty=True),
            CONTROLLER_UUID: t_str(non_empty=True),
            SENSOR_ID: t_int(minimum=1),
            TIMER_ID: t_int(minimum=1),
            OP: t_enum(["==", "!=", ">", ">=", "<", "<=", "elapsed", "fired"]),
            VALUE: spec_rule_condition_value(),
            FILTER_ID: t_int(minimum=1),
            COOLDOWN_S: t_number(minimum=0),
        },
        required=[SRC, OP, VALUE],
    )


def spec_rule_action():
    """One entry of a rule 'do'/'else_do' block (targets + payload)."""
    return t_dict(
        {
            STATE_IDS: t_list(t_int(minimum=1), min_len=1),
            P_IDS: t_list(t_int(minimum=1), min_len=1),
            VALUE_ID: t_int(minimum=1),
            PARAMETERS: t_map(spec_param_value()),
            NOTIFY: t_str(non_empty=True),
        },
        required=[],
    )


def spec_control_strategy():
    """Control strategy: PID configuration or control_method reference."""
    pid = t_dict(
        {
            TYPE: t_enum(["PID"]),
            PID_CONFIG_ID: t_int(minimum=1),
            ANTI_WINDUP: t_bool(),
            OUT_MIN: t_number(),
            OUT_MAX: t_number(),
            SAMPLE_TIME: t_number(minimum=0),
            ERROR_SCALE: t_number(),
            DT_MAX_S: t_number(minimum=0),
        },
        required=[TYPE, PID_CONFIG_ID],
    )
    method = t_dict(
        {TYPE: t_enum(["control_method"]), METHOD_ID: t_int(minimum=1)},
    )
    return t_union([pid, method])


def _forbid_s7_inline_secrets(fields):
    """Add explicit never-valid fields for inline secret material."""
    result = dict(fields)
    for field in S7_INLINE_SECRET_FIELDS:
        result[field] = t_union([])
    return result


def spec_s7_security():
    """Build the secret-free S7 authentication and TLS reference contract."""
    fields = {
        MODE: t_enum(["classic", "legacy_password", "secure"]),
        USE_TLS: t_bool(),
        VERIFY_PEER: t_bool(),
        USE_SYSTEM_CA: t_bool(),
        TLS_CA_PATH: t_str(non_empty=True),
        TLS_CERT_PATH: t_str(non_empty=True),
        TLS_KEY_PATH: t_str(non_empty=True),
        CREDENTIAL_REF: t_str(non_empty=True),
        PASSWORD_ENV: t_str(non_empty=True),
    }
    return t_dict(_forbid_s7_inline_secrets(fields), required=[])


def spec_s7_comm():
    """Build all canonical S7 transport options accepted by the adapter."""
    fields = {
        AREA_RIDS: t_map(t_int(minimum=0)),
        CONNECTION_TYPE: t_int(minimum=1, maximum=10),
        OPTIMIZED_BLOCK_ACCESS: t_bool(),
        RACK: t_int(minimum=0, maximum=31),
        SLOT: t_int(minimum=0, maximum=31),
        S7_BATCH_FALLBACK_SEQUENTIAL: t_bool(),
        S7_MAX_PARALLEL: t_int(minimum=1, maximum=32),
        S7_MULTI_READ_MAX_GAP: t_int(minimum=0, maximum=65535),
        S7_MULTI_VAR_LIMIT: t_int(
            minimum=1, maximum=S7_MAX_MULTI_VARS
        ),
        S7_READ_OPTIMIZER: t_bool(),
        SECURITY: spec_s7_security(),
        STRING_ENCODING: t_str(non_empty=True),
    }
    return t_dict(_forbid_s7_inline_secrets(fields), required=[])


def spec_io_row(id_field, group_field):
    """Build strict Modbus or typed S7 sensor/actuator row variants."""
    common = {
        id_field: t_int(minimum=1),
        group_field: t_str(non_empty=True),
        PROFILE_ID: t_int(minimum=1),
    }
    required = [id_field, BUS_TYPE, FUNCTION_TYPE, ADDRESS, group_field]

    modbus_fields = dict(common)
    modbus_fields.update({
        BUS_TYPE: t_enum(["modbus"]),
        FUNCTION_TYPE: t_enum([
            "read_coil",
            "write_coil",
            "read_holding_registers",
            "write_register",
        ]),
        ADDRESS: t_int(minimum=0),
    })

    s7_fields = dict(common)
    s7_fields.update({
        BUS_TYPE: t_enum(["plc_s7"]),
        FUNCTION_TYPE: t_enum(["read_s7", "write_s7"]),
        ADDRESS: t_str(non_empty=True),
    })

    return t_union([
        t_dict(modbus_fields, required=required),
        t_dict(s7_fields, required=required),
    ])


def set_schema_required_fields(spec, required):
    """Set required fields on a dictionary schema or every union variant."""
    if not isinstance(spec, dict):
        raise TypeError("schema spec must be a dictionary")
    kind = spec.get("kind")
    if kind == "union":
        for alternative in spec.get("specs", []):
            set_schema_required_fields(alternative, required)
        return spec
    if kind != "dict":
        raise ValueError("required fields need a dictionary or union schema")
    spec["required"] = list(required)
    return spec


def spec_io_group(id_field, group_field):
    """Generic per-controller I/O group with routing fields and rows."""
    return t_dict(
        {
            SITE_ID: t_str(non_empty=True),
            NODE_ID: t_str(non_empty=True),
            LOCAL_CONTROLLER_ID: t_int(minimum=1),
            CONTROLLER_UUID: t_str(non_empty=True),
            CONTROLLER_KEY: t_str(non_empty=True),
            ENABLED: t_bool(),
            ROWS: t_list(spec_io_row(id_field, group_field)),
        },
        required=[NODE_ID, LOCAL_CONTROLLER_ID, CONTROLLER_UUID,
                  CONTROLLER_KEY, ENABLED, ROWS],
    )


def get_config_scope(config):
    """Return the normalized meta.scope value of one config document."""
    if not isinstance(config, dict):
        return ""
    meta_rows = config.get("meta") or []
    if not isinstance(meta_rows, list) or not meta_rows:
        return ""
    meta = meta_rows[0]
    if not isinstance(meta, dict):
        return ""
    return str(meta.get(SCOPE) or "").strip().lower()


def is_cross_config(config):
    """Return True only for isolated logical Cross execution shards."""
    return get_config_scope(config) in CROSS_CONFIG_SCOPES


def build_config_schema():
    """
    Build the complete declarative schema for device_state_norm v5.
    Returns a dict: section_name -> spec. Fully data-driven and constant-
    driven so new sections/fields extend the system without magic strings.
    """
    controller_route = t_dict(
        {
            LOCAL_CONTROLLER_ID: t_int(minimum=1),
            CONTROLLER_UUID: t_str(non_empty=True),
            CONTROLLER_KEY: t_str(non_empty=True),
        }
    )

    schema = {}

    schema["meta"] = t_list(
        t_dict(
            {
                SCHEMA: t_enum(["device_state_norm"]),
                VERSION: t_int(minimum=1),
                DESCRIPTION: t_str(),
                SCOPE: t_enum([
                    "node_local", "cross", "cross_controller",
                    "cross_worker", "master_global",
                ]),
                NODE_ID: t_str(non_empty=True),
                OWNER_NODE_ID: t_str(non_empty=True),
                DOMAIN_ID: t_str(non_empty=True),
                CROSS_DOMAIN_ID: t_str(non_empty=True),
            },
            required=[SCHEMA, VERSION, DESCRIPTION],
        ),
        min_len=1,
    )

    schema["local_node"] = t_list(
        t_dict(
            {
                SITE_ID: t_str(non_empty=True),
                NODE_ID: t_str(non_empty=True),
                ACTIVE_CONTROLLER_UUIDS: t_list(t_str(non_empty=True)),
                ACTIVE_CONTROLLER_ROUTES: t_list(controller_route),
            },
            required=[NODE_ID, ACTIVE_CONTROLLER_UUIDS,
                      ACTIVE_CONTROLLER_ROUTES],
        ),
        min_len=1,
    )

    schema["sites"] = t_list(
        t_dict(
            {
                SITE_ID: t_str(non_empty=True),
                SITE_NAME: t_str(non_empty=True),
                ENABLED: t_bool(),
                DESCRIPTION: t_str(),
            }
        ),
        min_len=1,
    )

    schema["nodes"] = t_list(
        t_dict(
            {
                SITE_ID: t_str(non_empty=True),
                NODE_ID: t_str(non_empty=True),
                NODE_ROLE: t_enum(["worker", "coordinator"]),
                ENABLED: t_bool(),
                DESCRIPTION: t_str(),
            },
            required=[NODE_ID, NODE_ROLE, ENABLED, DESCRIPTION],
        ),
        min_len=1,
    )

    schema["site_controller_map"] = t_list(
        t_dict(
            {
                SITE_ID: t_str(non_empty=True),
                NODE_ID: t_str(non_empty=True),
                LOCAL_CONTROLLER_ID: t_int(minimum=1),
                CONTROLLER_UUID: t_str(non_empty=True),
                CONTROLLER_KEY: t_str(non_empty=True),
                ENABLED: t_bool(),
                ROUTE_KEY: t_str(non_empty=True),
                ROUTING_ROLE: t_enum(["owner", "mirror", "standby"]),
            },
            required=[NODE_ID, LOCAL_CONTROLLER_ID, CONTROLLER_UUID,
                      CONTROLLER_KEY, ENABLED, ROUTE_KEY, ROUTING_ROLE],
        ),
        min_len=1,
    )

    schema["modbus_functions"] = t_list(
        t_dict(
            {
                MODBUS_FUNCTION_ID: t_int(minimum=1),
                MODULE_TYPE: t_str(non_empty=True),
                MODBUS_FUNCTION_TYPE: t_str(non_empty=True),
            }
        )
    )

    schema["scaling_profiles"] = t_list(
        t_dict(
            {
                PROFILE_ID: t_int(minimum=1),
                NAME: t_str(non_empty=True),
                RAW_MIN: t_int(),
                RAW_MAX: t_int(),
                ENG_MIN: t_number(),
                ENG_MAX: t_number(),
                UNIT: t_str(non_empty=True),
            }
        )
    )

    schema["controllers"] = t_list(
        t_dict(
            {
                CONTROLLER_ID: t_int(minimum=1),
                CONTROLLER_UUID: t_str(non_empty=True),
                CONTROLLER_KEY: t_str(non_empty=True),
                DEVICE_NAME: t_str(non_empty=True),
                BUS_TYPE: t_enum(["modbus", "plc_s7"]),
                HOST: t_str(non_empty=True),
                PORT: t_int(minimum=1, maximum=65535),
                UNIT: t_str(non_empty=True),
                COMM: spec_s7_comm(),
                TAGS: t_list(t_str()),
            },
            required=[
                CONTROLLER_ID, CONTROLLER_UUID, CONTROLLER_KEY, DEVICE_NAME,
                BUS_TYPE, HOST, PORT, UNIT, TAGS,
            ],
        ),
        min_len=1,
    )

    schema["virtual_controllers"] = t_list(
        t_dict(
            {
                VIRT_CONTROLLER_ID: t_int(minimum=1),
                CONTROLLER_UUID: t_str(non_empty=True),
                VIRT_ACTIVE: t_bool(),
                DEVICE_NAME: t_str(non_empty=True),
                HOST: t_str(non_empty=True),
                PORT: t_int(minimum=1, maximum=65535),
            }
        )
    )

    schema["sensors"] = t_list(spec_io_group(SENSOR_ID, SENSOR_GROUP))
    schema["actuators"] = t_list(spec_io_group(ACTUATOR_ID, ACTUATOR_GROUP))

    schema["sensor_types"] = t_list(
        t_dict({SENSOR_TYPES_ID: t_int(minimum=1), SENSOR_TYPE: t_str(non_empty=True)})
    )
    schema["actuator_types"] = t_list(
        t_dict({ACTUATOR_TYPES_ID: t_int(minimum=1), ACTUATOR_TYPE: t_str(non_empty=True)})
    )

    schema["parameter_generic_templates"] = t_list(
        t_dict(
            {
                SCHEMA_ID: t_int(minimum=1),
                STATE_TYPE: t_str(non_empty=True),
                DESCRIPTION: t_str(),
            }
        )
    )

    schema["schema_mappings"] = t_list(
        t_dict({MAPPING_ID: t_int(minimum=1), SCHEMA_ID: t_int(minimum=1)})
    )

    schema["processes"] = t_list(
        t_dict(
            {
                P_ID: t_int(minimum=1),
                PROCESS_NAME: t_str(non_empty=True),
                PROCESS_LOCATION: t_str(),
                OWNER_NODE_ID: t_str(non_empty=True),
            },
            required=[P_ID, PROCESS_NAME, PROCESS_LOCATION],
        )
    )

    schema["process_values"] = t_list(
        t_dict({VALUE_ID: t_int(minimum=1), STATE_VALUE: t_str(non_empty=True)})
    )

    schema["process_modes"] = t_list(
        t_dict({MODE_ID: t_int(minimum=1), MODE: t_str(non_empty=True)})
    )

    schema["pid_configs"] = t_list(
        t_dict(
            {
                PID_CONFIG_ID: t_int(minimum=1),
                PID_PARAMETERS: t_dict(
                    {KP: t_number(), KI: t_number(), KD: t_number()}
                ),
            }
        )
    )

    schema["control_method_templates"] = t_list(
        t_dict(
            {
                TEMPLATE_ID: t_int(minimum=1),
                TEMPLATE_NAME: t_str(non_empty=True),
                TEMPLATE_DESCRIPTION: t_str(),
                TYPE: t_str(non_empty=True),
                EXPR_ID: t_str(non_empty=True),
                PARAMETERS: t_list(t_str(non_empty=True)),
            },
            required=[TEMPLATE_ID, TEMPLATE_NAME, TYPE, PARAMETERS],
        )
    )

    schema["control_methods"] = t_list(
        t_dict(
            {
                METHOD_ID: t_int(minimum=1),
                TEMPLATE_ID: t_int(minimum=1),
                ARGS: t_map(t_number()),
            }
        )
    )

    schema["process_states"] = t_list(
        t_dict(
            {
                STATE_ID: t_int(minimum=1),
                P_ID: t_int(minimum=1),
                VALUE_ID: t_int(minimum=1),
                MODE_ID: t_int(minimum=1),
                MAPPING_ID: t_int(minimum=1),
                IO_REFS: t_dict(
                    {
                        ACTUATORS: t_list(spec_io_actuator_ref()),
                        SENSORS: t_list(spec_io_sensor_ref()),
                    }
                ),
                PARAMETERS: t_map(spec_param_value()),
                CONTROL_STRATEGY: t_optional(spec_control_strategy()),
                SETPOINT_SOURCE: t_optional(t_any()),
            }
        ),
    )

    schema["rule_engine"] = t_list(
        t_dict(
            {
                ENGINE_ID: t_int(minimum=1),
                ENABLED: t_bool(),
                REQUIRED_MODE_ID: t_int(minimum=1),
                WRITABLE_FIELDS: t_list(t_str(non_empty=True)),
                SP_TOLERANCE: t_number(minimum=0),
                SAFETY: t_map(t_any()),
                DEFAULTS: t_map(t_any()),
                RUNTIME: t_map(t_any()),
                DEBUG: t_map(t_any()),
            },
            required=[ENGINE_ID, ENABLED, REQUIRED_MODE_ID],
        ),
        min_len=1,
    )

    schema["filters"] = t_list(
        t_dict(
            {
                FILTER_ID: t_int(minimum=1),
                SENSOR_REFS: t_list(spec_io_sensor_ref(), min_len=1),
                CHAIN: t_list(
                    t_dict(
                        {
                            TYPE: t_str(non_empty=True),
                            WINDOW: t_int(minimum=1),
                            ALPHA: t_number(minimum=0, maximum=1),
                            PERIOD_S: t_number(minimum=0),
                        },
                        required=[TYPE],
                    ),
                    min_len=1,
                ),
            }
        )
    )

    schema["timers"] = t_list(
        t_dict(
            {
                TIMER_ID: t_int(minimum=1),
                DURATION_S: t_number(minimum=0),
                INTERVAL_S: t_number(minimum=0),
                AT: t_str(non_empty=True),
                CRON: t_str(non_empty=True),
                DESCRIPTION: t_str(),
            },
            required=[TIMER_ID],
        )
    )

    schema["rules"] = t_list(
        t_dict(
            {
                RULE_ID: t_int(minimum=1),
                ENGINE_ID: t_int(minimum=1),
                GROUP: t_enum(["process", "safety", "notify"]),
                PRIORITY: t_int(minimum=0),
                NAME: t_str(non_empty=True),
                ENABLED: t_bool(),
                LATCH: t_enum(["manual", "auto"]),
                DESCRIPTION: t_str(),
                WHEN: t_list(spec_rule_condition(), min_len=1),
                UNTIL: t_list(spec_rule_condition(), min_len=1),
                DO: t_list(spec_rule_action(), min_len=1),
                ELSE_DO: t_list(spec_rule_action(), min_len=1),
                OWNER_NODE_ID: t_str(non_empty=True),
                DOMAIN_ID: t_str(non_empty=True),
                ACTIVATION_STATE: t_enum(("staged", "active", "retiring", "inactive")),
                DEPLOYMENT_STATUS: t_enum((
                    "active_node", "active_cross", "replaced_local",
                    "monitor_only", "disabled_with_reason",
                )),
                EXECUTION_EPOCH: t_int(minimum=0),
                EXECUTION_OWNER: t_optional(t_str(non_empty=True)),
                EXECUTION_SCOPE: t_optional(t_str(non_empty=True)),
            },
            required=[
                RULE_ID, ENGINE_ID, GROUP, PRIORITY,
                NAME, ENABLED, WHEN, DO,
            ],
        )
    )

    return schema


def build_scope_config_schema(config):
    """
    Select the canonical schema profile for one document.

    Cross profiles are derived from the normal device schema and relax only
    fields that describe physical hardware. UUID identities, logical local
    I/O ids and all shared process/rule specs therefore remain one canonical
    source of truth.
    """
    schemas = CONTEXT["schemas"]
    if not is_cross_config(config):
        return schemas

    scoped = dict(schemas)

    local_node = copy.deepcopy(schemas["local_node"])
    local_node["min_len"] = 0
    scoped["local_node"] = local_node

    controllers = copy.deepcopy(schemas["controllers"])
    controllers["min_len"] = 0
    controller_row = controllers["item"]
    controller_row["fields"][OWNER_NODE_ID] = t_str(non_empty=True)
    controller_row["required"] = [
        CONTROLLER_UUID,
        CONTROLLER_KEY,
        OWNER_NODE_ID,
    ]
    scoped["controllers"] = controllers

    routes = copy.deepcopy(schemas["site_controller_map"])
    routes["min_len"] = 0
    scoped["site_controller_map"] = routes

    io_profiles = (
        ("sensors", SENSOR_ID, SENSOR_GROUP),
        ("actuators", ACTUATOR_ID, ACTUATOR_GROUP),
    )
    for section_name, id_field, group_field in io_profiles:
        section = copy.deepcopy(schemas[section_name])
        group = section["item"]
        group["required"] = [
            CONTROLLER_UUID,
            CONTROLLER_KEY,
            ENABLED,
            ROWS,
        ]
        io_row = group["fields"][ROWS]["item"]
        set_schema_required_fields(io_row, [id_field, group_field])
        scoped[section_name] = section

    return scoped

# =============================================================================
# CODEBASE - FUNCTIONS - PART 4: IDENTITY INDEX & REFERENTIAL INTEGRITY
# =============================================================================

def build_identity_index(config):
    """
    Build all identity/lookup sets used for referential integrity checks.
    Identity model : controller_uuid + local sensor_id/actuator_id
    Routing model  : site_id + node_id + local_controller_id
    """
    index = {
        "controller_uuids": set(),
        "controller_keys": set(),
        "site_ids": set(),
        "node_keys": set(),          # (site_id, node_id)
        "route_keys": set(),         # (site_id, node_id, local_controller_id)
        "sensor_ids": set(),         # (controller_uuid, sensor_id)
        "actuator_ids": set(),       # (controller_uuid, actuator_id)
        "profile_ids": set(),
        "schema_ids": set(),
        "mapping_ids": set(),
        "p_ids": set(),
        "value_ids": set(),
        "mode_ids": set(),
        "state_ids": set(),
        "pid_config_ids": set(),
        "template_ids": set(),
        "method_ids": set(),
        "engine_ids": set(),
        "filter_ids": set(),
        "timer_ids": set(),
    }
    for row in config.get("controllers", []):
        index["controller_uuids"].add(row.get("controller_uuid"))
        index["controller_keys"].add(row.get("controller_key"))
    for row in config.get("sites", []):
        index["site_ids"].add(row.get("site_id"))
    for row in config.get("nodes", []):
        index["node_keys"].add((row.get("site_id"), row.get("node_id")))
    for row in config.get("site_controller_map", []):
        index["route_keys"].add(
            (row.get("site_id"), row.get("node_id"), row.get("local_controller_id"))
        )
    for group in config.get("sensors", []):
        for row in group.get("rows", []):
            index["sensor_ids"].add((group.get("controller_uuid"), row.get("sensor_id")))
    for group in config.get("actuators", []):
        for row in group.get("rows", []):
            index["actuator_ids"].add((group.get("controller_uuid"), row.get("actuator_id")))
    index["state_parameter_keys"] = {}
    for row in config.get("process_states", []):
        parameters = row.get("parameters", {})
        if isinstance(parameters, dict):
            index["state_parameter_keys"][row.get("state_id")] = set(parameters.keys())
    simple_id_sources = [
        ("scaling_profiles", "profile_id", "profile_ids"),
        ("parameter_generic_templates", "schema_id", "schema_ids"),
        ("schema_mappings", "mapping_id", "mapping_ids"),
        ("processes", "p_id", "p_ids"),
        ("process_values", "value_id", "value_ids"),
        ("process_modes", "mode_id", "mode_ids"),
        ("process_states", "state_id", "state_ids"),
        ("pid_configs", "pid_config_id", "pid_config_ids"),
        ("control_method_templates", "template_id", "template_ids"),
        ("control_methods", "method_id", "method_ids"),
        ("rule_engine", "engine_id", "engine_ids"),
        ("filters", "filter_id", "filter_ids"),
        ("timers", "timer_id", "timer_ids"),
    ]
    for section, field, target in simple_id_sources:
        for row in config.get(section, []):
            index[target].add(row.get(field))
    return index


def _check_unique_field(config, report, spec):
    section, field, optional = spec
    seen = set()
    for index, row in enumerate(config.get(section, [])):
        if not isinstance(row, dict):
            continue
        key = row.get(field)
        if optional and key is None:
            continue
        if key in seen:
            add_error(
                report,
                index_path(section, index),
                "duplicate " + field + " " + repr(key),
            )
        seen.add(key)


def _check_route_uniqueness(config, report):
    seen_routes = set()
    seen_route_names = set()
    for index, row in enumerate(config.get("site_controller_map", [])):
        if not isinstance(row, dict):
            continue
        route = (
            row.get("site_id"),
            row.get("node_id"),
            row.get("local_controller_id"),
        )
        if route in seen_routes:
            add_error(
                report,
                index_path("site_controller_map", index),
                "duplicate routing key " + repr(route),
            )
        seen_routes.add(route)
        route_name = row.get("route_key")
        if route_name in seen_route_names:
            add_error(
                report,
                index_path("site_controller_map", index) + ".route_key",
                "duplicate route_key " + repr(route_name),
            )
        seen_route_names.add(route_name)


def _check_io_section_uniqueness(config, report, section, id_field):
    seen_groups = set()
    seen_io = set()
    for group_index, group in enumerate(config.get(section, [])):
        if not isinstance(group, dict):
            continue
        uuid_value = group.get("controller_uuid")
        if uuid_value in seen_groups:
            add_error(
                report,
                index_path(section, group_index),
                "duplicate controller I/O group " + repr(uuid_value),
            )
        seen_groups.add(uuid_value)
        for row_index, row in enumerate(group.get("rows", [])):
            if not isinstance(row, dict):
                continue
            identity = (uuid_value, row.get(id_field))
            if identity in seen_io:
                add_error(
                    report,
                    index_path(index_path(section, group_index) + ".rows", row_index),
                    "duplicate I/O identity " + repr(identity),
                )
            seen_io.add(identity)


def check_uniqueness(config, report):
    """Verify primary identifiers and compound entity identities."""
    unique_specs = [
        ("sites", "site_id", False),
        ("nodes", "node_id", False),
        ("controllers", "controller_uuid", False),
        ("controllers", "controller_key", False),
        ("controllers", "controller_id", True),
        ("virtual_controllers", "virt_controller_id", False),
        ("modbus_functions", "modbus_function_id", False),
        ("scaling_profiles", "profile_id", False),
        ("sensor_types", "sensor_types_id", False),
        ("actuator_types", "actuator_types_id", False),
        ("parameter_generic_templates", "schema_id", False),
        ("processes", "p_id", False),
        ("process_values", "value_id", False),
        ("process_modes", "mode_id", False),
        ("process_states", "state_id", False),
        ("pid_configs", "pid_config_id", False),
        ("control_method_templates", "template_id", False),
        ("control_methods", "method_id", False),
        ("rule_engine", "engine_id", False),
        ("filters", "filter_id", False),
        ("timers", "timer_id", False),
        ("rules", "rule_id", False),
        ("schema_mappings", "mapping_id", False),
    ]
    for spec in unique_specs:
        _check_unique_field(config, report, spec)
    _check_route_uniqueness(config, report)
    _check_io_section_uniqueness(config, report, "sensors", "sensor_id")
    _check_io_section_uniqueness(config, report, "actuators", "actuator_id")


def _route_key_candidates(route):
    node_id = str(route.get("node_id") or "")
    local_id = str(route.get("local_controller_id") or "")
    site_id = str(route.get("site_id") or "")
    base = node_id + ".ctrl_" + local_id
    candidates = {base}
    if site_id:
        candidates.add(site_id + "." + base)
    return candidates


def _route_matches_group(route, group):
    fields = (
        "site_id",
        "node_id",
        "local_controller_id",
        "controller_uuid",
        "controller_key",
    )
    for field in fields:
        if route.get(field) != group.get(field):
            return False
    return True


def _active_route_matches(active_route, candidate, node_id):
    if str(candidate.get("node_id") or "") != node_id:
        return False
    if candidate.get("local_controller_id") != active_route.get("local_controller_id"):
        return False
    return candidate.get("controller_key") == active_route.get("controller_key")


def _verify_one_local_route(report, base, route_index, active_route, routes_by_uuid, node_id):
    if not isinstance(active_route, dict):
        return
    controller_uuid = active_route.get("controller_uuid")
    candidates = routes_by_uuid.get(controller_uuid) or []
    matcher = partial(_active_route_matches, active_route, node_id=node_id)
    if not any(matcher(candidate) for candidate in candidates):
        add_error(
            report,
            index_path(join_path(base, "active_controller_routes"), route_index),
            "active route does not match site_controller_map",
        )


def _verify_local_node_routes(config, report, routes_by_uuid, node_id):
    for local_index, local in enumerate(config.get("local_node", [])):
        if not isinstance(local, dict):
            continue
        base = index_path("local_node", local_index)
        active_uuids = list(local.get("active_controller_uuids") or [])
        active_routes = list(local.get("active_controller_routes") or [])
        route_uuids = [
            route.get("controller_uuid")
            for route in active_routes
            if isinstance(route, dict)
        ]
        if len(active_uuids) != len(set(active_uuids)):
            add_error(report, join_path(base, "active_controller_uuids"),
                      "duplicate active controller UUID")
        if len(route_uuids) != len(set(route_uuids)):
            add_error(report, join_path(base, "active_controller_routes"),
                      "duplicate active controller route")
        if set(active_uuids) != set(route_uuids):
            add_error(report, base,
                      "active_controller_uuids and active_controller_routes differ")
        verifier = partial(
            _verify_one_local_route,
            report,
            base,
            routes_by_uuid=routes_by_uuid,
            node_id=node_id,
        )
        for route_index, active_route in enumerate(active_routes):
            verifier(route_index, active_route)


def _build_routing_maps(config, report, controllers):
    owners = {}
    routes_by_uuid = {}
    for index, route in enumerate(config.get("site_controller_map", [])):
        if not isinstance(route, dict):
            continue
        controller_uuid = route.get("controller_uuid")
        routes_by_uuid.setdefault(controller_uuid, []).append(route)
        controller = controllers.get(controller_uuid)
        if isinstance(controller, dict) and route.get("controller_key") != controller.get(
                "controller_key"):
            add_error(
                report,
                index_path("site_controller_map", index) + ".controller_key",
                "route controller_key differs from controller UUID record",
            )
        route_key = str(route.get("route_key") or "")
        if route_key and route_key not in _route_key_candidates(route):
            add_error(
                report,
                index_path("site_controller_map", index) + ".route_key",
                "route_key is inconsistent with node/local_controller identity",
            )
        enabled_owner = (
            route.get("enabled") is not False
            and str(route.get("routing_role") or "owner") == "owner"
        )
        if enabled_owner:
            owners.setdefault(controller_uuid, []).append(route.get("node_id"))
    return owners, routes_by_uuid


def _verify_controller_owner_routes(controllers, owners, report):
    for controller_uuid in sorted(controllers):
        matches = owners.get(controller_uuid) or []
        if len(matches) != 1:
            add_error(
                report,
                "site_controller_map",
                "controller {} requires exactly one enabled owner route; found {}".format(
                    controller_uuid,
                    len(matches),
                ),
            )


def _verify_io_routing_groups(config, report, controllers, routes_by_uuid, cross_scope):
    for section in ("sensors", "actuators"):
        for group_index, group in enumerate(config.get(section, [])):
            if not isinstance(group, dict):
                continue
            base = index_path(section, group_index)
            controller_uuid = group.get("controller_uuid")
            controller = controllers.get(controller_uuid)
            if isinstance(controller, dict) and group.get("controller_key") != controller.get(
                    "controller_key"):
                add_error(
                    report,
                    base + ".controller_key",
                    "I/O controller_key differs from controller UUID record",
                )
            if cross_scope:
                continue
            matching_routes = routes_by_uuid.get(controller_uuid) or []
            matcher = partial(_route_matches_group, group=group)
            if not any(matcher(route) for route in matching_routes):
                add_error(
                    report,
                    base,
                    "I/O routing metadata does not match the controller owner route",
                )


def _verify_node_local_scope(config, report, routes_by_uuid):
    meta = _meta_row(config)
    if str(meta.get("scope") or "") != "node_local":
        return
    node_id = str(meta.get("node_id") or "")
    if not node_id:
        add_error(report, "meta[0].node_id", "node_local scope requires node_id")
    sections = ("nodes", "local_node", "site_controller_map", "sensors", "actuators")
    for section in sections:
        for index, row in enumerate(config.get(section, [])):
            if not isinstance(row, dict) or row.get("node_id") is None:
                continue
            if str(row.get("node_id")) != node_id:
                add_error(
                    report,
                    index_path(section, index) + ".node_id",
                    "node_local shard contains foreign node",
                )
    _verify_local_node_routes(config, report, routes_by_uuid, node_id)


def verify_routing_ownership(config, report):
    """Enforce hardware ownership and all controller-routing relationships."""
    cross_scope = is_cross_config(config)
    controllers = {
        row.get("controller_uuid"): row
        for row in config.get("controllers", [])
        if isinstance(row, dict) and row.get("controller_uuid")
    }
    owners, routes_by_uuid = _build_routing_maps(config, report, controllers)
    if not cross_scope:
        _verify_controller_owner_routes(controllers, owners, report)
    _verify_io_routing_groups(
        config,
        report,
        controllers,
        routes_by_uuid,
        cross_scope,
    )
    _verify_node_local_scope(config, report, routes_by_uuid)


def check_ref(index, index_key, value, path, report, label):
    """Generic single cross-reference existence check."""
    if value not in index[index_key]:
        add_error(report, path, label + " " + repr(value) + " not found in " + index_key)


def _verify_rule_engine_references(config, report, ref):
    for index, engine in enumerate(config.get("rule_engine", [])):
        ref(
            "mode_ids",
            engine.get("required_mode_id"),
            join_path(index_path("rule_engine", index), "required_mode_id"),
            report,
            "mode_id",
        )


def verify_references(config, report):
    """
    Verify every cross-reference in the config against the identity index.
    Fully generic: composed from the single check_ref() primitive via
    functools.partial, so new reference rules are one line each.
    """
    index = build_identity_index(config)
    ref = partial(check_ref, index)
    cross_scope = is_cross_config(config)

    # --- routing layer -------------------------------------------------
    for i, row in enumerate(config.get("site_controller_map", [])):
        base = index_path("site_controller_map", i)
        if row.get("site_id") is not None:
            ref("site_ids", row.get("site_id"),
                join_path(base, "site_id"), report, "site_id")
        ref("controller_uuids", row.get("controller_uuid"),
            join_path(base, "controller_uuid"), report, "controller_uuid")
        if (row.get("site_id"), row.get("node_id")) not in index["node_keys"]:
            add_error(report, join_path(base, "node_id"),
                      "node " + repr(row.get("node_id")) + " not declared in nodes")

    for i, row in enumerate(config.get("local_node", [])):
        base = index_path("local_node", i)
        for j, uuid_value in enumerate(row.get("active_controller_uuids", [])):
            ref("controller_uuids", uuid_value,
                index_path(join_path(base, "active_controller_uuids"), j),
                report, "controller_uuid")
        for j, route in enumerate(row.get("active_controller_routes", [])):
            ref("controller_uuids", route.get("controller_uuid"),
                index_path(join_path(base, "active_controller_routes"), j),
                report, "controller_uuid")

    # --- virtual controllers -------------------------------------------
    for i, row in enumerate(config.get("virtual_controllers", [])):
        ref("controller_uuids", row.get("controller_uuid"),
            join_path(index_path("virtual_controllers", i), "controller_uuid"),
            report, "controller_uuid")

    # --- I/O groups: controller, routing and profile references --------
    for section in ["sensors", "actuators"]:
        for i, group in enumerate(config.get(section, [])):
            base = index_path(section, i)
            ref("controller_uuids", group.get("controller_uuid"),
                join_path(base, "controller_uuid"), report, "controller_uuid")
            if not cross_scope:
                route = (group.get("site_id"), group.get("node_id"),
                         group.get("local_controller_id"))
                if route not in index["route_keys"]:
                    add_error(report, base, "routing key " + repr(route)
                              + " not found in site_controller_map")
            for j, row in enumerate(group.get("rows", [])):
                if "profile_id" in row:
                    ref("profile_ids", row.get("profile_id"),
                        index_path(join_path(base, "rows"), j) + ".profile_id",
                        report, "profile_id")

    # --- schema mappings -------------------------------------------------
    for i, row in enumerate(config.get("schema_mappings", [])):
        ref("schema_ids", row.get("schema_id"),
            join_path(index_path("schema_mappings", i), "schema_id"),
            report, "schema_id")

    # --- control methods -> templates ------------------------------------
    for i, row in enumerate(config.get("control_methods", [])):
        ref("template_ids", row.get("template_id"),
            join_path(index_path("control_methods", i), "template_id"),
            report, "template_id")

    # --- rule engine ------------------------------------------------------
    _verify_rule_engine_references(config, report, ref)

    # --- process states ---------------------------------------------------
    for i, state in enumerate(config.get("process_states", [])):
        base = index_path("process_states", i)
        ref("p_ids", state.get("p_id"), join_path(base, "p_id"), report, "p_id")
        ref("value_ids", state.get("value_id"), join_path(base, "value_id"), report, "value_id")
        ref("mode_ids", state.get("mode_id"), join_path(base, "mode_id"), report, "mode_id")
        ref("mapping_ids", state.get("mapping_id"),
            join_path(base, "mapping_id"), report, "mapping_id")
        io_refs = state.get("io_refs", {})
        for j, actuator in enumerate(io_refs.get("actuators", [])):
            identity = (actuator.get("controller_uuid"), actuator.get("actuator_id"))
            if identity not in index["actuator_ids"]:
                add_error(report, index_path(join_path(base, "io_refs.actuators"), j),
                          "actuator identity " + repr(identity) + " not found")
        for j, sensor in enumerate(io_refs.get("sensors", [])):
            identity = (sensor.get("controller_uuid"), sensor.get("sensor_id"))
            if identity not in index["sensor_ids"]:
                add_error(report, index_path(join_path(base, "io_refs.sensors"), j),
                          "sensor identity " + repr(identity) + " not found")
        strategy = state.get("control_strategy")
        if isinstance(strategy, dict):
            if strategy.get("type") == "PID":
                ref("pid_config_ids", strategy.get("pid_config_id"),
                    join_path(base, "control_strategy.pid_config_id"),
                    report, "pid_config_id")
            if strategy.get("type") == "control_method":
                ref("method_ids", strategy.get("method_id"),
                    join_path(base, "control_strategy.method_id"),
                    report, "method_id")

    # --- filters -----------------------------------------------------------
    for i, filter_row in enumerate(config.get("filters", [])):
        base = index_path("filters", i)
        for j, sensor in enumerate(filter_row.get("sensor_refs", [])):
            identity = (sensor.get("controller_uuid"), sensor.get("sensor_id"))
            if identity not in index["sensor_ids"]:
                add_error(report, index_path(join_path(base, "sensor_refs"), j),
                          "sensor identity " + repr(identity) + " not found")

    # --- rules ---------------------------------------------------------------
    for i, rule in enumerate(config.get("rules", [])):
        base = index_path("rules", i)
        ref("engine_ids", rule.get("engine_id"),
            join_path(base, "engine_id"), report, "engine_id")
        for block_name in ["when", "until"]:
            for j, cond in enumerate(rule.get(block_name, [])):
                cond_path = index_path(join_path(base, block_name), j)
                verify_rule_condition_refs(index, cond, cond_path, report)
        for block_name in ["do", "else_do"]:
            for j, action in enumerate(rule.get(block_name, [])):
                action_path = index_path(join_path(base, block_name), j)
                for state_id in action.get("state_ids", []):
                    ref("state_ids", state_id, join_path(action_path, "state_ids"),
                        report, "state_id")
                for p_id in action.get("p_ids", []):
                    ref("p_ids", p_id, join_path(action_path, "p_ids"), report, "p_id")
                if "value_id" in action:
                    ref("value_ids", action.get("value_id"),
                        join_path(action_path, "value_id"), report, "value_id")


def verify_rule_condition_refs(index, cond, path, report):
    """Verify all references inside one rule condition, per src variant."""
    ref = partial(check_ref, index)
    src = cond.get("src")
    if src == "state":
        ref("state_ids", cond.get("state_id"),
            join_path(path, "state_id"), report, "state_id")
    if src == "sensor":
        identity = (cond.get("controller_uuid"), cond.get("sensor_id"))
        if identity not in index["sensor_ids"]:
            add_error(report, path, "sensor identity " + repr(identity) + " not found")
        if "filter_id" in cond:
            ref("filter_ids", cond.get("filter_id"),
                join_path(path, "filter_id"), report, "filter_id")
    if src == "timer":
        ref("timer_ids", cond.get("timer_id"),
            join_path(path, "timer_id"), report, "timer_id")
    value = cond.get(VALUE)
    if isinstance(value, dict) and PARAM_OF_STATE in value:
        target_state_id = value.get(PARAM_OF_STATE)
        ref("state_ids", target_state_id,
            join_path(path, "value.param_of_state"), report, "state_id")
        parameter_keys = index["state_parameter_keys"].get(target_state_id, set())
        if target_state_id in index["state_ids"] and value.get(KEY) not in parameter_keys:
            add_error(report, join_path(path, "value.key"),
                      "parameter key " + repr(value.get(KEY))
                      + " not found in parameters of state " + str(target_state_id))

# =============================================================================
# CODEBASE - FUNCTIONS - PART 5: FULL CONFIG VERIFICATION PIPELINE
# =============================================================================

def _meta_row(config):
    rows = config.get("meta") or [] if isinstance(config, dict) else []
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows[0]
    return {}


def _append_contract_errors(report, contract):
    for row in contract.get("errors") or []:
        add_error(
            report,
            str(row.get("path") or ""),
            str(row.get("error") or "config contract error"),
        )
    for row in contract.get("warnings") or []:
        add_warning(
            report,
            str(row.get("path") or ""),
            str(row.get("warning") or "config contract warning"),
        )


def verify_scope_metadata(config, report):
    """Verify scope identity and version-dependent ownership declarations."""
    meta = _meta_row(config)
    version = _compat_get_config_version(config)
    scope = str(meta.get("scope") or "").strip().lower()
    if version in (8, 9) and not scope:
        add_error(report, "meta[0].scope", "schema version requires an explicit scope")
        return
    if scope == "node_local" and not str(meta.get("node_id") or ""):
        add_error(report, "meta[0].node_id", "node_local scope requires node_id")
    if scope in CROSS_CONFIG_SCOPES:
        if not str(meta.get("owner_node_id") or ""):
            add_error(report, "meta[0].owner_node_id", "cross scope requires owner_node_id")
        if not str(meta.get("domain_id") or meta.get("cross_domain_id") or ""):
            add_error(report, "meta[0].domain_id", "cross scope requires a domain identity")


def _execution_scope_expectations(meta):
    scope = str(meta.get("scope") or "").strip().lower()
    if scope == "node_local":
        owner = str(meta.get("node_id") or "")
        return scope, owner, "node:" + owner if owner else ""
    if scope in CROSS_CONFIG_SCOPES:
        return scope, str(meta.get("owner_node_id") or ""), ""
    return scope, "", ""


def _verify_execution_entity_owners(
        config, report, version, scope, expected_owner):
    scoped_authority = scope == "node_local" or scope in CROSS_CONFIG_SCOPES
    for section_name in ("processes", "rules"):
        for index, row in enumerate(config.get(section_name, [])):
            if not isinstance(row, dict):
                continue
            base = index_path(section_name, index)
            owner = str(row.get("owner_node_id") or "")
            if version == 9 and scoped_authority and not owner:
                add_error(report, base + ".owner_node_id",
                          "V9 execution entity requires owner_node_id")
            if owner and expected_owner and owner != expected_owner:
                add_error(report, base + ".owner_node_id",
                          "logical owner differs from shard owner")

def _verify_rule_execution_metadata(config, report, version, scope,
                                    expected_owner, expected_scope):
    required_fields = (
        "activation_state",
        "deployment_status",
        "execution_epoch",
        "execution_owner",
        "execution_scope",
    )
    for index, rule in enumerate(config.get("rules", [])):
        if not isinstance(rule, dict):
            continue
        base = index_path("rules", index)
        execution_owner = str(rule.get("execution_owner") or "")
        if execution_owner and expected_owner and execution_owner != expected_owner:
            add_error(report, base + ".execution_owner",
                      "execution_owner differs from shard owner")
        execution_scope = str(rule.get("execution_scope") or "")
        if scope == "node_local" and execution_scope and execution_scope != expected_scope:
            add_error(report, base + ".execution_scope",
                      "execution_scope differs from node shard identity")
        if version != 9:
            continue
        for field in required_fields:
            if field not in rule:
                add_error(report, base, "V9 rule requires field '" + field + "'")
        if scope == "node_local" or scope in CROSS_CONFIG_SCOPES:
            if not execution_owner:
                add_error(report, base + ".execution_owner",
                          "scoped V9 rule requires execution_owner")
            if not execution_scope:
                add_error(report, base + ".execution_scope",
                          "scoped V9 rule requires execution_scope")


def verify_execution_ownership(config, report):
    """Verify logical owner declarations without changing legacy V5 imports."""
    meta = _meta_row(config)
    version = _compat_get_config_version(config)
    scope, expected_owner, expected_scope = _execution_scope_expectations(meta)
    _verify_execution_entity_owners(
        config,
        report,
        version,
        scope,
        expected_owner,
    )
    _verify_rule_execution_metadata(
        config,
        report,
        version,
        scope,
        expected_owner,
        expected_scope,
    )


def verify_meta(config, report):
    """Verify schema identity and version before any deep validation."""
    meta_rows = config.get("meta", [])
    if not isinstance(meta_rows, list) or not meta_rows:
        add_error(report, "meta", "missing or invalid meta section")
        return
    meta = meta_rows[0]
    if not isinstance(meta, dict):
        add_error(report, "meta[0]", "meta entry must be an object")
        return
    if meta.get("schema") != CONTEXT["supported_schema"]:
        add_error(report, "meta[0].schema",
                  "unsupported schema " + repr(meta.get("schema")))
    if meta.get("version") not in CONTEXT["supported_schema_versions"]:
        add_error(
            report,
            "meta[0].version",
            "unsupported schema version " + repr(meta.get("version")),
        )


def verify_sections(config, report):
    """Validate every registered section against its declarative spec.

    Sections marked required_table=False in CONFIG_SCHEMA_REGISTRY are
    optional: when absent they are skipped instead of raising an error
    (e.g. the optional site tier in site-free node shard configs).
    """
    schemas = build_scope_config_schema(config)
    for section_name in sorted(schemas.keys()):
        if section_name not in config:
            registry_entry = CONFIG_SCHEMA_REGISTRY.get(section_name) or {}
            if registry_entry.get("required_table", True):
                add_error(report, section_name, "missing required section")
            continue
        validate_value(schemas[section_name], config[section_name], section_name, report)
    for section_name in sorted(config.keys()):
        if section_name not in schemas:
            if CONTEXT["options"]["strict_unknown_fields"]:
                add_error(report, section_name, "unknown top-level section")
            else:
                add_warning(report, section_name, "unknown top-level section (not in schema)")


def collect_stats(config, report):
    """Deterministic quantitative summary of the verified config."""
    stats = {}
    for section_name in sorted(config.keys()):
        value = config[section_name]
        if isinstance(value, list):
            stats[section_name] = len(value)
    stats["sensor_rows"] = sum(
        len(group.get("rows", [])) for group in config.get("sensors", [])
    )
    stats["actuator_rows"] = sum(
        len(group.get("rows", [])) for group in config.get("actuators", [])
    )
    report["stats"] = stats


def get_c_type(python_key):
    """Return the C/C11/Cython target type for a semantic key (codegen/FFI)."""
    return CONTEXT["c_type_registry"].get(python_key, "void*")


def register_c_type(python_key, c_type_name):
    """Adaptively register/override one semantic key -> C type mapping."""
    CONTEXT["c_type_registry"][python_key] = c_type_name


def get_section_schema(section_name):
    """Central accessor into the structural config schema registry."""
    return CONTEXT["config_schema_registry"].get(section_name)


def register_section_layout(section_name, layout):
    """
    Adaptively register/override one structural section layout at runtime.
    layout: {"kind": "singleton"|"table"|"grouped_rows", "required_table": bool,
             "required"/"group_required"/"row_required": tuple of field names}
    Takes effect immediately in verify_registry_layout() - no core changes.
    """
    CONTEXT["config_schema_registry"][section_name] = layout


def get_required_fields_for_section(section_name):
    """Flat tuple of all required field names of one section (any kind)."""
    schema = get_section_schema(section_name)
    if not schema:
        return ()
    return (schema.get("required", ())
            + schema.get("group_required", ())
            + schema.get("row_required", ()))


def check_required_keys(required, row, path, report):
    """Generic required-key presence check on one object row."""
    for field in required:
        if field not in row:
            add_error(report, path, "registry: missing required field '" + field + "'")


def build_scope_config_registry(config):
    """Derive the structural registry profile selected by meta.scope."""
    registry = CONTEXT["config_schema_registry"]
    scope = _compat_get_config_scope(config)
    version = _compat_get_config_version(config)
    if version == LEGACY_VERSION or scope == "master_global":
        scoped = dict(registry)
        local_node = dict(registry["local_node"])
        local_node["kind"] = "table"
        scoped["local_node"] = local_node
        return scoped
    if not is_cross_config(config):
        return registry

    scoped = dict(registry)

    local_node = dict(registry["local_node"])
    local_node["allow_empty"] = True
    scoped["local_node"] = local_node

    controllers = dict(registry["controllers"])
    controllers["required"] = (
        CONTROLLER_UUID,
        CONTROLLER_KEY,
        OWNER_NODE_ID,
    )
    scoped["controllers"] = controllers

    io_profiles = (
        ("sensors", SENSOR_ID, SENSOR_GROUP),
        ("actuators", ACTUATOR_ID, ACTUATOR_GROUP),
    )
    for section_name, id_field, group_field in io_profiles:
        section = dict(registry[section_name])
        section["group_required"] = (
            CONTROLLER_UUID,
            CONTROLLER_KEY,
            ENABLED,
            ROWS,
        )
        section["row_required"] = (id_field, group_field)
        scoped[section_name] = section

    return scoped

def verify_registry_layout(config, report):
    """
    Fast, additive structural pre-check driven by CONFIG_SCHEMA_REGISTRY:
    section presence (required_table), section kind (singleton | table |
    grouped_rows) and minimal required field sets. Runs BEFORE the deep
    declarative validation - cheap early rejection of malformed configs.
    Fully data-driven via functools.partial composition.
    """
    registry = build_scope_config_registry(config)
    for section_name in sorted(registry.keys()):
        schema = registry[section_name]
        if section_name not in config:
            if schema["required_table"]:
                add_error(report, section_name, "registry: missing required section")
            continue
        section = config[section_name]
        if not isinstance(section, list):
            add_error(report, section_name, "registry: section must be a list")
            continue
        kind = schema["kind"]
        allow_empty = bool(schema.get("allow_empty"))
        if kind == "singleton" and len(section) != 1 and not (
                allow_empty and len(section) == 0):
            add_error(report, section_name,
                      "registry: singleton section has " + str(len(section)) + " entries")
        for i, row in enumerate(section):
            row_path = index_path(section_name, i)
            if not isinstance(row, dict):
                add_error(report, row_path, "registry: entry must be an object")
                continue
            if kind == "grouped_rows":
                check_group = partial(check_required_keys, schema["group_required"])
                check_group(row, row_path, report)
                for j, io_row in enumerate(row.get("rows", [])):
                    check_row = partial(check_required_keys, schema["row_required"])
                    check_row(io_row, index_path(join_path(row_path, "rows"), j), report)
            else:
                check_required = partial(check_required_keys, schema.get("required", ()))
                check_required(row, row_path, report)


def _collect_representation_stats(config, runtime_config, report, representation):
    report["stats"]["schema_version"] = _compat_get_config_version(config)
    report["stats"]["scope"] = _compat_get_config_scope(config)
    report["stats"]["representation"] = representation
    report["stats"]["source_io_layout"] = (
        _compat_get_config_io_layout(config)
    )
    report["stats"]["runtime_io_layout"] = (
        _compat_get_config_io_layout(runtime_config)
    )
    report["stats"]["storage_io_layout"] = (
        _compat_get_expected_storage_layout(config) or "unknown"
    )


def verify_config_data(config, **kwargs):
    """
    Verify a storage document, a grouped runtime snapshot, or either form.

    ``representation`` may be ``storage``, ``runtime`` or ``auto``. The
    backwards-compatible default ``auto`` accepts both valid representations,
    while file/transport trust boundaries use ``storage`` explicitly.
    """
    representation = str(kwargs.get("representation") or "auto").strip().lower()
    report = new_report()
    try:
        if not isinstance(config, dict):
            add_error(report, "", "config root must be an object, got " + type_name(config))
            return finalize_report(report)

        verify_meta(config, report)
        verify_scope_metadata(config, report)
        if (
            _compat_get_config_version(config) == LEGACY_VERSION
            and not kwargs.get("allow_legacy_v5", False)
        ):
            add_error(
                report,
                "meta[0].version",
                "legacy V5 requires explicit import_legacy_v5_config",
            )
            return finalize_report(report)
        contract = _compat_inspect_config_contract(
            config,
            representation=representation,
        )
        _append_contract_errors(report, contract)
        if report["errors"]:
            report["stats"]["schema_version"] = (
                _compat_get_config_version(config)
            )
            report["stats"]["scope"] = _compat_get_config_scope(config)
            report["stats"]["representation"] = representation
            report["stats"]["source_io_layout"] = contract.get("source_io_layout")
            return finalize_report(report)

        runtime_config = _compat_normalize_config_for_runtime(
            config,
            representation=representation,
        )
        verify_registry_layout(runtime_config, report)
        verify_sections(runtime_config, report)
        check_uniqueness(runtime_config, report)
        verify_routing_ownership(runtime_config, report)
        verify_execution_ownership(runtime_config, report)
        verify_references(runtime_config, report)
        collect_stats(runtime_config, report)
        _collect_representation_stats(
            config,
            runtime_config,
            report,
            representation,
        )
    except ValueError as exc:
        add_error(report, "sensors/actuators", str(exc))
    except Exception as exc:
        LOGGER.exception("verification pipeline failed")
        add_error(report, "", "internal verification error: " + str(exc))
    return finalize_report(report)


def verify_storage_config_data(config):
    """Strictly verify the version-defined file/transport representation."""
    return verify_config_data(config, representation="storage")


def verify_runtime_config_data(config):
    """Strictly verify the canonical grouped runtime representation."""
    return verify_config_data(config, representation="runtime")


# =============================================================================
# CODEBASE - FUNCTIONS - PART 6: DETERMINISTIC SERIALIZATION & TRANSPORT
# =============================================================================

def json_ready(value):
    """Return a deterministic JSON-compatible copy of a contract value."""
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def canonical_text(value):
    """Return the single deterministic JSON representation."""
    return canonical_dumps(json_ready(value))


def canonical_bytes(value):
    """Return canonical UTF-8 bytes for transport and persistence."""
    return canonical_text(value).encode(CONTEXT["options"]["encoding"])


def decode_json_bytes(payload, **kwargs):
    """Decode bounded UTF-8 JSON bytes and optionally require an object root."""
    maximum = max(1, int(kwargs.get("maximum", 4 * 1024 * 1024)))
    raw = bytes(payload)
    if len(raw) > maximum:
        raise ValueError("JSON payload exceeds the configured limit")
    value = json.loads(raw.decode(CONTEXT["options"]["encoding"]))
    if kwargs.get("require_object", False) and not isinstance(value, dict):
        raise ValueError("JSON payload root must be an object")
    return value


def fingerprint(value):
    """Return the normative lowercase raw SHA-256 hex fingerprint."""
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def text_fingerprint(value):
    """Return a raw SHA-256 checksum for exact UTF-8 text content."""
    text = str(value)
    return hashlib.sha256(text.encode(CONTEXT["options"]["encoding"])).hexdigest()


def is_fingerprint(value):
    """Return whether a value is exactly one lowercase raw SHA-256 hex string."""
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def canonical_dumps(data):
    """
    Deterministic, byte-stable JSON encoding: sorted keys, fixed separators,
    UTF-8 pass-through. Identical data always yields identical bytes -
    safe for hashing, diffing, MQTT transport and DB persistence.
    """
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def normalize_config_for_runtime(config, **kwargs):
    """Public central adapter to the grouped runtime representation."""
    return _compat_normalize_config_for_runtime(config, **kwargs)


def project_config_for_storage(config, **kwargs):
    """Public central adapter to the version-defined storage representation."""
    return _compat_project_config_for_storage(config, **kwargs)


def config_fingerprint(config):
    """SHA-256 fingerprint of the version-defined canonical storage form."""
    canonical_config = _compat_canonicalize_config_for_fingerprint(config)
    return fingerprint(canonical_config)


def deep_copy_config(config):
    """Isolated deep copy for snapshot isolation between threads."""
    return copy.deepcopy(config)


def load_config_file(file_path):
    """Load strict storage JSON and return the proven grouped runtime snapshot."""
    report = new_report()
    try:
        handle = open(file_path, "r", encoding=CONTEXT["options"]["encoding"])
        try:
            config = json.load(handle)
        finally:
            handle.close()
    except (OSError, ValueError) as exc:
        add_error(report, "", "failed to read/parse " + str(file_path) + ": " + str(exc))
        return None, finalize_report(report)

    report = verify_storage_config_data(config)
    if report["status"] != "OK":
        return None, report
    runtime_config = normalize_config_for_runtime(config, representation="storage")
    report["stats"]["fingerprint"] = config_fingerprint(config)
    LOGGER.info("config loaded and verified: %s", file_path)
    return runtime_config, report


def save_config_file(config, file_path):
    """Verify either representation and persist the defined storage form."""
    try:
        runtime_config = normalize_config_for_runtime(config, representation="auto")
    except Exception as exc:
        report = new_report()
        add_error(report, "", "failed to normalize config: " + str(exc))
        return finalize_report(report)

    report = verify_runtime_config_data(runtime_config)
    if report["status"] != "OK":
        LOGGER.error("refusing to persist invalid config: %s", file_path)
        return report
    storage_config = project_config_for_storage(runtime_config)
    storage_report = verify_storage_config_data(storage_config)
    if storage_report["status"] != "OK":
        LOGGER.error("refusing to persist invalid storage projection: %s", file_path)
        return storage_report
    try:
        target = os.path.abspath(str(file_path))
        target_dir = os.path.dirname(target) or os.curdir
        file_descriptor, temporary_path = tempfile.mkstemp(
            prefix="." + os.path.basename(target) + ".",
            suffix=".tmp", dir=target_dir)
        try:
            handle = os.fdopen(
                file_descriptor, "w", encoding=CONTEXT["options"]["encoding"])
            file_descriptor = None
            try:
                handle.write(canonical_dumps(storage_config))
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                handle.close()
            os.replace(temporary_path, target)
            temporary_path = None
            if hasattr(os, "O_DIRECTORY"):
                directory_fd = os.open(target_dir, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            if file_descriptor is not None:
                os.close(file_descriptor)
            if temporary_path is not None and os.path.exists(temporary_path):
                os.unlink(temporary_path)
        storage_report["stats"]["fingerprint"] = config_fingerprint(storage_config)
        LOGGER.info("config verified and persisted: %s", file_path)
    except OSError as exc:
        add_error(storage_report, "",
                  "failed to write " + str(file_path) + ": " + str(exc))
        finalize_report(storage_report)
    return storage_report


# =============================================================================
# INTERFACE - EXTERNAL CALLABLE
# =============================================================================

def register_schema(section_name, spec):
    """Extend/override the central schema registry at composition time."""
    CONTEXT["schemas"][section_name] = spec


def register_reference_check(check_function):
    """Register an additional data-driven cross-reference check function."""
    CONTEXT["reference_checks"].append(check_function)


def format_report(report):
    """Render a verification report as a deterministic human-readable text."""
    lines = ["status: " + report["status"]]
    for key in sorted((report.get("stats") or {}).keys()):
        lines.append(str(key) + ": " + str(report["stats"][key]))
    for warning in report["warnings"]:
        lines.append("WARN  " + warning["path"] + " :: " + warning["warning"])
    for error in report["errors"]:
        lines.append("ERROR " + error["path"] + " :: " + error["error"])
    return "\n".join(lines)


def initialize_context():
    """
    Central initialisation: register the shared V5/V8/V9 runtime schema and
    both registries.
    Registries are copied into CONTEXT so runtime adaptations via
    register_section_layout()/register_c_type() never mutate the
    pristine module-level defaults (deterministic baseline preserved).
    """
    CONTEXT["schemas"] = build_config_schema()
    CONTEXT["config_schema_registry"] = dict(CONFIG_SCHEMA_REGISTRY)
    CONTEXT["c_type_registry"] = dict(C_TYPE_REGISTRY)
    LOGGER.debug("context initialised with %d section schemas", len(CONTEXT["schemas"]))


initialize_context()

# =============================================================================
# PUBLIC CONFIG-NORM BOUNDARY
# =============================================================================

CANONICAL_VERSION = 9
CROSS_VERSION = 8
LEGACY_VERSION = 5
RUNTIME_IO_LAYOUT = "grouped"
CONFIG_NORM_VERSION = {
    "node": CANONICAL_VERSION,
    "cross": CROSS_VERSION,
    "legacy_import": LEGACY_VERSION,
}
IO_SECTIONS = copy.deepcopy(_compat_IO_SECTIONS)
ENTITY_SECTIONS = copy.deepcopy(_compat_ENTITY_SECTIONS)
OWNERSHIP_FIELDS = frozenset(_compat_OWNERSHIP_FIELDS)


def get_config_version(config):
    """Return the declared integer config version or zero."""
    return _compat_get_config_version(config)


def get_expected_storage_layout(config):
    """Return the version-defined physical I/O layout."""
    return _compat_get_expected_storage_layout(config)


def get_io_layout(config, section_name):
    """Return the physical layout of one I/O section."""
    return _compat_get_io_layout(config, section_name)


def get_config_io_layout(config):
    """Return the unambiguous physical I/O layout of a config."""
    return _compat_get_config_io_layout(config)


def inspect_config_contract(config, **kwargs):
    """Return the canonical representation-contract report."""
    return _compat_inspect_config_contract(config, **kwargs)


def canonicalize_config_for_fingerprint(config):
    """Return the single version-defined fingerprint representation."""
    return _compat_canonicalize_config_for_fingerprint(config)


def resolve_io_entity_path(config, selector):
    """Resolve a stable I/O entity selector against a runtime snapshot."""
    return _compat_resolve_io_entity_path(config, selector)


def resolve_entity_path(config, selector):
    """Resolve a stable entity selector against a runtime snapshot."""
    return _compat_resolve_entity_path(config, selector)


def is_numeric_entity_path(path):
    """Return whether a legacy numeric path addresses an entity row."""
    return _compat_is_numeric_entity_path(path)


def normalize_patch_selector(config, path, **kwargs):
    """Normalize one legacy selector against an immutable snapshot."""
    return _compat_normalize_patch_selector(config, path, **kwargs)


def _raise_verification_error(report, label):
    if report.get("status") == "OK":
        return
    raise ValueError(label + ": " + format_report(report))


def ensure_v9_storage_config(config):
    """Return a verified detached canonical Node-V9 storage snapshot."""
    if get_config_version(config) != CANONICAL_VERSION:
        raise ValueError("Node storage snapshot must declare version 9")
    report = verify_storage_config_data(config)
    _raise_verification_error(report, "invalid Node-V9 config")
    return copy.deepcopy(config)


def ensure_v8_cross_storage_config(config):
    """Return a verified detached hardware-free Cross-V8 storage snapshot."""
    if get_config_version(config) != CROSS_VERSION or not is_cross_config(config):
        raise ValueError("Cross storage snapshot must declare scoped version 8")
    report = verify_storage_config_data(config)
    _raise_verification_error(report, "invalid Cross-V8 config")
    return copy.deepcopy(config)


def import_legacy_v5_config(config, **kwargs):
    """Explicitly validate and conservatively migrate V5 to global Node-V9."""
    if get_config_version(config) != LEGACY_VERSION:
        raise ValueError("legacy import requires a V5 snapshot")
    source_report = verify_config_data(
        config, representation="storage", allow_legacy_v5=True)
    _raise_verification_error(source_report, "invalid legacy V5 config")

    runtime = normalize_config_for_runtime(config, representation="storage")
    candidate = copy.deepcopy(runtime)
    meta = candidate.get("meta") or []
    if not meta or not isinstance(meta[0], dict):
        raise ValueError("legacy config requires one meta object")
    target_scope = str(
        kwargs.get("default_scope")
        or meta[0].get("scope")
        or "master_global"
    ).strip().lower()
    if target_scope not in ("master_global", "node_local"):
        raise ValueError(
            "legacy V5 import supports master_global or explicit node_local")
    node_id = str(meta[0].get("node_id") or "").strip()
    if target_scope == "node_local" and not node_id:
        raise ValueError("node_local legacy import requires meta.node_id")
    meta[0]["version"] = CANONICAL_VERSION
    meta[0]["scope"] = target_scope
    candidate["meta"] = meta

    for rule in candidate.get("rules") or []:
        rule["activation_state"] = "inactive"
        rule["deployment_status"] = "disabled_with_reason"
        rule["execution_epoch"] = 0
        rule["execution_owner"] = (
            node_id if target_scope == "node_local" else None)
        rule["execution_scope"] = (
            "node:" + node_id if target_scope == "node_local" else None)

    storage = project_config_for_storage(candidate)
    target_report = verify_storage_config_data(storage)
    _raise_verification_error(target_report, "invalid imported Node-V9 config")
    return storage


def inspect_cross_v8_contract(config, **kwargs):
    """Return the canonical Cross-V8 contract report."""
    report = inspect_config_contract(config, **kwargs)
    if report.get("status") != "OK":
        return report
    if get_config_version(config) != CROSS_VERSION:
        report["errors"].append({
            "path": "meta[0].version",
            "error": "cross config must declare version 8",
        })
    if not is_cross_config(config):
        report["errors"].append({
            "path": "meta[0].scope",
            "error": "cross config requires an explicit cross scope",
        })
    if report["errors"]:
        report["status"] = "FAILED"
    return report


def normalize_cross_v8_for_runtime(config, **kwargs):
    """Return the canonical grouped Cross-V8 runtime snapshot."""
    report = inspect_cross_v8_contract(config, representation=kwargs.get("representation", "storage"))
    if report.get("status") != "OK":
        raise ValueError(format_report(report))
    return normalize_config_for_runtime(config, **kwargs)


def project_cross_v8_for_storage(config):
    """Return the canonical flat Cross-V8 storage snapshot."""
    candidate = project_config_for_storage(config)
    report = inspect_cross_v8_contract(candidate, representation="storage")
    if report.get("status") != "OK":
        raise ValueError(format_report(report))
    return candidate




