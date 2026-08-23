# -*- coding: utf-8 -*-

# src/library/_json_formator.py

"""
=============================================================================
JSON FORMATTER - CANONICAL READABLE CONFIG FORMAT
=============================================================================

What it is:
    A pure-functional and deterministic JSON formatter for automation-system
    configuration files. It preserves JSON data, applies a domain-oriented
    section order, places identity fields first, moves parameters and comments
    towards the end, and sorts table records by explicit stable entity keys.

Why it is needed:
    - Configuration files remain human-readable and easy to review in Git.
    - Primary IDs and routing identities appear at predictable positions.
    - V5 grouped I/O tables and V8/V9 flat I/O tables are formatted correctly.
    - Table sorting no longer depends on whichever ``*_id`` key happens to be
      encountered first in an input dictionary.
    - Repeated formatting is idempotent: equal input produces byte-identical
      output with the same settings.
    - Operational list order is preserved for rules, actions, filter chains,
      tags, parameters and other sequence-sensitive arrays.
    - The fingerprint_safe mode reorders only object fields and sections;
      it never reorders record arrays and therefore preserves fingerprints
      whose canonicalization treats array order as significant.

How it works:
    1. JSON is loaded into plain standard-library ``dict`` and ``list`` values.
    2. Dictionaries are recursively reordered through explicit path profiles
       plus a deterministic generic fallback.
    3. Known table arrays are sorted through stable entity-key tuples.
       Unknown top-level record tables use conservative ID-key detection.
    4. Arrays of records are rendered as one compact JSON object per line.
    5. Output can be validated and atomically written in place.

Compatibility:
    - Standard library only.
    - Functional implementation; no classes, decorators, dataclasses, lambdas,
      type hints or ``SimpleNamespace``.
    - Compatible with the existing ``format_json``, ``sort_json_data`` and
      ``process_file`` entry points.
    - Designed for Python 3.14+ free-threaded execution. No mutable global
      runtime state is modified after module import.

=============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import argparse

import json
import os
import re
import sys
import tempfile
from functools import partial

from src.library import _generic_type_library as type_library


# =============================================================================
# CONSTANTS
# =============================================================================

FORMATTER_VERSION = "3.0.0"
DEFAULT_SORT_MODE = "canonical"
DEFAULT_INDENT_SIZE = 2

DEFAULT_REFERENCE_CANDIDATES = (
    "___device_state_data.json",
    "___device_state_data_.json",
    "device_state_data.json",
    "device_state_data_.json",
)

SORT_MODE_ALIASES = {
    "canonical": "canonical",
    "readable": "canonical",
    "domain": "canonical",
    "v9": "canonical",
    "default": "canonical",
    "fingerprint_safe": "fingerprint_safe",
    "canonical_preserve": "fingerprint_safe",
    "preserve_records": "fingerprint_safe",
    "safe": "fingerprint_safe",
    "numeric_pk": "numeric_pk",
    "numeric": "numeric_pk",
    "id": "numeric_pk",
    "ids": "numeric_pk",
    "pk": "numeric_pk",
    "primary_key": "numeric_pk",
    "reference": "reference",
    "ref": "reference",
    "alpha": "alpha",
    "alphabetic": "alpha",
    "alphabetisch": "alpha",
    "table_size": "table_size",
    "size": "table_size",
    "none": "none",
    "off": "none",
    "keep": "none",
    "preserve": "none",
}

TOP_LEVEL_ORDER = (
    "meta",
    "sites",
    "nodes",
    "local_node",
    "site_controller_map",
    "controllers",
    "virtual_controllers",
    "modbus_functions",
    "scaling_profiles",
    "sensor_types",
    "actuator_types",
    "sensors",
    "actuators",
    "process_modes",
    "process_values",
    "parameter_generic_templates",
    "schema_mappings",
    "pid_configs",
    "control_method_templates",
    "control_methods",
    "processes",
    "process_states",
    "filters",
    "timers",
    "rule_engine",
    "rules",
)

DEFAULT_TOP_SPACING = {
    "sites": 1,
    "controllers": 1,
    "sensors": 1,
    "process_modes": 1,
    "processes": 1,
    "process_states": 2,
    "filters": 1,
    "timers": 1,
    "rule_engine": 1,
    "rules": 1,
    "synthax_standard": 2,
    "automation": 1,
    "sensor_filters": 1,
    "process_state_mappings": 1,
    "control_method_templates": 1,
    "control_methods": 1,
    "process_timers": 1,
    "triggers": 1,
    "actions": 1,
    "transitions": 1,
    "dynamic_rule_engine": 1,
}

FIELD_ORDER_BY_PATH = {
    ("meta", "[]"): (
        "version",
        "schema",
        "scope",
        "node_id",
        "owner_node_id",
        "domain_id",
        "global_revision",
        "scope_revision",
        "local_config_revision",
        "fingerprint",
        "description",
    ),
    ("sites", "[]"): (
        "site_id",
        "site_name",
        "enabled",
        "description",
    ),
    ("nodes", "[]"): (
        "node_id",
        "site_id",
        "node_role",
        "enabled",
        "description",
    ),
    ("local_node", "[]"): (
        "node_id",
        "site_id",
        "active_controller_uuids",
        "active_controller_routes",
    ),
    ("site_controller_map", "[]"): (
        "local_controller_id",
        "node_id",
        "site_id",
        "controller_uuid",
        "controller_key",
        "route_key",
        "routing_role",
        "enabled",
        "description",
    ),
    ("controllers", "[]"): (
        "controller_id",
        "local_controller_id",
        "node_id",
        "site_id",
        "owner_node_id",
        "controller_uuid",
        "controller_key",
        "device_name",
        "enabled",
        "bus_type",
        "host",
        "port",
        "unit",
        "tags",
        "description",
    ),
    ("virtual_controllers", "[]"): (
        "virt_controller_id",
        "controller_id",
        "controller_uuid",
        "device_name",
        "virt_active",
        "host",
        "port",
        "description",
    ),
    ("modbus_functions", "[]"): (
        "modbus_function_id",
        "module_type",
        "modbus_function_type",
        "description",
    ),
    ("scaling_profiles", "[]"): (
        "profile_id",
        "name",
        "unit",
        "raw_min",
        "raw_max",
        "eng_min",
        "eng_max",
        "description",
    ),
    ("sensor_types", "[]"): (
        "sensor_types_id",
        "sensor_type",
        "description",
    ),
    ("actuator_types", "[]"): (
        "actuator_types_id",
        "actuator_type",
        "description",
    ),
    ("sensors", "[]"): (
        "local_controller_id",
        "sensor_id",
        "profile_id",
        "sensor_types_id",
        "node_id",
        "site_id",
        "controller_uuid",
        "controller_key",
        "enabled",
        "sensor_group",
        "address",
        "bus_type",
        "function_type",
        "rows",
        "parameters",
        "tags",
        "description",
    ),
    ("sensors", "[]", "rows", "[]"): (
        "sensor_id",
        "profile_id",
        "sensor_types_id",
        "sensor_group",
        "address",
        "bus_type",
        "function_type",
        "enabled",
        "parameters",
        "tags",
        "description",
    ),
    ("actuators", "[]"): (
        "local_controller_id",
        "actuator_id",
        "profile_id",
        "actuator_types_id",
        "node_id",
        "site_id",
        "controller_uuid",
        "controller_key",
        "enabled",
        "actuator_group",
        "address",
        "bus_type",
        "function_type",
        "rows",
        "parameters",
        "tags",
        "description",
    ),
    ("actuators", "[]", "rows", "[]"): (
        "actuator_id",
        "profile_id",
        "actuator_types_id",
        "actuator_group",
        "address",
        "bus_type",
        "function_type",
        "enabled",
        "parameters",
        "tags",
        "description",
    ),
    ("process_modes", "[]"): (
        "mode_id",
        "mode",
        "description",
    ),
    ("process_values", "[]"): (
        "value_id",
        "state_value",
        "description",
    ),
    ("parameter_generic_templates", "[]"): (
        "schema_id",
        "state_type",
        "description",
    ),
    ("schema_mappings", "[]"): (
        "mapping_id",
        "schema_id",
        "description",
    ),
    ("pid_configs", "[]"): (
        "pid_config_id",
        "pid_parameters",
        "description",
    ),
    ("control_method_templates", "[]"): (
        "template_id",
        "template_name",
        "type",
        "expr_id",
        "parameters",
        "template_description",
        "description",
    ),
    ("control_methods", "[]"): (
        "method_id",
        "template_id",
        "type",
        "args",
        "parameters",
        "description",
    ),
    ("processes", "[]"): (
        "p_id",
        "owner_node_id",
        "execution_epoch",
        "process_name",
        "process_location",
        "enabled",
        "parameters",
        "description",
    ),
    ("process_states", "[]"): (
        "state_id",
        "p_id",
        "value_id",
        "mode_id",
        "mapping_id",
        "owner_node_id",
        "execution_epoch",
        "enabled",
        "io_refs",
        "control_strategy",
        "setpoint_source",
        "parameters",
        "tags",
        "description",
    ),
    ("filters", "[]"): (
        "filter_id",
        "sensor_refs",
        "enabled",
        "chain",
        "parameters",
        "description",
    ),
    ("timers", "[]"): (
        "timer_id",
        "enabled",
        "duration_s",
        "interval_s",
        "at",
        "cron",
        "parameters",
        "description",
    ),
    ("rule_engine", "[]"): (
        "engine_id",
        "required_mode_id",
        "enabled",
        "writable_fields",
        "sp_tolerance",
        "defaults",
        "runtime",
        "safety",
        "parameters",
        "debug",
        "description",
    ),
    ("rules", "[]"): (
        "rule_id",
        "engine_id",
        "owner_node_id",
        "execution_owner",
        "execution_scope",
        "execution_epoch",
        "deployment_status",
        "activation_state",
        "group",
        "priority",
        "name",
        "enabled",
        "when",
        "until",
        "do",
        "else_do",
        "latch",
        "parameters",
        "tags",
        "description",
    ),
}

FIELD_ORDER_BY_PARENT_KEY = {
    "active_controller_routes": (
        "local_controller_id",
        "node_id",
        "site_id",
        "controller_uuid",
        "controller_key",
        "route_key",
        "routing_role",
        "enabled",
        "description",
    ),
    "io_refs": (
        "sensors",
        "actuators",
    ),
    "sensor_refs": (
        "sensor_id",
        "filter_id",
        "profile_id",
        "controller_uuid",
        "node_id",
        "site_id",
        "parameters",
        "description",
    ),
    "sensors": (
        "sensor_id",
        "filter_id",
        "profile_id",
        "controller_uuid",
        "node_id",
        "site_id",
        "parameters",
        "description",
    ),
    "actuators": (
        "actuator_id",
        "profile_id",
        "controller_uuid",
        "node_id",
        "site_id",
        "parameters",
        "description",
    ),
    "control_strategy": (
        "pid_config_id",
        "method_id",
        "template_id",
        "type",
        "sample_time",
        "dt_max_s",
        "out_min",
        "out_max",
        "error_scale",
        "anti_windup",
        "parameters",
        "description",
    ),
    "pid_parameters": (
        "kp",
        "ki",
        "kd",
    ),
    "when": (
        "state_id",
        "sensor_id",
        "actuator_id",
        "timer_id",
        "filter_id",
        "controller_uuid",
        "src",
        "field",
        "op",
        "value",
        "cooldown_s",
        "parameters",
        "description",
    ),
    "until": (
        "state_id",
        "sensor_id",
        "actuator_id",
        "timer_id",
        "filter_id",
        "controller_uuid",
        "src",
        "field",
        "op",
        "value",
        "cooldown_s",
        "parameters",
        "description",
    ),
    "do": (
        "state_ids",
        "p_ids",
        "value_id",
        "actuator_id",
        "controller_uuid",
        "parameters",
        "notify",
        "description",
    ),
    "else_do": (
        "state_ids",
        "p_ids",
        "value_id",
        "actuator_id",
        "controller_uuid",
        "parameters",
        "notify",
        "description",
    ),
    "defaults": (
        "eval",
        "exec",
        "priority",
    ),
    "runtime": (
        "threading",
        "tick_ms",
        "snapshot_isolation",
        "identity_model",
        "routing_model",
        "remote_signal_refs",
        "max_signal_age_s",
        "cross_scope_exec",
        "cross_worker_exec",
        "coordinator_min_priority",
        "parameters",
        "description",
    ),
    "safety": (
        "max_output_delta_per_step",
        "on_missing_sensor",
        "on_invalid_pid",
        "parameters",
        "description",
    ),
    "debug": (
        "control",
        "pid",
        "pid_state_ids",
        "pid_interval_s",
        "pid_saturation_interval_s",
        "log",
        "parameters",
        "description",
    ),
    "on_missing_sensor": (
        "value_id",
        "mode_id",
        "parameters",
        "description",
    ),
    "on_invalid_pid": (
        "value_id",
        "mode_id",
        "parameters",
        "description",
    ),
}

TABLE_SORT_KEYS_BY_PATH = {
    ("meta",): ("version", "schema", "scope", "node_id", "owner_node_id", "domain_id"),
    ("sites",): ("site_id",),
    ("nodes",): ("node_id",),
    ("local_node",): ("node_id",),
    ("site_controller_map",): ("node_id", "local_controller_id", "controller_uuid"),
    ("controllers",): ("controller_id", "controller_uuid"),
    ("virtual_controllers",): ("virt_controller_id", "controller_uuid"),
    ("modbus_functions",): ("modbus_function_id",),
    ("scaling_profiles",): ("profile_id",),
    ("sensor_types",): ("sensor_types_id",),
    ("actuator_types",): ("actuator_types_id",),
    ("sensors",): ("node_id", "local_controller_id", "controller_uuid", "sensor_id"),
    ("sensors", "[]", "rows"): ("sensor_id",),
    ("actuators",): ("node_id", "local_controller_id", "controller_uuid", "actuator_id"),
    ("actuators", "[]", "rows"): ("actuator_id",),
    ("process_modes",): ("mode_id",),
    ("process_values",): ("value_id",),
    ("parameter_generic_templates",): ("schema_id",),
    ("schema_mappings",): ("mapping_id",),
    ("pid_configs",): ("pid_config_id",),
    ("control_method_templates",): ("template_id",),
    ("control_methods",): ("method_id",),
    ("processes",): ("p_id",),
    ("process_states",): ("state_id",),
    ("filters",): ("filter_id",),
    ("timers",): ("timer_id",),
    ("rule_engine",): ("engine_id",),
    ("rules",): ("rule_id",),
}

ORDER_SENSITIVE_ARRAY_KEYS = frozenset(
    (
        "when",
        "until",
        "do",
        "else_do",
        "chain",
        "parameters",
        "args",
        "tags",
        "log",
        "writable_fields",
        "active_controller_uuids",
        "state_ids",
        "p_ids",
        "pid_state_ids",
        "rule_sets",
        "automation_process_state_mappings",
        "transitions",
        "actions",
        "triggers",
    )
)

LATE_FIELD_KEYS = frozenset(
    (
        "parameters",
        "args",
        "description",
        "template_description",
        "comment",
        "comments",
        "note",
        "notes",
        "tags",
        "debug",
        "metadata",
    )
)

IDENTITY_KEYS = frozenset(
    (
        "controller_uuid",
        "message_id",
        "operation_id",
        "entity_key",
        "route_key",
        "node_id",
        "site_id",
        "owner_node_id",
        "execution_owner",
        "execution_scope",
        "domain_id",
        "fingerprint",
    )
)

STATUS_KEYS = frozenset(
    (
        "enabled",
        "active",
        "activation_state",
        "deployment_status",
        "routing_role",
        "node_role",
        "scope",
        "type",
        "mode",
        "state_value",
        "name",
        "controller_key",
        "device_name",
    )
)

GENERIC_PRIMARY_KEY_PRIORITY = (
    "id",
    "state_id",
    "rule_id",
    "controller_id",
    "sensor_id",
    "actuator_id",
    "p_id",
    "filter_id",
    "timer_id",
    "engine_id",
    "method_id",
    "template_id",
    "mapping_id",
    "schema_id",
    "profile_id",
    "pid_config_id",
    "mode_id",
    "value_id",
    "node_id",
    "site_id",
)

INTEGER_STRING_PATTERN = re.compile(r"^[+-]?\d+$")


# =============================================================================
# CONTEXT MANAGEMENT
# =============================================================================


def build_format_context(indent_size):
    return {
        "indent_size": int(indent_size),
        "indent_str": " " * int(indent_size),
        "dump_inline": partial(json.dumps, ensure_ascii=False, separators=(",", ": ")),
        "top_spacing": dict(DEFAULT_TOP_SPACING),
    }


# =============================================================================
# HELPER FUNCTIONS - BASIC JSON OPERATIONS
# =============================================================================


def _json_dumps_compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_dumps_canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256_text(text):
    return type_library.text_fingerprint(text)


def _load_json_from_text(json_text):
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        repaired = json_repair(json_text)
        return json.loads(repaired)


def _load_json(filename):
    with open(filename, "r", encoding="utf-8") as handle:
        return _load_json_from_text(handle.read())


def json_repair(json_text):
    """Best-effort repair of common non-destructive JSON syntax issues."""
    repaired = re.sub(r",\s*}", "}", json_text)
    repaired = re.sub(r",\s*]", "]", repaired)

    def _quote_replacer(match):
        return '"' + match.group(1) + '"'

    repaired = re.sub(r"'([^']*)'", _quote_replacer, repaired)
    repaired = re.sub(r'([,\{\s])(\w+):\s*', r'\1"\2": ', repaired)
    repaired = re.sub(r'""([^\"]+)""', r'"\1"', repaired)
    repaired = re.sub(r"//.*$", "", repaired, flags=re.MULTILINE)
    repaired = re.sub(r"/\*.*?\*/", "", repaired, flags=re.DOTALL)
    return repaired


# =============================================================================
# HELPER FUNCTIONS - PATH AND KEY CLASSIFICATION
# =============================================================================


def _path_to_text(path):
    if not path:
        return "<root>"
    parts = []
    for part in path:
        text = str(part)
        if text == "[]":
            parts.append("[]")
            continue
        if not parts:
            parts.append(text)
            continue
        parts.append("." + text)
    return "".join(parts)


def _is_id_key(key):
    if not isinstance(key, str):
        return False
    text = key.strip().lower()
    if not text:
        return False
    return text == "id" or text.endswith("_id") or text.endswith("_ids")


def _is_uuid_key(key):
    if not isinstance(key, str):
        return False
    return key.strip().lower().endswith("_uuid")


def _field_category(key):
    text = str(key).strip().lower()
    if _is_id_key(text):
        return 10
    if _is_uuid_key(text) or text in IDENTITY_KEYS:
        return 20
    if text in STATUS_KEYS:
        return 30
    if text.endswith("_ref") or text.endswith("_refs"):
        return 50
    if text in ("when", "until", "do", "else_do", "io_refs", "control_strategy"):
        return 55
    if text in ("parameters", "args"):
        return 80
    if text in ("debug", "log", "tags", "metadata"):
        return 90
    if text in ("description", "template_description", "comment", "comments", "note", "notes"):
        return 100
    return 40


def _generic_field_sort_key(key):
    text = str(key)
    lowered = text.lower()
    return (_field_category(text), lowered, text)


def _preferred_order_for_path(path):
    explicit = FIELD_ORDER_BY_PATH.get(tuple(path))
    if explicit is not None:
        return explicit

    parent_key = None
    if path:
        parent_key = str(path[-1])
        if parent_key == "[]" and len(path) >= 2:
            parent_key = str(path[-2])

    if parent_key is not None:
        parent_order = FIELD_ORDER_BY_PARENT_KEY.get(parent_key)
        if parent_order is not None:
            return parent_order

    return tuple()


def _reorder_mapping(mapping, preferred_order):
    result = {}
    emitted = set()

    for key in preferred_order:
        if key in mapping:
            result[key] = mapping[key]
            emitted.add(key)

    remaining = [key for key in mapping.keys() if key not in emitted]
    remaining.sort(key=_generic_field_sort_key)

    for key in remaining:
        result[key] = mapping[key]

    return result


def _reorder_top_level(mapping):
    result = {}
    emitted = set()

    for key in TOP_LEVEL_ORDER:
        if key in mapping:
            result[key] = mapping[key]
            emitted.add(key)

    remaining = [key for key in mapping.keys() if key not in emitted]
    remaining.sort(key=partial(str.lower))

    for key in remaining:
        result[key] = mapping[key]

    return result


# =============================================================================
# HELPER FUNCTIONS - TABLE IDENTITY AND SORTING
# =============================================================================


def _convert_sortable_scalar(value):
    if value is None:
        return (3, "")
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, int):
        return (0, value)
    if isinstance(value, float):
        if value.is_integer():
            return (0, int(value))
        return (1, value)
    if isinstance(value, str):
        text = value.strip()
        if INTEGER_STRING_PATTERN.fullmatch(text):
            try:
                return (0, int(text))
            except ValueError:
                pass
        return (2, text.casefold(), text)
    return (4, _json_dumps_canonical(value))


def _table_record_sort_key(record, key_fields, original_index):
    values = []
    for key in key_fields:
        values.append(_convert_sortable_scalar(record.get(key)))
    values.append((5, original_index))
    return tuple(values)


def _all_records_have_key(records, key):
    for record in records:
        if not isinstance(record, dict):
            return False
        if key not in record:
            return False
        if isinstance(record.get(key), (dict, list)):
            return False
    return True


def _singular_section_name(section):
    text = str(section).strip().lower()
    if text.endswith("ies") and len(text) > 3:
        return text[:-3] + "y"
    if text.endswith("ses") and len(text) > 3:
        return text[:-2]
    if text.endswith("s") and len(text) > 1:
        return text[:-1]
    return text


def _find_generic_table_key(records, path):
    if not records or not all(isinstance(record, dict) for record in records):
        return tuple()

    common_keys = set(records[0].keys())
    for record in records[1:]:
        common_keys.intersection_update(record.keys())

    candidates = []
    for key in common_keys:
        if not _is_id_key(key):
            continue
        if not _all_records_have_key(records, key):
            continue
        candidates.append(str(key))

    if not candidates:
        return tuple()

    section = path[0] if path else ""
    singular_candidate = _singular_section_name(section) + "_id"

    def _candidate_score(key):
        if key == singular_candidate:
            return (0, key)
        if key == "id":
            return (1, key)
        if key in GENERIC_PRIMARY_KEY_PRIORITY:
            return (2, GENERIC_PRIMARY_KEY_PRIORITY.index(key), key)
        return (3, len(key), key)

    candidates.sort(key=_candidate_score)
    return (candidates[0],)


def _table_sort_keys(path, records):
    explicit = TABLE_SORT_KEYS_BY_PATH.get(tuple(path))
    if explicit is not None:
        usable = []
        for key in explicit:
            if any(key in record for record in records if isinstance(record, dict)):
                usable.append(key)
        return tuple(usable)

    if len(path) == 1:
        return _find_generic_table_key(records, path)

    return tuple()


def _is_order_sensitive_array(path):
    if not path:
        return False
    return str(path[-1]) in ORDER_SENSITIVE_ARRAY_KEYS


def _sort_record_table(records, path, statistics):
    if not records or not all(isinstance(record, dict) for record in records):
        return records

    if _is_order_sensitive_array(path):
        statistics["order_sensitive_lists_preserved"] += 1
        return records

    key_fields = _table_sort_keys(path, records)
    if not key_fields:
        statistics["record_tables_without_identity"] += 1
        return records

    indexed = list(enumerate(records))

    def _sorter(entry):
        index, record = entry
        return _table_record_sort_key(record, key_fields, index)

    sorted_indexed = sorted(indexed, key=_sorter)
    sorted_records = [record for _, record in sorted_indexed]

    statistics["record_tables_checked"] += 1
    if sorted_records == records:
        statistics["record_tables_already_sorted"] += 1
        return records

    statistics["record_tables_sorted"] += 1
    statistics["records_sorted"] += len(records)
    statistics["sorted_paths"].append(
        {
            "path": _path_to_text(path),
            "keys": list(key_fields),
            "count": len(records),
        }
    )
    return sorted_records


# =============================================================================
# CANONICALIZATION
# =============================================================================


def _new_canonical_statistics():
    return {
        "formatter_version": FORMATTER_VERSION,
        "dicts_checked": 0,
        "dicts_reordered": 0,
        "lists_checked": 0,
        "record_tables_checked": 0,
        "record_tables_sorted": 0,
        "record_tables_already_sorted": 0,
        "record_tables_without_identity": 0,
        "order_sensitive_lists_preserved": 0,
        "records_sorted": 0,
        "sorted_paths": [],
    }


def _canonicalize_value(value, path, statistics, reorder_fields, sort_tables):
    if isinstance(value, dict):
        statistics["dicts_checked"] += 1
        original_keys = list(value.keys())

        if not path and reorder_fields:
            ordered = _reorder_top_level(value)
        elif reorder_fields:
            preferred = _preferred_order_for_path(path)
            ordered = _reorder_mapping(value, preferred)
        else:
            ordered = dict(value)

        if list(ordered.keys()) != original_keys:
            statistics["dicts_reordered"] += 1

        result = {}
        for key, child in ordered.items():
            result[key] = _canonicalize_value(
                child,
                path + (str(key),),
                statistics,
                reorder_fields,
                sort_tables,
            )
        return result

    if isinstance(value, list):
        statistics["lists_checked"] += 1
        processed = []
        for child in value:
            processed.append(
                _canonicalize_value(
                    child,
                    path + ("[]",),
                    statistics,
                    reorder_fields,
                    sort_tables,
                )
            )

        if sort_tables:
            processed = _sort_record_table(processed, path, statistics)
        return processed

    return value


def canonicalize_json_data(data, reorder_fields=True, sort_tables=True):
    statistics = _new_canonical_statistics()
    original_signature = _json_dumps_compact(data)
    canonical = _canonicalize_value(
        data,
        tuple(),
        statistics,
        bool(reorder_fields),
        bool(sort_tables),
    )
    canonical_signature = _json_dumps_compact(canonical)
    statistics["already_canonical"] = original_signature == canonical_signature
    statistics["canonicalization_applied"] = original_signature != canonical_signature
    statistics["canonical_sha256"] = _sha256_text(_json_dumps_canonical(canonical))
    return canonical, statistics


# =============================================================================
# LEGACY-COMPATIBLE SORT MODES
# =============================================================================


def normalize_sort_mode(sort_mode):
    if sort_mode is None:
        return DEFAULT_SORT_MODE

    key = str(sort_mode).strip().lower()
    if key in SORT_MODE_ALIASES:
        return SORT_MODE_ALIASES[key]

    allowed = ", ".join(sorted(set(SORT_MODE_ALIASES.values())))
    raise argparse.ArgumentTypeError(
        "Invalid sort mode '" + str(sort_mode) + "'. Allowed: " + allowed
    )


def _sort_top_level_alpha(data):
    if not isinstance(data, dict):
        return data
    keys = sorted(data.keys(), key=partial(str.lower))
    return {key: data[key] for key in keys}


def _get_table_size(value):
    if isinstance(value, (list, dict, tuple, set, str, bytes)):
        return len(value)
    return 1


def _sort_top_level_table_size(data):
    if not isinstance(data, dict):
        return data

    keys = list(data.keys())
    index_map = {key: index for index, key in enumerate(keys)}

    def _sorter(key):
        return (-_get_table_size(data[key]), index_map[key], str(key))

    ordered_keys = sorted(keys, key=_sorter)
    return {key: data[key] for key in ordered_keys}


def _append_candidate_order(orders, path, new_keys):
    if path not in orders:
        orders[path] = []
    candidate = list(new_keys)
    if candidate not in orders[path]:
        orders[path].append(candidate)


def _collect_reference_key_orders(value, path, orders):
    if isinstance(value, dict):
        _append_candidate_order(orders, path, list(value.keys()))
        for key, child in value.items():
            _collect_reference_key_orders(child, path + (str(key),), orders)
        return

    if isinstance(value, list):
        wildcard_path = path + ("[]",)
        for child in value:
            if isinstance(child, (dict, list)):
                _collect_reference_key_orders(child, wildcard_path, orders)


def _choose_best_reference_order(current_keys, candidates):
    if not candidates:
        return None

    current_list = list(current_keys)
    current_set = set(current_list)
    best_candidate = None
    best_score = None

    for candidate in candidates:
        candidate_list = list(candidate)
        candidate_set = set(candidate_list)
        overlap = len(current_set.intersection(candidate_set))
        if overlap == 0:
            continue
        score = (
            1 if current_set == candidate_set else 0,
            overlap,
            -abs(len(candidate_list) - len(current_list)),
        )
        if best_score is None or score > best_score:
            best_score = score
            best_candidate = candidate_list

    return best_candidate


def _reorder_by_reference_orders(value, orders, path):
    if isinstance(value, dict):
        candidate = _choose_best_reference_order(value.keys(), orders.get(path, []))
        ordered = value
        if candidate:
            ordered = _reorder_mapping(value, candidate)

        result = {}
        for key, child in ordered.items():
            result[key] = _reorder_by_reference_orders(
                child,
                orders,
                path + (str(key),),
            )
        return result

    if isinstance(value, list):
        wildcard_path = path + ("[]",)
        result = []
        for child in value:
            if isinstance(child, (dict, list)):
                result.append(_reorder_by_reference_orders(child, orders, wildcard_path))
            else:
                result.append(child)
        return result

    return value


def _sort_top_level_reference(data, reference):
    if not isinstance(data, dict) or not isinstance(reference, dict):
        return data
    return _reorder_mapping(data, list(reference.keys()))


def sort_json_data(data, sort_mode=None, reference_data=None):
    mode = normalize_sort_mode(sort_mode)
    original_signature = _json_dumps_compact(data)

    if mode == "none":
        return data, {
            "formatter_version": FORMATTER_VERSION,
            "sort_mode": mode,
            "already_correctly_sorted": True,
            "sort_applied": False,
        }

    if mode in ("canonical", "fingerprint_safe", "numeric_pk"):
        canonical, statistics = canonicalize_json_data(
            data,
            reorder_fields=mode in ("canonical", "fingerprint_safe"),
            sort_tables=mode in ("canonical", "numeric_pk"),
        )
        new_signature = _json_dumps_compact(canonical)
        statistics.update(
            {
                "sort_mode": mode,
                "already_correctly_sorted": original_signature == new_signature,
                "sort_applied": original_signature != new_signature,
            }
        )
        return canonical, statistics

    if mode == "alpha":
        new_data = _sort_top_level_alpha(data)
    elif mode == "table_size":
        new_data = _sort_top_level_table_size(data)
    elif mode == "reference":
        if reference_data is None:
            raise ValueError(
                "Sort mode 'reference' requires a reference file. "
                "Use --sort-reference FILE."
            )
        top_sorted = _sort_top_level_reference(data, reference_data)
        orders = {}
        _collect_reference_key_orders(reference_data, tuple(), orders)
        new_data = _reorder_by_reference_orders(top_sorted, orders, tuple())
    else:
        raise ValueError("Unsupported sort mode: " + mode)

    new_signature = _json_dumps_compact(new_data)
    return new_data, {
        "formatter_version": FORMATTER_VERSION,
        "sort_mode": mode,
        "already_correctly_sorted": original_signature == new_signature,
        "sort_applied": original_signature != new_signature,
    }


# =============================================================================
# JSON FORMATTING
# =============================================================================


def _is_dynamic_rule_sets_path(path):
    return len(path) >= 2 and path[-1] == "rule_sets" and path[-2] == "dynamic_rule_engine"


def _is_automation_process_state_mappings_path(path):
    return bool(path) and path[-1] == "automation_process_state_mappings"


def object_to_oneline(obj, ctx):
    return ctx["dump_inline"](obj)


def format_value(value, level, is_last, path, ctx):
    if isinstance(value, dict):
        return format_dict(value, level, is_last, path, ctx)
    if isinstance(value, list):
        return format_array(value, level, is_last, path, ctx)
    suffix = "" if is_last else ","
    return json.dumps(value, ensure_ascii=False) + suffix


def format_dict(obj, level, is_last, path, ctx):
    if not obj:
        return "{}" + ("" if is_last else ",")

    indent = ctx["indent_str"] * level
    indent_next = ctx["indent_str"] * (level + 1)
    keys = list(obj.keys())
    lines = ["{"]

    for index, key in enumerate(keys):
        child = obj[key]
        is_last_key = index == len(keys) - 1
        key_json = json.dumps(key, ensure_ascii=False)

        if level == 0 and index > 0:
            gap = int(ctx["top_spacing"].get(str(key), 0))
            for _ in range(gap):
                lines.append("")

        if isinstance(child, (dict, list)):
            formatted = format_value(
                child,
                level + 1,
                is_last_key,
                path + [str(key)],
                ctx,
            )
            lines.append(indent_next + key_json + ": " + formatted)
        else:
            value_json = json.dumps(child, ensure_ascii=False)
            suffix = "" if is_last_key else ","
            lines.append(indent_next + key_json + ": " + value_json + suffix)

    suffix = "" if is_last else ","
    lines.append(indent + "}" + suffix)
    return "\n".join(lines)


def format_array(arr, level, is_last, path, ctx):
    if not arr:
        return "[]" + ("" if is_last else ",")

    indent = ctx["indent_str"] * level
    indent_next = ctx["indent_str"] * (level + 1)
    lines = ["["]

    all_dicts = all(isinstance(element, dict) for element in arr)
    expand_elements = (
        _is_dynamic_rule_sets_path(path)
        or _is_automation_process_state_mappings_path(path)
    )

    if all_dicts and not expand_elements:
        for index, element in enumerate(arr):
            suffix = "" if index == len(arr) - 1 else ","
            lines.append(indent_next + object_to_oneline(element, ctx) + suffix)
        suffix = "" if is_last else ","
        lines.append(indent + "]" + suffix)
        return "\n".join(lines)

    for index, element in enumerate(arr):
        is_last_element = index == len(arr) - 1
        formatted = format_value(
            element,
            level + 1,
            is_last_element,
            path + [str(index)],
            ctx,
        )
        lines.append(indent_next + formatted)

    suffix = "" if is_last else ","
    lines.append(indent + "]" + suffix)
    return "\n".join(lines)


def format_json(json_data, indent_size=DEFAULT_INDENT_SIZE):
    if isinstance(json_data, str):
        data = _load_json_from_text(json_data)
    else:
        data = json_data

    ctx = build_format_context(indent_size)
    return format_value(data, 0, True, [], ctx)


def canonical_format_json(json_data, indent_size=DEFAULT_INDENT_SIZE, sort_tables=True):
    if isinstance(json_data, str):
        data = _load_json_from_text(json_data)
    else:
        data = json_data

    canonical, statistics = canonicalize_json_data(
        data,
        reorder_fields=True,
        sort_tables=bool(sort_tables),
    )
    text = format_json(canonical, indent_size=indent_size)
    return text, statistics


# =============================================================================
# FILE OPERATIONS
# =============================================================================


def _atomic_write_file(filename, content):
    target = os.path.abspath(filename)
    target_dir = os.path.dirname(target) or "."
    target_base = os.path.basename(target)

    fd, tmp_path = tempfile.mkstemp(
        prefix=target_base + ".tmp.",
        dir=target_dir,
        text=True,
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            if not content.endswith("\n"):
                handle.write("\n")

        if os.path.exists(target):
            try:
                stat_info = os.stat(target)
                os.chmod(tmp_path, stat_info.st_mode)
            except Exception:
                pass

        os.replace(tmp_path, target)
        tmp_path = None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def find_reference_sort_file(input_file, explicit_file=None):
    if explicit_file:
        if os.path.isfile(explicit_file):
            return explicit_file
        return None

    input_abspath = os.path.abspath(input_file)
    input_dir = os.path.dirname(input_abspath) or "."
    input_base = os.path.basename(input_abspath)

    if input_base in DEFAULT_REFERENCE_CANDIDATES and os.path.isfile(input_abspath):
        return input_abspath

    candidates = []
    for name in DEFAULT_REFERENCE_CANDIDATES:
        candidates.append(os.path.join(input_dir, name))
        candidates.append(name)

    checked = set()
    for candidate in candidates:
        absolute = os.path.abspath(candidate)
        if absolute in checked:
            continue
        checked.add(absolute)
        if os.path.isfile(absolute):
            return absolute

    return None


def _find_default_test_file():
    for name in DEFAULT_REFERENCE_CANDIDATES:
        if os.path.isfile(name):
            return name
    return None


def _build_process_report(filename, target_file, sort_info, output_text):
    return {
        "formatter_version": FORMATTER_VERSION,
        "input_file": os.path.abspath(filename),
        "output_file": os.path.abspath(target_file) if target_file else None,
        "sort_mode": sort_info.get("sort_mode"),
        "already_correctly_sorted": bool(sort_info.get("already_correctly_sorted")),
        "sort_applied": bool(sort_info.get("sort_applied")),
        "output_sha256": _sha256_text(output_text + ("" if output_text.endswith("\n") else "\n")),
        "statistics": sort_info,
    }


def _print_status(sort_info, target_is_stdout_json):
    parts = [
        "Formatter: " + FORMATTER_VERSION,
        "Sort mode: " + str(sort_info.get("sort_mode")),
        "Already canonical: "
        + ("yes" if sort_info.get("already_correctly_sorted") else "no"),
    ]

    if "record_tables_checked" in sort_info:
        parts.append("Tables checked: " + str(sort_info["record_tables_checked"]))
    if "record_tables_sorted" in sort_info:
        parts.append("Tables sorted: " + str(sort_info["record_tables_sorted"]))
    if "dicts_reordered" in sort_info:
        parts.append("Objects reordered: " + str(sort_info["dicts_reordered"]))

    stream = sys.stderr if target_is_stdout_json else sys.stdout
    print(" | ".join(parts), file=stream)


def _process_option_defaults():
    return {
        "indent_size": DEFAULT_INDENT_SIZE,
        "snippet_automation": False,
        "validate": False,
        "inplace": False,
        "sort_mode": None,
        "sort_reference_file": None,
        "report_file": None,
        "check_only": False,
    }


def _build_process_options(args, kwargs):
    positional_names = (
        "indent_size",
        "snippet_automation",
        "validate",
        "inplace",
        "sort_mode",
        "sort_reference_file",
        "report_file",
        "check_only",
    )
    if len(args) > len(positional_names):
        raise TypeError("process_file received too many positional arguments")

    options = _process_option_defaults()
    for index, value in enumerate(args):
        options[positional_names[index]] = value

    unknown = [key for key in kwargs if key not in options]
    if unknown:
        raise TypeError("Unknown process_file option: " + str(unknown[0]))

    options.update(kwargs)
    return options


def _read_process_input(filename, snippet_automation):
    with open(filename, "r", encoding="utf-8") as handle:
        raw = handle.read()

    data = _load_json_from_text(raw)
    if not snippet_automation:
        return raw, data

    if not isinstance(data, dict):
        raise ValueError("Snippet mode expects a top-level JSON object.")

    return raw, {
        "automation_process_state_mappings": data.get(
            "automation_process_state_mappings"
        )
    }


def _load_process_reference(filename, mode, sort_reference_file):
    if mode != "reference":
        return None

    reference_path = find_reference_sort_file(
        filename,
        explicit_file=sort_reference_file,
    )
    if not reference_path:
        raise ValueError(
            "No reference file found for sort mode 'reference'. "
            "Use --sort-reference FILE."
        )
    return _load_json(reference_path)


def _resolve_process_target(filename, output_file, options):
    if options["inplace"]:
        return filename
    return output_file


def _normalized_text(text):
    if text.endswith("\n"):
        return text
    return text + "\n"


def _write_process_report(report_file, report):
    if not report_file:
        return
    _atomic_write_file(
        report_file,
        json.dumps(report, ensure_ascii=False, indent=2),
    )


def _write_process_output(filename, target_file, output_text, options):
    if not target_file:
        print(output_text)
        return

    same_path = os.path.abspath(target_file) == os.path.abspath(filename)
    if options["snippet_automation"] and same_path:
        raise ValueError(
            "In-place output is blocked in snippet mode because it would "
            "replace the complete source with a partial section."
        )
    _atomic_write_file(target_file, output_text)


def process_file(filename, output_file=None, *args, **kwargs):
    options = _build_process_options(args, kwargs)
    raw, data = _read_process_input(filename, options["snippet_automation"])
    mode = normalize_sort_mode(options["sort_mode"])
    reference_data = _load_process_reference(
        filename,
        mode,
        options["sort_reference_file"],
    )

    sorted_data, sort_info = sort_json_data(
        data,
        sort_mode=mode,
        reference_data=reference_data,
    )
    output_text = format_json(sorted_data, indent_size=options["indent_size"])
    if options["validate"]:
        json.loads(output_text)

    target_file = _resolve_process_target(filename, output_file, options)
    would_change = _normalized_text(raw) != _normalized_text(output_text)
    sort_info["would_change_file"] = would_change

    report = _build_process_report(filename, target_file, sort_info, output_text)
    if options["check_only"]:
        report["check_only"] = True
        report["check_passed"] = not would_change
    else:
        _write_process_output(filename, target_file, output_text, options)

    _write_process_report(options["report_file"], report)
    _print_status(
        sort_info,
        target_is_stdout_json=target_file is None and not options["check_only"],
    )
    return report


# =============================================================================
# COMMAND-LINE INTERFACE
# =============================================================================


def _add_cli_positional_arguments(parser):
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        help="Input JSON file.",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default=None,
        help="Optional output file. Without output, JSON is printed to stdout.",
    )


def _add_cli_write_arguments(parser):
    parser.add_argument(
        "--inplace",
        "-i",
        dest="inplace",
        action="store_true",
        help="Atomically replace the input file with the formatted result.",
    )
    parser.add_argument(
        "--indent",
        dest="indent",
        type=int,
        default=DEFAULT_INDENT_SIZE,
        help="Indent spaces per level. Default: 2.",
    )
    parser.add_argument(
        "--report",
        dest="report_file",
        default=None,
        help="Optional JSON report output path.",
    )


def _add_cli_validation_arguments(parser):
    parser.add_argument(
        "--validate",
        dest="validate",
        action="store_true",
        help="Re-parse formatted output before writing.",
    )
    parser.add_argument(
        "--check",
        dest="check_only",
        action="store_true",
        help="Do not write. Exit with status 3 when formatting would change the file.",
    )
    parser.add_argument(
        "--snippet-automation-process-state-mappings",
        dest="snippet_automation",
        action="store_true",
        help="Output only automation_process_state_mappings for inspection.",
    )


def _add_cli_sort_arguments(parser):
    parser.add_argument(
        "--sort",
        "-S",
        dest="sort_mode",
        type=normalize_sort_mode,
        default=DEFAULT_SORT_MODE,
        help=(
            "Sort/order mode: canonical, fingerprint_safe, numeric_pk, "
            "reference, alpha, table_size or none. Default: canonical."
        ),
    )
    parser.add_argument(
        "--sort-reference",
        dest="sort_reference_file",
        default=None,
        help="Reference JSON for --sort reference.",
    )


def _build_cli_parser():
    parser = argparse.ArgumentParser(
        prog=os.path.basename(sys.argv[0]),
        description=(
            "Deterministic automation-config JSON formatter with domain-oriented "
            "section order, ID-first records, optional entity-table sorting "
            "and a fingerprint-safe record-preserving mode."
        ),
    )
    _add_cli_positional_arguments(parser)
    _add_cli_write_arguments(parser)
    _add_cli_validation_arguments(parser)
    _add_cli_sort_arguments(parser)
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + FORMATTER_VERSION,
    )
    return parser


def _resolve_cli_input(parser, input_file):
    if input_file:
        return input_file

    default_file = _find_default_test_file()
    if default_file:
        return default_file

    parser.print_help()
    return None


def _validate_cli_output(input_file, args):
    if not args.inplace or not args.output:
        return True
    if os.path.abspath(args.output) == os.path.abspath(input_file):
        return True
    print("Error: use either --inplace or a separate output file, not both.")
    return False


def _run_cli_process(input_file, args):
    return process_file(
        input_file,
        args.output,
        indent_size=args.indent,
        snippet_automation=bool(args.snippet_automation),
        validate=bool(args.validate),
        inplace=bool(args.inplace),
        sort_mode=str(args.sort_mode),
        sort_reference_file=args.sort_reference_file,
        report_file=args.report_file,
        check_only=bool(args.check_only),
    )


def main():
    parser = _build_cli_parser()
    args = parser.parse_args()
    input_file = _resolve_cli_input(parser, args.input)
    if not input_file:
        return 1
    if not _validate_cli_output(input_file, args):
        return 2

    try:
        report = _run_cli_process(input_file, args)
    except FileNotFoundError:
        print("Error: file '" + str(input_file) + "' was not found.")
        return 1
    except Exception as exc:
        print("Error while processing: " + str(exc))
        return 1

    if args.check_only and not report.get("check_passed"):
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())


