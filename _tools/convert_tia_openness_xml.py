# -*- coding: utf-8 -*-

"""Convert TIA Portal Openness PLC tag XML into V9 S7 I/O rows.

The converter accepts absolute PLC tags, preserves the TIA tag name as the
V9 sensor/actuator group, validates every address with the runtime S7 parser,
and can either emit a mergeable fragment or append rows to one V9 storage
configuration. Ambiguous DB/M tags fail closed unless an explicit direction,
name pattern, or default direction resolves them.
"""

import argparse
import copy
import hashlib
import json
import os
import re
import sys
import tempfile
import xml.etree.ElementTree as element_tree


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library import _generic_type_library as type_library
from src.adapter.plc._tia_plc_adapter import parse_s7_address
from src.library._json_formator import canonical_format_json


MAX_XML_BYTES = 32 * 1024 * 1024

_RECORD_NAMES = frozenset({
    "member",
    "plctag",
    "swtagsplctag",
    "tag",
    "variable",
})

_ADDRESS_FIELDS = frozenset({
    "absoluteaddress",
    "address",
    "logicaladdress",
})

_TYPE_FIELDS = frozenset({
    "datatype",
    "datatypename",
    "type",
    "typename",
})

_NAME_FIELDS = frozenset({
    "name",
    "symbol",
    "symbolicname",
    "tagname",
})

_DIRECTION_FIELDS = frozenset({
    "access",
    "accessmode",
    "direction",
    "iotype",
    "usage",
})

_COMMENT_FIELDS = frozenset({
    "comment",
    "description",
    "remarks",
})

_TYPE_ALIASES = {
    "BIT": "BOOL",
    "BOOL": "BOOL",
    "BYTE": "BYTE",
    "CHAR": "CHAR",
    "DATE": "DATE",
    "DINT": "DINT",
    "DWORD": "DWORD",
    "INT": "INT",
    "LINT": "LINT",
    "LREAL": "LREAL",
    "LWORD": "LWORD",
    "REAL": "REAL",
    "SINT": "SINT",
    "TIME": "TIME",
    "TIMEOFDAY": "TOD",
    "TIME_OF_DAY": "TOD",
    "TOD": "TOD",
    "UDINT": "UDINT",
    "UINT": "UINT",
    "ULINT": "ULINT",
    "USINT": "USINT",
    "WORD": "WORD",
}

_ARRAY_BYTE_PATTERN = re.compile(
    r"^ARRAY\s*\[\s*(?P<lower>[0-9]+)\s*\.\.\s*"
    r"(?P<upper>[0-9]+)\s*\]\s*OF\s*BYTE$",
    re.IGNORECASE,
)

_STRING_PATTERN = re.compile(
    r"^(?P<kind>W?STRING)\s*(?:\[\s*(?P<size>[0-9]+)\s*\])?$",
    re.IGNORECASE,
)

_DB_VARIABLE_MARKER_PATTERN = re.compile(
    r"^(DB[0-9]+\.DB)[BWDL](?P<offset>[0-9]+)$",
    re.IGNORECASE,
)

_AREA_VARIABLE_MARKER_PATTERN = re.compile(
    r"^(?P<area>[MIEQA])[BWDL](?P<offset>[0-9]+)$",
    re.IGNORECASE,
)

_DEFAULT_SENSOR_PATTERN = (
    r"(^|[_.-])(sensor|input|in|read|feedback|actual|pv)([_.-]|$)"
)

_DEFAULT_ACTUATOR_PATTERN = (
    r"(^|[_.-])(actuator|output|out|write|command|cmd|setpoint|sp)"
    r"([_.-]|$)"
)


def _normalized_name(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _local_name(tag):
    text = str(tag or "")
    if "}" in text:
        text = text.rsplit("}", 1)[1]
    return text


def _text(value):
    return str(value or "").strip()


def _attribute_value(node, accepted_names):
    for key, value in sorted(node.attrib.items()):
        if _normalized_name(key) in accepted_names and _text(value):
            return _text(value)
    return ""


def _descendant_value(node, accepted_names):
    for child in node.iter():
        if child is node:
            continue
        if _normalized_name(_local_name(child.tag)) not in accepted_names:
            continue
        value = _text(child.text)
        if value:
            return value
        nested = " ".join(
            _text(item.text)
            for item in child.iter()
            if _text(item.text)
        ).strip()
        if nested:
            return nested
    return ""


def _field_value(node, accepted_names):
    return (
        _attribute_value(node, accepted_names)
        or _descendant_value(node, accepted_names)
    )


def _record_candidates(root):
    result = []
    for node in root.iter():
        name = _normalized_name(_local_name(node.tag))
        if name not in _RECORD_NAMES and not name.endswith("plctag"):
            continue
        address = _field_value(node, _ADDRESS_FIELDS)
        data_type = _field_value(node, _TYPE_FIELDS)
        if address and data_type:
            result.append(node)
    return result


def _load_xml(path):
    size = os.path.getsize(path)
    if size > MAX_XML_BYTES:
        raise ValueError("TIA XML exceeds {} bytes".format(MAX_XML_BYTES))
    with open(path, "rb") as handle:
        raw = handle.read()
    lowered = raw.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise ValueError("DTD and entity declarations are forbidden")
    try:
        return element_tree.fromstring(raw), raw
    except element_tree.ParseError as exc:
        raise ValueError("invalid TIA XML: {}".format(exc)) from exc


def _normalize_tia_type(value):
    raw = _text(value)
    compact = re.sub(r"\s+", "", raw).upper()
    alias = _TYPE_ALIASES.get(compact)
    if alias:
        return alias

    string_match = _STRING_PATTERN.fullmatch(raw)
    if string_match is not None:
        kind = string_match.group("kind").upper()
        size = int(string_match.group("size") or 254)
        return "{}[{}]".format(kind, size)

    array_match = _ARRAY_BYTE_PATTERN.fullmatch(raw)
    if array_match is not None:
        lower = int(array_match.group("lower"))
        upper = int(array_match.group("upper"))
        if upper < lower:
            raise ValueError("TIA byte array upper bound is below lower bound")
        return "BYTES[{}]".format((upper - lower) + 1)

    raise ValueError("unsupported TIA data type: {}".format(raw))


def _strip_variable_marker(base, data_type):
    if not (
        data_type.startswith("STRING[")
        or data_type.startswith("WSTRING[")
        or data_type.startswith("BYTES[")
    ):
        return base
    db_match = _DB_VARIABLE_MARKER_PATTERN.fullmatch(base)
    if db_match is not None:
        prefix = db_match.group(1)
        return "{}{}".format(prefix, db_match.group("offset"))
    area_match = _AREA_VARIABLE_MARKER_PATTERN.fullmatch(base)
    if area_match is not None:
        return "{}{}".format(
            area_match.group("area"),
            area_match.group("offset"),
        )
    return base


def _normalize_tia_address(address, data_type):
    base = re.sub(r"\s+", "", _text(address)).replace("%", "").upper()
    base = _strip_variable_marker(base, data_type)
    parsed = parse_s7_address("{}:{}".format(base, data_type))
    return parsed["canonical"]


def _direction_from_value(value):
    normalized = _normalized_name(value)
    if normalized in (
        "in",
        "input",
        "read",
        "readonly",
        "sensor",
        "status",
    ):
        return "sensor"
    if normalized in (
        "actuator",
        "command",
        "out",
        "output",
        "readwrite",
        "write",
        "writeonly",
    ):
        return "actuator"
    return ""


def _direction_from_annotation(comment):
    lowered = _text(comment).lower()
    sensor_tokens = ("[sensor]", "@sensor", "direction=sensor")
    actuator_tokens = ("[actuator]", "@actuator", "direction=actuator")
    if any(token in lowered for token in sensor_tokens):
        return "sensor"
    if any(token in lowered for token in actuator_tokens):
        return "actuator"
    return ""


def _direction_from_area(address):
    base = re.sub(r"\s+", "", _text(address)).replace("%", "").upper()
    if base.startswith(("I", "E")):
        return "sensor"
    if base.startswith(("Q", "A")):
        return "actuator"
    return ""


def _direction_from_patterns(name, sensor_pattern, actuator_pattern):
    sensor_match = bool(sensor_pattern.search(name))
    actuator_match = bool(actuator_pattern.search(name))
    if sensor_match and actuator_match:
        raise ValueError(
            "tag name matches sensor and actuator patterns: {}".format(name)
        )
    if sensor_match:
        return "sensor"
    if actuator_match:
        return "actuator"
    return ""


def _resolve_direction(record, options):
    explicit = _direction_from_value(record.get("direction"))
    annotated = _direction_from_annotation(record.get("comment"))
    area_direction = _direction_from_area(record.get("source_address"))
    patterned = _direction_from_patterns(
        record.get("name", ""),
        options["sensor_pattern"],
        options["actuator_pattern"],
    )
    selected = [
        value
        for value in (explicit, annotated, area_direction, patterned)
        if value
    ]
    if selected and any(value != selected[0] for value in selected[1:]):
        raise ValueError(
            "conflicting direction evidence for tag {}".format(
                record.get("name")
            )
        )
    if selected:
        return selected[0]
    default = options.get("default_direction")
    if default in ("sensor", "actuator", "skip"):
        return default
    raise ValueError(
        "ambiguous DB/M tag requires explicit direction: {}".format(
            record.get("name")
        )
    )


def _extract_record(node):
    name = _field_value(node, _NAME_FIELDS)
    address = _field_value(node, _ADDRESS_FIELDS)
    source_type = _field_value(node, _TYPE_FIELDS)
    if not name:
        name = "{}_{}".format(source_type, address)
    data_type = _normalize_tia_type(source_type)
    return {
        "name": name,
        "source_address": address,
        "source_type": source_type,
        "data_type": data_type,
        "address": _normalize_tia_address(address, data_type),
        "direction": _field_value(node, _DIRECTION_FIELDS),
        "comment": _field_value(node, _COMMENT_FIELDS),
    }


def _route_from_config(config, controller_uuid):
    controllers = [
        row
        for row in config.get("controllers") or []
        if isinstance(row, dict)
        and str(row.get("controller_uuid") or "") == controller_uuid
    ]
    if len(controllers) != 1:
        raise ValueError("controller_uuid must resolve to exactly one controller")
    controller = controllers[0]
    if str(controller.get("bus_type") or "") != "plc_s7":
        raise ValueError("selected V9 controller bus_type must be plc_s7")

    routes = [
        row
        for row in config.get("site_controller_map") or []
        if isinstance(row, dict)
        and str(row.get("controller_uuid") or "") == controller_uuid
    ]
    if len(routes) != 1:
        raise ValueError("controller_uuid must resolve to exactly one owner route")
    route = routes[0]
    return {
        "controller_uuid": controller_uuid,
        "controller_key": str(controller.get("controller_key") or ""),
        "local_controller_id": int(
            route.get("local_controller_id")
            or controller.get("controller_id")
        ),
        "node_id": str(route.get("node_id") or ""),
        "site_id": str(route.get("site_id") or ""),
    }


def _route_from_arguments(arguments):
    required = {
        "controller_uuid": arguments.controller_uuid,
        "controller_key": arguments.controller_key,
        "local_controller_id": arguments.local_controller_id,
        "node_id": arguments.node_id,
        "site_id": arguments.site_id,
    }
    missing = [
        key for key, value in required.items()
        if value in (None, "")
    ]
    if missing:
        raise ValueError(
            "fragment mode requires routing arguments: {}".format(
                ", ".join(sorted(missing))
            )
        )
    required["local_controller_id"] = int(required["local_controller_id"])
    return required


def _next_identity(config, section, id_field, controller_uuid, requested):
    if requested is not None:
        if int(requested) < 1:
            raise ValueError("{} must be positive".format(id_field))
        return int(requested)
    current = [
        int(row.get(id_field))
        for row in config.get(section) or []
        if isinstance(row, dict)
        and str(row.get("controller_uuid") or "") == controller_uuid
        and row.get(id_field) is not None
    ]
    return (max(current) + 1) if current else 1


def _make_row(record, route, identity, direction, profile_id=None):
    id_field = "sensor_id" if direction == "sensor" else "actuator_id"
    group_field = (
        "sensor_group" if direction == "sensor" else "actuator_group"
    )
    function_type = "read_s7" if direction == "sensor" else "write_s7"
    row = {
        "local_controller_id": int(route["local_controller_id"]),
        id_field: int(identity),
        "node_id": route["node_id"],
        "site_id": route["site_id"],
        "controller_uuid": route["controller_uuid"],
        "controller_key": route["controller_key"],
        "enabled": True,
        group_field: record["name"],
        "address": record["address"],
        "bus_type": "plc_s7",
        "function_type": function_type,
    }
    if profile_id is not None:
        row["profile_id"] = int(profile_id)
    return row


def _conversion_options(arguments):
    return {
        "sensor_pattern": re.compile(
            arguments.sensor_pattern,
            re.IGNORECASE,
        ),
        "actuator_pattern": re.compile(
            arguments.actuator_pattern,
            re.IGNORECASE,
        ),
        "default_direction": arguments.default_direction,
        "skip_invalid": bool(arguments.skip_invalid),
        "allow_duplicate_addresses": bool(
            arguments.allow_duplicate_addresses
        ),
    }


def convert_xml(xml_path, route, identity, options, profile_ids=None):
    root, raw = _load_xml(xml_path)
    candidates = _record_candidates(root)
    if not candidates:
        raise ValueError("TIA XML contains no absolute PLC tag records")

    records = []
    skipped = []
    for index, node in enumerate(candidates):
        try:
            record = _extract_record(node)
            direction = _resolve_direction(record, options)
            if direction == "skip":
                skipped.append({
                    "index": index,
                    "reason": "direction_skip",
                    "name": record.get("name"),
                })
                continue
            record["resolved_direction"] = direction
            records.append(record)
        except Exception as exc:
            if not options["skip_invalid"]:
                raise ValueError(
                    "TIA tag {} conversion failed: {}".format(index, exc)
                ) from exc
            skipped.append({
                "index": index,
                "reason": str(exc),
            })

    records.sort(key=_record_sort_key)
    profiles = profile_ids if isinstance(profile_ids, dict) else {}
    seen = set(options.get("existing_address_keys") or ())
    sensors = []
    actuators = []
    sensor_id = int(identity["sensor_id"])
    actuator_id = int(identity["actuator_id"])
    for record in records:
        direction = record["resolved_direction"]
        address_key = (direction, record["address"])
        if address_key in seen and not options["allow_duplicate_addresses"]:
            raise ValueError(
                "duplicate {} address: {}".format(direction, record["address"])
            )
        seen.add(address_key)
        if direction == "sensor":
            sensors.append(_make_row(
                record,
                route,
                sensor_id,
                direction,
                profile_id=profiles.get("sensor"),
            ))
            sensor_id += 1
        else:
            actuators.append(_make_row(
                record,
                route,
                actuator_id,
                direction,
                profile_id=profiles.get("actuator"),
            ))
            actuator_id += 1
    return {
        "conversion": {
            "format": "automation_tia_v9_io_fragment",
            "source_file": os.path.basename(xml_path),
            "source_sha256": hashlib.sha256(raw).hexdigest(),
            "controller_uuid": route["controller_uuid"],
            "requires_non_optimized_data_blocks": True,
            "sensor_count": len(sensors),
            "actuator_count": len(actuators),
            "skipped_count": len(skipped),
            "skipped": skipped,
        },
        "sensors": sensors,
        "actuators": actuators,
    }


def _record_sort_key(record):
    direction_rank = 0 if record["resolved_direction"] == "sensor" else 1
    return (
        direction_rank,
        str(record.get("name") or "").casefold(),
        record["address"],
    )


def _load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("base V9 configuration must be a JSON object")
    return value


def _existing_address_keys(config, controller_uuid):
    result = set()
    for section, direction in (
        ("sensors", "sensor"),
        ("actuators", "actuator"),
    ):
        for row in config.get(section) or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("controller_uuid") or "") != controller_uuid:
                continue
            address = str(row.get("address") or "").strip()
            if address:
                result.add((direction, address))
    return result


def _merge_fragment(config, fragment):
    result = copy.deepcopy(config)
    for section in ("sensors", "actuators"):
        rows = result.get(section)
        if not isinstance(rows, list):
            raise ValueError(
                "base configuration section {} is not a list".format(section)
            )
        rows.extend(copy.deepcopy(fragment.get(section) or []))
    report = type_library.verify_config_data(result)
    if report.get("status") != "OK":
        errors = report.get("errors") or []
        first = errors[0] if errors else {}
        raise ValueError(
            "merged V9 configuration failed at {}: {}".format(
                first.get("path", "<unknown>"),
                first.get("error", "unknown validation error"),
            )
        )
    return result


def _render_json(value, canonical=False):
    if canonical:
        text, unused_statistics = canonical_format_json(
            value,
            sort_tables=False,
        )
        return text + "\n"
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def _write_text_atomic(path, text):
    target = os.path.abspath(path)
    directory = os.path.dirname(target) or os.getcwd()
    descriptor, temporary = tempfile.mkstemp(
        prefix=".tia-convert-",
        suffix=".tmp",
        dir=directory,
        text=True,
    )
    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Convert absolute TIA Portal Openness PLC tags into V9 plc_s7 "
            "sensor and actuator rows."
        )
    )
    parser.add_argument("xml", help="TIA Portal Openness XML export")
    parser.add_argument("--output", default="-")
    parser.add_argument("--base-config")
    parser.add_argument("--controller-uuid", required=True)
    parser.add_argument("--controller-key")
    parser.add_argument("--local-controller-id", type=int)
    parser.add_argument("--node-id")
    parser.add_argument("--site-id")
    parser.add_argument("--sensor-start-id", type=int)
    parser.add_argument("--actuator-start-id", type=int)
    parser.add_argument("--sensor-profile-id", type=int)
    parser.add_argument("--actuator-profile-id", type=int)
    parser.add_argument(
        "--default-direction",
        choices=("error", "sensor", "actuator", "skip"),
        default="error",
    )
    parser.add_argument("--sensor-pattern", default=_DEFAULT_SENSOR_PATTERN)
    parser.add_argument(
        "--actuator-pattern",
        default=_DEFAULT_ACTUATOR_PATTERN,
    )
    parser.add_argument("--skip-invalid", action="store_true")
    parser.add_argument(
        "--allow-duplicate-addresses",
        action="store_true",
    )
    return parser


def _conversion_context(arguments):
    base = _load_json(arguments.base_config) if arguments.base_config else None
    if base is None:
        route = _route_from_arguments(arguments)
        sensor_id = int(arguments.sensor_start_id or 1)
        actuator_id = int(arguments.actuator_start_id or 1)
    else:
        route = _route_from_config(base, arguments.controller_uuid)
        sensor_id = _next_identity(
            base,
            "sensors",
            "sensor_id",
            arguments.controller_uuid,
            arguments.sensor_start_id,
        )
        actuator_id = _next_identity(
            base,
            "actuators",
            "actuator_id",
            arguments.controller_uuid,
            arguments.actuator_start_id,
        )
    options = _conversion_options(arguments)
    if base is not None:
        options["existing_address_keys"] = _existing_address_keys(
            base,
            arguments.controller_uuid,
        )
    return {
        "base": base,
        "route": route,
        "identity": {
            "sensor_id": sensor_id,
            "actuator_id": actuator_id,
        },
        "options": options,
        "profiles": {
            "sensor": arguments.sensor_profile_id,
            "actuator": arguments.actuator_profile_id,
        },
    }


def run_conversion(arguments):
    context = _conversion_context(arguments)
    fragment = convert_xml(
        arguments.xml,
        context["route"],
        context["identity"],
        context["options"],
        profile_ids=context["profiles"],
    )
    if context["base"] is None:
        return fragment, False
    return _merge_fragment(context["base"], fragment), True


def main(argv=None):
    arguments = _build_parser().parse_args(argv)
    try:
        result, canonical = run_conversion(arguments)
        rendered = _render_json(result, canonical=canonical)
        if arguments.output == "-":
            sys.stdout.write(rendered)
        else:
            _write_text_atomic(arguments.output, rendered)
        return 0
    except Exception as exc:
        sys.stderr.write("TIA conversion failed: {}\n".format(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())



