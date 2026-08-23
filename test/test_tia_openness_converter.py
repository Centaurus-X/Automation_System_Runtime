# -*- coding: utf-8 -*-

"""Deterministic TIA Openness XML to V9 S7 row regression tests."""

import copy
import json
import os
import re
import sys
import tempfile


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library import _generic_type_library as type_library
from _tools import convert_tia_openness_xml as converter


FIXTURE = os.path.join(
    PROJECT_ROOT,
    "test",
    "fixtures",
    "tia_openness_plc_tags.xml",
)

BASE_CONFIG = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "device_state_data_v9_node_fn-01.json",
)


def _options():
    return {
        "sensor_pattern": re.compile(
            converter._DEFAULT_SENSOR_PATTERN,
            re.IGNORECASE,
        ),
        "actuator_pattern": re.compile(
            converter._DEFAULT_ACTUATOR_PATTERN,
            re.IGNORECASE,
        ),
        "default_direction": "error",
        "skip_invalid": False,
        "allow_duplicate_addresses": False,
    }


def _route():
    return {
        "controller_uuid": "test-controller-uuid",
        "controller_key": "ctrl_tia_test",
        "local_controller_id": 9,
        "node_id": "fn-test",
        "site_id": "site-test",
    }


def _load_base():
    with open(BASE_CONFIG, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _make_s7_base():
    base = _load_base()
    controller = copy.deepcopy(base["controllers"][1])
    controller_uuid = controller["controller_uuid"]
    controller["bus_type"] = "plc_s7"
    controller["port"] = 102
    controller["comm"] = {
        "area_rids": {"DB": 132, "I": 129, "M": 131, "Q": 130},
        "connection_type": 3,
        "optimized_block_access": False,
        "rack": 0,
        "slot": 1,
        "s7_batch_fallback_sequential": True,
        "s7_max_parallel": 1,
        "s7_multi_read_max_gap": 5,
        "s7_multi_var_limit": 20,
        "s7_read_optimizer": True,
        "security": {
            "credential_ref": "plc/site-test/main",
            "mode": "secure",
            "tls_ca_path": "/run/secrets/plc-ca.crt",
            "tls_cert_path": "/run/secrets/plc-client.crt",
            "tls_key_path": "/run/secrets/plc-client.key",
            "use_system_ca": False,
            "use_tls": True,
            "verify_peer": True,
        },
        "string_encoding": "latin-1",
    }
    base["controllers"][1] = controller

    sensor_index = 0
    for row in base["sensors"]:
        if row.get("controller_uuid") != controller_uuid:
            continue
        row["bus_type"] = "plc_s7"
        row["function_type"] = "read_s7"
        row["address"] = "DB30.{}:INT".format(sensor_index * 2)
        sensor_index += 1

    actuator_index = 0
    for row in base["actuators"]:
        if row.get("controller_uuid") != controller_uuid:
            continue
        row["bus_type"] = "plc_s7"
        row["function_type"] = "write_s7"
        row["address"] = "DB31.{}:INT".format(actuator_index * 2)
        actuator_index += 1
    return base, controller_uuid


def _write_text(path, text):
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def _test_fragment_conversion():
    fragment = converter.convert_xml(
        FIXTURE,
        _route(),
        {"sensor_id": 10, "actuator_id": 20},
        _options(),
        profile_ids={"sensor": 1, "actuator": 2},
    )
    assert fragment["conversion"]["sensor_count"] == 4
    assert fragment["conversion"]["actuator_count"] == 2
    assert fragment["conversion"]["skipped_count"] == 0
    assert fragment["conversion"]["requires_non_optimized_data_blocks"] is True

    sensor_names = [row["sensor_group"] for row in fragment["sensors"]]
    actuator_names = [row["actuator_group"] for row in fragment["actuators"]]
    assert sensor_names == [
        "batch_name",
        "input_temperature",
        "maintenance_feedback",
        "process_actual_pv",
    ]
    assert actuator_names == ["output_valve", "valve_command"]
    assert [row["sensor_id"] for row in fragment["sensors"]] == [10, 11, 12, 13]
    assert [row["actuator_id"] for row in fragment["actuators"]] == [20, 21]
    assert [row["address"] for row in fragment["sensors"]] == [
        "DB10.20:STRING[10]",
        "I0:REAL",
        "M10.0:BOOL",
        "DB10.4:REAL",
    ]
    assert [row["address"] for row in fragment["actuators"]] == [
        "Q0.3:BOOL",
        "DB10.8:INT",
    ]
    assert all(row["bus_type"] == "plc_s7" for row in fragment["sensors"])
    assert all(row["function_type"] == "read_s7" for row in fragment["sensors"])
    assert all(row["function_type"] == "write_s7" for row in fragment["actuators"])


def _test_fail_closed_xml_and_direction():
    with tempfile.TemporaryDirectory(prefix="tia-converter-") as directory:
        ambiguous = os.path.join(directory, "ambiguous.xml")
        _write_text(
            ambiguous,
            "<Document><SW.Tags.PlcTag><AttributeList>"
            "<Name>neutral</Name><LogicalAddress>DB1.DBD0</LogicalAddress>"
            "<DataTypeName>Real</DataTypeName></AttributeList>"
            "</SW.Tags.PlcTag></Document>",
        )
        try:
            converter.convert_xml(
                ambiguous,
                _route(),
                {"sensor_id": 1, "actuator_id": 1},
                _options(),
            )
        except ValueError as exc:
            assert "ambiguous DB/M tag" in str(exc)
        else:
            raise AssertionError("ambiguous DB tag did not fail closed")

        unsafe = os.path.join(directory, "unsafe.xml")
        _write_text(
            unsafe,
            "<!DOCTYPE x [<!ENTITY y 'unsafe'>]><Document/>",
        )
        try:
            converter._load_xml(unsafe)
        except ValueError as exc:
            assert "DTD and entity" in str(exc)
        else:
            raise AssertionError("DTD-bearing XML was accepted")


def _test_validated_v9_merge():
    base, controller_uuid = _make_s7_base()
    route = converter._route_from_config(base, controller_uuid)
    options = _options()
    options["existing_address_keys"] = converter._existing_address_keys(
        base,
        controller_uuid,
    )
    fragment = converter.convert_xml(
        FIXTURE,
        route,
        {
            "sensor_id": converter._next_identity(
                base,
                "sensors",
                "sensor_id",
                controller_uuid,
                None,
            ),
            "actuator_id": converter._next_identity(
                base,
                "actuators",
                "actuator_id",
                controller_uuid,
                None,
            ),
        },
        options,
    )
    merged = converter._merge_fragment(base, fragment)
    report = type_library.verify_config_data(merged)
    assert report["status"] == "OK", report["errors"]
    unsafe = copy.deepcopy(merged)
    unsafe_controller = None
    for candidate in unsafe["controllers"]:
        if candidate.get("controller_uuid") == controller_uuid:
            unsafe_controller = candidate
            break
    assert unsafe_controller is not None
    unsafe_controller["comm"]["security"]["password"] = "forbidden"
    unsafe_report = type_library.verify_config_data(unsafe)
    assert unsafe_report["status"] == "FAILED"
    assert any(
        "password" in str(error.get("path") or error.get("message") or "")
        for error in unsafe_report["errors"]
    )
    added_sensors = [
        row for row in merged["sensors"]
        if row.get("controller_uuid") == controller_uuid
        and isinstance(row.get("address"), str)
        and row.get("address", "").startswith(("DB10.", "I0:", "M10."))
    ]
    added_actuators = [
        row for row in merged["actuators"]
        if row.get("controller_uuid") == controller_uuid
        and row.get("address") in ("Q0.3:BOOL", "DB10.8:INT")
    ]
    assert len(added_sensors) == 4
    assert len(added_actuators) == 2


def run_test():
    _test_fragment_conversion()
    _test_fail_closed_xml_and_direction()
    _test_validated_v9_merge()
    print("PASS TIA Openness XML conversion and validated V9 merge")


if __name__ == "__main__":
    run_test()


