# -*- coding: utf-8 -*-

# test/test_generic_type_cross_scope.py

"""
Verify the canonical generic type library's scope-dependent Cross profile.

Cross-V8 shards use logical UUID identities without duplicating physical bus
coordinates. Node-V9 documents retain strict hardware validation and V5 can
enter the runtime only through the explicit import boundary.
"""

import copy
import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library import _generic_type_library as type_library


CONFIG_ROOT = os.path.join(PROJECT_ROOT, "_config", "config_data")
CROSS_CONFIG = os.path.join(
    CONFIG_ROOT,
    "device_state_data_v8_cross_fn-01.json",
)
NODE_CONFIG = os.path.join(
    CONFIG_ROOT,
    "device_state_data_v9_node_fn-01.json",
)
LEGACY_CONFIG = os.path.join(
    CONFIG_ROOT, "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def load_json(path):
    """Load one immutable test source document."""
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def error_paths(report):
    """Return deterministic error paths from one verification report."""
    return [row.get("path") for row in report.get("errors", [])]


def test_cross_scope_profile():
    """Cross profile accepts logical UUID I/O and rejects invalid identity."""
    cross_config = load_json(CROSS_CONFIG)
    report = type_library.verify_config_data(cross_config)
    assert report["status"] == "OK", type_library.format_report(report)
    assert report["errors"] == []
    assert report["warnings"] == []

    logical_only = copy.deepcopy(cross_config)
    for controller in logical_only.get("controllers", []):
        controller.pop("controller_id", None)
        for field in ("bus_type", "host", "port", "unit", "device_name", "tags"):
            controller.pop(field, None)
    for section_name in ("sensors", "actuators"):
        for group in logical_only.get(section_name, []):
            group.pop("node_id", None)
            group.pop("local_controller_id", None)
            for io_row in group.get("rows", []):
                for field in ("bus_type", "function_type", "address"):
                    io_row.pop(field, None)
    logical_report = type_library.verify_config_data(logical_only)
    assert logical_report["status"] == "OK", type_library.format_report(
        logical_report
    )

    invalid_identity = copy.deepcopy(cross_config)
    invalid_identity["controllers"][0].pop("owner_node_id")
    invalid_report = type_library.verify_config_data(invalid_identity)
    assert invalid_report["status"] == "FAILED"
    assert "controllers[0]" in error_paths(invalid_report)


def test_node_profile_and_explicit_legacy_import_remain_strict():
    """Node-V9 stays physical and V5 requires the dedicated import API."""
    node_storage = load_json(NODE_CONFIG)
    node_config = type_library.normalize_config_for_runtime(
        node_storage,
        representation="storage",
    )
    node_report = type_library.verify_runtime_config_data(node_config)
    assert node_report["status"] == "OK", type_library.format_report(node_report)

    missing_bus = copy.deepcopy(node_config)
    sensor_group_index = next(
        index
        for index, group in enumerate(missing_bus["sensors"])
        if group.get("rows")
    )
    missing_bus["sensors"][sensor_group_index]["rows"][0].pop("bus_type")
    missing_report = type_library.verify_runtime_config_data(missing_bus)
    assert missing_report["status"] == "FAILED"
    expected_path = "sensors[{}].rows[0]".format(sensor_group_index)
    assert expected_path in error_paths(missing_report)

    legacy_config = load_json(LEGACY_CONFIG)
    legacy_report = type_library.verify_storage_config_data(legacy_config)
    assert legacy_report["status"] == "FAILED"
    assert "meta[0].version" in error_paths(legacy_report)

    imported = type_library.import_legacy_v5_config(
        legacy_config,
        default_scope="master_global",
    )
    imported_report = type_library.verify_storage_config_data(imported)
    assert imported_report["status"] == "OK", type_library.format_report(
        imported_report)
    assert imported["meta"][0]["version"] == type_library.CANONICAL_VERSION


def main():
    """Run the dependency-light executable test module."""
    test_cross_scope_profile()
    test_node_profile_and_explicit_legacy_import_remain_strict()
    print("PASS generic type Cross scope profile")


if __name__ == "__main__":
    main()
