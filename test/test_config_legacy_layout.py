# -*- coding: utf-8 -*-

"""V8/V9 active layout and explicit-only legacy config loading."""

import copy
import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.system.bootstrap_runtime_interface import _data_center_options


CONFIG_ROOT = os.path.join(PROJECT_ROOT, "_config", "config_data")
LEGACY_ROOT = os.path.join(CONFIG_ROOT, "_legacy")
APP_CONFIG = os.path.join(PROJECT_ROOT, "_config", "application", "app_config.yaml")
ACTIVE_CONFIGS = {
    "device_state_data_v8_cross_fn-01.json",
    "device_state_data_v9_node_fn-01.json",
    "device_state_data_v9_node_fn-02.json",
}
LEGACY_CONFIGS = {
    "device_state_data_v5_multi_worker_norm.json",
    "device_state_data_v7_cross_fn-01.json",
    "device_state_data_v7_node_fn-01.json",
    "device_state_data_v7_node_fn-02.json",
}
LEGACY_ARTIFACTS = LEGACY_CONFIGS | {
    "device_state_data_v5_validation_report.txt",
    "README.md",
}


def _load_version(path):
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    meta = config.get("meta") or []
    row = meta[0] if isinstance(meta, list) and meta else meta
    return int(row.get("version"))


def _test_layout_by_contract_version():
    active = {
        name for name in os.listdir(CONFIG_ROOT)
        if os.path.isfile(os.path.join(CONFIG_ROOT, name))
        and name.endswith(".json")
    }
    legacy = {
        name for name in os.listdir(LEGACY_ROOT)
        if os.path.isfile(os.path.join(LEGACY_ROOT, name))
        and name.endswith(".json")
    }
    legacy_artifacts = {
        name for name in os.listdir(LEGACY_ROOT)
        if os.path.isfile(os.path.join(LEGACY_ROOT, name))
    }
    assert active == ACTIVE_CONFIGS
    assert legacy == LEGACY_CONFIGS
    assert legacy_artifacts == LEGACY_ARTIFACTS
    assert all(_load_version(os.path.join(CONFIG_ROOT, name)) >= 8
               for name in active)
    assert all(_load_version(os.path.join(LEGACY_ROOT, name)) < 8
               for name in legacy)

    for directory, _subdirectories, filenames in os.walk(CONFIG_ROOT):
        for name in filenames:
            if not name.endswith(".json"):
                continue
            path = os.path.join(directory, name)
            relative = os.path.relpath(path, CONFIG_ROOT)
            parts = relative.replace("\\", "/").split("/")
            if _load_version(path) < 8:
                assert parts[0] == "_legacy", relative


def _test_standard_profiles_never_select_legacy():
    standard_configs = (
        {},
        {"runtime": {"data_center": {
            "json_path": "_config/config_data/device_state_data_v9_node_fn-01.json",
            "allow_legacy_import": False,
        }}},
    )
    for config in standard_configs:
        options = _data_center_options(config)
        normalized = options["json_path"].replace("\\", "/")
        assert "/_legacy/" not in normalized
        assert options["allow_legacy_import"] is False
        assert normalized.endswith("device_state_data_v9_node_fn-01.json")
    with open(APP_CONFIG, "r", encoding="utf-8") as handle:
        source = handle.read()
    assert "_config/config_data/_legacy/" not in source
    assert "_config/config_data/device_state_data_v7" not in source
    assert "_config/config_data/device_state_data_v5" not in source
    assert "worker_fn02:" in source
    assert "_config/config_data/device_state_data_v9_node_fn-02.json" in source


def _test_legacy_profile_is_explicit_and_fail_closed():
    config = {"runtime": {"data_center": {
        "json_path": (
            "_config/config_data/_legacy/"
            "device_state_data_v7_node_fn-02.json"),
        "allow_legacy_import": True,
        "legacy_import_profile": "lab_v2",
    }}}
    options = _data_center_options(config)
    normalized = options["json_path"].replace("\\", "/")
    assert "/_legacy/" in normalized
    assert options["allow_legacy_import"] is True
    rejected = copy.deepcopy(config)
    rejected["runtime"]["data_center"]["allow_legacy_import"] = False
    try:
        _data_center_options(rejected)
    except ValueError as error:
        assert "legacy config path requires" in str(error)
    else:
        raise AssertionError("legacy path was accepted without explicit opt-in")


def main():
    _test_layout_by_contract_version()
    _test_standard_profiles_never_select_legacy()
    _test_legacy_profile_is_explicit_and_fail_closed()
    print("PASS active V8/V9 config layout and explicit legacy isolation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
