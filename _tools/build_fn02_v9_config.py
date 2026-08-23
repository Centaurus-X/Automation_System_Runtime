# -*- coding: utf-8 -*-

"""Build the canonical fn-02 V9 Node shard from its isolated V5 source."""

import copy
import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._generic_system_contract_library import (
    config_compatibility,
    config_graph,
    entity_registry,
    shard_library,
    type_library,
)
from src.library._generic_type_library import (
    inspect_config_contract,
    normalize_config_for_runtime,
    project_config_for_storage,
)
from src.library._generic_type_library import fingerprint
from src.library._json_formator import canonical_format_json


CONFIG_ROOT = os.path.join(PROJECT_ROOT, "_config", "config_data")
LEGACY_ROOT = os.path.join(CONFIG_ROOT, "_legacy")
FN02_SOURCE = os.path.join(
    LEGACY_ROOT, "device_state_data_v7_node_fn-02.json")
GLOBAL_REFERENCE = os.path.join(
    LEGACY_ROOT, "device_state_data_v5_multi_worker_norm.json")
NODE_ID = "fn-02"
SITE_ID = "site-main"
CONTROLLER_UUID = "06ab4528-f8a3-5ec7-a91f-eea79d60761e"
CONTROLLER_KEY = "ctrl_wago_5"


def _read_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _one(rows, field, value):
    matches = []
    for row in rows or []:
        if isinstance(row, dict) and str(row.get(field)) == str(value):
            matches.append(row)
    if len(matches) != 1:
        raise ValueError(
            "reference lookup is not unique: {}={}".format(field, value))
    return matches[0]


def _require_status_ok(report, label):
    if report.get("status") != "OK":
        raise ValueError(label + " failed")


def _require_boolean_ok(report, label):
    if report.get("ok") is not True:
        raise ValueError(label + " failed")


def build_config():
    legacy = _read_json(FN02_SOURCE)
    reference = _read_json(GLOBAL_REFERENCE)
    source_meta = _one(legacy.get("meta"), "node_id", NODE_ID)
    if source_meta.get("schema") != "device_state_norm" \
            or int(source_meta.get("version") or 0) != 5 \
            or source_meta.get("scope") != "node_local":
        raise ValueError("fn-02 legacy source contract differs")
    source_controller = _one(
        legacy.get("controllers"), "controller_uuid", CONTROLLER_UUID)
    if source_controller.get("controller_key") != CONTROLLER_KEY:
        raise ValueError("fn-02 controller key differs")
    candidate = config_compatibility.import_legacy_v5_config(legacy)
    candidate["sites"] = [copy.deepcopy(
        _one(reference.get("sites"), "site_id", SITE_ID))]
    node = _one(candidate.get("nodes"), "node_id", NODE_ID)
    node["site_id"] = SITE_ID
    local_node = _one(candidate.get("local_node"), "node_id", NODE_ID)
    local_node["site_id"] = SITE_ID
    route = _one(
        candidate.get("site_controller_map"),
        "controller_uuid",
        CONTROLLER_UUID,
    )
    if str(route.get("node_id")) != NODE_ID \
            or int(route.get("local_controller_id") or 0) != 1 \
            or route.get("routing_role") != "owner":
        raise ValueError("fn-02 owner route differs")
    route["site_id"] = SITE_ID
    route["route_key"] = "site-main.fn-02.ctrl_1"
    for section in ("sensors", "actuators"):
        for entity in candidate.get(section) or []:
            if str(entity.get("controller_uuid")) != CONTROLLER_UUID \
                    or str(entity.get("node_id")) != NODE_ID \
                    or int(entity.get("local_controller_id") or 0) != 1 \
                    or entity.get("controller_key") != CONTROLLER_KEY \
                    or "rows" in entity:
                raise ValueError("fn-02 I/O identity or layout differs")
            entity["site_id"] = SITE_ID
    meta = _one(candidate.get("meta"), "node_id", NODE_ID)
    meta["description"] = (
        "V9-Node-Shard (fn-02): Canonical flat sensor and actuator entity "
        "tables with stable controller_uuid plus local sensor_id/actuator_id "
        "identity. The shard contains only fn-02-owned hardware and execution "
        "entities plus shared node-free type/template catalogs."
    )
    expected_actuators = {
        1: ("write_register", 523, None),
        2: ("write_register", 532, None),
        3: ("write_register", 534, 3),
    }
    if len(candidate.get("actuators") or []) != len(expected_actuators) \
            or candidate.get("sensors") != []:
        raise ValueError("fn-02 I/O count differs")
    for actuator in candidate["actuators"]:
        expected = expected_actuators.get(int(actuator.get("actuator_id")))
        actual = (
            actuator.get("function_type"),
            int(actuator.get("address")),
            actuator.get("profile_id"),
        )
        if actual != expected:
            raise ValueError("fn-02 actuator payload differs")
    process = _one(candidate.get("processes"), "p_id", 7)
    state = _one(candidate.get("process_states"), "state_id", 30)
    if process.get("owner_node_id") != NODE_ID \
            or int(state.get("p_id") or 0) != 7:
        raise ValueError("fn-02 process ownership differs")
    contract_report = inspect_config_contract(
        candidate, representation="storage")
    _require_status_ok(contract_report, "fn-02 V9 storage contract")
    type_report = type_library.verify_config_data(candidate)
    registry_report = entity_registry.verify_roundtrip(candidate)
    shard_report = shard_library.validate_local_execution_shard(
        candidate, NODE_ID)
    authority_report = config_graph.validate_authority_config(
        candidate, profile="production_v2")
    _require_status_ok(type_report, "fn-02 V9 storage types")
    runtime = normalize_config_for_runtime(
        candidate, representation="storage")
    _require_status_ok(
        type_library.verify_runtime_config_data(runtime),
        "fn-02 V9 runtime types",
    )
    restored = project_config_for_storage(runtime)
    if fingerprint(restored) != fingerprint(candidate):
        raise ValueError("fn-02 V9 Storage/Runtime fingerprint differs")
    _require_boolean_ok(registry_report, "fn-02 V9 entity roundtrip")
    _require_boolean_ok(shard_report, "fn-02 V9 local shard")
    _require_boolean_ok(authority_report, "fn-02 V9 authority")
    return candidate


def render_config():
    candidate = build_config()
    text, _statistics = canonical_format_json(
        candidate, sort_tables=False)
    rendered = json.loads(text)
    if fingerprint(rendered) != fingerprint(candidate):
        raise ValueError("fn-02 V9 formatter changed the fingerprint")
    return text + "\n"


def main():
    sys.stdout.write(render_config())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


