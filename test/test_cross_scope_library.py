# -*- coding: utf-8 -*-

"""Deterministic graph-scope, sharding and safety-boundary tests."""

import copy
import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._generic_system_contract_library import config_sharding_contracts as cross_scope


CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "test",
    "fixtures",
    "_legacy",
    "device_state_data_v7_fn-01_unsplit.json",
)


def _load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _noncritical_fixture(config):
    result = copy.deepcopy(config)
    result["rules"] = [
        row
        for row in result.get("rules") or []
        if str(row.get("group") or "").lower() != "safety"
    ]
    return result


def _executable_fixture(config):
    result = _noncritical_fixture(config)
    migration_rule_ids = {5, 7, 8, 9}
    result["rules"] = [
        row
        for row in result.get("rules") or []
        if row.get("rule_id") not in migration_rule_ids
    ]
    return result


def _cross_worker_fixture(config):
    result = _noncritical_fixture(config)
    remote_uuid = "06ab4528-f8a3-5ec7-a91f-eea79d60761e"
    result["nodes"].append(
        {
            "node_id": "fn-02",
            "node_role": "worker",
            "enabled": True,
            "description": "remote worker",
        }
    )
    result["site_controller_map"].append(
        {
            "node_id": "fn-02",
            "local_controller_id": 1,
            "controller_uuid": remote_uuid,
            "controller_key": "ctrl_remote",
            "enabled": True,
            "route_key": "fn-02.ctrl_1",
            "routing_role": "owner",
        }
    )
    result["controllers"].append(
        {
            "controller_id": 5,
            "controller_uuid": remote_uuid,
            "controller_key": "ctrl_remote",
            "device_name": "remote",
            "bus_type": "modbus",
            "host": "192.0.2.10",
            "port": 502,
            "unit": "0x1",
            "tags": [],
        }
    )
    result["actuators"].append(
        {
            "node_id": "fn-02",
            "local_controller_id": 1,
            "controller_uuid": remote_uuid,
            "controller_key": "ctrl_remote",
            "enabled": True,
            "rows": [
                {
                    "actuator_id": 1,
                    "bus_type": "modbus",
                    "function_type": "write_coil",
                    "address": 600,
                    "actuator_group": "X",
                }
            ],
        }
    )
    result["processes"].append(
        {
            "p_id": 30,
            "process_name": "remote_action",
            "process_location": "",
            "owner_node_id": "fn-01",
        }
    )
    result["process_states"].append(
        {
            "state_id": 30,
            "p_id": 30,
            "value_id": 2,
            "mode_id": 2,
            "mapping_id": 240,
            "io_refs": {
                "actuators": [
                    {"controller_uuid": remote_uuid, "actuator_id": 1}
                ],
                "sensors": [
                    {
                        "controller_uuid": "62671112-766c-59ed-b0ff-30a49d48f747",
                        "sensor_id": 3,
                    }
                ],
            },
            "parameters": {"flow_rate": 1.0},
            "control_strategy": None,
            "setpoint_source": None,
        }
    )
    result["rules"].append(
        {
            "rule_id": 30,
            "engine_id": 1,
            "group": "process",
            "priority": 900,
            "name": "cross_worker_test",
            "enabled": True,
            "when": [
                {
                    "src": "sensor",
                    "controller_uuid": "62671112-766c-59ed-b0ff-30a49d48f747",
                    "sensor_id": 3,
                    "op": ">",
                    "value": 45000,
                }
            ],
            "do": [{"state_ids": [30], "value_id": 1}],
            "owner_node_id": "fn-01",
        }
    )
    return result


def run_test():
    config = _load_config()
    analysis = cross_scope.classify_config_scopes(config)

    assert analysis["rule_scopes"][cross_scope.SCOPE_LOCAL] == (1, 2, 4, 10, 11)
    assert analysis["rule_scopes"][cross_scope.SCOPE_CROSS_CONTROLLER] == (
        3, 5, 6, 7, 8, 9, 13, 15, 17,
    )
    assert analysis["rule_scopes"][cross_scope.SCOPE_CROSS_WORKER] == ()
    assert len(analysis["state_scopes"][cross_scope.SCOPE_LOCAL]) == 11
    assert analysis["state_scopes"][cross_scope.SCOPE_CROSS_CONTROLLER] == (
        2, 3, 4, 14, 15, 17,
    )

    fingerprint = cross_scope.canonical_fingerprint(config)
    reordered = json.loads(json.dumps(config, sort_keys=True))
    assert cross_scope.canonical_fingerprint(reordered) == fingerprint
    changed = copy.deepcopy(config)
    changed["rules"][0]["priority"] += 1
    assert cross_scope.canonical_fingerprint(changed) != fingerprint

    local = cross_scope.build_local_execution_shard(config, "fn-01")
    assert local["meta"][0]["version"] == 9
    assert [row["rule_id"] for row in local["rules"]] == [1, 2, 4, 10, 11]
    assert len(local["process_states"]) == 11
    assert cross_scope.validate_local_execution_shard(local, "fn-01")["ok"]
    assert all(
        row.get("node_id") == "fn-01"
        for row in local["site_controller_map"]
    )
    controller_2 = "62671112-766c-59ed-b0ff-30a49d48f747"
    allowlisted = cross_scope.build_local_execution_shard(
        config,
        "fn-01",
        controller_allowlist=(controller_2,),
    )
    assert [row["rule_id"] for row in allowlisted["rules"]] == [1, 2, 10, 11]
    assert {row["controller_uuid"] for row in allowlisted["controllers"]} == {
        controller_2,
    }
    assert allowlisted["local_node"][0]["active_controller_uuids"] == [
        controller_2,
    ]
    denied = cross_scope.build_local_execution_shard(
        config,
        "fn-01",
        controller_allowlist=(),
    )
    assert denied["controllers"] == []
    assert denied["rules"] == []
    assert denied["process_states"] == []

    unknown = cross_scope.build_local_execution_shard(config, "fn-missing")
    assert unknown["rules"] == []
    assert unknown["process_states"] == []
    assert unknown["controllers"] == []
    assert unknown["sensors"] == []
    assert unknown["actuators"] == []

    ownership_failure = copy.deepcopy(config)
    ownership_failure["site_controller_map"].append(
        copy.deepcopy(ownership_failure["site_controller_map"][0])
    )
    ownership_failure["site_controller_map"][-1]["route_key"] = "duplicate.owner"
    assert not cross_scope.validate_controller_ownership(ownership_failure)["ok"]
    invalid_analysis = cross_scope.classify_config_scopes(ownership_failure)
    assert invalid_analysis["states"]["2"]["scope"] == cross_scope.SCOPE_INVALID

    partition = cross_scope.validate_scope_partition(config)
    assert not partition["ok"]
    assert any("critical rule 13" in message for message in partition["errors"])
    assert any("critical rule 15" in message for message in partition["errors"])
    assert any("critical rule 17" in message for message in partition["errors"])
    assert any("cross rule 5" in message and "migration required" in message
               for message in partition["errors"])
    assert any("cross rule 7" in message and "migration required" in message
               for message in partition["errors"])
    assert any("cross rule 8" in message and "migration required" in message
               for message in partition["errors"])
    assert any("cross rule 9" in message and "migration required" in message
               for message in partition["errors"])

    safe_config = _noncritical_fixture(config)
    assert not cross_scope.validate_scope_partition(safe_config)["ok"]
    executable_config = _executable_fixture(config)
    assert cross_scope.validate_scope_partition(executable_config)["ok"]
    safe_local = cross_scope.build_local_execution_shard(executable_config, "fn-01")
    safe_cross = cross_scope.build_cross_execution_shard(executable_config, "fn-01")
    assert safe_cross["meta"][0]["version"] == 8
    assert cross_scope.validate_cross_execution_shard(safe_cross, "fn-01")["ok"]
    assert cross_scope.validate_split_shards(
        executable_config,
        safe_local,
        safe_cross,
    )["ok"]
    local_ids = {row["rule_id"] for row in safe_local["rules"]}
    cross_ids = {row["rule_id"] for row in safe_cross["rules"]}
    assert local_ids.isdisjoint(cross_ids)
    assert cross_ids == {3, 6}
    assert {row["state_id"] for row in safe_cross["process_states"]} == {
        14, 15,
    }
    assert all(
        not {"host", "port", "bus_type", "unit"}.intersection(row)
        for row in safe_cross["controllers"]
    )
    for section in ("sensors", "actuators"):
        for row in safe_cross[section]:
            assert not {
                "address", "bus_type", "function_type", "unit",
            }.intersection(row)
    leaked_cross = copy.deepcopy(safe_cross)
    leaked_cross["controllers"][0]["host"] = "192.0.2.99"
    assert not cross_scope.validate_cross_execution_shard(
        leaked_cross,
        "fn-01",
    )["ok"]

    cross_worker = _cross_worker_fixture(config)
    cross_worker_analysis = cross_scope.classify_config_scopes(cross_worker)
    assert cross_worker_analysis["rules"]["30"]["scope"] == cross_scope.SCOPE_CROSS_WORKER
    assert cross_worker_analysis["rules"]["30"]["node_ids"] == ("fn-01", "fn-02")
    rule_30 = [row for row in cross_worker["rules"] if row.get("rule_id") == 30][0]
    assert cross_scope.cross_rule_eligibility(cross_worker, rule_30)["ok"]

    print("PASS deterministic local/cross graph sharding and safety boundary")


if __name__ == "__main__":
    run_test()

