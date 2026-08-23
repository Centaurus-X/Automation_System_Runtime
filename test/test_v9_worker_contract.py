# -*- coding: utf-8 -*-

"""Executable V9 Worker storage/runtime and stable-identity contract tests."""

import copy
import json
import os
import sqlite3
import sys
import tempfile

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._generic_system_contract_library import protocol_contracts
from src.library._generic_system_contract_library.config_sharding_contracts import apply_operations, diff_configs
from src.core.distributed_coordinator import _node_payload
from src.core.distributed_coordinator import _remember_node_scope
from src.core.distributed_coordinator import create_distributed_coordinator
from src.library import _generic_type_library as compatibility
from src.library import _generic_data_center as data_center
from src.library import _generic_type_library as type_library
from src.library._distributed_operation_store import get_metadata
from src.library._generic_system_contract_library.config_sharding_contracts import (
    build_cross_execution_shard,
    build_local_execution_shard,
)
from _tools.build_fn02_v9_config import render_config


V9_PATH = os.path.join(
    PROJECT_ROOT, "_config", "config_data",
    "device_state_data_v9_node_fn-01.json")
V9_FN02_PATH = os.path.join(
    PROJECT_ROOT, "_config", "config_data",
    "device_state_data_v9_node_fn-02.json")


def _load_v9():
    with open(V9_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _runtime_v9():
    return type_library.normalize_config_for_runtime(
        _load_v9(), representation="storage")


def _load_fn02_v9():
    with open(V9_FN02_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def test_state21_temperature_sensor_uses_engineering_profile():
    storage = _load_v9()
    controller_uuid = "62671112-766c-59ed-b0ff-30a49d48f747"
    sensors = [
        row for row in storage["sensors"]
        if row.get("controller_uuid") == controller_uuid
        and row.get("sensor_id") == 5
    ]
    assert len(sensors) == 1
    assert sensors[0].get("profile_id") == 1

    profiles = [
        row for row in storage["scaling_profiles"]
        if row.get("profile_id") == sensors[0]["profile_id"]
    ]
    assert len(profiles) == 1
    assert profiles[0]["unit"] == "C"
    assert profiles[0]["raw_min"] == 0
    assert profiles[0]["raw_max"] == 65535
    assert profiles[0]["eng_min"] == 0.0
    assert profiles[0]["eng_max"] == 1000.0

    states = [
        row for row in storage["process_states"]
        if row.get("state_id") == 21
    ]
    assert len(states) == 1
    assert states[0]["io_refs"]["sensors"] == [{
        "sensor_id": 5,
        "controller_uuid": controller_uuid,
    }]


def _route_rank(row, ranks):
    return ranks.get(str(row.get("controller_uuid")), 9999)


def _register_handler(state, command, handler):
    state["commands"][command] = handler
    return True


def _register_callback(state, name, handler):
    state[name].append(handler)
    return True


def _publish_command(*_args, **_kwargs):
    return {"ok": True}


def _mqtt_api(state):
    return {
        "publish_command": _publish_command,
        "register_command_handler": partial(
            _register_handler, state),
        "register_ack_handler": partial(
            _register_callback, state, "acks"),
        "register_connect_handler": partial(
            _register_callback, state, "connects"),
        "register_command_guard": partial(
            _register_callback, state, "guards"),
        "register_event_handler": partial(
            _register_callback, state, "events"),
    }


def _patch_command(message_id, payload):
    return {
        "command": "distributed.config.patch",
        "message_id": message_id,
        "source_node": "master",
        "payload": copy.deepcopy(payload),
    }


def test_storage_runtime_roundtrip_has_one_fingerprint():
    storage = _load_v9()
    runtime = _runtime_v9()
    assert type_library.verify_storage_config_data(storage)["status"] == "OK"
    assert type_library.verify_storage_config_data(runtime)["status"] == "FAILED"
    projected = type_library.project_config_for_storage(runtime)
    fingerprints = {
        type_library.config_fingerprint(storage),
        type_library.config_fingerprint(runtime),
        type_library.config_fingerprint(projected),
    }
    assert len(fingerprints) == 1
    assert "rows" not in projected["sensors"][0]
    assert "rows" not in projected["actuators"][0]

    reordered = _load_v9()
    reordered["site_controller_map"] = list(
        reversed(reordered["site_controller_map"]))
    route_order = {
        str(row["controller_uuid"]): index
        for index, row in enumerate(reordered["site_controller_map"])
    }
    for section in ("sensors", "actuators"):
        reordered[section] = sorted(
            reordered[section],
            key=partial(_route_rank, ranks=route_order),
        )
    reordered_runtime = type_library.normalize_config_for_runtime(
        reordered, representation="storage")
    assert type_library.project_config_for_storage(reordered_runtime) == reordered


def test_node_and_cross_shards_use_v9_and_v8_storage():
    runtime = _runtime_v9()
    node_runtime = build_local_execution_shard(runtime, "fn-01")
    cross_runtime = build_cross_execution_shard(runtime, "fn-01")
    node_storage = type_library.project_config_for_storage(node_runtime)
    cross_storage = type_library.project_config_for_storage(cross_runtime)
    assert node_storage["meta"][0]["version"] == 9
    assert cross_storage["meta"][0]["version"] == 8
    assert type_library.verify_storage_config_data(node_storage)["status"] == "OK"
    assert type_library.verify_storage_config_data(cross_storage)["status"] == "OK"


def test_fn02_profile_is_deterministic_flat_v9():
    storage = _load_fn02_v9()
    with open(V9_FN02_PATH, "r", encoding="utf-8") as handle:
        assert handle.read() == render_config()
    assert storage["meta"][0]["version"] == 9
    assert storage["meta"][0]["node_id"] == "fn-02"
    assert storage["sensors"] == []
    assert len(storage["actuators"]) == 3
    assert all("rows" not in row for row in storage["actuators"])
    assert type_library.verify_storage_config_data(storage)["status"] == "OK"
    runtime = type_library.normalize_config_for_runtime(
        storage, representation="storage")
    projected = type_library.project_config_for_storage(runtime)
    assert type_library.config_fingerprint(projected) \
        == type_library.config_fingerprint(storage)
    candidate = copy.deepcopy(runtime)
    candidate["actuators"][0]["rows"][2]["address"] = 535
    operations = diff_configs(runtime, candidate, representation="runtime")
    changed = [
        item for item in operations
        if item.get("section") == "actuators"
    ]
    assert len(changed) == 1
    assert changed[0]["entity_key"] \
        == "actuator:06ab4528-f8a3-5ec7-a91f-eea79d60761e:3"


def test_stable_selectors_drive_runtime_changes():
    storage = _load_v9()
    center = data_center.create_data_center_context()
    committed = data_center.commit_full_config(
        center, storage, representation="storage", source="test")
    assert committed["status"] == "OK"
    controller_uuid = "62671112-766c-59ed-b0ff-30a49d48f747"
    changes = (
        ({"section": "controllers", "controller_uuid": controller_uuid,
          "field": "device_name", "operation_kind": "controller_update"},
         "wago_2_runtime"),
        ({"section": "sensors", "controller_uuid": controller_uuid,
          "sensor_id": 1, "field": "address"}, 101),
        ({"section": "actuators", "controller_uuid": controller_uuid,
          "actuator_id": 3, "field": "address"}, 701),
        ({"section": "processes", "p_id": 1,
          "field": "process_location"}, "Runtime Reactor"),
        ({"section": "process_states", "state_id": 1,
          "field": "parameters.target_temp_c"}, 555.0),
        ({"section": "rules", "rule_id": 1,
          "field": "when[0].value"}, 541.0),
    )
    for selector, value in changes:
        found, before = data_center.heap_snapshot(center)
        assert found
        path = compatibility.normalize_patch_selector(
            before, selector, external=True)
        result = data_center.request_write_commit(
            center, path, value, source="test", verify=True)
        assert result["status"] == "OK", result
    found, after = data_center.heap_snapshot(center)
    assert found
    assert type_library.verify_runtime_config_data(after)["status"] == "OK"
    assert type_library.config_fingerprint(storage) != type_library.config_fingerprint(after)


def test_numeric_paths_and_ownership_updates_fail_closed():
    runtime = _runtime_v9()
    numeric_rejected = False
    try:
        compatibility.normalize_patch_selector(
            runtime, "processes[0].process_location", external=True)
    except ValueError:
        numeric_rejected = True
    assert numeric_rejected

    owner_rejected = False
    try:
        compatibility.normalize_patch_selector(
            runtime,
            {"section": "processes", "p_id": 1,
             "field": "owner_node_id"},
            external=True)
    except ValueError:
        owner_rejected = True
    assert owner_rejected


def test_process_file_diff_uses_stable_p_id_and_preserves_owner():
    runtime = _runtime_v9()
    candidate = copy.deepcopy(runtime)
    candidate["processes"][0]["process_location"] = "File E2E Reactor"
    operations = diff_configs(runtime, candidate, representation="runtime")
    process_operations = [
        operation for operation in operations
        if operation.get("section") == "processes"]
    assert len(process_operations) == 1
    operation = process_operations[0]
    assert operation["entity_key"] == "process:1"
    assert operation["value"]["payload"]["owner_node_id"] == "fn-01"
    rebuilt = apply_operations(runtime, operations, representation="runtime")
    assert rebuilt["processes"][0]["process_location"] == "File E2E Reactor"
    assert rebuilt["processes"][0]["owner_node_id"] == "fn-01"


def test_native_mqtt_patch_is_atomic_idempotent_and_fail_closed():
    runtime = _runtime_v9()
    runtime_state = {
        "config": copy.deepcopy(runtime),
        "revision": 1,
        "commits": 0,
        "last_kwargs": {},
    }
    mqtt_state = {
        "commands": {}, "acks": [], "connects": [],
        "guards": [], "events": [],
    }

    def get_snapshot():
        return copy.deepcopy(runtime_state["config"])

    def get_revision():
        return runtime_state["revision"]

    def get_fingerprint():
        return type_library.config_fingerprint(runtime_state["config"])

    def request_replace(config, **kwargs):
        assert kwargs.get("source") == "coordinator_replication"
        assert kwargs.get("representation") == "runtime"
        assert type_library.verify_runtime_config_data(config)["status"] == "OK"
        runtime_state["config"] = copy.deepcopy(config)
        runtime_state["revision"] += 1
        runtime_state["commits"] += 1
        runtime_state["last_kwargs"] = copy.deepcopy(kwargs)
        return {
            "ok": True,
            "revision": runtime_state["revision"],
            "fingerprint": get_fingerprint(),
        }

    temporary = tempfile.TemporaryDirectory(prefix="v9-native-patch-")
    journal_path = os.path.join(temporary.name, "worker.sqlite3")
    api = create_distributed_coordinator(
        options={
            "enabled": True,
            "role": "worker",
            "node_id": "fn-01",
            "coordinator_node_id": "master",
            "journal_path": journal_path,
            "config_authority": "master_global",
            "conflict_policy": "master_wins",
        },
        mqtt_api=_mqtt_api(mqtt_state),
        config_capabilities={
            "get_snapshot": get_snapshot,
            "get_revision": get_revision,
            "get_fingerprint": get_fingerprint,
            "request_replace": request_replace,
        },
    )
    context = api["context"]
    base_fingerprint = get_fingerprint()
    with context["lock"]:
        context["coordinator_term"] = 1
        context["authority_revision"] = 1
        context["authority_fingerprint"] = "a" * 64
        context["node_scope_revision"] = 1
        context["node_shard_fingerprint"] = base_fingerprint
    registration = protocol_contracts.normalize_registration(
        _node_payload(context))
    capabilities = registration["capabilities"]
    assert registration["contracts_version"] == "2.0"
    assert registration["type_schema_version"] == "2.0.0"
    assert registration["config_norm_version"] == (
        protocol_contracts.CONFIG_NORM_VERSION)
    assert capabilities["config_scope_operations_v1"] is True
    assert capabilities["node_patch"] is True
    assert capabilities["cross_runtime"] is False
    assert capabilities["cross_patch"] is False
    assert capabilities["canonical_config_v9"] is True
    assert capabilities["cross_full"] is False
    assert capabilities["cross_fact_consumer"] is False
    assert capabilities["master_automation_execution"] is False

    def cross_status():
        return {
            "enabled": True,
            "revision": 0,
            "fingerprint": "",
            "authority_epoch": 0,
            "execution_epoch": 0,
        }

    api["set_cross_callbacks"](status_reader=cross_status)
    active_registration = protocol_contracts.normalize_registration(
        _node_payload(context))
    active_capabilities = active_registration["capabilities"]
    assert active_capabilities["cross_runtime"] is True
    assert active_capabilities["cross_patch"] is True
    assert active_capabilities["cross_full"] is True
    assert active_capabilities["cross_fact_consumer"] is True

    candidate = copy.deepcopy(runtime)
    candidate["processes"][0]["process_location"] = "Native MQTT Reactor"
    operations = list(diff_configs(
        runtime, candidate, representation="runtime"))
    target_fingerprint = type_library.config_fingerprint(candidate)
    payload = {
        "coordinator_term": 2,
        "authority_revision": 2,
        "authority_fingerprint": "b" * 64,
        "scope_id": "node:fn-01",
        "base_scope_revision": 1,
        "base_scope_fingerprint": base_fingerprint,
        "target_scope_revision": 2,
        "target_scope_fingerprint": target_fingerprint,
        "operations": operations,
    }
    handler = mqtt_state["commands"]["distributed.config.patch"]
    command = _patch_command("native-patch-success", payload)
    result = handler(command)
    assert result["ok"], result
    assert result["transport_mode"] == "differential_patch"
    assert result["command"] == "distributed.config.patch"
    assert result["operation_count"] == len(operations) > 0
    assert result["target_scope_revision"] == 2
    assert result["target_scope_fingerprint"] == target_fingerprint
    assert runtime_state["commits"] == 1
    assert runtime_state["config"]["processes"][0]["process_location"] \
        == "Native MQTT Reactor"
    assert context["node_scope_revision"] == 2
    assert context["node_shard_fingerprint"] == target_fingerprint
    assert context["authority_revision"] == 2

    canonical_conflict = copy.deepcopy(payload)
    canonical_conflict["scope"] = "node:fn-02"
    conflict_result = handler(_patch_command(
        "native-patch-scope-conflict", canonical_conflict))
    assert not conflict_result["ok"] and conflict_result["code"] == "scope"

    missing_scope = copy.deepcopy(payload)
    missing_scope.pop("scope_id")
    missing_result = handler(_patch_command(
        "native-patch-scope-missing", missing_scope))
    assert not missing_result["ok"] and missing_result["code"] == "scope"

    duplicate = handler(command)
    assert duplicate["ok"] and duplicate["duplicate"]
    assert runtime_state["commits"] == 1

    connection = sqlite3.connect(journal_path)
    row = connection.execute(
        "SELECT state, result_json FROM inbox WHERE message_id=?",
        ("native-patch-success",),
    ).fetchone()
    connection.close()
    persisted_result = json.loads(row[1])
    assert row[0] == "DONE"
    assert persisted_result["transport_mode"] == "differential_patch"
    assert persisted_result["operation_count"] == len(operations)

    current = copy.deepcopy(runtime_state["config"])
    next_candidate = copy.deepcopy(current)
    next_candidate["process_states"][0]["parameters"]["target_temp_c"] = 557.0
    next_operations = list(diff_configs(
        current, next_candidate, representation="runtime"))
    next_fingerprint = type_library.config_fingerprint(next_candidate)
    mismatch_payload = {
        "coordinator_term": 3,
        "authority_revision": 3,
        "authority_fingerprint": "c" * 64,
        "scope_id": "node:fn-01",
        "base_scope_revision": 1,
        "base_scope_fingerprint": target_fingerprint,
        "target_scope_revision": 3,
        "target_scope_fingerprint": next_fingerprint,
        "operations": next_operations,
    }
    base_mismatch = handler(_patch_command(
        "native-patch-base-mismatch", mismatch_payload))
    assert not base_mismatch["ok"]
    assert base_mismatch["code"] == "base_scope_revision"
    assert runtime_state["commits"] == 1
    assert context["authority_revision"] == 2

    hash_payload = copy.deepcopy(mismatch_payload)
    hash_payload["base_scope_revision"] = 2
    hash_payload["operations"][0]["expected_value_hash"] = "0" * 64
    hash_mismatch = handler(_patch_command(
        "native-patch-hash-mismatch", hash_payload))
    assert not hash_mismatch["ok"]
    assert hash_mismatch["code"] == "patch_operations"
    assert runtime_state["commits"] == 1
    assert context["authority_revision"] == 2

    legacy_payload = copy.deepcopy(payload)
    legacy_payload.pop("scope_id")
    legacy_payload["scope"] = "node:fn-01"
    legacy_payload["base_scope_revision"] = 2
    legacy_payload["base_scope_fingerprint"] = target_fingerprint
    legacy_candidate = copy.deepcopy(runtime_state["config"])
    legacy_candidate["processes"][0]["process_location"] = "Legacy Scope Alias"
    legacy_payload["operations"] = list(diff_configs(
        runtime_state["config"], legacy_candidate, representation="runtime"))
    legacy_payload["target_scope_revision"] = 3
    legacy_payload["target_scope_fingerprint"] = \
        type_library.config_fingerprint(legacy_candidate)
    legacy_payload["authority_revision"] = 3
    legacy_payload["authority_fingerprint"] = "c" * 64
    legacy_result = handler(_patch_command(
        "native-patch-legacy-scope", legacy_payload))
    assert legacy_result["ok"]
    assert runtime_state["commits"] == 2

    api["stop"]()
    temporary.cleanup()


def test_node_payload_keeps_authoritative_pair_during_local_first_divergence():
    runtime_state = {
        "config": _runtime_v9(),
        "revision": 1,
        "commits": 0,
    }
    mqtt_state = {
        "commands": {}, "acks": [], "connects": [],
        "guards": [], "events": [],
    }

    def get_snapshot():
        return copy.deepcopy(runtime_state["config"])

    def get_revision():
        return runtime_state["revision"]

    def get_fingerprint():
        return type_library.config_fingerprint(runtime_state["config"])

    def request_replace(config, **kwargs):
        assert kwargs.get("source") == "coordinator_replication"
        assert kwargs.get("representation") == "storage"
        runtime_state["config"] = type_library.normalize_config_for_runtime(
            config, representation="storage")
        runtime_state["revision"] += 1
        runtime_state["commits"] += 1
        return {
            "ok": True,
            "revision": runtime_state["revision"],
            "fingerprint": get_fingerprint(),
        }

    temporary = tempfile.TemporaryDirectory(prefix="v9-node-pair-")
    journal_path = os.path.join(temporary.name, "worker.sqlite3")
    api = create_distributed_coordinator(
        options={
            "enabled": True,
            "role": "worker",
            "node_id": "fn-01",
            "coordinator_node_id": "master",
            "journal_path": journal_path,
            "config_authority": "master_global",
            "conflict_policy": "master_wins",
            "config_sync_mode": "local_first",
        },
        mqtt_api=_mqtt_api(mqtt_state),
        config_capabilities={
            "get_snapshot": get_snapshot,
            "get_revision": get_revision,
            "get_fingerprint": get_fingerprint,
            "request_replace": request_replace,
        },
    )
    context = api["context"]
    base_fingerprint = get_fingerprint()

    initial = _node_payload(context)
    assert initial["node_scope_revision"] == 0
    assert initial["node_shard_fingerprint"] == base_fingerprint
    assert initial["node_replica"] == {
        "scope": "node:fn-01",
        "authority_epoch": 0,
        "revision": 0,
        "fingerprint": base_fingerprint,
    }
    assert initial["live_config_fingerprint"] == base_fingerprint
    assert get_metadata(
        context["store"], "node_shard_fingerprint", "") == base_fingerprint
    assert int(get_metadata(
        context["store"], "node_scope_revision", -1)) == 0

    with context["lock"]:
        context["coordinator_term"] = 1
        context["authority_revision"] = 14
        context["authority_fingerprint"] = "a" * 64
    _remember_node_scope(
        context, {"target_scope_revision": 14}, base_fingerprint)

    candidate = copy.deepcopy(runtime_state["config"])
    candidate["processes"][0]["process_location"] = \
        "Local First Preapplied"
    runtime_state["config"] = candidate
    runtime_state["revision"] += 1
    target_fingerprint = get_fingerprint()
    assert target_fingerprint != base_fingerprint

    divergent = _node_payload(context)
    assert divergent["node_scope_revision"] == 14
    assert divergent["node_shard_fingerprint"] == base_fingerprint
    assert divergent["node_replica"] == {
        "scope": "node:fn-01",
        "authority_epoch": 0,
        "revision": 14,
        "fingerprint": base_fingerprint,
    }
    assert divergent["live_config_fingerprint"] == target_fingerprint

    target_storage = type_library.project_config_for_storage(candidate)
    full_payload = {
        "coordinator_term": 2,
        "authority_revision": 15,
        "authority_fingerprint": "b" * 64,
        "shard_fingerprint": target_fingerprint,
        "target_scope_revision": 15,
        "target_scope_fingerprint": target_fingerprint,
        "config": target_storage,
    }
    full_result = mqtt_state["commands"]["distributed.config.apply"]({
        "command": "distributed.config.apply",
        "message_id": "local-first-full-convergence",
        "source_node": "master",
        "payload": full_payload,
    })
    assert full_result["ok"], full_result
    assert runtime_state["commits"] == 1

    converged = _node_payload(context)
    assert converged["node_scope_revision"] == 15
    assert converged["node_shard_fingerprint"] == target_fingerprint
    assert converged["node_replica"] == {
        "scope": "node:fn-01",
        "authority_epoch": 0,
        "revision": 15,
        "fingerprint": target_fingerprint,
    }
    assert converged["live_config_fingerprint"] == target_fingerprint

    api["stop"]()
    temporary.cleanup()


def main():
    tests = (
        test_storage_runtime_roundtrip_has_one_fingerprint,
        test_node_and_cross_shards_use_v9_and_v8_storage,
        test_state21_temperature_sensor_uses_engineering_profile,
        test_fn02_profile_is_deterministic_flat_v9,
        test_stable_selectors_drive_runtime_changes,
        test_numeric_paths_and_ownership_updates_fail_closed,
        test_process_file_diff_uses_stable_p_id_and_preserves_owner,
        test_native_mqtt_patch_is_atomic_idempotent_and_fail_closed,
        test_node_payload_keeps_authoritative_pair_during_local_first_divergence,
    )
    for test_function in tests:
        test_function()
        print("PASS " + test_function.__name__)
    print("PASS V9 Worker storage/runtime contract ({} checks)".format(len(tests)))


if __name__ == "__main__":
    main()




