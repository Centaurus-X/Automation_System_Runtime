# -*- coding: utf-8 -*-

"""Authenticated MQTT coverage for Authority V5 transfer and restore."""

import copy
import json
import os
import sys
import tempfile

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.distributed_coordinator import create_distributed_coordinator
from src.library import _generic_type_library as type_authority
from src.library._generic_system_contract_library import persistence_contracts


CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "device_state_data_v9_node_fn-01.json",
)


def _load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _register_command(state, command, handler):
    state["commands"][command] = handler
    return True


def _register_single(state, name, handler):
    state[name].append(handler)
    return True


def _publish_command(*_args, **_kwargs):
    return {"ok": True}


def _mqtt_api(state):
    return {
        "publish_command": _publish_command,
        "register_command_handler": partial(_register_command, state),
        "register_ack_handler": partial(_register_single, state, "acks"),
        "register_connect_handler": partial(
            _register_single, state, "connects"),
        "register_command_guard": partial(_register_single, state, "guards"),
        "register_event_handler": partial(_register_single, state, "events"),
        "publish_event": partial(_register_single, state, "published"),
    }


def _read_config(state):
    return copy.deepcopy(state["config"])


def _read_revision(state):
    return int(state["revision"])


def _read_fingerprint(state):
    return type_authority.config_fingerprint(state["config"])


def _replace_config(state, config, **kwargs):
    state["config"] = copy.deepcopy(config)
    state["revision"] += 1
    state["sem_writes"].append({
        "config": copy.deepcopy(config),
        "kwargs": copy.deepcopy(kwargs),
    })
    return {
        "ok": True,
        "operation_id": kwargs.get("operation_id"),
        "revision": state["revision"],
        "fingerprint": type_authority.config_fingerprint(config),
    }


def _safety_snapshot():
    return {
        "enabled": True,
        "gates": {
            "actuator_tasks": True,
            "external_commits": True,
        },
    }


def _constant(value, *_args, **_kwargs):
    return copy.deepcopy(value)


def _config_capabilities(state):
    return {
        "get_snapshot": partial(_read_config, state),
        "get_revision": partial(_read_revision, state),
        "get_fingerprint": partial(_read_fingerprint, state),
        "get_safety_snapshot": _safety_snapshot,
        "request_replace": partial(_replace_config, state),
        "request_scope_operations": partial(_constant, {"ok": True}),
        "request_action_plan": partial(_constant, {"ok": True}),
        "authorize_external_scope": partial(_constant, {"ok": True}),
    }


def _mqtt_state():
    return {
        "commands": {},
        "acks": [],
        "connects": [],
        "guards": [],
        "events": [],
        "published": [],
    }


def _command(name, message_id, payload, authority_term):
    return {
        "command": name,
        "message_id": message_id,
        "source_node": "master",
        "metadata": {"authority_term": int(authority_term)},
        "payload": copy.deepcopy(payload),
    }


def _create_worker(directory, config, mqtt_state, config_state, authority_mode):
    master_global = authority_mode == "master_global"
    return create_distributed_coordinator(
        options={
            "enabled": True,
            "role": "worker",
            "node_id": "fn-01",
            "coordinator_node_id": "master",
            "journal_path": os.path.join(directory, "worker.sqlite3"),
            "registration_interval_s": 30.0,
            "config_authority": authority_mode,
            "config_sync_mode": "global_first" if master_global else "local_first",
            "conflict_policy": "master_wins" if master_global else "worker_authoritative",
        },
        mqtt_api=_mqtt_api(mqtt_state),
        config_capabilities=_config_capabilities(config_state),
    )


def test_authenticated_authority_transfer_applies_sem_and_fences_term():
    master_config = _load_config()
    node_config = copy.deepcopy(master_config)
    node_config["meta"][0]["description"] = "Node-selected MQTT authority truth"
    node_fingerprint = type_authority.config_fingerprint(node_config)
    mqtt_state = _mqtt_state()
    config_state = {
        "config": copy.deepcopy(master_config),
        "revision": 5,
        "sem_writes": [],
    }
    with tempfile.TemporaryDirectory(prefix="authority_mqtt_a5_") as directory:
        api = _create_worker(
            directory, master_config, mqtt_state, config_state, "master_global")
        try:
            handler = mqtt_state["commands"]["distributed.authority.transfer"]
            payload = {
                "transfer_id": "transfer-mqtt-a5",
                "target_mode": "node_local",
                "source_truth": "node_local",
                "operator_confirmed": True,
                "operator_subject": "operator:test",
                "master_snapshot": master_config,
                "node_snapshot": node_config,
                "authority_epoch": 1,
                "fingerprint": node_fingerprint,
            }
            result = handler(_command(
                "distributed.authority.transfer",
                "authority-transfer-mqtt-1",
                payload,
                5,
            ))
            assert result["ok"] is True, result
            assert result["accepted"] is True
            assert result["authority_epoch"] == 1
            assert result["fingerprint"] == node_fingerprint
            assert len(config_state["sem_writes"]) == 1
            sem_kwargs = config_state["sem_writes"][0]["kwargs"]
            assert sem_kwargs["source"] == "authority_transfer"
            assert sem_kwargs["transfer_id"] == "transfer-mqtt-a5"
            assert api["get_scope_head"]("node:fn-01")["authority_mode"] == "node_local"
            assert api["snapshot"]()["config_authority"] == "node_local"
            stale = copy.deepcopy(payload)
            stale["target_mode"] = "master_global"
            stale["source_truth"] = "master_global"
            rejected = handler(_command(
                "distributed.authority.transfer",
                "authority-transfer-mqtt-stale",
                stale,
                4,
            ))
            assert rejected["ok"] is False
            assert rejected["code"] == "stale_fencing_term"
        finally:
            api["stop"]()


def test_restore_mqtt_remains_quarantined_until_verified_preflight():
    current_config = _load_config()
    restored_config = copy.deepcopy(current_config)
    restored_config["meta"][0]["description"] = "Restored MQTT truth"
    restored_fingerprint = type_authority.config_fingerprint(restored_config)
    snapshot = persistence_contracts.make_scope_snapshot(
        restored_config,
        scope="node:fn-01",
        authority_epoch=0,
        scope_revision=0,
        execution_epoch=0,
        fingerprint=restored_fingerprint,
    )
    mqtt_state = _mqtt_state()
    config_state = {
        "config": copy.deepcopy(current_config),
        "revision": 8,
        "sem_writes": [],
    }
    with tempfile.TemporaryDirectory(prefix="authority_mqtt_a6_") as directory:
        api = _create_worker(
            directory, current_config, mqtt_state, config_state, "node_local")
        try:
            request_handler = mqtt_state["commands"]["distributed.restore.request"]
            request_result = request_handler(_command(
                "distributed.restore.request",
                "restore-request-mqtt-1",
                {
                    "restore_id": "restore-mqtt-a6",
                    "scope": "node:fn-01",
                    "authority_epoch": 1,
                    "selected_source": "master_mirror",
                    "snapshot": snapshot,
                    "operator_confirmed": True,
                    "identity_confirmed": True,
                },
                7,
            ))
            assert request_result["ok"] is True, request_result
            assert request_result["hardware_writes_enabled"] is False
            transitions = api["context"]["authority_transitions"]
            assert transitions["hardware_writes_allowed"]() is False
            assert len(config_state["sem_writes"]) == 1
            activate_handler = mqtt_state["commands"]["distributed.restore.activate"]
            activate_result = activate_handler(_command(
                "distributed.restore.activate",
                "restore-activate-mqtt-1",
                {
                    "restore_id": "restore-mqtt-a6",
                    "authority_epoch": 1,
                    "fingerprint": restored_fingerprint,
                    "preflight_required": True,
                },
                7,
            ))
            assert activate_result["ok"] is True, activate_result
            assert activate_result["preflight_ok"] is True
            assert activate_result["hardware_writes_enabled"] is True
            assert transitions["hardware_writes_allowed"]() is True
            assert type_authority.config_fingerprint(
                config_state["config"]) == restored_fingerprint
        finally:
            api["stop"]()


def main():
    tests = (
        test_authenticated_authority_transfer_applies_sem_and_fences_term,
        test_restore_mqtt_remains_quarantined_until_verified_preflight,
    )
    for test_function in tests:
        test_function()
        print("PASS " + test_function.__name__)
    print("PASS Authority MQTT transition suite ({} checks)".format(len(tests)))


if __name__ == "__main__":
    main()
