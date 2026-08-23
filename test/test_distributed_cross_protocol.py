# -*- coding: utf-8 -*-

"""Distributed cross-shard separation and master execution fencing test."""

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
from src.library._generic_system_contract_library.config_sharding_contracts import (
    build_cross_execution_shard,
    build_local_execution_shard,
    classify_config_scopes,
    diff_configs,
)
from src.library import _generic_type_library as type_library


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


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
            _register_single, state, "connects"
        ),
        "register_command_guard": partial(_register_single, state, "guards"),
        "register_event_handler": partial(_register_single, state, "events"),
        "publish_event": partial(_register_single, state, "published"),
    }


def _config_capabilities(config):
    return {
        "get_snapshot": partial(copy.deepcopy, config),
        "get_revision": partial(_constant, 7),
        "request_replace": partial(_constant, {"ok": True}),
        "request_action_plan": partial(_constant, {"ok": True}),
        "authorize_external_scope": partial(
            _constant,
            {"ok": True, "code": "external_commit_allowed"},
        ),
    }


def _constant(value, *_args, **_kwargs):
    return copy.deepcopy(value)


def _cross_apply(captured, status, package):
    captured.append(copy.deepcopy(package))
    status.update({
        "revision": package["revision"],
        "fingerprint": package["fingerprint"],
        "execution_epoch": package["execution_epoch"],
        "authority_config": copy.deepcopy(package["config"]),
    })
    return {"ok": True, "status": "applied"}


def _cross_event_reject(captured, event, **kwargs):
    captured.append({
        "event": copy.deepcopy(event),
        "source_node": kwargs.get("source_node"),
    })
    return {"ok": False, "code": "unknown_fact"}


def _capture_dispatch(captured, task):
    captured.append(copy.deepcopy(task))
    return True


def _command(command, message_id, payload):
    return {
        "command": command,
        "message_id": message_id,
        "source_node": "master",
        "payload": copy.deepcopy(payload),
    }


def _create_worker(directory, config, mqtt_state):
    return create_distributed_coordinator(
        options={
            "enabled": True,
            "role": "worker",
            "node_id": "fn-01",
            "coordinator_node_id": "master",
            "journal_path": os.path.join(directory, "worker.sqlite3"),
            "registration_interval_s": 30.0,
            "local_automation_enabled": True,
            "config_sync_mode": "local_first",
        },
        mqtt_api=_mqtt_api(mqtt_state),
        config_capabilities=_config_capabilities(config),
    )


def _test_cross_callbacks(api, mqtt_state, global_config):
    assert not mqtt_state["events"]
    applied = []
    rejected = []
    cross_source = build_cross_execution_shard(global_config, "fn-01")
    cross_config = type_library.project_cross_v8_for_storage(cross_source)
    cross_runtime = type_library.normalize_cross_v8_for_runtime(
        cross_config, representation="storage")
    baseline = copy.deepcopy(cross_runtime)
    baseline["rules"][0]["enabled"] = not bool(
        baseline["rules"][0]["enabled"])
    baseline_fingerprint = type_library.config_fingerprint(baseline)
    fingerprint = type_library.config_fingerprint(cross_runtime)
    classification = type_library.fingerprint(
        classify_config_scopes(cross_runtime))
    operations = diff_configs(
        baseline,
        cross_runtime,
        allow_cross_v8=True,
        representation="runtime",
    )
    assert operations
    status = {
        "enabled": True,
        "node_id": "fn-01",
        "domain_id": "cross-default",
        "authority_epoch": 0,
        "revision": 0,
        "execution_epoch": 0,
        "fingerprint": baseline_fingerprint,
        "authority_config": copy.deepcopy(baseline),
    }
    api["set_cross_callbacks"](
        event_handler=partial(_cross_event_reject, rejected),
        config_applier=partial(_cross_apply, applied, status),
        status_reader=partial(_constant, status),
    )
    api["set_cross_callbacks"](
        event_handler=partial(_cross_event_reject, rejected),
        config_applier=partial(_cross_apply, applied, status),
        status_reader=partial(_constant, status),
    )
    assert len(mqtt_state["events"]) == 1

    event = {
        "event_type": "SENSOR_VALUE_UPDATE",
        "message_id": "cross-event-1",
        "source_node": "fn-02",
        "payload": {
            "controller_uuid": "remote-controller",
            "sensor_id": 1,
            "value": 12,
        },
    }
    assert mqtt_state["events"][0](event) is False
    assert len(rejected) == 1
    assert api["snapshot"]()["statistics"]["remote_sensor_rejected"] == 1

    payload = {
        "coordinator_term": 1,
        "base_authority_epoch": 0,
        "base_scope_revision": 0,
        "execution_epoch": 0,
        "scope": "cross:cross-default",
        "operation_format": "config_scope_operations_v1",
        "operations": operations,
        "classification_fingerprint": classification,
        "target_scope_fingerprint": fingerprint,
        "cross_fingerprint": fingerprint,
    }
    handler = mqtt_state["commands"]["distributed.cross.apply"]
    result = handler(_command(
        "distributed.cross.apply", "cross-apply-1", payload
    ))
    assert result["ok"] and result["code"] == "accepted", result
    assert result["scope_revision"] == 1
    assert result["execution_epoch"] == 0
    assert len(applied) == 1
    assert applied[0]["revision"] == 1
    assert applied[0]["fingerprint"] == fingerprint
    snapshot = api["snapshot"]()
    assert snapshot["statistics"]["cross_replications_applied"] == 1
    assert snapshot["journal"]["counts"]["mirror_outbox"]["pending"] == 1
    mismatched = copy.deepcopy(payload)
    mismatched["base_scope_revision"] = 1
    mismatched["operations"] = diff_configs(
        cross_runtime,
        baseline,
        allow_cross_v8=True,
        representation="runtime",
    )
    mismatched["classification_fingerprint"] = "0" * 64
    mismatched["target_scope_fingerprint"] = baseline_fingerprint
    mismatched["cross_fingerprint"] = baseline_fingerprint
    rejected_classification = handler(_command(
        "distributed.cross.apply", "cross-apply-classification-mismatch", mismatched
    ))
    assert not rejected_classification["ok"]
    assert rejected_classification["code"] == "classification_mismatch"
    assert len(applied) == 1

def _cross_actuator_task(controller_uuid, operation_id, execution_epoch, proof=None):
    metadata = {
        "execution_scope": "cross",
        "cross_domain_id": "cross-default",
        "cross_owner_node_id": "fn-02",
        "execution_epoch": execution_epoch,
    }
    if proof is not None:
        metadata["registry_transition"] = copy.deepcopy(proof)
    return {
        "task_uuid": operation_id,
        "controller_uuid": controller_uuid,
        "actuator_id": 1,
        "control_value": 2,
        "source": "cross_automation:cross-default",
        "execution_scope": "cross",
        "execution_epoch": execution_epoch,
        "meta": metadata,
    }


def _cross_execute_payload(operation_id, task):
    return {
        "coordinator_term": 1,
        "authority_revision": 0,
        "require_hardware_readback": True,
        "operation_id": operation_id,
        "request_source_node": "fn-02",
        "task": copy.deepcopy(task),
    }


def _test_master_execution_fence(api, mqtt_state, local_config):
    dispatched = []
    api["set_local_dispatcher"](partial(_capture_dispatch, dispatched))
    controller_uuid = local_config["controllers"][0]["controller_uuid"]
    handler = mqtt_state["commands"]["distributed.actuator.execute"]
    automatic = {
        "coordinator_term": 1,
        "authority_revision": 0,
        "require_hardware_readback": True,
        "operation_id": "master-automation-1",
        "request_source_node": "master",
        "task": {
            "task_uuid": "master-automation-1",
            "controller_uuid": controller_uuid,
            "actuator_id": 1,
            "control_value": 1,
            "source": "automation_1",
        },
    }
    rejected = handler(_command(
        "distributed.actuator.execute", "master-automation-command", automatic
    ))
    assert not rejected["ok"]
    assert rejected["code"] == "master_execution_forbidden"
    assert not dispatched

    epoch_zero = _cross_actuator_task(
        controller_uuid, "cross-relay-epoch-0", 0)
    accepted_zero = handler(_command(
        "distributed.actuator.execute",
        "cross-relay-command-epoch-0",
        _cross_execute_payload("cross-relay-epoch-0", epoch_zero),
    ))
    assert accepted_zero["ok"] and accepted_zero["state"] == "DISPATCHED"
    assert len(dispatched) == 1

    epoch_one_without_proof = _cross_actuator_task(
        controller_uuid, "cross-relay-epoch-1-invalid", 1)
    rejected_advance = handler(_command(
        "distributed.actuator.execute",
        "cross-relay-command-epoch-1-invalid",
        _cross_execute_payload(
            "cross-relay-epoch-1-invalid", epoch_one_without_proof),
    ))
    assert not rejected_advance["ok"]
    assert rejected_advance["code"] == "execution_epoch_stale"
    assert len(dispatched) == 1

    transition_proof = {
        "old_owner_frozen_ack": True,
        "registry_persisted": True,
        "new_owner_node_id": "fn-02",
        "execution_epoch": 1,
        "registry_intent_id": "registry-cross-default-1",
    }
    epoch_one = _cross_actuator_task(
        controller_uuid,
        "cross-relay-epoch-1",
        1,
        proof=transition_proof,
    )
    accepted_one = handler(_command(
        "distributed.actuator.execute",
        "cross-relay-command-epoch-1",
        _cross_execute_payload("cross-relay-epoch-1", epoch_one),
    ))
    assert accepted_one["ok"] and accepted_one["state"] == "DISPATCHED"
    assert len(dispatched) == 2

    stale_epoch = _cross_actuator_task(
        controller_uuid, "cross-relay-stale-epoch", 0)
    rejected_stale = handler(_command(
        "distributed.actuator.execute",
        "cross-relay-command-stale-epoch",
        _cross_execute_payload("cross-relay-stale-epoch", stale_epoch),
    ))
    assert not rejected_stale["ok"]
    assert rejected_stale["code"] == "execution_epoch_stale"
    assert len(dispatched) == 2
    snapshot = api["snapshot"]()
    assert snapshot["journal"]["counts"]["registry_view"]["TOTAL"] == 1

def _run_test():
    with open(SOURCE_CONFIG, "r", encoding="utf-8") as handle:
        global_config = json.load(handle)
    local_config = build_local_execution_shard(global_config, "fn-01")
    mqtt_state = {
        "commands": {},
        "acks": [],
        "connects": [],
        "guards": [],
        "events": [],
        "published": [],
    }
    with tempfile.TemporaryDirectory(prefix="distributed_cross_") as directory:
        api = _create_worker(directory, local_config, mqtt_state)
        try:
            _test_cross_callbacks(api, mqtt_state, global_config)
            _test_master_execution_fence(api, mqtt_state, local_config)
        finally:
            api["stop"]()


if __name__ == "__main__":
    _run_test()
    print("PASS distributed cross callbacks, shards and master execution fence")








