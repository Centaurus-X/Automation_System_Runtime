# -*- coding: utf-8 -*-

"""Two-node config replication and remote actuator terminal-ACK test."""

import copy
import json
import os
import shutil
import sys
import tempfile
import threading
import time

from functools import partial
from queue import Empty


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._state_shard_manager import (
    build_controller_projection,
    get_node_controller_uuids,
)
from src.system.bootstrap_runtime_interface import (
    get_runtime_config_snapshot,
    get_runtime_snapshot,
    request_runtime_config_patch,
    start_runtime,
    stop_runtime,
)


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)
WORKER_STATE_ID = 99

BROKER = None


def _topic_matches(pattern, topic):
    wanted = str(pattern).split("/")
    actual = str(topic).split("/")
    for index, token in enumerate(wanted):
        if token == "#":
            return True
        if index >= len(actual):
            return False
        if token not in ("+", actual[index]):
            return False
    return len(wanted) == len(actual)


def _broker_create():
    return {"lock": threading.RLock(), "clients": [], "published": []}


def _transport_snapshot(context):
    return {
        "state": context["state"],
        "connected": context["connected"],
        "thread_alive": False,
        "outbound_queue_depth": 0,
    }


def _transport_start(context):
    with context["broker"]["lock"]:
        if context not in context["broker"]["clients"]:
            context["broker"]["clients"].append(context)
    context["state"] = "connected"
    context["connected"] = True
    context["on_connect"](_transport_snapshot(context))


def _transport_stop(context, **_kwargs):
    with context["broker"]["lock"]:
        if context in context["broker"]["clients"]:
            context["broker"]["clients"].remove(context)
    context["state"] = "stopped"
    context["connected"] = False
    return _transport_snapshot(context)


def _transport_wait(context, _timeout):
    return context["connected"]


def _transport_publish(context, topic, payload, **kwargs):
    message = {
        "topic": str(topic), "payload": bytes(payload),
        "qos": kwargs.get("qos", 0), "retain": kwargs.get("retain", False),
    }
    with context["broker"]["lock"]:
        context["broker"]["published"].append(copy.deepcopy(message))
        clients = tuple(context["broker"]["clients"])
    for client in clients:
        if not client["connected"]:
            continue
        if any(_topic_matches(pattern, topic) for pattern in client["subscriptions"]):
            client["on_message"](copy.deepcopy(message))
    return True


def _transport_subscribe(context, topic, **_kwargs):
    context["subscriptions"].add(str(topic))
    return True


def _transport_factory(**kwargs):
    context = {
        "broker": BROKER,
        "state": "created",
        "connected": False,
        "subscriptions": {
            str(item["topic"]) for item in kwargs.get("subscriptions") or []
        },
        "on_message": kwargs.get("on_message"),
        "on_connect": kwargs.get("on_connect"),
        "on_disconnect": kwargs.get("on_disconnect"),
    }
    return {
        "context": context,
        "start": partial(_transport_start, context),
        "stop": partial(_transport_stop, context),
        "publish": partial(_transport_publish, context),
        "subscribe": partial(_transport_subscribe, context),
        "wait_connected": partial(_transport_wait, context),
        "snapshot": partial(_transport_snapshot, context),
    }


def _fake_controller(**kwargs):
    event_queue = kwargs["event_queue_mbc"]
    resources = kwargs.get("runtime_resources") or {}
    while not kwargs["shutdown_event"].is_set():
        try:
            event = event_queue.get(timeout=0.05)
        except Empty:
            continue
        if event.get("_sentinel"):
            break
        payload = event.get("payload") or {}
        if event.get("event_type") == "CONFIG_SHARD_UPDATE":
            config = payload.get("config") or {}
            kwargs["config_data"].clear()
            kwargs["config_data"].update(copy.deepcopy(config))
            acknowledgement = payload.get("acknowledgement") or {}
            acknowledgement["result"] = {
                "ok": True, "revision": payload.get("revision")}
            signal = acknowledgement.get("event")
            if signal is not None:
                signal.set()
            continue
        if event.get("event_type") != "ACTUATOR_TASK":
            continue
        task = payload.get("task") or {}
        validator = resources.get("validate_actuator_write")
        report = resources.get("actuator_result_reporter")
        guard = validator(resources.get("worker_uuid"), task)
        assert guard.get("ok"), guard
        report({
            "distributed_operation_id": task["distributed_operation_id"],
            "task_uuid": task.get("task_uuid"),
            "controller_uuid": task.get("controller_uuid"),
            "controller_id": task.get("controller_id"),
            "actuator_id": task.get("actuator_id"),
            "write_succeeded": True,
            "readback": task.get("control_value"),
            "readback_checked": True,
            "readback_verified": True,
            "success": True,
        })


def _wait_for(predicate, timeout_s=8.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _distributed_snapshot(runtime):
    return runtime["distributed_coordinator"]["snapshot"]()


def _authority_revision(runtime):
    return _distributed_snapshot(runtime)["authority_revision"]


def _fingerprints_equal(left, right):
    return (
        _distributed_snapshot(left)["authority_fingerprint"]
        == _distributed_snapshot(right)["authority_fingerprint"]
        and _authority_revision(left) == _authority_revision(right)
    )


def _operation_state(runtime, operation_id):
    operation = runtime["distributed_coordinator"][
        "get_actuator_operation"](operation_id)
    return operation.get("state") if operation else None


def _main_context(node_id, role, **kwargs):
    config_path = kwargs.get("config_path")
    journal_path = kwargs.get("journal_path")
    return {
        "shutdown_event": threading.Event(),
        "app_config": {
            "app": {"site_id": "site-test", "node_id": node_id},
            "runtime": {
                "event_router": {"enabled": True, "worker_threads": 2,
                                 "ingress_timeout_s": 0.02},
                "value_store": {"persistence": {"enabled": False}},
                "data_center": {"json_path": config_path,
                                "verify_on_load": True, "verify_on_save": True,
                                "allow_legacy_import": True,
                                "legacy_import_profile": "lab_v2"},
            },
            "thread_management": {
                "ingress_timeout_s": 0.02,
                "pool_min_workers": 1, "pool_max_workers": 2,
                "controllers_enabled": role == "worker",
                "connections_enabled": False,
            },
            "state_event_management": {
                "ingress_timeout_s": 0.02, "request_timeout_s": 5.0,
                "persistence_enabled": False,
                "safety": {"enabled": False},
            },
            "mqtt_interface": {
                "enabled": True, "required": True,
                "heartbeat_interval_s": 30.0,
                "transport": {"tls": {"enabled": True}},
                "commands": {"allowed": ["runtime.status"]},
                "event_publication": {"event_types": [
                    "CONFIG_COMMIT_APPLIED", "REMOTE_ACTUATOR_RESULT",
                    "SENSOR_VALUE_UPDATE"]},
            },
            "distributed_runtime": {
                "enabled": True, "role": role,
                "config_authority": "master_global",
                "coordinator_node_id": "fn-01",
                "local_automation_enabled": role == "worker",
                "config_sync_mode": (
                    "local_first" if role == "worker" else "global_first"),
                "conflict_policy": "master_wins",
                "journal_path": journal_path,
                "replication_targets": ["fn-02"] if role == "coordinator" else [],
                "registration_interval_s": 0.2,
                "retry_interval_s": 0.05,
                "command_ttl_s": 5.0,
                "require_hardware_readback": True,
            },
            "shutdown": {"join_timeout_s": 3.0},
        },
    }


def _start_runtime_pair(directory):
    coordinator_config = os.path.join(directory, "coordinator.json")
    worker_config = os.path.join(directory, "worker.json")
    _write_test_config(coordinator_config)
    shutil.copy2(coordinator_config, worker_config)
    coordinator_main = _main_context(
        "fn-01", "coordinator", config_path=coordinator_config,
        journal_path=os.path.join(directory, "coordinator.sqlite3"))
    worker_main = _main_context(
        "fn-02", "worker", config_path=worker_config,
        journal_path=os.path.join(directory, "worker.sqlite3"))
    start_runtime(coordinator_main, mqtt_transport_factory=_transport_factory)
    start_runtime(
        worker_main, mqtt_transport_factory=_transport_factory,
        controller_target=_fake_controller)
    return coordinator_main, worker_main


def _write_test_config(path):
    with open(SOURCE_CONFIG, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    worker_uuid = get_node_controller_uuids(config, "fn-02")[0]
    template = next(
        row for row in config["process_states"] if int(row["state_id"]) == 9
    )
    state = copy.deepcopy(template)
    state.update({
        "state_id": WORKER_STATE_ID,
        "parameters": {"target_temp_c": 554.0},
        "io_refs": {
            "sensors": [],
            "actuators": [{
                "controller_uuid": worker_uuid,
                "actuator_id": 1,
            }],
        },
    })
    config["process_states"].append(state)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2, sort_keys=True)


def _verify_config_authority(coordinator, worker):
    assert _wait_for(partial(_fingerprints_equal, coordinator, worker))
    before = _distributed_snapshot(coordinator)["statistics"]
    staged_before = before["config_replications_staged"]
    cross_staged_before = before["cross_replications_staged"]
    time.sleep(0.45)
    after = _distributed_snapshot(coordinator)["statistics"]
    assert after["config_replications_staged"] == staged_before
    assert after["cross_replications_staged"] == cross_staged_before, {
        "before": before,
        "after": after,
        "worker": _distributed_snapshot(worker),
        "runtime": get_runtime_snapshot(worker),
    }
    rejected = request_runtime_config_patch(
        worker, "controllers.0.port", 504,
        source="forbidden_worker_write")
    assert rejected.get("ok") is False
    assert (rejected.get("safety") or {}).get("code") == "not_config_authority"
    proposal = worker["distributed_coordinator"]["request_action_plan"](
        [{"state_ids": [WORKER_STATE_ID],
          "parameters": {"target_temp_c": 557.0}}],
        operation_id="distributed-rule-plan-1",
    )
    assert proposal.get("ok"), proposal
    assert proposal.get("local_committed") is True
    assert (proposal.get("sync") or {}).get("mode") == "local_first"
    assert _wait_for(partial(_fingerprints_equal, coordinator, worker))
    assert _wait_for(partial(_proposal_converged, worker)), {
        "worker": _distributed_snapshot(worker),
        "coordinator": _distributed_snapshot(coordinator),
        "proposal": worker["distributed_coordinator"]["get_proposal"](
            "config-proposal-local:fn-02:distributed-rule-plan-1"),
    }
    assert _state_parameter(worker, WORKER_STATE_ID) == 557.0
    first_base = worker["config_capabilities"]["get_revision"]()
    first_offline_style = worker["distributed_coordinator"][
        "request_action_plan"](
            [{"state_ids": [WORKER_STATE_ID],
              "parameters": {"target_temp_c": 558.0}}],
            operation_id="distributed-rule-plan-2",
            base_revision=first_base,
        )
    assert first_offline_style.get("ok"), first_offline_style
    second_base = worker["config_capabilities"]["get_revision"]()
    second_offline_style = worker["distributed_coordinator"][
        "request_action_plan"](
            [{"state_ids": [WORKER_STATE_ID],
              "parameters": {"target_temp_c": 559.0}}],
            operation_id="distributed-rule-plan-3",
            base_revision=second_base,
        )
    assert second_offline_style.get("ok"), second_offline_style
    assert _wait_for(partial(_parameter_is, worker, 559.0))
    assert _wait_for(partial(_parameter_is, coordinator, 559.0))
    assert _wait_for(partial(_proposal_count_converged, worker, 3))
    assert _wait_for(partial(_fingerprints_equal, coordinator, worker))
    base = coordinator["config_capabilities"]["get_revision"]()
    patch = request_runtime_config_patch(
        coordinator, "controllers.4.port", 503,
        base_revision=base, source="distributed_test")
    assert patch.get("ok"), patch
    assert _wait_for(partial(_fingerprints_equal, coordinator, worker))
    worker_uuid = get_node_controller_uuids(
        get_runtime_config_snapshot(worker), "fn-02")[0]
    controllers = get_runtime_config_snapshot(worker)["controllers"]
    local = next(
        row for row in controllers if row.get("controller_uuid") == worker_uuid
    )
    assert local["port"] == 503


def _verify_remote_actuator(coordinator, worker):
    config = get_runtime_config_snapshot(coordinator)
    controller_uuid = get_node_controller_uuids(config, "fn-02")[0]
    actuator = build_controller_projection(config, controller_uuid)["actuators"][0]
    operation_id = "remote-actuator-test-1"
    task = copy.deepcopy(actuator)
    task.update({
        "task_uuid": operation_id,
        "control_value": 321,
        "config_revision": coordinator["config_capabilities"]["get_revision"](),
    })
    accepted = coordinator["distributed_coordinator"][
        "dispatch_remote_actuator"](task)
    assert accepted
    assert _wait_for(partial(
        _state_is, coordinator, operation_id, "CONFIRMED"))
    assert _operation_state(worker, operation_id) == "CONFIRMED"
    return controller_uuid


def _verify_remote_sensor(coordinator, worker, controller_uuid):
    sensor = {"controller_uuid": controller_uuid, "sensor_id": 1,
              "value": 12.5, "quality": "good", "sequence": 1}
    publisher = worker["mqtt_interface"]["publish_event"]
    assert publisher(
        "SENSOR_VALUE_UPDATE", sensor, message_id="sensor-event-test-1")
    assert publisher(
        "SENSOR_VALUE_UPDATE", sensor, message_id="sensor-event-test-1")
    assert _wait_for(partial(
        _sensor_value_is, coordinator, controller_uuid,
        sensor_id=1, expected=12.5))
    coordinator_snapshot = _distributed_snapshot(coordinator)
    worker_snapshot = _distributed_snapshot(worker)
    assert coordinator_snapshot["statistics"]["remote_actuator_results"] >= 1
    assert coordinator_snapshot["statistics"]["remote_sensor_ingested"] == 1
    assert worker_snapshot["statistics"]["actuator_results_recorded"] == 1


def _stop_runtime_pair(coordinator_main, worker_main):
    stop_runtime(worker_main)
    stop_runtime(coordinator_main)
    assert not any(thread.is_alive() for thread in worker_main["started_threads"])
    assert not any(thread.is_alive() for thread in coordinator_main["started_threads"])


def _run_test():
    global BROKER
    BROKER = _broker_create()
    coordinator_main = None
    worker_main = None
    with tempfile.TemporaryDirectory(prefix="distributed_runtime_") as directory:
        try:
            coordinator_main, worker_main = _start_runtime_pair(directory)
            coordinator = coordinator_main["runtime"]
            worker = worker_main["runtime"]
            _verify_config_authority(coordinator, worker)
            controller_uuid = _verify_remote_actuator(coordinator, worker)
            _verify_remote_sensor(coordinator, worker, controller_uuid)
            _stop_runtime_pair(coordinator_main, worker_main)
            coordinator_main = None
            worker_main = None
        finally:
            if isinstance((worker_main or {}).get("runtime"), dict):
                stop_runtime(worker_main)
            if isinstance((coordinator_main or {}).get("runtime"), dict):
                stop_runtime(coordinator_main)


def _state_is(runtime, operation_id, expected):
    return _operation_state(runtime, operation_id) == expected


def _sensor_value_is(runtime, controller_uuid, **kwargs):
    entry = runtime["sensor_value_capabilities"]["get_sensor_snapshot"](
        controller_uuid, kwargs.get("sensor_id"))
    return bool(entry and entry.get("value") == kwargs.get("expected"))


def _proposal_converged(runtime):
    journal = _distributed_snapshot(runtime).get("journal") or {}
    counts = (journal.get("counts") or {}).get("outbox") or {}
    return int(counts.get("CONVERGED") or 0) >= 1


def _proposal_count_converged(runtime, expected):
    journal = _distributed_snapshot(runtime).get("journal") or {}
    counts = (journal.get("counts") or {}).get("outbox") or {}
    return int(counts.get("CONVERGED") or 0) >= int(expected)


def _parameter_is(runtime, expected):
    return _state_parameter(runtime, WORKER_STATE_ID) == float(expected)


def _state_parameter(runtime, state_id):
    states = get_runtime_config_snapshot(runtime)["process_states"]
    state = next(row for row in states if int(row["state_id"]) == int(state_id))
    return float(state["parameters"]["target_temp_c"])


if __name__ == "__main__":
    _run_test()
    print("PASS two-node config replication and remote actuator hardware ACK")
