# -*- coding: utf-8 -*-

"""Distributed Worker keeps local automation live while the Master is offline."""

import copy
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

from src.library._event_interface import make_event, sync_queue_put
from src.library._generic_system_contract_library import protocol_contracts
from src.core.distributed_coordinator import _node_payload
from src.core.thread_management import _request_rule_actions
from src.system.bootstrap_runtime_interface import (
    get_runtime_config_snapshot,
    start_runtime,
    stop_runtime,
)


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)
CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"


def _actuator_limits():
    return [
        {
            "controller_uuid": CONTROLLER_UUID,
            "actuator_id": 1, "allowed": [False, True],
        },
        {
            "controller_uuid": CONTROLLER_UUID,
            "actuator_id": 2, "allowed": [False, True],
        },
        {
            "controller_uuid": CONTROLLER_UUID,
            "actuator_id": 3, "min": 0, "max": 65535,
        },
        {
            "controller_uuid": CONTROLLER_UUID,
            "actuator_id": 4, "allowed": [False, True],
        },
        {
            "controller_uuid": CONTROLLER_UUID,
            "actuator_id": 5, "min": 0, "max": 65535,
        },
    ]


def _transport_snapshot(context):
    return {
        "state": context["state"],
        "connected": context["connected"],
        "thread_alive": False,
        "outbound_queue_depth": 0,
    }


def _transport_start(context):
    context["state"] = "disconnected"
    context["connected"] = False


def _transport_start_connected(context):
    context["state"] = "connected"
    context["connected"] = True
    callback = context.get("on_connect")
    if callable(callback):
        callback(_transport_snapshot(context))


def _transport_stop(context, **_kwargs):
    context["state"] = "stopped"
    context["connected"] = False
    return _transport_snapshot(context)


def _transport_wait(context, _timeout):
    return context["connected"]


def _transport_publish(context, topic, payload, **kwargs):
    context["published"].append({
        "topic": str(topic),
        "payload": bytes(payload),
        "qos": kwargs.get("qos", 0),
    })
    return True


def _transport_subscribe(context, topic, **_kwargs):
    context["subscriptions"].add(str(topic))
    return True


def _transport_factory(**kwargs):
    context = {
        "state": "created",
        "connected": False,
        "published": [],
        "subscriptions": set(),
        "on_connect": kwargs.get("on_connect"),
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


def _connected_transport_factory(**kwargs):
    api = _transport_factory(**kwargs)
    context = api["context"]
    api["start"] = partial(_transport_start_connected, context)
    return api


def _capture_controller_events(captured, **kwargs):
    event_queue = kwargs["event_queue_mbc"]
    shutdown_event = kwargs["shutdown_event"]
    while not shutdown_event.is_set():
        try:
            event = event_queue.get(timeout=0.05)
        except Empty:
            continue
        if event.get("_sentinel"):
            break
        if event.get("event_type") == "ACTUATOR_TASK":
            captured.append(copy.deepcopy(event))


def _local_commit_sync_failure(_actions, **_kwargs):
    return {
        "ok": False,
        "code": "proposal_journal",
        "local_committed": True,
    }


def _master_sync_timeout(_actions, **_kwargs):
    return {"ok": False, "code": "proposal_timeout"}


def _main_context(config_path, journal_path):
    return {
        "shutdown_event": threading.Event(),
        "app_config": {
            "app": {"site_id": "site-test", "node_id": "fn-01"},
            "runtime": {
                "event_router": {
                    "enabled": True, "worker_threads": 2,
                    "ingress_timeout_s": 0.02,
                },
                "value_store": {"persistence": {"enabled": False}},
                "data_center": {
                    "json_path": config_path,
                    "verify_on_load": True, "verify_on_save": True,
                    "allow_legacy_import": True,
                    "legacy_import_profile": "lab_v2",
                },
            },
            "thread_management": {
                "pool_min_workers": 2, "pool_max_workers": 2,
                "controllers_enabled": True, "connections_enabled": False,
                "rule_engine_enabled": True,
                "rule_scope": "local_controllers",
                "fsm_scope": "local_controllers",
                "controller_allowlist": [CONTROLLER_UUID],
                "ingress_timeout_s": 0.02,
            },
            "state_event_management": {
                "persistence_enabled": False,
                "ingress_timeout_s": 0.02,
                "request_timeout_s": 3.0,
                "safety": {
                    "enabled": True,
                    "rule_actions": {
                        "enabled": True,
                        "require_base_revision": True,
                        "enforce_writable_fields": True,
                        "enforce_required_mode": True,
                    },
                    "external_commits": {"enabled": False},
                    "actuator_tasks": {
                        "enabled": True,
                        "require_config_revision": True,
                        "require_limits": True,
                        "limits": _actuator_limits(),
                    },
                },
            },
            "mqtt_interface": {
                "enabled": True, "required": True,
                "startup_connection_required": False,
                "heartbeat_interval_s": 30.0,
                "transport": {"tls": {"enabled": True}},
                "commands": {"allowed": ["runtime.status"]},
                "event_publication": {"event_types": [
                    "SENSOR_VALUE_UPDATE", "CONFIG_COMMIT_APPLIED",
                    "ACTUATOR_FEEDBACK", "FSM_STATE_UPDATE",
                ]},
            },
            "distributed_runtime": {
                "enabled": True, "role": "worker",
                "config_authority": "node_local",
                "coordinator_node_id": "master-01",
                "journal_path": journal_path,
                "local_automation_enabled": True,
                "config_sync_mode": "local_first",
                "conflict_policy": "master_wins",
                "registration_interval_s": 30.0,
                "retry_interval_s": 0.05,
                "command_ttl_s": 5.0,
            },
            "shutdown": {"join_timeout_s": 3.0},
        },
    }


def _wait_for_actuators(captured, expected_addresses):
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        tasks = [event["payload"]["task"] for event in list(captured)]
        addresses = {int(task["address"]) for task in tasks}
        if set(expected_addresses).issubset(addresses):
            return tasks
        time.sleep(0.02)
    raise AssertionError(
        "distributed Worker did not produce all local actuator tasks: "
        + repr(list(captured)))


def _wait_for_pipelines(context, minimum):
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with context["pipeline_lock"]:
            idle = not context["pipeline_active"] and not context["pipeline_pending"]
        if idle and context["statistics"]["pipelines_completed"] >= minimum:
            return
        time.sleep(0.02)
    raise AssertionError("offline distributed Worker pipelines did not complete")


def _emit_sensor_values(runtime, capabilities, token):
    values = (
        (1, 652), (2, 61225), (3, 28573),
        (4, 55086), (5, 18064), (6, 41373),
    )
    for sensor_id, value in values:
        assert capabilities["write_sensor_value"](
            controller_uuid=CONTROLLER_UUID,
            sensor_id=sensor_id,
            value=value,
            quality="good",
            writer_token=token,
        )["ok"]
        assert sync_queue_put(
            runtime["queues"]["queue_event_send"],
            make_event(
                "SENSOR_VALUE_UPDATE",
                payload={
                    "controller_uuid": CONTROLLER_UUID,
                    "controller_id": 2,
                    "sensor_id": sensor_id,
                    "value": value,
                    "quality": "good",
                },
                source="local-authority-test",
                target="thread_management",
            ),
        )


def _assert_local_authority(runtime, component):
    distributed = runtime["distributed_coordinator"]
    registration = _node_payload(distributed["context"])
    normalized = protocol_contracts.normalize_registration(registration)
    capabilities = normalized["capabilities"]
    assert tuple(registration["controller_uuids"]) == (CONTROLLER_UUID,)
    assert capabilities["config_scope_operations_v1"] is True
    assert capabilities["local_automation_authority"] is True
    assert capabilities["master_automation_execution"] is False
    assert distributed["snapshot"]()["local_automation_enabled"] is True
    assert distributed["snapshot"]()["config_sync_mode"] == "local_first"
    assert component["resources"]["sensor_pipeline_enabled"] is True
    assert component["rule_binding"]["scope"]["selected"] == 3
    # Six controller-local state rows collapse into one deterministic process
    # machine after the strict node/controller projection.
    assert component["fsm"]["scope"]["selected"] == 1
    assert len(component["fsm"]["machines"]) == 1


def _assert_sync_continuity(component):
    writer = component["resources"]["config_writer"]
    original_request = writer["request_action_plan"]
    metadata = {
        "source_event_id": "sync-continuity",
        "pipeline_sequence": 1,
        "retry_count": 0,
        "sensor_payload": {
            "controller_uuid": CONTROLLER_UUID,
            "sensor_id": 1,
            "value": 652,
        },
    }
    writer["request_action_plan"] = _local_commit_sync_failure
    assert _request_rule_actions(
        component, [{"state_ids": [1]}], metadata, 1) == "ok"
    writer["request_action_plan"] = _master_sync_timeout
    assert _request_rule_actions(
        component, [{"state_ids": [1]}], metadata, 1) == "control_fallback"
    writer["request_action_plan"] = original_request
    assert component["statistics"]["rule_action_sync_degraded"] == 1
    assert component["statistics"]["rule_action_control_fallbacks"] == 1


def _exercise_local_control(runtime, component, captured):
    base_revision = runtime["config_capabilities"]["get_revision"]()
    local_commit = runtime["distributed_coordinator"]["request_action_plan"](
        [{"state_ids": [1], "parameters": {"target_temp_c": 555.0}}],
        operation_id="offline-local-rule-1",
        base_revision=base_revision,
    )
    assert local_commit.get("ok"), local_commit
    assert local_commit.get("local_committed") is True
    sync = local_commit.get("sync") or {}
    post_commit = local_commit.get("post_commit") or {}
    assert sync.get("mode") == "node_local"
    assert sync.get("state") == "COMMITTED"
    assert sync.get("durable") is True
    assert sync.get("mirror_id") == post_commit.get("mirror_id")
    assert int(sync.get("scope_revision") or 0) > 0
    assert post_commit.get("status") == "node_authoritative_committed"
    assert runtime["distributed_coordinator"]["get_proposal"](
        "config-proposal-local:fn-01:offline-local-rule-1") is None
    state_1 = next(
        row for row in get_runtime_config_snapshot(runtime)["process_states"]
        if int(row["state_id"]) == 1)
    assert float(state_1["parameters"]["target_temp_c"]) == 555.0
    capabilities = runtime["sensor_value_capabilities"]
    token = capabilities["create_writer_token"]("local-authority-test")
    assert capabilities["claim_writer"](CONTROLLER_UUID, token)["ok"]
    _emit_sensor_values(runtime, capabilities, token)
    _wait_for_pipelines(component, 6)
    tasks = _wait_for_actuators(captured, {515, 516, 524, 546})
    assert {int(task["address"]) for task in tasks} == {515, 516, 524, 546}
    assert component["statistics"]["actuator_rejected"] == 0
    assert component["statistics"]["rule_jobs"] >= 1
    assert component["statistics"]["automation_jobs"] >= 1
    journal = runtime["distributed_coordinator"]["snapshot"]()["journal"]
    outbox = (journal.get("counts") or {}).get("outbox") or {}
    assert int(outbox.get("PENDING") or 0) + int(
        outbox.get("AWAITING_ACK") or 0) >= 1


def _run_test(**kwargs):
    with tempfile.TemporaryDirectory(prefix="worker-local-authority-") as directory:
        config_path = os.path.join(directory, "config.json")
        journal_path = os.path.join(directory, "distributed.sqlite3")
        shutil.copy2(SOURCE_CONFIG, config_path)
        captured = []
        main_context = _main_context(config_path, journal_path)
        if kwargs.get("configured_forward_only"):
            distributed = main_context["app_config"]["distributed_runtime"]
            distributed["local_automation_enabled"] = False
            distributed["config_sync_mode"] = "global_first"
        try:
            start_runtime(
                main_context,
                mqtt_transport_factory=(
                    kwargs.get("mqtt_transport_factory") or _transport_factory),
                controller_target=partial(_capture_controller_events, captured),
            )
            runtime = main_context["runtime"]
            component = runtime["components"]["thread_management"]["context"]
            _assert_local_authority(runtime, component)
            _assert_sync_continuity(component)
            _exercise_local_control(runtime, component, captured)
        finally:
            if isinstance(main_context.get("runtime"), dict):
                stop_runtime(main_context)


if __name__ == "__main__":
    _run_test()
    _run_test(
        mqtt_transport_factory=_connected_transport_factory,
        configured_forward_only=True,
    )
    print(
        "PASS distributed Worker local authority with offline and connected "
        "MQTT transport")
