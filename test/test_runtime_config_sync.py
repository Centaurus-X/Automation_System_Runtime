# -*- coding: utf-8 -*-

"""End-to-end config commit, rollout, pool event and staged-stop test."""

import os
import shutil
import sys
import tempfile
import threading
import time


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._event_interface import make_event, sync_queue_put
from src.system.bootstrap_runtime_interface import (
    get_runtime_config_snapshot,
    get_runtime_snapshot,
    request_runtime_config_patch,
    start_runtime,
    stop_runtime,
)


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _make_main_context(config_path):
    return {
        "shutdown_event": threading.Event(),
        "app_config": {
            "app": {"node_id": "fn-01"},
            "runtime": {
                "event_router": {
                    "enabled": True,
                    "worker_threads": 2,
                    "ingress_timeout_s": 0.02,
                },
                "value_store": {
                    "change_queue_maxsize": 64,
                    "persistence": {"enabled": False},
                },
                "data_center": {
                    "json_path": config_path,
                    "verify_on_load": True,
                    "verify_on_save": True,
                    "allow_legacy_import": True,
                    "legacy_import_profile": "lab_v2",
                },
            },
            "thread_management": {
                "pool_min_workers": 2,
                "pool_max_workers": 2,
                "controllers_enabled": False,
                "connections_enabled": False,
                "ingress_timeout_s": 0.02,
            },
            "state_event_management": {
                "persistence_enabled": False,
                "ingress_timeout_s": 0.02,
                "request_timeout_s": 3.0,
            },
            "shutdown": {"join_timeout_s": 3.0},
        },
    }


def _wait_for_jobs(runtime_ctx):
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        snapshot = get_runtime_snapshot(runtime_ctx)
        statistics = snapshot["thread_management"]["statistics"]
        if (
            statistics["rule_jobs"] >= 1
            and statistics["automation_jobs"] >= 1
            and statistics["config_rollouts"] >= 3
        ):
            return snapshot
        time.sleep(0.02)
    raise AssertionError("automation/rule jobs did not complete")


def _wait_for_pipeline_idle(runtime_ctx, expected_sequence):
    deadline = time.monotonic() + 8.0

    while time.monotonic() < deadline:
        snapshot = get_runtime_snapshot(runtime_ctx)
        pipeline = snapshot["thread_management"]["sensor_pipeline"]

        if (
            pipeline["sequence"] >= expected_sequence
            and not pipeline["active"]
            and pipeline["pending_count"] == 0
        ):
            return snapshot

        time.sleep(0.02)

    raise AssertionError("coalesced sensor pipeline did not become idle")


def _wait_for_fsm_state(runtime_ctx, process_name, expected_state):
    deadline = time.monotonic() + 3.0

    while time.monotonic() < deadline:
        snapshot = get_runtime_snapshot(runtime_ctx)
        state = snapshot["thread_management"]["fsm"]["states"].get(process_name)

        if state == expected_state:
            return snapshot

        time.sleep(0.02)

    raise AssertionError("FSM did not reach " + expected_state)


def _wait_for_fsm_updates(runtime_ctx, minimum_updates):
    deadline = time.monotonic() + 3.0

    while time.monotonic() < deadline:
        snapshot = get_runtime_snapshot(runtime_ctx)
        fsm = snapshot["thread_management"]["fsm"]
        updates = snapshot["thread_management"]["statistics"]["fsm_updates"]

        if (
            updates >= minimum_updates
            and not fsm["active_instances"]
            and fsm["pending_events"] == 0
        ):
            return snapshot

        time.sleep(0.02)

    raise AssertionError("ordered FSM event queue did not complete")


def _test_config_commit(runtime_ctx):
    before = get_runtime_config_snapshot(runtime_ctx)
    current = before["process_states"][0]["value_id"]
    replacement = 2 if current != 2 else 1
    result = request_runtime_config_patch(
        runtime_ctx, "process_states[0].value_id", replacement,
        source="runtime_config_sync_test")
    assert result["ok"] is True and result["rollout"]["ok"] is True
    snapshot = get_runtime_snapshot(runtime_ctx)
    assert snapshot["data_center"]["revision"] == result["revision"]
    assert snapshot["thread_management"]["config_revision"] == result["revision"]


def _test_fsm(runtime_ctx):
    process_name = "ketonic_decarboxylation"
    fsm_event = make_event(
        "FSM_EVENT",
        payload={"process_name": process_name, "event_name": "activate", "data": {}},
        source="runtime_config_sync_test",
        target="thread_management",
    )
    assert sync_queue_put(runtime_ctx["queues"]["queue_event_send"], fsm_event)
    snapshot = _wait_for_fsm_state(runtime_ctx, process_name, "activated")
    assert snapshot["thread_management"]["statistics"]["fsm_updates"] == 1
    for event_name in (
        "deactivate", "maintenance_start", "maintenance_end", "activate",
    ):
        ordered_event = make_event(
            "FSM_EVENT",
            payload={
                "process_name": process_name,
                "event_name": event_name,
                "data": {},
            },
            source="runtime_config_sync_ordered_fsm_test",
            target="thread_management",
        )
        assert sync_queue_put(runtime_ctx["queues"]["queue_event_send"], ordered_event)
    ordered = _wait_for_fsm_updates(runtime_ctx, 5)
    assert ordered["thread_management"]["fsm"]["states"][process_name] == "activated"
    return process_name


def _write_sensor(capabilities, controller_uuid, token, value):
    assert capabilities["write_sensor_value"](
        controller_uuid=controller_uuid, sensor_id=1, value=value,
        quality="good", writer_token=token)["ok"]
    event = make_event(
        "SENSOR_VALUE_UPDATE",
        payload={"controller_uuid": controller_uuid, "sensor_id": 1,
                 "value": value, "quality": "good", "timestamp": time.time()},
        source="runtime_config_sync_test",
        target="thread_management",
    )
    return event


def _test_sensor_pipeline(runtime_ctx, process_name):
    capabilities = runtime_ctx["sensor_value_capabilities"]
    controller_uuid = "62671112-766c-59ed-b0ff-30a49d48f747"
    token = capabilities["create_writer_token"]("runtime_config_sync_test")
    assert capabilities["claim_writer"](controller_uuid, token)["ok"]
    event = _write_sensor(capabilities, controller_uuid, token, 600.0)
    assert sync_queue_put(runtime_ctx["queues"]["queue_event_send"], event)
    snapshot = _wait_for_jobs(runtime_ctx)
    statistics = snapshot["thread_management"]["statistics"]
    assert statistics["automation_jobs"] == 1 and statistics["rule_jobs"] == 1
    assert snapshot["state_event_management"]["statistics"]["commits_ok"] >= 3
    assert snapshot["data_center"]["revision"] == snapshot[
        "thread_management"]["config_revision"]
    assert snapshot["thread_management"]["fsm"]["states"][process_name] == "activated"
    return capabilities, controller_uuid, token


def _test_sensor_burst(runtime_ctx, capabilities, controller_uuid, token):
    for offset in range(32):
        event = _write_sensor(
            capabilities, controller_uuid, token, 601.0 + offset)
        event["source"] = "runtime_config_sync_burst"
        assert sync_queue_put(runtime_ctx["queues"]["queue_event_send"], event)
    snapshot = _wait_for_pipeline_idle(runtime_ctx, 33)
    statistics = snapshot["thread_management"]["statistics"]
    assert statistics["sensor_events_coalesced"] > 0
    assert statistics["stale_rule_results"] == 0
    assert statistics["stale_automation_results"] == 0


def run_test():
    temporary_root = tempfile.mkdtemp(prefix="runtime-config-sync-")
    config_path = os.path.join(temporary_root, "config.json")
    shutil.copy2(SOURCE_CONFIG, config_path)
    main_ctx = _make_main_context(config_path)
    try:
        start_runtime(main_ctx)
        runtime_ctx = main_ctx["runtime"]
        _test_config_commit(runtime_ctx)
        process_name = _test_fsm(runtime_ctx)
        capabilities, controller_uuid, token = _test_sensor_pipeline(
            runtime_ctx, process_name)
        _test_sensor_burst(runtime_ctx, capabilities, controller_uuid, token)
    finally:
        if _is_runtime_present(main_ctx):
            stop_runtime(main_ctx)
        shutil.rmtree(temporary_root, ignore_errors=True)

    final_snapshot = get_runtime_snapshot(main_ctx["runtime"])
    assert final_snapshot["state"] == "stopped"
    assert not final_snapshot["queue_names"]
    assert not any(item["thread_alive"] for item in final_snapshot["components"].values())
    print("PASS runtime config sync, event jobs and staged shutdown")


def _is_runtime_present(main_ctx):
    return isinstance(main_ctx.get("runtime"), dict)


if __name__ == "__main__":
    run_test()
