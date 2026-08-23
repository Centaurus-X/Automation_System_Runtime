# -*- coding: utf-8 -*-

"""Worker profile regression test for sensor-to-actuator dispatch."""

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

from src.core.thread_management import dispatch_local_actuator_task
from src.library._event_interface import make_event, sync_queue_put
from src.system.bootstrap_runtime_interface import start_runtime, stop_runtime


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)
CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"
REMOTE_CONTROLLER_UUID = "d11032fa-33db-56de-a4ed-7e16cca3ded7"


def _capture_controller_events(captured, **kwargs):
    event_queue = kwargs["event_queue_mbc"]
    shutdown_event = kwargs["shutdown_event"]
    while not shutdown_event.is_set():
        try:
            event = event_queue.get(timeout=0.05)
        except Empty:
            continue
        if not isinstance(event, dict):
            continue
        if event.get("_type") == "__STOP__":
            break
        if event.get("event_type") == "ACTUATOR_TASK":
            captured.append(event)


def _main_context(config_path):
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
                "value_store": {"persistence": {"enabled": False}},
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
                "controllers_enabled": True,
                "connections_enabled": False,
                "rule_engine_enabled": False,
                "controller_allowlist": [CONTROLLER_UUID],
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


def _write_deterministic_config(path):
    with open(SOURCE_CONFIG, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    for state in config.get("process_states") or []:
        state["value_id"] = 2
        if int(state.get("state_id", -1)) in (1, 3):
            state["value_id"] = 1
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(config, handle, ensure_ascii=False, indent=2)


def _wait_for_actuator(captured):
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if captured:
            return captured[0]
        time.sleep(0.02)
    raise AssertionError("local actuator task was not dispatched")


def run_test():
    temporary_root = tempfile.mkdtemp(prefix="worker-actuator-pipeline-")
    config_path = os.path.join(temporary_root, "config.json")
    _write_deterministic_config(config_path)
    captured = []
    main_context = _main_context(config_path)
    try:
        start_runtime(
            main_context,
            controller_target=partial(_capture_controller_events, captured),
        )
        runtime = main_context["runtime"]
        management = runtime["components"]["thread_management"]["context"]
        assert dispatch_local_actuator_task(
            management,
            {
                "task_uuid": "remote-scope-regression",
                "controller_uuid": REMOTE_CONTROLLER_UUID,
                "actuator_id": 3,
                "address": 543,
                "control_value": 1,
                "config_revision": 0,
            },
        ) is False
        assert management["statistics"]["actuator_rejected_ownership"] == 1

        capabilities = runtime["sensor_value_capabilities"]
        token = capabilities["create_writer_token"]("worker_actuator_test")
        assert capabilities["claim_writer"](CONTROLLER_UUID, token)["ok"]
        assert capabilities["write_sensor_value"](
            controller_uuid=CONTROLLER_UUID,
            sensor_id=1,
            value=600.0,
            quality="good",
            writer_token=token,
        )["ok"]
        event = make_event(
            "SENSOR_VALUE_UPDATE",
            payload={
                "controller_uuid": CONTROLLER_UUID,
                "controller_id": 2,
                "sensor_id": 1,
                "value": 600.0,
                "quality": "good",
            },
            source="worker_actuator_test",
            target="thread_management",
        )
        assert sync_queue_put(runtime["queues"]["queue_event_send"], event)
        actuator_event = _wait_for_actuator(captured)
        task = actuator_event["payload"]["task"]
        assert task["controller_uuid"] == CONTROLLER_UUID
        assert task["actuator_id"] == 3
        assert task["address"] == 524
        assert task["function_type"] == "write_register"
        time.sleep(0.1)
        assert len(captured) == 1
        assert all(
            item["payload"]["task"]["controller_uuid"] == CONTROLLER_UUID
            for item in captured
        )
        statistics = runtime["components"]["thread_management"][
            "context"
        ]["statistics"]
        assert statistics["rule_jobs"] == 0
        assert statistics["automation_jobs"] >= 1
        assert statistics["actuator_dispatched"] >= 1
    finally:
        if isinstance(main_context.get("runtime"), dict):
            stop_runtime(main_context)
        shutil.rmtree(temporary_root, ignore_errors=True)

    print("PASS isolated worker sensor-to-actuator pipeline")


if __name__ == "__main__":
    run_test()
