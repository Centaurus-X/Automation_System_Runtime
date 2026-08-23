# -*- coding: utf-8 -*-

"""All active local control states dispatch in autonomous Worker mode."""

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
from src.system.bootstrap_runtime_interface import start_runtime, stop_runtime
from src.system.bootstrap_system_interface import load_app_config


CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"
SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data",
    "device_state_data_v9_node_fn-01.json",
)


def _capture(captured, **kwargs):
    event_queue = kwargs["event_queue_mbc"]
    shutdown = kwargs["shutdown_event"]
    while not shutdown.is_set():
        try:
            event = event_queue.get(timeout=0.05)
        except Empty:
            continue
        if isinstance(event, dict) and event.get("_sentinel"):
            break
        if isinstance(event, dict) and event.get("event_type") == "ACTUATOR_TASK":
            captured.append(copy.deepcopy(event))


def _profile(config_path):
    app = load_app_config(active_profile="standalone")
    app["runtime"]["data_center"]["json_path"] = config_path
    app["runtime"]["data_center"]["sqlite_enabled"] = False
    app["runtime"]["value_store"]["persistence"]["enabled"] = False
    app["state_event_management"]["persistence_enabled"] = False
    app["thread_management"]["connections_enabled"] = False
    app["thread_management"]["ingress_timeout_s"] = 0.02
    app["runtime"]["event_router"]["ingress_timeout_s"] = 0.02
    return app


def _wait_for_pipelines(context, minimum, captured, expected_tasks):
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with context["pipeline_lock"]:
            idle = not context["pipeline_active"] and not context["pipeline_pending"]
        completed = context["statistics"]["pipelines_completed"] >= minimum
        if idle and completed and len(captured) >= expected_tasks:
            return
        time.sleep(0.02)
    raise AssertionError(
        "local sensor pipelines or actuator event capture did not complete")


def run_test():
    with tempfile.TemporaryDirectory(prefix="worker-all-local-actuators-") as directory:
        config_path = os.path.join(directory, "config.json")
        shutil.copy2(SOURCE_CONFIG, config_path)
        captured = []
        main_context = {
            "shutdown_event": threading.Event(),
            "app_config": _profile(config_path),
        }
        try:
            start_runtime(
                main_context,
                controller_target=partial(_capture, captured),
            )
            runtime = main_context["runtime"]
            management = runtime["components"]["thread_management"]["context"]
            capabilities = runtime["sensor_value_capabilities"]
            token = capabilities["create_writer_token"]("all-local-actuator-test")
            assert capabilities["claim_writer"](CONTROLLER_UUID, token)["ok"]
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
                        source="all-local-actuator-test",
                        target="thread_management",
                    ),
                )
            _wait_for_pipelines(management, 6, captured, 4)
            tasks = [event["payload"]["task"] for event in captured]
            assert {int(task["address"]) for task in tasks} == {
                515, 516, 524, 546,
            }, tasks
            assert all(task["controller_uuid"] == CONTROLLER_UUID for task in tasks)
            assert management["statistics"]["actuator_dispatched"] == 4
            assert management["statistics"]["actuator_rejected"] == 0
        finally:
            if isinstance(main_context.get("runtime"), dict):
                stop_runtime(main_context)
    print("PASS all active local actuator states in standalone mode")


if __name__ == "__main__":
    run_test()
