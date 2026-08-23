# -*- coding: utf-8 -*-

"""UUID projection tests for controller and automation config views."""

import copy
import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._state_shard_manager import (
    build_automation_projection,
    build_controller_projection,
    get_node_controller_uuids,
)
from src.core._automation_thread import run_automation_cycle


CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _deterministic_config_fixture():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        source = json.load(handle)
    config = copy.deepcopy(source)
    for state in config.get("process_states") or []:
        state["value_id"] = 2
        if int(state.get("state_id", -1)) in (1, 3):
            state["value_id"] = 1
    return config


def run_test():
    config = _deterministic_config_fixture()
    identities = get_node_controller_uuids(config, "fn-01")
    assert len(identities) == 4
    assert len(get_node_controller_uuids(config, None)) == 5
    controller_uuid = "62671112-766c-59ed-b0ff-30a49d48f747"
    controller = build_controller_projection(config, controller_uuid)
    assert len(controller["controllers"]) == 1
    assert len(controller["sensors"]) == 6
    assert len(controller["actuators"]) == 5
    automation = build_automation_projection(config, controller_uuid, 1)
    assert automation["sensor_id"] is not None
    assert len(automation["config"]["sensors"]) == 10
    assert len(automation["config"]["actuators"]) == 14
    reverse = automation["identity_maps"]["sensors"]
    assert reverse[str(automation["sensor_id"])]["controller_uuid"] == controller_uuid
    tasks = run_automation_cycle(
        automation["config"],
        {
            "sensor_id": automation["sensor_id"],
            "controller_uuid": controller_uuid,
            "value": 600.0,
            "timestamp": 0.0,
        },
        {"persistent_state": {}},
    )
    assert len(tasks) == 2
    assert {(task["controller_uuid"], task["actuator_id"]) for task in tasks} == {
        ("62671112-766c-59ed-b0ff-30a49d48f747", 3),
        ("d11032fa-33db-56de-a4ed-7e16cca3ded7", 3),
    }
    assert all(task["projected_actuator_id"] != task["actuator_id"] for task in tasks)

    local_tasks = run_automation_cycle(
        automation["config"],
        {
            "sensor_id": automation["sensor_id"],
            "controller_uuid": controller_uuid,
            "value": 600.0,
            "timestamp": 0.0,
        },
        {
            "persistent_state": {},
            "node_id": "fn-01",
            "allowed_controller_ids": (2,),
            "controller_scope_explicit": True,
        },
    )
    assert len(local_tasks) == 1
    assert local_tasks[0]["controller_uuid"] == controller_uuid
    assert local_tasks[0]["actuator_id"] == 3

    denied_tasks = run_automation_cycle(
        automation["config"],
        {
            "sensor_id": automation["sensor_id"],
            "controller_uuid": controller_uuid,
            "value": 600.0,
            "timestamp": 0.0,
        },
        {
            "persistent_state": {},
            "allowed_controller_ids": (),
            "controller_scope_explicit": True,
        },
    )
    assert denied_tasks == []
    print("PASS deterministic UUID projections and fail-closed controller scope")


if __name__ == "__main__":
    run_test()
