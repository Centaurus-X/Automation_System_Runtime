# -*- coding: utf-8 -*-

"""Owner-side canonicalisation of untrusted Cross actuator intents."""

import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.thread_management import _canonicalize_local_actuator_task


CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"


def run_test():
    path = os.path.join(
        PROJECT_ROOT, "_config", "config_data", "_legacy",
        "device_state_data_v7_node_fn-01.json",
    )
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    malicious = {
        "controller_uuid": CONTROLLER_UUID,
        "actuator_id": 1,
        "control_value": 120.0,
        "address": 1999,
        "function_type": "write_register",
        "bus_type": "forged",
        "unit": "0xff",
        "controller_id": 999,
        "execution_scope": "cross",
        "metadata": {"cross_runtime": {"execution_scope": "cross"}},
    }
    canonical = _canonicalize_local_actuator_task(config, malicious)
    assert canonical["controller_id"] == 2
    assert canonical["address"] == 514
    assert canonical["function_type"] == "write_coil"
    assert canonical["bus_type"] == "modbus"
    assert canonical["control_value"] == 1
    assert canonical["engineering_value"] == 120.0
    assert _canonicalize_local_actuator_task(config, {
        "controller_uuid": CONTROLLER_UUID,
        "actuator_id": 999,
        "control_value": 1,
    }) is None
    print("PASS owner-side Cross actuator intent canonicalisation")


if __name__ == "__main__":
    run_test()
