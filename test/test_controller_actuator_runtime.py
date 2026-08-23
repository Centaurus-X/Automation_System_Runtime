# -*- coding: utf-8 -*-

"""Actuator discovery and timeout-driven batching regression tests."""

import copy
import os
import sys
import threading

from functools import partial
from queue import Queue


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core import _controller_thread as controller


def _discovery_defaults(_ctx):
    return {"simulator_actuator_discovery": "noop_write"}


def _virtual_endpoint(_ctx):
    return {"host": "127.0.0.1", "port": 5020}


def _discovery_read(state, _ctx, data_type, address):
    state["reads"].append((data_type, address))
    return state["values"].get(address, 0)


def _discovery_write(state, _ctx, data_type, address, value):
    state["writes"].append((data_type, address, value))
    state["values"][address] = value
    return True


def _test_virtual_discovery_is_complete_and_value_neutral():
    original_defaults = controller._get_controller_timing_defaults
    original_endpoint = controller._get_active_virtual_endpoint
    original_read = controller._conn_read_single
    original_write = controller._conn_write_single
    state = {
        "reads": [], "writes": [],
        "values": {514: 1, 515: 0, 516: 1, 524: 1234, 546: 4321},
    }
    context = {
        "controller_id": 2,
        "statistics": {},
        "shutdown_event": threading.Event(),
    }
    actuators = [
        {"actuator_id": 1, "address": 514, "function_type": "write_coil"},
        {"actuator_id": 2, "address": 515, "function_type": "write_coil"},
        {"actuator_id": 3, "address": 524, "function_type": "write_register"},
        {"actuator_id": 4, "address": 516, "function_type": "write_coil"},
        {"actuator_id": 5, "address": 546, "function_type": "write_register"},
    ]
    try:
        controller._get_controller_timing_defaults = _discovery_defaults
        controller._get_active_virtual_endpoint = _virtual_endpoint
        controller._conn_read_single = partial(_discovery_read, state)
        controller._conn_write_single = partial(_discovery_write, state)
        before = copy.deepcopy(state["values"])
        assert controller._simulator_actuator_discovery(context, actuators)
        assert state["values"] == before
        assert {item[1] for item in state["writes"]} == {514, 515, 516, 524, 546}
        assert context["statistics"]["actuators_discovered"] == 5
        writes = len(state["writes"])
        assert controller._simulator_actuator_discovery(context, actuators)
        assert len(state["writes"]) == writes
    finally:
        controller._get_controller_timing_defaults = original_defaults
        controller._get_active_virtual_endpoint = original_endpoint
        controller._conn_read_single = original_read
        controller._conn_write_single = original_write


def _batch_settings(_ctx):
    return {
        "batch_actuator": True,
        "batch_actuator_timeout": 0.05,
    }


def _capture_execution(captured, signal, _ctx, task):
    captured.append(copy.deepcopy(task))
    signal.set()


def _test_batch_timeout_does_not_wait_for_sensor_cycle():
    original_settings = controller._get_comm_settings
    original_execute = controller._execute_actuator_task
    captured = []
    signal = threading.Event()
    shutdown = threading.Event()
    context = {
        "controller_id": 2,
        "event_queue_mbc": Queue(),
        "shutdown_event": shutdown,
        "running": True,
        "actuator_batcher": {
            "pending": {}, "lock": threading.Lock(),
            "first_pending_monotonic": None,
            "stats": {
                "batches_executed": 0, "tasks_batched": 0,
                "tasks_total": 0, "last_flush_ts": 0.0,
            },
        },
    }
    listener = None
    try:
        controller._get_comm_settings = _batch_settings
        controller._execute_actuator_task = partial(
            _capture_execution, captured, signal)
        listener = threading.Thread(
            target=partial(controller._event_listener_loop, context),
            name="ActuatorBatchTimeoutTest",
            daemon=False,
        )
        listener.start()
        context["event_queue_mbc"].put({
            "event_type": "ACTUATOR_TASK",
            "payload": {
                "task": {
                    "controller_id": 2, "actuator_id": 3,
                    "address": 524, "function_type": "write_register",
                    "control_value": 99, "meta": {"state_id": 1},
                },
                "source": "batch_timeout_test",
            },
        })
        assert signal.wait(0.5), "actuator batch did not flush on timeout"
        assert len(captured) == 1
        assert captured[0]["address"] == 524
    finally:
        context["running"] = False
        shutdown.set()
        context["event_queue_mbc"].put({"_type": "__STOP__", "_sentinel": True})
        if listener is not None:
            listener.join(timeout=1.0)
        controller._get_comm_settings = original_settings
        controller._execute_actuator_task = original_execute
    assert listener is not None and not listener.is_alive()


def run_test():
    _test_virtual_discovery_is_complete_and_value_neutral()
    _test_batch_timeout_does_not_wait_for_sensor_cycle()
    print("PASS actuator discovery and timeout-driven batch execution")


if __name__ == "__main__":
    run_test()
