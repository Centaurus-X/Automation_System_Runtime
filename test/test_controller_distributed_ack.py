# -*- coding: utf-8 -*-

"""Real controller execution path reports a readback-bound distributed ACK."""

import os
import sys
import threading

from functools import partial
from queue import Queue


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core import _controller_thread as controller


def _allow_owner(worker_uuid, task, **_kwargs):
    return {"ok": worker_uuid == task.get("controller_uuid")}


def _collect(bucket, value):
    bucket.append(value)
    return True


def _write(*_args, **_kwargs):
    return True


def _read(_context, _data_type, _address):
    return 321


def _run_test():
    results = []
    old_write = controller._conn_write_single
    old_read = controller._conn_read_single
    controller._conn_write_single = _write
    controller._conn_read_single = _read
    try:
        context = {
            "controller_id": 5,
            "controller_data": {"controller_id": 5, "controller_uuid": "c-5"},
            "config_data": {},
            "config_lock": threading.RLock(),
            "runtime_resources": {
                "validate_actuator_write": _allow_owner,
                "actuator_result_reporter": partial(_collect, results),
            },
            "worker_uuid": "c-5",
            "local_actuator_lock": threading.RLock(),
            "local_actuator_values": {},
            "queue_event_send": Queue(),
            "actuator_last_write_ts": {},
        }
        task = {
            "task_uuid": "task-1",
            "distributed_operation_id": "task-1",
            "controller_id": 5,
            "controller_uuid": "c-5",
            "actuator_id": 1,
            "address": 523,
            "function_type": "write_register",
            "control_value": 321,
        }
        controller._enqueue_or_execute_actuator_task(context, task)
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["readback_checked"] is True
        assert results[0]["readback_verified"] is True
        assert results[0]["readback"] == 321
        assert results[0]["distributed_operation_id"] == "task-1"
    finally:
        controller._conn_write_single = old_write
        controller._conn_read_single = old_read


if __name__ == "__main__":
    _run_test()
    print("PASS controller distributed hardware readback ACK path")
