# -*- coding: utf-8 -*-

"""Every staged shutdown owner is attempted after one owner fails."""

import os
import sys
import threading

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.system import bootstrap_runtime_interface as runtime_bootstrap


STEP_NAMES = (
    "mqtt_quiesce",
    "distributed_quiesce",
    "thread_management",
    "distributed_coordinator",
    "mqtt_interface",
    "state_event_management",
    "sensor_value_persistence",
    "event_router",
    "runtime_value_store",
    "runtime_queues",
)


def _record_step(calls, name, _context):
    calls.append(name)
    if name == "mqtt_quiesce":
        raise RuntimeError("injected shutdown failure")


def _run_test():
    function_names = (
        "_quiesce_mqtt",
        "_quiesce_distributed",
        "_stop_tm",
        "_stop_distributed",
        "_stop_mqtt",
        "_stop_sem",
        "_stop_value_persistence",
        "_stop_router",
        "_close_value_store_resource",
        "_remove_runtime_queues",
    )
    originals = {
        name: getattr(runtime_bootstrap, name) for name in function_names}
    calls = []
    try:
        for function_name, step_name in zip(function_names, STEP_NAMES):
            setattr(
                runtime_bootstrap,
                function_name,
                partial(_record_step, calls, step_name),
            )
        context = {"system_shutdown_event": threading.Event()}
        failed = False
        try:
            runtime_bootstrap._staged_stop(context)
        except RuntimeError as exc:
            failed = True
            assert "mqtt_quiesce" in str(exc)
        assert failed
        assert tuple(calls) == STEP_NAMES
        assert context["system_shutdown_event"].is_set()
    finally:
        for name, target in originals.items():
            setattr(runtime_bootstrap, name, target)


if __name__ == "__main__":
    _run_test()
    print("PASS staged shutdown continues after an owner failure")
