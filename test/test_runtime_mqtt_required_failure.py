# -*- coding: utf-8 -*-

"""Required MQTT connection startup rollback test."""

import os
import shutil
import sys
import tempfile
import threading

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.system.bootstrap_runtime_interface import start_runtime, stop_runtime


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _snapshot(context):
    return {
        "state": context["state"], "connected": False,
        "thread_alive": False, "outbound_queue_depth": 0,
    }


def _start(context):
    context["state"] = "disconnected"


def _stop(context, **_kwargs):
    context["state"] = "stopped"
    return _snapshot(context)


def _publish(_context, _topic, _payload, **_kwargs):
    return False


def _wait(_context, _timeout):
    return False


def _unavailable_transport(**_kwargs):
    context = {"state": "created"}
    return {
        "context": context,
        "start": partial(_start, context),
        "stop": partial(_stop, context),
        "publish": partial(_publish, context),
        "subscribe": partial(_publish, context),
        "wait_connected": partial(_wait, context),
        "snapshot": partial(_snapshot, context),
    }


def _main_context(config_path):
    return {
        "shutdown_event": threading.Event(),
        "app_config": {
            "app": {"site_id": "site-test", "node_id": "fn-01"},
            "runtime": {
                "event_router": {"enabled": True, "worker_threads": 1,
                                 "ingress_timeout_s": 0.02},
                "value_store": {"persistence": {"enabled": False}},
                "data_center": {"json_path": config_path,
                                "allow_legacy_import": True,
                                "legacy_import_profile": "lab_v2"},
            },
            "thread_management": {
                "pool_min_workers": 1, "pool_max_workers": 1,
                "controllers_enabled": False, "connections_enabled": False,
            },
            "state_event_management": {"persistence_enabled": False},
            "mqtt_interface": {
                "enabled": True, "required": True, "connect_wait_s": 0.1,
                "transport": {"tls": {"enabled": True}},
            },
            "shutdown": {"join_timeout_s": 1.0},
        },
    }


def _test_required_rollback(directory):
    config_path = os.path.join(directory, "config-required.json")
    shutil.copy2(SOURCE_CONFIG, config_path)
    main_ctx = _main_context(config_path)
    failed = False
    try:
        start_runtime(main_ctx, mqtt_transport_factory=_unavailable_transport)
    except RuntimeError:
        failed = True
    assert failed
    runtime = main_ctx["runtime"]
    assert runtime["state"] == "failed"
    assert main_ctx["shutdown_event"].is_set()
    assert main_ctx["started_threads"] == []
    for component in runtime["components"].values():
        thread = component.get("thread")
        assert thread is None or not thread.is_alive()


def _test_degraded_local_start(directory):
    config_path = os.path.join(directory, "config-degraded.json")
    shutil.copy2(SOURCE_CONFIG, config_path)
    main_ctx = _main_context(config_path)
    main_ctx["app_config"]["mqtt_interface"][
        "startup_connection_required"] = False
    start_runtime(main_ctx, mqtt_transport_factory=_unavailable_transport)
    try:
        runtime = main_ctx["runtime"]
        assert runtime["state"] == "running"
        component = runtime["components"]["mqtt_interface"]
        assert component["state"] == "degraded"
        assert component["thread"].is_alive()
        assert not main_ctx["shutdown_event"].is_set()
    finally:
        stop_runtime(main_ctx)
    assert main_ctx["runtime"]["state"] == "stopped"
    assert not main_ctx["started_threads"]


def _run_test():
    with tempfile.TemporaryDirectory(prefix="mqtt_required_") as directory:
        _test_required_rollback(directory)
        _test_degraded_local_start(directory)


if __name__ == "__main__":
    _run_test()
    print("PASS MQTT required rollback and degraded local reconnect startup")
