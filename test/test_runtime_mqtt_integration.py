# -*- coding: utf-8 -*-

"""Runtime MQTT, external SEM commit, local ownership and shutdown test."""

import copy
import json
import os
import shutil
import sys
import tempfile
import threading
import time

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._event_interface import make_event, sync_queue_put
from src.library import _generic_type_library as type_library
from src.library._state_shard_manager import get_node_controller_uuids
from src.system.bootstrap_runtime_interface import (
    get_runtime_config_snapshot,
    get_runtime_snapshot,
    start_runtime,
    stop_runtime,
)


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _fake_snapshot(context):
    return {
        "state": context["state"], "connected": context["connected"],
        "thread_alive": False, "outbound_queue_depth": 0,
    }


def _fake_start(context):
    context["state"] = "connected"
    context["connected"] = True
    context["on_connect"](_fake_snapshot(context))


def _fake_stop(context, **_kwargs):
    context["state"] = "stopped"
    context["connected"] = False
    return _fake_snapshot(context)


def _fake_publish(context, topic, payload, **kwargs):
    context["published"].append({
        "topic": topic,
        "payload": bytes(payload),
        "qos": kwargs.get("qos"),
        "retain": kwargs.get("retain"),
    })
    return True


def _fake_wait(context, _timeout):
    return context["connected"]


def _fake_transport_factory(**kwargs):
    context = {
        "state": "created", "connected": False, "published": [],
        "on_message": kwargs.get("on_message"),
        "on_connect": kwargs.get("on_connect"),
        "on_disconnect": kwargs.get("on_disconnect"),
    }
    return {
        "context": context,
        "start": partial(_fake_start, context),
        "stop": partial(_fake_stop, context),
        "publish": partial(_fake_publish, context),
        "subscribe": partial(_fake_publish, context),
        "wait_connected": partial(_fake_wait, context),
        "snapshot": partial(_fake_snapshot, context),
    }


def _idle_controller(**kwargs):
    kwargs["shutdown_event"].wait()


def _wait_for(predicate, timeout_s=5.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _has_topic(transport, fragment):
    return any(fragment in item["topic"] for item in transport["published"])


def _deliver_command(transport, interface_context, command):
    suffix = str(command["command"]).replace(".", "/")
    transport["on_message"]({
        "topic": interface_context["topics"]["node_root"] + "/commands/" + suffix,
        "payload": json.dumps(command).encode("utf-8"),
        "retain": False,
    })


def _assert_local_workers(tm, config):
    expected = set(get_node_controller_uuids(config, "fn-01"))
    assert set(tm["controller_workers"]) == expected, (
        tuple(sorted(tm["controller_workers"])), tuple(sorted(expected)))
    remote = set(get_node_controller_uuids(config, "fn-02"))
    assert not remote & set(tm["controller_workers"])
    return expected


def _main_context(config_path):
    safety = {
        "enabled": True,
        "external_commits": {
            "enabled": True,
            "require_base_revision": True,
            "allowed_operations": ["patch", "replace"],
            "allowed_principals": ["integration-operator"],
            "allowed_patch_modes": ["set"],
            "allowed_path_prefixes": ["process_states"],
        },
        "rule_actions": {"enabled": False},
        "actuator_tasks": {"enabled": False},
    }
    return {
        "shutdown_event": threading.Event(),
        "app_config": {
            "app": {"site_id": "site-test", "node_id": "fn-01"},
            "runtime": {
                "event_router": {"enabled": True, "worker_threads": 2,
                                 "ingress_timeout_s": 0.02},
                "value_store": {"persistence": {"enabled": False}},
                "data_center": {"json_path": config_path,
                                "verify_on_load": True, "verify_on_save": True,
                                "allow_legacy_import": True,
                                "legacy_import_profile": "lab_v2"},
            },
            "thread_management": {
                "ingress_timeout_s": 0.02, "pool_min_workers": 1,
                "pool_max_workers": 2, "controllers_enabled": True,
                "connections_enabled": False,
            },
            "state_event_management": {
                "ingress_timeout_s": 0.02, "request_timeout_s": 5.0,
                "persistence_enabled": False, "safety": safety,
            },
            "mqtt_interface": {
                "enabled": True, "required": False,
                "site_id": "site-test", "node_id": "fn-01",
                "heartbeat_interval_s": 30.0,
                "transport": {"tls": {"enabled": True}},
                "commands": {"require_base_revision": True,
                             "allowed": ["config.patch", "config.replace",
                                         "config.get"]},
                "event_publication": {"event_types": [
                    "SENSOR_VALUE_UPDATE", "CONFIG_COMMIT_APPLIED"]},
            },
            "shutdown": {"join_timeout_s": 3.0},
        },
    }


def _run_test():
    with tempfile.TemporaryDirectory(prefix="runtime_mqtt_") as directory:
        config_path = os.path.join(directory, "config.json")
        shutil.copy2(SOURCE_CONFIG, config_path)
        main_ctx = _main_context(config_path)
        try:
            start_runtime(
                main_ctx,
                controller_target=_idle_controller,
                mqtt_transport_factory=_fake_transport_factory,
            )
            runtime = main_ctx["runtime"]
            mqtt = runtime["mqtt_interface"]
            interface_context = mqtt["context"]
            transport = interface_context["transport"]["context"]
            tm = runtime["components"]["thread_management"]["context"]
            with open(config_path, "r", encoding="utf-8") as handle:
                full_config = json.load(handle)
            expected = _assert_local_workers(tm, full_config)
            command = {
                "protocol": "automation.mqtt.v1",
                "kind": "command",
                "message_id": "runtime-command-1",
                "site_id": "site-test",
                "source_node": "operator-console",
                "target_node": "fn-01",
                "created_at_s": time.time(),
                "command": "config.patch",
                "principal": "integration-operator",
                "payload": {
                    "path": {
                        "section": "process_states",
                        "state_id": full_config["process_states"][0]["state_id"],
                        "field": "parameters.target_temp_c",
                    },
                    "value": 555.0,
                    "base_revision": 1,
                },
            }
            _deliver_command(transport, interface_context, command)
            assert _wait_for(partial(_has_topic, transport, "/acks/runtime-command-1"))
            config = get_runtime_config_snapshot(runtime)
            assert config["process_states"][0]["parameters"]["target_temp_c"] == 555.0
            assert tm["config_revision"] == 2
            assert _has_topic(transport, "/events/config_commit_applied/")
            replacement = type_library.project_config_for_storage(config)
            replacement["process_states"][0]["parameters"]["target_temp_c"] = 556.0
            replace_command = {
                "protocol": "automation.mqtt.v1",
                "kind": "command",
                "message_id": "runtime-command-2",
                "site_id": "site-test",
                "source_node": "operator-console",
                "target_node": "fn-01",
                "created_at_s": time.time(),
                "command": "config.replace",
                "principal": "integration-operator",
                "payload": {"config": replacement, "base_revision": 2},
            }
            _deliver_command(transport, interface_context, replace_command)
            assert _wait_for(partial(_has_topic, transport, "/acks/runtime-command-2"))
            config = get_runtime_config_snapshot(runtime)
            assert config["process_states"][0]["parameters"]["target_temp_c"] == 556.0
            assert tm["config_revision"] == 3
            sensor_event = make_event(
                "SENSOR_VALUE_UPDATE",
                payload={"controller_uuid": next(iter(expected)),
                         "sensor_id": 1, "value": 12.5, "quality": "good"},
                source="integration_test",
                target="thread_management",
            )
            assert sync_queue_put(runtime["queues"]["queue_event_send"], sensor_event)
            assert _wait_for(partial(
                _has_topic, transport, "/events/sensor_value_update/"))
            stop_runtime(main_ctx)
            snapshot = get_runtime_snapshot(runtime)
            assert snapshot["state"] == "stopped"
            assert snapshot["mqtt_interface"]["state"] == "stopped"
            assert not any(thread.is_alive() for thread in main_ctx["started_threads"])
        finally:
            active_runtime = main_ctx.get("runtime")
            if (
                isinstance(active_runtime, dict)
                and active_runtime.get("state") != "stopped"
            ):
                stop_runtime(main_ctx)


if __name__ == "__main__":
    _run_test()
    print("PASS runtime MQTT external commit, local ownership, events and shutdown")
