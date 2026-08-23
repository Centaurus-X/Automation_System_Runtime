# -*- coding: utf-8 -*-

"""Functional MQTT interface command, ACK, dedupe and event test."""

import copy
import json
import os
import sys
import threading
import time

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.interfaces import mqtt_interface


def _fake_snapshot(context):
    return {
        "state": context["state"],
        "connected": context["connected"],
        "thread_alive": False,
        "outbound_queue_depth": 0,
    }


def _fake_start(context):
    context["state"] = "connected"
    context["connected"] = True
    context["on_connect"](_fake_snapshot(context))
    return None


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


def _fake_transport_factory(**kwargs):
    context = {
        "state": "created",
        "connected": False,
        "published": [],
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


def _fake_wait(context, _timeout):
    return context["connected"]


def _wait_for(predicate, timeout_s=2.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _published_acks(transport):
    return [
        json.loads(item["payload"].decode("utf-8"))
        for item in transport["published"]
        if "/acks/" in item["topic"]
    ]


def _run_test():
    state = {"revision": 4, "config": {"process_states": [{"value_id": 1}]}, "writes": 0}
    lock = threading.RLock()

    def request_patch(path, value=None, **kwargs):
        with lock:
            state["writes"] += 1
            state["config"]["last"] = {"path": path, "value": value}
            state["revision"] += 1
            return {"ok": True, "revision": state["revision"], "rollout": {"ok": True}}

    def get_revision():
        with lock:
            return state["revision"]

    capabilities = {
        "request_patch": request_patch,
        "request_replace": partial(_return_result, {"ok": True}),
        "request_sync": partial(_return_result, {"ok": True}),
        "get_snapshot": partial(_state_config, state, lock),
        "get_revision": get_revision,
        "get_shard": partial(_return_result, {"sections": {}}),
    }
    options = {
        "enabled": True,
        "site_id": "site-test",
        "node_id": "fn-01",
        "heartbeat_interval_s": 30.0,
        "transport": {"tls": {"enabled": True}},
        "commands": {
            "require_base_revision": True,
            "allowed": ["config.patch", "config.get", "runtime.status"],
        },
        "command_auth": {
            "require_hmac": True,
            "sign_outbound": True,
            "secret": "unit-test-secret",
        },
        "event_publication": {"event_types": ["SENSOR_VALUE_UPDATE"]},
    }
    api = mqtt_interface.create_mqtt_interface(
        options=options,
        config_capabilities=capabilities,
        runtime_status_reader=partial(_return_result, {"state": "running"}),
        transport_factory=_fake_transport_factory,
    )
    api["start"]()
    context = api["context"]
    transport = context["transport"]["context"]
    command = mqtt_interface._base_envelope(context, "command", "cmd-1")
    command.update({
        "command": "config.patch",
        "target_node": "fn-01",
        "principal": "test-operator",
        "payload": {
            "path": "process_states.0.value_id",
            "value": 2,
            "base_revision": 4,
        },
    })
    signed = mqtt_interface._sign_envelope(context, command)
    message = {
        "topic": context["topics"]["node_root"] + "/commands/config/patch",
        "payload": mqtt_interface._canonical_bytes(signed),
        "retain": False,
    }
    transport["on_message"](copy.deepcopy(message))
    assert _wait_for(partial(_writes_equal, state, lock, 1))
    assert _wait_for(partial(_ack_count_at_least, transport, 1))
    transport["on_message"](copy.deepcopy(message))
    assert _wait_for(partial(_ack_count_at_least, transport, 2))
    assert state["writes"] == 1
    assert _published_acks(transport)[-1]["result"]["ok"] is True
    assert api["publish_event"](
        "SENSOR_VALUE_UPDATE", {"controller_uuid": "c-1", "sensor_id": 1})
    api["stop"](join_timeout_s=2.0)
    snapshot = api["snapshot"]()
    assert snapshot["state"] == "stopped"
    assert snapshot["statistics"]["duplicates"] == 1


def _return_result(result, *_args, **_kwargs):
    return copy.deepcopy(result)


def _state_config(state, lock):
    with lock:
        return copy.deepcopy(state["config"])


def _writes_equal(state, lock, expected):
    with lock:
        return state["writes"] == expected


def _ack_count_at_least(transport, minimum):
    return len(_published_acks(transport)) >= minimum


if __name__ == "__main__":
    _run_test()
    print("PASS MQTT interface command ACK, HMAC, dedupe and event publication")
