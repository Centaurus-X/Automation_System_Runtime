# -*- coding: utf-8 -*-

"""Cross-I/O fact validation, routing, isolation and no-GIL lock test."""

import copy
import os
import sys
import threading

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.adapter._cross_io_adapter import create_cross_io_adapter


LOCAL_CONTROLLER = "controller-local"
REMOTE_CONTROLLER = "controller-remote"


def _capture(target, task):
    target.append(copy.deepcopy(task))
    return {"ok": True, "operation_id": task.get("task_uuid")}


def _sensor_event(controller_uuid, sensor_id, value, timestamp, sequence, **kwargs):
    payload = {
        "controller_uuid": controller_uuid,
        "sensor_id": sensor_id,
        "value": value,
        "quality": kwargs.get("quality", "good"),
        "timestamp": timestamp,
        "sensor_store_sequence": sequence,
    }
    return {
        "event_type": "SENSOR_VALUE_UPDATE",
        "source_node": kwargs.get("source_node"),
        "payload": {
            "event_type": "SENSOR_VALUE_UPDATE",
            "payload": payload,
        },
    }


def _adapter(local_calls, remote_calls):
    return create_cross_io_adapter(
        options={
            "node_id": "fn-01",
            "owner_node_id": "fn-01",
            "cross_domain_id": "pressure-relief",
            "max_signal_age_s": 10.0,
            "max_future_skew_s": 1.0,
        },
        ownership={
            LOCAL_CONTROLLER: "fn-01",
            REMOTE_CONTROLLER: "fn-02",
        },
        required_facts=[
            {"controller_uuid": LOCAL_CONTROLLER, "sensor_id": 1},
            {"controller_uuid": REMOTE_CONTROLLER, "sensor_id": 3},
        ],
        local_controller_uuids=[LOCAL_CONTROLLER],
        local_dispatcher=partial(_capture, local_calls),
        remote_dispatcher=partial(_capture, remote_calls),
    )


def _test_fact_validation(api):
    unknown = api["ingest_sensor_event"](
        _sensor_event(LOCAL_CONTROLLER, 99, 1, 100.0, 1),
        source_node_id="fn-01",
        now_s=100.0,
    )
    assert not unknown["ok"] and unknown["code"] == "unknown_fact"

    forged = api["ingest_sensor_event"](
        _sensor_event(REMOTE_CONTROLLER, 3, 9, 100.0, 1),
        source_node_id="fn-01",
        now_s=100.0,
    )
    assert not forged["ok"] and forged["code"] == "owner_mismatch"

    asserted_owner = _sensor_event(
        REMOTE_CONTROLLER, 3, 9, 100.0, 1
    )
    asserted_owner["payload"]["payload"]["owner_node_id"] = "fn-02"
    forged = api["ingest_sensor_event"](asserted_owner, now_s=100.0)
    assert not forged["ok"] and forged["code"] == "owner_mismatch"

    accepted = api["ingest_sensor_event"](
        _sensor_event(REMOTE_CONTROLLER, 3, 42, 100.0, 7),
        source_node_id="fn-02",
        now_s=100.0,
    )
    assert accepted["ok"]
    duplicate = api["ingest_sensor_event"](
        _sensor_event(REMOTE_CONTROLLER, 3, 99, 101.0, 7),
        source_node_id="fn-02",
        now_s=101.0,
    )
    assert not duplicate["ok"] and duplicate["code"] == "stale_sequence"
    snapshot = api["get_fact_snapshot"](now_s=105.0)
    assert snapshot["sensors"][REMOTE_CONTROLLER][3]["value"] == 42
    assert not snapshot["complete"]
    assert snapshot["missing"][0]["reason"] == "not_received"

    stale = api["get_fact_snapshot"](now_s=111.0)
    assert REMOTE_CONTROLLER not in stale["sensors"]
    assert any(item["reason"] == "stale" for item in stale["missing"])

    unavailable = api["ingest_sensor_event"](
        _sensor_event(
            LOCAL_CONTROLLER, 1, None, 112.0, 1, quality="unavailable"
        ),
        source_node_id="fn-01",
        now_s=112.0,
    )
    assert not unavailable["ok"]
    assert unavailable["code"] == "invalid_quality"


def _test_routes_and_isolation(api, local_calls, remote_calls):
    local_task = {
        "task_uuid": "local-task",
        "execution_epoch": 0,
        "controller_uuid": LOCAL_CONTROLLER,
        "actuator_id": 1,
        "control_value": 1,
        "metadata": {"caller": "cross-test"},
    }
    original = copy.deepcopy(local_task)
    local = api["route_actuator_task"](local_task)
    assert local["ok"] and local["route"] == "local"
    assert len(local_calls) == 1 and not remote_calls
    assert local_task == original
    assert local_calls[0]["execution_scope"] == "cross"
    assert local_calls[0]["cross_owner_node_id"] == "fn-01"
    assert local_calls[0]["cross_domain_id"] == "pressure-relief"
    assert local_calls[0]["target_owner_node_id"] == "fn-01"

    remote_task = {
        "task_uuid": "remote-task",
        "execution_epoch": 0,
        "controller_uuid": REMOTE_CONTROLLER,
        "actuator_id": 8,
        "control_value": 123,
    }
    remote = api["route_actuator_task"](remote_task)
    assert remote["ok"] and remote["route"] == "remote"
    assert len(remote_calls) == 1 and len(local_calls) == 1
    assert remote_calls[0]["target_owner_node_id"] == "fn-02"

    unknown = api["route_actuator_task"]({
        "controller_uuid": "controller-unknown",
        "execution_epoch": 0,
        "actuator_id": 1,
        "control_value": 0,
    })
    assert not unknown["ok"] and unknown["code"] == "unknown_target"
    assert len(remote_calls) == 1 and len(local_calls) == 1

    local_calls[0]["metadata"]["caller"] = "mutated"
    assert local["task"]["metadata"]["caller"] == "cross-test"


def _concurrent_ingest(api, sequence):
    report = api["ingest_sensor_event"](
        _sensor_event(
            LOCAL_CONTROLLER,
            1,
            sequence,
            200.0 + sequence,
            sequence,
        ),
        source_node_id="fn-01",
        now_s=240.0,
    )
    assert isinstance(report, dict)


def _test_free_threaded_locking(api):
    threads = []
    for sequence in range(1, 33):
        thread = threading.Thread(
            target=_concurrent_ingest,
            args=(api, sequence),
            name="CrossIoTest-{}".format(sequence),
        )
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join(2.0)
        assert not thread.is_alive()
    facts = api["get_fact_snapshot"](now_s=233.0)
    assert facts["sensors"][LOCAL_CONTROLLER][1]["sequence"] == 32
    exported = api["snapshot"]()
    exported["statistics"]["facts_accepted"] = -1
    assert api["snapshot"]()["statistics"]["facts_accepted"] >= 1


def _run_test():
    local_calls = []
    remote_calls = []
    api = _adapter(local_calls, remote_calls)
    _test_fact_validation(api)
    _test_routes_and_isolation(api, local_calls, remote_calls)
    _test_free_threaded_locking(api)


if __name__ == "__main__":
    _run_test()
    print("PASS isolated cross-I/O facts, routing and free-threaded locking")

