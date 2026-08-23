# -*- coding: utf-8 -*-

"""Controller worker ownership, config isolation and local-stop test."""

import json
import os
import sys
import threading

from functools import partial
from queue import Queue


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core._worker_thread import (
    apply_controller_config,
    create_controller_worker,
    start_controller_worker,
    stop_controller_worker,
)
from src.core import _controller_thread as controller_thread
from src.library._state_shard_manager import build_controller_projection


CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _idle_controller(**kwargs):
    kwargs["shutdown_event"].wait()


def _apply_package(results, worker, package):
    results.append(apply_controller_config(worker, package))


def _test_live_controller_ack(config, controller_uuid):
    projected = build_controller_projection(config, controller_uuid)
    controller = projected["controllers"][0]
    stop_event = threading.Event()
    event_queue = Queue()
    context = controller_thread.build_controller_context(
        controller_id=controller["controller_id"],
        controller_data=controller,
        config_data=projected,
        config_lock=threading.RLock(),
        sensor_values={},
        sensor_lock=threading.RLock(),
        queue_event_send=Queue(),
        event_queue_mbc=event_queue,
        local_sensor_values={},
        local_sensor_lock=threading.RLock(),
        local_actuator_values={},
        local_actuator_lock=threading.RLock(),
        shutdown_event=stop_event,
        runtime_resources={},
    )
    listener = threading.Thread(
        target=partial(controller_thread._event_listener_loop, context),
        daemon=False,
    )
    listener.start()
    updated = build_controller_projection(config, controller_uuid)
    updated["controllers"][0]["device_name"] = "updated-in-test"
    acknowledgement = {"event": threading.Event(), "result": None}
    event_queue.put({
        "event_type": "CONFIG_SHARD_UPDATE",
        "payload": {
            "config": updated,
            "revision": 10,
            "acknowledgement": acknowledgement,
        },
    })
    assert acknowledgement["event"].wait(3.0)
    assert acknowledgement["result"]["ok"] is True
    assert context["config_revision"] == 10
    assert context["controller_data"]["device_name"] == "updated-in-test"
    stop_event.set()
    event_queue.put({"_type": "__STOP__", "_sentinel": True})
    listener.join(timeout=3.0)
    assert not listener.is_alive()


def run_test():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    identities = (
        "62671112-766c-59ed-b0ff-30a49d48f747",
        "d11032fa-33db-56de-a4ed-7e16cca3ded7",
    )
    workers = [
        create_controller_worker(
            controller_uuid=identity,
            config_snapshot=config,
            controller_target=_idle_controller,
        )
        for identity in identities
    ]
    for worker in workers:
        start_controller_worker(worker)
    assert workers[0]["config_data"] is not workers[1]["config_data"]
    assert workers[0]["config_lock"] is not workers[1]["config_lock"]
    assert workers[0]["event_queue"] is not workers[1]["event_queue"]
    package = {
        "sections": config,
        "revision": 9,
        "fingerprint": "isolation-test",
        "snapshot_at": "test",
    }
    results = []
    update_threads = [
        threading.Thread(
            target=partial(_apply_package, results, worker, package),
            daemon=False,
        )
        for worker in workers
    ]
    for thread in update_threads:
        thread.start()
    for thread in update_threads:
        thread.join(timeout=3.0)
    assert len(results) == 2
    assert all(result["ok"] for result in results)
    assert stop_controller_worker(workers[0])["state"] == "stopped"
    assert workers[1]["thread"].is_alive()
    assert stop_controller_worker(workers[1])["state"] == "stopped"
    _test_live_controller_ack(config, identities[0])
    print("PASS isolated controller workers and local shutdown")


if __name__ == "__main__":
    run_test()
