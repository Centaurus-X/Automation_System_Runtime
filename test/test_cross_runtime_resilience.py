# -*- coding: utf-8 -*-

"""Cross snapshot recovery, FSM pool path and replay identity test."""

import copy
import json
import os
import stat
import sys
import tempfile
import time

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core._cross_worker_thread import (
    _deterministic_task_id,
    create_cross_worker,
    dispatch_cross_event,
    dispatch_cross_fsm_event,
    dispatch_cross_pool_result,
    get_cross_worker_snapshot,
    start_cross_worker,
    stop_cross_worker,
)
from src.library import _generic_type_library as type_library
from src.library._generic_system_contract_library.config_sharding_contracts import (
    canonical_fingerprint,
)
from src.library._cross_snapshot_store import (
    create_cross_snapshot_capabilities,
    create_cross_snapshot_store,
)
from src.library._generic_thread_pool import create_thread_pool


CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "device_state_data_v8_cross_fn-01.json",
)
CONTROLLER_UUID = "82fb9eb0-3fd9-5db5-a04e-7f06e405cfdf"


def _cross_runtime(config):
    return type_library.normalize_cross_v8_for_runtime(
        config, representation="storage")

def _collect(target, task):
    target.append(copy.deepcopy(task))
    return True


def _event(sequence, timestamp=None):
    timestamp = time.time() if timestamp is None else float(timestamp)
    return {
        "event_type": "SENSOR_VALUE_UPDATE",
        "payload": {
            "controller_uuid": CONTROLLER_UUID,
            "sensor_id": 1,
            "value": 30.0,
            "quality": "good",
            "timestamp": timestamp,
            "sequence": sequence,
        },
    }


def _pool():
    pool = create_thread_pool(
        name="CrossResiliencePool",
        min_workers=1,
        max_workers=1,
        json_config={},
        join_timeout_s=2.0,
    )
    pool["start"]()
    return pool


def _context(config, pool, path, tasks):
    return create_cross_worker(
        options={
            "enabled": True,
            "node_id": "fn-01",
            "domain_id": "cross-fn-01-default",
            "snapshot_path": path,
            "max_signal_age_s": 15.0,
            "join_timeout_s": 2.0,
        },
        package={
            "config": config,
            "revision": 1,
            "execution_epoch": 0,
            "fingerprint": canonical_fingerprint(config),
        },
        pool=pool,
        local_dispatcher=partial(_collect, tasks),
        remote_dispatcher=partial(_collect, tasks),
    )


def _relay_until(pool, context, predicate, timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate(get_cross_worker_snapshot(context)):
            return True
        try:
            result = pool["_out_q"].get(timeout=0.1)
        except Exception:
            continue
        try:
            assert dispatch_cross_pool_result(context, result)
        finally:
            pool["_out_q"].task_done()
    return False


def _pipeline_complete(snapshot):
    return snapshot["statistics"]["jobs_completed"] >= 1


def _fsm_complete(snapshot):
    return snapshot["statistics"]["fsm_updates"] >= 1


def _tamper_snapshot(path):
    with open(path, "r", encoding="utf-8") as handle:
        record = json.load(handle)
    record["revision"] = 99
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(record, handle, sort_keys=True)


def run_test():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        legacy_config = json.load(handle)
    config = _cross_runtime(legacy_config)
    stable_event = _event(7, timestamp=1000.0)
    trigger_a = {"event_key": stable_event["payload"], "now": 1000.0}
    trigger_b = {"event_key": stable_event["payload"], "now": 9000.0}
    task = {
        "controller_uuid": CONTROLLER_UUID,
        "actuator_id": 1,
        "control_value": 1,
    }
    assert _deterministic_task_id(
        "cross-fn-01-default", 1, 0, trigger_a, task, 1
    ) == _deterministic_task_id(
        "cross-fn-01-default", 1, 0, trigger_b, task, 1
    )

    with tempfile.TemporaryDirectory(prefix="cross-runtime-") as directory:
        path = os.path.join(directory, "snapshot.json")
        tasks = []
        pool = _pool()
        context = _context(config, pool, path, tasks)
        start_cross_worker(context)
        event = _event(1)
        assert dispatch_cross_event(
            context, event, received_at_s=event["payload"]["timestamp"])
        assert _relay_until(pool, context, _pipeline_complete)
        assert tasks
        assert dispatch_cross_fsm_event(context, {
            "process_name": "ester_hydrolysis",
            "event_name": "activate",
        })
        assert _relay_until(pool, context, _fsm_complete)
        snapshot = get_cross_worker_snapshot(context)
        assert snapshot["fsm"]["states"][
            "ester_hydrolysis"]["state"] == "activated"
        if os.name == "posix":
            assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
        stop_cross_worker(context, join_timeout_s=2.0)
        pool["shutdown"](join_timeout_s=2.0)

        recovered_tasks = []
        recovered_pool = _pool()
        recovered = _context(config, recovered_pool, path, recovered_tasks)
        recovered_snapshot = get_cross_worker_snapshot(recovered)
        assert recovered_snapshot["io"]["cached_fact_count"] == 1
        assert recovered_snapshot["fsm"]["states"][
            "ester_hydrolysis"]["state"] == "activated"
        start_cross_worker(recovered)
        duplicate = copy.deepcopy(event)
        assert dispatch_cross_event(
            recovered,
            duplicate,
            received_at_s=duplicate["payload"]["timestamp"] + 1.0,
        )
        time.sleep(0.2)
        assert get_cross_worker_snapshot(recovered)["statistics"][
            "jobs_submitted"] == 0
        assert recovered_tasks == []
        stop_cross_worker(recovered, join_timeout_s=2.0)
        recovered_pool["shutdown"](join_timeout_s=2.0)

        _tamper_snapshot(path)
        store = create_cross_snapshot_store(path)
        api = create_cross_snapshot_capabilities(store)
        tampered = api["load"]()
        assert not tampered["ok"]
        assert tampered["code"] == "record_fingerprint"

    print("PASS Cross snapshot recovery, FSM pool path and stable replay identity")


if __name__ == "__main__":
    run_test()


