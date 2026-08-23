# -*- coding: utf-8 -*-

"""Remote fact to remote UUID action through the isolated Cross Worker."""

import copy
import os
import sys
import time

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core._cross_worker_thread import (
    create_cross_worker,
    dispatch_cross_event,
    dispatch_cross_pool_result,
    get_cross_worker_snapshot,
    start_cross_worker,
    stop_cross_worker,
)
from src.library._generic_system_contract_library.config_sharding_contracts import (
    build_cross_execution_shard,
    canonical_fingerprint,
    validate_cross_execution_shard,
)
from src.library._generic_thread_pool import create_thread_pool
from test.test_cross_scope_library import _cross_worker_fixture, _load_config


REMOTE_UUID = "06ab4528-f8a3-5ec7-a91f-eea79d60761e"


def _capture(target, task):
    target.append(copy.deepcopy(task))
    return True


def _remote_fact_fixture():
    config = _cross_worker_fixture(_load_config())
    config["sensors"].append({
        "node_id": "fn-02",
        "local_controller_id": 1,
        "controller_uuid": REMOTE_UUID,
        "controller_key": "ctrl_remote",
        "enabled": True,
        "rows": [{
            "sensor_id": 1,
            "bus_type": "modbus",
            "function_type": "read_holding_registers",
            "address": 601,
            "sensor_group": "X",
        }],
    })
    state = next(
        row for row in config["process_states"] if row.get("state_id") == 30)
    state["io_refs"]["sensors"].append({
        "controller_uuid": REMOTE_UUID,
        "sensor_id": 1,
    })
    rule = next(row for row in config["rules"] if row.get("rule_id") == 30)
    rule["when"] = [{
        "src": "sensor",
        "controller_uuid": REMOTE_UUID,
        "sensor_id": 1,
        "op": ">",
        "value": 45000,
    }]
    shard = build_cross_execution_shard(config, "fn-01")
    shard["meta"][0]["domain_id"] = "cross-worker-e2e"
    return shard


def _relay(pool, context, timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        snapshot = get_cross_worker_snapshot(context)
        if snapshot["statistics"]["jobs_completed"] >= 1:
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


def run_test():
    config = _remote_fact_fixture()
    assert validate_cross_execution_shard(config, "fn-01")["ok"]
    local_tasks = []
    remote_tasks = []
    pool = create_thread_pool(
        name="CrossWorkerEndToEndPool",
        min_workers=1,
        max_workers=1,
        json_config={},
        join_timeout_s=2.0,
    )
    pool["start"]()
    context = create_cross_worker(
        options={
            "enabled": True,
            "node_id": "fn-01",
            "domain_id": "cross-worker-e2e",
            "max_signal_age_s": 15.0,
            "join_timeout_s": 2.0,
        },
        package={
            "config": config,
            "revision": 1,
            "fingerprint": canonical_fingerprint(config),
        },
        pool=pool,
        local_dispatcher=partial(_capture, local_tasks),
        remote_dispatcher=partial(_capture, remote_tasks),
    )
    start_cross_worker(context)
    now_s = time.time()
    assert dispatch_cross_event(
        context,
        {
            "event_type": "SENSOR_VALUE_UPDATE",
            "source_node": "fn-02",
            "payload": {
                "controller_uuid": REMOTE_UUID,
                "sensor_id": 1,
                "value": 50000,
                "quality": "good",
                "timestamp": now_s,
                "sequence": 1,
            },
        },
        source_node="fn-02",
        received_at_s=now_s,
    )
    assert _relay(pool, context)
    selected = [
        task for task in remote_tasks
        if task.get("controller_uuid") == REMOTE_UUID
        and task.get("actuator_id") == 1
    ]
    assert selected
    assert all(task.get("target_owner_node_id") == "fn-02" for task in selected)
    assert all("address" not in task for task in selected)
    snapshot = get_cross_worker_snapshot(context)
    assert snapshot["statistics"]["tasks_remote"] >= 1
    stop_cross_worker(context, join_timeout_s=2.0)
    pool["shutdown"](join_timeout_s=2.0)
    print("PASS remote fact to fenced remote UUID action through Cross Worker")


if __name__ == "__main__":
    run_test()
