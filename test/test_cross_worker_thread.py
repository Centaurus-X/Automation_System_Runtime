# -*- coding: utf-8 -*-

"""Isolated Cross Worker lifecycle, pool and automation integration test."""

import copy
import json
import os
import sys
import time

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core._cross_worker_thread import (
    apply_cross_config,
    create_cross_worker,
    dispatch_cross_event,
    dispatch_cross_pool_result,
    freeze_cross_execution,
    get_cross_worker_snapshot,
    start_cross_worker,
    stop_cross_worker,
)
from src.library import _generic_type_library as type_library
from src.library._generic_system_contract_library.config_sharding_contracts import (
    canonical_fingerprint,
    classify_config_scopes,
)
from src.library._generic_thread_pool import create_thread_pool


CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "device_state_data_v8_cross_fn-01.json",
)


def _collect(target, task):
    target.append(copy.deepcopy(task))
    return True


def _relay_pool_results(pool, cross_context, target_count, timeout_s=5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        snapshot = get_cross_worker_snapshot(cross_context)
        completed = snapshot["statistics"]["jobs_completed"]
        if completed >= target_count:
            return True
        try:
            result = pool["_out_q"].get(timeout=0.1)
        except Exception:
            continue
        try:
            assert dispatch_cross_pool_result(cross_context, result)
        finally:
            pool["_out_q"].task_done()
        time.sleep(0.01)
    return False


def _sensor_event(sequence, value=30.0):
    return {
        "event_type": "SENSOR_VALUE_UPDATE",
        "payload": {
            "controller_uuid": "82fb9eb0-3fd9-5db5-a04e-7f06e405cfdf",
            "sensor_id": 1,
            "value": value,
            "quality": "good",
            "timestamp": time.time(),
            "sequence": sequence,
        },
    }


def _cross_runtime(config):
    return type_library.normalize_cross_v8_for_runtime(
        config, representation="storage")


def _authority_package(
    config, revision, execution_epoch=0, activation_proof=None
):
    package = {
        "config": copy.deepcopy(config),
        "revision": int(revision),
        "execution_epoch": int(execution_epoch),
        "fingerprint": canonical_fingerprint(config),
        "classification_fingerprint": type_library.fingerprint(
            classify_config_scopes(config)),
    }
    if activation_proof is not None:
        package["activation_proof"] = copy.deepcopy(activation_proof)
    return package


def run_test():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        legacy_config = json.load(handle)
    config = _cross_runtime(legacy_config)
    local_tasks = []
    remote_tasks = []
    pool = create_thread_pool(
        name="CrossWorkerTestPool",
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
            "domain_id": "cross-fn-01-default",
            "max_signal_age_s": 15.0,
            "join_timeout_s": 2.0,
        },
        package=_authority_package(config, 1),
        pool=pool,
        local_dispatcher=partial(_collect, local_tasks),
        remote_dispatcher=partial(_collect, remote_tasks),
    )
    thread = start_cross_worker(context)
    assert thread is not None and thread.is_alive()
    assert dispatch_cross_event(context, _sensor_event(1))
    assert _relay_pool_results(pool, context, 1)
    assert len(local_tasks) == 2
    assert remote_tasks == []
    assert all(task.get("execution_scope") == "cross" for task in local_tasks)
    assert all(task.get("execution_epoch") == 0 for task in local_tasks)
    assert all("address" not in task for task in local_tasks)
    assert len({task["task_uuid"] for task in local_tasks}) == 2

    stale = apply_cross_config(
        context, _authority_package(config, 0))
    assert not stale["ok"] and stale["code"] == "stale_revision"
    changed = copy.deepcopy(config)
    changed["meta"][0]["description"] += " changed"
    conflict = apply_cross_config(
        context, _authority_package(changed, 1))
    assert not conflict["ok"] and conflict["code"] == "revision_conflict"
    applied = apply_cross_config(
        context, _authority_package(changed, 2))
    assert applied["ok"] and applied["revision"] == 2

    frozen = freeze_cross_execution(
        context,
        expected_execution_epoch=0,
        target_execution_epoch=1,
        freeze_token="freeze-cross-fn-01-1",
        registry_intent_id="registry-intent-1",
    )
    assert frozen["ok"] and frozen["old_owner_frozen_ack"] is True
    rejected_same_epoch = apply_cross_config(
        context, _authority_package(changed, 3))
    assert not rejected_same_epoch["ok"]
    assert rejected_same_epoch["code"] == "execution_epoch_stale"

    activated_config = copy.deepcopy(changed)
    activated_config["meta"][0]["description"] += " activated"
    activation_proof = {
        "old_owner_frozen_ack": True,
        "registry_persisted": True,
        "new_owner_node_id": "fn-01",
        "execution_epoch": 1,
        "registry_intent_id": "registry-intent-1",
    }
    activated = apply_cross_config(
        context,
        _authority_package(
            activated_config,
            3,
            execution_epoch=1,
            activation_proof=activation_proof,
        ),
    )
    assert activated["ok"] and activated["status"] == "owner_activated"
    assert activated["execution_epoch"] == 1
    assert dispatch_cross_event(context, _sensor_event(2))
    assert _relay_pool_results(pool, context, 2)
    migrated_tasks = local_tasks[2:]
    assert migrated_tasks
    assert all(task["execution_epoch"] == 1 for task in migrated_tasks)
    assert all(
        task["meta"]["registry_transition"] == activation_proof
        for task in migrated_tasks
    )

    stopped = stop_cross_worker(context, join_timeout_s=2.0)
    assert stopped["state"] == "stopped"
    assert not stopped["thread_alive"]
    # The Cross Worker borrows the central pool and never owns its lifecycle.
    assert pool["get_status_snapshot"]()["manager_alive"]
    pool_snapshot = pool["shutdown"](join_timeout_s=2.0)
    assert pool_snapshot["shutdown_status"] == "stopped"
    print("PASS isolated Cross Worker, shared pool and fenced owner migration")

if __name__ == "__main__":
    run_test()


