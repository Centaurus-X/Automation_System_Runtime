# -*- coding: utf-8 -*-

"""SEM safety, atomic action plan, revision and deduplication test."""

import copy
import os
import shutil
import sys
import tempfile
import threading

from functools import partial
from queue import Queue


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.state_event_management import (
    register_external_publisher,
    register_rollout_handler,
    request_config_action_plan,
    request_config_patch,
    request_config_replace,
    start_state_event_management,
    stop_state_event_management,
)
from src.library import _generic_data_center as data_center


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _rollout(bucket, request):
    bucket.append(request)
    return {"ok": True, "revision": request["revision"]}


def _publish(bucket, event_type, payload, **_kwargs):
    bucket.append({"event_type": event_type, "payload": payload})
    return True


def _fail_once_rollout(bucket, request):
    bucket.append(request)
    if len(bucket) == 1:
        return {"ok": False, "error": "injected rollout failure"}
    return {"ok": True, "revision": request["revision"], "restored": True}


def _options():
    return {
        "ingress_timeout_s": 0.02,
        "request_timeout_s": 3.0,
        "persistence_enabled": False,
        "safety": {
            "enabled": True,
            "rule_actions": {
                "enabled": True,
                "require_base_revision": True,
                "enforce_writable_fields": True,
                "enforce_required_mode": True,
                "parameter_limits": {
                    "target_temp_c": {"min": 0.0, "max": 800.0},
                },
            },
            "external_commits": {
                "enabled": True,
                "require_base_revision": True,
                "allowed_operations": ["patch"],
                "allowed_patch_modes": ["set"],
                "allowed_path_prefixes": ["process_states"],
            },
        },
    }


def _action(value, state_id=1):
    return [{
        "seq": 0,
        "action": {
            "state_ids": [state_id],
            "parameters": {"target_temp_c": value},
        },
    }]


def _run_test():
    with tempfile.TemporaryDirectory(prefix="sem_safety_") as directory:
        config_path = os.path.join(directory, "config.json")
        shutil.copy2(SOURCE_CONFIG, config_path)
        center = data_center.create_data_center_context(
            name="sem-safety-test", json_path=config_path,
            allow_legacy_import=True, legacy_import_profile="lab_v2")
        assert data_center.load_json_into_heap(center)["status"] == "OK"
        queues = {"queue_event_state": Queue(), "queue_event_send": Queue()}
        runtime = {}
        thread = start_state_event_management(
            data_center=center,
            queues=queues,
            stop_event=threading.Event(),
            options=_options(),
            runtime=runtime,
        )
        context = runtime["context"]
        rollouts = []
        events = []
        register_rollout_handler(context, partial(_rollout, rollouts))
        register_external_publisher(context, partial(_publish, events))
        valid = request_config_action_plan(
            context, _action(556.0), base_revision=1,
            operation_id="rule-plan-1")
        assert valid["ok"] is True and valid["revision"] == 2
        duplicate = request_config_action_plan(
            context, _action(556.0), base_revision=1,
            operation_id="rule-plan-1")
        assert duplicate["ok"] is True and duplicate["duplicate"] is True
        assert data_center.get_revision(center) == 2
        atomic = _action(557.0) + [{
            "seq": 1,
            "action": {"state_ids": [1], "parameters": {"target_temp_c": 900.0}},
        }]
        rejected = request_config_action_plan(
            context, atomic, base_revision=2, operation_id="rule-plan-2")
        assert rejected["ok"] is False
        assert data_center.heap_get(
            center, "process_states.0.parameters.target_temp_c") == 556.0
        wrong_mode = request_config_action_plan(
            context, _action(100.0, state_id=3), base_revision=2,
            operation_id="rule-plan-3")
        assert wrong_mode["ok"] is False
        stale = request_config_action_plan(
            context, _action(557.0), base_revision=1,
            operation_id="rule-plan-4")
        assert stale["ok"] is False and stale["safety"]["code"] == "stale_revision"
        external = request_config_patch(
            context,
            {
                "section": "process_states",
                "state_id": 1,
                "field": "parameters.target_temp_c",
            },
            558.0,
            source="mqtt_external:test",
            external=True,
            principal="test",
            base_revision=2,
            operation_id="external-1",
        )
        assert external["ok"] is True and external["revision"] == 3
        found, snapshot = data_center.heap_snapshot(center)
        assert found
        denied = request_config_replace(
            context, snapshot, source="mqtt_external:test", external=True,
            principal="test", base_revision=3, operation_id="external-2")
        assert denied["ok"] is False
        assert data_center.get_revision(center) == 3
        assert len(rollouts) == 2
        assert len(events) == 2
        assert all(item["event_type"] == "CONFIG_COMMIT_APPLIED" for item in events)

        # A runtime projection failure must leave no half-applied heap. The
        # compensating commit and rollout restore the exact prior bundle while
        # the original request remains visibly failed.
        found, stable = data_center.heap_snapshot(center)
        assert found
        failed_rollouts = []
        register_rollout_handler(
            context, partial(_fail_once_rollout, failed_rollouts))
        candidate = copy.deepcopy(stable)
        candidate["process_states"][0]["parameters"]["target_temp_c"] = 559.0
        failed = request_config_replace(
            context, candidate, source="runtime_compensation_test",
            operation_id="compensation-1")
        assert failed["ok"] is False
        assert failed["compensation"]["restored"] is True
        found, restored = data_center.heap_snapshot(center)
        assert found and restored == stable
        assert len(failed_rollouts) == 2
        assert data_center.get_revision(center) == 5
        result = stop_state_event_management(context, join_timeout_s=3.0)
        assert result["state"] == "stopped"
        assert not thread.is_alive()


if __name__ == "__main__":
    _run_test()
    print("PASS SEM safety, atomicity, revision, dedupe and external policy")
