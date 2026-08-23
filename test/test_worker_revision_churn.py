# -*- coding: utf-8 -*-

"""Unrelated config revisions cannot starve a local control result."""

import copy
import json
import os
import sys
import threading


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core import thread_management as management
from src.library._state_shard_manager import build_automation_projection


CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"
CONFIG_PATH = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _capture_dispatch(captured, _context, task):
    captured.append(copy.deepcopy(task))
    return True


def _test_compatible_stale_result_is_dispatched_at_current_revision():
    config = _load_config()
    projection = build_automation_projection(config, CONTROLLER_UUID, 1)
    captured = []
    context = {
        "config_lock": threading.RLock(),
        "config_snapshot": copy.deepcopy(config),
        "config_revision": 2,
        "pipeline_lock": threading.RLock(),
        "pipeline_active": True,
        "pipeline_current": {"identity": CONTROLLER_UUID + ":1"},
        "pipeline_pending": {},
        "statistics": management._tm_statistics(),
    }
    original_dispatch = management._dispatch_actuator_task
    try:
        management._dispatch_actuator_task = management.partial(
            _capture_dispatch, captured)
        management._handle_automation_result(context, {
            "result": [{
                "controller_uuid": CONTROLLER_UUID,
                "controller_id": 2,
                "actuator_id": 3,
                "address": 524,
                "function_type": "write_register",
                "control_value": 123,
            }],
            "meta": {
                "config_revision": 1,
                "projection_fingerprint": (
                    management._automation_projection_fingerprint(projection)
                ),
                "sensor_payload": {
                    "controller_uuid": CONTROLLER_UUID, "sensor_id": 1,
                },
                "retry_count": 0,
            },
        })
    finally:
        management._dispatch_actuator_task = original_dispatch
    assert len(captured) == 1
    assert captured[0]["config_revision"] == 2
    assert context["statistics"]["stale_automation_results"] == 1
    assert context["statistics"]["stale_automation_compatible"] == 1
    assert context["statistics"]["pipelines_completed"] == 1


def _request_stale(_actions, **_kwargs):
    return {"ok": False, "code": "stale_revision"}


def _capture_rule_retry(captured, _context, _payload, **kwargs):
    captured.append(copy.deepcopy(kwargs))
    return "retry-job-1"


def _test_top_level_stale_response_retries_rule_job():
    captured = []
    context = {
        "resources": {
            "config_writer": {"request_action_plan": _request_stale},
        },
        "statistics": management._tm_statistics(),
    }
    original_submit = management._submit_rule_job
    try:
        management._submit_rule_job = management.partial(
            _capture_rule_retry, captured)
        result = management._request_rule_actions(
            context,
            [{"rule_id": 1, "action": {"state_ids": [1], "value_id": 1}}],
            {
                "source_event_id": "event-1", "pipeline_sequence": 1,
                "sensor_payload": {
                    "controller_uuid": CONTROLLER_UUID, "sensor_id": 1,
                },
                "retry_count": 0,
            },
            3,
        )
    finally:
        management._submit_rule_job = original_submit
    assert result == "retry"
    assert len(captured) == 1
    assert captured[0]["retry_count"] == 1


def run_test():
    _test_compatible_stale_result_is_dispatched_at_current_revision()
    _test_top_level_stale_response_retries_rule_job()
    print("PASS compatible revision churn and top-level stale retry")


if __name__ == "__main__":
    run_test()
