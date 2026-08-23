# -*- coding: utf-8 -*-

"""Functional optional RuleEngine, external commit and actuator safety test."""

import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._config_safety_policy import (
    authorize_external_commit,
    create_safety_context,
    evaluate_actuator_task,
    prepare_rule_action_plan,
)


CONFIG_PATH = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)


def _load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _sensor_reader(_controller_uuid, _sensor_id):
    return {"value": 42.0, "quality": "good"}


def _options():
    return {
        "enabled": True,
        "rule_actions": {
            "enabled": True,
            "require_base_revision": True,
            "enforce_writable_fields": True,
            "enforce_required_mode": True,
            "parameter_limits": {"target_temp_c": {"min": 0.0, "max": 800.0}},
            "interlocks": [{
                "name": "sensor_quality",
                "conditions": [{
                    "source": "sensor",
                    "controller_uuid": "unused",
                    "sensor_id": 1,
                    "field": "quality",
                    "operator": "eq",
                    "value": "good",
                }],
            }],
        },
        "external_commits": {
            "enabled": True,
            "require_base_revision": True,
            "allowed_operations": ["patch"],
            "allowed_principals": ["integration-operator"],
            "allowed_patch_modes": ["set"],
            "allowed_path_prefixes": ["process_states"],
        },
        "actuator_tasks": {
            "enabled": True,
            "require_limits": True,
            "limits": [{"actuator_id": 1, "min": 0.0, "max": 100.0}],
        },
    }


def _run_test():
    config = _load_config()
    options = _options()
    context = create_safety_context(options, sensor_reader=_sensor_reader)
    valid = [{
        "seq": 0,
        "action": {"state_ids": [1], "parameters": {"target_temp_c": 600.0}},
    }]
    report = prepare_rule_action_plan(
        context, config, valid, current_revision=7, base_revision=7)
    assert report["ok"] is True
    forbidden = [{
        "seq": 0,
        "action": {"state_ids": [1], "control_strategy": {"type": "unsafe"}},
    }]
    report = prepare_rule_action_plan(
        context, config, forbidden, current_revision=7, base_revision=7)
    assert report["ok"] is False and report["code"] == "writable_fields"
    wrong_mode = [{
        "seq": 0,
        "action": {"state_ids": [3], "value_id": 2},
    }]
    report = prepare_rule_action_plan(
        context, config, wrong_mode, current_revision=7, base_revision=7)
    assert report["ok"] is False and report["code"] == "required_mode"
    outside = [{
        "seq": 0,
        "action": {"state_ids": [1], "parameters": {"target_temp_c": 900.0}},
    }]
    report = prepare_rule_action_plan(
        context, config, outside, current_revision=7, base_revision=7)
    assert report["ok"] is False and report["code"] == "parameter_limit"
    external = authorize_external_commit(context, "patch", {
        "path": "process_states.0.value_id",
        "mode": "set",
        "base_revision": 7,
        "principal": "integration-operator",
    }, 7)
    assert external["ok"] is True, external
    denied = authorize_external_commit(context, "patch", {
        "path": "credentials.password",
        "mode": "set",
        "base_revision": 7,
        "principal": "integration-operator",
    }, 7)
    assert denied["ok"] is False and denied["code"] == "path_denied"
    task = {"task_uuid": "task-1", "actuator_id": 1, "control_value": 50.0,
            "config_revision": 7}
    assert evaluate_actuator_task(
        context, config, task, current_revision=7)["ok"] is True
    task["config_revision"] = 6
    stale_task = evaluate_actuator_task(
        context, config, task, current_revision=7)
    assert stale_task["ok"] is False and stale_task["code"] == "stale_revision"
    task["config_revision"] = 7
    task["control_value"] = 101.0
    assert evaluate_actuator_task(
        context, config, task, current_revision=7)["ok"] is False


if __name__ == "__main__":
    _run_test()
    print("PASS optional config and actuator safety policy")
