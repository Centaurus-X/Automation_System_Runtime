# -*- coding: utf-8 -*-

"""Runtime controller cache invalidation for PID configuration changes."""

import copy
import os
import sys
import threading


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._automation_engine import (
    controller_update,
    ensure_controller_for_state,
    scaled_to_raw,
)


def _context():
    return {
        "controllers": {},
        "controller_kinds": {},
        "_cache_pid_configs": {
            1: {
                "pid_config_id": 1,
                "kp": 0.01,
                "ki": 0.001,
                "kd": 0.0,
            },
        },
        "_cache_actuators": {},
        "_cache_scaling_profiles": {
            2: {
                "raw_min": 0,
                "raw_max": 65535,
                "eng_min": 0.0,
                "eng_max": 10.0,
            },
        },
        "thread_index": "runtime-reconfiguration-test",
        "shared_automation_states": {},
        "shared_automation_states_lock": threading.RLock(),
    }


def _state():
    return {
        "state_id": 1,
        "control_strategy": {
            "pid_config_id": 1,
            "type": "PID",
            "sample_time": 0.1,
            "dt_max_s": 0.5,
            "out_min": 0.0,
            "out_max": 10.0,
            "error_scale": 1.0,
            "anti_windup": True,
        },
        "parameters": {"target_temp_c": 524.0},
    }


def _assert_identical_pid_actuator_paths_are_deterministic():
    context = _context()
    state_524 = _state()
    state_524["parameters"]["target_temp_c"] = 554.0
    state_546 = copy.deepcopy(state_524)
    state_546["state_id"] = 21

    controller_524 = ensure_controller_for_state(context, state_524)
    controller_546 = ensure_controller_for_state(context, state_546)
    assert controller_524 is not controller_546
    assert controller_524["_config_signature"] == (
        controller_546["_config_signature"])

    actuator_524 = {"actuator_id": 3, "address": 524, "profile_id": 2}
    actuator_546 = {"actuator_id": 5, "address": 546, "profile_id": 2}
    measurements = (500.0, 510.0, 530.0, 545.0, 550.0, 540.0)
    outputs_524 = []
    outputs_546 = []
    registers_524 = []
    registers_546 = []

    for index, measurement in enumerate(measurements, 1):
        now_ts = index * 0.1
        output_524 = controller_update(
            controller_524, 554.0, measurement, now_ts)
        output_546 = controller_update(
            controller_546, 554.0, measurement, now_ts)
        outputs_524.append(output_524)
        outputs_546.append(output_546)
        registers_524.append(int(round(
            scaled_to_raw(context, actuator_524, output_524))))
        registers_546.append(int(round(
            scaled_to_raw(context, actuator_546, output_546))))

    assert outputs_524 == outputs_546
    assert registers_524 == registers_546
    assert all(0 <= value <= 65535 for value in registers_524)


def _run_test():
    context = _context()
    state = _state()

    first = ensure_controller_for_state(context, state)
    assert first is not None
    assert first["kp"] == 0.01
    first["integral"] = 7.0
    first_signature = first.get("_config_signature")
    assert first_signature

    # Setpoints are runtime inputs, not controller construction parameters.
    state["parameters"]["target_temp_c"] = 546.0
    same = ensure_controller_for_state(context, state)
    assert same is first
    assert same["integral"] == 7.0
    assert same["_config_signature"] == first_signature

    # Preserve an old round-robin record to prove that changed PID gains do
    # not restore stale integral memory into the rebuilt controller.
    context["shared_automation_states"] = {
        "states": {
            1: {
                "v": 1,
                "controller": copy.deepcopy(same),
                "controller_kind": "PID",
            },
        },
    }
    context["_cache_pid_configs"][1]["kp"] = 0.25
    rebuilt = ensure_controller_for_state(context, state)
    assert rebuilt is not first
    assert rebuilt["kp"] == 0.25
    assert rebuilt["integral"] == 0.0
    assert rebuilt["_config_signature"] != first_signature

    # A same-signature controller migration still restores dynamic PID memory.
    shared_controller = copy.deepcopy(rebuilt)
    shared_controller["integral"] = 4.0
    context["shared_automation_states"]["states"][1] = {
        "v": 2,
        "controller": shared_controller,
        "controller_kind": "PID",
    }
    context["controllers"].pop(1)
    migrated = ensure_controller_for_state(context, state)
    assert migrated["kp"] == 0.25
    assert migrated["integral"] == 4.0
    assert migrated["_config_signature"] == rebuilt["_config_signature"]

    _assert_identical_pid_actuator_paths_are_deterministic()

    print(
        "PASS runtime PID cache invalidation, signature-safe memory "
        "migration, and address-independent actuator output")


if __name__ == "__main__":
    _run_test()
