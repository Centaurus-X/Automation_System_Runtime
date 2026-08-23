# -*- coding: utf-8 -*-

# test/test_worker_simulation_profile.py


"""
Verify the explicit Worker simulation profile, controller selection and the
existing virtual Modbus endpoint without opening a network connection.
"""


import copy
import json
import os
import sys
import threading
import time

from queue import Queue


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "device_state_data_v9_node_fn-01.json",
)
SIMULATION_CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"
FN02_CONTROLLER_UUID = "06ab4528-f8a3-5ec7-a91f-eea79d60761e"


from src.adapter._bus_timing import cycle_sleep, load_timing_profile_from_config
from src.adapter._connection_registry import resolve_conn_key_for_controller
from src.adapter._connection_thread import sync_execute
from src.core._controller_thread import _get_active_virtual_endpoint
from src.core.thread_management import _build_fsm_context
from src.library._config_safety_policy import (
    authorize_external_commit,
    create_safety_context,
    evaluate_actuator_task,
)
from src.library._generic_type_library import normalize_config_for_runtime
from src.system.bootstrap_runtime_interface import (
    _data_center_options,
    _tm_options,
    _value_persistence_options,
    start_runtime,
    stop_runtime,
)
from src.system.bootstrap_system_interface import load_app_config


def _fake_controller_target(**kwargs):
    shutdown_event = kwargs.get("shutdown_event")
    if shutdown_event is not None:
        shutdown_event.wait()


def _load_device_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return normalize_config_for_runtime(
            json.load(handle), representation="storage")


def _controller_by_uuid(config, controller_uuid):
    for controller in config.get("controllers") or []:
        if str(controller.get("controller_uuid") or "") == controller_uuid:
            return controller
    return None


def _test_environment_profile_selection():
    previous = os.environ.get("AUTOMATION_PROFILE")
    os.environ["AUTOMATION_PROFILE"] = "worker_simulation"
    try:
        config = load_app_config()
    finally:
        if previous is None:
            os.environ.pop("AUTOMATION_PROFILE", None)
        else:
            os.environ["AUTOMATION_PROFILE"] = previous
    assert config["_active_profile"] == "worker_simulation"
    options = config["thread_management"]
    assert options["controllers_enabled"] is True
    assert options["connections_enabled"] is True
    assert options["rule_engine_enabled"] is True
    assert options["rule_scope"] == "local_controllers"
    assert options["fsm_scope"] == "local_controllers"
    assert options["controller_allowlist"] == [SIMULATION_CONTROLLER_UUID]
    distributed = config["distributed_runtime"]
    assert distributed["local_automation_enabled"] is True
    assert distributed["config_sync_mode"] == "local_first"
    assert _value_persistence_options(config)["enabled"] is True
    persistence = _data_center_options(config)
    assert persistence["json_style"] == "table"
    assert persistence["sort_mode"] == "fingerprint_safe"
    assert persistence["preserve_config_identity_on_save"] is True
    limits = config["state_event_management"]["safety"][
        "actuator_tasks"]["limits"]
    exact_limits = [item for item in limits if item.get("actuator_id") is not None]
    adaptive_limits = [item for item in limits if item.get("actuator_id") is None]
    assert {int(item["actuator_id"]) for item in exact_limits} == {1, 2, 3, 4, 5}
    assert adaptive_limits == [{
        "controller_uuid": SIMULATION_CONTROLLER_UUID,
        "min": 0,
        "max": 65535,
    }]
    defaults = options["controller_timing_defaults"]
    assert defaults[SIMULATION_CONTROLLER_UUID]["poll_interval_s"] == 10.0
    assert defaults[SIMULATION_CONTROLLER_UUID][
        "simulator_actuator_discovery"] == "noop_write"
    assert defaults[SIMULATION_CONTROLLER_UUID]["batch_actuator_timeout"] == 0.5
    return config


def _test_standalone_profile_selection():
    config = load_app_config(active_profile="standalone")
    assert config["_active_profile"] == "standalone"
    assert config["mqtt_interface"]["enabled"] is False
    assert config["mqtt_interface"]["required"] is False
    assert config["distributed_runtime"]["enabled"] is False
    options = config["thread_management"]
    assert options["controllers_enabled"] is True
    assert options["connections_enabled"] is True
    assert options["rule_engine_enabled"] is True
    assert options["rule_scope"] == "local_controllers"
    assert options["controller_allowlist"] == [SIMULATION_CONTROLLER_UUID]
    limits = config["state_event_management"]["safety"][
        "actuator_tasks"]["limits"]
    assert {int(item["actuator_id"]) for item in limits} == {1, 2, 3, 4, 5}
    defaults = options["controller_timing_defaults"]
    assert defaults[SIMULATION_CONTROLLER_UUID]["poll_interval_s"] == 10.0
    assert defaults[SIMULATION_CONTROLLER_UUID][
        "simulator_actuator_discovery"] == "noop_write"


def _test_fn02_owner_profile_selection():
    config = load_app_config(active_profile="worker_fn02")
    assert config["_active_profile"] == "worker_fn02"
    assert config["app"]["node_id"] == "fn-02"
    data_path = config["runtime"]["data_center"]["json_path"]
    assert data_path.startswith("_config/config_data/")
    assert "/_legacy/" not in data_path
    assert data_path.endswith("device_state_data_v9_node_fn-02.json")
    assert config["runtime"]["data_center"]["allow_legacy_import"] is False
    with open(os.path.join(PROJECT_ROOT, data_path), "r", encoding="utf-8") as handle:
        node_config = json.load(handle)
    assert node_config["meta"][0]["scope"] == "node_local"
    assert node_config["meta"][0]["node_id"] == "fn-02"
    assert node_config["meta"][0]["version"] == 9
    assert all("rows" not in row for row in node_config["sensors"])
    assert all("rows" not in row for row in node_config["actuators"])
    options = config["thread_management"]
    assert options["controllers_enabled"] is True
    assert options["connections_enabled"] is True
    assert options["controller_allowlist"] == [FN02_CONTROLLER_UUID]
    tls = config["mqtt_interface"]["transport"]["tls"]
    assert tls["cert_file"].endswith("worker_fn-02.crt")
    assert tls["key_file"].endswith("worker_fn-02.key")
    assert config["cross_runtime"]["enabled"] is False
    assert config["cross_runtime"]["node_id"] == "fn-02"
    assert config["cross_runtime"]["config_path"] == ""
    limits = config["state_event_management"]["safety"][
        "actuator_tasks"]["limits"]
    assert {int(item["actuator_id"]) for item in limits} == {1, 2, 3}


def _test_simulator_polling(profile):
    config = _load_device_config()
    controller = _controller_by_uuid(config, SIMULATION_CONTROLLER_UUID)
    defaults = profile["thread_management"]["controller_timing_defaults"][
        SIMULATION_CONTROLLER_UUID
    ]
    timing = load_timing_profile_from_config(
        config,
        controller,
        defaults=defaults,
    )
    assert timing["cycle_time_s"] == 10.0
    assert 1.0 / timing["cycle_time_s"] == 0.1

    synchronized = copy.deepcopy(config)
    synchronized["polling"] = {"poll_interval_s": 2.0}
    overridden = load_timing_profile_from_config(
        synchronized,
        controller,
        defaults=defaults,
    )
    assert overridden["cycle_time_s"] == 2.0

    stop_event = threading.Event()
    stop_event.set()
    started = time.monotonic()
    cycle_sleep(timing, time.monotonic(), stop_event)
    assert time.monotonic() - started < 0.2


def _test_active_rule_rearm_and_pid_target(profile):
    relative = profile["runtime"]["data_center"]["json_path"]
    path = relative if os.path.isabs(relative) else os.path.join(PROJECT_ROOT, relative)
    with open(path, "r", encoding="utf-8") as handle:
        config = normalize_config_for_runtime(
            json.load(handle), representation="storage")
    state = next(
        item for item in config["process_states"]
        if int(item.get("state_id", -1)) == 1)
    rule = next(
        item for item in config["rules"]
        if int(item.get("rule_id", -1)) == 1)
    rule_2 = next(
        item for item in config["rules"]
        if int(item.get("rule_id", -1)) == 2)
    actuator = next(
        row for group in config["actuators"]
        if group.get("controller_uuid") == SIMULATION_CONTROLLER_UUID
        for row in group.get("rows") or ()
        if int(row.get("actuator_id", -1)) == 3)
    assert state["mode_id"] == 2
    assert state["control_strategy"]["type"] == "PID"
    assert state["io_refs"]["actuators"] == [{
        "actuator_id": 3, "controller_uuid": SIMULATION_CONTROLLER_UUID}]
    assert (actuator["address"], actuator["function_type"]) == (
        524, "write_register")
    assert rule["do"] == [{"state_ids": [1], "value_id": 1}]
    assert rule["else_do"] == [{"state_ids": [1], "value_id": 2}]
    for checked_rule, state_id in ((rule, 1), (rule_2, 11)):
        assert all(
            not (
                condition.get("src") == "state"
                and int(condition.get("state_id", -1)) == state_id
            )
            for condition in checked_rule["when"]
        )


def _test_all_simulator_actuators_are_safely_authorized(profile):
    config = _load_device_config()
    options = profile["state_event_management"]["safety"]
    context = create_safety_context(options)
    valid_values = {1: True, 2: False, 3: 0, 4: True, 5: 65535}
    for actuator_id, value in sorted(valid_values.items()):
        result = evaluate_actuator_task(
            context,
            config,
            {
                "task_uuid": "profile-actuator-{}".format(actuator_id),
                "controller_uuid": SIMULATION_CONTROLLER_UUID,
                "actuator_id": actuator_id,
                "control_value": value,
                "config_revision": 7,
            },
            current_revision=7,
        )
        assert result["ok"] is True, result

    unsafe_coil = evaluate_actuator_task(
        context,
        config,
        {
            "task_uuid": "profile-actuator-unsafe-coil",
            "controller_uuid": SIMULATION_CONTROLLER_UUID,
            "actuator_id": 2,
            "control_value": "unsafe",
            "config_revision": 7,
        },
        current_revision=7,
    )
    assert unsafe_coil["ok"] is False
    assert unsafe_coil["code"] == "task_limit"

    unsafe_register = evaluate_actuator_task(
        context,
        config,
        {
            "task_uuid": "profile-actuator-unsafe-register",
            "controller_uuid": SIMULATION_CONTROLLER_UUID,
            "actuator_id": 5,
            "control_value": 65536,
            "config_revision": 7,
        },
        current_revision=7,
    )
    assert unsafe_register["ok"] is False
    assert unsafe_register["code"] == "task_limit"

    dynamic_register = {
        "task_uuid": "profile-actuator-dynamic-v9",
        "controller_uuid": SIMULATION_CONTROLLER_UUID,
        "actuator_id": 60000,
        "control_value": 1,
        "config_revision": 7,
    }
    dynamic_allowed = evaluate_actuator_task(
        context, config, dynamic_register, current_revision=7)
    assert dynamic_allowed["ok"] is True, dynamic_allowed

    dynamic_unsafe = evaluate_actuator_task(
        context,
        config,
        dict(dynamic_register, control_value=65536),
        current_revision=7,
    )
    assert dynamic_unsafe["ok"] is False
    assert dynamic_unsafe["code"] == "task_limit"

    dynamic_stale = evaluate_actuator_task(
        context,
        config,
        dict(dynamic_register, config_revision=6),
        current_revision=7,
    )
    assert dynamic_stale["ok"] is False
    assert dynamic_stale["code"] == "stale_revision"

    foreign_dynamic = evaluate_actuator_task(
        context,
        config,
        dict(dynamic_register, controller_uuid=FN02_CONTROLLER_UUID),
        current_revision=7,
    )
    assert foreign_dynamic["ok"] is False
    assert foreign_dynamic["code"] == "task_limit"


def _test_external_process_proposal_policy(profile):
    options = profile["state_event_management"]["safety"]
    context = create_safety_context(options)
    operation = {
        "op": "replace_field",
        "section": "processes",
        "entity_key": "process:1",
        "json_pointer": "/process_location",
        "expected_value_hash": "a" * 64,
        "value": "Control Plane E2E Reactor",
    }
    request = {
        "operation_format": "config_scope_operations_v1",
        "operations": [operation],
        "base_revision": 7,
        "principal": "master_relay",
    }
    allowed = authorize_external_commit(
        context, "scope_operations", request, 7)
    assert allowed["ok"] is True, allowed
    denied_request = copy.deepcopy(request)
    denied_request["operations"][0]["section"] = "credentials"
    denied = authorize_external_commit(
        context, "scope_operations", denied_request, 7)
    assert denied["ok"] is False
    assert denied["code"] == "path_denied"

def _test_virtual_modbus_endpoint(profile):
    config = _load_device_config()
    controller = _controller_by_uuid(config, SIMULATION_CONTROLLER_UUID)
    assert controller is not None
    connection_key = resolve_conn_key_for_controller(controller, config)
    assert connection_key == "modbus_tcp:192.168.0.3:5020"
    endpoint = _get_active_virtual_endpoint({
        "controller_id": controller["controller_id"],
        "controller_data": controller,
        "config_data": config,
        "config_lock": threading.RLock(),
    })
    assert endpoint == {
        "host": "192.168.0.3",
        "port": 5020,
        "virt_controller_id": 1,
    }
    fsm = _build_fsm_context(config, _tm_options(profile))
    assert fsm["ok"] is True
    assert fsm["scope"]["mode"] == "local_controllers"
    assert fsm["scope"]["selected"] == 1
    assert len(fsm["machines"]) == 1


def _test_connection_wait_is_cancellable():
    cancel_event = threading.Event()
    cancel_event.set()
    started = time.monotonic()
    result = sync_execute(
        Queue(),
        {"op": "read_block"},
        timeout_s=5.0,
        cancel_event=cancel_event,
    )
    elapsed = time.monotonic() - started
    assert result["ok"] is False
    assert result["error"] == "cancelled"
    assert elapsed < 0.5


def _test_runtime_controller_selection(profile):
    app_config = copy.deepcopy(profile)
    app_config["thread_management"]["connections_enabled"] = False
    app_config["mqtt_interface"]["enabled"] = False
    app_config["mqtt_interface"]["required"] = False
    app_config["distributed_runtime"]["enabled"] = False
    main_context = {
        "shutdown_event": threading.Event(),
        "app_config": app_config,
    }
    try:
        start_runtime(main_context, controller_target=_fake_controller_target)
        runtime = main_context["runtime"]
        component = runtime["components"]["thread_management"]
        workers = component["context"]["controller_workers"]
        assert tuple(workers) == (SIMULATION_CONTROLLER_UUID,)
        assert workers[SIMULATION_CONTROLLER_UUID]["thread"].is_alive()
        defaults = workers[SIMULATION_CONTROLLER_UUID]["runtime_resources"][
            "controller_timing_defaults"
        ]
        assert defaults["poll_interval_s"] == 10.0
    finally:
        if isinstance(main_context.get("runtime"), dict):
            stop_runtime(main_context)
    assert main_context["runtime"]["state"] == "stopped"
    assert not any(thread.is_alive() for thread in main_context["started_threads"])


def run_test():
    profile = _test_environment_profile_selection()
    _test_standalone_profile_selection()
    _test_fn02_owner_profile_selection()
    _test_simulator_polling(profile)
    _test_active_rule_rearm_and_pid_target(profile)
    _test_all_simulator_actuators_are_safely_authorized(profile)
    _test_external_process_proposal_policy(profile)
    _test_virtual_modbus_endpoint(profile)
    _test_connection_wait_is_cancellable()
    _test_runtime_controller_selection(profile)
    print("PASS Worker simulation profile, endpoint and controller allowlist")


if __name__ == "__main__":
    run_test()




