# -*- coding: utf-8 -*-

# test/test_runtime_value_store_integration.py


"""
Real runtime-bootstrap smoke test plus controller write-before-event check for
the central runtime sensor-value store.
"""


import os
import sys
import threading
import logging

from functools import partial


PROJECT_ROOT = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core import _controller_thread as controller_thread
from src.library._runtime_value_store import (
    close_runtime_value_store,
    create_runtime_value_store,
    create_sensor_value_capabilities,
    get_sensor_value_snapshot,
)
from src.system.bootstrap_runtime_interface import (
    get_runtime_snapshot,
    get_sensor_values_snapshot,
    start_runtime,
    stop_runtime,
)
from src.system.bootstrap_system_interface import start_system


CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"


def _check(condition, message):
    if not condition:
        raise AssertionError(message)


def _test_runtime_start_rollback():
    main_ctx = start_system(
        {},
        active_profile="unit_test",
        enable_ascii=False,
        enable_cpu_probe=False,
    )

    failed = False
    runtime_logger = logging.getLogger(
        "src.system.bootstrap_runtime_interface"
    )
    logger_disabled = runtime_logger.disabled
    runtime_logger.disabled = True

    try:
        start_runtime(
            main_ctx,
            router_target=False,
        )

    except RuntimeError:
        failed = True

    finally:
        runtime_logger.disabled = logger_disabled

    snapshot = get_runtime_snapshot(
        main_ctx["runtime"]
    )
    _check(failed, "invalid router target did not fail startup")
    _check(snapshot["state"] == "failed", "rollback state mismatch")
    _check(
        snapshot["sensor_value_store"]["closed"],
        "startup rollback did not close the value store",
    )
    _check(not snapshot["queue_names"], "startup rollback leaked queues")


def _test_real_runtime_bootstrap():
    main_ctx = start_system(
        {},
        active_profile="unit_test",
        enable_ascii=False,
        enable_cpu_probe=False,
    )

    try:
        start_runtime(main_ctx)
        runtime_ctx = main_ctx["runtime"]
        _check(
            runtime_ctx["options"]["value_change_queue_maxsize"] == 4096,
            "YAML value-store configuration was not applied",
        )
        tm_context = runtime_ctx[
            "components"
        ]["thread_management"]["context"]
        capabilities = tm_context[
            "resources"
        ]["sensor_value_store"]

        _check(
            "runtime_value_store" not in tm_context["resources"],
            "thread management received the raw value store",
        )

        token = capabilities["create_writer_token"](
            "runtime_integration_test"
        )
        claim = capabilities["claim_writer"](
            CONTROLLER_UUID,
            token,
        )
        _check(claim.get("ok"), claim.get("message"))

        write = capabilities["write_sensor_value"](
            controller_uuid=CONTROLLER_UUID,
            sensor_id=1,
            value=42.5,
            quality="good",
            writer_token=token,
        )
        _check(write.get("ok"), write.get("message"))

        snapshot = get_sensor_values_snapshot(
            runtime_ctx,
            CONTROLLER_UUID,
            1,
        )
        _check(snapshot["value"] == 42.5, "runtime snapshot mismatch")

        release = capabilities["release_writer"](
            CONTROLLER_UUID,
            token,
        )
        _check(release.get("ok"), release.get("message"))

    finally:
        stop_runtime(main_ctx)

    runtime_snapshot = get_runtime_snapshot(
        main_ctx["runtime"]
    )
    _check(runtime_snapshot["state"] == "stopped", "runtime did not stop")
    _check(
        runtime_snapshot["sensor_value_store"]["closed"],
        "runtime value store did not close",
    )
    _check(
        get_sensor_values_snapshot(
            main_ctx["runtime"],
            CONTROLLER_UUID,
            1,
        )["value"] == 42.5,
        "post-shutdown snapshot is unavailable",
    )


def _capture_sensor_event(observations, store, ctx, event_type, payload, **kwargs):
    snapshot = get_sensor_value_snapshot(
        store,
        payload.get("controller_uuid"),
        payload.get("sensor_id"),
    )
    observations.append({
        "event_type": event_type,
        "payload": payload,
        "snapshot_at_emit": snapshot,
        "target": kwargs.get("target"),
    })


def _make_controller_context(capabilities):
    return {
        "controller_id": 2,
        "controller_data": {
            "controller_id": 2,
            "controller_uuid": CONTROLLER_UUID,
        },
        "runtime_resources": {
            "sensor_value_store": capabilities,
        },
        "statistics": {
            "sensor_store_writes": 0,
            "sensor_store_errors": 0,
        },
        "sensor_values": {},
        "sensor_lock": threading.Lock(),
        "local_sensor_values": {},
        "local_sensor_lock": threading.Lock(),
        "sensor_store_enabled": False,
        "sensor_store_writer_claimed": False,
        "sensor_store_writer_token": None,
    }


def _test_controller_write_before_event():
    store = create_runtime_value_store(
        name="controller-integration-test",
        change_queue_maxsize=32,
    )
    capabilities = create_sensor_value_capabilities(
        store
    )
    ctx = _make_controller_context(
        capabilities
    )
    claim = controller_thread._claim_sensor_store_writer(
        ctx
    )
    _check(claim.get("ok"), claim.get("message"))

    observations = []
    original_emit = controller_thread._emit_event
    controller_thread._emit_event = partial_capture = (
        _bind_capture(observations, store)
    )

    try:
        emitted = controller_thread._process_sensor_value(
            ctx,
            {
                "sensor_id": 1,
                "address": 7,
                "function_type": "read_holding_registers",
                "threshold": 50.0,
                "critical": False,
                "force_automation": False,
            },
            {},
            value=41.25,
            timestamp="2026-07-10T12:00:00+00:00",
        )

    finally:
        controller_thread._emit_event = original_emit

    _check(callable(partial_capture), "event capture was not callable")
    _check(emitted == 1, "changed sensor value emitted no event")
    _check(len(observations) == 1, "unexpected sensor event count")

    observation = observations[0]
    snapshot = observation["snapshot_at_emit"]
    payload = observation["payload"]

    _check(snapshot is not None, "value was absent when event was emitted")
    _check(snapshot["value"] == 41.25, "stored value mismatch at emit")
    _check(
        payload["sensor_store_sequence"] == snapshot["sequence"],
        "event and central snapshot sequence differ",
    )
    _check(
        controller_thread._release_sensor_store_writer(ctx),
        "controller writer release failed",
    )
    _check(close_runtime_value_store(store), "controller test store close failed")


def _bind_capture(observations, store):
    return partial(
        _capture_sensor_event,
        observations,
        store,
    )


def run_test():
    _test_runtime_start_rollback()
    _test_real_runtime_bootstrap()
    _test_controller_write_before_event()
    print("PASS runtime bootstrap and controller sensor-store integration")


if __name__ == "__main__":
    run_test()
