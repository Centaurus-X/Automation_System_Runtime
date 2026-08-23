# -*- coding: utf-8 -*-

"""S7 registry, connection-thread and controller-batch integration tests."""

import logging
import os
import socket
import struct
import sys
import time


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.adapter._adapter_interface import make_request
from src.adapter._connection_registry import (
    create_connection_registry,
    register_adapter_factory,
    registry_get,
    registry_snapshot,
    registry_stop_all,
    resolve_conn_key_for_controller,
)
from src.adapter._connection_thread import (
    start_connections_from_config,
    sync_execute,
)
from src.adapter.plc._tia_plc_adapter import create_tia_plc_adapter
from src.core._controller_thread import (
    _extract_values_from_s7_batch,
    _group_sensors_for_s7_multi_reads,
)


def _load_snap7():
    try:
        import snap7
        from snap7.type import SrvArea
    except ImportError:
        if os.environ.get("REQUIRE_S7_EMULATOR") == "1":
            raise
        return None, None
    return snap7, SrvArea


def _reserve_port():
    endpoint = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        endpoint.bind(("127.0.0.1", 0))
        return int(endpoint.getsockname()[1])
    finally:
        endpoint.close()


def _wait_connected(registry, conn_key, timeout_s=2.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        context = registry_get(registry, conn_key)
        if context and context.get("status", {}).get("connected"):
            return context
        time.sleep(0.01)
    return None


def _test_connection_keys():
    modbus = {
        "controller_id": 1,
        "bus_type": "modbus",
        "host": "192.0.2.10",
        "port": 502,
    }
    assert resolve_conn_key_for_controller(modbus) == (
        "modbus_tcp:192.0.2.10:502"
    )
    s7_slot_one = {
        "controller_id": 2,
        "bus_type": "plc_s7",
        "host": "192.0.2.20",
        "port": 102,
        "comm": {"rack": 0, "slot": 1},
    }
    s7_slot_two = {
        "controller_id": 3,
        "bus_type": "plc_s7",
        "host": "192.0.2.20",
        "port": 102,
        "comm": {"rack": 0, "slot": 2},
    }
    first = resolve_conn_key_for_controller(s7_slot_one)
    second = resolve_conn_key_for_controller(s7_slot_two)
    assert first == "plc_s7:192.0.2.20:102:rack=0:slot=1"
    assert second == "plc_s7:192.0.2.20:102:rack=0:slot=2"
    assert first != second


def _test_controller_batch_mapping():
    sensors = [
        {
            "sensor_id": index + 1,
            "function_type": "read_s7",
            "address": "DB10.{}:INT".format(index * 2),
        }
        for index in range(45)
    ]
    groups = _group_sensors_for_s7_multi_reads(sensors, 20)
    assert [group["count"] for group in groups] == [20, 20, 5]
    values = _extract_values_from_s7_batch(
        groups[0],
        list(range(20)),
    )
    assert values == {
        index + 1: index
        for index in range(20)
    }


def _test_real_connection_thread():
    snap7, server_area = _load_snap7()
    if snap7 is None:
        print("SKIP python-snap7 connection-thread integration")
        return
    logging.getLogger("snap7").setLevel(logging.CRITICAL)
    port = _reserve_port()
    server = snap7.Server(log=False)
    db10 = bytearray(128)
    db10[0:4] = struct.pack(">f", 42.5)
    db10[8:10] = struct.pack(">h", -12)
    server.register_area(server_area.DB, 10, db10)
    server.start_to("127.0.0.1", port)

    registry = create_connection_registry(name="tia-integration")
    register_adapter_factory("plc_s7", create_tia_plc_adapter)
    controller = {
        "controller_id": 1,
        "controller_uuid": "tia-integration-controller",
        "controller_key": "ctrl_tia_integration",
        "bus_type": "plc_s7",
        "host": "127.0.0.1",
        "port": port,
        "unit": "0x1",
        "comm": {
            "rack": 0,
            "slot": 1,
            "s7_multi_var_limit": 20,
        },
    }
    config = {
        "controllers": [controller],
        "virtual_controllers": [],
    }
    conn_key = resolve_conn_key_for_controller(controller, config)
    try:
        queues = start_connections_from_config(registry, config)
        assert list(queues) == [conn_key]
        context = _wait_connected(registry, conn_key)
        assert context is not None
        assert context["config"]["rack"] == 0
        assert context["config"]["slot"] == 1
        result = sync_execute(
            queues[conn_key],
            make_request(
                "read_multi",
                "s7",
                addresses=("DB10.0:REAL", "DB10.8:INT"),
            ),
            timeout_s=2.0,
        )
        assert result["ok"], result
        assert abs(result["data"][0] - 42.5) < 0.0001
        assert result["data"][1] == -12
        snapshot = registry_snapshot(registry)
        assert snapshot[conn_key]["connected"] is True
        assert snapshot[conn_key]["reads_total"] == 2
    finally:
        registry_stop_all(registry, join_timeout_s=3.0)
        server.stop()


def run_test():
    _test_connection_keys()
    _test_controller_batch_mapping()
    _test_real_connection_thread()
    print("PASS TIA registry, connection thread and controller batch integration")


if __name__ == "__main__":
    run_test()
