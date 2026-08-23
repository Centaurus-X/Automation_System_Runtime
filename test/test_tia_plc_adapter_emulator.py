# -*- coding: utf-8 -*-

"""Real TCP regression against the pinned python-snap7 server emulator."""

import logging
import math
import os
import socket
import struct
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.adapter._adapter_interface import make_request
from src.adapter.plc._tia_plc_adapter import create_tia_plc_adapter


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


def _execute(adapter, context, operation, address, value=None):
    return adapter["execute"](
        context,
        make_request(
            operation,
            "s7",
            address=address,
            value=value,
        ),
    )


def _assert_close(actual, expected):
    assert math.isclose(actual, expected, rel_tol=1e-6, abs_tol=1e-6)


def run_test():
    snap7, server_area = _load_snap7()
    if snap7 is None:
        print("SKIP python-snap7 is not installed")
        return

    logging.getLogger("snap7").setLevel(logging.CRITICAL)
    port = _reserve_port()
    server = snap7.Server(log=False)
    db10 = bytearray(512)
    markers = bytearray(128)
    inputs = bytearray(128)
    outputs = bytearray(128)
    db10[4:8] = struct.pack(">f", 12.5)
    db10[12:14] = struct.pack(">h", -321)
    markers[1] = 0b00000100
    inputs[2:4] = struct.pack(">h", 1234)
    server.register_area(server_area.DB, 10, db10)
    server.register_area(server_area.MK, 0, markers)
    server.register_area(server_area.PE, 0, inputs)
    server.register_area(server_area.PA, 0, outputs)
    server.start_to("127.0.0.1", port)

    adapter = create_tia_plc_adapter()
    context = {
        "conn_key": "snap7-emulator",
        "host": "127.0.0.1",
        "port": port,
        "config": {
            "rack": 0,
            "slot": 1,
            "s7_multi_var_limit": 20,
            "s7_batch_fallback_sequential": False,
        },
        "status": {},
        "metrics": {},
    }
    try:
        assert adapter["connect"](context), context["status"]
        multi = adapter["execute"](
            context,
            make_request(
                "read_multi",
                "s7",
                addresses=(
                    "DB10.4:REAL",
                    "DB10.12:INT",
                    "M1.2:BOOL",
                    "I2:INT",
                ),
            ),
        )
        assert multi["ok"], multi
        _assert_close(multi["data"][0], 12.5)
        assert multi["data"][1:] == [-321, True, 1234]

        real_write = _execute(
            adapter,
            context,
            "write_single",
            "DB10.4:REAL",
            77.25,
        )
        bool_write = _execute(
            adapter,
            context,
            "write_single",
            "Q0.3:BOOL",
            True,
        )
        assert real_write["ok"], real_write
        assert bool_write["ok"], bool_write
        readback = adapter["execute"](
            context,
            make_request(
                "read_multi",
                "s7",
                addresses=("DB10.4:REAL", "Q0.3:BOOL"),
            ),
        )
        assert readback["ok"], readback
        _assert_close(readback["data"][0], 77.25)
        assert readback["data"][1] is True

        health = adapter["health_check"](context)
        assert health["ok"] and health["plc_status"] == "RUN", health
        assert context["metrics"]["reads_total"] == 6
        assert context["metrics"]["writes_total"] == 2
    finally:
        adapter["disconnect"](context)
        server.stop()

    print("PASS python-snap7 TCP emulator DB/M/I/Q batch, write and health")


if __name__ == "__main__":
    run_test()
