# -*- coding: utf-8 -*-

"""Functional unit and contract tests for the generic TIA S7 adapter."""

import math
import os
import sys

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.adapter._adapter_interface import make_request, validate_adapter
from src.adapter.plc import _tia_plc_adapter as tia


def _area_memory(state, area, db_number):
    if area == "DB":
        return state["db"].setdefault(int(db_number), bytearray(512))
    return state["areas"][area]


def _read_area(state, area, db_number, start, size):
    memory = _area_memory(state, area, db_number)
    begin = int(start)
    end = begin + int(size)
    return bytearray(memory[begin:end])


def _write_area(state, area, db_number, start, data):
    memory = _area_memory(state, area, db_number)
    begin = int(start)
    raw = bytes(data)
    memory[begin:begin + len(raw)] = raw
    return 0


def _create_fake_client(state):
    def connect(host, rack, slot, *args, **kwargs):
        state["connect_args"] = {
            "host": host,
            "rack": rack,
            "slot": slot,
            "port": kwargs.get("tcp_port") or (args[0] if args else None),
        }
        state["connected"] = True

    def disconnect():
        state["connected"] = False

    def get_connected():
        return state["connected"]

    def get_cpu_state():
        return state["cpu_state"]

    def db_read(db_number, start, size):
        return _read_area(state, "DB", db_number, start, size)

    def db_write(db_number, start, data):
        return _write_area(state, "DB", db_number, start, data)

    def mb_read(start, size):
        return _read_area(state, "M", 0, start, size)

    def mb_write(start, size, data):
        assert int(size) == len(data)
        return _write_area(state, "M", 0, start, data)

    def eb_read(start, size):
        return _read_area(state, "I", 0, start, size)

    def ab_read(start, size):
        return _read_area(state, "Q", 0, start, size)

    def ab_write(start, data):
        return _write_area(state, "Q", 0, start, data)

    def read_multi_vars(items):
        state["batch_sizes"].append(len(items))
        if state.get("batch_error"):
            raise RuntimeError("synthetic batch failure")
        payload = [
            _read_area(
                state,
                item["area"],
                item.get("db_number", 0),
                item["start"],
                item["size"],
            )
            for item in items
        ]
        return 0, payload

    def set_session_password(password):
        state["session_password"] = password

    def clear_session_password():
        state["session_password_cleared"] = True

    return {
        "connect": connect,
        "disconnect": disconnect,
        "get_connected": get_connected,
        "get_cpu_state": get_cpu_state,
        "db_read": db_read,
        "db_write": db_write,
        "mb_read": mb_read,
        "mb_write": mb_write,
        "eb_read": eb_read,
        "ab_read": ab_read,
        "ab_write": ab_write,
        "read_multi_vars": read_multi_vars,
        "set_session_password": set_session_password,
        "clear_session_password": clear_session_password,
    }


def _state():
    return {
        "connected": False,
        "cpu_state": "S7CpuStatusRun",
        "db": {10: bytearray(512)},
        "areas": {
            "M": bytearray(512),
            "I": bytearray(512),
            "Q": bytearray(512),
        },
        "batch_sizes": [],
    }


def _context(state, config=None):
    return {
        "conn_key": "fake-s7",
        "host": "127.0.0.1",
        "port": 1102,
        "config": dict(config or {"rack": 0, "slot": 1}),
        "status": {},
        "metrics": {},
        "_client_factory": partial(_create_fake_client, state),
        "_area_map": {
            "DB": "DB",
            "M": "M",
            "I": "I",
            "Q": "Q",
        },
    }


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


def _assert_number(actual, expected):
    if isinstance(expected, float):
        assert math.isclose(actual, expected, rel_tol=1e-6, abs_tol=1e-6)
    else:
        assert actual == expected


def _test_address_parser():
    cases = {
        "DB10.4:REAL": "DB10.4:REAL",
        "DB10.DBD4:REAL": "DB10.4:REAL",
        "DB10:4:REAL": "DB10.4:REAL",
        "M10.5:BOOL": "M10.5:BOOL",
        "MX10.5:BOOL": "M10.5:BOOL",
        "E2:INT": "I2:INT",
        "AW4:WORD": "Q4:WORD",
        "DB2.8:STRING[12]": "DB2.8:STRING[12]",
        "DB2.32:WSTRING[5]": "DB2.32:WSTRING[5]",
        "DB2.64:BYTE_ARRAY[3]": "DB2.64:BYTES[3]",
    }
    for source, expected in cases.items():
        assert tia.parse_s7_address(source)["canonical"] == expected

    invalid = (
        "DB10.0:BOOL",
        "DB10.0.1:INT",
        "DB10.DBW0:REAL",
        "X0:INT",
        "DB1.0:DT",
        "DB70000.0:INT",
        "DB1.0:STRING[255]",
    )
    for address in invalid:
        try:
            tia.parse_s7_address(address)
        except ValueError:
            continue
        raise AssertionError("invalid S7 address accepted: {}".format(address))


def _test_typed_io_and_health():
    state = _state()
    context = _context(state)
    adapter = tia.create_tia_plc_adapter()
    valid, missing = validate_adapter(adapter)
    assert valid, missing
    assert adapter["connect"](context), context["status"]

    values = (
        ("DB10.0.0:BOOL", True),
        ("DB10.1:BYTE", 250),
        ("DB10.2:SINT", -12),
        ("DB10.4:UINT", 65000),
        ("DB10.8:DINT", -1234567),
        ("DB10.12:REAL", 12.25),
        ("DB10.16:LREAL", 1234.5),
        ("DB10.24:STRING[5]", "abc"),
        ("DB10.32:WSTRING[5]", "äö"),
        ("DB10.48:BYTES[3]", [1, 2, 255]),
        ("M2.6:BOOL", True),
        ("Q4:INT", -44),
    )
    for address, value in values:
        result = _execute(adapter, context, "write_single", address, value)
        assert result["ok"], result
    for address, expected in values:
        result = _execute(adapter, context, "read_single", address)
        assert result["ok"], result
        _assert_number(result["data"], expected)

    blocked = _execute(adapter, context, "write_single", "I0:INT", 5)
    assert blocked["ok"] is False
    assert "read_only" in blocked["error"]

    block_write = adapter["execute"](
        context,
        make_request(
            "write_block",
            "s7",
            address="DB10.64:INT",
            value=[10, 20, 30],
        ),
    )
    assert block_write["ok"] and block_write["data"] == 3
    block_read = adapter["execute"](
        context,
        make_request(
            "read_block",
            "s7",
            start="DB10.64:INT",
            count=3,
        ),
    )
    assert block_read["ok"] and block_read["data"] == [10, 20, 30]

    health = adapter["health_check"](context)
    assert health["ok"] and health["plc_status"] == "RUN"
    state["cpu_state"] = "S7CpuStatusStop"
    health = adapter["health_check"](context)
    assert health["ok"] and health["plc_status"] == "STOP"
    adapter["disconnect"](context)
    assert context["status"]["connected"] is False


def _test_batch_limit_and_fallback():
    state = _state()
    context = _context(state, {
        "rack": 0,
        "slot": 1,
        "s7_multi_var_limit": 20,
        "s7_batch_fallback_sequential": True,
    })
    adapter = tia.create_tia_plc_adapter()
    assert adapter["connect"](context)
    addresses = []
    for index in range(45):
        address = "DB10.{}:INT".format(index * 2)
        addresses.append(address)
        result = _execute(adapter, context, "write_single", address, index)
        assert result["ok"], result
    state["batch_sizes"].clear()
    batch = adapter["execute"](
        context,
        make_request("read_multi", "s7", addresses=addresses),
    )
    assert batch["ok"] and batch["data"] == list(range(45))
    assert state["batch_sizes"] == [20, 20, 5]

    state["batch_error"] = True
    fallback = adapter["execute"](
        context,
        make_request("read_multi", "s7", addresses=addresses[:3]),
    )
    assert fallback["ok"] and fallback["data"] == [0, 1, 2]
    adapter["disconnect"](context)


def _test_security_fail_closed():
    adapter = tia.create_tia_plc_adapter()
    inline_state = _state()
    inline = _context(inline_state, {
        "rack": 0,
        "slot": 1,
        "security": {"password": "forbidden"},
    })
    assert adapter["connect"](inline) is False
    assert "inline_secret_forbidden" in inline["status"]["last_error"]

    optimized = _context(_state(), {
        "rack": 0,
        "slot": 1,
        "optimized_block_access": True,
    })
    assert adapter["connect"](optimized) is False
    assert "optimized_block_access" in optimized["status"]["last_error"]

    secure = _context(_state(), {
        "rack": 0,
        "slot": 1,
        "security": {
            "mode": "tls",
            "verify_peer": True,
        },
    })
    assert adapter["connect"](secure) is False
    assert "tls_peer_verification" in secure["status"]["last_error"]

    password_state = _state()
    password_context = _context(password_state, {
        "rack": 0,
        "slot": 1,
        "security": {
            "mode": "legacy_password",
            "password_env": "AUTOMATION_TEST_S7_PASSWORD",
        },
    })
    os.environ["AUTOMATION_TEST_S7_PASSWORD"] = "test-only-secret"
    try:
        assert adapter["connect"](password_context)
        assert password_state["session_password"] == "test-only-secret"
        adapter["disconnect"](password_context)
        assert password_state["session_password_cleared"] is True
    finally:
        os.environ.pop("AUTOMATION_TEST_S7_PASSWORD", None)


def run_test():
    _test_address_parser()
    _test_typed_io_and_health()
    _test_batch_limit_and_fallback()
    _test_security_fail_closed()
    print("PASS generic TIA S7 adapter contract, codecs, batch and security")


if __name__ == "__main__":
    run_test()
