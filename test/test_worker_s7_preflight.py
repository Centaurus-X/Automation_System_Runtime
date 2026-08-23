# -*- coding: utf-8 -*-

"""Protocol-aware Worker preflight tests for the optional Snap7 backend."""

import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.system.worker_preflight import _validate_protocol_dependencies


def _missing_module(name):
    assert name == "snap7"
    return None


def _present_module(name):
    assert name == "snap7"
    return {"name": name}


def _device_config():
    return {
        "controllers": [
            {
                "controller_uuid": "modbus-controller",
                "bus_type": "modbus",
            },
            {
                "controller_uuid": "s7-controller",
                "bus_type": "plc_s7",
            },
        ],
    }


def run_test():
    disabled = _validate_protocol_dependencies(
        {"controllers_enabled": False},
        _device_config(),
        _missing_module,
    )
    assert disabled == ()

    modbus_only = _validate_protocol_dependencies(
        {
            "controllers_enabled": True,
            "controller_allowlist": ["modbus-controller"],
        },
        _device_config(),
        _missing_module,
    )
    assert modbus_only == ("modbus",)

    s7_ready = _validate_protocol_dependencies(
        {
            "controllers_enabled": True,
            "controller_allowlist": ["s7-controller"],
        },
        _device_config(),
        _present_module,
    )
    assert s7_ready == ("plc_s7",)

    try:
        _validate_protocol_dependencies(
            {
                "controllers_enabled": True,
                "controller_allowlist": ["s7-controller"],
            },
            _device_config(),
            _missing_module,
        )
    except ValueError as exc:
        assert "requirements-plc-s7.txt" in str(exc)
    else:
        raise AssertionError("missing python-snap7 was not rejected")

    print("PASS protocol-aware Worker preflight for optional Snap7 backend")


if __name__ == "__main__":
    run_test()
