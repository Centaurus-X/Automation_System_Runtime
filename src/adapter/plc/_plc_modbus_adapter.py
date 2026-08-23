# -*- coding: utf-8 -*-

# src/adapter/plc/_plc_modbus_adapter.py

"""SPS-Anbindung über Modbus TCP – Synchron, funktional.

Semantisch getrennte Schnittstelle für SPS-Kommunikation.
Technisch delegiert an den Modbus TCP Adapter, ergänzt aber
SPS-spezifische Funktionalität:

    - Automatisches Register-Mapping (Coils, Holding, Input)
    - SPS-Status-Abfrage (Run/Stop/Error)
    - Programm-spezifische Adressierung
    - Health-Monitoring mit Watchdog-Counter

Eigener Thread im Connection Layer (wie andere Adapter).

Kein OOP, kein Decorator, kein Lambda, kein asyncio.
"""

import time
import logging

from functools import partial

try:
    from src.adapter._adapter_interface import make_result_ok, make_result_error
    from src.adapter.fieldbus._modbus_tcp_adapter import (
        _connect as _modbus_connect,
        _disconnect as _modbus_disconnect,
        _is_connected as _modbus_is_connected,
        _execute as _modbus_execute,
    )
except ImportError:
    from _adapter_interface import make_result_ok, make_result_error
    from _modbus_tcp_adapter import (
        _connect as _modbus_connect,
        _disconnect as _modbus_disconnect,
        _is_connected as _modbus_is_connected,
        _execute as _modbus_execute,
    )


logger = logging.getLogger(__name__)


# =============================================================================
# SPS-spezifische Operationen
# =============================================================================

def _health_check(conn_ctx):
    """SPS Health-Check: Liest Watchdog-Register (konfigurierbar)."""
    if not _modbus_is_connected(conn_ctx):
        return {"ok": False, "latency_ms": 0.0, "plc_status": "disconnected"}

    config = conn_ctx.get("config", {})
    watchdog_reg = int(config.get("watchdog_register", 0))
    status_reg = int(config.get("status_register", -1))

    t0 = time.perf_counter()

    # Watchdog-Register lesen
    result = _modbus_execute(conn_ctx, {
        "op": "read_single",
        "data_type": "holding",
        "address": watchdog_reg,
        "unit": conn_ctx.get("unit", 1),
    })
    dt = (time.perf_counter() - t0) * 1000.0

    plc_status = "unknown"
    if status_reg >= 0:
        try:
            status_result = _modbus_execute(conn_ctx, {
                "op": "read_single",
                "data_type": "holding",
                "address": status_reg,
                "unit": conn_ctx.get("unit", 1),
            })
            if status_result.get("ok"):
                raw = status_result.get("data", 0)
                plc_status = _decode_plc_status(raw)
        except Exception:
            pass

    return {
        "ok": result.get("ok", False),
        "latency_ms": dt,
        "plc_status": plc_status,
    }


def _decode_plc_status(raw_value):
    """Dekodiert SPS-Status aus Register-Wert."""
    try:
        v = int(raw_value)
    except Exception:
        return "unknown"

    status_map = {
        0: "stopped",
        1: "running",
        2: "error",
        3: "maintenance",
        4: "initializing",
    }
    return status_map.get(v, "unknown_{}".format(v))


def _execute(conn_ctx, request):
    """SPS-Execute mit optionalem Register-Offset.

    Unterstützt zusätzlich:
        op="plc_status"     → liest SPS-Status
        op="plc_watchdog"   → liest/schreibt Watchdog
    """
    op = str(request.get("op", ""))

    if op == "plc_status":
        return _read_plc_status(conn_ctx, request)

    if op == "plc_watchdog":
        return _handle_watchdog(conn_ctx, request)

    # Register-Offset aus Config anwenden
    config = conn_ctx.get("config", {})
    offset = int(config.get("register_offset", 0))
    if offset != 0:
        request = dict(request)
        if request.get("start") is not None:
            request["start"] = int(request["start"]) + offset
        if request.get("address") is not None:
            request["address"] = int(request["address"]) + offset

    return _modbus_execute(conn_ctx, request)


def _read_plc_status(conn_ctx, request):
    config = conn_ctx.get("config", {})
    status_reg = int(config.get("status_register", 0))
    t0 = time.perf_counter()

    result = _modbus_execute(conn_ctx, {
        "op": "read_single",
        "data_type": "holding",
        "address": status_reg,
        "unit": conn_ctx.get("unit", 1),
    })
    dt = (time.perf_counter() - t0) * 1000.0

    if result.get("ok"):
        status = _decode_plc_status(result.get("data"))
        return make_result_ok(
            {"status": status, "raw": result.get("data")},
            latency_ms=dt, request=request,
        )
    return make_result_error("plc_status_read_failed", latency_ms=dt, request=request)


def _handle_watchdog(conn_ctx, request):
    """Watchdog-Counter inkrementieren (Lebenszeichen an SPS)."""
    config = conn_ctx.get("config", {})
    watchdog_reg = int(config.get("watchdog_register", 0))

    counter = conn_ctx.get("_watchdog_counter", 0) + 1
    if counter > 65535:
        counter = 1
    conn_ctx["_watchdog_counter"] = counter

    return _modbus_execute(conn_ctx, {
        "op": "write_single",
        "data_type": "register",
        "address": watchdog_reg,
        "value": counter,
        "unit": conn_ctx.get("unit", 1),
    })


# =============================================================================
# Factory
# =============================================================================

def create_plc_modbus_adapter():
    return {
        "connect":       _modbus_connect,
        "disconnect":    _modbus_disconnect,
        "is_connected":  _modbus_is_connected,
        "health_check":  _health_check,
        "execute":       _execute,
        "protocol_type": "plc_modbus",
        "capabilities":  frozenset({
            "read_block", "read_single", "write_single", "write_block",
        }),
    }
