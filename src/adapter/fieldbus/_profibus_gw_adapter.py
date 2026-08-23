# -*- coding: utf-8 -*-

# src/adapter/fieldbus/_profibus_gw_adapter.py

"""PROFIBUS DP Gateway Adapter – Synchron, funktional.

PROFIBUS DP ist ein serieller Feldbus (RS-485) und kann NICHT direkt
über Standard-Ethernet angesprochen werden. Der Zugriff erfolgt über
ein PROFIBUS-Gateway:

    Ethernet → Gateway → PROFIBUS DP Bus

Unterstützte Gateway-Typen:
    1. HMS Anybus Communicator   → exponiert Modbus TCP
    2. Siemens IE/PB Link PN IO → exponiert PROFINET
    3. Hilscher netTAP           → exponiert Modbus TCP oder EtherNet/IP

Dieser Adapter wrappet die Gateway-Kommunikation.
Standard: Modbus TCP Durchleitung (da am weitesten verbreitet).

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
        _health_check as _modbus_health_check,
    )
except ImportError:
    from _adapter_interface import make_result_ok, make_result_error
    from _modbus_tcp_adapter import (
        _connect as _modbus_connect,
        _disconnect as _modbus_disconnect,
        _is_connected as _modbus_is_connected,
        _execute as _modbus_execute,
        _health_check as _modbus_health_check,
    )


logger = logging.getLogger(__name__)


# =============================================================================
# Gateway-Mapping: PROFIBUS Slave → Modbus Unit
# =============================================================================

def _map_profibus_address(conn_ctx, request):
    """Mappt PROFIBUS-Adressen auf Gateway-Modbus-Register.

    Gateway-Konfiguration (in conn_ctx["config"]):
        gateway_type:     "anybus" | "hilscher" | "siemens"
        slave_map:        {profibus_addr: modbus_unit, ...}
        register_offset:  Globaler Offset für Gateway-Register
    """
    config = conn_ctx.get("config", {})
    gw_type = str(config.get("gateway_type", "anybus"))
    slave_map = config.get("slave_map", {})
    register_offset = int(config.get("register_offset", 0))

    # PROFIBUS-Adresse → Modbus Unit ID
    pb_addr = request.get("meta", {}).get("profibus_address")
    if pb_addr is not None and slave_map:
        mapped_unit = slave_map.get(int(pb_addr))
        if mapped_unit is not None:
            request = dict(request)
            request["unit"] = int(mapped_unit)

    # Register-Offset anwenden
    if register_offset != 0:
        request = dict(request)
        if request.get("start") is not None:
            request["start"] = int(request["start"]) + register_offset
        if request.get("address") is not None:
            request["address"] = int(request["address"]) + register_offset

    return request


# =============================================================================
# Lifecycle (delegiert an Modbus TCP)
# =============================================================================

def _connect(conn_ctx):
    """Verbindung zum PROFIBUS-Gateway (Modbus TCP Seite)."""
    logger.info("PROFIBUS-GW: Verbinde über Modbus TCP Gateway")
    return _modbus_connect(conn_ctx)


def _disconnect(conn_ctx):
    return _modbus_disconnect(conn_ctx)


def _is_connected(conn_ctx):
    return _modbus_is_connected(conn_ctx)


def _health_check(conn_ctx):
    return _modbus_health_check(conn_ctx)


def _execute(conn_ctx, request):
    """Führt I/O über Gateway aus.

    1. Mappt PROFIBUS-Adressen auf Gateway-Register
    2. Delegiert an Modbus TCP Execute
    """
    mapped_request = _map_profibus_address(conn_ctx, request)
    result = _modbus_execute(conn_ctx, mapped_request)

    # Metriken unter profibus_gw Namespace
    conn_ctx["status"]["last_io_ts"] = time.time()
    return result


# =============================================================================
# Factory
# =============================================================================

def create_profibus_gw_adapter():
    return {
        "connect":       _connect,
        "disconnect":    _disconnect,
        "is_connected":  _is_connected,
        "health_check":  _health_check,
        "execute":       _execute,
        "protocol_type": "profibus_gw",
        "capabilities":  frozenset({
            "read_block", "read_single", "write_single", "write_block",
        }),
    }
