# -*- coding: utf-8 -*-

# src/adapter/fieldbus/_modbus_tcp_adapter.py

"""Modbus TCP Adapter – Synchron, funktional, ohne OOP.

Verantwortlichkeit:
    - Verbindung zu Modbus TCP Geräten herstellen/halten
    - Read/Write Operationen über pymodbus (synchroner Client)
    - Fehlerbehandlung und Ergebnis-Normalisierung

Nicht-Verantwortlichkeiten:
    - Kein Connection-Management (das macht der Connection Thread)
    - Kein Timing (das macht die Timing Engine)
    - Keine Geschäftslogik

Synchroner Code, kein asyncio, kein OOP, kein Decorator.
"""


import time
import logging

from functools import partial

try:
    from src.adapter._adapter_interface import (
        make_result_ok,
        make_result_error,
    )
except ImportError:
    from _adapter_interface import (
        make_result_ok,
        make_result_error,
    )


logger = logging.getLogger(__name__)


# =============================================================================
# pymodbus Versions-Kompatibilität
# =============================================================================

def _detect_unit_kwarg_name():
    """Erkennt den korrekten Keyword-Namen für die Unit/Slave-ID.

    pymodbus < 3.7:  'slave'
    pymodbus >= 3.7: 'device_id'
    """
    try:
        import inspect
        from pymodbus.client.mixin import ModbusClientMixin
        sig = inspect.signature(ModbusClientMixin.read_holding_registers)
        params = set(sig.parameters.keys())
        if "device_id" in params:
            return "device_id"
        if "slave" in params:
            return "slave"
    except Exception:
        pass
    return "device_id"


_UNIT_KWARG = _detect_unit_kwarg_name()
logger.info("pymodbus Unit-Keyword: %s", _UNIT_KWARG)


def _unit_kw(unit):
    """Liefert {device_id: unit} oder {slave: unit} je nach pymodbus-Version."""
    return {_UNIT_KWARG: int(unit)}


# =============================================================================
# Verbindungs-Lifecycle
# =============================================================================

def _connect(conn_ctx):
    """Verbindung zu Modbus TCP Gerät herstellen (synchron).

    Gibt True bei Erfolg zurück, False bei Fehler.
    Der Client wird in conn_ctx["client"] gespeichert.
    """
    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        logger.error("pymodbus nicht installiert!")
        return False

    host = conn_ctx.get("host", "")
    port = conn_ctx.get("port", 502)

    if not host:
        logger.error("Kein Host konfiguriert für %s", conn_ctx.get("conn_key"))
        return False

    try:
        # Bestehenden Client schließen falls vorhanden
        old_client = conn_ctx.get("client")
        if old_client is not None:
            try:
                old_client.close()
            except Exception:
                pass
            conn_ctx["client"] = None

        client_kwargs = {
            "host": host,
            "port": port,
            "timeout": 3,
            "retries": 1,
        }

        # retry_on_empty wurde in pymodbus >= 3.7 entfernt
        try:
            import inspect
            sig = inspect.signature(ModbusTcpClient.__init__)
            if "retry_on_empty" in sig.parameters:
                client_kwargs["retry_on_empty"] = True
        except Exception:
            pass

        client = ModbusTcpClient(**client_kwargs)

        ok = client.connect()
        if ok:
            conn_ctx["client"] = client
            conn_ctx["status"]["connected"] = True
            conn_ctx["status"]["last_connect_ts"] = time.time()
            conn_ctx["status"]["connection_failures"] = 0
            logger.info("Modbus TCP verbunden: %s:%d", host, port)
            return True

        logger.warning("Modbus TCP connect fehlgeschlagen: %s:%d", host, port)
        conn_ctx["status"]["connection_failures"] += 1
        return False

    except Exception as exc:
        logger.error("Modbus TCP connect Exception %s:%d: %s", host, port, exc)
        conn_ctx["status"]["connection_failures"] += 1
        return False


def _disconnect(conn_ctx):
    """Verbindung sauber trennen."""
    client = conn_ctx.get("client")
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
    conn_ctx["client"] = None
    conn_ctx["status"]["connected"] = False
    conn_ctx["status"]["last_disconnect_ts"] = time.time()


def _is_connected(conn_ctx):
    """Prüft ob Verbindung aktiv ist."""
    client = conn_ctx.get("client")
    if client is None:
        return False
    try:
        return bool(client.connected)
    except Exception:
        return False


def _health_check(conn_ctx):
    """Minimaler Health-Check: liest Register 0."""
    if not _is_connected(conn_ctx):
        return {"ok": False, "latency_ms": 0.0}

    client = conn_ctx.get("client")
    unit = conn_ctx.get("unit", 1)
    t0 = time.perf_counter()

    try:
        result = client.read_holding_registers(0, count=1, **_unit_kw(unit))
        dt = (time.perf_counter() - t0) * 1000.0

        if hasattr(result, "isError") and result.isError():
            return {"ok": False, "latency_ms": dt}
        return {"ok": True, "latency_ms": dt}
    except Exception:
        dt = (time.perf_counter() - t0) * 1000.0
        return {"ok": False, "latency_ms": dt}


# =============================================================================
# I/O Operationen (synchron)
# =============================================================================

def _execute_read_block(client, request, unit_kw):
    """Blockweises Lesen von Coils oder Holding-Registers."""
    data_type = request.get("data_type", "holding")
    start = int(request.get("start", 0))
    count = int(request.get("count", 1))
    t0 = time.perf_counter()

    try:
        if data_type in ("coil", "discrete"):
            result = client.read_coils(start, count=count, **unit_kw)
            dt = (time.perf_counter() - t0) * 1000.0

            if hasattr(result, "isError") and result.isError():
                return make_result_error(
                    "read_coils error: {}".format(result),
                    latency_ms=dt, request=request,
                )
            if hasattr(result, "bits") and result.bits:
                bits = list(result.bits)
                if len(bits) < count:
                    bits = bits + [False] * (count - len(bits))
                return make_result_ok(bits[:count], latency_ms=dt, request=request)
            return make_result_error("no_bits_in_response", latency_ms=dt, request=request)

        elif data_type in ("holding", "input", "register"):
            if data_type == "input":
                result = client.read_input_registers(start, count=count, **unit_kw)
            else:
                result = client.read_holding_registers(start, count=count, **unit_kw)
            dt = (time.perf_counter() - t0) * 1000.0

            if hasattr(result, "isError") and result.isError():
                return make_result_error(
                    "read_registers error: {}".format(result),
                    latency_ms=dt, request=request,
                )
            if hasattr(result, "registers") and result.registers:
                regs = list(result.registers)
                if len(regs) < count:
                    regs = regs + [None] * (count - len(regs))
                return make_result_ok(regs[:count], latency_ms=dt, request=request)
            return make_result_error("no_registers_in_response", latency_ms=dt, request=request)

        else:
            dt = (time.perf_counter() - t0) * 1000.0
            return make_result_error(
                "unknown_data_type: {}".format(data_type),
                latency_ms=dt, request=request,
            )

    except Exception as exc:
        dt = (time.perf_counter() - t0) * 1000.0
        return make_result_error(str(exc), latency_ms=dt, request=request)


def _execute_read_single(client, request, unit_kw):
    """Einzelnes Register/Coil lesen."""
    # Delegiert an read_block mit count=1
    single_req = dict(request)
    single_req["start"] = int(request.get("address", 0))
    single_req["count"] = 1
    result = _execute_read_block(client, single_req, unit_kw)

    # Einzelwert extrahieren
    if result.get("ok") and isinstance(result.get("data"), list) and len(result["data"]) > 0:
        result["data"] = result["data"][0]
    return result


def _execute_write_single(client, request, unit_kw):
    """Einzelnes Register/Coil schreiben."""
    data_type = request.get("data_type", "register")
    address = int(request.get("address", 0))
    value = request.get("value")
    t0 = time.perf_counter()

    try:
        if data_type == "coil":
            result = client.write_coil(address, bool(value), **unit_kw)
            dt = (time.perf_counter() - t0) * 1000.0

            if hasattr(result, "isError") and result.isError():
                return make_result_error(
                    "write_coil error @{}: {}".format(address, result),
                    latency_ms=dt, request=request,
                )
            return make_result_ok(True, latency_ms=dt, request=request)

        elif data_type in ("register", "holding"):
            try:
                v_int = int(value)
            except Exception:
                v_int = 0

            result = client.write_register(address, v_int, **unit_kw)
            dt = (time.perf_counter() - t0) * 1000.0

            if hasattr(result, "isError") and result.isError():
                return make_result_error(
                    "write_register error @{}: {}".format(address, result),
                    latency_ms=dt, request=request,
                )
            return make_result_ok(True, latency_ms=dt, request=request)

        else:
            dt = (time.perf_counter() - t0) * 1000.0
            return make_result_error(
                "unknown_write_data_type: {}".format(data_type),
                latency_ms=dt, request=request,
            )

    except Exception as exc:
        dt = (time.perf_counter() - t0) * 1000.0
        return make_result_error(str(exc), latency_ms=dt, request=request)


def _execute_write_block(client, request, unit_kw):
    """Blockweises Schreiben von Registern/Coils."""
    data_type = request.get("data_type", "register")
    start = int(request.get("start", request.get("address", 0)))
    values = request.get("value", [])
    t0 = time.perf_counter()

    if not isinstance(values, (list, tuple)):
        values = [values]

    try:
        if data_type == "coil":
            bool_values = [bool(v) for v in values]
            result = client.write_coils(start, bool_values, **unit_kw)
            dt = (time.perf_counter() - t0) * 1000.0

            if hasattr(result, "isError") and result.isError():
                return make_result_error(
                    "write_coils error @{}: {}".format(start, result),
                    latency_ms=dt, request=request,
                )
            return make_result_ok(True, latency_ms=dt, request=request)

        elif data_type in ("register", "holding"):
            int_values = []
            for v in values:
                try:
                    int_values.append(int(v))
                except Exception:
                    int_values.append(0)

            result = client.write_registers(start, int_values, **unit_kw)
            dt = (time.perf_counter() - t0) * 1000.0

            if hasattr(result, "isError") and result.isError():
                return make_result_error(
                    "write_registers error @{}: {}".format(start, result),
                    latency_ms=dt, request=request,
                )
            return make_result_ok(True, latency_ms=dt, request=request)

        else:
            dt = (time.perf_counter() - t0) * 1000.0
            return make_result_error(
                "unknown_write_block_data_type: {}".format(data_type),
                latency_ms=dt, request=request,
            )

    except Exception as exc:
        dt = (time.perf_counter() - t0) * 1000.0
        return make_result_error(str(exc), latency_ms=dt, request=request)


# =============================================================================
# Dispatcher: execute()
# =============================================================================

def _execute(conn_ctx, request):
    """Führt eine Modbus-Operation aus (synchron).

    Zentraler Dispatch basierend auf request["op"].
    """
    client = conn_ctx.get("client")
    if client is None:
        return make_result_error("not_connected", request=request)

    if not _is_connected(conn_ctx):
        return make_result_error("connection_lost", request=request)

    op = str(request.get("op", ""))
    unit = int(request.get("unit", conn_ctx.get("unit", 1)))
    ukw = _unit_kw(unit)

    try:
        if op == "read_block":
            result = _execute_read_block(client, request, ukw)
        elif op == "read_single":
            result = _execute_read_single(client, request, ukw)
        elif op == "write_single":
            result = _execute_write_single(client, request, ukw)
        elif op == "write_block":
            result = _execute_write_block(client, request, ukw)
        else:
            result = make_result_error("unknown_op: {}".format(op), request=request)

        # Metriken aktualisieren
        conn_ctx["status"]["last_io_ts"] = time.time()
        m = conn_ctx["metrics"]
        if op.startswith("read"):
            m["reads_total"] += 1
            lat = result.get("latency_ms", 0.0)
            m["latency_read_avg"] = (m["latency_read_avg"] * 9.0 + lat) / 10.0
        elif op.startswith("write"):
            m["writes_total"] += 1
            lat = result.get("latency_ms", 0.0)
            m["latency_write_avg"] = (m["latency_write_avg"] * 9.0 + lat) / 10.0

        if not result.get("ok", False):
            m["errors_total"] += 1

        return result

    except Exception as exc:
        conn_ctx["metrics"]["errors_total"] += 1
        return make_result_error(
            "execute_exception: {}".format(exc),
            request=request,
        )


# =============================================================================
# Factory: Adapter erstellen
# =============================================================================

def create_modbus_tcp_adapter():
    """Factory-Funktion: Erstellt einen Modbus TCP Adapter (dict).

    Der Adapter implementiert das generische Interface aus _adapter_interface.
    Rein funktional, kein OOP, kein State im Adapter selbst –
    aller State lebt im conn_ctx.
    """
    return {
        # Lifecycle
        "connect":       _connect,
        "disconnect":    _disconnect,
        "is_connected":  _is_connected,
        "health_check":  _health_check,

        # I/O
        "execute":       _execute,

        # Metadata
        "protocol_type": "modbus_tcp",
        "capabilities":  frozenset({
            "read_block",
            "read_single",
            "write_single",
            "write_block",
        }),
    }
