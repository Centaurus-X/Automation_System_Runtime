# -*- coding: utf-8 -*-

# src/adapter/fieldbus/_profinet_rt_adapter.py

"""PROFINET RT Adapter – Synchron, funktional, ohne OOP.

Kommunikation über python-snap7 (S7comm über TCP/IP).
Snap7 spricht S7-Protokoll, das von vielen PROFINET-fähigen
Siemens-SPSen (S7-1200, S7-1500, LOGO!) nativ unterstützt wird.

Für reine PROFINET RT I/O-Module ohne S7-Stack wird ein
PROFINET-Gateway (z.B. HMS Anybus) empfohlen, das Modbus TCP exponiert
→ dann _modbus_tcp_adapter.py verwenden.

Abhängigkeit: pip install python-snap7
Systemabhängigkeit: libsnap7 (snap7.dll / libsnap7.so)

Kein OOP, kein Decorator, kein Lambda, kein asyncio.
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
# Verbindungs-Lifecycle
# =============================================================================

def _connect(conn_ctx):
    """Verbindung zu Siemens SPS via snap7 (S7comm über TCP)."""
    try:
        import snap7
    except ImportError:
        logger.error("python-snap7 nicht installiert! pip install python-snap7")
        return False

    host = conn_ctx.get("host", "")
    rack = int(conn_ctx.get("config", {}).get("rack", 0))
    slot = int(conn_ctx.get("config", {}).get("slot", 1))

    if not host:
        logger.error("PROFINET: Kein Host konfiguriert")
        return False

    try:
        old = conn_ctx.get("client")
        if old is not None:
            try:
                old.disconnect()
                old.destroy()
            except Exception:
                pass
            conn_ctx["client"] = None

        client = snap7.client.Client()
        client.connect(host, rack, slot)

        if client.get_connected():
            conn_ctx["client"] = client
            conn_ctx["status"]["connected"] = True
            conn_ctx["status"]["last_connect_ts"] = time.time()
            conn_ctx["status"]["connection_failures"] = 0
            logger.info("PROFINET verbunden: %s (rack=%d, slot=%d)", host, rack, slot)
            return True

        logger.warning("PROFINET connect fehlgeschlagen: %s", host)
        conn_ctx["status"]["connection_failures"] += 1
        return False

    except Exception as exc:
        logger.error("PROFINET connect Exception: %s", exc)
        conn_ctx["status"]["connection_failures"] += 1
        return False


def _disconnect(conn_ctx):
    client = conn_ctx.get("client")
    if client is not None:
        try:
            client.disconnect()
            client.destroy()
        except Exception:
            pass
    conn_ctx["client"] = None
    conn_ctx["status"]["connected"] = False
    conn_ctx["status"]["last_disconnect_ts"] = time.time()


def _is_connected(conn_ctx):
    client = conn_ctx.get("client")
    if client is None:
        return False
    try:
        return bool(client.get_connected())
    except Exception:
        return False


def _health_check(conn_ctx):
    if not _is_connected(conn_ctx):
        return {"ok": False, "latency_ms": 0.0}
    t0 = time.perf_counter()
    try:
        client = conn_ctx["client"]
        info = client.get_cpu_info()
        dt = (time.perf_counter() - t0) * 1000.0
        return {"ok": True, "latency_ms": dt}
    except Exception:
        dt = (time.perf_counter() - t0) * 1000.0
        return {"ok": False, "latency_ms": dt}


# =============================================================================
# I/O Operationen
# =============================================================================

def _execute(conn_ctx, request):
    """Snap7 I/O Dispatch.

    data_type Mapping:
        "db"       → DB-Bereich (DB-Nummer in address, Offset in start)
        "input"    → Eingänge (PE)
        "output"   → Ausgänge (PA)
        "marker"   → Merker (M)
        "timer"    → Timer (T)
        "counter"  → Zähler (Z)
    """
    client = conn_ctx.get("client")
    if client is None or not _is_connected(conn_ctx):
        return make_result_error("not_connected", request=request)

    op = str(request.get("op", ""))
    data_type = str(request.get("data_type", "db"))
    t0 = time.perf_counter()

    try:
        import snap7.util as s7util
        import snap7.type as s7type

        area_map = {
            "db":      0x84,   # snap7.type.Areas.DB
            "input":   0x81,   # PE
            "output":  0x82,   # PA
            "marker":  0x83,   # MK
        }
        area = area_map.get(data_type, 0x84)
        db_number = int(request.get("unit", 1))
        start = int(request.get("start", request.get("address", 0)))
        count = int(request.get("count", 1))

        if op == "read_block":
            # Liest count Bytes ab start
            size = count * 2  # 2 Bytes pro Wort (16-Bit)
            raw = client.read_area(area, db_number, start, size)
            dt = (time.perf_counter() - t0) * 1000.0

            # Bytes → Word-Liste (Big-Endian, Siemens-Standard)
            values = []
            for i in range(0, len(raw), 2):
                if i + 1 < len(raw):
                    values.append((raw[i] << 8) | raw[i + 1])
                else:
                    values.append(raw[i])

            conn_ctx["status"]["last_io_ts"] = time.time()
            conn_ctx["metrics"]["reads_total"] += 1
            return make_result_ok(values[:count], latency_ms=dt, request=request)

        elif op == "read_single":
            raw = client.read_area(area, db_number, start, 2)
            dt = (time.perf_counter() - t0) * 1000.0
            value = (raw[0] << 8) | raw[1] if len(raw) >= 2 else 0
            conn_ctx["metrics"]["reads_total"] += 1
            return make_result_ok(value, latency_ms=dt, request=request)

        elif op == "write_single":
            value = request.get("value", 0)
            try:
                v_int = int(value)
            except Exception:
                v_int = 0
            data = bytearray([(v_int >> 8) & 0xFF, v_int & 0xFF])
            client.write_area(area, db_number, start, data)
            dt = (time.perf_counter() - t0) * 1000.0
            conn_ctx["metrics"]["writes_total"] += 1
            return make_result_ok(True, latency_ms=dt, request=request)

        elif op == "write_block":
            values = request.get("value", [])
            if not isinstance(values, (list, tuple)):
                values = [values]
            data = bytearray()
            for v in values:
                try:
                    vi = int(v)
                except Exception:
                    vi = 0
                data.extend([(vi >> 8) & 0xFF, vi & 0xFF])
            client.write_area(area, db_number, start, data)
            dt = (time.perf_counter() - t0) * 1000.0
            conn_ctx["metrics"]["writes_total"] += 1
            return make_result_ok(True, latency_ms=dt, request=request)

        else:
            dt = (time.perf_counter() - t0) * 1000.0
            return make_result_error("unknown_op: {}".format(op), latency_ms=dt, request=request)

    except Exception as exc:
        dt = (time.perf_counter() - t0) * 1000.0
        conn_ctx["metrics"]["errors_total"] += 1
        return make_result_error(str(exc), latency_ms=dt, request=request)


# =============================================================================
# Factory
# =============================================================================

def create_profinet_rt_adapter():
    return {
        "connect":       _connect,
        "disconnect":    _disconnect,
        "is_connected":  _is_connected,
        "health_check":  _health_check,
        "execute":       _execute,
        "protocol_type": "profinet_rt",
        "capabilities":  frozenset({
            "read_block", "read_single", "write_single", "write_block",
        }),
    }
