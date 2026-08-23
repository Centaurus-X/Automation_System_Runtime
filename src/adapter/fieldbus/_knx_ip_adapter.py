# -*- coding: utf-8 -*-

# src/adapter/fieldbus/_knx_ip_adapter.py

"""KNX IP Adapter – KNXnet/IP Tunneling, synchron, funktional.

Kommunikation über KNXnet/IP Interface oder Router.
Nutzt xknx (Python KNX Library) im synchronen Modus.

KNX-Adressierung:
    Gruppen-Adressen (GA):  "1/2/3" (Haupt/Mittel/Unter)
    Physische Adressen:      "1.1.5" (Bereich.Linie.Gerät)

Das generische Interface mappt:
    data_type="group"    → KNX Gruppenadresse (z.B. "1/2/3")
    data_type="dpt"      → Datapoint Type (z.B. DPT 9.001 = Temperatur)

Abhängigkeit: pip install xknx

Kein OOP, kein Decorator, kein Lambda, kein asyncio.

HINWEIS: xknx ist intern asyncio-basiert. Wir wrappen es synchron
         mit einem dedizierten Mini-Event-Loop im Connection Thread.
"""

import time
import logging
import threading

from functools import partial

try:
    from src.adapter._adapter_interface import make_result_ok, make_result_error
except ImportError:
    from _adapter_interface import make_result_ok, make_result_error


logger = logging.getLogger(__name__)


# =============================================================================
# xknx Sync-Wrapper
# =============================================================================

def _run_in_knx_loop(conn_ctx, coro):
    """Führt eine xknx-Coroutine synchron aus.

    xknx ist intern async – wir betreiben einen dedizierten
    Event-Loop im Connection Thread für KNX-Operationen.
    """
    loop = conn_ctx.get("_knx_loop")
    if loop is None:
        import asyncio
        loop = asyncio.new_event_loop()
        conn_ctx["_knx_loop"] = loop

    try:
        return loop.run_until_complete(coro)
    except Exception as exc:
        logger.error("KNX Loop Fehler: %s", exc)
        raise


# =============================================================================
# Lifecycle
# =============================================================================

def _connect(conn_ctx):
    try:
        import xknx as xknx_mod
    except ImportError:
        logger.error("xknx nicht installiert! pip install xknx")
        return False

    host = conn_ctx.get("host", "")
    port = int(conn_ctx.get("port", 3671))

    if not host:
        return False

    try:
        old = conn_ctx.get("client")
        if old is not None:
            try:
                _run_in_knx_loop(conn_ctx, old.stop())
            except Exception:
                pass

        knx = xknx_mod.XKNX(
            daemon_mode=False,
            connection_config=xknx_mod.io.ConnectionConfig(
                connection_type=xknx_mod.io.ConnectionType.TUNNELING,
                gateway_ip=host,
                gateway_port=port,
            ),
        )
        _run_in_knx_loop(conn_ctx, knx.start())

        conn_ctx["client"] = knx
        conn_ctx["status"]["connected"] = True
        conn_ctx["status"]["last_connect_ts"] = time.time()
        conn_ctx["status"]["connection_failures"] = 0
        logger.info("KNX IP verbunden: %s:%d", host, port)
        return True

    except Exception as exc:
        logger.error("KNX connect: %s", exc)
        conn_ctx["status"]["connection_failures"] += 1
        return False


def _disconnect(conn_ctx):
    client = conn_ctx.get("client")
    if client is not None:
        try:
            _run_in_knx_loop(conn_ctx, client.stop())
        except Exception:
            pass
    conn_ctx["client"] = None
    conn_ctx["status"]["connected"] = False

    loop = conn_ctx.get("_knx_loop")
    if loop is not None:
        try:
            loop.close()
        except Exception:
            pass
        conn_ctx["_knx_loop"] = None


def _is_connected(conn_ctx):
    client = conn_ctx.get("client")
    return client is not None and conn_ctx["status"].get("connected", False)


def _health_check(conn_ctx):
    if not _is_connected(conn_ctx):
        return {"ok": False, "latency_ms": 0.0}
    return {"ok": True, "latency_ms": 0.0}


# =============================================================================
# I/O
# =============================================================================

def _execute(conn_ctx, request):
    """KNX I/O über xknx.

    read_single + data_type="group" → Gruppen-Telegamm lesen
    write_single + data_type="group" → Gruppen-Telegamm schreiben
    """
    client = conn_ctx.get("client")
    if client is None or not _is_connected(conn_ctx):
        return make_result_error("not_connected", request=request)

    op = str(request.get("op", ""))
    data_type = str(request.get("data_type", "group"))
    ga = str(request.get("address", ""))
    t0 = time.perf_counter()

    try:
        import xknx as xknx_mod

        if op == "read_single" and data_type == "group":
            # GroupValueRead senden
            from xknx.core import ValueReader
            reader = ValueReader(client, xknx_mod.telegram.GroupAddress(ga))
            telegram = _run_in_knx_loop(conn_ctx, reader.read())
            dt = (time.perf_counter() - t0) * 1000.0

            if telegram is not None and hasattr(telegram, "payload"):
                value = telegram.payload.value if hasattr(telegram.payload, "value") else None
                conn_ctx["metrics"]["reads_total"] += 1
                return make_result_ok(value, latency_ms=dt, request=request)
            return make_result_error("no_response", latency_ms=dt, request=request)

        elif op == "write_single" and data_type == "group":
            from xknx.telegram import Telegram, GroupAddress
            from xknx.dpt import DPTArray
            value = request.get("value")

            if isinstance(value, bool):
                from xknx.dpt import DPTBinary
                payload = DPTBinary(1 if value else 0)
            elif isinstance(value, (int, float)):
                payload = DPTArray(int(value))
            else:
                payload = DPTArray(0)

            telegram = Telegram(
                destination_address=GroupAddress(ga),
                payload=payload,
            )
            _run_in_knx_loop(conn_ctx, client.telegrams.put(telegram))
            dt = (time.perf_counter() - t0) * 1000.0
            conn_ctx["metrics"]["writes_total"] += 1
            return make_result_ok(True, latency_ms=dt, request=request)

        elif op == "read_block":
            # Mehrere GAs lesen (kommasepariert)
            gas = [g.strip() for g in ga.split(",") if g.strip()]
            values = []
            for g in gas:
                try:
                    from xknx.core import ValueReader
                    reader = ValueReader(client, xknx_mod.telegram.GroupAddress(g))
                    telegram = _run_in_knx_loop(conn_ctx, reader.read())
                    if telegram and hasattr(telegram, "payload"):
                        values.append(
                            telegram.payload.value
                            if hasattr(telegram.payload, "value") else None
                        )
                    else:
                        values.append(None)
                except Exception:
                    values.append(None)
            dt = (time.perf_counter() - t0) * 1000.0
            conn_ctx["metrics"]["reads_total"] += 1
            return make_result_ok(values, latency_ms=dt, request=request)

        else:
            dt = (time.perf_counter() - t0) * 1000.0
            return make_result_error("unknown_op: {}".format(op), latency_ms=dt, request=request)

    except Exception as exc:
        dt = (time.perf_counter() - t0) * 1000.0
        conn_ctx["metrics"]["errors_total"] += 1
        return make_result_error(str(exc), latency_ms=dt, request=request)


def create_knx_ip_adapter():
    return {
        "connect":       _connect,
        "disconnect":    _disconnect,
        "is_connected":  _is_connected,
        "health_check":  _health_check,
        "execute":       _execute,
        "protocol_type": "knx_ip",
        "capabilities":  frozenset({
            "read_single", "read_block", "write_single",
            "group_read", "group_write",
        }),
    }
