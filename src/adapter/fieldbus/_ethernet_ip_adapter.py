# -*- coding: utf-8 -*-

# src/adapter/fieldbus/_ethernet_ip_adapter.py

"""EtherNet/IP adapter – CIP-basiert, synchron, funktional.

Kommunikation über pycomm3 (CIP über TCP/IP).
Unterstützt Allen-Bradley / Rockwell PLCs (CompactLogix, ControlLogix).

EtherNet/IP nutzt Tag-basierte Adressierung statt Register-Nummern.
Das generische Interface mappt:
    data_type="tag"    → Tag-Name als "address" (String!)
    data_type="io"     → Assembly Instance I/O

Abhängigkeit: pip install pycomm3

Kein OOP, kein Decorator, kein Lambda, kein asyncio.
"""

import time
import logging

from functools import partial

from src.adapter._adapter_interface import (
    make_result_ok,
    make_result_error,
)


logger = logging.getLogger(__name__)


def _connect(conn_ctx):
    try:
        from pycomm3 import LogixDriver
    except ImportError:
        logger.error("pycomm3 nicht installiert! pip install pycomm3")
        return False

    host = conn_ctx.get("host", "")
    slot = int(conn_ctx.get("config", {}).get("slot", 0))

    if not host:
        return False

    try:
        old = conn_ctx.get("client")
        if old is not None:
            try:
                old.close()
            except Exception:
                pass

        plc = LogixDriver(host, slot=slot, init_tags=True, init_program_tags=True)
        plc.open()

        conn_ctx["client"] = plc
        conn_ctx["status"]["connected"] = True
        conn_ctx["status"]["last_connect_ts"] = time.time()
        conn_ctx["status"]["connection_failures"] = 0
        conn_ctx["_tag_cache"] = dict(plc.tags) if hasattr(plc, "tags") else {}
        logger.info("EtherNet/IP verbunden: %s (slot=%d, tags=%d)",
                     host, slot, len(conn_ctx["_tag_cache"]))
        return True

    except Exception as exc:
        logger.error("EtherNet/IP connect: %s", exc)
        conn_ctx["status"]["connection_failures"] += 1
        return False


def _disconnect(conn_ctx):
    client = conn_ctx.get("client")
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
    conn_ctx["client"] = None
    conn_ctx["status"]["connected"] = False


def _is_connected(conn_ctx):
    client = conn_ctx.get("client")
    if client is None:
        return False
    try:
        return bool(client._connected)
    except Exception:
        return False


def _health_check(conn_ctx):
    if not _is_connected(conn_ctx):
        return {"ok": False, "latency_ms": 0.0}
    t0 = time.perf_counter()
    try:
        client = conn_ctx["client"]
        info = client.get_plc_info()
        dt = (time.perf_counter() - t0) * 1000.0
        return {"ok": bool(info), "latency_ms": dt}
    except Exception:
        return {"ok": False, "latency_ms": (time.perf_counter() - t0) * 1000.0}


def _execute(conn_ctx, request):
    """EtherNet/IP I/O via pycomm3.

    data_type="tag" → Tag-basiert: address ist Tag-Name (str)
    read_block + data_type="tag" → multiple Tags lesen (address=kommasepariert)
    """
    client = conn_ctx.get("client")
    if client is None or not _is_connected(conn_ctx):
        return make_result_error("not_connected", request=request)

    op = str(request.get("op", ""))
    data_type = str(request.get("data_type", "tag"))
    t0 = time.perf_counter()

    try:
        if op == "read_single" and data_type == "tag":
            tag_name = str(request.get("address", ""))
            result = client.read(tag_name)
            dt = (time.perf_counter() - t0) * 1000.0
            if result and result.error is None:
                conn_ctx["metrics"]["reads_total"] += 1
                return make_result_ok(result.value, latency_ms=dt, request=request)
            return make_result_error(str(result.error), latency_ms=dt, request=request)

        elif op == "read_block" and data_type == "tag":
            # Mehrere Tags auf einmal lesen
            tag_str = str(request.get("address", request.get("start", "")))
            tags = [t.strip() for t in tag_str.split(",") if t.strip()]
            if not tags:
                count = int(request.get("count", 1))
                base = str(request.get("start", ""))
                tags = ["{}[{}]".format(base, i) for i in range(count)]
            results = client.read(*tags)
            dt = (time.perf_counter() - t0) * 1000.0
            if not isinstance(results, list):
                results = [results]
            values = []
            for r in results:
                if r and r.error is None:
                    values.append(r.value)
                else:
                    values.append(None)
            conn_ctx["metrics"]["reads_total"] += 1
            return make_result_ok(values, latency_ms=dt, request=request)

        elif op == "write_single" and data_type == "tag":
            tag_name = str(request.get("address", ""))
            value = request.get("value")
            result = client.write((tag_name, value))
            dt = (time.perf_counter() - t0) * 1000.0
            if result and result.error is None:
                conn_ctx["metrics"]["writes_total"] += 1
                return make_result_ok(True, latency_ms=dt, request=request)
            return make_result_error(str(result.error), latency_ms=dt, request=request)

        elif op == "write_block" and data_type == "tag":
            tag_str = str(request.get("address", request.get("start", "")))
            values = request.get("value", [])
            if not isinstance(values, list):
                values = [values]
            tags = [t.strip() for t in tag_str.split(",")]
            write_pairs = list(zip(tags, values))
            results = client.write(*write_pairs)
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


def create_ethernet_ip_adapter():
    return {
        "connect":       _connect,
        "disconnect":    _disconnect,
        "is_connected":  _is_connected,
        "health_check":  _health_check,
        "execute":       _execute,
        "protocol_type": "ethernet_ip",
        "capabilities":  frozenset({
            "read_single", "read_block", "write_single", "write_block",
            "tag_read", "tag_write",
        }),
    }
