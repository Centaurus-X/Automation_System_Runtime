# -*- coding: utf-8 -*-

# src/adapter/plc/_plc_opcua_adapter.py

"""SPS-Anbindung über OPC-UA – Synchron (wrapped), funktional.

OPC-UA bietet standardisierte, herstellerunabhängige Kommunikation
mit SPSen (Siemens, Beckhoff, B&R, Codesys, etc.).

Node-Adressierung:
    OPC-UA nutzt NodeId statt Register-Nummern:
        "ns=3;s=PLC1.Temperature"    (String-basiert)
        "ns=2;i=4711"               (Integer-basiert)

Das generische Interface mappt:
    data_type="node"   → OPC-UA Node (address = NodeId String)
    read_block         → Mehrere Nodes lesen (kommasepariert)
    write_single       → Einzelnen Node schreiben

Abhängigkeit: pip install asyncua (opcua-asyncio)

HINWEIS: asyncua ist intern asyncio-basiert. Wir betreiben einen
         dedizierten Mini-Event-Loop pro Connection Thread.

Kein OOP, kein Decorator, kein Lambda.
"""

import time
import logging

from functools import partial

try:
    from src.adapter._adapter_interface import make_result_ok, make_result_error
except ImportError:
    from _adapter_interface import make_result_ok, make_result_error


logger = logging.getLogger(__name__)


# =============================================================================
# Async-Wrapper (asyncua ist intern async)
# =============================================================================

def _run_in_opcua_loop(conn_ctx, coro):
    """Führt eine asyncua-Coroutine synchron aus."""
    loop = conn_ctx.get("_opcua_loop")
    if loop is None:
        import asyncio
        loop = asyncio.new_event_loop()
        conn_ctx["_opcua_loop"] = loop
    try:
        return loop.run_until_complete(coro)
    except Exception as exc:
        logger.error("OPC-UA Loop Fehler: %s", exc)
        raise


# =============================================================================
# Lifecycle
# =============================================================================

def _connect(conn_ctx):
    try:
        from asyncua import Client as OpcClient
    except ImportError:
        logger.error("asyncua nicht installiert! pip install asyncua")
        return False

    host = conn_ctx.get("host", "")
    port = int(conn_ctx.get("port", 4840))
    endpoint = conn_ctx.get("config", {}).get(
        "endpoint",
        "opc.tcp://{}:{}".format(host, port),
    )

    if not host and "://" not in endpoint:
        return False

    try:
        old = conn_ctx.get("client")
        if old is not None:
            try:
                _run_in_opcua_loop(conn_ctx, old.disconnect())
            except Exception:
                pass

        async def _async_connect():
            client = OpcClient(endpoint)
            # Optionale Security
            security_policy = conn_ctx.get("config", {}).get("security_policy")
            if security_policy:
                await client.set_security_string(security_policy)
            # Optionale Authentifizierung
            username = conn_ctx.get("config", {}).get("username")
            password = conn_ctx.get("config", {}).get("password")
            if username:
                client.set_user(username)
                client.set_password(password or "")
            await client.connect()
            return client

        client = _run_in_opcua_loop(conn_ctx, _async_connect())

        conn_ctx["client"] = client
        conn_ctx["status"]["connected"] = True
        conn_ctx["status"]["last_connect_ts"] = time.time()
        conn_ctx["status"]["connection_failures"] = 0
        logger.info("OPC-UA verbunden: %s", endpoint)
        return True

    except Exception as exc:
        logger.error("OPC-UA connect: %s", exc)
        conn_ctx["status"]["connection_failures"] += 1
        return False


def _disconnect(conn_ctx):
    client = conn_ctx.get("client")
    if client is not None:
        try:
            _run_in_opcua_loop(conn_ctx, client.disconnect())
        except Exception:
            pass
    conn_ctx["client"] = None
    conn_ctx["status"]["connected"] = False

    loop = conn_ctx.get("_opcua_loop")
    if loop is not None:
        try:
            loop.close()
        except Exception:
            pass
        conn_ctx["_opcua_loop"] = None


def _is_connected(conn_ctx):
    client = conn_ctx.get("client")
    return client is not None and conn_ctx["status"].get("connected", False)


def _health_check(conn_ctx):
    if not _is_connected(conn_ctx):
        return {"ok": False, "latency_ms": 0.0}
    t0 = time.perf_counter()
    try:
        async def _async_check():
            client = conn_ctx["client"]
            # Server-Status lesen (Standard-Node)
            node = client.get_node("ns=0;i=2259")  # ServerStatus
            val = await node.read_value()
            return val

        _run_in_opcua_loop(conn_ctx, _async_check())
        dt = (time.perf_counter() - t0) * 1000.0
        return {"ok": True, "latency_ms": dt}
    except Exception:
        return {"ok": False, "latency_ms": (time.perf_counter() - t0) * 1000.0}


# =============================================================================
# I/O
# =============================================================================

def _parse_node_id(node_id_str):
    """Parst einen NodeId-String: 'ns=3;s=Var1' oder 'ns=2;i=4711'."""
    return str(node_id_str)


def _execute(conn_ctx, request):
    """OPC-UA I/O.

    data_type="node" → OPC-UA Node-Zugriff
    address: NodeId String (z.B. "ns=3;s=PLC1.Temperature")
    """
    client = conn_ctx.get("client")
    if client is None or not _is_connected(conn_ctx):
        return make_result_error("not_connected", request=request)

    op = str(request.get("op", ""))
    node_id = str(request.get("address", ""))
    t0 = time.perf_counter()

    try:
        if op == "read_single":
            async def _read():
                node = client.get_node(_parse_node_id(node_id))
                return await node.read_value()

            value = _run_in_opcua_loop(conn_ctx, _read())
            dt = (time.perf_counter() - t0) * 1000.0
            conn_ctx["metrics"]["reads_total"] += 1
            return make_result_ok(value, latency_ms=dt, request=request)

        elif op == "read_block":
            # Mehrere Nodes (kommasepariert oder Array-Index)
            nodes_str = str(request.get("address", request.get("start", "")))
            node_ids = [n.strip() for n in nodes_str.split(",") if n.strip()]

            if not node_ids:
                count = int(request.get("count", 1))
                base = str(request.get("start", ""))
                node_ids = [base] * count  # Single-Node repeated

            async def _read_multi():
                values = []
                for nid in node_ids:
                    node = client.get_node(_parse_node_id(nid))
                    v = await node.read_value()
                    values.append(v)
                return values

            values = _run_in_opcua_loop(conn_ctx, _read_multi())
            dt = (time.perf_counter() - t0) * 1000.0
            conn_ctx["metrics"]["reads_total"] += 1
            return make_result_ok(values, latency_ms=dt, request=request)

        elif op == "write_single":
            value = request.get("value")

            async def _write():
                from asyncua import ua
                node = client.get_node(_parse_node_id(node_id))
                # Datentyp vom Server erfragen
                dv = await node.read_data_value()
                variant_type = dv.Value.VariantType if dv.Value else None
                if variant_type is not None:
                    await node.write_value(ua.DataValue(ua.Variant(value, variant_type)))
                else:
                    await node.write_value(value)

            _run_in_opcua_loop(conn_ctx, _write())
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

def create_plc_opcua_adapter():
    return {
        "connect":       _connect,
        "disconnect":    _disconnect,
        "is_connected":  _is_connected,
        "health_check":  _health_check,
        "execute":       _execute,
        "protocol_type": "plc_opcua",
        "capabilities":  frozenset({
            "read_single", "read_block", "write_single",
        }),
    }
