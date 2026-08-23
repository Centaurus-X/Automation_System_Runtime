# -*- coding: utf-8 -*-

# src/adapter/_connection_thread.py

"""Connection Thread – Synchroner, event-gesteuerter I/O-Thread.

Verantwortlichkeit:
    - Statische, persistente Verbindung zu einem physischen Gerät halten
    - I/O-Requests aus der Request-Queue abarbeiten (blockierend!)
    - Automatisches Reconnect mit exponentiellem Backoff
    - Health-Monitoring und Metriken
    - Event-Emission bei Statusänderungen

Design-Prinzipien:
    - 1 Thread pro physische Verbindung (conn_key)
    - Synchron, kein asyncio
    - Blockierende Queue.get() → CPU im Leerlauf < 1%
    - Kein OOP, kein Decorator, kein Lambda
    - functools.partial statt lambda
    - Event-Driven: Statusänderungen als Events in Event-Queue

Lebenszyklus:
    1. start_connection_thread() → Thread erstellen + starten
    2. connection_thread_main() → Endlos-Loop mit blocking Queue
    3. stop: stop_event.set() → Thread terminiert sauber
"""


import threading
import time
import logging

from queue import Queue, Empty
from functools import partial


logger = logging.getLogger(__name__)


# =============================================================================
# Sentinels
# =============================================================================

CONNECTION_STOP = {"_type": "__CONN_STOP__", "_sentinel": True}


def _is_stop_sentinel(obj):
    """Prüft ob ein Objekt ein Stop-Sentinel ist."""
    try:
        return (isinstance(obj, dict)
                and obj.get("_type") == "__CONN_STOP__"
                and obj.get("_sentinel") is True)
    except Exception:
        return False


# =============================================================================
# Backoff-Berechnung
# =============================================================================

def _calculate_backoff_s(failures, *, base_s=1.0, max_s=30.0, factor=2.0):
    """Exponentieller Backoff mit Obergrenze.

    failures=0 → 0s (sofort)
    failures=1 → 1s
    failures=2 → 2s
    failures=3 → 4s
    failures=5 → 16s
    failures=8+ → 30s (max)
    """
    if failures <= 0:
        return 0.0
    backoff = base_s * (factor ** min(failures - 1, 10))
    return min(backoff, max_s)


def _should_reconnect(conn_ctx):
    """Prüft ob ein Reconnect-Versuch erlaubt ist (Backoff beachten)."""
    status = conn_ctx.get("status", {})
    failures = int(status.get("connection_failures", 0))

    if failures <= 0:
        return True

    backoff_s = _calculate_backoff_s(failures)
    backoff_until = float(status.get("backoff_until_ts", 0.0))

    if time.time() >= backoff_until:
        return True
    return False


def _mark_backoff(conn_ctx):
    """Setzt den Backoff-Zeitpunkt nach einem fehlgeschlagenen Connect."""
    status = conn_ctx["status"]
    failures = int(status.get("connection_failures", 0))
    backoff_s = _calculate_backoff_s(failures)
    status["backoff_until_ts"] = time.time() + backoff_s


# =============================================================================
# Event-Emission
# =============================================================================

def _emit_connection_event(conn_ctx, event_type, extra=None):
    """Sendet ein Connection-Event in die Event-Queue (falls konfiguriert)."""
    eq = conn_ctx.get("event_queue")
    if eq is None:
        return

    evt = {
        "event_type": str(event_type),
        "source": "connection_thread",
        "target": "thread_management",
        "conn_key":   conn_ctx.get("conn_key", "?"),
        "protocol":   conn_ctx.get("protocol_type", "?"),
        "host":       conn_ctx.get("host", ""),
        "port":       conn_ctx.get("port", 0),
        "timestamp":  time.time(),
    }
    if extra and isinstance(extra, dict):
        evt.update(extra)

    try:
        eq.put_nowait(evt)
    except Exception:
        try:
            eq.put(evt, timeout=0.1)
        except Exception:
            pass


# =============================================================================
# Haupt-Loop
# =============================================================================

def connection_thread_main(conn_ctx, *, stop_event=None,
                            request_queue=None, response_queue=None):
    """Haupt-Loop des Connection Threads.

    Arbeitet Requests aus der request_queue ab.
    Blockiert auf Queue.get() wenn keine Requests anstehen → CPU idle.

    Parameter:
        conn_ctx:       Connection Context (dict)
        stop_event:     threading.Event zum sauberen Beenden
        request_queue:  Queue für eingehende I/O-Requests
        response_queue: Queue für Ergebnisse (optional, wenn caller
                        ein Future/Event verwendet, kann dies None sein)
    """
    if stop_event is None:
        stop_event = threading.Event()
    if request_queue is None:
        request_queue = Queue()

    adapter = conn_ctx.get("adapter")
    conn_key = conn_ctx.get("conn_key", "?")

    if adapter is None:
        logger.error("[%s] Kein Adapter konfiguriert – Thread beendet", conn_key)
        return

    connect_fn = adapter.get("connect")
    disconnect_fn = adapter.get("disconnect")
    is_connected_fn = adapter.get("is_connected")
    execute_fn = adapter.get("execute")

    if not all(callable(f) for f in [connect_fn, execute_fn]):
        logger.error("[%s] Adapter hat keine callable connect/execute", conn_key)
        return

    logger.info("[%s] Connection Thread gestartet (%s)",
                conn_key, conn_ctx.get("protocol_type", "?"))

    _emit_connection_event(conn_ctx, "CONNECTION_THREAD_STARTED")

    # ---- Initial Connect ----
    _try_connect(conn_ctx, connect_fn)

    # ---- Haupt-Loop ----
    while not stop_event.is_set():
        # Verbindung prüfen / wiederherstellen
        if not is_connected_fn(conn_ctx):
            if _should_reconnect(conn_ctx):
                ok = _try_connect(conn_ctx, connect_fn)
                if not ok:
                    _mark_backoff(conn_ctx)
                    # Kurz schlafen, aber stop_event beachten
                    stop_event.wait(timeout=1.0)
                    continue
            else:
                # Im Backoff: warten bis Backoff abgelaufen
                remaining = (conn_ctx["status"].get("backoff_until_ts", 0.0)
                             - time.time())
                wait_s = max(0.1, min(remaining, 2.0))
                stop_event.wait(timeout=wait_s)
                continue

        # ---- Request aus Queue holen (BLOCKIEREND!) ----
        # Das ist der Kern der niedrigen CPU-Last:
        # Thread schläft hier bis ein Request kommt oder Timeout.
        try:
            request = request_queue.get(timeout=1.0)
        except Empty:
            # Kein Request → nächste Iteration (Connection-Check)
            continue

        # Stop-Sentinel prüfen
        if _is_stop_sentinel(request):
            break
        if request is None:
            continue

        # ---- Request verarbeiten ----
        try:
            result = execute_fn(conn_ctx, request)
        except Exception as exc:
            logger.error("[%s] Execute Exception: %s", conn_key, exc)
            result = {
                "ok": False,
                "error": str(exc),
                "data": None,
                "request": request,
            }

        # ---- Response senden ----
        # Option A: Response-Queue
        if response_queue is not None:
            try:
                response_queue.put_nowait(result)
            except Exception:
                try:
                    response_queue.put(result, timeout=0.5)
                except Exception:
                    logger.warning("[%s] Response-Queue voll", conn_key)

        # Option B: Callback im Request (für synchrone Aufrufe)
        callback = request.get("_callback")
        if callback is not None and callable(callback):
            try:
                callback(result)
            except Exception as exc:
                logger.warning("[%s] Callback Fehler: %s", conn_key, exc)

        # Option C: Event (im Request) – für synchrones Warten
        response_event = request.get("_response_event")
        response_slot = request.get("_response_slot")
        if response_event is not None and response_slot is not None:
            response_slot["result"] = result
            response_event.set()

        # Verbindungsstatus nach fehlerhaftem I/O prüfen
        if not result.get("ok", False):
            error_str = str(result.get("error", "")).lower()
            # Bei Verbindungsfehlern → Reconnect im nächsten Zyklus
            if any(kw in error_str for kw in
                   ("connection", "not_connected", "reset", "broken", "eof")):
                conn_ctx["status"]["connected"] = False
                _emit_connection_event(conn_ctx, "CONNECTION_LOST",
                                       {"error": result.get("error")})

        # task_done für Queue-Accounting
        try:
            request_queue.task_done()
        except (ValueError, AttributeError):
            pass

    # ---- Shutdown ----
    logger.info("[%s] Connection Thread beendet", conn_key)
    if callable(disconnect_fn):
        try:
            disconnect_fn(conn_ctx)
        except Exception:
            pass
    _emit_connection_event(conn_ctx, "CONNECTION_THREAD_STOPPED")


def _try_connect(conn_ctx, connect_fn):
    """Versucht eine Verbindung herzustellen. Emittiert Events."""
    conn_key = conn_ctx.get("conn_key", "?")
    was_connected = conn_ctx["status"].get("connected", False)

    try:
        ok = connect_fn(conn_ctx)
    except Exception as exc:
        logger.error("[%s] Connect Exception: %s", conn_key, exc)
        ok = False

    if ok:
        conn_ctx["metrics"]["reconnects_total"] += 1
        if not was_connected:
            _emit_connection_event(conn_ctx, "CONNECTION_ESTABLISHED")
        return True

    _emit_connection_event(conn_ctx, "CONNECTION_FAILED", {
        "failures": conn_ctx["status"].get("connection_failures", 0),
    })
    return False


# =============================================================================
# Synchroner I/O-Aufruf (Request/Response mit Event-Wait)
# =============================================================================

def sync_execute(request_queue, request, *, timeout_s=5.0, cancel_event=None):
    """Synchroner I/O-Aufruf über den Connection Thread.

    Sendet einen Request in die Queue des Connection Threads,
    wartet blockierend auf die Antwort (via threading.Event).

    Gibt das result_dict zurück oder ein Error-Result bei Timeout.

    WICHTIG: Diese Funktion blockiert den aufrufenden Thread
    bis das Ergebnis da ist oder der Timeout abläuft.
    Das ist beabsichtigt für synchronen Code!
    """
    if request_queue is None:
        return {"ok": False, "error": "no_request_queue", "data": None}

    # Response-Slot: dict + Event
    response_event = threading.Event()
    response_slot = {"result": None}

    # Request mit Response-Mechanismus anreichern
    enriched = dict(request)
    enriched["_response_event"] = response_event
    enriched["_response_slot"] = response_slot

    # Request in Queue
    try:
        request_queue.put(enriched, timeout=1.0)
    except Exception:
        return {"ok": False, "error": "queue_full", "data": None}

    # Wait in short, deterministic intervals so runtime shutdown can cancel
    # an in-flight request without retaining the controller polling thread.
    timeout = max(0.0, float(timeout_s))
    deadline = time.monotonic() + timeout
    got_response = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            break
        if response_event.wait(timeout=min(0.1, remaining)):
            got_response = True
            break
        if callable(getattr(cancel_event, "is_set", None)) and cancel_event.is_set():
            return {
                "ok": False,
                "error": "cancelled",
                "data": None,
                "request": request,
            }

    if not got_response:
        return {
            "ok": False,
            "error": "timeout_{}s".format(timeout_s),
            "data": None,
            "request": request,
        }

    return response_slot.get("result") or {
        "ok": False,
        "error": "empty_response",
        "data": None,
    }


# =============================================================================
# Thread Lifecycle
# =============================================================================

def start_connection_thread(registry, conn_key, conn_ctx, *,
                             request_queue=None):
    """Erstellt und startet einen Connection Thread.

    Registriert den Thread in der Registry.
    Gibt die request_queue zurück (erstellt eine neue falls nicht übergeben).
    """
    try:
        from _connection_registry import (
            registry_register,
            registry_set_thread,
            registry_get_stop_event,
        )
    except ImportError:
        from src.adapter._connection_registry import (
            registry_register,
            registry_set_thread,
            registry_get_stop_event,
        )

    if request_queue is None:
        request_queue = Queue()

    # Request-Queue im conn_ctx speichern
    conn_ctx["request_queue"] = request_queue

    # In Registry registrieren
    registry_register(registry, conn_key, conn_ctx)
    stop_event = registry_get_stop_event(registry, conn_key)

    # Thread erstellen
    thread_name = "Conn-{}".format(conn_key)
    thread_fn = partial(
        connection_thread_main,
        conn_ctx,
        stop_event=stop_event,
        request_queue=request_queue,
    )

    thread = threading.Thread(
        target=thread_fn,
        name=thread_name,
        daemon=False,
    )

    # Thread in Registry registrieren und starten
    registry_set_thread(registry, conn_key, thread)
    thread.start()

    logger.info("Connection Thread gestartet: %s", thread_name)
    return request_queue


def stop_connection_thread(registry, conn_key, *, join_timeout_s=3.0):
    """Stoppt einen einzelnen Connection Thread."""
    try:
        from _connection_registry import (
            registry_unregister,
        )
    except ImportError:
        from src.adapter._connection_registry import (
            registry_unregister,
        )

    # Stop-Sentinel in Request-Queue
    conn_ctx = None
    try:
        from _connection_registry import registry_get
    except ImportError:
        from src.adapter._connection_registry import registry_get

    conn_ctx = registry_get(registry, conn_key)
    if conn_ctx and conn_ctx.get("request_queue"):
        try:
            conn_ctx["request_queue"].put_nowait(CONNECTION_STOP)
        except Exception:
            pass

    # Registry deregistriert, trennt Verbindung, joint Thread
    registry_unregister(registry, conn_key)


# =============================================================================
# Batch-Startup (für alle Controller aus Config)
# =============================================================================

def start_connections_from_config(registry, config_data, *, event_queue=None):
    """Startet Connection Threads für alle Controller in der Config.

    Nutzt die Controller-Config um conn_keys abzuleiten und
    shared Connections (gleiche IP:Port) zu erkennen.

    Gibt ein Mapping conn_key → request_queue zurück.
    """
    from src.adapter._adapter_interface import create_connection_context
    from src.adapter._connection_registry import (
        create_adapter_for_protocol,
        registry_get,
        resolve_conn_key_for_controller,
    )

    controllers = config_data.get("controllers") or []
    virtual_controllers = config_data.get("virtual_controllers") or []

    # conn_key → request_queue Mapping
    queues = {}

    for ctrl in controllers:
        if not isinstance(ctrl, dict):
            continue

        cid = ctrl.get("controller_id")
        controller_uuid = str(ctrl.get("controller_uuid") or "")
        protocol = str(ctrl.get("protocol_type") or
                       ctrl.get("bus_type") or "modbus").lower()
        protocol_aliases = {
            "modbus": "modbus_tcp",
            "s7": "plc_s7",
            "s7comm": "plc_s7",
            "tia": "plc_s7",
            "tia_portal": "plc_s7",
        }
        protocol = protocol_aliases.get(protocol, protocol)
        if protocol == "modbus":
            protocol = "modbus_tcp"

        host = str(ctrl.get("host") or ctrl.get("ip_address") or "")
        default_port = {"plc_opcua": 4840, "plc_s7": 102}.get(protocol, 502)
        port = int(ctrl.get("port", default_port))
        unit = ctrl.get("unit", "0x1")

        # Virtual Controller prüfen
        for v in virtual_controllers:
            same_uuid = (
                controller_uuid
                and str(v.get("controller_uuid") or "") == controller_uuid
            )
            same_legacy_id = int(v.get("controller_id", -1)) == int(cid)
            active = bool(v.get("virt_active", v.get("enabled", False)))
            if (same_uuid or same_legacy_id) and active:
                host = str(v.get("host") or v.get("ip_address") or host)
                if v.get("port") is not None:
                    port = int(v["port"])
                break

        conn_key = resolve_conn_key_for_controller(ctrl, config_data)

        # Bereits gestartet? → Shared Connection (Multiplexing)
        if registry_get(registry, conn_key) is not None:
            # Queue bereits vorhanden → wiederverwenden
            existing = registry_get(registry, conn_key)
            if existing and existing.get("request_queue"):
                queues.setdefault(conn_key, existing["request_queue"])
            logger.info("Shared Connection für C%s: %s", cid, conn_key)
            continue

        # Unit konvertieren
        if isinstance(unit, str):
            try:
                unit_int = int(unit.replace("0x", ""), 16)
            except Exception:
                unit_int = 1
        else:
            try:
                unit_int = int(unit)
            except Exception:
                unit_int = 1

        # Adapter erstellen
        adapter = create_adapter_for_protocol(protocol)
        if adapter is None:
            logger.error("Kein Adapter für C%s (%s) – übersprungen", cid, protocol)
            continue

        comm_config = dict(ctrl.get("comm") or {})
        for comm_key in (
            "area_rids",
            "connection_type",
            "optimized_block_access",
            "rack",
            "s7_batch_fallback_sequential",
            "s7_max_parallel",
            "s7_multi_read_max_gap",
            "s7_multi_var_limit",
            "s7_read_optimizer",
            "security",
            "slot",
            "string_encoding",
        ):
            if comm_key in ctrl and comm_key not in comm_config:
                comm_config[comm_key] = ctrl[comm_key]

        # Connection Context erstellen
        conn_ctx = create_connection_context(
            conn_key=conn_key,
            protocol_type=protocol,
            host=host,
            port=port,
            unit=unit_int,
            adapter=adapter,
            config=comm_config,
        )

        # Event-Queue setzen
        if event_queue is not None:
            conn_ctx["event_queue"] = event_queue

        # Thread starten
        rq = start_connection_thread(registry, conn_key, conn_ctx)
        queues[conn_key] = rq

        logger.info("Connection gestartet: C%s → %s", cid, conn_key)

    return queues
