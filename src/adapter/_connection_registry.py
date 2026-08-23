# -*- coding: utf-8 -*-

# src/adapter/_connection_registry.py

"""Connection Registry – Zentrale Verwaltung aller aktiven Verbindungen.

Verantwortlichkeit:
    - Registrierung und Deregistrierung von Verbindungen
    - Thread-sichere Zugriffe auf den globalen Verbindungspool
    - Snapshot/Status-Abfragen für Monitoring
    - Adapter-Auflösung anhand protocol_type

Design:
    - Rein funktional, dict-basierter CTX
    - Thread-sicher über RLock
    - Kein OOP, kein Decorator, kein Lambda
    - functools.partial wo nötig
"""


import threading
import time
import logging

from functools import partial


logger = logging.getLogger(__name__)


# =============================================================================
# Adapter-Fabrik-Registry (protocol_type → create_adapter Funktion)
# =============================================================================

# Wird beim Import der Adapter-Module befüllt
_ADAPTER_FACTORIES = {}
_ADAPTER_FACTORIES_LOCK = threading.RLock()


def register_adapter_factory(protocol_type, factory_fn):
    """Registriert eine Adapter-Factory für einen Protokoll-Typ.

    factory_fn: callable() -> adapter_dict
    """
    with _ADAPTER_FACTORIES_LOCK:
        _ADAPTER_FACTORIES[str(protocol_type)] = factory_fn
    logger.info("Adapter-Factory registriert: %s", protocol_type)


def get_adapter_factory(protocol_type):
    """Liefert die registrierte Factory oder None."""
    with _ADAPTER_FACTORIES_LOCK:
        return _ADAPTER_FACTORIES.get(str(protocol_type))


def create_adapter_for_protocol(protocol_type):
    """Erzeugt einen neuen Adapter-Instance (dict) für das Protokoll."""
    factory = get_adapter_factory(protocol_type)
    if factory is None:
        logger.error("Kein Adapter registriert für Protokoll: %s", protocol_type)
        return None
    try:
        return factory()
    except Exception as exc:
        logger.error("Adapter-Factory Fehler für %s: %s", protocol_type, exc)
        return None


def list_registered_protocols():
    """Liste aller registrierten Protokoll-Typen."""
    with _ADAPTER_FACTORIES_LOCK:
        return sorted(_ADAPTER_FACTORIES.keys())


# =============================================================================
# Connection Registry CTX
# =============================================================================

def create_connection_registry(*, name="default"):
    """Erzeugt eine neue Connection Registry.

    Die Registry verwaltet:
    - connections: conn_key → conn_ctx (aktive Verbindungen)
    - threads:     conn_key → threading.Thread (Connection Threads)
    - stop_events: conn_key → threading.Event (pro Verbindung)

    Thread-sicher über einen globalen RLock.
    """
    return {
        "name":          str(name),
        "connections":   {},           # conn_key → conn_ctx
        "threads":       {},           # conn_key → threading.Thread
        "stop_events":   {},           # conn_key → threading.Event
        "lock":          threading.RLock(),

        "global_stop":   threading.Event(),

        "metrics": {
            "total_registered":   0,
            "total_unregistered": 0,
            "active_connections": 0,
            "started_ts":         time.time(),
        },

        # Event-Queue für Registry-Events (optional, für Event-Driven Architecture)
        "event_queue":   None,         # wird von außen gesetzt
    }


# =============================================================================
# CRUD Operationen
# =============================================================================

def registry_register(registry, conn_key, conn_ctx):
    """Registriert eine Verbindung in der Registry.

    Gibt True bei Erfolg zurück.
    Wenn conn_key bereits existiert, wird False zurückgegeben (kein Overwrite).
    """
    with registry["lock"]:
        if conn_key in registry["connections"]:
            logger.warning("Connection bereits registriert: %s", conn_key)
            return False

        registry["connections"][str(conn_key)] = conn_ctx
        registry["stop_events"][str(conn_key)] = threading.Event()
        registry["metrics"]["total_registered"] += 1
        registry["metrics"]["active_connections"] = len(registry["connections"])

        logger.info("Connection registriert: %s (%s)",
                     conn_key, conn_ctx.get("protocol_type", "?"))
        return True


def registry_unregister(registry, conn_key):
    """Entfernt eine Verbindung aus der Registry.

    Setzt das stop_event für den Connection Thread und
    entfernt alle Referenzen.
    """
    with registry["lock"]:
        # Stop-Signal an Connection Thread
        stop_evt = registry["stop_events"].pop(str(conn_key), None)
        if stop_evt is not None:
            stop_evt.set()

        conn_ctx = registry["connections"].pop(str(conn_key), None)
        thread = registry["threads"].pop(str(conn_key), None)

        registry["metrics"]["total_unregistered"] += 1
        registry["metrics"]["active_connections"] = len(registry["connections"])

    # Verbindung trennen (außerhalb des Locks)
    if conn_ctx is not None:
        adapter = conn_ctx.get("adapter")
        if adapter and callable(adapter.get("disconnect")):
            try:
                adapter["disconnect"](conn_ctx)
            except Exception as exc:
                logger.warning("Disconnect Fehler für %s: %s", conn_key, exc)

    # Thread joinen (mit Timeout)
    if thread is not None and thread.is_alive():
        try:
            thread.join(timeout=3.0)
        except Exception:
            pass

    logger.info("Connection deregistriert: %s", conn_key)
    return conn_ctx


def registry_get(registry, conn_key):
    """Holt einen conn_ctx aus der Registry (thread-safe)."""
    with registry["lock"]:
        return registry["connections"].get(str(conn_key))


def registry_get_stop_event(registry, conn_key):
    """Holt das stop_event für einen Connection Thread."""
    with registry["lock"]:
        return registry["stop_events"].get(str(conn_key))


def registry_set_thread(registry, conn_key, thread):
    """Setzt die Thread-Referenz für einen conn_key."""
    with registry["lock"]:
        registry["threads"][str(conn_key)] = thread


def registry_list_keys(registry):
    """Liste aller registrierten conn_keys."""
    with registry["lock"]:
        return list(registry["connections"].keys())


def registry_list_connected(registry):
    """Liste aller verbundenen conn_keys."""
    with registry["lock"]:
        result = []
        for key, ctx in registry["connections"].items():
            if ctx.get("status", {}).get("connected", False):
                result.append(key)
        return result


# =============================================================================
# Snapshots und Monitoring
# =============================================================================

def registry_snapshot(registry):
    """Erstellt einen Snapshot aller Verbindungen (für Monitoring).

    Gibt ein dict mit conn_key → Status-Info zurück.
    Keine Referenzen auf interne Objekte!
    """
    with registry["lock"]:
        snapshot = {}
        for key, ctx in registry["connections"].items():
            status = ctx.get("status", {})
            metrics = ctx.get("metrics", {})
            snapshot[key] = {
                "conn_key":       key,
                "protocol_type":  ctx.get("protocol_type", "?"),
                "host":           ctx.get("host", ""),
                "port":           ctx.get("port", 0),
                "connected":      status.get("connected", False),
                "failures":       status.get("connection_failures", 0),
                "last_io_ts":     status.get("last_io_ts", 0.0),
                "reads_total":    metrics.get("reads_total", 0),
                "writes_total":   metrics.get("writes_total", 0),
                "errors_total":   metrics.get("errors_total", 0),
            }
        return snapshot


def registry_metrics(registry):
    """Liefert die globalen Registry-Metriken."""
    with registry["lock"]:
        return dict(registry.get("metrics", {}))


# =============================================================================
# Batch-Operationen
# =============================================================================

def registry_stop_all(registry, *, join_timeout_s=5.0):
    """Stoppt alle Connection Threads und räumt die Registry auf.

    1. global_stop setzen
    2. alle stop_events setzen
    3. alle Verbindungen trennen
    4. alle Threads joinen
    """
    registry["global_stop"].set()

    with registry["lock"]:
        all_keys = list(registry["connections"].keys())

    for key in all_keys:
        stop_evt = registry_get_stop_event(registry, key)
        if stop_evt is not None:
            stop_evt.set()

    # Verbindungen trennen
    for key in all_keys:
        conn_ctx = registry_get(registry, key)
        if conn_ctx is not None:
            adapter = conn_ctx.get("adapter")
            if adapter and callable(adapter.get("disconnect")):
                try:
                    adapter["disconnect"](conn_ctx)
                except Exception:
                    pass

    # Threads joinen
    deadline = time.perf_counter() + float(join_timeout_s)
    with registry["lock"]:
        threads = list(registry["threads"].values())
    for th in threads:
        if th is not None and th.is_alive():
            remaining = max(0.1, deadline - time.perf_counter())
            try:
                th.join(timeout=remaining)
            except Exception:
                pass

    # Cleanup
    with registry["lock"]:
        registry["connections"].clear()
        registry["threads"].clear()
        registry["stop_events"].clear()
        registry["metrics"]["active_connections"] = 0

    logger.info("Connection Registry gestoppt: %d Verbindungen geschlossen", len(all_keys))


# =============================================================================
# Lookup-Helfer für Controller-Config → conn_key
# =============================================================================

def resolve_conn_key_for_controller(controller_data, config_data=None):
    """Leitet den conn_key aus der Controller-Konfiguration ab.

    Berücksichtigt:
    - protocol_type (default: "modbus_tcp" wenn bus_type == "modbus")
    - host / ip_address
    - port
    - Virtual Controller (falls aktiv)
    """
    protocol = str(controller_data.get("protocol_type") or
                   controller_data.get("bus_type") or "modbus").lower()
    protocol_aliases = {
        "modbus": "modbus_tcp",
        "s7": "plc_s7",
        "s7comm": "plc_s7",
        "tia": "plc_s7",
        "tia_portal": "plc_s7",
    }
    protocol = protocol_aliases.get(protocol, protocol)

    # Legacy-Mapping: bus_type → protocol_type
    if protocol == "modbus":
        protocol = "modbus_tcp"

    host = str(controller_data.get("host") or
               controller_data.get("ip_address") or "")
    default_port = {"plc_opcua": 4840, "plc_s7": 102}.get(protocol, 502)
    port = int(controller_data.get("port", default_port))

    # Virtual Controller prüfen
    if config_data:
        virt = _find_active_virtual(controller_data, config_data)
        if virt is not None:
            host = str(virt.get("host") or virt.get("ip_address") or host)
            if virt.get("port") is not None:
                port = int(virt["port"])

    comm = controller_data.get("comm")
    comm = comm if isinstance(comm, dict) else {}
    rack = int(controller_data.get("rack", comm.get("rack", 0)))
    slot = int(controller_data.get("slot", comm.get("slot", 1)))
    discriminator = "rack={}:slot={}".format(rack, slot) if protocol == "plc_s7" else ""


    from src.adapter._adapter_interface import build_conn_key
    return build_conn_key(
        protocol,
        host,
        port,
        endpoint_discriminator=discriminator,
    )


def _find_active_virtual(controller_data, config_data):
    """Find an active virtual endpoint by UUID, with legacy ID fallback."""
    try:
        controller_uuid = str(controller_data.get("controller_uuid") or "")
        controller_id = int(controller_data.get("controller_id", -1))
        for v in config_data.get("virtual_controllers") or []:
            same_uuid = (
                controller_uuid
                and str(v.get("controller_uuid") or "") == controller_uuid
            )
            same_legacy_id = int(v.get("controller_id", -1)) == controller_id
            active = bool(v.get("virt_active", v.get("enabled", False)))
            if (same_uuid or same_legacy_id) and active:
                return v
    except Exception:
        pass
    return None
