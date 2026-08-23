# -*- coding: utf-8 -*-

# src/adapter/_adapter_interface.py

"""Generisches Adapter-Interface für alle Feldbus-Protokolle.

Jeder Adapter ist ein dict mit definierten Callables.
Kein OOP, kein Decorator, kein Lambda.

Unterstützte Protokolle (Standard-Ethernet):
    - modbus_tcp      (pymodbus, Modbus TCP/UDP)
    - profinet_rt     (python-snap7 / Gateway)
    - profibus_gw     (Gateway → Modbus TCP / EtherNet/IP)
    - ethernet_ip     (pycomm3, CIP)
    - knx_ip          (KNXnet/IP Interface/Router)

Separate SPS-Schnittstelle (eigener Thread):
    - plc_modbus      (SPS über Modbus TCP)
    - plc_opcua       (SPS über OPC-UA)

Design:
    - Rein funktional, dict-basierte Adapter
    - functools.partial statt lambda
    - Synchron (kein asyncio)
    - Schwache Kopplung: Adapter kennt nur seinen eigenen State
"""


# =============================================================================
# Interface-Vertrag (jeder Adapter MUSS diese Keys liefern)
# =============================================================================

REQUIRED_ADAPTER_KEYS = frozenset({
    # Lifecycle
    "connect",          # (conn_ctx) -> bool
    "disconnect",       # (conn_ctx) -> None
    "is_connected",     # (conn_ctx) -> bool
    "health_check",     # (conn_ctx) -> {"ok": bool, "latency_ms": float}

    # I/O
    "execute",          # (conn_ctx, request) -> result_dict

    # Metadata
    "protocol_type",    # str
    "capabilities",     # frozenset of str
})


# Bekannte Capabilities
KNOWN_CAPABILITIES = frozenset({
    "read_block",
    "read_multi",
    "read_single",
    "write_single",
    "write_block",
    "pdo",
    "plc_status",
    "sdo",
    "tag_read",
    "tag_write",
    "typed_absolute_addressing",
    "group_read",
    "group_write",
})


# Bekannte Protokoll-Typen
KNOWN_PROTOCOL_TYPES = frozenset({
    "modbus_tcp",
    "profinet_rt",
    "profibus_gw",
    "ethernet_ip",
    "knx_ip",
    "plc_modbus",
    "plc_opcua",
    "plc_s7",
})


# Bekannte Operationen
KNOWN_OPS = frozenset({
    "read_block",
    "read_multi",
    "read_single",
    "write_single",
    "write_block",
    "plc_status",
})


# =============================================================================
# Request / Result Format
# =============================================================================

def make_request(op, data_type, *, address=0, start=0, count=1,
                 value=None, unit=1, meta=None, addresses=None):
    """Erzeugt ein standardisiertes Request-Dict.

    op:         "read_block" | "read_single" | "write_single" | "write_block"
    data_type:  Protokoll-gemappt (z.B. "coil", "holding", "register")
    address:    Einzel-Adresse (für single ops)
    start:      Start-Adresse (für block ops)
    count:      Anzahl (für block ops)
    value:      Schreibwert (für write ops)
    unit:       Geräte-ID (Modbus slave/device_id)
    meta:       Optionales dict, wird durchgereicht
    """
    request = {
        "op":        str(op),
        "data_type": str(data_type),
        "address":   address,
        "start":     start,
        "count":     int(count),
        "value":     value,
        "unit":      int(unit),
        "meta":      dict(meta) if meta else {},
    }
    if addresses is not None:
        request["addresses"] = list(addresses)
    return request


def make_result(ok, *, data=None, error=None, latency_ms=0.0, request=None):
    """Erzeugt ein standardisiertes Result-Dict."""
    return {
        "ok":         bool(ok),
        "data":       data,
        "error":      str(error) if error else None,
        "latency_ms": float(latency_ms),
        "request":    request,
    }


def make_result_ok(data, latency_ms=0.0, request=None):
    """Shortcut für erfolgreiches Result."""
    return make_result(True, data=data, latency_ms=latency_ms, request=request)


def make_result_error(error, latency_ms=0.0, request=None):
    """Shortcut für fehlerhaftes Result."""
    return make_result(False, error=error, latency_ms=latency_ms, request=request)


# =============================================================================
# Adapter-Validierung
# =============================================================================

def validate_adapter(adapter):
    """Prüft, ob ein Adapter-Dict alle erforderlichen Keys hat.

    Gibt (True, []) oder (False, [fehlende_keys]) zurück.
    """
    if not isinstance(adapter, dict):
        return False, ["adapter_is_not_a_dict"]

    missing = []
    for key in REQUIRED_ADAPTER_KEYS:
        if key not in adapter:
            missing.append(key)

    # Callables prüfen
    callable_keys = {"connect", "disconnect", "is_connected", "health_check", "execute"}
    for key in callable_keys:
        if key in adapter and not callable(adapter[key]):
            missing.append("{}_not_callable".format(key))

    if missing:
        return False, missing
    return True, []


# =============================================================================
# Data-Type Mapping: function_type (legacy) → generisches data_type
# =============================================================================

# Modbus function_type → generisches Mapping
MODBUS_FUNCTION_TYPE_MAP = {
    "read_coil":                "coil",
    "read_holding_registers":   "holding",
    "read_input_registers":     "input",
    "read_discrete_inputs":     "discrete",
    "write_coil":               "coil",
    "write_register":           "register",
    "write_holding_register":   "register",
    "write_single_register":    "register",
}

S7_FUNCTION_TYPE_MAP = {
    "read_s7": "s7",
    "read_value": "s7",
    "write_s7": "s7",
    "write_value": "s7",
}



def map_function_type_to_data_type(function_type, protocol_type="modbus_tcp"):
    """Mappt legacy function_type auf generisches data_type."""
    if protocol_type in ("modbus_tcp", "plc_modbus"):
        return MODBUS_FUNCTION_TYPE_MAP.get(str(function_type), str(function_type))
    if protocol_type == "plc_s7":
        return S7_FUNCTION_TYPE_MAP.get(str(function_type), "s7")
    # Andere Protokolle: direkt durchreichen
    return str(function_type)


def map_function_type_to_op(function_type, protocol_type="modbus_tcp"):
    """Mappt legacy function_type auf generische Operation."""
    ft = str(function_type).lower()
    if protocol_type == "plc_s7" and ft in ("read_s7", "read_value"):
        return "read_single"
    if protocol_type == "plc_s7" and ft in ("write_s7", "write_value"):
        return "write_single"
    if ft.startswith("read_"):
        if "registers" in ft or "inputs" in ft:
            return "read_block"
        return "read_block"
    if ft.startswith("write_"):
        return "write_single"
    return "read_single"


# =============================================================================
# Connection Context Factory
# =============================================================================

def create_connection_context(*, conn_key, protocol_type, host, port,
                               unit=1, adapter=None, config=None):
    """Erzeugt einen neuen Connection-Context (CTX).

    Jeder Connection-Thread bekommt seinen eigenen CTX.
    """
    return {
        "conn_key":       str(conn_key),
        "protocol_type":  str(protocol_type),
        "host":           str(host),
        "port":           int(port),
        "unit":           int(unit),
        "adapter":        adapter,
        "config":         dict(config) if config else {},

        # Status (wird vom Connection Thread verwaltet)
        "status": {
            "connected":              False,
            "last_connect_ts":        0.0,
            "last_disconnect_ts":     0.0,
            "last_io_ts":             0.0,
            "connection_failures":    0,
            "backoff_until_ts":       0.0,
        },

        # Interner Client-State (Adapter-spezifisch)
        "client":   None,

        # Metriken
        "metrics": {
            "reads_total":        0,
            "writes_total":       0,
            "errors_total":       0,
            "reconnects_total":   0,
            "latency_read_avg":   0.0,
            "latency_write_avg":  0.0,
            "uptime_s":           0.0,
        },
    }


def build_conn_key(protocol_type, host, port, endpoint_discriminator=None):
    """Erzeugt einen eindeutigen Connection-Key."""
    base = "{}:{}:{}".format(str(protocol_type), str(host), int(port))
    discriminator = str(endpoint_discriminator or "").strip()
    if discriminator:
        return "{}:{}".format(base, discriminator)
    return base
