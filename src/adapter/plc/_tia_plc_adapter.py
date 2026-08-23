# -*- coding: utf-8 -*-

# src/adapter/plc/_tia_plc_adapter.py

"""Generic Siemens TIA Portal PLC adapter using synchronous S7 communication.

The stable backend is python-snap7 over ISO-on-TCP (RFC 1006, port 102).
Absolute addresses use ``DB<n>.<offset>:<type>`` and M/I/Q area variants.
S7CommPlus/TLS is selected explicitly and fails closed when its optional
backend or certificate material is unavailable. Credentials are resolved from
an environment variable or an injected credential provider; inline secrets in
the communication config are rejected.

Absolute DB access requires non-optimized data blocks in TIA Portal. Optimized
blocks must be accessed through OPC UA or an explicitly supported symbolic
S7CommPlus path instead.

No OOP, no decorators, no lambdas, no asyncio.
"""

import importlib
import logging
import os
import re
import struct
import time


try:
    from src.adapter._adapter_interface import make_result_error, make_result_ok
except ImportError:
    from _adapter_interface import make_result_error, make_result_ok


LOGGER = logging.getLogger(__name__)

S7_MAX_MULTI_VARS = 20

_AREA_ALIASES = {
    "A": "Q",
    "E": "I",
    "I": "I",
    "M": "M",
    "Q": "Q",
}

_AREA_ENUM_NAMES = {
    "DB": "DB",
    "M": "MK",
    "I": "PE",
    "Q": "PA",
}

_AREA_NUMBERS = {
    "DB": 0x84,
    "M": 0x83,
    "I": 0x81,
    "Q": 0x82,
}

_MARKER_TYPES = {
    "B": "BYTE",
    "D": "DWORD",
    "L": "LWORD",
    "W": "WORD",
    "X": "BOOL",
}

_TYPE_SPECS = {
    "BOOL": {"kind": "bool", "size": 1},
    "BYTE": {"kind": "number", "size": 1, "format": ">B"},
    "USINT": {"kind": "number", "size": 1, "format": ">B"},
    "SINT": {"kind": "number", "size": 1, "format": ">b"},
    "CHAR": {"kind": "char", "size": 1},
    "WORD": {"kind": "number", "size": 2, "format": ">H"},
    "UINT": {"kind": "number", "size": 2, "format": ">H"},
    "INT": {"kind": "number", "size": 2, "format": ">h"},
    "DWORD": {"kind": "number", "size": 4, "format": ">I"},
    "UDINT": {"kind": "number", "size": 4, "format": ">I"},
    "DINT": {"kind": "number", "size": 4, "format": ">i"},
    "REAL": {"kind": "number", "size": 4, "format": ">f"},
    "TIME": {"kind": "number", "size": 4, "format": ">i"},
    "TOD": {"kind": "number", "size": 4, "format": ">I"},
    "DATE": {"kind": "number", "size": 2, "format": ">H"},
    "LWORD": {"kind": "number", "size": 8, "format": ">Q"},
    "ULINT": {"kind": "number", "size": 8, "format": ">Q"},
    "LINT": {"kind": "number", "size": 8, "format": ">q"},
    "LREAL": {"kind": "number", "size": 8, "format": ">d"},
}

_TYPE_ALIASES = {
    "TIME_OF_DAY": "TOD",
    "TIMEOFDAY": "TOD",
}

_DB_ADDRESS_PATTERN = re.compile(
    r"^DB(?P<db>[0-9]+)(?:\.|:)(?:DB)?(?P<marker>[XBWDL])?"
    r"(?P<byte>[0-9]+)(?:\.(?P<bit>[0-7]))?$",
    re.IGNORECASE,
)

_AREA_ADDRESS_PATTERN = re.compile(
    r"^(?P<area>[MIQEA])(?P<marker>[XBWDL])?"
    r"(?P<byte>[0-9]+)(?:\.(?P<bit>[0-7]))?$",
    re.IGNORECASE,
)

_VARIABLE_TYPE_PATTERN = re.compile(
    r"^(?P<kind>STRING|WSTRING|BYTES?|BYTE_ARRAY)\[(?P<size>[0-9]+)\]$",
    re.IGNORECASE,
)

_INLINE_SECRET_KEYS = frozenset({
    "api_key",
    "certificate_data",
    "key_data",
    "password",
    "private_key",
    "secret",
    "token",
})


def _is_dict(value):
    return isinstance(value, dict)


def _safe_int(value, default, minimum=None, maximum=None):
    try:
        result = int(value)
    except Exception:
        result = int(default)
    if minimum is not None and result < minimum:
        return int(default)
    if maximum is not None and result > maximum:
        return int(default)
    return result


def _safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(default)


def _config(conn_ctx):
    value = conn_ctx.get("config") if _is_dict(conn_ctx) else None
    return value if _is_dict(value) else {}


def _type_spec(type_name):
    normalized = str(type_name or "").strip().upper().replace(" ", "_")
    normalized = _TYPE_ALIASES.get(normalized, normalized)
    fixed = _TYPE_SPECS.get(normalized)
    if fixed is not None:
        result = dict(fixed)
        result["name"] = normalized
        return result

    match = _VARIABLE_TYPE_PATTERN.fullmatch(normalized)
    if match is None:
        raise ValueError("unsupported_s7_data_type: {}".format(type_name))

    length = int(match.group("size"))
    kind = match.group("kind").upper()
    if length < 1:
        raise ValueError("s7_data_type_length_must_be_positive")

    if kind == "STRING":
        if length > 254:
            raise ValueError("s7_string_length_exceeds_254")
        return {
            "kind": "string",
            "length": length,
            "name": "STRING[{}]".format(length),
            "size": length + 2,
        }
    if kind == "WSTRING":
        if length > 16382:
            raise ValueError("s7_wstring_length_exceeds_16382")
        return {
            "kind": "wstring",
            "length": length,
            "name": "WSTRING[{}]".format(length),
            "size": (length * 2) + 4,
        }
    return {
        "kind": "bytes",
        "length": length,
        "name": "BYTES[{}]".format(length),
        "size": length,
    }


def _resolve_type(type_token, marker):
    selected = str(type_token or "").strip().upper()
    inferred = _MARKER_TYPES.get(str(marker or "").upper(), "")
    if not selected:
        selected = inferred
    if not selected:
        raise ValueError("s7_address_requires_explicit_data_type")
    return _type_spec(selected)


def _validate_marker(marker, type_info, bit_offset):
    normalized = str(marker or "").upper()
    if bit_offset is not None and type_info["name"] != "BOOL":
        raise ValueError("s7_bit_address_requires_bool_type")
    if type_info["name"] == "BOOL" and bit_offset is None:
        raise ValueError("s7_bool_address_requires_bit_offset")
    if normalized == "X" and type_info["name"] != "BOOL":
        raise ValueError("s7_dbx_or_area_x_requires_bool_type")
    expected_size = {"B": 1, "W": 2, "D": 4, "L": 8}.get(normalized)
    if expected_size is not None and int(type_info["size"]) != expected_size:
        raise ValueError("s7_address_marker_type_size_mismatch")


def parse_s7_address(address):
    """Parse and validate one absolute Siemens address into a plain dict."""
    raw = str(address or "").strip()
    if not raw:
        raise ValueError("empty_s7_address")

    if ":" in raw:
        base, type_token = raw.rsplit(":", 1)
    else:
        base, type_token = raw, ""
    compact = re.sub(r"\s+", "", base).upper()

    db_match = _DB_ADDRESS_PATTERN.fullmatch(compact)
    area_match = _AREA_ADDRESS_PATTERN.fullmatch(compact)
    if db_match is None and area_match is None:
        raise ValueError("invalid_s7_address: {}".format(raw))

    if db_match is not None:
        area = "DB"
        db_number = int(db_match.group("db"))
        byte_offset = int(db_match.group("byte"))
        bit_text = db_match.group("bit")
        marker = db_match.group("marker") or ""
    else:
        area = _AREA_ALIASES[area_match.group("area").upper()]
        db_number = 0
        byte_offset = int(area_match.group("byte"))
        bit_text = area_match.group("bit")
        marker = area_match.group("marker") or ""

    bit_offset = int(bit_text) if bit_text is not None else None
    type_info = _resolve_type(type_token, marker)
    _validate_marker(marker, type_info, bit_offset)

    if db_number < 0 or db_number > 65535:
        raise ValueError("s7_db_number_out_of_range")
    if byte_offset < 0:
        raise ValueError("s7_byte_offset_out_of_range")

    result = {
        "address": raw,
        "area": area,
        "bit_offset": bit_offset,
        "byte_offset": byte_offset,
        "data_type": type_info["name"],
        "db_number": db_number,
        "size": int(type_info["size"]),
        "type_info": type_info,
    }
    result["canonical"] = _canonical_address(result)
    return result


def _canonical_address(spec):
    if spec["area"] == "DB":
        base = "DB{}.{}".format(spec["db_number"], spec["byte_offset"])
    else:
        base = "{}{}".format(spec["area"], spec["byte_offset"])
    if spec.get("bit_offset") is not None:
        base += ".{}".format(spec["bit_offset"])
    return "{}:{}".format(base, spec["data_type"])


def _decode_value(spec, raw_data, config=None):
    data = bytes(raw_data or b"")
    info = spec["type_info"]
    if len(data) < int(info["size"]):
        raise ValueError("s7_read_returned_too_few_bytes")

    kind = info["kind"]
    if kind == "bool":
        return bool(data[0] & (1 << int(spec["bit_offset"])))
    if kind == "number":
        return struct.unpack(info["format"], data[:info["size"]])[0]
    if kind == "char":
        encoding = str((config or {}).get("string_encoding") or "latin-1")
        return data[:1].decode(encoding, errors="strict")
    if kind == "bytes":
        return list(data[:info["length"]])
    if kind == "string":
        current_length = int(data[1])
        maximum = min(int(data[0]), int(info["length"]))
        if current_length > maximum:
            raise ValueError("invalid_s7_string_header")
        encoding = str((config or {}).get("string_encoding") or "latin-1")
        return data[2:2 + current_length].decode(encoding, errors="strict")
    if kind == "wstring":
        maximum, current_length = struct.unpack(">HH", data[:4])
        maximum = min(int(maximum), int(info["length"]))
        if current_length > maximum:
            raise ValueError("invalid_s7_wstring_header")
        end = 4 + (int(current_length) * 2)
        return data[4:end].decode("utf-16-be", errors="strict")
    raise ValueError("unsupported_s7_decode_type")


def _encode_value(spec, value, config=None):
    info = spec["type_info"]
    kind = info["kind"]
    if kind == "bool":
        return bytes((1 << int(spec["bit_offset"]),)) if bool(value) else b"\x00"
    if kind == "number":
        return struct.pack(info["format"], value)
    if kind == "char":
        encoding = str((config or {}).get("string_encoding") or "latin-1")
        encoded = str(value).encode(encoding, errors="strict")
        if len(encoded) != 1:
            raise ValueError("s7_char_requires_exactly_one_byte")
        return encoded
    if kind == "bytes":
        encoded = bytes(value)
        if len(encoded) != int(info["length"]):
            raise ValueError("s7_bytes_length_mismatch")
        return encoded
    if kind == "string":
        encoding = str((config or {}).get("string_encoding") or "latin-1")
        encoded = str(value).encode(encoding, errors="strict")
        if len(encoded) > int(info["length"]):
            raise ValueError("s7_string_value_too_long")
        padding = b"\x00" * (int(info["length"]) - len(encoded))
        return bytes((int(info["length"]), len(encoded))) + encoded + padding
    if kind == "wstring":
        text = str(value)
        if len(text) > int(info["length"]):
            raise ValueError("s7_wstring_value_too_long")
        encoded = text.encode("utf-16-be", errors="strict")
        padding = b"\x00" * ((int(info["length"]) - len(text)) * 2)
        return struct.pack(">HH", int(info["length"]), len(text)) + encoded + padding
    raise ValueError("unsupported_s7_encode_type")


def _client_method(client, method_name):
    if _is_dict(client):
        method = client.get(method_name)
    else:
        method = getattr(client, method_name, None)
    return method if callable(method) else None


def _call_client(client, method_name, *args, **kwargs):
    method = _client_method(client, method_name)
    if method is None:
        raise RuntimeError("s7_client_method_unavailable: {}".format(method_name))
    return method(*args, **kwargs)


def _client_connected(client):
    if client is None:
        return False
    for method_name in ("get_connected", "is_connected"):
        method = _client_method(client, method_name)
        if method is not None:
            try:
                return bool(method())
            except Exception:
                return False
    if _is_dict(client):
        return bool(client.get("connected", False))
    try:
        return bool(getattr(client, "connected"))
    except Exception:
        return False


def _find_inline_secret(value, path="comm"):
    if _is_dict(value):
        for key in sorted(value):
            normalized = str(key).strip().lower()
            child_path = "{}.{}".format(path, key)
            if normalized in _INLINE_SECRET_KEYS and value.get(key) not in (None, ""):
                return child_path
            found = _find_inline_secret(value.get(key), child_path)
            if found:
                return found
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            found = _find_inline_secret(item, "{}[{}]".format(path, index))
            if found:
                return found
    return ""


def _security_config(config):
    value = config.get("security") if _is_dict(config) else None
    return value if _is_dict(value) else {}


def _security_mode(config):
    security = _security_config(config)
    mode = str(
        security.get("mode")
        or config.get("security_mode")
        or "classic"
    ).strip().lower()
    if _safe_bool(security.get("use_tls", config.get("use_tls")), False):
        mode = "tls"
    aliases = {
        "iso_on_tcp": "classic",
        "legacy": "classic",
        "legacy_s7": "classic",
        "none": "classic",
        "s7comm": "classic",
        "s7commplus": "secure",
        "s7commplus_tls": "secure",
        "tls": "secure",
    }
    normalized = aliases.get(mode, mode)
    if normalized not in ("classic", "legacy_password", "secure"):
        raise ValueError("unsupported_s7_security_mode")
    return normalized


def _validate_security_config(config):
    secret_path = _find_inline_secret(config)
    if secret_path:
        return "inline_secret_forbidden_at_{}".format(secret_path)

    if _safe_bool(config.get("optimized_block_access"), False):
        return "optimized_block_access_incompatible_with_absolute_s7_addressing"

    try:
        mode = _security_mode(config)
    except ValueError as exc:
        return str(exc)

    security = _security_config(config)
    if mode == "secure":
        verify_peer = _safe_bool(security.get("verify_peer"), True)
        use_system_ca = _safe_bool(security.get("use_system_ca"), False)
        ca_path = str(security.get("tls_ca_path") or "").strip()
        if verify_peer and not ca_path and not use_system_ca:
            return "tls_peer_verification_requires_tls_ca_path_or_system_ca"
        cert_path = str(security.get("tls_cert_path") or "").strip()
        key_path = str(security.get("tls_key_path") or "").strip()
        if bool(cert_path) != bool(key_path):
            return "tls_client_certificate_and_key_must_be_configured_together"
        for label, path in (
            ("tls_ca_path", ca_path),
            ("tls_cert_path", cert_path),
            ("tls_key_path", key_path),
        ):
            if path and not os.path.isfile(path):
                return "{}_not_found".format(label)
    return ""


def _resolve_password(conn_ctx):
    config = _config(conn_ctx)
    security = _security_config(config)
    provider = conn_ctx.get("credential_provider")
    reference = str(security.get("credential_ref") or "").strip()
    if reference:
        if not callable(provider):
            raise RuntimeError("s7_credential_provider_unavailable")
        value = provider(reference)
        if value in (None, ""):
            raise RuntimeError("s7_credential_reference_unresolved")
        return str(value)

    env_name = str(security.get("password_env") or "").strip()
    if env_name:
        value = os.environ.get(env_name)
        if value in (None, ""):
            raise RuntimeError("s7_password_environment_variable_unset")
        return str(value)
    return None


def _load_client_factory(conn_ctx, backend):
    injected_key = "_secure_client_factory" if backend == "secure" else "_classic_client_factory"
    injected = conn_ctx.get(injected_key) or conn_ctx.get("_client_factory")
    if callable(injected):
        return injected

    if backend == "secure":
        try:
            module = importlib.import_module("s7commplus")
        except ImportError as exc:
            raise RuntimeError("s7commplus_tls_backend_unavailable") from exc
        factory = getattr(module, "Client", None)
        if not callable(factory):
            raise RuntimeError("s7commplus_client_factory_unavailable")
        return factory

    try:
        module = importlib.import_module("snap7")
    except ImportError as exc:
        raise RuntimeError(
            "python_snap7_not_installed; install python-snap7==3.1.0"
        ) from exc
    factory = getattr(module, "Client", None)
    if callable(factory):
        return factory
    client_module = getattr(module, "client", None)
    factory = getattr(client_module, "Client", None)
    if not callable(factory):
        raise RuntimeError("python_snap7_client_factory_unavailable")
    return factory


def _set_client_attribute(client, name, value):
    if _is_dict(client):
        if name in client and not callable(client.get(name)):
            client[name] = value
        return
    try:
        setattr(client, name, value)
    except Exception:
        return


def _configure_classic_client(client, config):
    _set_client_attribute(
        client,
        "use_optimizer",
        _safe_bool(config.get("s7_read_optimizer"), True),
    )
    _set_client_attribute(
        client,
        "multi_read_max_gap",
        _safe_int(config.get("s7_multi_read_max_gap"), 5, 0, 65535),
    )
    maximum_parallel = _safe_int(config.get("s7_max_parallel"), 1, 1, 32)
    if "s7_max_parallel" in config:
        _set_client_attribute(client, "max_parallel", maximum_parallel)
    connection_type = config.get("connection_type")
    if connection_type is not None:
        method = _client_method(client, "set_connection_type")
        if method is not None:
            method(_safe_int(connection_type, 1, 1, 10))


def _connect_classic(conn_ctx, client, password):
    config = _config(conn_ctx)
    host = str(conn_ctx.get("host") or "").strip()
    port = _safe_int(conn_ctx.get("port"), 102, 1, 65535)
    rack = _safe_int(config.get("rack"), 0, 0, 31)
    slot = _safe_int(config.get("slot"), 1, 0, 31)
    _configure_classic_client(client, config)
    try:
        _call_client(client, "connect", host, rack, slot, tcp_port=port)
    except TypeError:
        _call_client(client, "connect", host, rack, slot, port)

    if password is not None:
        setter = _client_method(client, "set_session_password")
        if setter is None:
            raise RuntimeError("classic_s7_password_authentication_unsupported")
        setter(password)


def _connect_secure(conn_ctx, client, password):
    config = _config(conn_ctx)
    security = _security_config(config)
    kwargs = {
        "port": _safe_int(conn_ctx.get("port"), 102, 1, 65535),
        "rack": _safe_int(config.get("rack"), 0, 0, 31),
        "slot": _safe_int(config.get("slot"), 1, 0, 31),
        "use_tls": True,
        "tls_cert": security.get("tls_cert_path") or None,
        "tls_key": security.get("tls_key_path") or None,
        "tls_ca": security.get("tls_ca_path") or None,
        "password": password,
    }
    _call_client(client, "connect", str(conn_ctx.get("host") or ""), **kwargs)


def _connect(conn_ctx):
    config = _config(conn_ctx)
    error = _validate_security_config(config)
    status = conn_ctx.setdefault("status", {})
    if error:
        status["connected"] = False
        status["connection_failures"] = int(status.get("connection_failures", 0)) + 1
        status["last_error"] = error
        LOGGER.error("S7 configuration rejected for %s: %s", conn_ctx.get("conn_key"), error)
        return False

    if not str(conn_ctx.get("host") or "").strip():
        status["last_error"] = "s7_host_required"
        status["connection_failures"] = int(status.get("connection_failures", 0)) + 1
        return False

    client = None
    password = None
    try:
        backend = _security_mode(config)
        effective_backend = "secure" if backend == "secure" else "classic"
        factory = _load_client_factory(conn_ctx, effective_backend)
        client = factory()
        if backend in ("legacy_password", "secure"):
            password = _resolve_password(conn_ctx)
        if effective_backend == "secure":
            _connect_secure(conn_ctx, client, password)
        else:
            _connect_classic(conn_ctx, client, password)
        if not _client_connected(client):
            raise RuntimeError("s7_client_did_not_report_connected")

        conn_ctx["client"] = client
        conn_ctx["_s7_backend"] = effective_backend
        status["connected"] = True
        status["last_connect_ts"] = time.time()
        status["connection_failures"] = 0
        status["last_error"] = None
        LOGGER.info(
            "S7 connected: %s:%s rack=%s slot=%s backend=%s",
            conn_ctx.get("host"),
            conn_ctx.get("port", 102),
            config.get("rack", 0),
            config.get("slot", 1),
            effective_backend,
        )
        return True
    except Exception as exc:
        if client is not None:
            try:
                method = _client_method(client, "disconnect")
                if method is not None:
                    method()
            except Exception:
                pass
        conn_ctx["client"] = None
        status["connected"] = False
        status["connection_failures"] = int(status.get("connection_failures", 0)) + 1
        status["last_error"] = str(exc)
        LOGGER.error("S7 connect failed for %s: %s", conn_ctx.get("conn_key"), exc)
        return False
    finally:
        password = None


def _disconnect(conn_ctx):
    client = conn_ctx.get("client")
    if client is not None:
        try:
            method = _client_method(client, "clear_session_password")
            if method is not None:
                method()
        except Exception:
            pass
        try:
            method = _client_method(client, "disconnect")
            if method is not None:
                method()
        except Exception as exc:
            LOGGER.warning("S7 disconnect failed for %s: %s", conn_ctx.get("conn_key"), exc)
    conn_ctx["client"] = None
    status = conn_ctx.setdefault("status", {})
    status["connected"] = False
    status["last_disconnect_ts"] = time.time()


def _is_connected(conn_ctx):
    connected = _client_connected(conn_ctx.get("client"))
    conn_ctx.setdefault("status", {})["connected"] = connected
    return connected


def _normalize_cpu_state(value):
    text = str(value or "UNKNOWN").strip().upper()
    if "RUN" in text:
        return "RUN"
    if "STOP" in text:
        return "STOP"
    return text if text else "UNKNOWN"


def _health_check(conn_ctx):
    started = time.perf_counter()
    if not _is_connected(conn_ctx):
        return {
            "ok": False,
            "connected": False,
            "latency_ms": 0.0,
            "plc_status": "DISCONNECTED",
        }
    try:
        cpu_state = _normalize_cpu_state(
            _call_client(conn_ctx["client"], "get_cpu_state")
        )
        latency = (time.perf_counter() - started) * 1000.0
        return {
            "ok": cpu_state not in ("UNKNOWN", "DISCONNECTED"),
            "connected": True,
            "latency_ms": latency,
            "plc_status": cpu_state,
            "backend": conn_ctx.get("_s7_backend", "classic"),
            "absolute_db_access_requires_non_optimized_blocks": True,
        }
    except Exception as exc:
        return {
            "ok": False,
            "connected": _is_connected(conn_ctx),
            "latency_ms": (time.perf_counter() - started) * 1000.0,
            "plc_status": "UNKNOWN",
            "error": str(exc),
        }


def _resolve_area_value(conn_ctx, area):
    injected = conn_ctx.get("_area_map")
    if _is_dict(injected) and area in injected:
        return injected[area]

    try:
        type_module = importlib.import_module("snap7.type")
        area_enum = getattr(type_module, "Area", None)
    except ImportError:
        area_enum = None
    if area_enum is None:
        try:
            types_module = importlib.import_module("snap7.types")
            area_enum = getattr(types_module, "Areas", None)
        except ImportError:
            area_enum = None
    if area_enum is not None:
        value = getattr(area_enum, _AREA_ENUM_NAMES[area], None)
        if value is not None:
            return value
    return _AREA_NUMBERS[area]


def _secure_area_rid(conn_ctx, area):
    area_rids = _config(conn_ctx).get("area_rids")
    if not _is_dict(area_rids) or area not in area_rids:
        raise RuntimeError("s7commplus_area_rid_required_for_{}".format(area))
    return int(area_rids[area])


def _read_raw_classic(conn_ctx, spec):
    client = conn_ctx["client"]
    start = int(spec["byte_offset"])
    size = int(spec["size"])
    area = spec["area"]
    direct_names = {"DB": "db_read", "M": "mb_read", "I": "eb_read", "Q": "ab_read"}
    method_name = direct_names[area]
    method = _client_method(client, method_name)
    if method is not None:
        if area == "DB":
            return bytes(method(int(spec["db_number"]), start, size))
        return bytes(method(start, size))
    return bytes(_call_client(
        client,
        "read_area",
        _resolve_area_value(conn_ctx, area),
        int(spec["db_number"]),
        start,
        size,
    ))


def _read_raw_secure(conn_ctx, spec):
    client = conn_ctx["client"]
    if spec["area"] == "DB":
        return bytes(_call_client(
            client,
            "db_read",
            int(spec["db_number"]),
            int(spec["byte_offset"]),
            int(spec["size"]),
        ))
    return bytes(_call_client(
        client,
        "read_area",
        _secure_area_rid(conn_ctx, spec["area"]),
        int(spec["byte_offset"]),
        int(spec["size"]),
    ))


def _read_raw(conn_ctx, spec):
    if conn_ctx.get("_s7_backend") == "secure":
        return _read_raw_secure(conn_ctx, spec)
    return _read_raw_classic(conn_ctx, spec)


def _write_raw_classic(conn_ctx, spec, raw_data):
    client = conn_ctx["client"]
    area = spec["area"]
    if area == "I":
        raise RuntimeError("s7_input_area_is_read_only")
    data = bytearray(raw_data)
    start = int(spec["byte_offset"])
    direct_names = {"DB": "db_write", "M": "mb_write", "Q": "ab_write"}
    method_name = direct_names[area]
    method = _client_method(client, method_name)
    if method is not None:
        if area == "DB":
            return method(int(spec["db_number"]), start, data)
        try:
            return method(start, len(data), data)
        except TypeError:
            return method(start, data)
    return _call_client(
        client,
        "write_area",
        _resolve_area_value(conn_ctx, area),
        int(spec["db_number"]),
        start,
        data,
    )


def _write_raw_secure(conn_ctx, spec, raw_data):
    client = conn_ctx["client"]
    if spec["area"] == "I":
        raise RuntimeError("s7_input_area_is_read_only")
    data = bytes(raw_data)
    if spec["area"] == "DB":
        return _call_client(
            client,
            "db_write",
            int(spec["db_number"]),
            int(spec["byte_offset"]),
            data,
        )
    return _call_client(
        client,
        "write_area",
        _secure_area_rid(conn_ctx, spec["area"]),
        int(spec["byte_offset"]),
        data,
    )


def _write_raw(conn_ctx, spec, raw_data):
    if conn_ctx.get("_s7_backend") == "secure":
        return _write_raw_secure(conn_ctx, spec, raw_data)
    return _write_raw_classic(conn_ctx, spec, raw_data)


def _read_value(conn_ctx, spec):
    return _decode_value(spec, _read_raw(conn_ctx, spec), _config(conn_ctx))


def _write_value(conn_ctx, spec, value):
    if spec["data_type"] == "BOOL":
        current = bytearray(_read_raw(conn_ctx, spec))
        mask = 1 << int(spec["bit_offset"])
        if bool(value):
            current[0] |= mask
        else:
            current[0] &= 0xFF ^ mask
        _write_raw(conn_ctx, spec, current)
        return bool(value)
    encoded = _encode_value(spec, value, _config(conn_ctx))
    _write_raw(conn_ctx, spec, encoded)
    return value


def _batch_limit(conn_ctx):
    return _safe_int(
        _config(conn_ctx).get("s7_multi_var_limit"),
        S7_MAX_MULTI_VARS,
        1,
        S7_MAX_MULTI_VARS,
    )


def _classic_read_multi_raw(conn_ctx, specs):
    client = conn_ctx["client"]
    method = _client_method(client, "read_multi_vars")
    if method is None:
        raise RuntimeError("s7_read_multi_vars_unavailable")
    items = []
    for spec in specs:
        items.append({
            "area": _resolve_area_value(conn_ctx, spec["area"]),
            "db_number": int(spec["db_number"]),
            "start": int(spec["byte_offset"]),
            "size": int(spec["size"]),
        })
    response = method(items)
    if isinstance(response, tuple) and len(response) == 2:
        result_code, payload = response
    else:
        result_code, payload = 0, response
    if int(result_code or 0) != 0:
        raise RuntimeError("s7_read_multi_vars_failed_code_{}".format(result_code))
    if not isinstance(payload, (list, tuple)) or len(payload) != len(specs):
        raise RuntimeError("s7_read_multi_vars_invalid_payload")
    return [bytes(item) for item in payload]


def _secure_read_multi_raw(conn_ctx, specs):
    if not all(spec["area"] == "DB" for spec in specs):
        raise RuntimeError("s7commplus_mixed_area_batch_requires_sequential_fallback")
    method = _client_method(conn_ctx["client"], "db_read_multi")
    if method is None:
        raise RuntimeError("s7commplus_db_read_multi_unavailable")
    items = [
        (
            int(spec["db_number"]),
            int(spec["byte_offset"]),
            int(spec["size"]),
        )
        for spec in specs
    ]
    payload = method(items)
    if not isinstance(payload, (list, tuple)) or len(payload) != len(specs):
        raise RuntimeError("s7commplus_db_read_multi_invalid_payload")
    return [bytes(item) for item in payload]


def _read_multi_raw_chunk(conn_ctx, specs):
    if len(specs) == 1:
        return [_read_raw(conn_ctx, specs[0])]
    if conn_ctx.get("_s7_backend") == "secure":
        return _secure_read_multi_raw(conn_ctx, specs)
    return _classic_read_multi_raw(conn_ctx, specs)


def _read_multi_values(conn_ctx, addresses):
    specs = [parse_s7_address(address) for address in addresses]
    if not specs:
        return []
    limit = _batch_limit(conn_ctx)
    raw_values = []
    fallback = _safe_bool(
        _config(conn_ctx).get("s7_batch_fallback_sequential"),
        True,
    )
    for start in range(0, len(specs), limit):
        chunk = specs[start:start + limit]
        try:
            raw_values.extend(_read_multi_raw_chunk(conn_ctx, chunk))
        except Exception as exc:
            if not fallback:
                raise
            LOGGER.warning(
                "S7 batch read fallback for %s (%d item(s)): %s",
                conn_ctx.get("conn_key"),
                len(chunk),
                exc,
            )
            raw_values.extend(_read_raw(conn_ctx, spec) for spec in chunk)
    return [
        _decode_value(spec, raw, _config(conn_ctx))
        for spec, raw in zip(specs, raw_values)
    ]


def _addresses_from_request(request):
    addresses = request.get("addresses")
    if addresses is None:
        meta = request.get("meta")
        if _is_dict(meta):
            addresses = meta.get("addresses")
    if addresses is None and isinstance(request.get("address"), (list, tuple)):
        addresses = request.get("address")
    if not isinstance(addresses, (list, tuple)):
        return []
    result = []
    for item in addresses:
        address = item.get("address") if _is_dict(item) else item
        if address not in (None, ""):
            result.append(str(address))
    return result


def _advance_spec(spec, index):
    result = dict(spec)
    result["type_info"] = dict(spec["type_info"])
    if spec["data_type"] == "BOOL":
        absolute_bit = int(spec["bit_offset"]) + int(index)
        result["byte_offset"] = int(spec["byte_offset"]) + (absolute_bit // 8)
        result["bit_offset"] = absolute_bit % 8
    else:
        result["byte_offset"] = int(spec["byte_offset"]) + (
            int(index) * int(spec["size"])
        )
    result["canonical"] = _canonical_address(result)
    return result


def _contiguous_addresses(address, count):
    spec = parse_s7_address(address)
    return [
        _advance_spec(spec, index)["canonical"]
        for index in range(max(0, int(count)))
    ]


def _write_block(conn_ctx, request):
    values = request.get("value")
    if not isinstance(values, (list, tuple)):
        raise ValueError("s7_write_block_requires_value_list")
    addresses = _addresses_from_request(request)
    if not addresses:
        address = request.get("address", request.get("start"))
        addresses = _contiguous_addresses(address, len(values))
    if len(addresses) != len(values):
        raise ValueError("s7_write_block_address_value_count_mismatch")
    for address, value in zip(addresses, values):
        _write_value(conn_ctx, parse_s7_address(address), value)
    return len(values)


def _update_metrics(conn_ctx, operation, latency_ms, item_count=1):
    metrics = conn_ctx.setdefault("metrics", {})
    is_read = operation.startswith("read") or operation in ("group_read", "plc_status")
    counter = "reads_total" if is_read else "writes_total"
    average = "latency_read_avg" if is_read else "latency_write_avg"
    previous_count = int(metrics.get(counter, 0))
    increment = max(1, int(item_count))
    new_count = previous_count + increment
    previous_average = float(metrics.get(average, 0.0))
    metrics[counter] = new_count
    metrics[average] = (
        (previous_average * previous_count) + (float(latency_ms) * increment)
    ) / float(new_count)
    conn_ctx.setdefault("status", {})["last_io_ts"] = time.time()


def _execute(conn_ctx, request):
    started = time.perf_counter()
    if not _is_connected(conn_ctx):
        return make_result_error("not_connected", request=request)
    if not _is_dict(request):
        return make_result_error("invalid_request", request=request)

    operation = str(request.get("op") or "").strip().lower()
    item_count = 1
    try:
        if operation in ("read_multi", "group_read"):
            addresses = _addresses_from_request(request)
            if not addresses:
                raise ValueError("s7_read_multi_requires_addresses")
            data = _read_multi_values(conn_ctx, addresses)
            item_count = len(addresses)
        elif operation == "read_single":
            address = request.get("address", request.get("start"))
            data = _read_value(conn_ctx, parse_s7_address(address))
        elif operation == "read_block":
            addresses = _addresses_from_request(request)
            if not addresses:
                address = request.get("start", request.get("address"))
                addresses = _contiguous_addresses(address, int(request.get("count", 1)))
            data = _read_multi_values(conn_ctx, addresses)
            item_count = len(addresses)
        elif operation == "write_single":
            address = request.get("address", request.get("start"))
            data = _write_value(
                conn_ctx,
                parse_s7_address(address),
                request.get("value"),
            )
        elif operation == "write_block":
            data = _write_block(conn_ctx, request)
            item_count = int(data)
        elif operation == "plc_status":
            health = _health_check(conn_ctx)
            if not health.get("ok"):
                raise RuntimeError(health.get("error") or "plc_health_check_failed")
            data = health
        else:
            raise ValueError("unknown_s7_op: {}".format(operation))

        latency = (time.perf_counter() - started) * 1000.0
        _update_metrics(conn_ctx, operation, latency, item_count)
        return make_result_ok(data, latency_ms=latency, request=request)
    except Exception as exc:
        latency = (time.perf_counter() - started) * 1000.0
        metrics = conn_ctx.setdefault("metrics", {})
        metrics["errors_total"] = int(metrics.get("errors_total", 0)) + 1
        return make_result_error(str(exc), latency_ms=latency, request=request)


def create_tia_plc_adapter():
    """Return the exact functional adapter contract for ``plc_s7``."""
    return {
        "connect": _connect,
        "disconnect": _disconnect,
        "is_connected": _is_connected,
        "health_check": _health_check,
        "execute": _execute,
        "protocol_type": "plc_s7",
        "capabilities": frozenset({
            "read_block",
            "read_multi",
            "read_single",
            "write_block",
            "write_single",
            "plc_status",
            "typed_absolute_addressing",
        }),
    }
