# -*- coding: utf-8 -*-

# src/library/_lightweight_mqtt_client.py


"""
 =============================================================================
 _LIGHTWEIGHT_MQTT_CLIENT
 =============================================================================

 What it is:
     A small functional MQTT 3.1.1 client implemented with the Python standard
     library. It supports TLS, optional mTLS, QoS 0/1 publish, subscriptions,
     keepalive, reconnect and QoS 1 retransmission.

 Why it is needed:
     Runtime nodes need an MQTT transport without making the automation core
     depend on a large framework or on module-global client state.

 How it works:
     One transport thread exclusively owns the socket. Producers enqueue
     publish/subscribe commands. Incoming packets are decoded and forwarded to
     a non-blocking callback supplied by the MQTT integration interface.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import logging
import os
import select
import socket
import ssl
import struct
import threading
import time

from functools import partial
from queue import Empty, Full, Queue


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "lightweight_mqtt_client",
    "library_version": "1.0.0",
    "protocol": "MQTT",
    "protocol_level": 4,
    "options": {
        "host": "localhost",
        "port": 8883,
        "client_id": "automation-node",
        "keepalive_s": 30,
        "clean_session": True,
        "connect_timeout_s": 5.0,
        "socket_timeout_s": 2.0,
        "reconnect_min_s": 1.0,
        "reconnect_max_s": 30.0,
        "outbound_queue_maxsize": 4096,
        "max_packet_size": 4 * 1024 * 1024,
        "allow_insecure_transport": False,
    },
}

PACKET_CONNECT = 1
PACKET_CONNACK = 2
PACKET_PUBLISH = 3
PACKET_PUBACK = 4
PACKET_SUBSCRIBE = 8
PACKET_SUBACK = 9
PACKET_PINGREQ = 12
PACKET_PINGRESP = 13
PACKET_DISCONNECT = 14


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _safe_int(value, default, minimum=0):
    try:
        result = int(value)
    except Exception:
        return int(default)
    return result if result >= minimum else int(default)


def _safe_float(value, default, minimum=0.0):
    try:
        result = float(value)
    except Exception:
        return float(default)
    return result if result >= minimum else float(default)


def _safe_bool(value, default):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
    return bool(default)


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _now():
    return time.monotonic()


def _bytes(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode("utf-8")


# =============================================================================
# MQTT PACKET CODEC
# =============================================================================

def encode_remaining_length(value):
    remaining = _safe_int(value, -1)
    if remaining < 0 or remaining > 268435455:
        raise ValueError("MQTT remaining length is out of range")
    encoded = bytearray()
    while True:
        digit = remaining % 128
        remaining //= 128
        if remaining:
            digit |= 0x80
        encoded.append(digit)
        if not remaining:
            return bytes(encoded)


def encode_utf8(value):
    encoded = _bytes(value)
    if len(encoded) > 65535:
        raise ValueError("MQTT UTF-8 field exceeds 65535 bytes")
    return struct.pack("!H", len(encoded)) + encoded


def _decode_utf8(payload, offset):
    if offset + 2 > len(payload):
        raise ValueError("truncated MQTT UTF-8 length")
    length = struct.unpack("!H", payload[offset:offset + 2])[0]
    start = offset + 2
    end = start + length
    if end > len(payload):
        raise ValueError("truncated MQTT UTF-8 value")
    return payload[start:end].decode("utf-8"), end


def _fixed_packet(first_byte, payload):
    body = _bytes(payload)
    return bytes((int(first_byte),)) + encode_remaining_length(len(body)) + body


def build_connect_packet(options):
    flags = 0
    payload = bytearray()
    clean_session = _safe_bool(options.get("clean_session"), True)
    username = options.get("username")
    password = options.get("password")
    will = options.get("will") if _is_dict(options.get("will")) else None
    if clean_session:
        flags |= 0x02
    if will is not None:
        will_qos = _safe_int(will.get("qos"), 1)
        if will_qos not in (0, 1):
            raise ValueError("only MQTT will QoS 0/1 is supported")
        flags |= 0x04 | (will_qos << 3)
        if _safe_bool(will.get("retain"), True):
            flags |= 0x20
    if username is not None:
        flags |= 0x80
    if password is not None:
        flags |= 0x40
    payload.extend(encode_utf8(options.get("client_id") or ""))
    if will is not None:
        payload.extend(encode_utf8(will.get("topic") or ""))
        payload.extend(encode_utf8(will.get("payload") or b""))
    if username is not None:
        payload.extend(encode_utf8(username))
    if password is not None:
        payload.extend(encode_utf8(password))
    variable = encode_utf8("MQTT")
    variable += bytes((CONTEXT["protocol_level"], flags))
    variable += struct.pack("!H", _safe_int(options.get("keepalive_s"), 30))
    return _fixed_packet(0x10, variable + bytes(payload))


def build_publish_packet(topic, payload, **kwargs):
    qos = _safe_int(kwargs.get("qos"), 0)
    if qos not in (0, 1):
        raise ValueError("only MQTT publish QoS 0/1 is supported")
    flags = (qos << 1)
    if _safe_bool(kwargs.get("retain"), False):
        flags |= 0x01
    if _safe_bool(kwargs.get("dup"), False):
        flags |= 0x08
    body = bytearray(encode_utf8(topic))
    if qos == 1:
        packet_id = _safe_int(kwargs.get("packet_id"), 0)
        if packet_id < 1 or packet_id > 65535:
            raise ValueError("QoS 1 publish requires a packet id")
        body.extend(struct.pack("!H", packet_id))
    body.extend(_bytes(payload))
    return _fixed_packet(0x30 | flags, bytes(body))


def build_subscribe_packet(packet_id, subscriptions):
    body = bytearray(struct.pack("!H", int(packet_id)))
    for topic, qos in subscriptions:
        normalized_qos = _safe_int(qos, 0)
        if normalized_qos not in (0, 1):
            raise ValueError("only MQTT subscription QoS 0/1 is supported")
        body.extend(encode_utf8(topic))
        body.append(normalized_qos)
    return _fixed_packet(0x82, bytes(body))


def parse_publish_packet(first_byte, payload):
    topic, offset = _decode_utf8(payload, 0)
    qos = (int(first_byte) >> 1) & 0x03
    packet_id = None
    if qos == 1:
        if offset + 2 > len(payload):
            raise ValueError("truncated MQTT publish packet id")
        packet_id = struct.unpack("!H", payload[offset:offset + 2])[0]
        offset += 2
    if qos not in (0, 1):
        raise ValueError("incoming MQTT QoS 2 is not supported")
    return {
        "topic": topic,
        "payload": bytes(payload[offset:]),
        "qos": qos,
        "retain": bool(first_byte & 0x01),
        "dup": bool(first_byte & 0x08),
        "packet_id": packet_id,
    }


# =============================================================================
# SOCKET AND TLS
# =============================================================================

def _recv_exact(sock, length):
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise ConnectionError("MQTT socket closed")
        chunks.extend(chunk)
    return bytes(chunks)


def _read_remaining_length(sock):
    multiplier = 1
    value = 0
    for _position in range(4):
        digit = _recv_exact(sock, 1)[0]
        value += (digit & 0x7F) * multiplier
        if not digit & 0x80:
            return value
        multiplier *= 128
    raise ValueError("malformed MQTT remaining length")


def read_mqtt_packet(sock, max_packet_size=4194304):
    first_byte = _recv_exact(sock, 1)[0]
    remaining = _read_remaining_length(sock)
    if remaining > int(max_packet_size):
        raise ValueError("MQTT packet exceeds configured maximum")
    return first_byte, _recv_exact(sock, remaining)


def build_ssl_context(tls_options):
    tls = tls_options if _is_dict(tls_options) else {}
    cafile = tls.get("ca_file") or None
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    if _safe_bool(tls.get("insecure_skip_verify"), False):
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    cert_file = tls.get("cert_file")
    key_file = tls.get("key_file")
    if cert_file:
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    ciphers = tls.get("ciphers")
    if ciphers:
        context.set_ciphers(str(ciphers))
    return context


def _open_socket(context):
    options = context["options"]
    raw = socket.create_connection(
        (options["host"], options["port"]),
        timeout=options["connect_timeout_s"],
    )
    raw.settimeout(options["socket_timeout_s"])
    tls = options["tls"]
    if _safe_bool(tls.get("enabled"), True):
        ssl_context = build_ssl_context(tls)
        server_name = tls.get("server_hostname") or options["host"]
        wrapped = ssl_context.wrap_socket(raw, server_hostname=server_name)
        wrapped.settimeout(options["socket_timeout_s"])
        return wrapped
    if not options["allow_insecure_transport"]:
        raw.close()
        raise ValueError("unencrypted MQTT transport is disabled")
    return raw


# =============================================================================
# CONTEXT AND PUBLIC QUEUE API
# =============================================================================

def _resolve_options(kwargs):
    defaults = CONTEXT["options"]
    tls = copy.deepcopy(kwargs.get("tls") or {})
    options = {
        "host": str(kwargs.get("host") or defaults["host"]),
        "port": _safe_int(kwargs.get("port"), defaults["port"], 1),
        "client_id": str(kwargs.get("client_id") or defaults["client_id"]),
        "keepalive_s": _safe_int(kwargs.get("keepalive_s"), defaults["keepalive_s"], 1),
        "clean_session": _safe_bool(kwargs.get("clean_session"), True),
        "connect_timeout_s": _safe_float(
            kwargs.get("connect_timeout_s"), defaults["connect_timeout_s"], 0.1),
        "socket_timeout_s": _safe_float(
            kwargs.get("socket_timeout_s"), defaults["socket_timeout_s"], 0.1),
        "reconnect_min_s": _safe_float(
            kwargs.get("reconnect_min_s"), defaults["reconnect_min_s"], 0.05),
        "reconnect_max_s": _safe_float(
            kwargs.get("reconnect_max_s"), defaults["reconnect_max_s"], 0.05),
        "outbound_queue_maxsize": _safe_int(
            kwargs.get("outbound_queue_maxsize"),
            defaults["outbound_queue_maxsize"],
        ),
        "max_packet_size": _safe_int(
            kwargs.get("max_packet_size"), defaults["max_packet_size"], 1024),
        "allow_insecure_transport": _safe_bool(
            kwargs.get("allow_insecure_transport"), False),
        "username": kwargs.get("username"),
        "password": kwargs.get("password"),
        "will": copy.deepcopy(kwargs.get("will")),
        "tls": tls,
    }
    if options["reconnect_max_s"] < options["reconnect_min_s"]:
        options["reconnect_max_s"] = options["reconnect_min_s"]
    return options


def create_mqtt_client(**kwargs):
    options = _resolve_options(kwargs)
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "options": options,
        "lock": threading.RLock(),
        "stop_event": threading.Event(),
        "connected_event": threading.Event(),
        "thread": None,
        "socket": None,
        "state": "created",
        "outbound_queue": Queue(maxsize=options["outbound_queue_maxsize"]),
        "subscriptions": {},
        "pending_qos1": {},
        "packet_id": 0,
        "on_message": kwargs.get("on_message"),
        "on_connect": kwargs.get("on_connect"),
        "on_disconnect": kwargs.get("on_disconnect"),
        "last_io_s": _now(),
        "last_ping_s": None,
        "last_error": None,
        "statistics": {
            "connects": 0,
            "disconnects": 0,
            "reconnects": 0,
            "publish_queued": 0,
            "publish_sent": 0,
            "publish_received": 0,
            "puback_received": 0,
            "subscribe_sent": 0,
            "queue_rejected": 0,
            "protocol_errors": 0,
        },
    }
    for subscription in kwargs.get("subscriptions") or []:
        if _is_dict(subscription) and subscription.get("topic"):
            context["subscriptions"][str(subscription["topic"])] = _safe_int(
                subscription.get("qos"), 1)
    return _build_api(context)


def _get_context(api_or_context):
    if _is_dict(api_or_context) and _is_dict(api_or_context.get("context")):
        return api_or_context["context"]
    return api_or_context


def _queue_command(context, command):
    try:
        context["outbound_queue"].put_nowait(command)
        return True
    except Full:
        with context["lock"]:
            context["statistics"]["queue_rejected"] += 1
        return False


def mqtt_publish(api_or_context, topic, payload, **kwargs):
    context = _get_context(api_or_context)
    qos = _safe_int(kwargs.get("qos"), 0)
    if qos not in (0, 1) or not str(topic):
        return False
    command = {
        "kind": "publish",
        "topic": str(topic),
        "payload": _bytes(payload),
        "qos": qos,
        "retain": _safe_bool(kwargs.get("retain"), False),
    }
    written = _queue_command(context, command)
    if written:
        with context["lock"]:
            context["statistics"]["publish_queued"] += 1
    return written


def mqtt_subscribe(api_or_context, topic, qos=1):
    context = _get_context(api_or_context)
    normalized_qos = _safe_int(qos, 1)
    if not str(topic) or normalized_qos not in (0, 1):
        return False
    with context["lock"]:
        context["subscriptions"][str(topic)] = normalized_qos
    if context["connected_event"].is_set():
        return _queue_command(context, {
            "kind": "subscribe",
            "subscriptions": ((str(topic), normalized_qos),),
        })
    return True


# =============================================================================
# TRANSPORT THREAD
# =============================================================================

def _next_packet_id(context):
    with context["lock"]:
        for _attempt in range(65535):
            context["packet_id"] = (context["packet_id"] % 65535) + 1
            if context["packet_id"] not in context["pending_qos1"]:
                return context["packet_id"]
    raise RuntimeError("all MQTT packet identifiers are in use")


def _send_packet(context, packet):
    sock = context.get("socket")
    if sock is None:
        raise ConnectionError("MQTT socket is unavailable")
    sock.sendall(packet)
    context["last_io_s"] = _now()


def _send_publish(context, command):
    qos = command["qos"]
    packet_id = _next_packet_id(context) if qos == 1 else None
    packet = build_publish_packet(
        command["topic"],
        command["payload"],
        qos=qos,
        retain=command["retain"],
        packet_id=packet_id,
    )
    _send_packet(context, packet)
    if qos == 1:
        with context["lock"]:
            context["pending_qos1"][packet_id] = packet
    with context["lock"]:
        context["statistics"]["publish_sent"] += 1


def _send_subscribe(context, subscriptions):
    packet_id = _next_packet_id(context)
    _send_packet(context, build_subscribe_packet(packet_id, subscriptions))
    with context["lock"]:
        context["statistics"]["subscribe_sent"] += 1


def _drain_commands(context, limit=64):
    processed = 0
    while processed < int(limit):
        try:
            command = context["outbound_queue"].get_nowait()
        except Empty:
            break
        try:
            if command.get("kind") == "publish":
                _send_publish(context, command)
            elif command.get("kind") == "subscribe":
                _send_subscribe(context, command.get("subscriptions") or ())
        except Exception:
            try:
                context["outbound_queue"].put_nowait(command)
            except Full:
                with context["lock"]:
                    context["statistics"]["queue_rejected"] += 1
            raise
        finally:
            context["outbound_queue"].task_done()
        processed += 1
    return processed


def _notify_callback(context, name, *args):
    callback = context.get(name)
    if not callable(callback):
        return True
    try:
        return callback(*args)
    except Exception:
        LOGGER.exception("MQTT callback failed: %s", name)
        return False


def _handle_publish(context, first_byte, payload):
    message = parse_publish_packet(first_byte, payload)
    with context["lock"]:
        context["statistics"]["publish_received"] += 1
    accepted = _notify_callback(context, "on_message", message)
    if message["qos"] == 1 and accepted is not False:
        _send_packet(
            context,
            _fixed_packet(0x40, struct.pack("!H", message["packet_id"])),
        )


def _handle_puback(context, payload):
    if len(payload) != 2:
        return False
    packet_id = struct.unpack("!H", payload)[0]
    with context["lock"]:
        context["pending_qos1"].pop(packet_id, None)
        context["statistics"]["puback_received"] += 1
    return True


def _handle_packet(context, first_byte, payload):
    packet_type = int(first_byte) >> 4
    context["last_io_s"] = _now()
    if packet_type == PACKET_PUBLISH:
        _handle_publish(context, first_byte, payload)
        return
    if packet_type == PACKET_PUBACK and _handle_puback(context, payload):
        return
    if packet_type == PACKET_PINGRESP:
        context["last_ping_s"] = None
        return
    if packet_type == PACKET_SUBACK:
        return
    with context["lock"]:
        context["statistics"]["protocol_errors"] += 1


def _resend_pending(context):
    with context["lock"]:
        packets = tuple(context["pending_qos1"].values())
    for packet in packets:
        duplicated = bytes((packet[0] | 0x08,)) + packet[1:]
        _send_packet(context, duplicated)


def _establish_connection(context):
    sock = _open_socket(context)
    context["socket"] = sock
    _send_packet(context, build_connect_packet(context["options"]))
    first_byte, payload = read_mqtt_packet(
        sock, context["options"]["max_packet_size"])
    if first_byte >> 4 != PACKET_CONNACK or len(payload) != 2:
        raise ConnectionError("MQTT broker returned no valid CONNACK")
    if payload[1] != 0:
        raise ConnectionError("MQTT CONNACK rejected with code {}".format(payload[1]))
    context["connected_event"].set()
    context["state"] = "connected"
    context["last_ping_s"] = None
    with context["lock"]:
        context["statistics"]["connects"] += 1
    subscriptions = tuple(sorted(context["subscriptions"].items()))
    if subscriptions:
        _send_subscribe(context, subscriptions)
    _resend_pending(context)
    _notify_callback(context, "on_connect", get_mqtt_snapshot(context))


def _close_socket(context, reason):
    sock = context.get("socket")
    was_active = sock is not None or context["connected_event"].is_set()
    context["socket"] = None
    context["connected_event"].clear()
    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass
    if was_active:
        with context["lock"]:
            context["statistics"]["disconnects"] += 1
        _notify_callback(context, "on_disconnect", str(reason))


def _keepalive(context):
    now = _now()
    keepalive = context["options"]["keepalive_s"]
    if context["last_ping_s"] is not None:
        if now - context["last_ping_s"] > keepalive:
            raise TimeoutError("MQTT PINGRESP timeout")
        return
    if now - context["last_io_s"] >= keepalive * 0.75:
        _send_packet(context, b"\xc0\x00")
        context["last_ping_s"] = now


def _connected_cycle(context):
    _drain_commands(context)
    sock = context["socket"]
    readable, _writable, _errors = select.select((sock,), (), (), 0.05)
    if readable:
        first_byte, payload = read_mqtt_packet(
            sock, context["options"]["max_packet_size"])
        _handle_packet(context, first_byte, payload)
    _keepalive(context)


def _transport_loop(context):
    delay = context["options"]["reconnect_min_s"]
    context["state"] = "starting"
    while not context["stop_event"].is_set():
        try:
            _establish_connection(context)
            delay = context["options"]["reconnect_min_s"]
            while not context["stop_event"].is_set():
                _connected_cycle(context)
        except Exception as exc:
            context["last_error"] = str(exc)
            context["state"] = "disconnected"
            _close_socket(context, exc)
            if context["stop_event"].wait(delay):
                break
            with context["lock"]:
                context["statistics"]["reconnects"] += 1
            delay = min(context["options"]["reconnect_max_s"], delay * 2.0)
    if context["connected_event"].is_set():
        try:
            _send_packet(context, b"\xe0\x00")
        except Exception:
            pass
    _close_socket(context, "shutdown")
    context["state"] = "stopped"


# =============================================================================
# PUBLIC LIFECYCLE AND DIAGNOSTICS
# =============================================================================

def start_mqtt_client(api_or_context):
    context = _get_context(api_or_context)
    thread = context.get("thread")
    if _thread_alive(thread):
        return thread
    context["stop_event"].clear()
    thread = threading.Thread(
        target=partial(_transport_loop, context),
        name="MQTT-Transport-" + context["options"]["client_id"],
        daemon=False,
    )
    context["thread"] = thread
    thread.start()
    return thread


def stop_mqtt_client(api_or_context, **kwargs):
    context = _get_context(api_or_context)
    context["stop_event"].set()
    sock = context.get("socket")
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
    thread = context.get("thread")
    timeout = _safe_float(kwargs.get("join_timeout_s"), 5.0, 0.1)
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=timeout)
    return get_mqtt_snapshot(context)


def wait_mqtt_connected(api_or_context, timeout_s=5.0):
    context = _get_context(api_or_context)
    return context["connected_event"].wait(_safe_float(timeout_s, 5.0, 0.0))


def get_mqtt_snapshot(api_or_context):
    context = _get_context(api_or_context)
    with context["lock"]:
        return {
            "module": context["module"],
            "version": context["version"],
            "state": context["state"],
            "connected": context["connected_event"].is_set(),
            "thread_alive": _thread_alive(context.get("thread")),
            "host": context["options"]["host"],
            "port": context["options"]["port"],
            "client_id": context["options"]["client_id"],
            "tls_enabled": _safe_bool(context["options"]["tls"].get("enabled"), True),
            "subscriptions": dict(context["subscriptions"]),
            "outbound_queue_depth": context["outbound_queue"].qsize(),
            "pending_qos1": len(context["pending_qos1"]),
            "statistics": dict(context["statistics"]),
            "last_error": context["last_error"],
        }


def _build_api(context):
    return {
        "context": context,
        "start": partial(start_mqtt_client, context),
        "stop": partial(stop_mqtt_client, context),
        "publish": partial(mqtt_publish, context),
        "subscribe": partial(mqtt_subscribe, context),
        "wait_connected": partial(wait_mqtt_connected, context),
        "snapshot": partial(get_mqtt_snapshot, context),
    }
