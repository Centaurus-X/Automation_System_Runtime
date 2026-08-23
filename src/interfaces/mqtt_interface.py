# -*- coding: utf-8 -*-

# src/interfaces/mqtt_interface.py


"""
 =============================================================================
 MQTT_INTERFACE
 =============================================================================

 Runtime-facing MQTT integration boundary. It exposes asynchronous outbound
 event publication and a dedicated inbound command dispatcher. Configuration
 writes are never performed here: commands invoke the SEM capabilities and an
 ACK is published only after atomic commit and synchronous rollout completed.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import hashlib
import hmac
import json
import logging
import os
import ssl
import stat
import threading
import time
import uuid

from functools import partial
from queue import Empty, Full, Queue

from src.library._lightweight_mqtt_client import create_mqtt_client
from src.library import _generic_type_library as type_library


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "mqtt_interface",
    "library_version": "1.1.0",
    "protocol": "automation.mqtt.v1",
    "options": {
        "enabled": False,
        "required": False,
        "startup_connection_required": None,
        "site_id": "site-main",
        "node_id": "node-unknown",
        "topic_prefix": "automation/v1",
        "command_queue_maxsize": 256,
        "dedupe_limit": 2048,
        "heartbeat_interval_s": 15.0,
        "flush_timeout_s": 2.0,
        "connect_wait_s": 5.0,
        "max_command_age_s": 300.0,
        "max_clock_skew_s": 30.0,
        "allow_retained_commands": False,
    },
}


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


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


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _mapping(parent, key):
    value = parent.get(key) if _is_dict(parent) else None
    return value if _is_dict(value) else {}


def _canonical_bytes(value):
    text = json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, default=str,
    )
    return text.encode("utf-8")


def _json_loads(payload):
    value = json.loads(bytes(payload).decode("utf-8"))
    if not _is_dict(value):
        raise ValueError("MQTT envelope must be a JSON object")
    return value


def _topic_token(value, fallback):
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ ."
    text = "".join(character if character in allowed else "-"
                   for character in str(value or fallback))
    return text.strip().replace(" ", "-") or str(fallback)


def _resolve_secret(value, env_name):
    if env_name:
        return os.environ.get(str(env_name))
    return value


def _event_type(event):
    if not _is_dict(event):
        return ""
    return str(event.get("event_type") or event.get("type") or "").upper()


# =============================================================================
# OPTIONS AND TOPICS
# =============================================================================

def _resolved_options(options):
    defaults = CONTEXT["options"]
    supplied = copy.deepcopy(options) if _is_dict(options) else {}
    resolved = dict(supplied)
    for key, default in defaults.items():
        resolved.setdefault(key, default)
    resolved["enabled"] = _safe_bool(resolved.get("enabled"), False)
    resolved["required"] = _safe_bool(resolved.get("required"), False)
    startup_required = resolved.get("startup_connection_required")
    if startup_required is None:
        startup_required = resolved["required"]
    resolved["startup_connection_required"] = _safe_bool(
        startup_required, resolved["required"])
    resolved["site_id"] = _topic_token(resolved.get("site_id"), "site-main")
    resolved["node_id"] = _topic_token(resolved.get("node_id"), "node-unknown")
    resolved["topic_prefix"] = str(
        resolved.get("topic_prefix") or defaults["topic_prefix"]).strip("/")
    resolved["command_queue_maxsize"] = _safe_int(
        resolved.get("command_queue_maxsize"), defaults["command_queue_maxsize"], 1)
    resolved["dedupe_limit"] = _safe_int(
        resolved.get("dedupe_limit"), defaults["dedupe_limit"], 1)
    for key in ("heartbeat_interval_s", "flush_timeout_s", "connect_wait_s"):
        resolved[key] = _safe_float(resolved.get(key), defaults[key], 0.05)
    resolved["max_command_age_s"] = _safe_float(
        resolved.get("max_command_age_s"), defaults["max_command_age_s"], 0.0)
    resolved["max_clock_skew_s"] = _safe_float(
        resolved.get("max_clock_skew_s"), defaults["max_clock_skew_s"], 0.0)
    return resolved


def _build_topics(options):
    base = "/".join((
        options["topic_prefix"], options["site_id"],
    ))
    node_root = "/".join((base, "nodes", options["node_id"]))
    return {
        "base": base,
        "node_root": node_root,
        "status": node_root + "/status",
        "heartbeat": node_root + "/heartbeat",
        "registration": node_root + "/registration",
        "config_state": node_root + "/config/state",
        "events": base + "/events",
        "all_events": base + "/events/#",
        "acks": node_root + "/acks",
        "node_acks": node_root + "/acks/#",
        "node_commands": node_root + "/commands/#",
        "broadcast_commands": base + "/broadcast/commands/#",
    }


def _validate_file(path, label):
    if not path:
        return
    if not os.path.isfile(path):
        raise ValueError(label + " does not exist: " + str(path))
    if not os.access(path, os.R_OK):
        raise ValueError(label + " is not readable: " + str(path))
    if os.path.getsize(path) <= 0:
        raise ValueError(label + " is empty: " + str(path))


def _validate_private_key_permissions(path, tls):
    if not path or not _safe_bool(
            tls.get("enforce_private_key_permissions"), False):
        return
    mode = stat.S_IMODE(os.stat(path).st_mode)
    if mode & 0o077:
        raise ValueError(
            "MQTT client private key permissions must be 0600 or stricter: "
            + str(path))


def _validate_tls_material(tls):
    ca_file = tls.get("ca_file")
    cert_file = tls.get("cert_file")
    key_file = tls.get("key_file")
    try:
        context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH, cafile=ca_file or None)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        if cert_file:
            context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise ValueError("MQTT TLS material is invalid: " + str(exc)) from exc


def _validate_client_identity(options, tls):
    cert_file = tls.get("cert_file")
    if not cert_file or not _safe_bool(tls.get("require_identity_uri"), False):
        return
    decoder = getattr(getattr(ssl, "_ssl", None), "_test_decode_cert", None)
    if not callable(decoder):
        raise ValueError("MQTT certificate identity decoder is unavailable")
    try:
        details = decoder(cert_file)
        now_s = time.time()
        not_before = details.get("notBefore")
        not_after = details.get("notAfter")
        if not_before and now_s < ssl.cert_time_to_seconds(not_before):
            raise ValueError("MQTT client certificate is not valid yet")
        if not_after and now_s >= ssl.cert_time_to_seconds(not_after):
            raise ValueError("MQTT client certificate has expired")
        identities = {
            str(value)
            for kind, value in details.get("subjectAltName") or ()
            if str(kind).upper() == "URI"
        }
    except (OSError, ssl.SSLError) as exc:
        raise ValueError("MQTT client certificate cannot be decoded") from exc
    expected = "urn:automation:{}:worker:{}".format(
        options["site_id"], options["node_id"])
    if expected not in identities:
        raise ValueError(
            "MQTT client certificate URI identity mismatch; expected "
            + expected)


def _validate_transport(options):
    transport = _mapping(options, "transport")
    tls = _mapping(transport, "tls")
    tls_enabled = _safe_bool(tls.get("enabled"), True)
    if not tls_enabled and not _safe_bool(transport.get("allow_insecure_transport"), False):
        raise ValueError("unencrypted MQTT requires allow_insecure_transport=true")
    if _safe_bool(tls.get("insecure_skip_verify"), False):
        if not _safe_bool(transport.get("allow_insecure_tls"), False):
            raise ValueError("insecure TLS verification requires allow_insecure_tls=true")
    if tls_enabled and _safe_bool(tls.get("require_ca_file"), False):
        if not tls.get("ca_file"):
            raise ValueError("MQTT TLS requires an explicit CA file")
    if tls_enabled and _safe_bool(
            tls.get("require_client_certificate"), False):
        if not tls.get("cert_file") or not tls.get("key_file"):
            raise ValueError(
                "MQTT mTLS requires a client certificate and private key")
    _validate_file(tls.get("ca_file"), "MQTT CA file")
    _validate_file(tls.get("cert_file"), "MQTT client certificate")
    _validate_file(tls.get("key_file"), "MQTT client private key")
    if bool(tls.get("cert_file")) != bool(tls.get("key_file")):
        raise ValueError("MQTT mTLS requires cert_file and key_file together")
    _validate_private_key_permissions(tls.get("key_file"), tls)
    if tls_enabled:
        _validate_tls_material(tls)
        _validate_client_identity(options, tls)


def validate_mqtt_options(options):
    resolved = _resolved_options(options)
    if resolved["enabled"]:
        _validate_transport(resolved)
    return resolved


def _transport_options(context):
    options = context["options"]
    transport = copy.deepcopy(_mapping(options, "transport"))
    credentials = _mapping(options, "credentials")
    username = _resolve_secret(credentials.get("username"), credentials.get("username_env"))
    password = _resolve_secret(credentials.get("password"), credentials.get("password_env"))
    transport["client_id"] = str(
        transport.get("client_id") or "automation-" + options["node_id"])
    transport["username"] = username
    transport["password"] = password
    transport["subscriptions"] = [
        {"topic": context["topics"]["node_commands"], "qos": 1},
        {"topic": context["topics"]["broadcast_commands"], "qos": 1},
        {"topic": context["topics"]["node_acks"], "qos": 1},
    ]
    transport["will"] = {
        "topic": context["topics"]["status"],
        "payload": _canonical_bytes(_status_envelope(context, "offline", False)),
        "qos": 1,
        "retain": True,
    }
    return transport


# =============================================================================
# ENVELOPE AUTHENTICATION AND CONSTRUCTION
# =============================================================================

def _auth_secret(context):
    auth = _mapping(context["options"], "command_auth")
    return _resolve_secret(auth.get("secret"), auth.get("secret_env"))


def _signature_payload(envelope):
    unsigned = copy.deepcopy(envelope)
    unsigned.pop("signature", None)
    return _canonical_bytes(unsigned)


def _sign_envelope(context, envelope):
    auth = _mapping(context["options"], "command_auth")
    if not _safe_bool(auth.get("sign_outbound"), False):
        return envelope
    secret = _auth_secret(context)
    if not secret:
        raise ValueError("outbound HMAC signing requires a secret")
    signed = copy.deepcopy(envelope)
    signed["signature"] = hmac.new(
        str(secret).encode("utf-8"), _signature_payload(signed), hashlib.sha256,
    ).hexdigest()
    return signed


def _verify_envelope(context, envelope):
    auth = _mapping(context["options"], "command_auth")
    if not _safe_bool(auth.get("require_hmac"), False):
        return True
    secret = _auth_secret(context)
    supplied = str(envelope.get("signature") or "")
    if not secret or not supplied:
        return False
    expected = hmac.new(
        str(secret).encode("utf-8"), _signature_payload(envelope), hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, supplied)


def _base_envelope(context, kind, message_id=None):
    return {
        "protocol": CONTEXT["protocol"],
        "kind": str(kind),
        "message_id": str(message_id or uuid.uuid4()),
        "site_id": context["options"]["site_id"],
        "source_node": context["options"]["node_id"],
        "created_at_s": time.time(),
    }


def _status_envelope(context, state, graceful=True):
    envelope = _base_envelope(context, "status")
    envelope.update({
        "state": str(state),
        "graceful": bool(graceful),
        "interface_version": CONTEXT["library_version"],
    })
    return envelope


def _ack_envelope(context, command, result):
    envelope = _base_envelope(context, "ack", command.get("message_id"))
    envelope.update({
        "target_node": command.get("source_node"),
        "command": command.get("command"),
        "ok": bool(result.get("ok")),
        "result": copy.deepcopy(result),
    })
    return envelope


# =============================================================================
# TRANSPORT PUBLICATION
# =============================================================================

def _transport_publish(context, topic, envelope, qos=1, retain=False):
    transport = context.get("transport") or {}
    publisher = transport.get("publish")
    if not callable(publisher):
        return False
    try:
        signed = _sign_envelope(context, envelope)
        encoded = _canonical_bytes(signed)
        maximum = _safe_int(
            _mapping(context["options"], "transport").get("max_packet_size"),
            4 * 1024 * 1024,
            1024,
        )
        if len(encoded) > maximum:
            raise ValueError("MQTT JSON envelope exceeds max_packet_size")
        written = publisher(topic, encoded, qos=qos, retain=retain)
    except Exception as exc:
        context["last_error"] = str(exc)
        return False
    with context["lock"]:
        key = "published" if written else "publish_rejected"
        context["statistics"][key] += 1
    return bool(written)


def _publish_status(context, state, graceful=True):
    return _transport_publish(
        context, context["topics"]["status"],
        _status_envelope(context, state, graceful), qos=1, retain=True,
    )


def _publish_registration(context):
    envelope = _base_envelope(context, "node_registration")
    envelope["capabilities"] = {
        "config_read": _allowed_command(context, "config.get"),
        "config_patch": _allowed_command(context, "config.patch"),
        "config_replace": _allowed_command(context, "config.replace"),
        "event_publish": True,
    }
    return _transport_publish(
        context, context["topics"]["registration"], envelope, qos=1, retain=True)


def _publish_heartbeat(context):
    envelope = _base_envelope(context, "heartbeat")
    status_reader = context.get("runtime_status_reader")
    if callable(status_reader):
        try:
            envelope["runtime"] = status_reader()
        except Exception as exc:
            envelope["runtime"] = {"state": "unavailable", "error": str(exc)}
    written = _transport_publish(
        context, context["topics"]["heartbeat"], envelope, qos=0, retain=False)
    context["last_heartbeat_s"] = time.monotonic()
    return written


def publish_mqtt_event(api_or_context, event_type, payload, **kwargs):
    context = _get_context(api_or_context)
    if not context["options"]["enabled"]:
        return False
    event_name = str(event_type or "").upper()
    publication = _mapping(context["options"], "event_publication")
    if not _event_publication_allowed(context, event_name):
        return False
    envelope = _base_envelope(context, "event", kwargs.get("message_id"))
    envelope.update({
        "event_type": event_name,
        "target_node": kwargs.get("target_node"),
        "payload": copy.deepcopy(payload),
    })
    topic_name = _topic_token(event_name.lower(), "unknown")
    topic = "/".join((
        context["topics"]["events"], topic_name, context["options"]["node_id"],
    ))
    qos = _safe_int(kwargs.get("qos"), publication.get("qos", 1))
    written = _transport_publish(context, topic, envelope, qos=qos, retain=False)
    if written and event_name.startswith("CONFIG_"):
        state = _base_envelope(context, "config_state")
        source = payload if _is_dict(payload) else {}
        state["revision"] = source.get("revision")
        state["fingerprint"] = source.get("fingerprint")
        _transport_publish(
            context, context["topics"]["config_state"], state, qos=1, retain=True)
    return written


def publish_mqtt_command(api_or_context, target_node, command, **kwargs):
    context = _get_context(api_or_context)
    payload = kwargs.get("payload")
    if not context["options"]["enabled"]:
        return {"ok": False, "error": "MQTT interface is disabled"}
    target = _topic_token(target_node, "broadcast")
    command_name = str(command or "").strip().lower()
    if not command_name:
        return {"ok": False, "error": "command is required"}
    envelope = _base_envelope(context, "command", kwargs.get("message_id"))
    envelope.update({
        "target_node": target,
        "command": command_name,
        "principal": kwargs.get("principal") or context["options"]["node_id"],
        "payload": copy.deepcopy(payload) if _is_dict(payload) else {},
    })
    if kwargs.get("expires_at_s") is not None:
        envelope["expires_at_s"] = kwargs["expires_at_s"]
    if kwargs.get("metadata") is not None:
        envelope["metadata"] = copy.deepcopy(kwargs["metadata"])
    if target in ("broadcast", "*"):
        root = context["topics"]["base"] + "/broadcast/commands"
    else:
        root = "/".join((context["topics"]["base"], "nodes", target, "commands"))
    suffix = "/".join(part for part in command_name.split(".") if part)
    topic = root + "/" + suffix
    written = _transport_publish(
        context, topic, envelope,
        qos=_safe_int(kwargs.get("qos"), 1), retain=False,
    )
    with context["lock"]:
        context["statistics"]["commands_sent"] += int(bool(written))
    return {
        "ok": bool(written),
        "message_id": envelope["message_id"],
        "topic": topic,
    }


def mirror_runtime_event(api_or_context, event):
    context = _get_context(api_or_context)
    if not _event_publication_allowed(context, _event_type(event)):
        return None
    return publish_mqtt_event(
        context, _event_type(event), copy.deepcopy(event),
        message_id=event.get("event_id") if _is_dict(event) else None,
    )


def _event_publication_allowed(context, event_type):
    publication = _mapping(context["options"], "event_publication")
    allowed = {str(item).upper() for item in publication.get("event_types") or []}
    return not allowed or str(event_type).upper() in allowed


# =============================================================================
# INBOUND VALIDATION AND DEDUPLICATION
# =============================================================================

def _get_context(api_or_context):
    if _is_dict(api_or_context) and _is_dict(api_or_context.get("context")):
        return api_or_context["context"]
    return api_or_context


def _command_from_topic(context, topic):
    marker = "/commands/"
    if marker not in str(topic):
        return ""
    suffix = str(topic).split(marker, 1)[1].strip("/")
    return suffix.replace("/", ".").lower()


def _command_error(code, message, **kwargs):
    result = {"ok": False, "code": str(code), "error": str(message)}
    result.update(copy.deepcopy(kwargs))
    return result


def _command_age_valid(context, envelope):
    now = time.time()
    expires = envelope.get("expires_at_s")
    if expires is not None and _safe_float(expires, 0.0) < now:
        return False
    maximum = context["options"]["max_command_age_s"]
    created = _safe_float(envelope.get("created_at_s"), 0.0)
    if created > now + context["options"]["max_clock_skew_s"]:
        return False
    if maximum > 0.0 and created > 0.0 and now - created > maximum:
        return False
    return True


def _allowed_command(context, name):
    with context["lock"]:
        if str(name).lower() in context.get("command_handlers", {}):
            return True
    commands = _mapping(context["options"], "commands")
    allowed = {str(item).lower() for item in commands.get("allowed") or []}
    return not allowed or str(name).lower() in allowed


def _envelope_semantic_error(context, envelope, topic_command):
    command = envelope["command"]
    if command and topic_command and command != topic_command:
        return _command_error("topic_mismatch", "command and topic differ")
    if envelope.get("protocol") != CONTEXT["protocol"]:
        return _command_error("protocol", "unsupported protocol")
    if envelope.get("kind") != "command":
        return _command_error("kind", "envelope kind must be command")
    if str(envelope.get("site_id") or "") != context["options"]["site_id"]:
        return _command_error("site", "command belongs to another site")
    if not str(envelope.get("message_id") or ""):
        return _command_error("message_id", "message_id is required")
    target = str(envelope.get("target_node") or context["options"]["node_id"])
    if target not in (context["options"]["node_id"], "*", "broadcast"):
        return _command_error("target", "command targets another node")
    return None


def _validate_command(context, message):
    if message.get("retain") and not context["options"]["allow_retained_commands"]:
        return None, _command_error("retained_command", "retained commands are disabled")
    try:
        envelope = _json_loads(message.get("payload") or b"")
    except Exception as exc:
        return None, _command_error("invalid_json", str(exc))
    topic_command = _command_from_topic(context, message.get("topic"))
    command = str(envelope.get("command") or topic_command).lower()
    envelope["command"] = command
    semantic_error = _envelope_semantic_error(context, envelope, topic_command)
    if semantic_error is not None:
        return envelope, semantic_error
    if not _verify_envelope(context, envelope):
        return envelope, _command_error("authentication", "invalid HMAC signature")
    if not _command_age_valid(context, envelope):
        return envelope, _command_error("expired", "command is expired or stale")
    if not _allowed_command(context, command):
        return envelope, _command_error("command_denied", command)
    payload = envelope.get("payload")
    envelope["payload"] = payload if _is_dict(payload) else {}
    return envelope, None


def _lookup_ack(context, message_id):
    identity = str(message_id or "")
    with context["lock"]:
        result = context["dedupe"].get(identity)
        if result is not None:
            context["statistics"]["duplicates"] += 1
        return copy.deepcopy(result)


def _remember_ack(context, message_id, ack):
    identity = str(message_id or "")
    with context["lock"]:
        if identity not in context["dedupe"]:
            context["dedupe_order"].append(identity)
        context["dedupe"][identity] = copy.deepcopy(ack)
        while len(context["dedupe_order"]) > context["options"]["dedupe_limit"]:
            expired = context["dedupe_order"].pop(0)
            context["dedupe"].pop(expired, None)


def _ack_topic(context, command):
    target = _topic_token(
        command.get("source_node"), context["options"]["node_id"])
    root = "/".join((context["topics"]["base"], "nodes", target, "acks"))
    return root + "/" + _topic_token(
        command.get("message_id"), "unknown")


def _publish_ack(context, command, result):
    envelope = _ack_envelope(context, command, result)
    topic = _ack_topic(context, command)
    _remember_ack(context, command.get("message_id"), envelope)
    return _transport_publish(context, topic, envelope, qos=1, retain=False)


# =============================================================================
# COMMAND EXECUTION
# =============================================================================

def _config_patch(context, command):
    payload = command["payload"]
    request = context["config_capabilities"].get("request_patch")
    if not callable(request):
        return _command_error("capability", "config patch capability unavailable")
    selector = payload.get("selector")
    if not isinstance(selector, dict):
        selector = payload.get("path")
    return request(
        selector, payload.get("value"),
        mode=payload.get("mode", "set"),
        operation_kind=payload.get("operation_kind") or payload.get("operation"),
        source="mqtt_external:" + str(command.get("principal") or "unknown"),
        external=True,
        principal=command.get("principal"),
        base_revision=payload.get("base_revision"),
        operation_id=command.get("message_id"),
        verify=True,
    )


def _config_replace(context, command):
    payload = command["payload"]
    request = context["config_capabilities"].get("request_replace")
    if not callable(request):
        return _command_error("capability", "config replace capability unavailable")
    return request(
        payload.get("config") or {},
        source="mqtt_external:" + str(command.get("principal") or "unknown"),
        external=True,
        principal=command.get("principal"),
        base_revision=payload.get("base_revision"),
        operation_id=command.get("message_id"),
        representation="storage",
    )


def _config_snapshot(context, command):
    reader = context["config_capabilities"].get("get_snapshot")
    if not callable(reader):
        return _command_error("capability", "config read capability unavailable")
    config = type_library.project_config_for_storage(reader())
    report = type_library.verify_storage_config_data(config)
    if report.get("status") != "OK":
        return _command_error(
            "config_contract", "config snapshot storage projection is invalid")
    meta_rows = config.get("meta") or []
    meta = meta_rows[0] if isinstance(meta_rows, list) and meta_rows else {}
    if (
        not isinstance(meta, dict)
        or int(meta.get("version") or 0) != 9
        or str(meta.get("scope") or "") not in ("node_local", "master_global")
    ):
        return _command_error(
            "config_contract", "MQTT config snapshot must be V9 storage")
    return {"ok": True, "config": config,
            "fingerprint": type_library.config_fingerprint(config),
            "message_id": command.get("message_id")}


def _config_shard(context, command):
    reader = context["config_capabilities"].get("get_shard")
    controller_uuid = command["payload"].get("controller_uuid")
    if not callable(reader) or not controller_uuid:
        return _command_error("capability", "config shard request is incomplete")
    return {
        "ok": True,
        "controller_uuid": controller_uuid,
        "shard": reader(controller_uuid),
    }


def _config_sync(context, command):
    request = context["config_capabilities"].get("request_sync")
    if not callable(request):
        return _command_error("capability", "config sync capability unavailable")
    return request(
        source="mqtt_external:" + str(command.get("principal") or "unknown"))


def _runtime_status(context, _command):
    reader = context.get("runtime_status_reader")
    if not callable(reader):
        return _command_error("capability", "runtime status capability unavailable")
    return {"ok": True, "runtime": reader()}


def _execute_command(context, command):
    handlers = {
        "config.patch": _config_patch,
        "config.update": _config_patch,
        "config.append": _config_patch,
        "config.delete": _config_patch,
        "config.replace": _config_replace,
        "config.get": _config_snapshot,
        "config.shard.get": _config_shard,
        "config.sync": _config_sync,
        "runtime.status": _runtime_status,
    }
    if command["command"] in ("config.update", "config.append", "config.delete"):
        command["payload"]["mode"] = command["command"].split(".")[1]
    revision_error = _validate_command_revision(context, command)
    if revision_error is not None:
        return revision_error
    handler = handlers.get(command["command"])
    with context["lock"]:
        custom_handler = context["command_handlers"].get(command["command"])
        guards = tuple(context["command_guards"])
    for guard in guards:
        guarded = guard(copy.deepcopy(command))
        if _is_dict(guarded) and not guarded.get("ok", False):
            return guarded
    if callable(custom_handler):
        handler = custom_handler
    if not callable(handler):
        return _command_error("unknown_command", command["command"])
    try:
        if callable(custom_handler):
            result = handler(copy.deepcopy(command))
        else:
            result = handler(context, command)
        return result if _is_dict(result) else {"ok": bool(result)}
    except Exception as exc:
        LOGGER.exception("MQTT command execution failed")
        return _command_error("execution", str(exc))


def _validate_command_revision(context, command):
    if command.get("command") not in (
        "config.patch", "config.update", "config.append",
        "config.delete", "config.replace",
    ):
        return None
    options = _mapping(context["options"], "commands")
    if not _safe_bool(options.get("require_base_revision"), True):
        return None
    supplied = command["payload"].get("base_revision")
    reader = context["config_capabilities"].get("get_revision")
    if supplied is None:
        return _command_error("base_revision_required", "base_revision is required")
    if not callable(reader):
        return _command_error("capability", "config revision capability unavailable")
    current = reader()
    if _safe_int(supplied, -1) != _safe_int(current, -2):
        return _command_error(
            "stale_revision", "configuration revision is stale",
            current_revision=current,
        )
    return None


# =============================================================================
# CALLBACKS AND COMMAND THREAD
# =============================================================================

def _is_ack_message(message):
    return "/acks/" in str(message.get("topic") or "")


def _is_event_message(message):
    return "/events/" in str(message.get("topic") or "")


def _validate_ack(context, message):
    try:
        envelope = _json_loads(message.get("payload") or b"")
    except Exception as exc:
        return None, _command_error("invalid_ack_json", str(exc))
    if envelope.get("protocol") != CONTEXT["protocol"]:
        return None, _command_error("protocol", "unsupported ACK protocol")
    if envelope.get("kind") != "ack":
        return None, _command_error("kind", "envelope kind must be ack")
    if str(envelope.get("site_id") or "") != context["options"]["site_id"]:
        return None, _command_error("site", "ACK belongs to another site")
    target = str(envelope.get("target_node") or "")
    if target and target != context["options"]["node_id"]:
        return None, _command_error("target", "ACK targets another node")
    if not _verify_envelope(context, envelope):
        return None, _command_error("authentication", "invalid ACK signature")
    if not _command_age_valid(context, envelope):
        return None, _command_error("expired", "ACK is expired or stale")
    return envelope, None


def _handle_ack_message(context, message):
    envelope, error = _validate_ack(context, message)
    if error is not None:
        with context["lock"]:
            context["statistics"]["acks_rejected"] += 1
        return False
    with context["lock"]:
        handlers = tuple(context["ack_handlers"])
        context["statistics"]["acks_received"] += 1
    handled = False
    for handler in handlers:
        try:
            handled = bool(handler(copy.deepcopy(envelope))) or handled
        except Exception:
            LOGGER.exception("MQTT ACK handler failed")
    return handled


def _validate_event_envelope(context, message):
    try:
        envelope = _json_loads(message.get("payload") or b"")
    except Exception as exc:
        return None, _command_error("invalid_event_json", str(exc))
    if envelope.get("protocol") != CONTEXT["protocol"]:
        return None, _command_error("protocol", "unsupported event protocol")
    if envelope.get("kind") != "event":
        return None, _command_error("kind", "envelope kind must be event")
    if str(envelope.get("site_id") or "") != context["options"]["site_id"]:
        return None, _command_error("site", "event belongs to another site")
    target = str(envelope.get("target_node") or "")
    if target and target not in (context["options"]["node_id"], "*", "broadcast"):
        return None, _command_error("target", "event targets another node")
    if not _verify_envelope(context, envelope):
        return None, _command_error("authentication", "invalid event signature")
    if not _command_age_valid(context, envelope):
        return None, _command_error("expired", "event is expired or stale")
    return envelope, None


def _handle_event_message(context, message):
    envelope, error = _validate_event_envelope(context, message)
    if error is not None:
        with context["lock"]:
            context["statistics"]["events_rejected"] += 1
        return False
    with context["lock"]:
        handlers = tuple(context["event_handlers"])
        context["statistics"]["events_received"] += 1
    handled = False
    for handler in handlers:
        try:
            handled = bool(handler(copy.deepcopy(envelope))) or handled
        except Exception:
            LOGGER.exception("MQTT event handler failed")
    return handled


def _on_transport_message(context, message):
    if context["stop_event"].is_set() or not context.get("accept_commands"):
        return False
    try:
        context["command_queue"].put_nowait(copy.deepcopy(message))
        with context["lock"]:
            context["statistics"]["commands_queued"] += 1
        return True
    except Full:
        with context["lock"]:
            context["statistics"]["commands_rejected"] += 1
        return False


def _on_transport_connect(context, _snapshot):
    context["state"] = "connected"
    _publish_status(context, "online", True)
    _publish_registration(context)
    with context["lock"]:
        handlers = tuple(context["connect_handlers"])
    for handler in handlers:
        try:
            handler()
        except Exception:
            LOGGER.exception("MQTT connect handler failed")


def _on_transport_disconnect(context, reason):
    if not context["stop_event"].is_set():
        context["state"] = "disconnected"
    context["last_error"] = str(reason)


def _handle_command_message(context, message):
    command, error = _validate_command(context, message)
    if command is None:
        with context["lock"]:
            context["statistics"]["commands_rejected"] += 1
        return
    cached = _lookup_ack(context, command.get("message_id"))
    if cached is not None:
        topic = _ack_topic(context, command)
        _transport_publish(context, topic, cached, qos=1, retain=False)
        return
    result = error if error is not None else _execute_command(context, command)
    _publish_ack(context, command, result)
    with context["lock"]:
        key = "commands_ok" if result.get("ok") else "commands_error"
        context["statistics"][key] += 1


def _heartbeat_due(context):
    elapsed = time.monotonic() - context["last_heartbeat_s"]
    return elapsed >= context["options"]["heartbeat_interval_s"]


def _command_loop(context):
    while not context["stop_event"].is_set():
        try:
            message = context["command_queue"].get(timeout=0.25)
        except Empty:
            if _heartbeat_due(context):
                _publish_heartbeat(context)
            continue
        try:
            if _is_ack_message(message):
                _handle_ack_message(context, message)
            elif _is_event_message(message):
                _handle_event_message(context, message)
            else:
                _handle_command_message(context, message)
        finally:
            context["command_queue"].task_done()


# =============================================================================
# CONTEXT AND LIFECYCLE
# =============================================================================

def register_mqtt_command_handler(api_or_context, command, handler):
    context = _get_context(api_or_context)
    name = str(command or "").strip().lower()
    if not name or not callable(handler):
        raise TypeError("command name and callable handler are required")
    with context["lock"]:
        context["command_handlers"][name] = handler
    return True


def register_mqtt_ack_handler(api_or_context, handler):
    context = _get_context(api_or_context)
    if not callable(handler):
        raise TypeError("ACK handler must be callable")
    with context["lock"]:
        if handler not in context["ack_handlers"]:
            context["ack_handlers"].append(handler)
    return True


def register_mqtt_connect_handler(api_or_context, handler):
    context = _get_context(api_or_context)
    if not callable(handler):
        raise TypeError("connect handler must be callable")
    with context["lock"]:
        if handler not in context["connect_handlers"]:
            context["connect_handlers"].append(handler)
    return True


def register_mqtt_event_handler(api_or_context, handler):
    context = _get_context(api_or_context)
    if not callable(handler):
        raise TypeError("event handler must be callable")
    with context["lock"]:
        if handler not in context["event_handlers"]:
            context["event_handlers"].append(handler)
    subscriber = (context.get("transport") or {}).get("subscribe")
    if callable(subscriber):
        subscriber(context["topics"]["all_events"], qos=1)
    return True


def register_mqtt_command_guard(api_or_context, handler):
    context = _get_context(api_or_context)
    if not callable(handler):
        raise TypeError("command guard must be callable")
    with context["lock"]:
        if handler not in context["command_guards"]:
            context["command_guards"].append(handler)
    return True


def create_mqtt_interface(**kwargs):
    options = validate_mqtt_options(kwargs.get("options"))
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "state": "created",
        "options": options,
        "topics": _build_topics(options),
        "lock": threading.RLock(),
        "stop_event": threading.Event(),
        "command_queue": Queue(maxsize=options["command_queue_maxsize"]),
        "command_thread": None,
        "transport": None,
        "config_capabilities": kwargs.get("config_capabilities") or {},
        "runtime_status_reader": kwargs.get("runtime_status_reader"),
        "command_handlers": {},
        "command_guards": [],
        "ack_handlers": [],
        "connect_handlers": [],
        "event_handlers": [],
        "dedupe": {},
        "dedupe_order": [],
        "last_heartbeat_s": time.monotonic(),
        "last_error": None,
        "accept_commands": False,
        "statistics": {
            "published": 0,
            "publish_rejected": 0,
            "commands_queued": 0,
            "commands_rejected": 0,
            "commands_ok": 0,
            "commands_error": 0,
            "commands_sent": 0,
            "acks_received": 0,
            "acks_rejected": 0,
            "events_received": 0,
            "events_rejected": 0,
            "duplicates": 0,
        },
    }
    auth = _mapping(options, "command_auth")
    if _safe_bool(auth.get("require_hmac"), False) and not _auth_secret(context):
        raise ValueError("required MQTT command HMAC secret is unavailable")
    factory = kwargs.get("transport_factory") or create_mqtt_client
    transport_options = _transport_options(context)
    context["transport"] = factory(
        on_message=partial(_on_transport_message, context),
        on_connect=partial(_on_transport_connect, context),
        on_disconnect=partial(_on_transport_disconnect, context),
        **transport_options
    )
    return _build_api(context)


def start_mqtt_interface(api_or_context):
    context = _get_context(api_or_context)
    if not context["options"]["enabled"]:
        context["state"] = "disabled"
        return None
    if _thread_alive(context.get("command_thread")):
        return context["command_thread"]
    context["stop_event"].clear()
    context["accept_commands"] = True
    thread = threading.Thread(
        target=partial(_command_loop, context),
        name="MQTT-Commands-" + context["options"]["node_id"],
        daemon=False,
    )
    context["command_thread"] = thread
    context["state"] = "starting"
    thread.start()
    context["transport"]["start"]()
    return thread


def _wait_for_outbound(context):
    deadline = time.monotonic() + context["options"]["flush_timeout_s"]
    while time.monotonic() < deadline:
        snapshot = context["transport"]["snapshot"]()
        if snapshot.get("outbound_queue_depth", 0) == 0:
            return True
        context["stop_event"].wait(0.02)
    return False


def quiesce_mqtt_interface(api_or_context, timeout_s=5.0):
    context = _get_context(api_or_context)
    context["accept_commands"] = False
    if context["state"] not in ("disabled", "stopped"):
        context["state"] = "quiescing"
    deadline = time.monotonic() + _safe_float(timeout_s, 5.0, 0.1)
    while time.monotonic() < deadline:
        if context["command_queue"].unfinished_tasks == 0:
            return True
        time.sleep(0.02)
    return False


def stop_mqtt_interface(api_or_context, **kwargs):
    context = _get_context(api_or_context)
    if context["state"] == "disabled":
        return get_mqtt_interface_snapshot(context)
    transport_before = context["transport"]["snapshot"]()
    never_started = (
        not _thread_alive(context.get("command_thread"))
        and not transport_before.get("thread_alive")
        and transport_before.get("state") in ("created", "stopped")
    )
    if never_started:
        context["accept_commands"] = False
        context["transport"]["stop"](
            join_timeout_s=kwargs.get("join_timeout_s", 5.0))
        context["state"] = "stopped"
        return get_mqtt_interface_snapshot(context)
    quiesce_mqtt_interface(
        context, timeout_s=kwargs.get("join_timeout_s", 5.0))
    _publish_status(context, "offline", True)
    _wait_for_outbound(context)
    context["stop_event"].set()
    context["transport"]["stop"](
        join_timeout_s=kwargs.get("join_timeout_s", 5.0))
    thread = context.get("command_thread")
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=_safe_float(kwargs.get("join_timeout_s"), 5.0, 0.1))
    transport_after = context["transport"]["snapshot"]()
    live = _thread_alive(thread) or transport_after.get("thread_alive")
    context["state"] = "stuck" if live else "stopped"
    return get_mqtt_interface_snapshot(context)


def wait_mqtt_interface_connected(api_or_context, timeout_s=None):
    context = _get_context(api_or_context)
    timeout = timeout_s or context["options"]["connect_wait_s"]
    return context["transport"]["wait_connected"](timeout)


def get_mqtt_interface_snapshot(api_or_context):
    context = _get_context(api_or_context)
    with context["lock"]:
        return {
            "module": context["module"],
            "version": context["version"],
            "state": context["state"],
            "enabled": context["options"]["enabled"],
            "required": context["options"]["required"],
            "startup_connection_required": context["options"][
                "startup_connection_required"],
            "site_id": context["options"]["site_id"],
            "node_id": context["options"]["node_id"],
            "command_thread_alive": _thread_alive(context.get("command_thread")),
            "command_queue_depth": context["command_queue"].qsize(),
            "dedupe_entries": len(context["dedupe"]),
            "topics": copy.deepcopy(context["topics"]),
            "statistics": dict(context["statistics"]),
            "transport": context["transport"]["snapshot"](),
            "last_error": context["last_error"],
        }


def _build_api(context):
    return {
        "context": context,
        "start": partial(start_mqtt_interface, context),
        "stop": partial(stop_mqtt_interface, context),
        "wait_connected": partial(wait_mqtt_interface_connected, context),
        "quiesce": partial(quiesce_mqtt_interface, context),
        "publish_event": partial(publish_mqtt_event, context),
        "publish_command": partial(publish_mqtt_command, context),
        "mirror_event": partial(mirror_runtime_event, context),
        "register_command_handler": partial(
            register_mqtt_command_handler, context),
        "register_ack_handler": partial(register_mqtt_ack_handler, context),
        "register_connect_handler": partial(
            register_mqtt_connect_handler, context),
        "register_event_handler": partial(
            register_mqtt_event_handler, context),
        "register_command_guard": partial(
            register_mqtt_command_guard, context),
        "snapshot": partial(get_mqtt_interface_snapshot, context),
    }
