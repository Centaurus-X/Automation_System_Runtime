# -*- coding: utf-8 -*-

# src/library/_generic_system_contract_library/protocol_contracts.py

"""Pure protocol, envelope, identity, topic and bounded transport contracts."""

import base64
import binascii
import copy
import datetime
import gzip
import hashlib
import io
import math
import re
import time
import uuid

from src.library import _generic_type_library as type_authority
from src.library._generic_system_contract_library import config_sharding_contracts
from src.library._generic_system_contract_library.errors import make_error, make_success


CONTRACTS_MAJOR = 2
CONTRACTS_MINOR = 0
CONTRACTS_VERSION = "2.0"
TYPE_SCHEMA_VERSION = type_authority.TYPE_SCHEMA_VERSION
CONFIG_NORM_VERSION = copy.deepcopy(type_authority.CONFIG_NORM_VERSION)
DEFAULT_SITE_ID = "site-main"
OPERATION_FORMAT = "config_scope_operations_v1"
CAPABILITIES = frozenset((
    OPERATION_FORMAT,
    "authority_epoch_v1",
    "authority_transfer_v1",
    "cross_owner_accept_v1",
    "mirror_v1",
    "restore_quarantine_v1",
    "chunk_reassembly",
    "direct_proposal_mtls",
"canonical_config_v9",
    "node_full",
    "node_patch",
    "cross_runtime",
    "cross_full",
    "cross_patch",
    "config_proposals",
    "cross_fact_consumer",
    "remote_actuator",
    "local_automation_authority",
))

canonical_bytes = type_authority.canonical_bytes
decode_json_bytes = type_authority.decode_json_bytes
fingerprint = type_authority.fingerprint
json_ready = type_authority.json_ready

_identities_TOKEN_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_identities_SERVICE_ROLES = ("master", "proxy", "worker", "monitoring")


def _identities_normalize_token(value, label="identity"):
    """Normalize one routing token and reject unsafe topic/path characters."""
    token = str(value or "").strip().lower()
    if not _identities_TOKEN_PATTERN.fullmatch(token):
        raise ValueError(str(label) + " has an invalid format")
    return token


def _identities_validate_service_identity(value, **kwargs):
    """Validate a service identity dictionary."""
    if not isinstance(value, dict):
        return make_error("identity_mismatch", "service identity must be an object")
    if set(value) != {"service", "instance_id"}:
        return make_error("identity_mismatch", "service identity fields are invalid")
    try:
        service = _identities_normalize_token(value.get("service"), "service")
        instance_id = _identities_normalize_token(value.get("instance_id"), "instance_id")
    except ValueError as exc:
        return make_error("identity_mismatch", str(exc))
    allowed = tuple(kwargs.get("allowed_services") or _identities_SERVICE_ROLES)
    if service not in allowed:
        return make_error("identity_mismatch", "service role is not allowed")
    return make_success(service=service, instance_id=instance_id)


def _identities_certificate_uri(site_id, service, instance_id):
    """Build the canonical URI SAN used by mTLS identities."""
    site = _identities_normalize_token(site_id, "site_id")
    role = _identities_normalize_token(service, "service")
    instance = _identities_normalize_token(instance_id, "instance_id")
    return "urn:automation:{}:{}:{}".format(site, role, instance)


def _identities_identity_matches_certificate(value, certificate_identity, **kwargs):
    """Bind payload identity to the authenticated certificate URI SAN."""
    checked = _identities_validate_service_identity(value, **kwargs)
    if not checked.get("ok"):
        return checked
    try:
        expected = _identities_certificate_uri(
            kwargs.get("site_id"), checked["service"], checked["instance_id"])
    except ValueError as exc:
        return make_error("identity_mismatch", str(exc))
    if str(certificate_identity or "") != expected:
        return make_error("identity_mismatch", "certificate and payload identity differ")
    return make_success(certificate_identity=expected)


_schemas_PAYLOAD_SCHEMAS = {
    "actuator.request": {
        "required": ("controller_uuid", "actuator_id", "value"),
        "allowed": (
            "controller_uuid", "actuator_id", "value", "idempotent",
            "device_command_id_supported", "timeout_s", "metadata",
        ),
    },
    "config.propose": {
        "required": (),
        "allowed": (
            "base_revision", "changes", "reason", "rollout_targets",
            "target_node_id", "operation", "operation_format", "scope",
            "base_authority_epoch", "base_scope_revision",
            "base_scope_fingerprint", "operations",
        ),
    },
    "config.rollback": {
        "required": ("base_revision", "target_revision"),
        "allowed": ("base_revision", "target_revision", "reason", "rollout_targets"),
    },
    "node.admission.approve": {
        "required": ("node_id",),
        "allowed": ("node_id", "certificate_identity", "reason"),
    },
    "node.drain": {
        "required": ("node_id",),
        "allowed": ("node_id", "deadline_s", "reason"),
    },
    "node.state.set": {
        "required": ("node_id", "desired_state"),
        "allowed": ("node_id", "desired_state", "reason"),
    },
    "nodes.list": {
        "required": (),
        "allowed": (),
    },
    "config.get": {
        "required": (),
        "allowed": ("revision", "scope"),
    },
    "config.history": {
        "required": (),
        "allowed": ("limit",),
    },
    "operation.get": {
        "required": ("operation_id",),
        "allowed": ("operation_id",),
    },
    "runtime.status": {
        "required": (),
        "allowed": (),
    },
    "terminal.execute": {
        "required": ("command",),
        "allowed": ("command",),
    },
}


def _schemas_validate_payload(event_type, payload, **kwargs):
    """Validate one known payload without executing business policy."""
    if not isinstance(payload, dict):
        return make_error("invalid_payload", "payload must be an object")
    schema = _schemas_PAYLOAD_SCHEMAS.get(str(event_type or ""))
    if schema is None:
        if kwargs.get("allow_unknown_event", False):
            return make_success(payload=payload)
        return make_error("invalid_payload", "event_type is not supported")
    missing = [key for key in schema["required"] if key not in payload]
    if missing:
        return make_error("invalid_payload", "required payload fields are missing", fields=missing)
    unknown = sorted(set(payload) - set(schema["allowed"]))
    if unknown:
        return make_error("invalid_payload", "unknown payload fields are not allowed", fields=unknown)
    validator = {
        "actuator.request": _schemas__validate_actuator,
        "config.propose": _schemas__validate_config_proposal,
        "config.rollback": _schemas__validate_config_rollback,
        "node.admission.approve": _schemas__validate_node,
        "node.drain": _schemas__validate_node,
        "node.state.set": _schemas__validate_node_state,
        "config.get": _schemas__validate_config_get,
        "config.history": _schemas__validate_history,
        "operation.get": _schemas__validate_operation,
        "terminal.execute": _schemas__validate_terminal,
    }.get(str(event_type or ""))
    checked = validator(payload) if callable(validator) else make_success()
    return make_success(payload=payload) if checked.get("ok") else checked


def _schemas__uuid(value, label):
    try:
        uuid.UUID(str(value))
    except Exception:
        return make_error("invalid_payload", str(label) + " must be a UUID")
    return make_success()


def _schemas__validate_actuator(payload):
    checked = _schemas__uuid(payload.get("controller_uuid"), "controller_uuid")
    if not checked.get("ok"):
        return checked
    actuator_id = str(payload.get("actuator_id") or "")
    if not actuator_id or len(actuator_id) > 128:
        return make_error("invalid_payload", "actuator_id is invalid")
    value = payload.get("value")
    if isinstance(value, float) and not math.isfinite(value):
        return make_error("invalid_payload", "actuator value must be finite")
    if payload.get("timeout_s") is not None:
        try:
            timeout_s = float(payload["timeout_s"])
        except Exception:
            return make_error("invalid_payload", "timeout_s must be numeric")
        if not 0.1 <= timeout_s <= 120.0:
            return make_error("invalid_payload", "timeout_s is outside its bounds")
    return make_success()


def _schemas__validate_legacy_config_proposal(payload):
    revision = payload.get("base_revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        return make_error(
            "invalid_payload", "base_revision must be a non-negative integer")
    changes = payload.get("changes")
    if not isinstance(changes, list) or not changes or len(changes) > 128:
        return make_error(
            "invalid_payload", "changes must be a bounded non-empty list")
    for change in changes:
        if not isinstance(change, dict):
            return make_error("invalid_payload", "each change must be an object")
        allowed = {"operation", "path", "value"}
        if set(change) - allowed or "path" not in change:
            return make_error(
                "invalid_payload", "configuration change fields are invalid")
        operation = str(change.get("operation") or "set").lower()
        if operation not in ("set", "update", "append", "delete"):
            return make_error(
                "invalid_payload", "configuration operation is invalid")
        path = str(change.get("path") or "")
        if not path or len(path) > 512:
            return make_error("invalid_payload", "configuration path is invalid")
        if operation != "delete" and "value" not in change:
            return make_error("invalid_payload", "configuration value is required")
    targets = payload.get("rollout_targets")
    if targets is not None:
        if not isinstance(targets, list) or len(targets) > 256:
            return make_error(
                "invalid_payload", "rollout_targets must be a bounded list")
        try:
            for node_id in targets:
                normalize_token(node_id, "node_id")
        except ValueError as exc:
            return make_error("invalid_payload", str(exc))
    return make_success()


def validate_config_proposal_payload(payload, **kwargs):
    """Validate one external proposal without mapping its operations."""
    if not isinstance(payload, dict):
        return make_error("invalid_payload", "proposal payload must be an object")
    authority_mode = str(kwargs.get("authority_mode") or "node_local")
    if authority_mode == "master_global":
        allowed = {"base_revision", "changes", "reason", "rollout_targets"}
        if set(payload) - allowed:
            return make_error(
                "invalid_payload", "legacy proposal fields are invalid")
        checked = _schemas__validate_legacy_config_proposal(payload)
        return (
            make_success(payload=copy.deepcopy(payload))
            if checked.get("ok")
            else checked
        )
    if authority_mode != "node_local":
        return make_error("invalid_payload", "authority mode is invalid")
    if {"base_revision", "changes", "rollout_targets"}.intersection(payload):
        return make_error(
            "unsupported_operation_format",
            "legacy path operations are not accepted by the node_local relay")
    allowed = {
        "target_node_id", "operation", "operation_format", "scope",
        "base_authority_epoch", "base_scope_revision",
        "base_scope_fingerprint", "reason", "operations",
    }
    required = allowed - {"reason"}
    unknown = sorted(set(payload) - allowed)
    missing = sorted(required - set(payload))
    if unknown or missing:
        return make_error(
            "invalid_payload", "canonical proposal fields are invalid",
            unknown_fields=unknown, missing_fields=missing)
    try:
        target = normalize_token(payload["target_node_id"], "target_node_id")
    except ValueError as exc:
        return make_error("invalid_payload", str(exc))
    scope = str(payload.get("scope") or "")
    if ":" not in scope or scope.split(":", 1)[0] not in ("node", "cross"):
        return make_error("invalid_payload", "proposal scope is invalid")
    try:
        normalize_token(scope.split(":", 1)[1], "scope_id")
    except ValueError as exc:
        return make_error("invalid_payload", str(exc))
    if scope.startswith("node:") and scope.split(":", 1)[1] != target:
        return make_error(
            "invalid_payload", "target node and scope owner differ")
    for name in ("base_authority_epoch", "base_scope_revision"):
        value = payload.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return make_error(
                "invalid_payload", name + " must be a non-negative integer")
    if not type_authority.is_fingerprint(payload.get("base_scope_fingerprint")):
        return make_error(
            "invalid_payload",
            "base_scope_fingerprint must be lowercase raw 64-hex")
    if payload.get("operation") != "scope_operations":
        return make_error(
            "unsupported_operation_format",
            "proposal operation must be scope_operations")
    if payload.get("operation_format") != OPERATION_FORMAT:
        return make_error(
            "unsupported_operation_format", "operation format is unsupported")
    reason = payload.get("reason")
    if reason is not None and (
            not isinstance(reason, str) or len(reason) > 2048):
        return make_error("invalid_payload", "proposal reason is invalid")
    try:
        operations = config_sharding_contracts.validate_scope_operations(
            payload.get("operations"),
            operation_format=payload.get("operation_format"),
        )
    except ValueError as exc:
        return make_error("unsupported_operation_format", str(exc))
    return make_success(
        payload=copy.deepcopy(payload),
        target_node_id=target,
        scope=scope,
        operations=operations,
    )


def _schemas__validate_config_proposal(payload):
    canonical_fields = {
        "target_node_id", "operation_format", "scope", "operations",
    }
    authority_mode = (
        "node_local" if canonical_fields.intersection(payload)
        else "master_global"
    )
    return validate_config_proposal_payload(
        payload, authority_mode=authority_mode)


def _schemas__validate_config_rollback(payload):
    for key in ("base_revision", "target_revision"):
        value = payload.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            return make_error("invalid_payload", key + " must be a positive integer")
    targets = payload.get("rollout_targets")
    if targets is not None:
        if not isinstance(targets, list) or len(targets) > 256:
            return make_error("invalid_payload", "rollout_targets must be a bounded list")
        try:
            for node_id in targets:
                normalize_token(node_id, "node_id")
        except ValueError as exc:
            return make_error("invalid_payload", str(exc))
    return make_success()


def _schemas__validate_node(payload):
    try:
        normalize_token(payload.get("node_id"), "node_id")
    except ValueError as exc:
        return make_error("invalid_payload", str(exc))
    deadline = payload.get("deadline_s")
    if deadline is not None:
        try:
            numeric = float(deadline)
        except Exception:
            return make_error("invalid_payload", "deadline_s must be numeric")
        if not 0.0 <= numeric <= 86400.0:
            return make_error("invalid_payload", "deadline_s is outside its bounds")
    return make_success()


def _schemas__validate_node_state(payload):
    checked = _schemas__validate_node(payload)
    if not checked.get("ok"):
        return checked
    if str(payload.get("desired_state") or "").upper() not in (
            "ACTIVE", "DRAINING", "MAINTENANCE", "QUARANTINED"):
        return make_error("invalid_payload", "desired_state is invalid")
    return make_success()


def _schemas__validate_config_get(payload):
    if payload.get("revision") is not None and payload.get("scope") is not None:
        return make_error(
            "invalid_payload", "revision and scope are mutually exclusive")
    revision = payload.get("revision")
    if revision is not None and (
            isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision < 1):
        return make_error(
            "invalid_payload", "revision must be a positive integer")
    scope = payload.get("scope")
    if scope is not None:
        text = str(scope)
        if ":" not in text or text.split(":", 1)[0] not in ("node", "cross"):
            return make_error("invalid_payload", "scope is invalid")
        try:
            normalize_token(text.split(":", 1)[1], "scope_id")
        except ValueError as exc:
            return make_error("invalid_payload", str(exc))
    return make_success()


def _schemas__validate_history(payload):
    limit = payload.get("limit")
    if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200):
        return make_error("invalid_payload", "limit must be between 1 and 200")
    return make_success()


def _schemas__validate_operation(payload):
    return _schemas__uuid(payload.get("operation_id"), "operation_id")


def _schemas__validate_terminal(payload):
    command = str(payload.get("command") or "")
    if not command or len(command) > 512 or any(ord(character) < 32 for character in command):
        return make_error("invalid_payload", "terminal command is invalid")
    return make_success()


_envelopes_CONTROL_PROTOCOL = "automation.control.v1"
_envelopes_SCHEMA_VERSION = 1
_envelopes_TOP_LEVEL_FIELDS = (
    "protocol", "schema_version", "message_id", "correlation_id",
    "causation_id", "idempotency_key", "event_type", "source", "target",
    "site_id", "principal", "issued_at", "expires_at", "authority_term",
    "config_revision", "payload", "extensions",
)
_envelopes_REQUIRED_FIELDS = (
    "protocol", "schema_version", "message_id", "correlation_id",
    "event_type", "source", "target", "site_id", "issued_at", "expires_at",
    "authority_term", "config_revision", "payload",
)


def _envelopes_utc_now():
    """Return an aware UTC timestamp."""
    return datetime.datetime.now(datetime.timezone.utc)


def _envelopes_format_timestamp(value):
    """Format one aware timestamp as canonical RFC3339 UTC."""
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    normalized = value.astimezone(datetime.timezone.utc)
    return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _envelopes_parse_timestamp(value):
    """Parse RFC3339 text into an aware UTC timestamp."""
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(datetime.timezone.utc)


def _envelopes__uuid_text(value, label):
    try:
        return str(uuid.UUID(str(value)))
    except Exception as exc:
        raise ValueError(str(label) + " must be a UUID") from exc


def _envelopes_make_envelope(event_type, source, target, **kwargs):
    """Construct one detached control envelope."""
    now = kwargs.get("now") or _envelopes_utc_now()
    ttl_s = float(kwargs.get("ttl_s", 30.0))
    message_id = _envelopes__uuid_text(kwargs.get("message_id") or uuid.uuid4(), "message_id")
    correlation = _envelopes__uuid_text(
        kwargs.get("correlation_id") or message_id, "correlation_id")
    causation = kwargs.get("causation_id")
    result = {
        "protocol": _envelopes_CONTROL_PROTOCOL,
        "schema_version": _envelopes_SCHEMA_VERSION,
        "message_id": message_id,
        "correlation_id": correlation,
        "causation_id": _envelopes__uuid_text(causation, "causation_id") if causation else None,
        "idempotency_key": kwargs.get("idempotency_key"),
        "event_type": str(event_type),
        "source": copy.deepcopy(source),
        "target": copy.deepcopy(target),
        "site_id": normalize_token(kwargs.get("site_id"), "site_id"),
        "principal": copy.deepcopy(kwargs.get("principal")),
        "issued_at": _envelopes_format_timestamp(now),
        "expires_at": _envelopes_format_timestamp(now + datetime.timedelta(seconds=ttl_s)),
        "authority_term": int(kwargs.get("authority_term", 0)),
        "config_revision": int(kwargs.get("config_revision", 0)),
        "payload": copy.deepcopy(kwargs.get("payload") or {}),
    }
    if kwargs.get("extensions") is not None:
        result["extensions"] = copy.deepcopy(kwargs["extensions"])
    return result


def _envelopes__validate_structure(envelope):
    if not isinstance(envelope, dict):
        return make_error("invalid_envelope", "envelope must be an object")
    missing = sorted(set(_envelopes_REQUIRED_FIELDS) - set(envelope))
    if missing:
        return make_error("invalid_envelope", "required fields are missing", fields=missing)
    unknown = sorted(set(envelope) - set(_envelopes_TOP_LEVEL_FIELDS))
    if unknown:
        return make_error("invalid_envelope", "unknown critical fields", fields=unknown)
    if envelope.get("protocol") != _envelopes_CONTROL_PROTOCOL:
        return make_error("unsupported_protocol", "protocol is not supported")
    if envelope.get("schema_version") != _envelopes_SCHEMA_VERSION:
        return make_error("unsupported_protocol", "schema version is not supported")
    return make_success()


def _envelopes__validate_time(envelope, **kwargs):
    try:
        issued = _envelopes_parse_timestamp(envelope.get("issued_at"))
        expires = _envelopes_parse_timestamp(envelope.get("expires_at"))
    except Exception as exc:
        return make_error("invalid_envelope", str(exc))
    now = kwargs.get("now") or _envelopes_utc_now()
    skew = datetime.timedelta(seconds=float(kwargs.get("clock_skew_s", 30.0)))
    if issued > now + skew:
        return make_error("expired", "message issue time is in the future")
    if expires < now - skew or expires <= issued:
        return make_error("expired", "message is expired or has an invalid lifetime")
    maximum_lifetime = datetime.timedelta(seconds=float(kwargs.get("maximum_ttl_s", 300.0)))
    if expires - issued > maximum_lifetime:
        return make_error("expired", "message lifetime exceeds its limit")
    return make_success(issued_at=issued, expires_at=expires)


def _envelopes_validate_envelope(envelope, **kwargs):
    """Validate size, structure, identities, time, routing and payload schema."""
    structured = _envelopes__validate_structure(envelope)
    if not structured.get("ok"):
        return structured
    maximum = int(kwargs.get("maximum_bytes", 1024 * 1024))
    if len(canonical_bytes(envelope)) > maximum:
        return make_error("invalid_envelope", "envelope exceeds its size limit")
    for label in ("message_id", "correlation_id", "causation_id"):
        if label == "causation_id" and envelope.get(label) is None:
            continue
        try:
            _envelopes__uuid_text(envelope.get(label), label)
        except ValueError as exc:
            return make_error("invalid_envelope", str(exc))
    source = validate_service_identity(envelope.get("source"))
    target = validate_service_identity(envelope.get("target"))
    if not source.get("ok") or not target.get("ok"):
        return make_error("identity_mismatch", "source or target identity is invalid")
    timed = _envelopes__validate_time(envelope, **kwargs)
    if not timed.get("ok"):
        return timed
    expected_site = kwargs.get("expected_site_id")
    if expected_site and envelope.get("site_id") != normalize_token(expected_site, "site_id"):
        return make_error("identity_mismatch", "message belongs to another site")
    if isinstance(envelope.get("authority_term"), bool) or not isinstance(
            envelope.get("authority_term"), int) or envelope["authority_term"] < 0:
        return make_error("invalid_envelope", "authority_term is invalid")
    if isinstance(envelope.get("config_revision"), bool) or not isinstance(
            envelope.get("config_revision"), int) or envelope["config_revision"] < 0:
        return make_error("invalid_envelope", "config_revision is invalid")
    key = envelope.get("idempotency_key")
    if key is not None and (not isinstance(key, str) or not 1 <= len(key) <= 128):
        return make_error("invalid_envelope", "idempotency_key is invalid")
    payload = validate_payload(
        envelope.get("event_type"), envelope.get("payload"),
        allow_unknown_event=kwargs.get("allow_unknown_event", False),
    )
    if not payload.get("ok"):
        return payload
    return make_success(envelope=copy.deepcopy(envelope))


_topics_TOPIC_PREFIX = "automation/v1"


def _topics_site_root(site_id, **kwargs):
    """Return the normalized site root."""
    prefix = str(kwargs.get("prefix") or _topics_TOPIC_PREFIX).strip("/")
    return prefix + "/" + normalize_token(site_id or DEFAULT_SITE_ID, "site_id")


def _topics_worker_command_topic(site_id, worker_id, command, **kwargs):
    """Return one legacy-compatible worker command topic."""
    suffix = normalize_token(command, "command").replace(".", "/")
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "nodes",
        normalize_token(worker_id, "worker_id"),
        "commands",
        suffix,
    ))


def _topics_worker_ack_filter(site_id, master_id, **kwargs):
    """Return the ACK filter used by the unchanged worker protocol."""
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "nodes",
        normalize_token(master_id, "master_id"),
        "acks/#",
    ))


def _topics_worker_event_filter(site_id, **kwargs):
    """Return the worker-event subscription filter."""
    return _topics_site_root(site_id, **kwargs) + "/events/#"


def _topics_master_command_topic(site_id, master_id, command, **kwargs):
    """Return one proxy-to-master control topic."""
    suffix = normalize_token(command, "command").replace(".", "/")
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "master",
        normalize_token(master_id, "master_id"),
        "commands",
        suffix,
    ))


def _topics_master_command_filter(site_id, master_id, **kwargs):
    """Return the complete master command subscription filter."""
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "master",
        normalize_token(master_id, "master_id"),
        "commands/#",
    ))


def _topics_proxy_ack_topic(site_id, proxy_id, responder_id, message_id, **kwargs):
    """Return one master-to-proxy ACK topic with explicit responder identity."""
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "proxy",
        normalize_token(proxy_id, "proxy_id"),
        "acks",
        normalize_token(responder_id, "responder_id"),
        normalize_token(message_id, "message_id"),
    ))


def _topics_proxy_ack_filter(site_id, proxy_id, **kwargs):
    """Return the ACK subscription filter for one proxy instance."""
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "proxy",
        normalize_token(proxy_id, "proxy_id"),
        "acks/#",
    ))


def _topics_control_event_topic(site_id, event_type, source_id, **kwargs):
    """Return one master-originated logical event topic."""
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "events",
        normalize_token(event_type, "event_type"),
        normalize_token(source_id, "source_id"),
    ))


def _topics_control_event_filter(site_id, source_id, **kwargs):
    """Return logical events emitted by exactly one control-plane source."""
    return "/".join((
        _topics_site_root(site_id, **kwargs),
        "events",
        "+",
        normalize_token(source_id, "source_id"),
    ))


_protocol_CONTRACTS_VERSION = "2.0"
_protocol_LEGACY_CONTRACTS_VERSION = "1.2"
_protocol_CANONICAL_CONFIG_VERSION = type_authority.CANONICAL_VERSION
_protocol_PROTOCOL_V1 = "automation.distributed.v1"
_protocol_PROTOCOL_V2 = "automation.distributed.v2"

_protocol_NODE_FULL = "distributed.config.apply"
_protocol_CROSS_FULL = "distributed.cross.apply"
_protocol_NODE_PATCH = "distributed.config.patch"
_protocol_CROSS_PATCH = "distributed.cross.patch"
_protocol_CONFIG_CHUNK = "distributed.config.chunk"

_protocol_TARGET_COMMANDS = (_protocol_NODE_FULL, _protocol_CROSS_FULL, _protocol_NODE_PATCH, _protocol_CROSS_PATCH)

_protocol_HARD_LIMITS = {
    "max_packet_size": 4 * 1024 * 1024,
    "chunk_bytes": 192 * 1024,
    "payload_bytes": 32 * 1024 * 1024,
    "encoded_bytes": 16 * 1024 * 1024,
    "chunks_per_transfer": 512,
    "transfers_per_node": 2,
    "transfer_ttl_s": 3600,
    "decompression_ratio": 20,
    "patch_operations": 500,
}


def _protocol__bounded_packet_size(value):
    try:
        supplied = int(value)
    except Exception:
        supplied = 1024 * 1024
    return min(_protocol_HARD_LIMITS["max_packet_size"], max(64 * 1024, supplied))


def _protocol__protocols(payload, message_protocol):
    values = payload.get("protocol_versions")
    if isinstance(values, list):
        result = tuple(sorted({str(value) for value in values if value in (_protocol_PROTOCOL_V1, _protocol_PROTOCOL_V2)}))
        if result:
            return result
    supplied = payload.get("protocol_version") or message_protocol or _protocol_PROTOCOL_V1
    return (str(supplied),) if supplied in (_protocol_PROTOCOL_V1, _protocol_PROTOCOL_V2) else (_protocol_PROTOCOL_V1,)


def _protocol__capabilities(payload, protocols, contracts_version):
    supplied = payload.get("capabilities")
    supplied = supplied if isinstance(supplied, dict) else {}
    modern = _protocol_PROTOCOL_V2 in protocols and contracts_version == _protocol_CONTRACTS_VERSION
    compatibility_peer = (
        _protocol_PROTOCOL_V2 in protocols and contracts_version == _protocol_LEGACY_CONTRACTS_VERSION)
    replication = bool(supplied.get("config_replication", True))
    cross_runtime = bool(supplied.get("cross_runtime", False))
    return {
        "canonical_config_v9": modern,
        "legacy_contract_peer": compatibility_peer,
        "node_full": bool(modern and supplied.get("node_full", replication)),
        "node_patch": bool(modern and supplied.get("node_patch", False)),
        "cross_runtime": bool(modern and cross_runtime),
        "cross_full": bool(modern and supplied.get("cross_full", cross_runtime)),
        "cross_patch": bool(modern and supplied.get("cross_patch", False)),
        "chunk_reassembly": bool(modern and supplied.get("chunk_reassembly", False)),
        "config_proposals": bool(modern and supplied.get("config_proposals", True)),
        "cross_fact_consumer": bool(modern and supplied.get("cross_fact_consumer", False)),
        "remote_actuator": bool(supplied.get("remote_actuator", False)),
        "master_automation_execution": bool(supplied.get("master_automation_execution", False)),
    }


def _protocol_normalize_registration(payload, **kwargs):
    """Normalize both the exact v0082 payload and the v2 registration shape."""
    if not isinstance(payload, dict):
        raise ValueError("registration payload must be an object")
    protocols = _protocol__protocols(payload, kwargs.get("message_protocol"))
    contracts_version = str(payload.get("contracts_version") or "legacy-v0082")
    capabilities = _protocol__capabilities(payload, protocols, contracts_version)
    node_replica = payload.get("node_replica")
    if not isinstance(node_replica, dict):
        node_replica = {
            "scope": "node:" + str(payload.get("node_id") or ""),
            "revision": int(payload.get("node_scope_revision") or payload.get("authority_revision") or 0),
            "fingerprint": payload.get("node_shard_fingerprint") or payload.get("fingerprint"),
        }
    cross_replica = payload.get("cross_replica")
    if not isinstance(cross_replica, dict) and (
        payload.get("cross_revision") is not None or payload.get("cross_fingerprint")
    ):
        cross_replica = {
            "scope": "cross:" + str(payload.get("node_id") or "") + ".default",
            "revision": int(payload.get("cross_revision") or 0),
            "fingerprint": payload.get("cross_fingerprint"),
        }
    return {
        "protocol_versions": protocols,
        "contracts_version": contracts_version,
        "config_versions": (_protocol_CANONICAL_CONFIG_VERSION,) \
            if contracts_version == _protocol_CONTRACTS_VERSION else (),
        "compatibility_peer": contracts_version == _protocol_LEGACY_CONTRACTS_VERSION,
        "capabilities": capabilities,
        "observed_global_revision": int(payload.get("observed_global_revision") or payload.get("authority_revision") or 0),
        "node_replica": copy.deepcopy(node_replica),
        "cross_replica": copy.deepcopy(cross_replica) if isinstance(cross_replica, dict) else None,
        "live_config_revision": int(payload.get("live_config_revision") or payload.get("local_config_revision") or 0),
        "pending_proposal_count": int(payload.get("pending_proposal_count") or 0),
        "max_packet_size": _protocol__bounded_packet_size(payload.get("max_packet_size")),
    }


def _protocol_selected_protocol(registration):
    """Select v2 only when its exact shared-contract version is present."""
    protocols = tuple(registration.get("protocol_versions") or ())
    if (
        _protocol_PROTOCOL_V2 in protocols
        and registration.get("contracts_version") == _protocol_CONTRACTS_VERSION
        and registration.get("capabilities", {}).get("canonical_config_v9") is True
    ):
        return _protocol_PROTOCOL_V2
    if _protocol_PROTOCOL_V1 in protocols:
        return _protocol_PROTOCOL_V1
    return None


def _protocol_validate_full_payload(payload, **kwargs):
    """Validate required aliases and scope identity of a Full command."""
    if not isinstance(payload, dict) or not isinstance(payload.get("config"), dict):
        return make_error("invalid_payload", "Full payload requires a config object")
    scope_kind = kwargs.get("scope_kind") or payload.get("shard_scope")
    if scope_kind not in ("node_local", "cross"):
        return make_error("invalid_payload", "shard scope is invalid")
    try:
        if scope_kind == "cross":
            type_authority.ensure_v8_cross_storage_config(payload["config"])
        else:
            type_authority.ensure_v9_storage_config(payload["config"])
    except ValueError as exc:
        required = "Cross-V8" if scope_kind == "cross" else "Node-V9"
        return make_error(
            "invalid_payload", "Full payload requires canonical " + required,
            detail=str(exc))
    target_fingerprint = payload.get("target_scope_fingerprint")
    alias = payload.get("shard_fingerprint") if scope_kind == "node_local" else payload.get("cross_fingerprint")
    if target_fingerprint and alias and target_fingerprint != alias:
        return make_error("fingerprint_mismatch", "Full fingerprint aliases differ")
    actual_fingerprint = fingerprint(payload["config"])
    if target_fingerprint and target_fingerprint != actual_fingerprint:
        return make_error("fingerprint_mismatch", "Full config fingerprint differs")
    if payload.get("global_revision") is not None and payload.get("authority_revision") is not None:
        if int(payload["global_revision"]) != int(payload["authority_revision"]):
            return make_error("stale_revision", "global revision aliases differ")
    return make_success(payload=copy.deepcopy(payload))


def _chunk__sha256(value):
    return hashlib.sha256(value).hexdigest()


def _chunk__transport_bytes(raw, compress):
    if not compress:
        return raw, "base64"
    compressed = gzip.compress(raw, mtime=0)
    ratio = len(raw) / max(1, len(compressed))
    if len(compressed) >= len(raw) or ratio > HARD_LIMITS["decompression_ratio"]:
        return raw, "base64"
    return compressed, "gzip-base64"


def _chunk__chunk_envelope(metadata, sequence, segment):
    result = dict(metadata)
    result.update({
        "command": CONFIG_CHUNK,
        "sequence": sequence,
        "chunk_sha256": _chunk__sha256(segment),
        "chunk_base64": base64.b64encode(segment).decode("ascii"),
    })
    return result


def _chunk__fits_packet(metadata, segment, packet_size):
    return len(canonical_bytes(_chunk__chunk_envelope(metadata, 0, segment))) <= packet_size


def _chunk__effective_chunk_size(metadata, payload, packet_size, requested):
    size = min(HARD_LIMITS["chunk_bytes"], max(1024, int(requested)))
    sample = payload[:size]
    while size >= 1024 and not _chunk__fits_packet(metadata, sample, packet_size):
        size = int(size * 0.8)
        sample = payload[:size]
    if size < 1024 or not _chunk__fits_packet(metadata, sample, packet_size):
        raise ValueError("packet limit cannot contain a safe chunk envelope")
    return size


def _chunk_encode_transfer(payload, **kwargs):
    """Serialize, optionally gzip and split a payload at raw byte boundaries."""
    target_command = str(kwargs.get("target_command") or "")
    if target_command not in TARGET_COMMANDS:
        raise ValueError("chunk target command is invalid")
    raw = canonical_bytes(payload)
    if len(raw) > HARD_LIMITS["payload_bytes"]:
        raise ValueError("uncompressed payload exceeds the hard limit")
    encoded, encoding = _chunk__transport_bytes(raw, kwargs.get("compress", True))
    if len(encoded) > HARD_LIMITS["encoded_bytes"]:
        raise ValueError("transport payload exceeds the hard limit")
    packet_size = min(
        HARD_LIMITS["max_packet_size"],
        max(64 * 1024, int(kwargs.get("max_packet_size") or 1024 * 1024)),
    )
    common = {
        "transfer_id": str(kwargs["transfer_id"]),
        "target_command": target_command,
        "scope": str(kwargs["scope"]),
        "target_scope_revision": int(kwargs["target_scope_revision"]),
        "target_scope_fingerprint": str(kwargs["target_scope_fingerprint"]),
        "encoding": encoding,
        "payload_sha256": _chunk__sha256(raw),
        "encoded_sha256": _chunk__sha256(encoded),
    }
    size = _chunk__effective_chunk_size(
        dict(common, total_chunks=HARD_LIMITS["chunks_per_transfer"]),
        encoded, packet_size, kwargs.get("chunk_bytes") or HARD_LIMITS["chunk_bytes"],
    )
    segments = tuple(encoded[index:index + size] for index in range(0, len(encoded), size))
    if not segments:
        segments = (b"",)
    if len(segments) > HARD_LIMITS["chunks_per_transfer"]:
        raise ValueError("chunk count exceeds the hard limit")
    metadata = dict(common, total_chunks=len(segments))
    chunks = tuple(
        _chunk__chunk_envelope(metadata, sequence, segment)
        for sequence, segment in enumerate(segments)
    )
    if any(len(canonical_bytes(chunk)) > packet_size for chunk in chunks):
        raise ValueError("serialized chunk envelope exceeds max_packet_size")
    return {
        "transfer_id": common["transfer_id"],
        "encoding": encoding,
        "payload_bytes": len(raw),
        "encoded_bytes": len(encoded),
        "chunks": chunks,
    }


def _chunk__metadata(chunks):
    fields = (
        "transfer_id", "target_command", "scope", "target_scope_revision",
        "target_scope_fingerprint", "encoding", "payload_sha256",
        "encoded_sha256", "total_chunks",
    )
    reference = {field: chunks[0].get(field) for field in fields}
    for chunk in chunks:
        if any(chunk.get(field) != reference[field] for field in fields):
            raise ValueError("chunk transfer metadata conflicts")
    return reference


def _chunk__decode_segment(chunk):
    try:
        segment = base64.b64decode(
            str(chunk.get("chunk_base64") or "").encode("ascii"), validate=True)
    except (UnicodeError, ValueError, binascii.Error) as exc:
        raise ValueError("chunk base64 is invalid") from exc
    if len(segment) > HARD_LIMITS["chunk_bytes"]:
        raise ValueError("raw chunk exceeds the hard limit")
    if _chunk__sha256(segment) != chunk.get("chunk_sha256"):
        raise ValueError("chunk hash differs")
    return segment


def _chunk__gunzip_bounded(encoded):
    with gzip.GzipFile(fileobj=io.BytesIO(encoded), mode="rb") as handle:
        raw = handle.read(HARD_LIMITS["payload_bytes"] + 1)
    if len(raw) > HARD_LIMITS["payload_bytes"]:
        raise ValueError("decompressed payload exceeds the hard limit")
    if len(raw) / max(1, len(encoded)) > HARD_LIMITS["decompression_ratio"]:
        raise ValueError("decompression ratio exceeds the hard limit")
    return raw


def _chunk_sequence_key(chunk):
    """Return a deterministic numeric chunk ordering key."""
    return int(chunk.get("sequence", -1))


def _chunk_reassemble_transfer(chunks):
    """Verify a complete transfer before decoding its canonical JSON object."""
    if not isinstance(chunks, (list, tuple)) or not chunks:
        raise ValueError("at least one chunk is required")
    if len(chunks) > HARD_LIMITS["chunks_per_transfer"]:
        raise ValueError("chunk count exceeds the hard limit")
    metadata = _chunk__metadata(chunks)
    total = int(metadata["total_chunks"])
    if total < 1 or total > HARD_LIMITS["chunks_per_transfer"]:
        raise ValueError("declared chunk count is invalid")
    ordered = sorted(chunks, key=_chunk_sequence_key)
    sequences = tuple(_chunk_sequence_key(chunk) for chunk in ordered)
    if sequences != tuple(range(total)) or len(ordered) != total:
        raise ValueError("chunks are missing or duplicated")
    segments = tuple(_chunk__decode_segment(chunk) for chunk in ordered)
    encoded = b"".join(segments)
    if len(encoded) > HARD_LIMITS["encoded_bytes"]:
        raise ValueError("encoded transfer exceeds the hard limit")
    if _chunk__sha256(encoded) != metadata["encoded_sha256"]:
        raise ValueError("encoded payload hash differs")
    if metadata["encoding"] == "gzip-base64":
        raw = _chunk__gunzip_bounded(encoded)
    elif metadata["encoding"] == "base64":
        raw = encoded
    else:
        raise ValueError("chunk encoding is unsupported")
    if _chunk__sha256(raw) != metadata["payload_sha256"]:
        raise ValueError("uncompressed payload hash differs")
    payload = decode_json_bytes(raw, maximum_bytes=HARD_LIMITS["payload_bytes"])
    return {"metadata": metadata, "payload": payload}


_legacy_LEGACY_PROTOCOL = "automation.mqtt.v1"
_legacy_LEGACY_DISTRIBUTED_PROTOCOL = "automation.distributed.v1"


def _legacy_make_worker_command(target_node, command, **kwargs):
    """Build the exact command shape consumed by the current worker."""
    now = float(kwargs.get("now_s", time.time()))
    message_id = str(kwargs.get("message_id") or uuid.uuid4())
    return {
        "protocol": _legacy_LEGACY_PROTOCOL,
        "kind": "command",
        "message_id": message_id,
        "site_id": normalize_token(kwargs.get("site_id"), "site_id"),
        "source_node": normalize_token(kwargs.get("source_node"), "source_node"),
        "target_node": normalize_token(target_node, "target_node"),
        "created_at_s": now,
        "expires_at_s": now + float(kwargs.get("ttl_s", 60.0)),
        "principal": str(kwargs.get("principal") or kwargs.get("source_node")),
        "command": str(command or "").strip().lower(),
        "payload": copy.deepcopy(kwargs.get("payload") or {}),
    }


def _legacy_make_worker_ack(command, source_node, result, **kwargs):
    """Build one legacy application ACK."""
    return {
        "protocol": _legacy_LEGACY_PROTOCOL,
        "kind": "ack",
        "message_id": str(command.get("message_id") or ""),
        "site_id": normalize_token(kwargs.get("site_id"), "site_id"),
        "source_node": normalize_token(source_node, "source_node"),
        "target_node": normalize_token(command.get("source_node"), "target_node"),
        "created_at_s": float(kwargs.get("now_s", time.time())),
        "command": str(command.get("command") or ""),
        "ok": bool(result.get("ok")),
        "result": copy.deepcopy(result),
    }


def _legacy_validate_worker_message(envelope, **kwargs):
    """Validate legacy identity, site, age and target before dispatch."""
    if not isinstance(envelope, dict):
        return make_error("invalid_envelope", "legacy envelope must be an object")
    if envelope.get("protocol") != _legacy_LEGACY_PROTOCOL:
        return make_error("unsupported_protocol", "legacy protocol differs")
    if envelope.get("kind") not in tuple(kwargs.get("allowed_kinds") or ("command", "ack", "event")):
        return make_error("invalid_envelope", "legacy message kind is invalid")
    if str(envelope.get("site_id") or "") != str(kwargs.get("site_id") or ""):
        return make_error("identity_mismatch", "legacy message belongs to another site")
    expected_target = str(kwargs.get("target_node") or "")
    target = str(envelope.get("target_node") or expected_target)
    if expected_target and target not in (expected_target, "*", "broadcast"):
        return make_error("identity_mismatch", "legacy message targets another node")
    now = float(kwargs.get("now_s", time.time()))
    skew = float(kwargs.get("clock_skew_s", 30.0))
    if float(envelope.get("created_at_s") or 0.0) > now + skew:
        return make_error("expired", "legacy message issue time is in the future")
    if float(envelope.get("expires_at_s") or now + 1.0) < now - skew:
        return make_error("expired", "legacy message has expired")
    return make_success(envelope=copy.deepcopy(envelope))


def _legacy_validate_worker_ack(envelope, expected, **kwargs):
    """Bind an ACK to its original target, message, command and term."""
    checked = _legacy_validate_worker_message(
        envelope,
        site_id=kwargs.get("site_id"),
        target_node=expected.get("source_node"),
        allowed_kinds=("ack",),
    )
    if not checked.get("ok"):
        return checked
    comparisons = (
        ("message_id", expected.get("message_id")),
        ("command", expected.get("command")),
        ("source_node", expected.get("target_node")),
    )
    for field, value in comparisons:
        if str(envelope.get(field) or "") != str(value or ""):
            return make_error("identity_mismatch", "legacy ACK " + field + " differs")
    expected_term = kwargs.get("authority_term")
    result = envelope.get("result") if isinstance(envelope.get("result"), dict) else {}
    actual_term = result.get("coordinator_term", result.get("authority_term"))
    if expected_term is not None and actual_term not in (None, expected_term):
        return make_error("stale_authority_term", "legacy ACK authority term differs")
    return make_success(envelope=copy.deepcopy(envelope))

# =============================================================================
# MIGRATED PUBLIC API
# =============================================================================

normalize_token = _identities_normalize_token
validate_service_identity = _identities_validate_service_identity
certificate_uri = _identities_certificate_uri
identity_matches_certificate = _identities_identity_matches_certificate
TOKEN_PATTERN = _identities_TOKEN_PATTERN
SERVICE_ROLES = _identities_SERVICE_ROLES
validate_payload = _schemas_validate_payload
PAYLOAD_SCHEMAS = _schemas_PAYLOAD_SCHEMAS
utc_now = _envelopes_utc_now
format_timestamp = _envelopes_format_timestamp
parse_timestamp = _envelopes_parse_timestamp
make_envelope = _envelopes_make_envelope
validate_envelope = _envelopes_validate_envelope
CONTROL_PROTOCOL = _envelopes_CONTROL_PROTOCOL
SCHEMA_VERSION = _envelopes_SCHEMA_VERSION
TOP_LEVEL_FIELDS = _envelopes_TOP_LEVEL_FIELDS
REQUIRED_FIELDS = _envelopes_REQUIRED_FIELDS
site_root = _topics_site_root
worker_command_topic = _topics_worker_command_topic
worker_ack_filter = _topics_worker_ack_filter
worker_event_filter = _topics_worker_event_filter
master_command_topic = _topics_master_command_topic
master_command_filter = _topics_master_command_filter
proxy_ack_topic = _topics_proxy_ack_topic
proxy_ack_filter = _topics_proxy_ack_filter
control_event_topic = _topics_control_event_topic
control_event_filter = _topics_control_event_filter
TOPIC_PREFIX = _topics_TOPIC_PREFIX
normalize_registration = _protocol_normalize_registration
selected_protocol = _protocol_selected_protocol
validate_full_payload = _protocol_validate_full_payload
CANONICAL_CONFIG_VERSION = _protocol_CANONICAL_CONFIG_VERSION
PROTOCOL_V1 = _protocol_PROTOCOL_V1
PROTOCOL_V2 = _protocol_PROTOCOL_V2
NODE_FULL = _protocol_NODE_FULL
CROSS_FULL = _protocol_CROSS_FULL
NODE_PATCH = _protocol_NODE_PATCH
CROSS_PATCH = _protocol_CROSS_PATCH
CONFIG_CHUNK = _protocol_CONFIG_CHUNK
TARGET_COMMANDS = _protocol_TARGET_COMMANDS
HARD_LIMITS = _protocol_HARD_LIMITS
encode_transfer = _chunk_encode_transfer
sequence_key = _chunk_sequence_key
reassemble_transfer = _chunk_reassemble_transfer
make_worker_command = _legacy_make_worker_command
make_worker_ack = _legacy_make_worker_ack
validate_worker_message = _legacy_validate_worker_message
validate_worker_ack = _legacy_validate_worker_ack
LEGACY_PROTOCOL = _legacy_LEGACY_PROTOCOL
LEGACY_DISTRIBUTED_PROTOCOL = _legacy_LEGACY_DISTRIBUTED_PROTOCOL


# =============================================================================
# CONTRACT MAJOR 2 PROTOCOL
# =============================================================================

CONFIG_PROPOSE = "distributed.config.propose"
MIRROR_ANNOUNCE = "distributed.mirror.announce"
MIRROR_REQUEST = "distributed.mirror.request"
MIRROR_DELTA = "distributed.mirror.delta"
MIRROR_SNAPSHOT = "distributed.mirror.snapshot"
MIRROR_CHUNK = "distributed.mirror.chunk"
MIRROR_ACK = "distributed.mirror.ack"
AUTHORITY_TRANSFER = "distributed.authority.transfer"
RESTORE_REQUEST = "distributed.restore.request"
RESTORE_ACTIVATE = "distributed.restore.activate"
TARGET_COMMANDS = tuple(sorted(set(TARGET_COMMANDS).union((
    MIRROR_SNAPSHOT,
    MIRROR_DELTA,
    AUTHORITY_TRANSFER,
    RESTORE_REQUEST,
    RESTORE_ACTIVATE,
))))


def effective_site_id(value):
    """Resolve a site exactly once with the normative siteless default."""
    return normalize_token(value or DEFAULT_SITE_ID, "site_id")


def parse_contract_version(value):
    """Parse major/minor from a string or explicit registration fields."""
    if isinstance(value, dict):
        major = value.get("contracts_major")
        minor = value.get("contracts_minor")
        if major is not None and minor is not None:
            return int(major), int(minor)
        value = value.get("contracts_version")
    parts = str(value or "").split(".")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return None
    return int(parts[0]), int(parts[1])


def contract_compatibility(peer):
    """Return the hard major gate and N/N-1 minor negotiation result."""
    parsed = parse_contract_version(peer)
    if parsed is None:
        return make_error("contracts_version_invalid", "contract version is invalid")
    major, minor = parsed
    if major != CONTRACTS_MAJOR:
        return make_error(
            "contracts_major_mismatch",
            "contract major differs",
            local_major=CONTRACTS_MAJOR,
            peer_major=major,
        )
    minimum_minor = max(0, CONTRACTS_MINOR - 1)
    if minor < minimum_minor or minor > CONTRACTS_MINOR:
        return make_error(
            "contracts_minor_unsupported",
            "contract minor is outside N/N-1",
            local_minor=CONTRACTS_MINOR,
            peer_minor=minor,
        )
    return make_success(major=major, minor=minor)


def _normalized_capabilities(payload):
    supplied = payload.get("capabilities")
    if isinstance(supplied, dict):
        if supplied.get("master_automation_execution") is True:
            raise ValueError("unsafe_capability")
        enabled = {
            str(key) for key, value in supplied.items()
            if value is True
        }
    elif isinstance(supplied, (list, tuple, set)):
        enabled = {str(value) for value in supplied}
    else:
        enabled = set()
    normalized = {
        name: name in enabled
        for name in sorted(CAPABILITIES)
    }
    normalized["master_automation_execution"] = False
    return normalized

def normalize_registration(payload, **kwargs):
    """Normalize one Major-2 registration and fail closed on incompatibility."""
    if not isinstance(payload, dict):
        raise ValueError("registration payload must be an object")
    compatibility = contract_compatibility(payload)
    if not compatibility.get("ok"):
        raise ValueError(compatibility["error"]["code"])
    if str(payload.get("type_schema_version") or "") != TYPE_SCHEMA_VERSION:
        raise ValueError("type_schema_mismatch")
    if payload.get("config_norm_version") != CONFIG_NORM_VERSION:
        raise ValueError("config_norm_mismatch")
    protocols = _protocol__protocols(payload, kwargs.get("message_protocol"))
    capabilities = _normalized_capabilities(payload)
    if capabilities.get(OPERATION_FORMAT) is not True:
        raise ValueError("unsupported_operation_format")
    node_id = normalize_token(payload.get("node_id"), "node_id")
    node_replica = payload.get("node_replica")
    if not isinstance(node_replica, dict):
        node_replica = {
            "scope": "node:" + node_id,
            "authority_epoch": int(payload.get("authority_epoch") or 0),
            "revision": int(payload.get("node_scope_revision") or 0),
            "fingerprint": payload.get("node_shard_fingerprint") or payload.get(
                "fingerprint"),
        }
    return {
        "site_id": effective_site_id(payload.get("site_id")),
        "node_id": node_id,
        "protocol_versions": protocols,
        "contracts_version": CONTRACTS_VERSION,
        "contracts_major": compatibility["major"],
        "contracts_minor": compatibility["minor"],
        "type_schema_version": TYPE_SCHEMA_VERSION,
        "config_norm_version": copy.deepcopy(CONFIG_NORM_VERSION),
        "capabilities": capabilities,
        "config_authority": str(payload.get("config_authority") or ""),
        "authority_epoch": int(payload.get("authority_epoch") or 0),
        "observed_global_revision": int(
            payload.get("observed_global_revision") or 0),
        "live_config_revision": int(
            payload.get("live_config_revision", payload.get(
                "local_config_revision", 0)) or 0),
        "node_replica": copy.deepcopy(node_replica),
        "cross_replica": copy.deepcopy(payload.get("cross_replica")),
        "pending_proposal_count": int(payload.get("pending_proposal_count") or 0),
        "max_packet_size": _protocol__bounded_packet_size(
            payload.get("max_packet_size")),
    }

def selected_protocol(registration):
    """Select v2 only for a compatible Major-2 capability peer."""
    if not isinstance(registration, dict):
        return None
    checked = contract_compatibility(registration)
    protocols = tuple(registration.get("protocol_versions") or ())
    capabilities = registration.get("capabilities") or {}
    operation_supported = (
        capabilities.get(OPERATION_FORMAT) is True
        if isinstance(capabilities, dict)
        else OPERATION_FORMAT in tuple(capabilities)
    )
    if (
        checked.get("ok")
        and PROTOCOL_V2 in protocols
        and operation_supported
    ):
        return PROTOCOL_V2
    return None

def validate_full_payload(payload, **kwargs):
    """Validate a legacy Full only in the explicit master-global path."""
    if kwargs.get("config_authority") != "master_global":
        return make_error("unsupported_operation_format", "Full decree is legacy-only")
    return _protocol_validate_full_payload(payload, **kwargs)


def worker_result_topic(site_id, worker_id, principal_id, request_id, **kwargs):
    """Return the closed direct-proposal result topic."""
    return "/".join((
        site_root(site_id, **kwargs),
        "nodes",
        normalize_token(worker_id, "worker_id"),
        "results",
        normalize_token(principal_id, "principal_id"),
        normalize_token(request_id, "request_id"),
    ))


def authenticated_principal(transport_identity, **kwargs):
    """Derive a principal exclusively from authenticated transport metadata."""
    if not isinstance(transport_identity, dict):
        return make_error("principal_denied", "authenticated transport identity is required")
    certificate_identity = str(transport_identity.get("certificate_identity") or "")
    service = str(transport_identity.get("service") or "")
    instance_id = str(transport_identity.get("instance_id") or "")
    checked = identity_matches_certificate(
        {"service": service, "instance_id": instance_id},
        certificate_identity,
        site_id=effective_site_id(kwargs.get("site_id")),
        allowed_services=tuple(kwargs.get("allowed_services") or ("master", "proxy", "monitoring")),
    )
    if not checked.get("ok"):
        return make_error("principal_denied", "transport identity is not authorized")
    return make_success(
        principal=instance_id,
        service=service,
        certificate_identity=certificate_identity,
    )


def validate_scope_fingerprint(value):
    """Validate the raw lowercase fingerprint contract."""
    if not type_authority.is_fingerprint(value):
        return make_error("fingerprint_invalid", "fingerprint must be lowercase raw 64-hex")
    return make_success(fingerprint=str(value))

