# -*- coding: utf-8 -*-

# src/system/worker_preflight.py


"""
 =============================================================================
 WORKER_PREFLIGHT
 =============================================================================

 What it is:
     A side-effect-free deployment preflight for Worker configuration and
     MQTT mutual-TLS identity material.

 Why it is needed:
     Deployment errors must be reported before queues, probes or fieldbus
     owners start. A missing certificate is a configuration defect, while an
     unreachable broker is a transient runtime condition; these cases must be
     distinguished explicitly.

 How it works:
     The selected application profile is loaded, cross-section invariants are
     checked and enabled MQTT TLS files are resolved against the project root.
     Certificate parsing, key matching, permissions, validity and the expected
     Worker URI identity are verified without opening a network connection.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import importlib.util
import json
import os
import ssl
import sys
import time

from src.interfaces.mqtt_interface import validate_mqtt_options
from src.system.bootstrap_system_interface import load_app_config
from src.library._generic_system_contract_library.config_sharding_contracts import validate_cross_execution_shard
from src.library import _generic_type_library as type_library


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

CONTEXT = {
    "library_name": "worker_preflight",
    "library_version": "1.2.0",
}


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
    return bool(default)


def _project_root():
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _absolute_project_path(path):
    if not path or os.path.isabs(str(path)):
        return os.path.normpath(str(path)) if path else path
    return os.path.normpath(os.path.join(_project_root(), str(path)))


def _runtime_uid():
    getter = getattr(os, "getuid", None)
    return getter() if callable(getter) else "user"


def _validate_python_cache_policy(config):
    runtime = config.get("runtime") or {}
    policy = runtime.get("python_cache") or {}
    if not _is_dict(policy):
        raise ValueError("runtime.python_cache must be an object")
    if _safe_bool(policy.get("bytecode_enabled"), True):
        raise ValueError("runtime.python_cache.bytecode_enabled must be false")
    if not _safe_bool(policy.get("project_cache_forbidden"), False):
        raise ValueError(
            "runtime.python_cache.project_cache_forbidden must be true")
    template = str(policy.get("root_template") or "").strip()
    if not template:
        raise ValueError("runtime.python_cache.root_template is required")
    cache_root = template.replace("{uid}", str(_runtime_uid()))
    if "{" in cache_root or "}" in cache_root:
        raise ValueError("runtime.python_cache.root_template has unknown fields")
    cache_root = os.path.normpath(cache_root)
    if not os.path.isabs(cache_root):
        raise ValueError("runtime.python_cache.root_template must be absolute")
    resolved_root = os.path.realpath(cache_root)
    resolved_tmp = os.path.realpath("/tmp")
    try:
        below_tmp = (
            os.path.commonpath((resolved_root, resolved_tmp)) == resolved_tmp
            and resolved_root != resolved_tmp
        )
    except ValueError:
        below_tmp = False
    if not below_tmp:
        raise ValueError("runtime.python_cache root must be below /tmp")
    try:
        inside_project = os.path.commonpath(
            (resolved_root, os.path.realpath(_project_root()))) == os.path.realpath(
                _project_root())
    except ValueError:
        inside_project = False
    if inside_project:
        raise ValueError("Python cache root may not be inside the project")
    environment_root = os.environ.get("PYTHONPYCACHEPREFIX") or ""
    if os.path.realpath(environment_root) != resolved_root:
        raise ValueError(
            "PYTHONPYCACHEPREFIX differs from runtime.python_cache policy")
    if not _safe_bool(os.environ.get("PYTHONDONTWRITEBYTECODE"), False):
        raise ValueError("PYTHONDONTWRITEBYTECODE=1 is required")
    if not bool(getattr(sys, "dont_write_bytecode", False)):
        raise ValueError("Python bytecode writes are not disabled")
    interpreter_root = getattr(sys, "pycache_prefix", None)
    if os.path.realpath(str(interpreter_root or "")) != resolved_root:
        raise ValueError("interpreter pycache_prefix differs from YAML policy")
    return resolved_root


def _resolved_mqtt_options(config):
    options = copy.deepcopy(config.get("mqtt_interface") or {})
    app = config.get("app") or {}
    options.setdefault("site_id", app.get("site_id") or "site-main")
    options.setdefault("node_id", app.get("node_id") or "node-unknown")
    transport = copy.deepcopy(options.get("transport") or {})
    tls = copy.deepcopy(transport.get("tls") or {})
    for key in ("ca_file", "cert_file", "key_file"):
        if tls.get(key):
            tls[key] = _absolute_project_path(tls[key])
    transport["tls"] = tls
    options["transport"] = transport
    return options


def _certificate_details(cert_file):
    decoder = getattr(getattr(ssl, "_ssl", None), "_test_decode_cert", None)
    if not callable(decoder):
        return None
    return decoder(cert_file)


def _certificate_uri_identities(details):
    identities = []
    if not _is_dict(details):
        return identities
    for kind, value in details.get("subjectAltName") or ():
        if str(kind).upper() == "URI":
            identities.append(str(value))
    return identities


def _validate_certificate_time(details, now_s):
    if not _is_dict(details):
        return
    not_before = details.get("notBefore")
    not_after = details.get("notAfter")
    if not_before and now_s < ssl.cert_time_to_seconds(not_before):
        raise ValueError("MQTT client certificate is not valid yet")
    if not_after and now_s >= ssl.cert_time_to_seconds(not_after):
        raise ValueError("MQTT client certificate has expired")


def _validate_identity(config, options):
    tls = ((options.get("transport") or {}).get("tls") or {})
    cert_file = tls.get("cert_file")
    if not cert_file or not _safe_bool(tls.get("require_identity_uri"), False):
        return []
    details = _certificate_details(cert_file)
    if details is None:
        return ["certificate URI identity decoder is unavailable"]
    _validate_certificate_time(details, time.time())
    app = config.get("app") or {}
    expected = "urn:automation:{}:worker:{}".format(
        app.get("site_id") or options.get("site_id"),
        app.get("node_id") or options.get("node_id"),
    )
    identities = _certificate_uri_identities(details)
    if expected not in identities:
        raise ValueError(
            "MQTT client certificate URI identity mismatch; expected "
            + expected)
    return []


def _device_config(config):
    data_path = (((config.get("runtime") or {}).get("data_center") or {}).get(
        "json_path"))
    if not data_path:
        data_path = (config.get("paths") or {}).get("device_state_json")
    absolute = _absolute_project_path(data_path)
    if not absolute or not os.path.isfile(absolute):
        raise ValueError("device-state configuration does not exist: " + str(absolute))
    with open(absolute, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _controller_uuids(config):
    data = _device_config(config)
    return {
        str(row.get("controller_uuid"))
        for row in data.get("controllers") or []
        if _is_dict(row) and row.get("controller_uuid")
    }


def _active_controller_protocols(threads, device_config):
    """Return protocols used by active controller owners in this profile."""
    if not _safe_bool(threads.get("controllers_enabled")):
        return ()
    allowlist = {
        str(value) for value in threads.get("controller_allowlist") or ()
    }
    protocols = set()
    for controller in device_config.get("controllers") or ():
        if not _is_dict(controller):
            continue
        controller_uuid = str(controller.get("controller_uuid") or "")
        if allowlist and controller_uuid not in allowlist:
            continue
        protocol = str(controller.get("bus_type") or "").strip().lower()
        if protocol:
            protocols.add(protocol)
    return tuple(sorted(protocols))


def _validate_protocol_dependencies(threads, device_config, module_finder=None):
    """Fail fast when an active optional hardware backend is unavailable."""
    protocols = _active_controller_protocols(threads, device_config)
    finder = module_finder or importlib.util.find_spec
    if "plc_s7" in protocols and finder("snap7") is None:
        raise ValueError(
            "plc_s7 requires python-snap7; install requirements-plc-s7.txt"
        )
    return protocols


def _validate_runtime_invariants(config):
    mqtt = config.get("mqtt_interface") or {}
    distributed = config.get("distributed_runtime") or {}
    threads = config.get("thread_management") or {}
    if _safe_bool(distributed.get("enabled")) and not _safe_bool(mqtt.get("enabled")):
        raise ValueError("distributed_runtime requires mqtt_interface.enabled=true")
    if _safe_bool(threads.get("connections_enabled")) and not _safe_bool(
            threads.get("controllers_enabled")):
        raise ValueError("connections_enabled requires controllers_enabled=true")
    configured_controllers = _controller_uuids(config)
    device_config = _device_config(config)
    report = type_library.verify_storage_config_data(device_config)
    if report.get("status") != "OK":
        raise ValueError("device-state configuration validation failed")
    _validate_protocol_dependencies(threads, device_config)
    app_node_id = str((config.get("app") or {}).get("node_id") or "")
    meta_rows = device_config.get("meta") or []
    meta = meta_rows[0] if meta_rows and _is_dict(meta_rows[0]) else {}
    if str(meta.get("scope") or "") != "node_local":
        raise ValueError("Worker device-state config must have meta.scope=node_local")
    if str(meta.get("node_id") or "") != app_node_id:
        raise ValueError(
            "Worker app.node_id differs from device-state meta.node_id")
    allowlist = tuple(str(value) for value in threads.get("controller_allowlist") or ())
    if _safe_bool(threads.get("controllers_enabled")) and allowlist:
        missing = sorted(set(allowlist) - configured_controllers)
        if missing:
            raise ValueError(
                "controller_allowlist contains unknown UUIDs: " + ", ".join(missing))
    cross = config.get("cross_runtime") or {}
    if _safe_bool(cross.get("enabled")):
        role = str(distributed.get("role") or "standalone").strip().lower()
        if _safe_bool(distributed.get("enabled")) and role == "coordinator":
            raise ValueError("coordinator role may not enable cross_runtime")
        cross_node_id = str(cross.get("node_id") or app_node_id)
        if cross_node_id != app_node_id:
            raise ValueError("cross-runtime node_id differs from app.node_id")
        cross_path = _absolute_project_path(cross.get("config_path"))
        if not cross_path or not os.path.isfile(cross_path):
            raise ValueError("cross-runtime configuration does not exist: " + str(cross_path))
        with open(cross_path, "r", encoding="utf-8") as handle:
            cross_config = json.load(handle)
        typed_cross_report = type_library.verify_storage_config_data(cross_config)
        if typed_cross_report.get("status") != "OK":
            raise ValueError("cross-runtime type validation failed")
        runtime_cross_config = type_library.normalize_config_for_runtime(
            cross_config, representation="storage")
        cross_report = validate_cross_execution_shard(
            runtime_cross_config, app_node_id)
        if not cross_report.get("ok"):
            raise ValueError("cross-runtime configuration validation failed")
        cross_meta_rows = cross_config.get("meta") or []
        cross_meta = (
            cross_meta_rows[0]
            if cross_meta_rows and _is_dict(cross_meta_rows[0])
            else {}
        )
        configured_domain = str(cross.get("domain_id") or "")
        shard_domain = str(
            cross_meta.get("domain_id")
            or cross_meta.get("cross_domain_id")
            or ""
        )
        if configured_domain and shard_domain != configured_domain:
            raise ValueError(
                "cross-runtime domain_id differs from Cross shard meta")


def _failure(error, profile):
    return {
        "ok": False,
        "profile": profile,
        "code": "worker_preflight_failed",
        "error_type": type(error).__name__,
        "message": str(error),
        "remediation": (
            "Run ./install_mqtt_identity.sh --source-dir <secure-directory> "
            "for distributed operation, or intentionally select "
            "AUTOMATION_PROFILE=standalone."
        ),
    }


# =============================================================================
# EXTERNAL INTERFACE
# =============================================================================

def validate_worker_environment(config_path=None, active_profile=None):
    profile = str(active_profile or os.environ.get("AUTOMATION_PROFILE") or "")
    try:
        config = load_app_config(config_path, active_profile)
        profile = str(config.get("_active_profile") or profile or "base")
        python_cache_root = _validate_python_cache_policy(config)
        _validate_runtime_invariants(config)
        options = _resolved_mqtt_options(config)
        options = validate_mqtt_options(options)
        warnings = _validate_identity(config, options) if options["enabled"] else []
        distributed = config.get("distributed_runtime") or {}
        distributed_enabled = _safe_bool(distributed.get("enabled"))
        role = str(distributed.get("role") or "standalone").strip().lower()
        configured_local = _safe_bool(
            distributed.get("local_automation_enabled"))
        configured_sync_mode = str(
            distributed.get("config_sync_mode") or "global_first"
        ).strip().lower()
        local_authority = bool(
            not distributed_enabled
            or role != "worker"
            or (
                configured_local
                and configured_sync_mode == "local_first"
            )
        )
        if distributed_enabled and role == "worker" and not local_authority:
            warnings = list(warnings)
            warnings.append(
                "distributed Worker local authority is mandatory; runtime "
                "will enforce local_automation_enabled=true and "
                "config_sync_mode=local_first"
            )
            local_authority = True
        return {
            "ok": True,
            "profile": profile,
            "node_id": options.get("node_id"),
            "site_id": options.get("site_id"),
            "mqtt_enabled": options["enabled"],
            "distributed_enabled": distributed_enabled,
            "distributed_role": role,
            "local_automation_enabled": local_authority,
            "configured_local_automation_enabled": configured_local,
            "configured_config_sync_mode": configured_sync_mode,
            "python_cache_root": python_cache_root,
            "python_bytecode_enabled": False,
            "startup_connection_required": options[
                "startup_connection_required"],
            "warnings": warnings,
        }
    except Exception as exc:
        return _failure(exc, profile or "unknown")


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    as_json = "--json" in arguments
    result = validate_worker_environment()
    if as_json:
        print(json.dumps(result, sort_keys=True, ensure_ascii=False))
    elif result["ok"]:
        mode = "distributed" if result["distributed_enabled"] else "standalone"
        print(
            "Worker preflight passed: profile={} mode={} mqtt={} "
            "automation={}".format(
                result["profile"], mode,
                "enabled" if result["mqtt_enabled"] else "disabled",
                "local" if result.get("local_automation_enabled") else "disabled",
            ))
        for warning in result.get("warnings") or ():
            print("Worker preflight warning: " + str(warning), file=sys.stderr)
    else:
        print(
            "Worker preflight failed: " + result["message"],
            file=sys.stderr,
        )
        print("Remediation: " + result["remediation"], file=sys.stderr)
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
