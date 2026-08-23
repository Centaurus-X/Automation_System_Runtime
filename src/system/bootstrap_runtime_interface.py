# -*- coding: utf-8 -*-

# src/system/bootstrap_runtime_interface.py


"""
 =============================================================================
 BOOTSTRAP_RUNTIME_INTERFACE
 =============================================================================

 What it is:
     The runtime composition root. It owns the Data Center, runtime queues,
     Event Router, State Event Management, Thread Management and sensor store.

 Why it is needed:
     Worker threads must not receive the complete mutable ``main_ctx``. The
     bootstrap constructs small capability dictionaries and retains lifecycle
     ownership so startup rollback and staged shutdown stay deterministic.

 How it works:
     Config is loaded synchronously before producers start. Router, SEM and TM
     then start with component-local stop events. SEM receives the only config
     writer authority; TM receives read and writer capabilities. Shutdown stops
     producers first, persists state, and terminates the Router last.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import json
import logging
import os
import threading
import time

from functools import partial
from queue import Full

from src.core.state_event_management import (
    create_config_capabilities,
    get_state_event_management_snapshot,
    register_external_publisher,
    register_post_commit_handler,
    register_write_authorizer,
    register_rollout_handler,
    request_config_patch,
    request_config_replace,
    request_config_sync,
    start_state_event_management,
    stop_state_event_management,
)
from src.core.distributed_coordinator import (
    create_distributed_coordinator,
    get_distributed_snapshot,
    start_distributed_coordinator,
    stop_distributed_coordinator,
)
from src.core.config_file_sync import (
    create_config_file_sync,
    get_config_file_sync_snapshot,
    prepare_startup_config_file_sync,
    start_config_file_sync,
    stop_config_file_sync,
)
from src.interfaces.mqtt_interface import (
    create_mqtt_interface,
    get_mqtt_interface_snapshot,
    start_mqtt_interface,
    stop_mqtt_interface,
    wait_mqtt_interface_connected,
)
from src.core.thread_management import (
    apply_config_rollout,
    dispatch_local_actuator_task,
    get_thread_management_snapshot,
    start_thread_management,
    stop_thread_management,
    set_cross_dispatchers,
)
from src.core._cross_worker_thread import (
    create_cross_capabilities,
    create_cross_worker,
    get_cross_worker_snapshot,
    start_cross_worker,
    stop_cross_worker,
)
from src.library._generic_system_contract_library.config_sharding_contracts import (
    build_cross_execution_shard,
    classify_config_scopes,
    validate_cross_execution_shard,
)
from src.library import _generic_data_center as data_center
from src.library import _generic_type_library as type_library
from src.library._event_interface import make_event, sync_queue_put, universal_serialize
from src.library._queue_factory import create_queue, remove_queue
from src.library._runtime_value_store import (
    close_runtime_value_store,
    create_runtime_value_store,
    create_sensor_value_capabilities,
    create_sensor_value_persistence,
    get_sensor_value_persistence_snapshot,
    start_sensor_value_persistence,
    stop_sensor_value_persistence,
)


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "bootstrap_runtime_interface",
    "library_version": "2.7.0",
    "description": "Capability-based runtime composition root",
    "options": {
        "queue_maxsize": 0,
        "router_thread_name": "EventRouter",
        "router_worker_threads": 4,
        "router_ingress_timeout_s": 0.25,
        "router_priority_batch_max": 64,
        "value_change_queue_maxsize": 4096,
        "join_timeout_s": 5.0,
    },
}

QUEUE_SPECS = (
    ("queue_event_send", "fifo"),
    ("queue_event_pc", "fifo"),
    ("queue_event_state", "fifo"),
    ("queue_event_mbc", "fifo"),
)


# =============================================================================
# HELP FUNCTIONS
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
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(default)


def _safe_int(value, default, minimum=0):
    try:
        result = int(value)
    except Exception:
        return int(default)
    return result if result >= minimum else int(default)


def _safe_float(value, default):
    try:
        result = float(value)
    except Exception:
        return float(default)
    return result if result > 0.0 else float(default)


def _mapping(parent, key):
    if not _is_dict(parent):
        return {}
    value = parent.get(key)
    return value if _is_dict(value) else {}


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _valid_shutdown_event(event):
    return all(callable(getattr(event, name, None))
               for name in ("is_set", "set", "wait"))


def _project_root():
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _absolute_project_path(value, default):
    path = str(value or default)
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.join(_project_root(), path))


def _runtime_sections(app_config):
    return {
        "runtime": _mapping(app_config, "runtime"),
        "thread_management": _mapping(app_config, "thread_management"),
        "state_event_management": _mapping(app_config, "state_event_management"),
        "paths": _mapping(app_config, "paths"),
        "shutdown": _mapping(app_config, "shutdown"),
        "app": _mapping(app_config, "app"),
        "mqtt_interface": _mapping(app_config, "mqtt_interface"),
        "distributed_runtime": _mapping(app_config, "distributed_runtime"),
        "config_file_sync": _mapping(app_config, "config_file_sync"),
        "cross_runtime": _mapping(app_config, "cross_runtime"),
    }


def _build_options(app_config):
    sections = _runtime_sections(app_config)
    runtime = sections["runtime"]
    queue_config = _mapping(runtime, "queues")
    router = _mapping(runtime, "event_router")
    values = _mapping(runtime, "value_store")
    sequence = _mapping(values, "sequence")
    defaults = CONTEXT["options"]
    return {
        "queue_maxsize": _safe_int(
            queue_config.get("maxsize"), defaults["queue_maxsize"]),
        "router_enabled": _safe_bool(router.get("enabled"), True),
        "router_thread_name": str(
            router.get("thread_name") or defaults["router_thread_name"]),
        "router_worker_threads": _safe_int(
            router.get("worker_threads"), defaults["router_worker_threads"], 1),
        "router_ingress_timeout_s": _safe_float(
            router.get("ingress_timeout_s"), defaults["router_ingress_timeout_s"]),
        "router_priority_batch_max": _safe_int(
            router.get("priority_batch_max"), defaults["router_priority_batch_max"], 1),
        "router_priority_enabled": _safe_bool(
            router.get("priority_enabled"), True),
        "value_change_queue_maxsize": _safe_int(
            values.get("change_queue_maxsize"),
            defaults["value_change_queue_maxsize"],
        ),
        "sequence_state_path": (
            _absolute_project_path(
                sequence.get("state_path"), "_runtime/sensor_sequence.json")
            if _safe_bool(sequence.get("enabled"), False) else None
        ),
        "sequence_reservation_size": _safe_int(
            sequence.get("reservation_size"), 4096, 1),
        "join_timeout_s": _safe_float(
            sections["shutdown"].get("join_timeout_s"), defaults["join_timeout_s"]),
    }


def _stop_payload(target):
    return universal_serialize(make_event(
        "__STOP__",
        payload={"reason": "runtime_shutdown"},
        source="bootstrap_runtime_interface",
        target=target,
        _type="__STOP__",
        _sentinel=True,
    ))


def _nudge(queue_object, target):
    if queue_object is None:
        return False
    try:
        queue_object.put_nowait(_stop_payload(target))
        return True
    except Full:
        return False
    except Exception:
        return False


# =============================================================================
# CONFIGURATION AND VALUE RESOURCES
# =============================================================================

def _config_persistence_receipt_path(app_config):
    sections = _runtime_sections(app_config)
    file_sync = sections["config_file_sync"]
    if not _safe_bool(file_sync.get("enabled"), False):
        return None
    node_id = str(sections["app"].get("node_id") or "node-unknown")
    configured = str(
        file_sync.get("persistence_receipt_path")
        or "_runtime/config_persistence_{node_id}.json").format(
            node_id=node_id)
    return _absolute_project_path(configured, configured)


def _data_center_options(app_config):
    sections = _runtime_sections(app_config)
    runtime_config = _mapping(sections["runtime"], "data_center")
    allow_legacy_import = _safe_bool(
        runtime_config.get("allow_legacy_import"), False)
    legacy_import_profile = str(
        runtime_config.get("legacy_import_profile")
        or "production_v2").strip().lower()
    if legacy_import_profile not in ("lab_v2", "production_v2"):
        raise ValueError(
            "legacy_import_profile must be lab_v2 or production_v2")
    configured_path = runtime_config.get("json_path")
    if not configured_path:
        configured_path = sections["paths"].get("device_state_json")
    json_path = _absolute_project_path(
        configured_path,
        "_config/config_data/device_state_data_v9_node_fn-01.json",
    )
    path_parts = str(json_path).replace("\\", "/").split("/")
    if "_legacy" in path_parts and not allow_legacy_import:
        raise ValueError(
            "legacy config path requires allow_legacy_import: true")
    sqlite_path = runtime_config.get("sqlite_path")
    if sqlite_path:
        sqlite_path = _absolute_project_path(sqlite_path, sqlite_path)
    return {
        "name": "runtime-config-master",
        "json_path": json_path,
        "json_receipt_path": _config_persistence_receipt_path(app_config),
        "sqlite_path": sqlite_path,
        "sqlite_enabled": _safe_bool(
            runtime_config.get("sqlite_enabled"), False),
        "json_style": str(runtime_config.get("json_style") or "table"),
        "indent_size": int(runtime_config.get("indent_size") or 2),
        "sort_mode": str(runtime_config.get("sort_mode") or "fingerprint_safe"),
        "verify_on_load": _safe_bool(runtime_config.get("verify_on_load"), True),
        "verify_on_save": _safe_bool(runtime_config.get("verify_on_save"), True),
        "preserve_config_identity_on_save": _safe_bool(
            runtime_config.get("preserve_config_identity_on_save"), True),
        "allow_legacy_import": allow_legacy_import,
        "legacy_import_profile": legacy_import_profile,
        "autosave_json": False,
        "autosync_sqlite": False,
    }


def _create_data_center(app_config):
    center = data_center.create_data_center_context(**_data_center_options(app_config))
    report = data_center.load_json_into_heap(center)
    if report.get("status") != "OK":
        raise RuntimeError("Data Center load failed: " + report.get("message", ""))
    found, snapshot = data_center.heap_snapshot(center)
    if not found or not _is_dict(snapshot):
        raise RuntimeError("Data Center returned no configuration snapshot")
    return center, snapshot, report


def _value_persistence_options(app_config):
    runtime = _mapping(app_config, "runtime")
    values = _mapping(runtime, "value_store")
    persistence = _mapping(values, "persistence")
    file_path = _absolute_project_path(
        persistence.get("json_path"),
        "_runtime/sensor_values.json",
    )
    return {
        "enabled": _safe_bool(persistence.get("enabled"), True),
        "file_path": file_path,
        "flush_interval_s": _safe_float(persistence.get("flush_interval_s"), 0.5),
        "batch_size": _safe_int(persistence.get("batch_size"), 256, 1),
    }


def _sem_options(app_config):
    sections = _runtime_sections(app_config)
    options = dict(sections["state_event_management"])
    options.setdefault("join_timeout_s", sections["shutdown"].get("join_timeout_s", 5.0))
    return options


def _tm_options(app_config):
    sections = _runtime_sections(app_config)
    options = dict(sections["thread_management"])
    options.setdefault("node_id", sections["app"].get("node_id", ""))
    options.setdefault(
        "safety", sections["state_event_management"].get("safety", {}))
    options.setdefault("join_timeout_s", sections["shutdown"].get("join_timeout_s", 5.0))
    return options


def _mqtt_options(app_config):
    sections = _runtime_sections(app_config)
    options = copy.deepcopy(sections["mqtt_interface"])
    app = sections["app"]
    options.setdefault("node_id", app.get("node_id", "node-unknown"))
    options.setdefault("site_id", app.get("site_id", "site-main"))
    transport = _mapping(options, "transport")
    transport = copy.deepcopy(transport)
    tls = copy.deepcopy(_mapping(transport, "tls"))
    for key in ("ca_file", "cert_file", "key_file"):
        if tls.get(key):
            tls[key] = _absolute_project_path(tls[key], tls[key])
    transport["tls"] = tls
    options["transport"] = transport
    return options


def _distributed_options(app_config):
    sections = _runtime_sections(app_config)
    options = copy.deepcopy(sections["distributed_runtime"])
    app = sections["app"]
    node_id = str(app.get("node_id") or "node-unknown")
    options.setdefault("node_id", node_id)
    options.setdefault("site_id", str(app.get("site_id") or "site-main"))
    data_center_config = _mapping(sections["runtime"], "data_center")
    options.setdefault(
        "authority_validation_profile",
        data_center_config.get("legacy_import_profile") or "production_v2",
    )
    configured = options.get("journal_path")
    if configured:
        configured = str(configured).format(node_id=node_id)
    else:
        configured = "_runtime/distributed_operations_{}.sqlite3".format(node_id)
    options["journal_path"] = _absolute_project_path(configured, configured)
    options.setdefault("join_timeout_s", sections["shutdown"].get("join_timeout_s", 5.0))
    return options


def _config_file_sync_options(app_config):
    sections = _runtime_sections(app_config)
    options = copy.deepcopy(sections["config_file_sync"])
    node_id = str(sections["app"].get("node_id") or "node-unknown")
    data_center_options = _data_center_options(app_config)
    configured_wal = str(
        options.get("wal_path")
        or "_runtime/config_file_sync_{node_id}.json").format(node_id=node_id)
    options["node_id"] = node_id
    options["config_authority"] = str(
        sections["distributed_runtime"].get("config_authority")
        or "node_local")
    options["config_path"] = data_center_options["json_path"]
    options["wal_path"] = _absolute_project_path(configured_wal, configured_wal)
    options["persistence_receipt_path"] = data_center_options[
        "json_receipt_path"]
    options.setdefault(
        "join_timeout_s", sections["shutdown"].get("join_timeout_s", 5.0))
    return options


def _cross_options(app_config):
    sections = _runtime_sections(app_config)
    options = copy.deepcopy(sections["cross_runtime"])
    options.setdefault("node_id", sections["app"].get("node_id", ""))
    configured = options.get("config_path")
    if configured:
        options["config_path"] = _absolute_project_path(configured, configured)
    node_id = str(options.get("node_id") or "node-unknown")
    domain_id = str(options.get("domain_id") or "cross-default")
    snapshot_path = options.get("snapshot_path")
    if snapshot_path:
        snapshot_path = str(snapshot_path).format(
            node_id=node_id,
            domain_id=domain_id,
        )
    else:
        snapshot_path = "_runtime/cross_snapshot_{}_{}.json".format(
            node_id,
            domain_id,
        )
    options["snapshot_path"] = _absolute_project_path(
        snapshot_path, snapshot_path)
    options.setdefault(
        "join_timeout_s", sections["shutdown"].get("join_timeout_s", 5.0))
    return options


# =============================================================================
# RUNTIME CONTEXT AND QUEUES
# =============================================================================

def create_runtime_context(main_ctx):
    if not _is_dict(main_ctx):
        raise TypeError("main_ctx must be a dict")
    shutdown_event = main_ctx.get("shutdown_event")
    if not _valid_shutdown_event(shutdown_event):
        raise ValueError("System bootstrap must provide shutdown_event")
    if shutdown_event.is_set():
        raise RuntimeError("Runtime cannot start after system shutdown")
    app_config = main_ctx.get("app_config") or {}
    if not _is_dict(app_config):
        raise TypeError("main_ctx app_config must be a dict")
    runtime_id = "runtime-{}".format(time.time_ns())
    options = _build_options(app_config)
    store = create_runtime_value_store(
        name=runtime_id + ":sensor_values",
        change_queue_maxsize=options["value_change_queue_maxsize"],
        sequence_state_path=options["sequence_state_path"],
        sequence_reservation_size=options["sequence_reservation_size"],
    )
    return {
        "runtime_id": runtime_id,
        "version": CONTEXT["library_version"],
        "state": "created",
        "created_at": time.time(),
        "started_at": None,
        "stopped_at": None,
        "last_error": None,
        "degraded_components": {},
        "lock": threading.RLock(),
        "system_shutdown_event": shutdown_event,
        "app_config": copy.deepcopy(app_config),
        "options": options,
        "queues": {},
        "queue_names": {},
        "components": {},
        "component_order": [],
        "component_stop_events": {},
        "_runtime_value_store": store,
        "sensor_value_capabilities": create_sensor_value_capabilities(store),
        "sensor_value_persistence": None,
        "mqtt_interface": None,
        "distributed_coordinator": None,
        "config_file_sync": None,
        "cross_worker": None,
        "cross_capabilities": {},
        "_data_center": None,
        "config_capabilities": {},
    }


def _create_queues(context):
    for canonical_name, queue_type in QUEUE_SPECS:
        queue_object, registered_name = create_queue(
            name="{}:{}".format(context["runtime_id"], canonical_name),
            queue_type=queue_type,
            maxsize=context["options"]["queue_maxsize"],
        )
        context["queues"][canonical_name] = queue_object
        context["queue_names"][canonical_name] = registered_name


def _remove_queues(context):
    for registered_name in tuple(context["queue_names"].values()):
        remove_queue(registered_name)
    context["queues"].clear()
    context["queue_names"].clear()


def _register_component(context, name, component):
    context["components"][name] = component
    context["component_order"].append(name)
    return component


# =============================================================================
# COMPONENT STARTUP
# =============================================================================

def _load_router_target():
    from src.core._event_router import sync_event_router
    return sync_event_router


def _start_router(context, target):
    if not callable(target):
        raise TypeError("router_target must be callable")
    if not context["options"]["router_enabled"]:
        raise RuntimeError("Event Router is required")
    stop_event = threading.Event()
    thread = threading.Thread(
        target=partial(
            target,
            shutdown_event=stop_event,
            queues_dict=context["queues"],
            router_name="RuntimeEventRouter",
            num_threads=context["options"]["router_worker_threads"],
            ingress_timeout_s=context["options"]["router_ingress_timeout_s"],
            enable_priority_routing=context["options"]["router_priority_enabled"],
            priority_batch_max=context["options"]["router_priority_batch_max"],
            enable_process_broadcast=False,
            enable_telemetry_mirror=False,
            enable_worker_mirror=False,
            enable_config_mirrors=False,
            enable_automation_status_mirror=False,
        ),
        name=context["options"]["router_thread_name"],
        daemon=False,
    )
    context["component_stop_events"]["event_router"] = stop_event
    thread.start()
    return _register_component(context, "event_router", {
        "thread": thread, "context": None, "state": "running"})


def _start_sem(context, target):
    if not callable(target):
        raise TypeError("state_event_management_start must be callable")
    stop_event = threading.Event()
    runtime = {}
    thread = target(
        data_center=context["_data_center"],
        queues=context["queues"],
        stop_event=stop_event,
        system_shutdown_event=context["system_shutdown_event"],
        options=_sem_options(context["app_config"]),
        sensor_value_reader=context["sensor_value_capabilities"].get(
            "get_sensor_snapshot"),
        runtime=runtime,
    )
    sem_context = runtime.get("context")
    if not _thread_alive(thread) or not _is_dict(sem_context):
        raise RuntimeError("State Event Management did not start correctly")
    context["component_stop_events"]["state_event_management"] = stop_event
    context["config_capabilities"] = create_config_capabilities(sem_context)
    return _register_component(context, "state_event_management", {
        "thread": thread, "context": sem_context, "state": "running"})


def _mqtt_runtime_status(context):
    revision = None
    reader = context.get("config_capabilities", {}).get("get_revision")
    if callable(reader):
        revision = reader()
    tm = context["components"].get("thread_management") or {}
    tm_context = tm.get("context") or {}
    return {
        "runtime_id": context["runtime_id"],
        "state": context["state"],
        "config_revision": revision,
        "thread_management": tm_context.get("state", "not_started"),
        "system_shutdown": context["system_shutdown_event"].is_set(),
    }


def _prepare_mqtt(context, factory, transport_factory=None):
    options = _mqtt_options(context["app_config"])
    if not _safe_bool(options.get("enabled"), False):
        return None
    if not callable(factory):
        raise TypeError("mqtt_interface_create must be callable")
    try:
        api = factory(
            options=options,
            config_capabilities=context["config_capabilities"],
            runtime_status_reader=partial(_mqtt_runtime_status, context),
            transport_factory=transport_factory,
        )
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "MQTT identity preflight failed: {}. Install the Worker mTLS "
            "identity with ./install_mqtt_identity.sh or intentionally use "
            "AUTOMATION_PROFILE=standalone.".format(exc)
        ) from exc
    mqtt_context = api.get("context") if _is_dict(api) else None
    if not _is_dict(mqtt_context):
        raise RuntimeError("MQTT interface returned no context")
    context["mqtt_interface"] = api
    sem = context["components"]["state_event_management"]["context"]
    register_external_publisher(sem, api.get("publish_event"))
    return _register_component(context, "mqtt_interface", {
        "thread": None,
        "context": mqtt_context,
        "api": api,
        "state": "prepared",
    })


def _start_prepared_mqtt(context, target):
    component = context["components"].get("mqtt_interface")
    if component is None:
        return None
    if not callable(target):
        raise TypeError("mqtt_interface_start must be callable")
    thread = target(component["context"])
    component["thread"] = thread
    component["state"] = "running"
    options = component["context"]["options"]
    startup_required = bool(options.get("startup_connection_required"))
    probe_timeout = options.get("connect_wait_s") if startup_required else 0.1
    connected = wait_mqtt_interface_connected(
        component["context"], probe_timeout)
    if startup_required and not connected:
        raise RuntimeError("required MQTT broker connection is unavailable")
    if not startup_required and not connected:
        component["state"] = "degraded"
        LOGGER.warning(
            "MQTT broker is not connected during startup; local Worker "
            "automation remains active and MQTT reconnects in the background")
    return thread


def _prepare_distributed(context, factory):
    options = _distributed_options(context["app_config"])
    thread_options = _tm_options(context["app_config"])
    if "controller_allowlist" in thread_options:
        options["controller_allowlist"] = copy.deepcopy(
            thread_options.get("controller_allowlist") or ())
    if not _safe_bool(options.get("enabled"), False):
        return None
    mqtt = context.get("mqtt_interface") or {}
    if not callable(mqtt.get("publish_command")):
        raise RuntimeError("Distributed runtime requires enabled MQTT interface")
    if not callable(factory):
        raise TypeError("distributed coordinator factory must be callable")
    api = factory(
        options=options,
        mqtt_api=mqtt,
        config_capabilities=context["config_capabilities"],
        sensor_value_capabilities=context["sensor_value_capabilities"],
        runtime_event_sink=partial(
            sync_queue_put, context["queues"]["queue_event_send"]),
    )
    distributed_context = api.get("context") if _is_dict(api) else None
    if not _is_dict(distributed_context):
        raise RuntimeError("Distributed coordinator returned no context")
    context["distributed_coordinator"] = api
    sem = context["components"]["state_event_management"]["context"]
    register_post_commit_handler(sem, api.get("handle_config_commit"))
    register_write_authorizer(sem, api.get("authorize_sem_write"))
    return _register_component(context, "distributed_coordinator", {
        "thread": None,
        "context": distributed_context,
        "api": api,
        "state": "prepared",
    })


def _start_prepared_distributed(context, target):
    component = context["components"].get("distributed_coordinator")
    if component is None:
        return None
    if not callable(target):
        raise TypeError("distributed coordinator start must be callable")
    thread = target(component["context"])
    component["thread"] = thread
    component["state"] = "running"
    return thread


def _prepare_config_file_sync(context):
    options = _config_file_sync_options(context["app_config"])
    if not _safe_bool(options.get("enabled"), False):
        return None
    distributed = context.get("distributed_coordinator") or {}
    if not distributed:
        LOGGER.warning(
            "Config file sync is disabled because the distributed runtime is "
            "not active; local automation remains available"
        )
        return None
    api = create_config_file_sync(
        options=options,
        config_capabilities=context["config_capabilities"],
        distributed_api=distributed,
    )
    file_context = api["context"]
    prepare_startup_config_file_sync(file_context)
    context["config_file_sync"] = api
    return _register_component(context, "config_file_sync", {
        "thread": None, "context": file_context, "api": api, "state": "prepared"})


def _start_prepared_config_file_sync(context):
    component = context["components"].get("config_file_sync")
    if component is None:
        return None
    thread = start_config_file_sync(component["context"])
    component["thread"] = thread
    component["state"] = "running"
    return thread


def _tm_resources(context):
    resources = {
        "runtime_id": context["runtime_id"],
        "sensor_value_store": dict(context["sensor_value_capabilities"]),
        "config_writer": dict(context["config_capabilities"]),
    }
    mqtt = context.get("mqtt_interface") or {}
    if callable(mqtt.get("mirror_event")):
        resources["external_event_mirror"] = mqtt["mirror_event"]
    distributed = context.get("distributed_coordinator") or {}
    distributed_context = distributed.get("context") or {}
    distributed_options = distributed_context.get("options") or {}
    distributed_enabled = bool(distributed_options.get("enabled"))
    distributed_role = distributed_options.get("role")
    local_automation_enabled = bool(
        distributed_options.get("local_automation_enabled"))
    pipeline_enabled = (
        not distributed_enabled
        or (
            distributed_role == "worker"
            and local_automation_enabled
        )
    )
    resources["sensor_pipeline_enabled"] = pipeline_enabled
    resources["automation_pipeline_enabled"] = pipeline_enabled
    log_method = LOGGER.info if pipeline_enabled else LOGGER.error
    log_method(
        "Local automation pipeline: %s distributed=%s role=%s "
        "local_automation_enabled=%s config_sync_mode=%s",
        "ENABLED" if pipeline_enabled else "DISABLED",
        distributed_enabled,
        distributed_role,
        local_automation_enabled,
        distributed_options.get("config_sync_mode"),
    )
    if distributed_enabled and callable(distributed.get("request_action_plan")):
        resources["config_writer"]["request_action_plan"] = distributed[
            "request_action_plan"]
    if distributed_enabled and callable(distributed.get("dispatch_remote_actuator")):
        resources["remote_actuator_dispatcher"] = distributed[
            "dispatch_remote_actuator"]
    if distributed_enabled and callable(distributed.get("report_actuator_result")):
        resources["actuator_result_reporter"] = distributed[
            "report_actuator_result"]
    if distributed_enabled and callable(distributed.get("validate_actuator_write")):
        resources["validate_actuator_write"] = distributed[
            "validate_actuator_write"]
    return resources


def _start_tm(context, target, **kwargs):
    if not callable(target):
        raise TypeError("thread_management_start must be callable")
    stop_event = threading.Event()
    runtime = {}
    found, config = data_center.heap_snapshot(context["_data_center"])
    if not found:
        raise RuntimeError("Runtime config snapshot is unavailable")
    thread = target(
        config=config,
        resources=_tm_resources(context),
        queues=context["queues"],
        stop_event=stop_event,
        system_shutdown_event=context["system_shutdown_event"],
        options=_tm_options(context["app_config"]),
        controller_target=kwargs.get("controller_target"),
        runtime=runtime,
    )
    tm_context = runtime.get("context")
    if not _thread_alive(thread) or not _is_dict(tm_context):
        raise RuntimeError("Thread Management did not start correctly")
    context["component_stop_events"]["thread_management"] = stop_event
    return _register_component(context, "thread_management", {
        "thread": thread, "context": tm_context, "state": "running"})


def _wire_config_rollout(context):
    sem_context = context["components"]["state_event_management"]["context"]
    tm_context = context["components"]["thread_management"]["context"]
    register_rollout_handler(sem_context, partial(apply_config_rollout, tm_context))
    result = request_config_sync(sem_context, source="runtime_bootstrap")
    if not result.get("ok"):
        raise RuntimeError("Initial config rollout failed: " + str(result))


def _wire_distributed_controller_dispatch(context):
    distributed = context.get("distributed_coordinator") or {}
    setter = distributed.get("set_local_dispatcher")
    if not callable(setter):
        return False
    tm_context = context["components"]["thread_management"]["context"]
    return setter(partial(dispatch_local_actuator_task, tm_context))


def _load_cross_config(context, options):
    configured = options.get("config_path")
    if configured:
        with open(configured, "r", encoding="utf-8") as handle:
            source = json.load(handle)
        report = type_library.verify_storage_config_data(source)
        if report.get("status") != "OK":
            raise RuntimeError("Cross storage config validation failed")
        source = type_library.normalize_config_for_runtime(
            source, representation="storage")
    else:
        found, source = data_center.heap_snapshot(context["_data_center"])
        if not found:
            raise RuntimeError("Cross runtime config snapshot is unavailable")
    meta = source.get("meta") if _is_dict(source) else None
    meta = meta[0] if isinstance(meta, list) and meta else {}
    if _is_dict(meta) and str(meta.get("scope") or "") == "cross":
        cross_config = copy.deepcopy(source)
    else:
        cross_config = build_cross_execution_shard(
            source, options.get("node_id"))
    validation = validate_cross_execution_shard(
        cross_config, options.get("node_id"))
    if not validation.get("ok"):
        raise RuntimeError(
            "Cross shard validation failed: "
            + "; ".join(validation.get("errors") or ()))
    return cross_config


def _cross_remote_dispatch(context, task):
    distributed = context.get("distributed_coordinator") or {}
    dispatcher = distributed.get("dispatch_remote_actuator")
    if not callable(dispatcher):
        return False
    return bool(dispatcher(copy.deepcopy(task)))


def _cross_state_publish(context, event):
    mqtt = context.get("mqtt_interface") or {}
    publisher = mqtt.get("mirror_event")
    if not callable(publisher):
        return False
    return bool(publisher(copy.deepcopy(event)))


def _prepare_cross_worker(context):
    options = _cross_options(context["app_config"])
    if not _safe_bool(options.get("enabled"), False):
        return None
    distributed = context.get("distributed_coordinator") or {}
    distributed_context = distributed.get("context") or {}
    if (distributed_context.get("options") or {}).get("role") == "coordinator":
        raise RuntimeError("Coordinator role may not execute a Cross Worker")
    tm_component = context["components"].get("thread_management") or {}
    tm_context = tm_component.get("context") or {}
    if not _is_dict(tm_context) or not _is_dict(tm_context.get("pool")):
        raise RuntimeError("Cross runtime requires Thread Management pool")
    cross_config = _load_cross_config(context, options)
    revision = _safe_int(options.get("initial_revision"), 0, 0)
    package = {
        "config": cross_config,
        "revision": revision,
        "execution_epoch": _safe_int(options.get("initial_execution_epoch"), 0, 0),
        "fingerprint": type_library.config_fingerprint(cross_config),
        "classification_fingerprint": type_library.fingerprint(
            classify_config_scopes(cross_config)),
    }
    cross_context = create_cross_worker(
        options=options,
        package=package,
        pool=tm_context["pool"],
        local_dispatcher=partial(dispatch_local_actuator_task, tm_context),
        remote_dispatcher=partial(_cross_remote_dispatch, context),
        state_publisher=partial(_cross_state_publish, context),
    )
    capabilities = create_cross_capabilities(cross_context)
    context["cross_worker"] = cross_context
    context["cross_capabilities"] = capabilities
    set_cross_dispatchers(
        tm_context,
        event_dispatcher=capabilities["dispatch_event"],
        result_dispatcher=capabilities["dispatch_pool_result"],
    )
    distributed = context.get("distributed_coordinator") or {}
    setter = distributed.get("set_cross_callbacks")
    if callable(setter):
        setter(
            event_handler=capabilities["dispatch_event"],
            config_applier=capabilities["apply_config"],
            freeze_execution=capabilities["freeze_execution"],
            status_reader=capabilities["snapshot"],
        )
    thread = start_cross_worker(cross_context)
    if not _thread_alive(thread):
        raise RuntimeError("Cross Worker did not start correctly")
    context["component_stop_events"]["cross_worker"] = cross_context[
        "stop_event"]
    return _register_component(context, "cross_worker", {
        "thread": thread,
        "context": cross_context,
        "state": "running",
    })


def _prepare_cross_worker_resilient(context):
    try:
        return _prepare_cross_worker(context)
    except Exception as exc:
        error = {
            "type": type(exc).__name__,
            "message": str(exc),
            "timestamp": time.time(),
        }
        context["degraded_components"]["cross_worker"] = error
        LOGGER.error(
            "Cross Worker is unavailable (%s): %s; isolated Cross "
            "coordination is disabled while node-local automation continues",
            type(exc).__name__,
            exc,
        )
        return _register_component(context, "cross_worker", {
            "thread": None,
            "context": None,
            "state": "degraded",
            "error": error,
        })


def _start_value_persistence(context):
    options = _value_persistence_options(context["app_config"])
    if not options["enabled"]:
        return None
    persistence = create_sensor_value_persistence(
        context["_runtime_value_store"],
        options["file_path"],
        flush_interval_s=options["flush_interval_s"],
        batch_size=options["batch_size"],
    )
    thread = start_sensor_value_persistence(persistence)
    context["sensor_value_persistence"] = persistence
    return _register_component(context, "sensor_value_persistence", {
        "thread": thread, "context": persistence, "state": "running"})


# =============================================================================
# STAGED SHUTDOWN AND ROLLBACK
# =============================================================================

def _stop_tm(context):
    component = context["components"].get("thread_management")
    if not component:
        return
    snapshot = stop_thread_management(
        context=component.get("context"),
        join_timeout_s=context["options"]["join_timeout_s"],
    )
    component["state"] = snapshot.get("state", "stopped")


def _stop_cross_worker(context):
    component = context["components"].get("cross_worker")
    if not component:
        return
    snapshot = stop_cross_worker(
        component.get("context"),
        join_timeout_s=context["options"]["join_timeout_s"],
    )
    component["state"] = snapshot.get("state", "stopped")


def _quiesce_mqtt(context):
    mqtt = context.get("mqtt_interface") or {}
    quiesce = mqtt.get("quiesce")
    if callable(quiesce):
        quiesce(timeout_s=context["options"]["join_timeout_s"])


def _stop_mqtt(context):
    component = context["components"].get("mqtt_interface")
    if not component:
        return
    snapshot = stop_mqtt_interface(
        component.get("context"),
        join_timeout_s=context["options"]["join_timeout_s"],
    )
    component["state"] = snapshot.get("state", "stopped")


def _quiesce_distributed(context):
    distributed = context.get("distributed_coordinator") or {}
    quiesce = distributed.get("quiesce")
    if callable(quiesce):
        quiesce(timeout_s=context["options"]["join_timeout_s"])


def _stop_distributed(context):
    component = context["components"].get("distributed_coordinator")
    if not component:
        return
    snapshot = stop_distributed_coordinator(
        component.get("context"),
        join_timeout_s=context["options"]["join_timeout_s"],
    )
    component["state"] = snapshot.get("state", "stopped")


def _stop_config_file_sync(context):
    component = context["components"].get("config_file_sync")
    if not component:
        return
    snapshot = stop_config_file_sync(
        component.get("context"), timeout_s=context["options"]["join_timeout_s"])
    component["state"] = snapshot.get("state", "stopped")


def _stop_sem(context):
    component = context["components"].get("state_event_management")
    if not component:
        return
    snapshot = stop_state_event_management(
        component.get("context"),
        join_timeout_s=context["options"]["join_timeout_s"],
    )
    component["state"] = snapshot.get("state", "stopped")


def _stop_value_persistence(context):
    persistence = context.get("sensor_value_persistence")
    if persistence is None:
        return
    snapshot = stop_sensor_value_persistence(
        persistence, join_timeout_s=context["options"]["join_timeout_s"])
    component = context["components"].get("sensor_value_persistence") or {}
    component["state"] = snapshot.get("state", "stopped")


def _stop_router(context):
    component = context["components"].get("event_router")
    if not component:
        return
    stop_event = context["component_stop_events"].get("event_router")
    if stop_event is not None:
        stop_event.set()
    thread = component.get("thread")
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=context["options"]["join_timeout_s"])
    component["state"] = "stuck" if _thread_alive(thread) else "stopped"


def _close_value_store_resource(context):
    close_runtime_value_store(context["_runtime_value_store"])


def _remove_runtime_queues(context):
    _remove_queues(context)


def _run_shutdown_step(context, name, target, errors):
    try:
        target(context)
        LOGGER.debug("Runtime shutdown step completed: %s", name)
    except Exception as exc:
        errors.append({"step": name, "error": str(exc)})
        LOGGER.error("Runtime shutdown step failed (%s): %s", name, exc)


def _staged_stop(context):
    errors = []
    steps = (
        ("config_file_sync", _stop_config_file_sync),
        ("mqtt_quiesce", _quiesce_mqtt),
        ("distributed_quiesce", _quiesce_distributed),
        ("cross_worker", _stop_cross_worker),
        ("thread_management", _stop_tm),
        ("distributed_coordinator", _stop_distributed),
        ("mqtt_interface", _stop_mqtt),
        ("state_event_management", _stop_sem),
        ("sensor_value_persistence", _stop_value_persistence),
        ("event_router", _stop_router),
    )
    for name, target in steps:
        _run_shutdown_step(context, name, target, errors)
    context["system_shutdown_event"].set()
    _run_shutdown_step(
        context, "runtime_value_store", _close_value_store_resource, errors)
    _run_shutdown_step(context, "runtime_queues", _remove_runtime_queues, errors)
    if errors:
        raise RuntimeError(
            "runtime shutdown steps failed: "
            + ", ".join(item["step"] for item in errors))
    return True


def _rollback(context):
    LOGGER.info("Runtime rollback starting")
    try:
        _staged_stop(context)
        LOGGER.info("Runtime rollback completed")
    except Exception as exc:
        LOGGER.error("Runtime rollback completed with errors: %s", exc)
        context["system_shutdown_event"].set()


# =============================================================================
# DIAGNOSTICS AND EXTERNAL CAPABILITIES
# =============================================================================

def get_sensor_values_snapshot(runtime_ctx, controller_uuid=None, sensor_id=None):
    capabilities = runtime_ctx.get("sensor_value_capabilities") or {}
    if controller_uuid is None:
        reader = capabilities.get("get_all_snapshot")
        arguments = ()
    elif sensor_id is None:
        reader = capabilities.get("get_controller_snapshot")
        arguments = (controller_uuid,)
    else:
        reader = capabilities.get("get_sensor_snapshot")
        arguments = (controller_uuid, sensor_id)
    if not callable(reader):
        raise RuntimeError("sensor value reader is unavailable")
    return reader(*arguments)


def get_runtime_config_snapshot(runtime_ctx):
    capabilities = runtime_ctx.get("config_capabilities") or {}
    reader = capabilities.get("get_snapshot")
    if not callable(reader):
        raise RuntimeError("runtime config reader is unavailable")
    return reader()


def request_runtime_config_patch(runtime_ctx, path, value=None, **kwargs):
    capabilities = runtime_ctx.get("config_capabilities") or {}
    writer = capabilities.get("request_patch")
    if not callable(writer):
        raise RuntimeError("runtime config writer is unavailable")
    return writer(path, value, **kwargs)


def request_runtime_config_replace(runtime_ctx, config, **kwargs):
    capabilities = runtime_ctx.get("config_capabilities") or {}
    writer = capabilities.get("request_replace")
    if not callable(writer):
        raise RuntimeError("runtime config writer is unavailable")
    return writer(config, **kwargs)


def get_runtime_snapshot(runtime_ctx):
    if not _is_dict(runtime_ctx):
        return {"state": "missing", "components": {}}
    components = {}
    for name, component in runtime_ctx["components"].items():
        thread = component.get("thread")
        components[name] = {
            "state": component.get("state"),
            "thread_alive": _thread_alive(thread),
        }
    sem_component = runtime_ctx["components"].get("state_event_management")
    tm_component = runtime_ctx["components"].get("thread_management")
    value_health = runtime_ctx["sensor_value_capabilities"]["health_snapshot"]()
    persistence = runtime_ctx.get("sensor_value_persistence")
    mqtt = runtime_ctx.get("mqtt_interface")
    distributed = runtime_ctx.get("distributed_coordinator")
    cross_worker = runtime_ctx.get("cross_worker")
    config_file_sync = runtime_ctx.get("config_file_sync")
    return {
        "runtime_id": runtime_ctx["runtime_id"],
        "version": runtime_ctx["version"],
        "state": runtime_ctx["state"],
        "created_at": runtime_ctx["created_at"],
        "started_at": runtime_ctx["started_at"],
        "stopped_at": runtime_ctx["stopped_at"],
        "last_error": copy.deepcopy(runtime_ctx["last_error"]),
        "degraded_components": copy.deepcopy(
            runtime_ctx.get("degraded_components") or {}),
        "queue_names": dict(runtime_ctx["queue_names"]),
        "components": components,
        "data_center": (
            data_center.describe_data_center(runtime_ctx["_data_center"])
            if runtime_ctx.get("_data_center") else {"state": "missing"}
        ),
        "state_event_management": (
            get_state_event_management_snapshot(sem_component["context"])
            if sem_component else {"state": "missing"}
        ),
        "thread_management": (
            get_thread_management_snapshot(tm_component["context"])
            if tm_component else {"state": "missing"}
        ),
        "sensor_value_store": value_health,
        "sensor_value_persistence": (
            get_sensor_value_persistence_snapshot(persistence)
            if persistence else {"state": "disabled"}
        ),
        "mqtt_interface": (
            get_mqtt_interface_snapshot(mqtt)
            if mqtt else {"state": "disabled", "enabled": False}
        ),
        "distributed_coordinator": (
            get_distributed_snapshot(distributed)
            if distributed else {"state": "disabled", "enabled": False}
        ),
        "config_file_sync": (
            get_config_file_sync_snapshot(config_file_sync)
            if config_file_sync else {"state": "disabled", "enabled": False}
        ),
        "cross_worker": (
            get_cross_worker_snapshot(cross_worker)
            if cross_worker else {"state": "disabled", "enabled": False}
        ),
    }


# =============================================================================
# EXTERNAL LIFECYCLE
# =============================================================================

def start_runtime(main_ctx=None, **kwargs):
    main_ctx = {} if main_ctx is None else main_ctx
    existing = main_ctx.get("runtime") if _is_dict(main_ctx) else None
    if _is_dict(existing) and existing.get("state") in ("running", "degraded"):
        return main_ctx
    context = create_runtime_context(main_ctx)
    main_ctx["runtime"] = context
    context["state"] = "starting"
    try:
        center, config, report = _create_data_center(context["app_config"])
        context["_data_center"] = center
        context["data_center_load_report"] = report
        context["initial_config"] = config
        _create_queues(context)
        router_target = (
            kwargs.get("router_target")
            if "router_target" in kwargs
            else _load_router_target()
        )
        _start_router(context, router_target)
        _start_sem(
            context,
            kwargs.get("state_event_management_start") or start_state_event_management,
        )
        _prepare_mqtt(
            context,
            kwargs.get("mqtt_interface_create") or create_mqtt_interface,
            transport_factory=kwargs.get("mqtt_transport_factory"),
        )
        _prepare_distributed(
            context,
            kwargs.get("distributed_coordinator_create")
            or create_distributed_coordinator,
        )
        _prepare_config_file_sync(context)
        _start_tm(
            context,
            kwargs.get("thread_management_start") or start_thread_management,
            controller_target=kwargs.get("controller_target"),
        )
        _wire_distributed_controller_dispatch(context)
        _prepare_cross_worker_resilient(context)
        _wire_config_rollout(context)
        _start_prepared_mqtt(
            context,
            kwargs.get("mqtt_interface_start") or start_mqtt_interface,
        )
        _start_prepared_distributed(
            context,
            kwargs.get("distributed_coordinator_start")
            or start_distributed_coordinator,
        )
        _start_prepared_config_file_sync(context)
        _start_value_persistence(context)
        context["started_at"] = time.time()
        context["state"] = (
            "degraded" if context["degraded_components"] else "running")
        main_ctx["started_threads"] = [
            component["thread"] for component in context["components"].values()
            if component.get("thread") is not None
        ]
        main_ctx["runtime_bootstrap_version"] = CONTEXT["library_version"]
        return main_ctx
    except Exception as exc:
        context["state"] = "failed"
        context["last_error"] = {
            "type": type(exc).__name__, "message": str(exc), "timestamp": time.time()}
        LOGGER.error(
            "Runtime startup failed (%s): %s",
            type(exc).__name__, exc,
        )
        _rollback(context)
        main_ctx["started_threads"] = [
            component["thread"] for component in context["components"].values()
            if _thread_alive(component.get("thread"))
        ]
        if main_ctx["started_threads"]:
            context["last_error"]["rollback_threads_alive"] = [
                thread.name for thread in main_ctx["started_threads"]]
            LOGGER.error(
                "Runtime rollback left live threads: %s",
                ", ".join(context["last_error"]["rollback_threads_alive"]),
            )
        raise RuntimeError("Runtime bootstrap failed") from exc


def stop_runtime(main_ctx=None, **kwargs):
    if not _is_dict(main_ctx):
        raise TypeError("main_ctx must be a dict")
    context = main_ctx.get("runtime")
    if not _is_dict(context) or context.get("state") == "stopped":
        return main_ctx
    context["state"] = "stopping"
    _staged_stop(context)
    live = any(_thread_alive(component.get("thread"))
               for component in context["components"].values())
    context["stopped_at"] = time.time()
    context["state"] = "degraded" if live else "stopped"
    main_ctx["started_threads"] = [
        component["thread"] for component in context["components"].values()
        if _thread_alive(component.get("thread"))
    ]
    return main_ctx


