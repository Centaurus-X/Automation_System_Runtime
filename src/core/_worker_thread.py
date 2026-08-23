# -*- coding: utf-8 -*-

# src/core/_worker_thread.py


"""
 =============================================================================
 _WORKER_THREAD
 =============================================================================

 What it is:
     A functional lifecycle wrapper for exactly one isolated controller owner.

 Why it is needed:
     Controller polling, event handling, local config and fieldbus requests
     must never be shared between controller UUIDs. Thread Management owns the
     wrappers; each wrapper owns its queues, stop signal and detached config.

 How it works:
     The wrapper starts the existing controller entry point with a UUID-local
     projection. Config updates are synchronously acknowledged by the running
     controller listener. Shutdown affects only this controller component.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import logging
import threading
import time

from functools import partial
from queue import Queue

from src.library._state_shard_manager import build_controller_projection


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "worker_thread",
    "library_version": "2.1.0",
    "description": "Isolated controller worker lifecycle",
    "options": {
        "join_timeout_s": 5.0,
        "config_ack_timeout_s": 5.0,
    },
}


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _is_dict(value):
    return isinstance(value, dict)


def _safe_float(value, default):
    try:
        result = float(value)
    except Exception:
        return float(default)
    return result if result > 0.0 else float(default)


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _load_controller_target():
    from src.core._controller_thread import run_control_thread
    return run_control_thread


def _controller_record(config, controller_uuid):
    controllers = config.get("controllers") if _is_dict(config) else []
    for row in controllers or []:
        if _is_dict(row) and str(row.get("controller_uuid")) == str(controller_uuid):
            return copy.deepcopy(row)
    return {}


def _controller_id(controller_data):
    for key in ("local_controller_id", "controller_id", "numeric_pk"):
        if controller_data.get(key) is None:
            continue
        try:
            return int(controller_data[key])
        except Exception:
            continue
    return 0


# =============================================================================
# CONTEXT
# =============================================================================

def create_controller_worker(**kwargs):
    controller_uuid = str(kwargs.get("controller_uuid") or "").strip()
    if not controller_uuid:
        raise ValueError("controller_uuid is required")
    source = kwargs.get("config_package") or kwargs.get("config_snapshot") or {}
    projection = build_controller_projection(source, controller_uuid)
    if not projection:
        raise ValueError("controller UUID is absent from config: " + controller_uuid)
    controller_data = _controller_record(projection, controller_uuid)
    resources = copy.copy(kwargs.get("runtime_resources") or {})
    resources["worker_uuid"] = controller_uuid
    resources["controller_timing_defaults"] = copy.deepcopy(
        kwargs.get("controller_timing_defaults") or {}
    )
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "controller_uuid": controller_uuid,
        "controller_id": _controller_id(controller_data),
        "controller_data": controller_data,
        "config_data": projection,
        "config_lock": threading.RLock(),
        "event_queue": Queue(),
        "request_queue": kwargs.get("request_queue"),
        "queue_event_send": kwargs.get("queue_event_send"),
        "stop_event": threading.Event(),
        "thread": None,
        "state": "created",
        "started_at": None,
        "stopped_at": None,
        "runtime_resources": resources,
        "controller_target": kwargs.get("controller_target") or _load_controller_target(),
        "options": {
            "join_timeout_s": _safe_float(
                kwargs.get("join_timeout_s"), CONTEXT["options"]["join_timeout_s"]),
            "config_ack_timeout_s": _safe_float(
                kwargs.get("config_ack_timeout_s"),
                CONTEXT["options"]["config_ack_timeout_s"],
            ),
        },
        "local_sensor_values": {},
        "local_actuator_values": {},
        "sensor_values": {},
        "local_sensor_lock": threading.RLock(),
        "local_actuator_lock": threading.RLock(),
        "sensor_lock": threading.RLock(),
        "statistics": {
            "config_updates": 0,
            "config_update_errors": 0,
            "events_dispatched": 0,
        },
        "last_error": None,
    }
    return context


def _controller_arguments(context):
    return {
        "controller_id": context["controller_id"],
        "controller_data": context["controller_data"],
        "config_data": context["config_data"],
        "config_lock": context["config_lock"],
        "sensor_values": context["sensor_values"],
        "sensor_lock": context["sensor_lock"],
        "queue_event_send": context["queue_event_send"],
        "event_queue_mbc": context["event_queue"],
        "local_sensor_values": context["local_sensor_values"],
        "local_sensor_lock": context["local_sensor_lock"],
        "local_actuator_values": context["local_actuator_values"],
        "local_actuator_lock": context["local_actuator_lock"],
        "shutdown_event": context["stop_event"],
        "request_queue": context["request_queue"],
        "runtime_resources": context["runtime_resources"],
    }


# =============================================================================
# LIFECYCLE AND CONFIG SYNCHRONISATION
# =============================================================================

def start_controller_worker(context):
    if not _is_dict(context):
        raise TypeError("controller worker context must be a dict")
    if _thread_alive(context.get("thread")):
        return context["thread"]
    target = context["controller_target"]
    if not callable(target):
        raise TypeError("controller_target must be callable")
    thread = threading.Thread(
        target=partial(target, **_controller_arguments(context)),
        name="ControllerWorker-" + context["controller_uuid"],
        daemon=False,
    )
    context["thread"] = thread
    context["state"] = "running"
    context["started_at"] = time.time()
    thread.start()
    return thread


def apply_controller_config(context, package, **kwargs):
    projection = build_controller_projection(package, context["controller_uuid"])
    if not projection:
        context["statistics"]["config_update_errors"] += 1
        return {"ok": False, "error": "controller removed from config"}
    revision = (package.get("revision") or
                (projection.get("_runtime_meta") or {}).get("revision"))
    runtime_controller = context["runtime_resources"].get("controller_context")

    if not _thread_alive(context.get("thread")) or not _is_dict(runtime_controller):
        with context["config_lock"]:
            context["config_data"].clear()
            context["config_data"].update(copy.deepcopy(projection))
            updated = _controller_record(projection, context["controller_uuid"])
            context["controller_data"].clear()
            context["controller_data"].update(updated)
        context["statistics"]["config_updates"] += 1
        return {"ok": True, "revision": revision, "mode": "local_swap"}

    acknowledgement = {"event": threading.Event(), "result": None}
    event = {
        "event_type": "CONFIG_SHARD_UPDATE",
        "payload": {
            "config": projection,
            "revision": revision,
            "acknowledgement": acknowledgement,
        },
    }
    context["event_queue"].put(event)
    timeout = _safe_float(
        kwargs.get("timeout_s"), context["options"]["config_ack_timeout_s"])
    completed = acknowledgement["event"].wait(timeout)
    result = acknowledgement.get("result") or {
        "ok": False,
        "error": "controller config acknowledgement timed out",
    }
    if completed and result.get("ok"):
        context["statistics"]["config_updates"] += 1
    else:
        context["statistics"]["config_update_errors"] += 1
    return copy.deepcopy(result)


def dispatch_controller_event(context, event):
    if not _is_dict(context) or not _is_dict(event):
        return False
    context["event_queue"].put(copy.deepcopy(event))
    context["statistics"]["events_dispatched"] += 1
    return True


def stop_controller_worker(context, **kwargs):
    if not _is_dict(context):
        return {"state": "missing"}
    context["state"] = "stopping"
    context["stop_event"].set()
    context["event_queue"].put({"_type": "__STOP__", "_sentinel": True})
    thread = context.get("thread")
    timeout = _safe_float(
        kwargs.get("join_timeout_s"), context["options"]["join_timeout_s"])
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=timeout)
    context["state"] = "stuck" if _thread_alive(thread) else "stopped"
    context["stopped_at"] = time.time()
    return get_controller_worker_snapshot(context)


def get_controller_worker_snapshot(context):
    if not _is_dict(context):
        return {"state": "missing"}
    with context["config_lock"]:
        runtime_meta = copy.deepcopy(
            context["config_data"].get("_runtime_meta") or {})
    return {
        "controller_uuid": context["controller_uuid"],
        "controller_id": context["controller_id"],
        "state": context["state"],
        "thread_alive": _thread_alive(context.get("thread")),
        "started_at": context["started_at"],
        "stopped_at": context["stopped_at"],
        "config": runtime_meta,
        "statistics": dict(context["statistics"]),
        "last_error": copy.deepcopy(context["last_error"]),
    }


def create_controller_worker_capabilities(context):
    return {
        "start": partial(start_controller_worker, context),
        "apply_config": partial(apply_controller_config, context),
        "dispatch_event": partial(dispatch_controller_event, context),
        "stop": partial(stop_controller_worker, context),
        "snapshot": partial(get_controller_worker_snapshot, context),
    }
