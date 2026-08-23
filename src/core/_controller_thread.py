# -*- coding: utf-8 -*-

# src/core/_controller_thread.py

"""
Controller-Thread – REFACTORED v4.0 (100% synchron)

Verantwortlichkeiten:
    - Periodisches Auslesen der Sensoren für DIESEN Controller
    - Empfängt Aktuator-Aufträge via Event-Queue (ACTUATOR_TASK)
    - Speichert Sensorwerte lokal + global (Lock-geschützt)
    - Sendet Events (SENSOR_VALUE_UPDATE, ACTUATOR_FEEDBACK, etc.)

ENTFERNT gegenüber v3:
    - asyncio (komplett) → threading + time.sleep
    - Observer (_observer_interface) → Event-Queue-Emission
    - pymodbus direkt → sync_execute() über Connection Layer
    - AsyncModbusTcpClient → Adapter in Schicht 1
    - command_delay() → timing_gate() aus _bus_timing
    - dual_channel_modbus_adapter → generischer bus_adapter

Architektur:
    - 2 Worker-Threads pro Controller:
      1. Sensor-Polling-Thread (deterministisch, blockierend)
      2. Event-Listener-Thread (blockierend auf Queue.get)
    - Main-Thread koordiniert Lifecycle + Shutdown
    - Kein OOP, kein Decorator, kein Lambda
    - functools.partial statt lambda
    - Blockierende Queues → CPU im Leerlauf ≈ 0%
"""


# =============================================================================
# Imports
# =============================================================================

import threading
import time
import uuid
import copy
import logging

from functools import partial
from queue import Queue, Empty
from datetime import datetime, timezone


def _sensor_sort_id(row):
    return int(row.get("sensor_id", 0))


def _actuator_sort_id(row):
    return int(row.get("actuator_id", 0))


def _item_key_as_text(item):
    return str(item[0])




from src.library._event_interface import make_event

from src.adapter._adapter_interface import (
    make_request,
    map_function_type_to_data_type,
    map_function_type_to_op,
)
from src.adapter._connection_thread import sync_execute
from src.adapter._bus_timing import (
    timing_gate,
    cycle_sleep,
    load_timing_profile_from_config,
    resolve_controller_comm_config,
    resolve_effective_controller_data,
)


logger = logging.getLogger(__name__)


# =============================================================================
# Konstanten
# =============================================================================

SENSOR_POLL_INTERVAL_DEFAULT = 0.1    # 100ms
SENSOR_POLL_INTERVAL_MIN = 0.01      # 10ms
STATUS_LOG_INTERVAL_S = 120.0        # Stats alle 2 Minuten


# =============================================================================
# Sentinel / Helper
# =============================================================================

def _is_stop_message(obj):
    try:
        return (isinstance(obj, dict)
                and obj.get("_type") == "__STOP__"
                and obj.get("_sentinel") is True)
    except Exception:
        return False


def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _safe_bool(value, default=False):
    """Best-effort bool conversion for configuration flags."""
    if isinstance(value, bool):
        return bool(value)
    if value is None:
        return bool(default)
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "y", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "n", "off", "disabled"):
            return False
    return bool(default)



def _controller_protocol(ctx):
    controller = ctx.get("controller_data") if isinstance(ctx, dict) else None
    controller = controller if isinstance(controller, dict) else {}
    protocol = str(
        controller.get("protocol_type") or controller.get("bus_type") or "modbus"
    ).strip().lower()
    aliases = {
        "modbus": "modbus_tcp",
        "s7": "plc_s7",
        "s7comm": "plc_s7",
        "tia": "plc_s7",
        "tia_portal": "plc_s7",
    }
    return aliases.get(protocol, protocol)


def _record_io_error(ctx):
    statistics = ctx.setdefault("statistics", {})
    statistics["io_errors"] = int(statistics.get("io_errors", 0)) + 1
    statistics["modbus_errors"] = int(statistics.get("modbus_errors", 0)) + 1

def _normalize_int_list(value):
    """Normalize a scalar/list/set config value into a de-duplicated int list."""
    if value is None:
        return []
    if isinstance(value, (str, int, float)):
        value = [value]
    if not isinstance(value, (list, tuple, set)):
        return []

    result = []
    seen = set()
    for item in value:
        item_int = _safe_int(item, None)
        if item_int is None or item_int in seen:
            continue
        seen.add(item_int)
        result.append(item_int)
    return result


def _normalize_control_type_set(value):
    """Normalize control strategy names used by automation force rules."""
    if value is None:
        value = ["PID", "PI", "PD"]
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set)):
        return {"PID", "PI", "PD"}

    result = set()
    for item in value:
        normalized = str(item or "").strip().upper()
        if normalized:
            result.add(normalized)
    if not result:
        result.add("PID")
        result.add("PI")
        result.add("PD")
    return result


def _resolve_force_automation_sensor_map(cfg):
    """Return sensor_id -> reason for deterministic control-loop polling.

    Normal sensors are event-driven and publish only on raw changes. Stateful
    PID/PI/PD loops must continue to run during constant phases as well;
    otherwise the filter chain, PID state and actuator refreshes stop although
    the controller is still alive.
    """
    if not isinstance(cfg, dict):
        return {}

    settings = cfg.get("automation_settings")
    if not isinstance(settings, dict):
        settings = {}

    force_map = {}
    for sid in _normalize_int_list(settings.get("force_automation_sensor_ids")):
        force_map[sid] = "automation_settings.force_automation_sensor_ids"

    force_controlled = _safe_bool(
        settings.get("force_automation_for_controlled_sensors"),
        True,
    )

    force_types = _normalize_control_type_set(
        settings.get("force_automation_control_strategy_types")
    )

    process_states = cfg.get("process_states") or []
    if not isinstance(process_states, list):
        return force_map

    for state in process_states:
        if not isinstance(state, dict):
            continue
        control_strategy = state.get("control_strategy") or {}
        if not isinstance(control_strategy, dict):
            continue
        control_type = str(control_strategy.get("type") or "").strip().upper()
        state_force_raw = control_strategy.get("force_automation", state.get("force_automation"))
        state_force = None
        if state_force_raw is not None:
            state_force = _safe_bool(state_force_raw, False)

        if control_type not in force_types:
            continue
        if state_force is False:
            continue
        if not force_controlled and state_force is not True:
            continue

        state_id = _safe_int(state.get("state_id"), None)
        reason = "control_strategy.%s" % control_type
        if state_id is not None:
            reason = "%s state=%s" % (reason, state_id)

        for sid in _normalize_int_list(state.get("sensor_ids")):
            force_map.setdefault(sid, reason)

    return force_map


def _first_of_pair(item):
    return item[0]


# =============================================================================
# Queue Helper (synchron, thread-safe)
# =============================================================================

def _queue_put_safe(q, item):
    """Non-blocking Queue.put mit Fallback auf timeout."""
    if q is None:
        return
    try:
        q.put_nowait(item)
    except Exception:
        try:
            q.put(item, timeout=0.2)
        except Exception:
            pass


def _emit_event(ctx, event_type, payload, *, target="pc"):
    """Sendet ein Event über queue_event_send (Thread-safe).

    Ersetzt Observer.notify_all() + async_queue_put().

    ROUTING-TARGETS (sync_event_router):
        "pc" / "tm" / "thread_management" → queue_event_pc  (TM liest hier)
        "state"                           → queue_event_state (SEM liest hier)
        "process_manager"                 → queue_event_proc  (PSM liest hier)

    WICHTIG: target="automation" → queue_event_mba wird von KEINEM Thread
    konsumiert und darf NICHT als Default verwendet werden!
    """
    try:
        evt = make_event(event_type, payload=payload, target=target)
        _queue_put_safe(ctx["queue_event_send"], evt)
    except Exception:
        pass


# =============================================================================
# Pair-Registry Helper (unverändert)
# =============================================================================

def _ensure_ctrl_task_pair_bucket(ctx):
    with ctx["pair_registry_lock"]:
        pair = ctx["ctrl_task_pair_registry"].get(ctx["controller_id"])
        if pair is None:
            ctx["ctrl_task_pair_registry"][ctx["controller_id"]] = ({}, threading.RLock())
            pair = ctx["ctrl_task_pair_registry"][ctx["controller_id"]]
    return pair


def _peek_ctrl_task(ctx, task_id):
    if not task_id:
        return None
    pair = _ensure_ctrl_task_pair_bucket(ctx)
    d, l = pair
    l.acquire()
    try:
        return d.get(str(task_id))
    finally:
        l.release()


def _pop_ctrl_task(ctx, task_id):
    """Poppt einen Task aus dem Pair-Store (thread-safe)."""
    if not task_id:
        return None
    pair = _ensure_ctrl_task_pair_bucket(ctx)
    d, l = pair
    l.acquire()
    try:
        return d.pop(str(task_id), None)
    finally:
        l.release()


# =============================================================================
# Epoch-Filter (unverändert)
# =============================================================================

def _accept_and_update_epoch(ctx, run_epoch_evt, thread_index=None):
    if not run_epoch_evt:
        return True
    epochs = ctx.setdefault("automation_epochs", {})
    tkey = "_global" if thread_index is None else str(thread_index)
    cur = epochs.get(tkey)
    if cur is None:
        epochs[tkey] = str(run_epoch_evt)
        return True
    return str(run_epoch_evt) == str(cur)


# =============================================================================
# Readback-Verification (unverändert)
# =============================================================================

def _cmd_readback_ok(function_type, command_value, readback_value):
    if readback_value is None:
        return False
    try:
        if function_type == "write_coil":
            return bool(readback_value) == bool(command_value)
        if function_type == "write_register":
            return int(readback_value) == int(command_value)
    except Exception:
        pass
    return readback_value == command_value


# =============================================================================
# Sensor Value Store
# =============================================================================

def update_sensor_value(sensor_values, sensor_lock, *,
                        controller_id, sensor_id, new_value):
    try:
        cid = int(controller_id)
    except Exception:
        cid = controller_id
    sid = str(sensor_id)
    ts = datetime.now(timezone.utc).isoformat()

    if sensor_values is None or sensor_lock is None:
        return False

    with sensor_lock:
        bucket = sensor_values.setdefault(cid, {})
        bucket[sid] = {
            "value": copy.deepcopy(new_value),
            "timestamp": ts,
        }
    return True


def _get_sensor_store_capabilities(ctx):
    resources = ctx.get("runtime_resources")

    if not isinstance(resources, dict):
        return {}

    capabilities = resources.get(
        "sensor_value_store"
    )

    if not isinstance(capabilities, dict):
        return {}

    return capabilities


def _get_controller_uuid(ctx):
    controller_data = ctx.get("controller_data") or {}
    controller_uuid = controller_data.get(
        "controller_uuid"
    )

    if controller_uuid is None:
        return None

    normalized = str(controller_uuid).strip()

    if not normalized:
        return None

    return normalized


def _claim_sensor_store_writer(ctx):
    capabilities = _get_sensor_store_capabilities(
        ctx
    )
    claim_writer = capabilities.get(
        "claim_writer"
    )
    create_token = capabilities.get(
        "create_writer_token"
    )

    if not callable(claim_writer):
        return {
            "ok": True,
            "enabled": False,
            "message": "runtime sensor store is not configured",
        }

    controller_uuid = _get_controller_uuid(ctx)

    if controller_uuid is None:
        return {
            "ok": False,
            "enabled": True,
            "message": "controller_uuid is required for sensor store",
        }

    writer_label = "controller_worker:{}".format(
        controller_uuid
    )

    if callable(create_token):
        writer_token = create_token(
            writer_label
        )
    else:
        writer_token = "{}:{}".format(
            writer_label,
            uuid.uuid4(),
        )

    result = claim_writer(
        {
            "controller_uuid": controller_uuid,
            "writer_label": writer_label,
        },
        writer_token,
    )

    if isinstance(result, dict) and result.get("ok"):
        ctx["sensor_store_writer_token"] = writer_token
        ctx["sensor_store_writer_claimed"] = True
        ctx["sensor_store_enabled"] = True

    return result


def _release_sensor_store_writer(ctx):
    if not ctx.get("sensor_store_writer_claimed"):
        return True

    capabilities = _get_sensor_store_capabilities(
        ctx
    )
    release_writer = capabilities.get(
        "release_writer"
    )

    if not callable(release_writer):
        return False

    result = release_writer(
        {
            "controller_uuid": _get_controller_uuid(ctx),
            "writer_label": "controller_worker",
        },
        ctx.get("sensor_store_writer_token"),
    )

    if isinstance(result, dict) and result.get("ok"):
        ctx["sensor_store_writer_claimed"] = False
        ctx["sensor_store_writer_token"] = None
        return True

    return False


def _mirror_legacy_sensor_value(ctx, sensor_id, value):
    try:
        return update_sensor_value(
            ctx.get("sensor_values"),
            ctx.get("sensor_lock"),
            controller_id=ctx.get("controller_id"),
            sensor_id=sensor_id,
            new_value=value,
        )

    except Exception:
        return False


def _write_central_sensor_value(ctx, sensor_id, value, **kwargs):
    capabilities = _get_sensor_store_capabilities(
        ctx
    )
    writer = capabilities.get(
        "write_sensor_value"
    )

    if not callable(writer):
        written = _mirror_legacy_sensor_value(
            ctx,
            sensor_id,
            value,
        )

        if not written:
            ctx["statistics"]["sensor_store_errors"] += 1

        return {
            "ok": bool(written),
            "legacy_fallback": True,
            "entry": {
                "controller_uuid": _get_controller_uuid(ctx),
                "sensor_id": sensor_id,
                "value": copy.deepcopy(value),
                "quality": kwargs.get("quality") or "unknown",
                "timestamp": kwargs.get("timestamp"),
            },
        }

    result = writer(
        controller_uuid=_get_controller_uuid(ctx),
        sensor_id=sensor_id,
        value=value,
        quality=kwargs.get("quality") or "good",
        timestamp=kwargs.get("timestamp"),
        source="controller_sensor_polling",
        metadata=kwargs.get("metadata") or {},
        writer_token=ctx.get(
            "sensor_store_writer_token"
        ),
    )

    if isinstance(result, dict) and result.get("ok"):
        ctx["statistics"]["sensor_store_writes"] += 1
        _mirror_legacy_sensor_value(
            ctx,
            sensor_id,
            value,
        )
        return result

    ctx["statistics"]["sensor_store_errors"] += 1

    if isinstance(result, dict):
        return result

    return {
        "ok": False,
        "message": "runtime sensor store returned an invalid result",
    }


# =============================================================================
# Config Helper (unverändert)
# =============================================================================

def _cfg_ro(ctx):
    with ctx["config_lock"]:
        src = ctx.get("config_data") or {}
        return copy.deepcopy(src)


def _get_controller_timing_defaults(ctx):
    """Return detached deployment defaults for this controller owner."""
    resources = ctx.get("runtime_resources")
    if not isinstance(resources, dict):
        return {}
    defaults = resources.get("controller_timing_defaults")
    if not isinstance(defaults, dict):
        return {}
    return copy.deepcopy(defaults)


def _get_comm_settings(ctx):
    """Liefert Kommunikations-Parameter aus der Live-Config.

    Wichtig:
    Controller-spezifische Patch-Sektionen (z. B. controllers_patch_example)
    werden hier explizit aufgelöst. Dadurch erreichen comm_profile /
    timing_profile / direkte Polling-Overrides zuverlässig den Controller-Thread.
    """
    try:
        from src.core._controller_thread_legacy import _get_comm_settings as _legacy
        return _legacy(ctx)
    except ImportError:
        pass

    base_cfg = {
        "sensor_read_interval": SENSOR_POLL_INTERVAL_DEFAULT,
        "actuator_write_interval": 0.2,
        "abs_time_out_ms": 150.0,
        "modbus_delay_read": 0.0,
        "modbus_delay_write": 0.0,
        "chk_ctrl_command": True,
        "actuator_feedback_stream": True,
        "sensor_block_size": 64,
        "batch_actuator": True,
        "batch_actuator_timeout": 0.5,
        "actuator_block_size": 64,
        "batch_actuator_flush_on_sensor_cycle": True,
    }

    effective_cfg = dict(base_cfg)

    try:
        with ctx["config_lock"]:
            cfg = ctx.get("config_data") or {}
            controller_seed = dict(ctx.get("controller_data") or {})
            if "controller_id" not in controller_seed and ctx.get("controller_id") is not None:
                controller_seed["controller_id"] = ctx.get("controller_id")

            resolved_cfg, _effective_controller, _profile_name = resolve_controller_comm_config(
                cfg,
                controller_seed,
                defaults=_get_controller_timing_defaults(ctx),
            )
            if isinstance(resolved_cfg, dict):
                effective_cfg.update(resolved_cfg)
    except Exception:
        pass

    def _f_first(source_keys, default):
        for source_key in source_keys:
            try:
                value = effective_cfg.get(source_key)
                if value is None:
                    continue
                return float(value)
            except Exception:
                continue
        return default

    def _f_ms_first(source_keys, default):
        value = _f_first(source_keys, None)
        if value is None:
            return default
        try:
            return float(value) / 1000.0
        except Exception:
            return default

    def _b(source_keys, default):
        for source_key in source_keys:
            value = effective_cfg.get(source_key)
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
        return default

    sensor_interval = _f_first(
        (
            "poll_interval_s",
            "sensor_poll_interval_s",
            "bus_poll_interval_s",
            "controller_poll_interval_s",
            "sensor_read_interval",
        ),
        SENSOR_POLL_INTERVAL_DEFAULT,
    )

    actuator_interval = _f_first(
        (
            "actuator_write_interval",
        ),
        0.2,
    )

    abs_timeout_s = _f_first(
        (
            "io_min_gap_s",
            "bus_min_gap_s",
            "request_gap_s",
            "request_interval_s",
            "abs_time_out",
        ),
        None,
    )
    if abs_timeout_s is None:
        abs_timeout_s = _f_ms_first(
            (
                "min_gap_ms",
                "abs_time_out_ms",
            ),
            0.0,
        )

    read_gap_s = _f_first(
        (
            "read_min_gap_s",
            "read_gap_s",
            "read_interval_s",
            "modbus_delay_read",
        ),
        None,
    )
    if read_gap_s is None:
        read_gap_s = _f_ms_first(
            (
                "channel_delay_read_ms",
            ),
            0.0,
        )

    write_gap_s = _f_first(
        (
            "write_min_gap_s",
            "write_gap_s",
            "write_interval_s",
            "modbus_delay_write",
        ),
        None,
    )
    if write_gap_s is None:
        write_gap_s = _f_ms_first(
            (
                "channel_delay_write_ms",
            ),
            0.0,
        )

    return {
        "sensor_interval": float(sensor_interval),
        "actuator_interval": float(actuator_interval),
        "abs_time_out": float(abs_timeout_s),
        "modbus_delay_read": float(read_gap_s),
        "modbus_delay_write": float(write_gap_s),
        "chk_ctrl_command": _b(("chk_ctrl_command",), True),
        "actuator_feedback_stream": _b(("actuator_feedback_stream",), True),
        "sensor_block_size": int(_f_first(("sensor_block_size",), 64)),
        "batch_actuator": _b(("batch_actuator",), True),
        "batch_actuator_timeout": _f_first(("batch_actuator_timeout",), 0.5),
        "actuator_block_size": int(_f_first(("actuator_block_size",), 64)),
        "batch_actuator_flush_on_sensor_cycle": _b(("batch_actuator_flush_on_sensor_cycle",), True),
    }


def _get_current_controller_data(ctx):
    """Resolve the current effective controller record from the live config."""
    try:
        cid = int(ctx.get("controller_id"))
    except Exception:
        cid = None

    try:
        with ctx["config_lock"]:
            cfg = ctx.get("config_data") or {}
            controller_seed = dict(ctx.get("controller_data") or {})
            if cid is not None and "controller_id" not in controller_seed:
                controller_seed["controller_id"] = cid
            effective = resolve_effective_controller_data(cfg, controller_seed)
            if isinstance(effective, dict) and effective:
                return effective
    except Exception:
        pass

    return dict(ctx.get("controller_data") or {})


def _timing_signature(timing_ctx):
    """Create a stable signature for the effective timing profile."""
    if not isinstance(timing_ctx, dict):
        return None

    try:
        return (
            str(timing_ctx.get("protocol_type") or ""),
            round(float(timing_ctx.get("cycle_time_s", 0.0) or 0.0), 9),
            round(float(timing_ctx.get("min_gap_s", 0.0) or 0.0), 9),
            round(float(timing_ctx.get("channel_delay_read_s", 0.0) or 0.0), 9),
            round(float(timing_ctx.get("channel_delay_write_s", 0.0) or 0.0), 9),
            bool(timing_ctx.get("deterministic", False)),
        )
    except Exception:
        return None


def _merge_timing_runtime_state(old_timing_ctx, new_timing_ctx):
    """Carry runtime timestamps over when the config profile is refreshed."""
    if not isinstance(new_timing_ctx, dict):
        return new_timing_ctx

    old = old_timing_ctx if isinstance(old_timing_ctx, dict) else {}
    for key in ("last_any_activity_ts", "last_read_ts", "last_write_ts"):
        try:
            new_timing_ctx[key] = float(old.get(key, 0.0) or 0.0)
        except Exception:
            new_timing_ctx[key] = 0.0
    return new_timing_ctx


def _log_timing_profile(ctx, timing_ctx, reason):
    """Emit a compact startup/change log for the effective poll timing."""
    if not isinstance(timing_ctx, dict):
        return

    try:
        cid = ctx.get("controller_id")
        cycle_time_s = float(timing_ctx.get("cycle_time_s", SENSOR_POLL_INTERVAL_DEFAULT) or SENSOR_POLL_INTERVAL_DEFAULT)
        min_gap_s = float(timing_ctx.get("min_gap_s", 0.0) or 0.0)
        read_gap_s = float(timing_ctx.get("channel_delay_read_s", 0.0) or 0.0)
        write_gap_s = float(timing_ctx.get("channel_delay_write_s", 0.0) or 0.0)
        poll_hz = (1.0 / cycle_time_s) if cycle_time_s > 0.0 else 0.0
        logger.info(
            "[C%s] TIMING_PROFILE (%s): cycle=%.3fs (%.2f Hz) min_gap=%.3fs read_gap=%.3fs write_gap=%.3fs deterministic=%s",
            cid,
            reason,
            cycle_time_s,
            poll_hz,
            min_gap_s,
            read_gap_s,
            write_gap_s,
            bool(timing_ctx.get("deterministic", False)),
        )
    except Exception:
        pass


def _refresh_timing_ctx(ctx, *, force=False):
    """Refresh the effective timing profile from the live config snapshot.

    This closes a subtle gap in the previous implementation: after a config
    reload, the controller kept using the startup timing profile because
    timing_ctx was only built once.
    """
    controller_data = _get_current_controller_data(ctx)
    config_data = ctx.get("config_data") or {}
    try:
        new_timing_ctx = load_timing_profile_from_config(
            config_data,
            controller_data,
            defaults=_get_controller_timing_defaults(ctx),
        )
    except Exception:
        new_timing_ctx = None

    old_timing_ctx = ctx.get("timing_ctx")
    old_sig = ctx.get("timing_signature")
    if old_sig is None:
        old_sig = _timing_signature(old_timing_ctx)

    new_sig = _timing_signature(new_timing_ctx)
    changed = force or (new_sig != old_sig)

    if changed:
        if isinstance(new_timing_ctx, dict):
            new_timing_ctx = _merge_timing_runtime_state(old_timing_ctx, new_timing_ctx)
        ctx["timing_ctx"] = new_timing_ctx
        ctx["timing_signature"] = new_sig
        _log_timing_profile(ctx, new_timing_ctx, "startup" if old_sig is None else "reload")

    return ctx.get("timing_ctx")


# =============================================================================
# Virtual Controller Endpoint
# =============================================================================

def _virtual_endpoint_matches(row, controller_uuid, controller_id):
    if not isinstance(row, dict):
        return False
    active = _safe_bool(
        row.get("virt_active", row.get("enabled", False)), False)
    if not active:
        return False
    row_uuid = str(row.get("controller_uuid") or "").strip()
    if controller_uuid and row_uuid == controller_uuid:
        return True
    legacy_id = _safe_int(row.get("controller_id"), None)
    return legacy_id is not None and legacy_id == controller_id


def _virtual_endpoint_data(row):
    port = _safe_int(row.get("port"), None)
    return {
        "host": row.get("ip_address") or row.get("host"),
        "port": port,
        "virt_controller_id": row.get("virt_controller_id"),
    }


def _get_active_virtual_endpoint(ctx):
    """Resolve the active virtual endpoint by the V5 controller identity.

    ``controller_uuid`` is the hard I/O identity in the normalized V5 data
    model. The numeric ``controller_id`` fallback keeps legacy datasets
    compatible without confusing it with ``virt_controller_id``.
    """
    controller_uuid = ""
    try:
        controller_uuid = str(
            (ctx.get("controller_data") or {}).get("controller_uuid") or ""
        ).strip()
    except Exception:
        controller_uuid = ""
    try:
        cid = int(ctx.get("controller_id"))
    except Exception:
        cid = -1
    try:
        with ctx["config_lock"]:
            config = ctx.get("config_data") or {}
            rows = list(config.get("virtual_controllers") or [])
    except Exception:
        return None
    for row in rows:
        if _virtual_endpoint_matches(row, controller_uuid, cid):
            return _virtual_endpoint_data(row)
    return None


# =============================================================================
# I/O über Connection Layer (NEU – ersetzt alle Modbus-Direktaufrufe)
# =============================================================================

def _conn_read_block(ctx, data_type, start, count):
    """Liest einen Block über den Connection Layer (synchron, blockierend).

    Ersetzt: read_modbus_coils_block(), read_modbus_registers_block()
    """
    rq = ctx.get("request_queue")
    if rq is None:
        return None

    timing_ctx = _refresh_timing_ctx(ctx)
    if timing_ctx:
        timing_gate(timing_ctx, channel="read")

    request = make_request(
        "read_block", data_type,
        start=start, count=count,
        unit=ctx.get("unit", 1),
    )

    result = sync_execute(
        rq,
        request,
        timeout_s=5.0,
        cancel_event=ctx.get("shutdown_event"),
    )
    if result.get("ok"):
        ctx["statistics"]["sensor_reads"] += 1
        return result.get("data")

    logger.warning(
        "[C%s] MODBUS_IN FAIL: READ %s start=%s count=%s err=%s",
        ctx.get("controller_id"), data_type, start, count,
        result.get("error", "?"),
    )
    _record_io_error(ctx)
    return None



def _conn_read_multi(ctx, addresses):
    """Read typed S7 addresses through one bounded multi-variable request."""
    request_queue = ctx.get("request_queue")
    if request_queue is None:
        return None
    normalized = [str(address) for address in addresses or [] if str(address)]
    if not normalized:
        return []

    timing_ctx = _refresh_timing_ctx(ctx)
    if timing_ctx:
        timing_gate(timing_ctx, channel="read")
    request = make_request(
        "read_multi",
        "s7",
        addresses=normalized,
        unit=ctx.get("unit", 1),
    )
    result = sync_execute(
        request_queue,
        request,
        timeout_s=5.0,
        cancel_event=ctx.get("shutdown_event"),
    )
    if result.get("ok"):
        ctx["statistics"]["sensor_reads"] += 1
        data = result.get("data")
        return data if isinstance(data, list) else None

    logger.warning(
        "[C%s] S7 READ_MULTI FAIL: count=%d err=%s",
        ctx.get("controller_id"),
        len(normalized),
        result.get("error", "?"),
    )
    _record_io_error(ctx)
    return None

def _conn_write_single(ctx, data_type, address, value):
    """Schreibt einen Einzelwert über den Connection Layer (synchron).

    Ersetzt: write_modbus_coil(), write_modbus_register()
    """
    rq = ctx.get("request_queue")
    if rq is None:
        return False

    timing_ctx = _refresh_timing_ctx(ctx)
    if timing_ctx:
        timing_gate(timing_ctx, channel="write")

    request = make_request(
        "write_single", data_type,
        address=address, value=value,
        unit=ctx.get("unit", 1),
    )

    result = sync_execute(
        rq,
        request,
        timeout_s=5.0,
        cancel_event=ctx.get("shutdown_event"),
    )
    if result.get("ok"):
        logger.info(
            "[C%s] IO_OUT protocol=%s: WRITE %s addr=%s val=%s lat=%.1fms",
            ctx.get("controller_id"), _controller_protocol(ctx),
            data_type, address, value,
            result.get("latency_ms", 0.0),
        )
        return True

    logger.warning(
        "[C%s] IO_OUT FAIL protocol=%s: WRITE %s addr=%s val=%s err=%s",
        ctx.get("controller_id"), _controller_protocol(ctx),
        data_type, address, value,
        result.get("error", "?"),
    )
    _record_io_error(ctx)
    return False


def _conn_read_single(ctx, data_type, address):
    """Liest einen Einzelwert über den Connection Layer (synchron).

    Ersetzt: read_modbus_coil(), read_modbus_register()
    """
    rq = ctx.get("request_queue")
    if rq is None:
        return None

    timing_ctx = _refresh_timing_ctx(ctx)
    if timing_ctx:
        timing_gate(timing_ctx, channel="read")

    request = make_request(
        "read_single", data_type,
        address=address,
        unit=ctx.get("unit", 1),
    )

    result = sync_execute(
        rq,
        request,
        timeout_s=5.0,
        cancel_event=ctx.get("shutdown_event"),
    )
    if result.get("ok"):
        return result.get("data")
    return None


def _simulator_actuator_discovery_mode(ctx):
    """Resolve the deployment-only actuator discovery policy."""
    defaults = _get_controller_timing_defaults(ctx)
    value = defaults.get("simulator_actuator_discovery")
    if value is True:
        return "read"
    normalized = str(value or "off").strip().lower()
    aliases = {
        "disabled": "off",
        "false": "off",
        "probe": "read",
        "read_only": "read",
        "writeback": "noop_write",
        "no_op_write": "noop_write",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in ("off", "read", "noop_write") else "off"


def _pending_simulator_actuators(actuators, completed):
    """Return stable discovery candidates not completed in an earlier cycle."""
    result = []
    for actuator in sorted(actuators or [], key=_actuator_sort_id):
        if not isinstance(actuator, dict):
            continue
        address = _safe_int(actuator.get("address"), None)
        function_type = str(actuator.get("function_type") or "")
        key = (function_type, address)
        if address is not None and key not in completed:
            result.append((key, actuator))
    return result


def _simulator_actuator_discovery(ctx, actuators):
    """Discover simulator actuators without changing their current values.

    A no-op write is allowed only when this controller has an active virtual
    endpoint and the local deployment profile explicitly selected it.
    """
    mode = _simulator_actuator_discovery_mode(ctx)
    if mode == "off":
        return True
    if not _get_active_virtual_endpoint(ctx):
        logger.warning(
            "[C%s] Simulator actuator discovery ignored without virtual endpoint",
            ctx.get("controller_id"),
        )
        return False
    state = ctx.setdefault("simulator_actuator_discovery", {
        "completed": set(), "attempts": 0, "failures": 0,
    })
    completed = state.setdefault("completed", set())
    pending = _pending_simulator_actuators(actuators, completed)
    if not pending:
        return True
    target_count = len(completed) + len(pending)
    discovered = 0
    for key, actuator in pending:
        if ctx.get("shutdown_event") and ctx["shutdown_event"].is_set():
            break
        state["attempts"] += 1
        address = key[1]
        function_type = key[0]
        data_type = map_function_type_to_data_type(
            function_type, _controller_protocol(ctx)
        )
        current = _conn_read_single(ctx, data_type, address)
        success = current is not None
        if success and mode == "noop_write":
            success = _conn_write_single(ctx, data_type, address, current)
            readback = _conn_read_single(ctx, data_type, address) if success else None
            success = bool(
                success
                and readback is not None
                and _cmd_readback_ok(function_type, current, readback)
            )
        if not success:
            state["failures"] += 1
            continue
        completed.add(key)
        discovered += 1
    ctx["statistics"]["actuator_discovery_attempts"] = state["attempts"]
    ctx["statistics"]["actuator_discovery_failures"] = state["failures"]
    ctx["statistics"]["actuators_discovered"] = len(completed)
    if discovered or pending:
        logger.info(
            "[C%s] SIMULATOR_ACTUATOR_DISCOVERY: mode=%s discovered=%d total=%d "
            "pending=%d failures=%d",
            ctx.get("controller_id"), mode, discovered, len(completed),
            max(0, len(pending) - discovered), state["failures"],
        )
    return len(completed) >= target_count


# =============================================================================
# Sensor-Bundling (unverändert)
# =============================================================================

def _group_sensors_for_block_reads(sensors, max_block_size):
    """Gruppiert Sensoren nach function_type und Adressbereichen."""
    per_ftype = {}

    for s in sensors or []:
        try:
            ftype = s.get("function_type")
            if ftype not in ("read_coil", "read_holding_registers"):
                continue
            addr_raw = s.get("address")
            if addr_raw is None:
                continue
            addr = int(addr_raw)
        except Exception:
            continue
        bucket = per_ftype.setdefault(ftype, [])
        bucket.append((addr, s))

    groups = []
    for ftype, entries in per_ftype.items():
        entries_sorted = sorted(entries, key=_first_of_pair)
        if not entries_sorted:
            continue

        block_start = entries_sorted[0][0]
        block_entries = [entries_sorted[0]]
        last_addr = entries_sorted[0][0]

        for addr, s in entries_sorted[1:]:
            if addr > last_addr + 1 or (addr - block_start) >= max_block_size:
                groups.append({
                    "function_type": ftype,
                    "start": block_start,
                    "count": (last_addr - block_start) + 1,
                    "entries": block_entries,
                })
                block_start = addr
                block_entries = [(addr, s)]
                last_addr = addr
            else:
                block_entries.append((addr, s))
                last_addr = addr

        groups.append({
            "function_type": ftype,
            "start": block_start,
            "count": (last_addr - block_start) + 1,
            "entries": block_entries,
        })

    return groups




def _group_sensors_for_s7_multi_reads(sensors, max_batch_size):
    """Create stable S7 batches capped by the protocol's 20-variable limit."""
    limit = max(1, min(int(max_batch_size), 20))
    eligible = []
    for sensor in sorted(sensors or [], key=_sensor_sort_id):
        if not isinstance(sensor, dict):
            continue
        function_type = str(sensor.get("function_type") or "").strip().lower()
        address = sensor.get("address")
        if function_type not in ("read_s7", "read_value", "read_single"):
            continue
        if not isinstance(address, str) or not address.strip():
            continue
        eligible.append((address.strip(), sensor))

    groups = []
    for start in range(0, len(eligible), limit):
        entries = eligible[start:start + limit]
        groups.append({
            "function_type": "read_s7",
            "addresses": [entry[0] for entry in entries],
            "entries": entries,
            "count": len(entries),
        })
    return groups


def _extract_values_from_s7_batch(batch, result_data):
    values = {}
    entries = batch.get("entries") or []
    data = result_data if isinstance(result_data, list) else []
    for index, entry in enumerate(entries):
        address, sensor = entry
        value = data[index] if index < len(data) else None
        sensor_id = sensor.get("sensor_id") or sensor.get("name")
        if sensor_id is None:
            sensor_id = "read_s7@{}".format(address)
        values[sensor_id] = value
    return values
def _extract_values_from_block(block, result_data):
    """Extrahiert Sensorwerte aus Block-Read-Ergebnis."""
    values = {}
    if not result_data:
        return values
    try:
        base = int(block.get("start", 0))
    except Exception:
        base = 0
    ftype = block.get("function_type")
    entries = block.get("entries") or []
    for addr, sensor in entries:
        idx = int(addr) - base
        val = None
        if 0 <= idx < len(result_data):
            try:
                val = result_data[idx]
            except Exception:
                val = None
        sensor_id = sensor.get("sensor_id") or sensor.get("name") or "{}@{}".format(ftype, addr)
        values[sensor_id] = val
    return values


# =============================================================================
# Actuator Batching (vereinfacht, synchron)
# =============================================================================

def _actuator_batch_key(task):
    ftype = task.get("function_type")
    addr = task.get("address")
    if addr is None:
        return (ftype, "aid", task.get("actuator_id"))
    try:
        addr_key = int(addr)
    except Exception:
        addr_key = str(addr)
    return (ftype, addr_key)


def _actuator_task_priority(task):
    meta = task.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    origin = str(meta.get("origin") or "").strip().lower()
    state_id = meta.get("state_id")
    if origin == "rule_engine":
        return 3
    if state_id is not None:
        return 1
    return 2


def _get_actuator_batcher(ctx):
    batcher = ctx.get("actuator_batcher")
    if not isinstance(batcher, dict):
        batcher = {"pending": {}, "stats": {
            "batches_executed": 0, "tasks_batched": 0,
            "tasks_total": 0, "last_flush_ts": 0.0,
        }, "lock": threading.Lock(), "first_pending_monotonic": None}
        ctx["actuator_batcher"] = batcher
    batcher.setdefault("pending", {})
    batcher.setdefault("stats", {
        "batches_executed": 0, "tasks_batched": 0,
        "tasks_total": 0, "last_flush_ts": 0.0,
    })
    if batcher.get("lock") is None:
        batcher["lock"] = threading.Lock()
    batcher.setdefault("first_pending_monotonic", None)
    return batcher


def _actuator_flush_pending(ctx, reason="timeout"):
    """Führt alle gepufferten Aktuator-Tasks aus (synchron)."""
    comm = _get_comm_settings(ctx)
    if not comm.get("batch_actuator", False):
        return

    batcher = _get_actuator_batcher(ctx)
    with batcher["lock"]:
        pending = batcher.get("pending") or {}
        if not pending:
            return
        tasks = list(pending.values())
        pending.clear()
        batcher["first_pending_monotonic"] = None
        stats = batcher["stats"]
        stats["batches_executed"] += 1
        stats["last_flush_ts"] = time.time()

    cid = ctx.get("controller_id")
    logger.info("[C%s] Actuator-Flush (%s): %d Tasks", cid, reason, len(tasks))

    for task in tasks:
        _execute_actuator_task(ctx, task)


def _actuator_batch_wait_s(ctx):
    """Return the bounded listener wait until the current batch is due."""
    comm = _get_comm_settings(ctx)
    if not comm.get("batch_actuator", False):
        return 1.0
    try:
        timeout_s = max(0.001, float(comm.get("batch_actuator_timeout", 0.5)))
    except Exception:
        timeout_s = 0.5
    batcher = _get_actuator_batcher(ctx)
    with batcher["lock"]:
        started = batcher.get("first_pending_monotonic")
        pending = bool(batcher.get("pending"))
    if not pending or started is None:
        return min(1.0, timeout_s)
    remaining = timeout_s - (time.monotonic() - float(started))
    return min(1.0, max(0.001, remaining))


def _actuator_flush_if_due(ctx):
    """Flush a batch when its configured wall-clock timeout has elapsed."""
    comm = _get_comm_settings(ctx)
    if not comm.get("batch_actuator", False):
        return False
    try:
        timeout_s = max(0.001, float(comm.get("batch_actuator_timeout", 0.5)))
    except Exception:
        timeout_s = 0.5
    batcher = _get_actuator_batcher(ctx)
    with batcher["lock"]:
        started = batcher.get("first_pending_monotonic")
        due = bool(batcher.get("pending")) and started is not None and (
            time.monotonic() - float(started)
        ) >= timeout_s
    if not due:
        return False
    _actuator_flush_pending(ctx, reason="timeout")
    return True


def _enqueue_or_execute_actuator_task(ctx, task):
    """Batching oder direkte Ausführung (synchron)."""
    if not ctx or not isinstance(task, dict):
        return

    if task.get("distributed_operation_id"):
        _execute_actuator_task(ctx, task)
        return

    comm = _get_comm_settings(ctx)
    batch_enabled = bool(comm.get("batch_actuator", False))

    if not batch_enabled:
        _execute_actuator_task(ctx, task)
        return

    batcher = _get_actuator_batcher(ctx)
    key = _actuator_batch_key(task)
    new_prio = _actuator_task_priority(task)
    with batcher["lock"]:
        pending = batcher["pending"]
        stats = batcher["stats"]
        existing = pending.get(key)
        if existing is not None:
            existing_prio = _actuator_task_priority(existing)
            if new_prio < existing_prio:
                stats["tasks_total"] += 1
                return
        if not pending:
            batcher["first_pending_monotonic"] = time.monotonic()
        pending[key] = copy.deepcopy(task)
        stats["tasks_total"] += 1
        if existing is None:
            stats["tasks_batched"] += 1


# =============================================================================
# Actuator Task Execution (NEU – synchron über Connection Layer)
# =============================================================================

def _report_distributed_actuator_result(ctx, task, result):
    operation_id = task.get("distributed_operation_id")
    if not operation_id:
        return False
    resources = ctx.get("runtime_resources") or {}
    reporter = resources.get("actuator_result_reporter")
    if not callable(reporter):
        return False
    payload = copy.deepcopy(result)
    payload["distributed_operation_id"] = operation_id
    payload["task_uuid"] = task.get("task_uuid")
    payload["controller_uuid"] = (
        task.get("controller_uuid")
        or (ctx.get("controller_data") or {}).get("controller_uuid")
    )
    try:
        return bool(reporter(payload))
    except Exception:
        logger.exception("Distributed actuator result reporting failed")
        return False


def _validate_actuator_write_owner(ctx, task):
    resources = ctx.get("runtime_resources") or {}
    validator = resources.get("validate_actuator_write")
    if callable(validator):
        return validator(
            ctx.get("worker_uuid"), task,
            config_snapshot=ctx.get("config_data"),
        )
    if task.get("distributed_operation_id"):
        return {"ok": False, "code": "capability",
                "error": "distributed write validator is unavailable"}
    return {"ok": True, "scope": "local"}

def _execute_actuator_task(ctx, task):
    """Führt einen einzelnen Aktuator-Task aus (synchron).

    Ersetzt: actuator_task_processor_event()
    Nutzt _conn_write_single() statt write_modbus_*()
    """
    if not isinstance(task, dict):
        return

    address = task.get("address")
    value = task.get("control_value")
    ftype = task.get("function_type")
    ctrl_id = _safe_int(task.get("controller_id"), -1)
    cid = ctx.get("controller_id")

    if ctrl_id != cid:
        _report_distributed_actuator_result(ctx, task, {
            "success": False,
            "readback_checked": False,
            "readback_verified": False,
            "error": "controller_id differs from isolated worker",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return

    guard = _validate_actuator_write_owner(ctx, task)
    if isinstance(guard, dict) and not guard.get("ok"):
        _report_distributed_actuator_result(ctx, task, {
            "success": False,
            "readback_checked": False,
            "readback_verified": False,
            "error": guard.get("error") or "actuator ownership rejected",
            "owner_guard": copy.deepcopy(guard),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        _emit_event(ctx, "ACTUATOR_WRITE_REJECTED_OWNER_LEASE", {
            "controller_id": cid,
            "controller_uuid": task.get("controller_uuid") or (ctx.get("controller_data") or {}).get("controller_uuid"),
            "actuator_id": task.get("actuator_id"),
            "actuator_uuid": task.get("actuator_uuid") or task.get("target_uuid"),
            "address": address,
            "function_type": ftype,
            "value": value,
            "owner_guard": guard,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, target="state")
        logger.warning(
            "[C%s] WRITE_REJECTED owner lease guard: aid=%s uuid=%s reason=%s",
            cid, task.get("actuator_id"), task.get("actuator_uuid") or task.get("target_uuid"),
            (guard.get("report") or {}).get("errors"),
        )
        return

    # Aktor-Intervall beachten
    _respect_actuator_interval(ctx)

    comm = _get_comm_settings(ctx)
    chk_cmd = bool(comm.get("chk_ctrl_command", True))
    feedback_stream = bool(comm.get("actuator_feedback_stream", False))

    # Write über Connection Layer (generisch!)
    data_type = map_function_type_to_data_type(ftype, _controller_protocol(ctx))
    ok = _conn_write_single(ctx, data_type, address, value)

    # Readback
    rb = None
    if chk_cmd and ok:
        rb = _conn_read_single(ctx, data_type, address)

    success = bool(ok)
    readback_verified = False
    if chk_cmd:
        readback_verified = bool(ok and _cmd_readback_ok(ftype, value, rb))
        success = readback_verified

    # Lokaler Aktuator-Store
    try:
        with ctx["local_actuator_lock"]:
            cm = ctx["local_actuator_values"].setdefault(cid, {})
            cm[str(address)] = {
                "command_value": value,
                "readback": rb,
                "success": success,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception:
        pass

    # ACTUATOR_VALUE_UPDATE Event → SEM (queue_event_state)
    _emit_event(ctx, "ACTUATOR_VALUE_UPDATE", {
        "controller_id": cid,
        "actuator_id": task.get("actuator_id"),
        "actuator_uuid": task.get("actuator_uuid") or task.get("target_uuid"),
        "controller_uuid": task.get("controller_uuid") or (ctx.get("controller_data") or {}).get("controller_uuid"),
        "address": address,
        "function_type": ftype,
        "value": value,
        "readback": rb,
        "success": success,
        "task_uuid": task.get("task_uuid"),
        "distributed_operation_id": task.get("distributed_operation_id"),
        "readback_checked": chk_cmd,
        "readback_verified": readback_verified,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }, target="state")

    _report_distributed_actuator_result(ctx, task, {
        "controller_id": cid,
        "actuator_id": task.get("actuator_id"),
        "address": address,
        "function_type": ftype,
        "command_value": value,
        "write_succeeded": bool(ok),
        "readback": rb,
        "readback_checked": chk_cmd,
        "readback_verified": readback_verified,
        "success": success,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    # Timestamp aktualisieren
    try:
        aid = task.get("actuator_id")
        if aid is not None:
            ctx.setdefault("actuator_last_write_ts", {})[int(aid)] = time.time()
    except Exception:
        pass

    # Log (ersetzt Observer notify)
    logger.info(
        "[C%s] WRITE: aid=%s addr=%s f=%s cmd=%s rb=%s ok=%s state_id=%s",
        cid, task.get("actuator_id"), address, ftype,
        value, rb, success, (task.get("meta") or {}).get("state_id"),
    )

    # ACTUATOR_FEEDBACK is routed through Thread Management so the local
    # MQTT event mirror can publish the verified command/readback result.
    if feedback_stream:
        _emit_event(ctx, "ACTUATOR_FEEDBACK", {
            "controller_id": cid,
            "actuator_id": task.get("actuator_id"),
            "actuator_uuid": task.get("actuator_uuid") or task.get("target_uuid"),
            "controller_uuid": task.get("controller_uuid") or (ctx.get("controller_data") or {}).get("controller_uuid"),
            "address": address,
            "function_type": ftype,
            "command_value": value,
            "readback": rb,
            "success": success,
            "task_uuid": task.get("task_uuid"),
            "state_id": (task.get("meta") or {}).get("state_id"),
            "origin": (task.get("meta") or {}).get("origin"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, target="pc")


def _respect_actuator_interval(ctx):
    """Erzwingt Minimalabstand zwischen Aktor-Kommandos (synchron)."""
    try:
        comm = _get_comm_settings(ctx)
        interval = float(comm.get("actuator_interval", 0.0) or 0.0)
    except Exception:
        interval = 0.0

    if interval <= 0.0:
        return

    store = ctx.setdefault("actuator_last_write_ts", {})
    try:
        last_ts = float(store.get("_global", 0.0) or 0.0)
    except Exception:
        last_ts = 0.0

    now = time.time()
    remaining = interval - (now - last_ts)
    if remaining > 0.0:
        time.sleep(remaining)
        now = time.time()

    store["_global"] = now


# =============================================================================
# Sensor Polling Loop (NEU – synchron)
# =============================================================================

def _store_local_sensor_value(ctx, sensor_id, value, **kwargs):
    try:
        controller_id = int(ctx.get("controller_id"))

        with ctx["local_sensor_lock"]:
            controller_values = ctx[
                "local_sensor_values"
            ].setdefault(controller_id, {})
            controller_values[str(sensor_id)] = {
                "value": copy.deepcopy(value),
                "timestamp": kwargs.get("timestamp"),
                "address": kwargs.get("address"),
                "function_type": kwargs.get("function_type"),
            }

        return True

    except Exception:
        return False


def _log_sensor_store_rejection(ctx, sensor_id, store_result):
    error_count = ctx["statistics"].get(
        "sensor_store_errors",
        0,
    )

    if error_count <= 3 or error_count % 100 == 0:
        logger.error(
            "[C%s] Sensor store rejected S%s: %s",
            ctx.get("controller_id"),
            sensor_id,
            store_result.get("message"),
        )


def _process_sensor_value(ctx, sensor, last_sensor_values, **kwargs):
    sensor_id = sensor.get("sensor_id")
    value = kwargs.get("value")
    timestamp = kwargs.get("timestamp")
    function_type = sensor.get("function_type")
    address = sensor.get("address")
    threshold = sensor.get("threshold")
    critical = _safe_bool(
        sensor.get("critical", False),
        False,
    )
    force_automation = _safe_bool(
        sensor.get("force_automation", False),
        False,
    )

    store_result = _write_central_sensor_value(
        ctx,
        sensor_id,
        value,
        quality="good" if value is not None else "unavailable",
        timestamp=timestamp,
        metadata={
            "controller_id": ctx.get("controller_id"),
            "address": address,
            "function_type": function_type,
            "threshold": threshold,
        },
    )

    if not store_result.get("ok"):
        _log_sensor_store_rejection(
            ctx,
            sensor_id,
            store_result,
        )
        return 0

    _store_local_sensor_value(
        ctx,
        sensor_id,
        value,
        timestamp=timestamp,
        address=address,
        function_type=function_type,
    )

    last_value = last_sensor_values.get(sensor_id)
    value_changed = (
        last_value != value
        or last_value is None and value is not None
    )
    last_sensor_values[sensor_id] = value

    if not (value_changed or critical or force_automation):
        return 0

    _publish_sensor_value_update(
        ctx,
        ctx.get("controller_id"),
        sensor_id,
        value=value,
        threshold=threshold,
        address=address,
        function_type=function_type,
        critical=critical,
        force_automation=force_automation,
        automation_reason=sensor.get("automation_reason"),
        store_entry=store_result.get("entry"),
    )

    return 1

def _sensor_polling_loop(ctx):
    """Deterministischer Sensor-Poll-Loop (synchron, blockierend).

    Läuft in einem eigenen Thread.
    Verwendet cycle_sleep() für deterministisches Timing.
    """
    cid = ctx["controller_id"]
    shutdown = ctx.get("shutdown_event")

    cycle_count = 0
    last_stats_log = time.time()
    last_sensor_log_ts = 0.0
    last_sensor_values = {}

    logger.info("[C%s] Sensor-Polling gestartet", cid)

    # ====== DIAGNOSTIC: Startup-Tabelle Sensoren/Aktuatoren für diesen Controller ======
    try:
        with ctx["config_lock"]:
            _diag_cfg = ctx.get("config_data") or {}
            _diag_sensors = _diag_cfg.get("sensors") or []
            _diag_actuators = _diag_cfg.get("actuators") or []

        _my_sensors = [s for s in _diag_sensors if int(s.get("controller_id", -1)) == int(cid)]
        _my_actuators = [a for a in _diag_actuators if int(a.get("controller_id", -1)) == int(cid)]

        _lines = []
        _lines.append("=" * 72)
        _lines.append("[C%s] DEVICE REGISTRY (startup)" % cid)
        _lines.append("-" * 72)
        _lines.append("  SENSORS (%d):" % len(_my_sensors))
        _lines.append("    %-8s %-10s %-24s %-8s" % ("SID", "ADDR", "FUNCTION_TYPE", "GROUP"))
        for _s in sorted(_my_sensors, key=_sensor_sort_id):
            _lines.append("    %-8s %-10s %-24s %-8s" % (
                _s.get("sensor_id"), _s.get("address"),
                _s.get("function_type", "?"), _s.get("sensor_group", "-"),
            ))
        _lines.append("  ACTUATORS (%d):" % len(_my_actuators))
        _lines.append("    %-8s %-10s %-24s %-8s" % ("AID", "ADDR", "FUNCTION_TYPE", "GROUP"))
        for _a in sorted(_my_actuators, key=_actuator_sort_id):
            _lines.append("    %-8s %-10s %-24s %-8s" % (
                _a.get("actuator_id"), _a.get("address"),
                _a.get("function_type", "?"), _a.get("actuator_group", "-"),
            ))
        _lines.append("  REQUEST_QUEUE: %s" % ("OK" if ctx.get("request_queue") else "MISSING!"))
        _lines.append("=" * 72)
        logger.info("\n".join(_lines))
    except Exception as _diag_exc:
        logger.warning("[C%s] Diagnostic table error: %s", cid, _diag_exc)

    while ctx.get("running", True):
        if shutdown and shutdown.is_set():
            break

        cycle_start = time.monotonic()
        cycle_count += 1

        try:
            # ------ Config-Snapshot (Sensoren) ------
            with ctx["config_lock"]:
                cfg = ctx.get("config_data") or {}
                cfg_sensors = cfg.get("sensors") or []
                cfg_actuators = cfg.get("actuators") or []

            timing_ctx = _refresh_timing_ctx(ctx)
            comm = _get_comm_settings(ctx)
            if isinstance(timing_ctx, dict):
                try:
                    interval = max(float(timing_ctx.get("cycle_time_s", SENSOR_POLL_INTERVAL_DEFAULT)),
                                   SENSOR_POLL_INTERVAL_MIN)
                except Exception:
                    interval = max(float(comm.get("sensor_interval", SENSOR_POLL_INTERVAL_DEFAULT)),
                                   SENSOR_POLL_INTERVAL_MIN)
            else:
                interval = max(float(comm.get("sensor_interval", SENSOR_POLL_INTERVAL_DEFAULT)),
                               SENSOR_POLL_INTERVAL_MIN)

            force_sensor_map = _resolve_force_automation_sensor_map(cfg)

            sensors = []
            for s in cfg_sensors:
                try:
                    if int(s.get("controller_id", 0)) != int(cid):
                        continue
                except Exception:
                    continue

                sensor_id_int = _safe_int(s.get("sensor_id"), None)
                force_reason = force_sensor_map.get(sensor_id_int)
                critical_flag = _safe_bool(s.get("critical", False), False)
                force_automation = bool(critical_flag or force_reason)
                automation_reason = "critical" if critical_flag else (force_reason or None)

                sensors.append({
                    "sensor_id": s.get("sensor_id"),
                    "function_type": s.get("function_type"),
                    "address": s.get("address"),
                    "threshold": s.get("threshold"),
                    "priority": s.get("priority", 1.0),
                    "critical": critical_flag,
                    "force_automation": force_automation,
                    "automation_reason": automation_reason,
                })

            actuators = []
            for actuator in cfg_actuators:
                try:
                    if int(actuator.get("controller_id", 0)) == int(cid):
                        actuators.append(copy.deepcopy(actuator))
                except Exception:
                    continue

            if not sensors:
                wait = getattr(shutdown, "wait", None)
                if callable(wait):
                    wait(interval)
                else:
                    time.sleep(interval)
                continue

            # ------ Verbindungs-Check ------
            rq = ctx.get("request_queue")
            if rq is None:
                wait = getattr(shutdown, "wait", None)
                if callable(wait):
                    wait(min(1.0, interval))
                else:
                    time.sleep(min(1.0, interval))
                continue

            # ------ Sensoren → Blöcke ------
            protocol = _controller_protocol(ctx)
            if protocol == "plc_s7":
                block_size = max(1, min(
                    int(comm.get("s7_multi_var_limit", 20)), 20
                ))
                blocks = _group_sensors_for_s7_multi_reads(sensors, block_size)
            else:
                block_size = int(comm.get("sensor_block_size", 64))
                blocks = _group_sensors_for_block_reads(sensors, block_size)

            # ------ Statistik-Log ------
            if time.time() - last_stats_log >= STATUS_LOG_INTERVAL_S:
                batcher = _get_actuator_batcher(ctx)
                with batcher["lock"]:
                    batcher_stats = copy.deepcopy(batcher.get("stats", {}))
                logger.info(
                    "[C%s] Stats: %d Sensoren, %d Blöcke, reads=%d, errors=%d, "
                    "batches=%d, tasks=%d",
                    cid, len(sensors), len(blocks),
                    ctx["statistics"].get("sensor_reads", 0),
                    ctx["statistics"].get("modbus_errors", 0),
                    batcher_stats.get("batches_executed", 0),
                    batcher_stats.get("tasks_total", 0),
                )
                last_stats_log = time.time()

            # ------ Blöcke lesen ------
            all_values = {}
            block_snapshots = []
            for block in blocks:
                if shutdown and shutdown.is_set():
                    break
                if protocol == "plc_s7":
                    addresses = block.get("addresses") or []
                    result_data = _conn_read_multi(ctx, addresses)
                    block_values = _extract_values_from_s7_batch(
                        block, result_data
                    )
                    all_values.update(block_values)
                    block_snapshots.append({
                        "data_type": "s7",
                        "addresses": list(addresses),
                        "count": len(addresses),
                        "data": result_data,
                    })
                    continue

                ftype = block["function_type"]
                start = block["start"]
                count = block["count"]
                data_type = map_function_type_to_data_type(
                    ftype, protocol
                )
                result_data = _conn_read_block(ctx, data_type, start, count)
                block_values = _extract_values_from_block(block, result_data)
                all_values.update(block_values)
                block_snapshots.append({
                    "data_type": data_type,
                    "start": start,
                    "count": count,
                    "data": (
                        list(result_data)
                        if isinstance(result_data, list)
                        else result_data
                    ),
                })

            if shutdown and shutdown.is_set():
                break

            _simulator_actuator_discovery(ctx, actuators)

            cycle_timestamp = datetime.now(timezone.utc).isoformat()
            events_sent = 0

            # ------ DIAGNOSTIC: Sensor-/Modbus-Snapshot (erste 3 Zyklen, danach alle 5s) ------
            _now = time.time()
            if cycle_count <= 3 or (_now - last_sensor_log_ts) >= 5.0:
                _mb_parts = []
                for _snap in block_snapshots:
                    addresses = _snap.get("addresses")
                    if isinstance(addresses, list):
                        address_text = ",".join(
                            str(address) for address in addresses[:4]
                        )
                        if len(addresses) > 4:
                            address_text += ",+{}".format(len(addresses) - 4)
                        _mb_parts.append(
                            "s7[{}]={}".format(address_text, _snap.get("data"))
                        )
                        continue
                    start = int(_snap.get("start", 0))
                    end = start + max(int(_snap.get("count", 0)) - 1, 0)
                    _mb_parts.append("{}[{}:{}]={}".format(
                        _snap.get("data_type"),
                        start,
                        end,
                        _snap.get("data"),
                    ))
                if _mb_parts:
                    logger.info(
                        "[C%s] IO_IN protocol=%s cycle=%d: %s",
                        cid, protocol, cycle_count, " | ".join(_mb_parts),
                    )

                _val_parts = []
                _addr_by_sid = {}
                for _sensor_meta in sensors:
                    try:
                        _addr_by_sid[int(_sensor_meta.get("sensor_id"))] = _sensor_meta.get("address")
                    except Exception:
                        continue
                for _sid, _val in sorted(all_values.items(), key=_item_key_as_text):
                    try:
                        _addr = _addr_by_sid.get(int(_sid), "?")
                    except Exception:
                        _addr = "?"
                    _val_parts.append("S%s@%s=%s" % (_sid, _addr, _val))
                if _val_parts:
                    logger.info("[C%s] SENSOR_READ cycle=%d: %s", cid, cycle_count, " | ".join(_val_parts))
                last_sensor_log_ts = _now

            # ------ Werte verarbeiten ------
            for sensor in sensors:
                sensor_id = sensor.get("sensor_id")
                value = all_values.get(sensor_id)

                events_sent += _process_sensor_value(
                    ctx,
                    sensor,
                    last_sensor_values,
                    value=value,
                    timestamp=cycle_timestamp,
                )

            ctx["statistics"]["sensor_batches"] = (
                ctx["statistics"].get("sensor_batches", 0) + len(blocks)
            )

            # ------ Optionaler Aktor-Flush am Zyklusende ------
            try:
                if (comm.get("batch_actuator", False)
                        and comm.get("batch_actuator_flush_on_sensor_cycle", False)):
                    _actuator_flush_pending(ctx, reason="sensor_cycle")
            except Exception:
                pass

            if events_sent > 0 and (cycle_count <= 3 or (_now - last_sensor_log_ts) < 0.1):
                logger.info("[C%s] SENSOR_EVENTS cycle=%d: emitted=%d", cid, cycle_count, events_sent)

            # ------ Deterministisches Sleep ------
            if timing_ctx:
                cycle_sleep(timing_ctx, cycle_start, shutdown)
            else:
                elapsed = time.monotonic() - cycle_start
                sleep_s = max(0.001, interval - elapsed)
                wait = getattr(shutdown, "wait", None)
                if callable(wait):
                    wait(sleep_s)
                else:
                    time.sleep(sleep_s)

        except Exception as e:
            logger.error("[C%s] SensorPoll Fehler: %s", cid, e, exc_info=True)
            _record_io_error(ctx)
            wait = getattr(shutdown, "wait", None)
            if callable(wait):
                wait(5.0)
            else:
                time.sleep(5.0)

    logger.info("[C%s] Sensor-Polling beendet", cid)


# =============================================================================
# Sensor Value Event Emission (vereinfacht)
# =============================================================================

def _publish_sensor_value_update(
    ctx,
    controller_id,
    sensor_id,
    **kwargs
):
    """Publish a SENSOR_VALUE_UPDATE event after the central store commit.

    No pair-registry entry is created. The event carries the exact sequence
    and controller revision returned by the committed runtime snapshot.
    """
    value = kwargs.get("value")
    store_entry = kwargs.get("store_entry") or {}
    ts = store_entry.get(
        "timestamp"
    ) or datetime.now(timezone.utc).isoformat()
    info = "available" if value is not None else "unavailable"

    _emit_event(ctx, "SENSOR_VALUE_UPDATE", {
        "controller_id": int(controller_id),
        "controller_uuid": (
            store_entry.get("controller_uuid")
            or _get_controller_uuid(ctx)
        ),
        "sensor_id": int(sensor_id),
        "threshold": kwargs.get("threshold"),
        "value": value,
        "quality": store_entry.get("quality"),
        "sensor_store_sequence": store_entry.get("sequence"),
        "sensor_store_revision": store_entry.get(
            "controller_revision"
        ),
        "address": kwargs.get("address"),
        "function_type": kwargs.get("function_type"),
        "critical": bool(kwargs.get("critical")),
        "force_automation": bool(
            kwargs.get("force_automation")
        ),
        "automation_reason": kwargs.get(
            "automation_reason"
        ),
        "timestamp": ts,
        "info": info,
    })


# =============================================================================
# Event-Listener Loop (NEU – synchron, blockierend)
# =============================================================================

def _event_listener_loop(ctx):
    """Empfängt Events aus der Controller-Queue (synchron, blockierend).

    Blockiert auf Queue.get() → CPU idle wenn keine Events.
    Dispatcht ACTUATOR_TASK Events direkt an die Ausführung.
    """
    cid = ctx["controller_id"]
    q = ctx["event_queue_mbc"]
    shutdown = ctx.get("shutdown_event")

    logger.info("[C%s] Event-Listener gestartet", cid)

    while ctx.get("running", True):
        if shutdown and shutdown.is_set():
            break

        # BLOCKIEREND: Wartet auf Events (CPU idle!)
        try:
            evt = q.get(timeout=_actuator_batch_wait_s(ctx))
        except Empty:
            _actuator_flush_if_due(ctx)
            continue
        except Exception:
            continue

        if evt is None:
            continue
        if _is_stop_message(evt):
            break
        if not isinstance(evt, dict):
            continue

        event_type = str(evt.get("event_type") or "")

        if event_type == "CONFIG_SHARD_UPDATE":
            _handle_config_shard_update(ctx, evt)
            continue

        # ------ ACTUATOR_TASK Dispatch ------
        if event_type == "ACTUATOR_TASK":
            try:
                _payload = evt.get("payload") or {}
                _task = _payload.get("task") or {}
                logger.info(
                    "[C%s] ACTUATOR_TASK RECEIVED: aid=%s addr=%s f=%s cmd=%s source=%s",
                    cid, _task.get("actuator_id"), _task.get("address"),
                    _task.get("function_type"), _task.get("control_value"),
                    _payload.get("source", "?"),
                )
                _handle_actuator_task_event(ctx, evt)
                _actuator_flush_if_due(ctx)
            except Exception as e:
                logger.error("[C%s] ACTUATOR_TASK Fehler: %s", cid, e, exc_info=True)
            continue

        # ------ Andere Event-Typen (erweiterbar) ------
        logger.debug("[C%s] Event empfangen: %s", cid, event_type)

    logger.info("[C%s] Event-Listener beendet", cid)


def _handle_config_shard_update(ctx, evt):
    """Atomically replace this controller's detached config projection."""
    payload = evt.get("payload") or {}
    projected = payload.get("config")
    acknowledgement = payload.get("acknowledgement")
    result = {
        "ok": False,
        "revision": payload.get("revision"),
        "controller_uuid": _get_controller_uuid(ctx),
    }

    try:
        if not isinstance(projected, dict):
            raise TypeError("CONFIG_SHARD_UPDATE requires a config dict")

        with ctx["config_lock"]:
            current = ctx.get("config_data")

            if not isinstance(current, dict):
                current = {}
                ctx["config_data"] = current

            current.clear()
            current.update(copy.deepcopy(projected))

            controllers = current.get("controllers") or []

            if controllers and isinstance(controllers[0], dict):
                ctx["controller_data"].clear()
                ctx["controller_data"].update(copy.deepcopy(controllers[0]))

            ctx["config_revision"] = payload.get("revision")

        _refresh_timing_ctx(ctx, force=True)
        result["ok"] = True

    except Exception as exc:
        result["error"] = str(exc)
        logger.error("Controller config update failed: %s", exc, exc_info=True)

    if isinstance(acknowledgement, dict):
        acknowledgement["result"] = result
        signal = acknowledgement.get("event")

        if callable(getattr(signal, "set", None)):
            signal.set()

    return result


def _handle_actuator_task_event(ctx, evt):
    """Verarbeitet ein ACTUATOR_TASK Event (synchron).

    Zwei Modi:
      A) payload.task_uuid → Task aus Pair-Registry poppen
      B) payload.task → direkt ausführen
    """
    cid = ctx["controller_id"]
    payload = evt.get("payload", {}) or {}

    # Epoch-Filter
    run_epoch_evt = payload.get("run_epoch")
    thread_index_evt = evt.get("thread_index")
    if not _accept_and_update_epoch(ctx, run_epoch_evt, thread_index_evt):
        logger.info("[C%s] ACTUATOR_TASK ignoriert (epoch mismatch)", cid)
        return

    # Modus B: Direkte Task-Ausführung
    direct_task = payload.get("task")
    if direct_task:
        _enqueue_or_execute_actuator_task(ctx, direct_task)
        return

    # Modus A: Task aus Pair-Registry
    task_id = payload.get("task_uuid") or payload.get("task_id")
    if not task_id:
        logger.info("[C%s] ACTUATOR_TASK ohne task_uuid – ignoriert", cid)
        return

    task = _pop_ctrl_task(ctx, task_id)
    if not task:
        logger.info("[C%s] Task %s nicht im Pair-Store", cid, task_id)
        return

    logger.info(
        "[C%s] Task %s: aid=%s addr=%s f=%s cmd=%s state_id=%s",
        cid, task_id, task.get("actuator_id"), task.get("address"),
        task.get("function_type"), task.get("control_value"),
        (task.get("meta") or {}).get("state_id"),
    )
    _enqueue_or_execute_actuator_task(ctx, task)


# =============================================================================
# Context Builder (NEU – vereinfacht, ohne Observer/asyncio)
# =============================================================================

def build_controller_context(
    controller_id,
    controller_data,
    config_data,
    config_lock,
    sensor_values,
    sensor_lock,
    queue_event_send,
    event_queue_mbc,
    local_sensor_values,
    local_sensor_lock,
    local_actuator_values,
    local_actuator_lock,
    shutdown_event=None,
    sensor_pair_registry=None,
    ctrl_task_pair_registry=None,
    pair_registry_lock=None,
    request_queue=None,
    runtime_resources=None,
):
    """Erstellt den Controller-Context (CTX).

    NEU gegenüber v3:
    - request_queue: Queue zum Connection Thread (Schicht 1)
    - timing_ctx: Timing-Profile aus _bus_timing (Schicht 2)
    - Kein asyncio, kein Observer, kein Executor
    """
    # Unit aus Config extrahieren
    unit_hex = controller_data.get("unit", "0x1")
    if isinstance(unit_hex, str):
        try:
            unit_int = int(unit_hex.replace("0x", ""), 16)
        except Exception:
            unit_int = 1
    else:
        try:
            unit_int = int(unit_hex)
        except Exception:
            unit_int = 1

    ctx = {
        "controller_id": int(controller_id),
        "controller_data": dict(controller_data or {}),
        "config_data": config_data,
        "config_lock": config_lock,
        "unit": unit_int,

        "sensor_values": sensor_values,
        "sensor_lock": sensor_lock,

        "local_sensor_values": {} if local_sensor_values is None else local_sensor_values,
        "local_sensor_lock": threading.Lock() if local_sensor_lock is None else local_sensor_lock,
        "local_actuator_values": {} if local_actuator_values is None else local_actuator_values,
        "local_actuator_lock": threading.Lock() if local_actuator_lock is None else local_actuator_lock,

        "queue_event_send": queue_event_send,
        "event_queue_mbc": event_queue_mbc,

        # NEU: Connection Layer Request Queue
        "request_queue": request_queue,
        "runtime_resources": runtime_resources if isinstance(runtime_resources, dict) else None,
        "worker_uuid": (
            runtime_resources.get("worker_uuid")
            if isinstance(runtime_resources, dict)
            else None
        ),
        "sensor_store_enabled": False,
        "sensor_store_writer_claimed": False,
        "sensor_store_writer_token": None,

        "running": True,
        "shutdown_event": shutdown_event,
        "config_revision": (
            (config_data.get("_runtime_meta") or {}).get("revision")
            if isinstance(config_data, dict)
            else 0
        ),

        "statistics": {
            "start_time": time.time(),
            "sensor_reads": 0,
            "modbus_errors": 0,
            "io_errors": 0,
            "sensor_batches": 0,
            "sensor_store_writes": 0,
            "sensor_store_errors": 0,
            "actuator_discovery_attempts": 0,
            "actuator_discovery_failures": 0,
            "actuators_discovered": 0,
        },

        "sensor_pair_registry": {} if sensor_pair_registry is None else sensor_pair_registry,
        "ctrl_task_pair_registry": {} if ctrl_task_pair_registry is None else ctrl_task_pair_registry,
        "pair_registry_lock": threading.RLock() if pair_registry_lock is None else pair_registry_lock,

        "automation_run_epoch": None,
        "automation_epochs": {},

        "sensor_last_read_ts": {},
        "actuator_last_write_ts": {},

        # Actuator Batcher
        "actuator_batcher": {
            "pending": {},
            "lock": threading.Lock(),
            "first_pending_monotonic": None,
            "stats": {
                "batches_executed": 0,
                "tasks_batched": 0,
                "tasks_total": 0,
                "last_flush_ts": 0.0,
            },
        },
    }

    # Timing-Profile laden (Schicht 2)
    ctx["timing_ctx"] = None
    ctx["timing_signature"] = None
    try:
        _refresh_timing_ctx(ctx, force=True)
    except Exception:
        ctx["timing_ctx"] = None
        ctx["timing_signature"] = None

    logger.info("[CONTROLLER %s] Context erstellt (sync, unit=%d)", controller_id, unit_int)

    if isinstance(runtime_resources, dict):
        runtime_resources["controller_context"] = ctx

    return ctx


# =============================================================================
# Main Loop + Thread Entry (NEU – synchron, 2 Worker-Threads)
# =============================================================================

def run_control_thread(
    controller_id,
    controller_data,
    config_data,
    config_lock,
    sensor_values,
    sensor_lock,
    queue_event_send,
    event_queue_mbc,
    local_sensor_values,
    local_sensor_lock,
    local_actuator_values,
    local_actuator_lock,
    node_id=None,
    shutdown_event=None,
    sensor_pair_registry=None,
    ctrl_task_pair_registry=None,
    pair_registry_lock=None,
    request_queue=None,
    runtime_resources=None,
):
    """Entry-Point des Controller-Threads (synchron).

    Startet 2 Worker-Threads:
      1. Sensor-Polling (deterministisch)
      2. Event-Listener (blockierend auf Queue)

    Wartet auf shutdown_event, dann sauber beenden.
    """
    ctx = None
    threads = []

    try:
        logger.info("Starte Controller %s (sync) ...", controller_id)

        ctx = build_controller_context(
            controller_id=controller_id,
            controller_data=controller_data,
            config_data=config_data,
            config_lock=config_lock,
            sensor_values=sensor_values,
            sensor_lock=sensor_lock,
            queue_event_send=queue_event_send,
            event_queue_mbc=event_queue_mbc,
            local_sensor_values=local_sensor_values,
            local_sensor_lock=local_sensor_lock,
            local_actuator_values=local_actuator_values,
            local_actuator_lock=local_actuator_lock,
            shutdown_event=shutdown_event,
            sensor_pair_registry=sensor_pair_registry,
            ctrl_task_pair_registry=ctrl_task_pair_registry,
            pair_registry_lock=pair_registry_lock,
            request_queue=request_queue,
            runtime_resources=runtime_resources,
        )

        writer_claim = _claim_sensor_store_writer(
            ctx
        )

        if not (
            isinstance(writer_claim, dict)
            and writer_claim.get("ok")
        ):
            raise RuntimeError(
                "Controller sensor store claim failed: {}".format(
                    (writer_claim or {}).get("message")
                )
            )

        # Worker 1: Sensor Polling
        sensor_thread = threading.Thread(
            target=partial(_sensor_polling_loop, ctx),
            name="C{}-SensorPoll".format(controller_id),
            daemon=False,
        )

        # Worker 2: Event Listener
        event_thread = threading.Thread(
            target=partial(_event_listener_loop, ctx),
            name="C{}-EventListener".format(controller_id),
            daemon=False,
        )

        threads = [sensor_thread, event_thread]

        sensor_thread.start()
        event_thread.start()

        # Main-Thread wartet auf Shutdown
        if shutdown_event:
            shutdown_event.wait()
        else:
            # Fallback: Warte auf Worker-Threads
            for th in threads:
                th.join()

    except KeyboardInterrupt:
        logger.info("Controller %s: KeyboardInterrupt", controller_id)

    except Exception as e:
        logger.error("Controller %s Fehler: %s", controller_id, e, exc_info=True)

    finally:
        # Sauber beenden
        if ctx:
            ctx["running"] = False

        # Auf Worker-Threads warten
        for th in threads:
            if th.is_alive():
                try:
                    th.join(timeout=3.0)
                except Exception:
                    pass

        live_threads = [
            th.name
            for th in threads
            if th.is_alive()
        ]

        if live_threads:
            logger.error(
                "Controller %s retains sensor writer; live workers: %s",
                controller_id,
                live_threads,
            )

        elif ctx and not _release_sensor_store_writer(ctx):
            logger.warning(
                "Controller %s could not release sensor store writer",
                controller_id,
            )

        logger.info("Controller %s beendet", controller_id)
