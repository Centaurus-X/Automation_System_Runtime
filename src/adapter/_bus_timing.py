# -*- coding: utf-8 -*-

# src/adapter/_bus_timing.py

"""Bus Timing Library – Schicht 2: Protokoll-spezifisches Timing.

Verantwortlichkeit:
    - Timing-Profile pro Protokoll verwalten
    - Minimalen Abstand zwischen I/O-Operationen erzwingen
    - Deterministisches Scheduling
    - Blockierendes Warten (time.sleep) für synchronen Code

Design:
    - Rein funktional, dict-basierte Timing-Contexts
    - Synchron: time.sleep() statt asyncio.sleep()
    - Kein OOP, kein Decorator, kein Lambda
    - functools.partial wo nötig
    - CPU-effizient: Wartet nur wenn nötig

Extrahiert aus dem alten _controller_thread.py:
    - command_delay()        → timing_gate()
    - abs_time_out           → min_gap_s
    - modbus_delay_read/write → channel_delay_s
"""


import time
import logging

from functools import partial


logger = logging.getLogger(__name__)


def _cfg_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


def _apply_first_float(config, target, target_key, source_keys):
    if not isinstance(config, dict):
        return False

    for source_key in source_keys:
        if source_key not in config:
            continue
        value = _cfg_float(config.get(source_key), None)
        if value is None:
            continue
        target[target_key] = value
        return True
    return False


_CONTROLLER_PATCH_SECTION_NAMES = (
    "controllers_patch",
    "controllers_patches",
    "controllers_patch_example",
    "controller_patch",
    "controller_patches",
    "controller_overrides",
    "controller_override",
    "controller_comm_overrides",
    "controller_timing_overrides",
)

_CONTROLLER_DIRECT_CONFIG_KEYS = (
    "sensor_read_interval",
    "actuator_write_interval",
    "abs_time_out",
    "abs_time_out_ms",
    "poll_interval_s",
    "sensor_poll_interval_s",
    "bus_poll_interval_s",
    "controller_poll_interval_s",
    "poll_rate_hz",
    "io_min_gap_s",
    "bus_min_gap_s",
    "request_gap_s",
    "request_interval_s",
    "read_min_gap_s",
    "read_gap_s",
    "read_interval_s",
    "write_min_gap_s",
    "write_gap_s",
    "write_interval_s",
    "cycle_time_ms",
    "min_gap_ms",
    "channel_delay_read_ms",
    "channel_delay_write_ms",
    "modbus_delay_read",
    "modbus_delay_write",
    "deterministic",
    "rack",
    "slot",
    "s7_multi_var_limit",
    "s7_multi_read_max_gap",
    "s7_read_optimizer",
    "s7_max_parallel",
    "deterministic_cycle",
    "sensor_block_size",
    "batch_actuator",
    "batch_actuator_timeout",
    "actuator_block_size",
    "batch_actuator_flush_on_sensor_cycle",
    "chk_ctrl_command",
    "actuator_feedback_stream",
    "comm_profile",
    "timing_profile",
    "comm_ref",
)


def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _merge_controller_patch_record(target, patch):
    merged = dict(target or {})
    if not isinstance(patch, dict):
        return merged

    for key, value in patch.items():
        if key == "controller_id":
            continue

        if key == "comm" and isinstance(value, dict):
            base_comm = merged.get("comm")
            if not isinstance(base_comm, dict):
                base_comm = {}
            new_comm = dict(base_comm)
            new_comm.update(value)
            merged["comm"] = new_comm
            continue

        merged[key] = value

    return merged


def _controller_patch_section_names(config_data):
    if not isinstance(config_data, dict):
        return ()

    names = []
    seen = set()

    for key in _CONTROLLER_PATCH_SECTION_NAMES:
        if key in config_data and key not in seen:
            names.append(key)
            seen.add(key)

    for key in config_data.keys():
        try:
            key_l = str(key).strip().lower()
        except Exception:
            continue
        if key in seen:
            continue
        if "controller" not in key_l:
            continue
        if "patch" in key_l or "override" in key_l:
            names.append(key)
            seen.add(key)

    return tuple(names)


def _find_live_controller_record(config_data, controller_id):
    cid = _safe_int(controller_id, None)
    if cid is None or not isinstance(config_data, dict):
        return {}

    controllers = config_data.get("controllers") or []
    if not isinstance(controllers, list):
        return {}

    for rec in controllers:
        if not isinstance(rec, dict):
            continue
        if _safe_int(rec.get("controller_id"), None) != cid:
            continue
        return dict(rec)

    return {}


def _find_controller_patch_record(config_data, controller_id):
    cid = _safe_int(controller_id, None)
    if cid is None or not isinstance(config_data, dict):
        return {}

    merged = {}

    for section_name in _controller_patch_section_names(config_data):
        section = config_data.get(section_name)

        if isinstance(section, dict):
            patch = section.get(cid)
            if patch is None:
                patch = section.get(str(cid))
            if isinstance(patch, dict):
                merged = _merge_controller_patch_record(merged, patch)
            continue

        if not isinstance(section, (list, tuple)):
            continue

        for rec in section:
            if not isinstance(rec, dict):
                continue
            if _safe_int(rec.get("controller_id"), None) != cid:
                continue
            merged = _merge_controller_patch_record(merged, rec)

    if merged:
        merged["controller_id"] = cid

    return merged


def resolve_effective_controller_data(config_data, controller_data=None):
    merged = {}
    if isinstance(controller_data, dict):
        merged.update(controller_data)

    cid = _safe_int(merged.get("controller_id"), None)
    live_record = _find_live_controller_record(config_data, cid)
    if live_record:
        merged.update(live_record)
        cid = _safe_int(merged.get("controller_id"), cid)

    patch_record = _find_controller_patch_record(config_data, cid)
    if patch_record:
        merged = _merge_controller_patch_record(merged, patch_record)

    return merged


def resolve_controller_comm_config(config_data, controller_data=None, defaults=None):
    merged = {}
    if isinstance(defaults, dict):
        merged.update(defaults)

    effective_controller = resolve_effective_controller_data(config_data, controller_data)

    if isinstance(config_data, dict):
        global_comm = config_data.get("comm")
        if isinstance(global_comm, dict):
            merged.update(global_comm)

        for section_name in ("bus_timing", "polling"):
            section = config_data.get(section_name)
            if isinstance(section, dict):
                merged.update(section)

        for key in (
            "sensor_read_interval",
            "actuator_write_interval",
            "abs_time_out",
            "abs_time_out_ms",
            "poll_interval_s",
            "sensor_poll_interval_s",
            "bus_poll_interval_s",
            "controller_poll_interval_s",
            "poll_rate_hz",
            "io_min_gap_s",
            "bus_min_gap_s",
            "request_gap_s",
            "request_interval_s",
            "read_min_gap_s",
            "read_gap_s",
            "read_interval_s",
            "write_min_gap_s",
            "write_gap_s",
            "write_interval_s",
            "cycle_time_ms",
            "min_gap_ms",
            "channel_delay_read_ms",
            "channel_delay_write_ms",
            "modbus_delay_read",
            "modbus_delay_write",
            "deterministic",
            "deterministic_cycle",
        ):
            if key in config_data:
                merged[key] = config_data.get(key)

    ctrl_comm = effective_controller.get("comm") or {}
    profile_name = (
        effective_controller.get("timing_profile")
        or effective_controller.get("comm_profile")
        or effective_controller.get("comm_ref")
        or (ctrl_comm.get("timing_profile") if isinstance(ctrl_comm, dict) else None)
        or (ctrl_comm.get("comm_profile") if isinstance(ctrl_comm, dict) else None)
        or (ctrl_comm.get("comm_ref") if isinstance(ctrl_comm, dict) else None)
    )

    if profile_name and isinstance(config_data, dict):
        timing_profiles = config_data.get("timing_profiles") or {}
        comm_profiles = config_data.get("comm_profiles") or {}
        profile = None
        if isinstance(timing_profiles, dict):
            profile = timing_profiles.get(str(profile_name))
        if profile is None and isinstance(comm_profiles, dict):
            profile = comm_profiles.get(str(profile_name))
        if isinstance(profile, dict):
            merged.update(profile)

    for key in _CONTROLLER_DIRECT_CONFIG_KEYS:
        if key == "comm":
            continue
        if key in effective_controller:
            merged[key] = effective_controller.get(key)

    if isinstance(ctrl_comm, dict):
        merged.update(ctrl_comm)

    return merged, effective_controller, profile_name


# =============================================================================
# Timing Profile Defaults pro Protokoll
# =============================================================================

PROTOCOL_TIMING_DEFAULTS = {
    "modbus_tcp": {
        "cycle_time_s":      0.1,        # 100ms Polling-Intervall
        "min_gap_s":         0.015,      # 15ms globaler Mindestabstand
        "channel_delay_read_s":  0.0,    # Kein zusätzlicher Read-Delay
        "channel_delay_write_s": 0.0,    # Kein zusätzlicher Write-Delay
        "deterministic":     False,
    },
    "profinet_rt": {
        "cycle_time_s":      0.032,      # 32ms typisch für PROFINET RT
        "min_gap_s":         0.001,      # 1ms
        "channel_delay_read_s":  0.0,
        "channel_delay_write_s": 0.0,
        "deterministic":     True,
    },
    "profibus_gw": {
        "cycle_time_s":      0.1,        # 100ms via Gateway
        "min_gap_s":         0.01,       # 10ms (Gateway-begrenzt)
        "channel_delay_read_s":  0.0,
        "channel_delay_write_s": 0.0,
        "deterministic":     False,
    },
    "ethernet_ip": {
        "cycle_time_s":      0.01,       # 10ms typisch
        "min_gap_s":         0.001,      # 1ms
        "channel_delay_read_s":  0.0,
        "channel_delay_write_s": 0.0,
        "deterministic":     False,
    },
    "knx_ip": {
        "cycle_time_s":      1.0,        # 1s (KNX ist langsam)
        "min_gap_s":         0.05,       # 50ms
        "channel_delay_read_s":  0.0,
        "channel_delay_write_s": 0.0,
        "deterministic":     False,
    },
    "plc_modbus": {
        "cycle_time_s":      0.1,
        "min_gap_s":         0.015,
        "channel_delay_read_s":  0.0,
        "channel_delay_write_s": 0.0,
        "deterministic":     False,
    },
    "plc_s7": {
        "cycle_time_s":      0.1,
        "min_gap_s":         0.005,
        "channel_delay_read_s":  0.0,
        "channel_delay_write_s": 0.0,
        "deterministic":     False,
    },
    "plc_opcua": {
        "cycle_time_s":      0.1,
        "min_gap_s":         0.005,      # 5ms (OPC-UA ist schneller)
        "channel_delay_read_s":  0.0,
        "channel_delay_write_s": 0.0,
        "deterministic":     False,
    },
}


# =============================================================================
# Timing Context
# =============================================================================

def create_timing_context(protocol_type, config=None):
    """Erzeugt einen Timing-Context für ein Protokoll.

    Merge-Reihenfolge:
    1. Protokoll-Defaults (PROTOCOL_TIMING_DEFAULTS)
    2. Config-Overrides (aus config_data["comm"] oder timing_profiles)
    """
    defaults = dict(PROTOCOL_TIMING_DEFAULTS.get(
        str(protocol_type), PROTOCOL_TIMING_DEFAULTS["modbus_tcp"]
    ))

    # Config-Overrides anwenden
    if isinstance(config, dict):
        for key in ("cycle_time_s", "min_gap_s",
                    "channel_delay_read_s", "channel_delay_write_s",
                    "deterministic"):
            if key in config:
                try:
                    if key == "deterministic":
                        defaults[key] = bool(config[key])
                    else:
                        defaults[key] = float(config[key])
                except Exception:
                    pass

        # Legacy-Mapping: abs_time_out_ms → min_gap_s
        if "abs_time_out_ms" in config and "min_gap_s" not in config:
            try:
                defaults["min_gap_s"] = float(config["abs_time_out_ms"]) / 1000.0
            except Exception:
                pass

        # Legacy-Mapping: abs_time_out → min_gap_s
        if "abs_time_out" in config and "min_gap_s" not in config:
            try:
                defaults["min_gap_s"] = float(config["abs_time_out"])
            except Exception:
                pass

        # Legacy-Mapping: modbus_delay_read → channel_delay_read_s
        if "modbus_delay_read" in config:
            try:
                defaults["channel_delay_read_s"] = float(config["modbus_delay_read"])
            except Exception:
                pass

        # Legacy-Mapping: modbus_delay_write → channel_delay_write_s
        if "modbus_delay_write" in config:
            try:
                defaults["channel_delay_write_s"] = float(config["modbus_delay_write"])
            except Exception:
                pass

        # Direkte, benutzerfreundliche Aliase für Polling / Rate-Limit
        _apply_first_float(
            config,
            defaults,
            "cycle_time_s",
            (
                "poll_interval_s",
                "sensor_poll_interval_s",
                "bus_poll_interval_s",
                "controller_poll_interval_s",
                "cycle_time_ms",
            ),
        )
        if "cycle_time_ms" in config:
            try:
                defaults["cycle_time_s"] = float(config["cycle_time_ms"]) / 1000.0
            except Exception:
                pass

        if "poll_rate_hz" in config:
            try:
                poll_rate_hz = float(config["poll_rate_hz"])
                if poll_rate_hz > 0.0:
                    defaults["cycle_time_s"] = 1.0 / poll_rate_hz
            except Exception:
                pass

        _apply_first_float(
            config,
            defaults,
            "min_gap_s",
            (
                "io_min_gap_s",
                "bus_min_gap_s",
                "request_gap_s",
                "request_interval_s",
                "min_gap_ms",
            ),
        )
        if "min_gap_ms" in config:
            try:
                defaults["min_gap_s"] = float(config["min_gap_ms"]) / 1000.0
            except Exception:
                pass

        _apply_first_float(
            config,
            defaults,
            "channel_delay_read_s",
            (
                "read_min_gap_s",
                "read_gap_s",
                "read_interval_s",
                "channel_delay_read_ms",
            ),
        )
        if "channel_delay_read_ms" in config:
            try:
                defaults["channel_delay_read_s"] = float(config["channel_delay_read_ms"]) / 1000.0
            except Exception:
                pass

        _apply_first_float(
            config,
            defaults,
            "channel_delay_write_s",
            (
                "write_min_gap_s",
                "write_gap_s",
                "write_interval_s",
                "channel_delay_write_ms",
            ),
        )
        if "channel_delay_write_ms" in config:
            try:
                defaults["channel_delay_write_s"] = float(config["channel_delay_write_ms"]) / 1000.0
            except Exception:
                pass

        if "deterministic_cycle" in config and "deterministic" not in config:
            try:
                defaults["deterministic"] = bool(config["deterministic_cycle"])
            except Exception:
                pass

    return {
        "protocol_type":          str(protocol_type),
        "cycle_time_s":           float(defaults.get("cycle_time_s", 0.1)),
        "min_gap_s":              float(defaults.get("min_gap_s", 0.0)),
        "channel_delay_read_s":   float(defaults.get("channel_delay_read_s", 0.0)),
        "channel_delay_write_s":  float(defaults.get("channel_delay_write_s", 0.0)),
        "deterministic":          bool(defaults.get("deterministic", False)),

        # Laufzeit-State
        "last_any_activity_ts":   0.0,
        "last_read_ts":           0.0,
        "last_write_ts":          0.0,
    }


# =============================================================================
# Timing Gate (Ersatz für command_delay)
# =============================================================================

def timing_gate(timing_ctx, channel=None):
    """Erzwingt den minimalen Abstand zwischen I/O-Operationen.

    Blockiert den aufrufenden Thread via time.sleep() wenn nötig.
    CPU-effizient: Wartet nur die tatsächlich nötige Restzeit.

    channel:
        "read"  → zusätzlicher channel_delay_read_s
        "write" → zusätzlicher channel_delay_write_s
        None    → nur min_gap_s (globaler Abstand)

    SYNCHRON: Diese Funktion blockiert den aufrufenden Thread.
    Das ist beabsichtigt – der Connection Thread soll warten!
    """
    min_gap = float(timing_ctx.get("min_gap_s", 0.0))

    if channel == "write":
        channel_delay = float(timing_ctx.get("channel_delay_write_s", 0.0))
        last_channel_ts = float(timing_ctx.get("last_write_ts", 0.0))
    elif channel == "read":
        channel_delay = float(timing_ctx.get("channel_delay_read_s", 0.0))
        last_channel_ts = float(timing_ctx.get("last_read_ts", 0.0))
    else:
        channel_delay = 0.0
        last_channel_ts = 0.0

    # Kein Timing nötig?
    if min_gap <= 0.0 and channel_delay <= 0.0:
        return

    now = time.time()
    sleep_needed = 0.0

    # Globaler Mindestabstand
    if min_gap > 0.0:
        last_any = float(timing_ctx.get("last_any_activity_ts", 0.0))
        remaining_gap = min_gap - (now - last_any)
        if remaining_gap > sleep_needed:
            sleep_needed = remaining_gap

    # Kanal-spezifischer Abstand
    if channel_delay > 0.0 and last_channel_ts > 0.0:
        remaining_chan = channel_delay - (now - last_channel_ts)
        if remaining_chan > sleep_needed:
            sleep_needed = remaining_chan

    # Blockierend warten
    if sleep_needed > 0.0:
        time.sleep(sleep_needed)
        now = time.time()

    # Zeitstempel aktualisieren
    timing_ctx["last_any_activity_ts"] = now
    if channel == "read":
        timing_ctx["last_read_ts"] = now
    elif channel == "write":
        timing_ctx["last_write_ts"] = now


# =============================================================================
# Deterministisches Cycle-Sleep
# =============================================================================

def cycle_sleep(timing_ctx, cycle_start_ts, stop_event=None):
    """Deterministisches Sleep bis zum nächsten Zyklusstart.

    Berechnet: sleep_time = cycle_time - (jetzt - cycle_start)
    Minimum: 1ms (nie 0, nie negativ)

    Für Sensor-Polling: sorgt für konstantes Intervall unabhängig
    von der Zyklusdauer.
    """
    cycle_time = float(timing_ctx.get("cycle_time_s", 0.1))
    elapsed = time.monotonic() - float(cycle_start_ts)
    sleep_s = cycle_time - elapsed

    if sleep_s < 0.001:
        sleep_s = 0.001

    wait = getattr(stop_event, "wait", None)
    if callable(wait):
        wait(sleep_s)
    else:
        time.sleep(sleep_s)


# =============================================================================
# Timing-Profile aus Config laden
# =============================================================================

def load_timing_profile_from_config(config_data, controller_data, defaults=None):
    """Lädt das Timing-Profile für einen Controller aus der Config.

    Merge-Reihenfolge:
    1. Protokoll-Defaults
    2. Deployment-profile defaults for the selected controller
    3. Globale config_data["comm"] / bus_timing / polling / top-level timing keys
    4. Controller-spezifische Patch-Sektionen (z. B. controllers_patch_example)
    5. timing_profiles/comm_profiles[profile_name]
    6. controller_data["comm"] bzw. direkte Controller-Overrides

    Wichtig:
    Controller-Patches werden NICHT in die Controllers-Tabelle materialisiert,
    sondern hier zur Laufzeit aufgelöst. Dadurch bleiben bestehende Features
    erhalten und Hot-Reloads greifen sofort im Controller-Thread.
    """
    effective_config, effective_controller, _profile_name = resolve_controller_comm_config(
        config_data,
        controller_data,
        defaults=defaults,
    )

    protocol = str(
        effective_controller.get("protocol_type")
        or effective_controller.get("bus_type")
        or "modbus"
    ).lower()
    if protocol == "modbus":
        protocol = "modbus_tcp"

    merged = dict(effective_config or {})

    # Legacy-Mapping: sensor_read_interval → cycle_time_s
    if "sensor_read_interval" in merged and "cycle_time_s" not in merged:
        try:
            merged["cycle_time_s"] = float(merged["sensor_read_interval"])
        except Exception:
            pass

    return create_timing_context(protocol, merged)
