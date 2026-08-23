# -*- coding: utf-8 -*-

# src/system/bootstrap_system_interface.py


"""
 =============================================================================
 BOOTSTRAP_SYSTEM_INTERFACE
 =============================================================================

 What it is:
     A clean, functional entry-point module that provides the single
     `start_system(...)` interface to initialize and start **all system-level
     libraries and infrastructure** (logging, configuration, ASCII startup art,
     metrics registry, CPU/Memory probe, system state registration, etc.).
     It does **NOT** create application queues or start core worker/controller/
     automation/gateway threads – those are deferred to a future
     `bootstrap_runtime_interface.py` (or equivalent) as explicitly requested.

 Why it is needed:
     • Eliminates the monolithic, scattered initialization code currently
       duplicated inside `async_xserver_main.py:main()`.
     • Guarantees deterministic, ordered startup of foundational services
       before any business logic or thread orchestration begins.
     • Respects the project-wide coding spec (functional style with
       `functools.partial`, no classes, no decorators, no type hints, English
       comments, high determinism).
     • Makes the YAML `app_config.yaml` (with profile merging) the single
       source of truth for startup behavior.
     • Uses a central `main_ctx` that is passed through all bootstrap phases
       for maximum consistency and minimal redundancy.

 How it works:
     1.  `start_system(main_ctx=None, config_path, active_profile, ...)` is called.
     2.  YAML is loaded + deep-merged with selected profile.
     3.  Critical runtime globals are patched.
     4.  `_system_init.initialize_system()` is invoked.
     5.  Optional ASCII startup sequence is executed.
     6.  CPU+Memory probe is started if enabled.
     7.  The central `main_ctx` is enriched with `shutdown_event`, `app_config`,
         probe handles etc. and returned.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import os
import sys
import time
import threading
import logging
from functools import partial
from copy import deepcopy

try:
    import yaml
except Exception:
    yaml = None


# -------------------------------------------------------
# Logger
logger = logging.getLogger(__name__)
# -------------------------------------------------------


# =============================================================================
# CENTRAL MODULE "CONTEXT MANAGEMENT"
# =============================================================================

CONTEXT = {
    "library_name": "bootstrap_system_interface",
    "library_version": "1.1.0",
    "description": "Single entry-point for system libraries bootstrap using central main_ctx",
    "options": {
        "default_config_path": "_config/application/app_config.yaml",
        "default_active_profile": None,
        "enable_ascii_default": True,
        "enable_cpu_probe_default": True,
        "ascii_default_sleep_s": 2.0,
    },
}


# =============================================================================
# HELP-FUNCTIONS
# =============================================================================

def _is_dict(obj):
    return isinstance(obj, dict)


def _deep_merge_dicts(base, override):
    if not _is_dict(base):
        base = {}
    if not _is_dict(override):
        override = {}
    merged = deepcopy(base)
    for k, v in override.items():
        if _is_dict(v) and _is_dict(merged.get(k)):
            merged[k] = _deep_merge_dicts(merged[k], v)
        else:
            merged[k] = deepcopy(v)
    return merged


def _as_bool(v, default=False):
    if v is None:
        return bool(default)
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "on", "enabled"):
            return True
        if s in ("0", "false", "no", "off", "disabled"):
            return False
    return bool(default)


def _as_int(v, default=0):
    try:
        return int(v) if v is not None else int(default)
    except Exception:
        return int(default)


def _as_str(v, default=""):
    try:
        return str(v) if v is not None else str(default)
    except Exception:
        return str(default)


def _resolve_project_root():
    current = os.path.abspath(__file__)
    return os.path.normpath(os.path.join(os.path.dirname(current), "..", ".."))


def _resolve_path(project_root, rel_or_abs):
    if not rel_or_abs:
        return rel_or_abs
    if os.path.isabs(rel_or_abs):
        return os.path.normpath(rel_or_abs)
    return os.path.normpath(os.path.join(project_root, rel_or_abs))


def _fmt_host_for_uri(host, use_ipv6):
    h = str(host).strip()
    if not h:
        return "localhost"
    if use_ipv6 and (":" in h) and not (h.startswith("[") and h.endswith("]")):
        return "[" + h + "]"
    return h


# =============================================================================
# CODEBASE
# =============================================================================

def load_app_config(config_path=None, active_profile=None):
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to load app_config.yaml; install python3-yaml")

    project_root = _resolve_project_root()
    cfg_file = _resolve_path(project_root, config_path or CONTEXT["options"]["default_config_path"])

    if not os.path.exists(cfg_file):
        raise FileNotFoundError("application config does not exist: " + cfg_file)

    try:
        with open(cfg_file, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except Exception as exc:
        raise ValueError(
            "application config could not be loaded: " + cfg_file
        ) from exc

    if not _is_dict(raw):
        raise ValueError("application config root must be a mapping: " + cfg_file)

    prof = str(active_profile).strip() if active_profile is not None else ""
    if not prof:
        prof = str(os.environ.get("AUTOMATION_PROFILE") or "").strip()
    if not prof:
        prof = str(raw.get("active_profile", "base")).strip() if raw.get("active_profile") is not None else "base"
    if not prof:
        prof = "base"

    base_cfg = raw.get("base", {}) if _is_dict(raw.get("base")) else {}
    profiles = raw.get("profiles", {}) if _is_dict(raw.get("profiles")) else {}
    if prof != "base" and prof not in profiles:
        raise ValueError("unknown application profile: " + prof)
    prof_cfg = profiles.get(prof, {}) if _is_dict(profiles.get(prof)) else {}

    effective = _deep_merge_dicts(base_cfg, prof_cfg)
    effective["_active_profile"] = prof
    logger.info("[bootstrap] YAML loaded: file=%s profile=%s", cfg_file, prof)
    return effective


def _apply_runtime_globals(effective_cfg):
    if not _is_dict(effective_cfg):
        return

    node_id = effective_cfg.get("app", {}).get("node_id")
    if node_id is not None:
        globals()["node_id"] = _as_str(node_id, globals().get("node_id", "fn-01"))

    show_ascii = effective_cfg.get("app", {}).get("show_ascii")
    if show_ascii is not None:
        globals()["show_ascii"] = _as_bool(show_ascii, True)

    net = effective_cfg.get("network", {})
    master = net.get("master", {}) if _is_dict(net.get("master")) else {}

    local_mode = _as_bool(net.get("local_mode"), globals().get("LOCAL_MODE", False))
    globals()["LOCAL_MODE"] = local_mode

    master_ip = _as_str(master.get("ip"), globals().get("cfg", {}).get("ip", "192.168.0.26"))
    protocol = _as_str(master.get("protocol"), "ws")
    use_ipv6 = _as_bool(master.get("use_ipv6"), False)
    worker_port = _as_int(master.get("websocket_port_worker"), globals().get("WEBSOCKET_PORT_WORKER", 8000))
    client_port = _as_int(master.get("websocket_port_client"), globals().get("WEBSOCKET_PORT_CLIENT", 8001))
    local_ip_override = _as_str(master.get("local_ip_override"), "localhost")

    globals()["WEBSOCKET_PORT_WORKER"] = int(worker_port)
    globals()["WEBSOCKET_PORT_CLIENT"] = int(client_port)

    host = local_ip_override if local_mode else master_ip
    globals()["MASTER_URI"] = f"{protocol}://{_fmt_host_for_uri(host, use_ipv6)}:{int(worker_port)}"

    globals()["cfg"] = {
        "ip": str(master_ip),
        "worker_port": int(worker_port),
        "client_port": int(client_port),
        "protocol": str(protocol),
        "use_ipv6": bool(use_ipv6),
        "local_mode": bool(local_mode),
    }

    ws = globals().get("worker_state")
    if _is_dict(ws):
        try:
            ws_lock = globals().get("worker_state_lock")
            rt_mode = effective_cfg.get("worker_state", {}).get("run_time_mode")
            persist = effective_cfg.get("worker_state", {}).get("persist_data")

            if ws_lock is not None:
                with ws_lock:
                    ws["LOCAL_MODE"] = bool(local_mode)
                    ws["MASTER"] = globals()["MASTER_URI"]
                    if globals().get("node_id"):
                        ws["NODE_ID"] = globals().get("node_id")
                    if rt_mode is not None:
                        ws["RUN_TIME"] = _as_str(rt_mode, ws.get("RUN_TIME", ""))
                    if persist is not None:
                        ws["PERSIST_DATA"] = _as_bool(persist, ws.get("PERSIST_DATA", True))
            else:
                ws["LOCAL_MODE"] = bool(local_mode)
                ws["MASTER"] = globals()["MASTER_URI"]
        except Exception:
            pass

    project_root = _resolve_project_root()
    main_rel = effective_cfg.get("paths", {}).get("device_state_json")
    test_rel = effective_cfg.get("paths", {}).get("device_state_json_test")
    if main_rel:
        globals()["CONFIG_PATH"] = _resolve_path(project_root, _as_str(main_rel, ""))
    if test_rel:
        globals()["CONFIG_PATH_"] = _resolve_path(project_root, _as_str(test_rel, ""))

    logger.debug("[bootstrap] Runtime globals applied (LOCAL_MODE=%s, MASTER_URI=%s)",
                 local_mode, globals().get("MASTER_URI"))


def _get_ascii_config(effective_cfg):
    ascii_cfg = effective_cfg.get("app", {}).get("ascii", {}) if _is_dict(effective_cfg.get("app")) else {}
    return {
        "blank_lines_before": _as_int(ascii_cfg.get("blank_lines_before_each_art"), 3),
        "blank_lines_after": _as_int(ascii_cfg.get("blank_lines_after_each_art"), 3),
        "sleep_s": float(ascii_cfg.get("sleep_s_default", CONTEXT["options"]["ascii_default_sleep_s"])),
        "arts": ascii_cfg.get("arts", [
            {"name": "ASCII_TEXT", "indent": 3, "palette": "CYBER_TEXT_PALETTE"},
            {"name": "ASCII_CYBER_PUNK", "indent": 3, "palette": "CYBERPALM_PALETTE"},
            {"name": "ASCII_CTRL", "indent": 3, "palette": None},
            {"name": "ASCII_PENGUIN", "indent": 9, "palette": None},
            {"name": "METRICS_LEGENDE", "indent": 3, "palette": None},
        ]),
    }


def _print_ascii_startup(effective_cfg):
    try:
        import src.system._ascii_plotter as ascii_plotter
    except Exception as exc:
        logger.warning("[bootstrap] Could not import _ascii_plotter: %s", exc)
        return

    if not globals().get("show_ascii", True):
        logger.info("[bootstrap] ASCII startup disabled via config")
        return

    ascii_conf = _get_ascii_config(effective_cfg)
    blank_before = ascii_conf["blank_lines_before"]
    blank_after = ascii_conf["blank_lines_after"]
    sleep_s = ascii_conf["sleep_s"]

    for art in ascii_conf["arts"]:
        name = art.get("name")
        indent = _as_int(art.get("indent"), 3)
        palette_name = art.get("palette")

        ascii_str = getattr(ascii_plotter, name, None)
        if ascii_str is None:
            logger.debug("[bootstrap] ASCII art '%s' not found in plotter – skipped", name)
            continue

        rgb = None
        if palette_name == "CYBER_TEXT_PALETTE":
            rgb = ascii_plotter.CYBER_TEXT_PALETTE
        elif palette_name == "CYBERPALM_PALETTE":
            rgb = ascii_plotter.CYBERPALM_PALETTE

        for _ in range(blank_before):
            print()

        ascii_plotter.print_ascii_art(ascii_str, indent=indent, rgb=rgb)

        for _ in range(blank_after):
            print()

        if sleep_s > 0:
            time.sleep(sleep_s)

    logger.info("[bootstrap] ASCII startup sequence completed")


def _start_cpu_mem_probe_if_enabled(effective_cfg, shutdown_event):
    cpu_cfg = effective_cfg.get("cpu_probe", {}) if _is_dict(effective_cfg.get("cpu_probe")) else {}
    enabled = _as_bool(cpu_cfg.get("enabled"), CONTEXT["options"]["enable_cpu_probe_default"])

    if not enabled:
        logger.info("[bootstrap] CPU-Mem probe disabled via config")
        return None, None, None

    try:
        import src.system.cpu_mem_probe as cpu_probe
    except Exception as exc:
        logger.warning("[bootstrap] cpu_mem_probe not importable: %s", exc)
        return None, None, None

    info = {}
    info_lock = threading.Lock()

    probe_thread = cpu_probe.start_cpu_mem_probe(
        shutdown_event=shutdown_event,
        info=info,
        info_lock=info_lock,
        interval=float(cpu_cfg.get("interval_s", 1.0)),
        top_n=_as_int(cpu_cfg.get("top_n"), 12),
        min_pct=float(cpu_cfg.get("min_pct", 0.1)),
        history_max=_as_int(cpu_cfg.get("history_max"), 300),
        normalize_per_cpu=True,
        log_top=_as_bool(cpu_cfg.get("log_top"), True),
    )

    if probe_thread is not None:
        logger.info("[bootstrap] CPU-Mem probe started (interval=%.1fs)", float(cpu_cfg.get("interval_s", 1.0)))
    return info, info_lock, probe_thread


# =============================================================================
# INTERFACE
# =============================================================================

def start_system(main_ctx=None, config_path=None, active_profile=None,
                 enable_ascii=None, enable_cpu_probe=None):
    """
    Phase 1: System Bootstrap.

    Accepts an optional central `main_ctx` dictionary, enriches it with all
    system-level resources (`shutdown_event`, `app_config`, CPU/Mem probe etc.)
    and returns the enriched context.

    This design enables a clean single-context flow through both bootstrap
    phases with minimal redundancy.
    """
    if main_ctx is None:
        main_ctx = {}
    if not isinstance(main_ctx, dict):
        main_ctx = {}

    global logger
    logger.info("[system] === SYSTEM BOOTSTRAP STARTING ===")

    effective_cfg = load_app_config(config_path, active_profile)
    _apply_runtime_globals(effective_cfg)

    try:
        import src.system._system_init as _system_init
        _system_init.initialize_system()
        logger = logging.getLogger(__name__)
        logger.info("[system] Logging system initialized")
        logger.info(
            "[system] Active application profile: %s",
            effective_cfg.get("_active_profile", "base"),
        )
    except Exception as exc:
        print(f"[system][FATAL] Could not initialize logging: {exc}")
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    ascii_enabled = _as_bool(enable_ascii, globals().get("show_ascii", True))
    if ascii_enabled:
        _print_ascii_startup(effective_cfg)
    else:
        logger.info("[system] ASCII startup skipped (enable_ascii=False)")

    shutdown_event = threading.Event()
    # shutdown_event = main_ctx.get("shutdown_event")

    cpu_enabled = _as_bool(enable_cpu_probe, True)
    cpu_info, cpu_lock, cpu_thread = (None, None, None)
    if cpu_enabled:
        cpu_info, cpu_lock, cpu_thread = _start_cpu_mem_probe_if_enabled(effective_cfg, shutdown_event)

    try:
        import src.system._metrics_system as _metrics
        logger.debug("[system] Metrics system ready")
    except Exception:
        pass

    # Enrich the central main_ctx instead of creating a new dict
    main_ctx.update({
        "shutdown_event": shutdown_event,
        "app_config": effective_cfg,
        "cpu_mem_info": cpu_info,
        "cpu_mem_info_lock": cpu_lock,
        "cpu_probe_thread": cpu_thread,
        "project_root": _resolve_project_root(),
        "system_bootstrap_version": CONTEXT["library_version"],
        "timestamp": time.time(),
    })

    logger.info("[system] === SYSTEM BOOTSTRAP COMPLETE ===")
    logger.info("[system] Context ready for runtime bootstrap phase.")

    return main_ctx


# Convenience alias
initialize_and_start_system = start_system


def _initialize_context():
    logger.debug(
        "%s v%s ready (call start_system(main_ctx) to bootstrap)",
        CONTEXT["library_name"],
        CONTEXT["library_version"],
    )


_initialize_context()
