# -*- coding: utf-8 -*-

# src/system/_system_init.py


"""
 =============================================================================
 _SYSTEM_INIT
 =============================================================================

 What it is:
     A standard-library-only, functional logging and system diagnostics layer.

 Why it is needed:
     System bootstrap needs deterministic logging without optional packages,
     custom classes or hidden global executors. Legacy callers also use this
     module for thread, queue and environment diagnostics.

 How it works:
     One protected dict stores logging policy. Standard logging handlers and
     callable filters are configured explicitly. Every diagnostic returns a
     detached dictionary and optional dependencies are resolved lazily.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import atexit
import gzip
import importlib.metadata
import logging
import logging.handlers
import os
import pathlib
import queue
import shutil
import sys
import threading
import time

from datetime import datetime, timezone
from functools import partial


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "system_init",
    "library_version": "2.0.0",
    "lock": threading.RLock(),
    "initialized": False,
    "cleanup_registered": False,
    "started_at": time.time(),
    "current_log_file": None,
    "handlers": [],
    "allowed_modules": None,
    "module_levels": {},
    "disabled_modules": set(),
    "allowed_threads": None,
    "thread_levels": {},
    "disabled_threads": set(),
    "rotation": {
        "rotation_type": "size",
        "max_bytes": 10 * 1024 * 1024,
        "backup_count": 10,
        "when": "midnight",
        "interval": 1,
        "compress": False,
    },
}


# =============================================================================
# PATHS AND BASIC HELPERS
# =============================================================================

def get_project_root():
    return os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))


PROJECT_ROOT = get_project_root()


def get_logs_directory():
    directory = os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(directory, exist_ok=True)
    return directory


def get_default_log_file():
    return os.path.join(get_logs_directory(), "system.log")


def create_async_queue():
    return queue.Queue()


def get_active_threads_safe():
    try:
        return list(threading.enumerate())
    except Exception as exc:
        LOGGER.warning("Active-thread inspection failed: %s", exc)
        return []


def is_main_process():
    return os.getpid() != 0


def get_process_info():
    return {
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "cwd": os.getcwd(),
        "executable": sys.executable,
        "argv": list(sys.argv),
    }


def _resolve_log_path(log_file):
    path = str(log_file or "system.log")
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.join(get_logs_directory(), os.path.basename(path))


def _resolve_log_level(value, default=logging.INFO):
    if isinstance(value, int):
        return value
    name = str(value or "").strip().upper()
    return getattr(logging, name, default)


# =============================================================================
# LOG POLICY
# =============================================================================

def set_allowed_modules(*modules):
    with CONTEXT["lock"]:
        CONTEXT["allowed_modules"] = {
            str(module) for module in modules if str(module).strip()}


def allow_all_modules():
    with CONTEXT["lock"]:
        CONTEXT["allowed_modules"] = None


def set_module_level(module_name, level):
    with CONTEXT["lock"]:
        CONTEXT["module_levels"][str(module_name)] = _resolve_log_level(level)


def disable_module_logging(module_name):
    with CONTEXT["lock"]:
        CONTEXT["disabled_modules"].add(str(module_name))


def enable_module_logging(module_name, level=None):
    name = str(module_name)
    with CONTEXT["lock"]:
        CONTEXT["disabled_modules"].discard(name)
    if level is not None:
        set_module_level(name, level)


def set_allowed_threads(*thread_names):
    with CONTEXT["lock"]:
        CONTEXT["allowed_threads"] = {
            str(name) for name in thread_names if str(name).strip()}


def allow_all_threads():
    with CONTEXT["lock"]:
        CONTEXT["allowed_threads"] = None


def set_thread_level(thread_name, level):
    with CONTEXT["lock"]:
        CONTEXT["thread_levels"][str(thread_name)] = _resolve_log_level(level)


def disable_thread_logging(thread_name):
    with CONTEXT["lock"]:
        CONTEXT["disabled_threads"].add(str(thread_name))


def enable_thread_logging(thread_name, level=None):
    name = str(thread_name)
    with CONTEXT["lock"]:
        CONTEXT["disabled_threads"].discard(name)
    if level is not None:
        set_thread_level(name, level)


def set_rotation_config(rotation_type="size", **kwargs):
    allowed = {
        "max_bytes", "backup_count", "when", "interval", "compress"}
    with CONTEXT["lock"]:
        CONTEXT["rotation"]["rotation_type"] = str(rotation_type)
        for key in allowed:
            if key in kwargs:
                CONTEXT["rotation"][key] = kwargs[key]
        return dict(CONTEXT["rotation"])


def apply_thread_selection_config(selection):
    if not isinstance(selection, dict):
        return False
    if selection.get("allow_all", False):
        allow_all_threads()
    elif isinstance(selection.get("allowed"), list):
        set_allowed_threads(*selection["allowed"])
    for name in selection.get("disabled", []) or []:
        disable_thread_logging(name)
    for name, level in (selection.get("levels") or {}).items():
        set_thread_level(name, level)
    return True


def _module_policy(record, snapshot):
    name = str(record.name)
    if any(name == item or name.startswith(item + ".")
           for item in snapshot["disabled_modules"]):
        return False
    allowed = snapshot["allowed_modules"]
    if allowed and not any(name == item or name.startswith(item + ".")
                           for item in allowed):
        return False
    threshold = snapshot["module_levels"].get(name)
    return threshold is None or record.levelno >= threshold


def _thread_policy(record, snapshot):
    name = str(record.threadName)
    if name in snapshot["disabled_threads"]:
        return False
    allowed = snapshot["allowed_threads"]
    if allowed and name not in allowed:
        return False
    threshold = snapshot["thread_levels"].get(name)
    return threshold is None or record.levelno >= threshold


def _record_allowed(record, context):
    with context["lock"]:
        snapshot = {
            "allowed_modules": (
                None if context["allowed_modules"] is None
                else set(context["allowed_modules"])) ,
            "module_levels": dict(context["module_levels"]),
            "disabled_modules": set(context["disabled_modules"]),
            "allowed_threads": (
                None if context["allowed_threads"] is None
                else set(context["allowed_threads"])),
            "thread_levels": dict(context["thread_levels"]),
            "disabled_threads": set(context["disabled_threads"]),
        }
    return _module_policy(record, snapshot) and _thread_policy(record, snapshot)


# =============================================================================
# LOG HANDLERS
# =============================================================================

def _maybe_compress_log_file(filename):
    if not filename or filename.endswith(".gz") or not os.path.exists(filename):
        return False
    target = filename + ".gz"
    try:
        with open(filename, "rb") as source:
            with gzip.open(target, "wb") as destination:
                shutil.copyfileobj(source, destination)
        os.unlink(filename)
        return True
    except OSError:
        return False


def _make_file_handler(file_path):
    rotation = CONTEXT["rotation"]
    if rotation["rotation_type"] == "time":
        return logging.handlers.TimedRotatingFileHandler(
            file_path,
            when=str(rotation["when"]),
            interval=int(rotation["interval"]),
            backupCount=int(rotation["backup_count"]),
            encoding="utf-8",
        )
    return logging.handlers.RotatingFileHandler(
        file_path,
        maxBytes=int(rotation["max_bytes"]),
        backupCount=int(rotation["backup_count"]),
        encoding="utf-8",
    )


def _close_managed_handlers():
    root = logging.getLogger()
    with CONTEXT["lock"]:
        handlers = list(CONTEXT["handlers"])
        CONTEXT["handlers"].clear()
    for handler in handlers:
        try:
            root.removeHandler(handler)
            handler.flush()
            handler.close()
        except Exception:
            continue


def setup_logging(log_file="system.log", log_level=logging.INFO):
    file_path = _resolve_log_path(log_file)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    level = _resolve_log_level(log_level)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(threadName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _close_managed_handlers()
    file_handler = _make_file_handler(file_path)
    stream_handler = logging.StreamHandler()
    policy = partial(_record_allowed, context=CONTEXT)
    for handler in (file_handler, stream_handler):
        handler.setLevel(level)
        handler.setFormatter(formatter)
        handler.addFilter(policy)
    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)
    with CONTEXT["lock"]:
        CONTEXT["handlers"] = [file_handler, stream_handler]
        CONTEXT["current_log_file"] = file_path
        CONTEXT["initialized"] = True
    return file_path


def get_logger(name=None):
    return logging.getLogger(name) if name else logging.getLogger()


def cleanup_old_logs(base_pattern="system_*.log", keep_days=30, log_dir=None):
    directory = log_dir or get_logs_directory()
    cutoff = time.time() - max(0, int(keep_days)) * 86400
    removed = []
    for path in pathlib.Path(directory).glob(base_pattern):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed.append(str(path))
        except OSError:
            continue
    return tuple(removed)


# =============================================================================
# SYSTEM DIAGNOSTICS
# =============================================================================

def get_memory_usage():
    try:
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        divisor = 1024.0 if sys.platform != "darwin" else 1024.0 * 1024.0
        return float(value) / divisor
    except Exception:
        return 0.0


def get_system_info():
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "platform": sys.platform,
        "process": get_process_info(),
        "thread_count": len(get_active_threads_safe()),
        "memory_mb": get_memory_usage(),
        "free_threading": is_free_threading_enabled(),
    }


def log_system_info():
    info = get_system_info()
    LOGGER.info(
        "System pid=%s threads=%s memory_mb=%.2f free_threading=%s",
        info["process"]["pid"],
        info["thread_count"],
        info["memory_mb"],
        info["free_threading"],
    )
    return info


def get_thread_info():
    return tuple({
        "name": thread.name,
        "ident": thread.ident,
        "daemon": thread.daemon,
        "alive": thread.is_alive(),
    } for thread in get_active_threads_safe())


def is_free_threading_enabled():
    probe = getattr(sys, "_is_gil_enabled", None)
    if callable(probe):
        try:
            return not bool(probe())
        except Exception:
            return False
    return False


def assert_free_threading_or_exit(logger_instance=None, hard=False):
    active = is_free_threading_enabled()
    target = logger_instance or LOGGER
    if active:
        target.info("Python free-threading mode is active")
        return True
    message = "Python free-threading mode is not active"
    if hard:
        raise RuntimeError(message)
    target.warning(message)
    return False


def log_python_pip_versions():
    packages = ("numpy", "pandas", "websockets", "requests", "pytest", "colorama")
    versions = {"python": sys.version.split()[0]}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    LOGGER.info("Runtime versions: %s", versions)
    return versions


def log_imported_modules():
    names = tuple(sorted(sys.modules))
    LOGGER.debug("Imported modules: %d", len(names))
    return names


def check_system_processes():
    LOGGER.debug("System process check completed")
    return {"ok": True, "pid": os.getpid()}


def register_system_state(logger_instance=None):
    target = logger_instance or LOGGER
    info = get_system_info()
    target.info("System state registered: pid=%s", info["process"]["pid"])
    return info


def log_level_test():
    LOGGER.debug("logging level test: debug")
    LOGGER.info("logging level test: info")
    LOGGER.warning("logging level test: warning")
    LOGGER.error("logging level test: error")
    return True


# =============================================================================
# THREAD HELPERS
# =============================================================================

def run_in_thread(target, name=None, daemon=True, *args, **kwargs):
    thread = threading.Thread(
        target=partial(target, *args, **kwargs),
        name=name,
        daemon=bool(daemon),
    )
    thread.start()
    return thread


def _queue_broker_loop(context):
    source = context["queue"]
    while not context["stop_event"].is_set():
        try:
            item = source.get(timeout=0.25)
        except queue.Empty:
            continue
        try:
            if item is None:
                break
            context["handler"](item)
        except Exception:
            LOGGER.exception("Queue broker handler failed")
        finally:
            task_done = getattr(source, "task_done", None)
            if callable(task_done):
                task_done()


def create_queue_broker_thread(source_queue, name, handler_func, stop_event=None):
    if not callable(handler_func):
        raise TypeError("handler_func must be callable")
    context = {
        "queue": source_queue,
        "handler": handler_func,
        "stop_event": stop_event or threading.Event(),
    }
    thread = threading.Thread(
        target=partial(_queue_broker_loop, context),
        name=str(name),
        daemon=False,
    )
    thread.start()
    return thread


# =============================================================================
# PERFORMANCE COMPATIBILITY
# =============================================================================

def _performance_log(context, label, **values):
    entry = {"label": str(label), "timestamp": time.time()}
    entry.update(values)
    with context["lock"]:
        context["entries"].append(entry)
    return entry


def _performance_snapshot(context):
    with context["lock"]:
        return tuple(dict(entry) for entry in context["entries"])


def PerformanceLogger(name="performance"):
    context = {"name": str(name), "lock": threading.RLock(), "entries": []}
    return {
        "context": context,
        "log": partial(_performance_log, context),
        "snapshot": partial(_performance_snapshot, context),
    }


# =============================================================================
# PUBLIC LIFECYCLE
# =============================================================================

def configure_logging_for_production():
    set_rotation_config(
        "size", max_bytes=20 * 1024 * 1024, backup_count=10, compress=True)
    return setup_logging("system.log", logging.INFO)


def initialize_system():
    with CONTEXT["lock"]:
        already_initialized = CONTEXT["initialized"]
    if already_initialized:
        return {
            "ok": True,
            "log_file": CONTEXT["current_log_file"],
            "already_initialized": True,
        }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = setup_logging("system_" + stamp + ".log", logging.INFO)
    check_system_processes()
    info = register_system_state(LOGGER)
    versions = log_python_pip_versions()
    with CONTEXT["lock"]:
        if not CONTEXT["cleanup_registered"]:
            atexit.register(cleanup_system)
            CONTEXT["cleanup_registered"] = True
    LOGGER.info("System initialization complete")
    return {
        "ok": True,
        "log_file": file_path,
        "system": info,
        "versions": versions,
        "already_initialized": False,
    }


def cleanup_system():
    with CONTEXT["lock"]:
        if not CONTEXT["initialized"]:
            return {"ok": True, "already_stopped": True}
        file_path = CONTEXT["current_log_file"]
        compress = bool(CONTEXT["rotation"].get("compress"))
        CONTEXT["initialized"] = False
    LOGGER.info("System cleanup starting")
    _close_managed_handlers()
    compressed = _maybe_compress_log_file(file_path) if compress else False
    return {
        "ok": True,
        "already_stopped": False,
        "log_file": file_path,
        "compressed": compressed,
        "uptime_s": max(0.0, time.time() - CONTEXT["started_at"]),
    }
