# -*- coding: utf-8 -*-

# src/sync_xserver_main.py


"""
 =============================================================================
 SYNC_XSERVER_MAIN
 =============================================================================

 What it is:
     The small process entry point for the synchronous free-threaded runtime.

 Why it is needed:
     Process signals, system bootstrap, runtime bootstrap and final shutdown
     need one deterministic owner without importing optional application
     libraries before configuration and logging are ready.

 How it works:
     System resources start first, followed by the runtime composition root.
     Signals set the system shutdown event. The finalizer always performs the
     staged runtime stop and then joins the optional CPU probe.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import logging
import os
import signal
import threading

from src.system.bootstrap_runtime_interface import start_runtime, stop_runtime
from src.system.bootstrap_system_interface import start_system


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

LOGGER = logging.getLogger(__name__)

SIGNAL_CONTEXT = {
    "shutdown_event": None,
    "last_signal": None,
}


# =============================================================================
# SIGNAL AND SHUTDOWN HELPERS
# =============================================================================

def signal_handler(signum, frame):
    event = SIGNAL_CONTEXT.get("shutdown_event")
    SIGNAL_CONTEXT["last_signal"] = signum
    LOGGER.info("Signal %s received; shutdown requested", signum)
    if callable(getattr(event, "set", None)):
        event.set()


def _install_signal_handlers():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def _join_cpu_probe(main_ctx):
    thread = main_ctx.get("cpu_probe_thread")
    if thread is None or thread is threading.current_thread():
        return
    try:
        thread.join(timeout=5.0)
    except Exception as exc:
        LOGGER.warning("CPU probe join failed: %s", exc)


def _environment_flag(name):
    value = str(os.environ.get(name) or "").strip().lower()
    return value in ("1", "true", "yes", "on", "enabled")


def _root_exception(error):
    current = error
    visited = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        following = current.__cause__ or current.__context__
        if following is None:
            break
        current = following
    return current


def _log_startup_failure(error):
    root = _root_exception(error)
    LOGGER.error(
        "Worker startup failed (%s): %s",
        type(root).__name__, root,
        exc_info=_environment_flag("AUTOMATION_DEBUG_TRACEBACK"),
    )
    if not _environment_flag("AUTOMATION_DEBUG_TRACEBACK"):
        LOGGER.error(
            "Set AUTOMATION_DEBUG_TRACEBACK=1 only when a full diagnostic "
            "traceback is required")


# =============================================================================
# MAIN
# =============================================================================

def main():
    main_ctx = {}
    runtime_started = False
    exit_code = 0
    shutdown_reason = "normal"
    _install_signal_handlers()
    LOGGER.info("Starting synchronous worker runtime")

    try:
        main_ctx = start_system(main_ctx)
        SIGNAL_CONTEXT["shutdown_event"] = main_ctx["shutdown_event"]
        main_ctx = start_runtime(main_ctx)
        runtime_started = True
        LOGGER.info("Worker runtime is ready")

        while not main_ctx["shutdown_event"].wait(timeout=1.0):
            continue
        if SIGNAL_CONTEXT.get("last_signal") is not None:
            shutdown_reason = "signal_{}".format(
                SIGNAL_CONTEXT["last_signal"])

    except KeyboardInterrupt:
        shutdown_reason = "keyboard_interrupt"
        event = main_ctx.get("shutdown_event")
        if callable(getattr(event, "set", None)):
            event.set()

    except Exception as exc:
        exit_code = 1
        shutdown_reason = "startup_failure"
        _log_startup_failure(exc)

    finally:
        LOGGER.info("Worker shutdown starting (reason=%s)", shutdown_reason)
        if runtime_started:
            try:
                stop_runtime(main_ctx)
            except Exception as exc:
                exit_code = 1
                LOGGER.error(
                    "Runtime shutdown failed: %s",
                    exc,
                    exc_info=_environment_flag("AUTOMATION_DEBUG_TRACEBACK"),
                )
        event = main_ctx.get("shutdown_event")
        if callable(getattr(event, "set", None)):
            event.set()
        _join_cpu_probe(main_ctx)
        SIGNAL_CONTEXT["shutdown_event"] = None
        SIGNAL_CONTEXT["last_signal"] = None

    LOGGER.info("Worker shutdown complete (exit_code=%s)", exit_code)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
