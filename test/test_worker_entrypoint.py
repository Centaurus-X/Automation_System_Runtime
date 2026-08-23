# -*- coding: utf-8 -*-

"""Worker process entry-point error handling and shutdown logging test."""

import io
import logging
import os
import sys
import threading


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src import sync_xserver_main as worker_main


def _start_system(main_ctx):
    main_ctx["shutdown_event"] = threading.Event()
    return main_ctx


def _fail_runtime(_main_ctx):
    try:
        raise ValueError("test identity is missing")
    except ValueError as exc:
        raise RuntimeError("runtime bootstrap failed") from exc


def _no_operation(*_args, **_kwargs):
    return None


def _run_test():
    original_system = worker_main.start_system
    original_runtime = worker_main.start_runtime
    original_handlers = worker_main._install_signal_handlers
    original_join = worker_main._join_cpu_probe
    previous_debug = os.environ.pop("AUTOMATION_DEBUG_TRACEBACK", None)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)
    logger = worker_main.LOGGER
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        worker_main.start_system = _start_system
        worker_main.start_runtime = _fail_runtime
        worker_main._install_signal_handlers = _no_operation
        worker_main._join_cpu_probe = _no_operation
        result = worker_main.main()
    finally:
        worker_main.start_system = original_system
        worker_main.start_runtime = original_runtime
        worker_main._install_signal_handlers = original_handlers
        worker_main._join_cpu_probe = original_join
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        if previous_debug is not None:
            os.environ["AUTOMATION_DEBUG_TRACEBACK"] = previous_debug
    output = stream.getvalue()
    assert result == 1
    assert "Worker startup failed (ValueError): test identity is missing" in output
    assert "Worker shutdown starting (reason=startup_failure)" in output
    assert "Worker shutdown complete (exit_code=1)" in output
    assert "Traceback" not in output


if __name__ == "__main__":
    _run_test()
    print("PASS concise startup error and deterministic shutdown logging")
