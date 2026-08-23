# -*- coding: utf-8 -*-

"""A failed Cross component must not stop the node-local runtime."""

import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.system.bootstrap_runtime_interface import (
    get_runtime_snapshot,
    start_runtime,
    stop_runtime,
)
from src.system.bootstrap_system_interface import start_system


def run_test():
    main_context = start_system(
        {},
        active_profile="unit_test",
        enable_ascii=False,
        enable_cpu_probe=False,
    )
    cross = main_context["app_config"].setdefault("cross_runtime", {})
    cross.update({
        "enabled": True,
        "node_id": "fn-01",
        "domain_id": "broken-cross-test",
        "config_path": os.path.join(PROJECT_ROOT, "missing-cross.json"),
    })
    try:
        start_runtime(main_context)
        snapshot = get_runtime_snapshot(main_context["runtime"])
        assert snapshot["state"] == "degraded"
        assert snapshot["components"]["thread_management"]["thread_alive"]
        assert snapshot["components"]["cross_worker"]["state"] == "degraded"
        assert "cross_worker" in snapshot["degraded_components"]
        assert snapshot["thread_management"]["state"] == "running"
    finally:
        stop_runtime(main_context)
    print("PASS Cross failure is contained and local runtime remains active")


if __name__ == "__main__":
    run_test()
