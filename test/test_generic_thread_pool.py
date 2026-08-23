# -*- coding: utf-8 -*-

"""Deterministic lifecycle, scale and no-GIL shutdown regression tests."""

import os
import sys
import threading
import time

from functools import partial
from queue import Empty


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._generic_thread_pool import create_thread_pool


def _sum_values(values):
    return sum(values)


def _wait_then_return(started, release, value):
    started.set()
    release.wait(timeout=2.0)
    return value


def _shutdown_into(pool, result):
    result["snapshot"] = pool["shutdown"](join_timeout_s=2.0)


def _scale_until_stopped(pool, stop_event):
    target = 1
    while not stop_event.is_set():
        pool["scale_to"](target)
        target = 4 if target == 1 else 1
        stop_event.wait(0.001)


def _assert_stopped(snapshot):
    assert snapshot["shutdown"] is True
    assert snapshot["shutdown_status"] == "stopped", snapshot
    assert snapshot["live_threads"] == (), snapshot
    assert snapshot["manager_alive"] is False, snapshot
    assert not any(
        worker["alive"] for worker in snapshot["workers"].values()
    ), snapshot


def _test_result_lifecycle():
    pool = create_thread_pool(min_workers=2, max_workers=3)
    pool["start"]()
    job_id = pool["submit"](_sum_values, [1, 2, 3])
    try:
        result = pool["_out_q"].get(timeout=3.0)
    except Empty as exc:
        raise AssertionError("thread pool emitted no result") from exc
    assert result["job_id"] == job_id
    assert result["ok"] is True
    assert result["result"] == 6
    snapshot = pool["shutdown"](join_timeout_s=3.0)
    _assert_stopped(snapshot)


def _test_shutdown_with_active_job():
    pool = create_thread_pool(
        min_workers=1,
        max_workers=2,
        monitor_interval_s=0.01,
        worker_idle_timeout_s=0.01,
    )
    pool["start"]()
    started = threading.Event()
    release = threading.Event()
    job_id = pool["submit"](_wait_then_return, started, release, "done")
    assert job_id
    assert started.wait(timeout=2.0)

    result = {}
    shutdown_thread = threading.Thread(
        target=partial(_shutdown_into, pool, result),
        name="ThreadPoolActiveShutdownTest",
        daemon=False,
    )
    shutdown_thread.start()
    assert pool["context"]["shutdown_event"].wait(timeout=1.0)
    release.set()
    shutdown_thread.join(timeout=3.0)
    assert shutdown_thread.is_alive() is False
    _assert_stopped(result["snapshot"])


def _test_shutdown_during_scaling():
    pool = create_thread_pool(
        min_workers=1,
        max_workers=4,
        monitor_interval_s=0.01,
        worker_idle_timeout_s=0.01,
    )
    pool["start"]()
    stop_scaling = threading.Event()
    scale_thread = threading.Thread(
        target=partial(_scale_until_stopped, pool, stop_scaling),
        name="ThreadPoolScaleRaceTest",
        daemon=False,
    )
    scale_thread.start()
    time.sleep(0.02)
    snapshot = pool["shutdown"](join_timeout_s=2.0)
    stop_scaling.set()
    scale_thread.join(timeout=2.0)
    assert scale_thread.is_alive() is False
    _assert_stopped(snapshot)


def _test_repeated_start_stop():
    for index in range(1000):
        pool = create_thread_pool(
            name="ThreadPoolCycle{}".format(index),
            min_workers=1,
            max_workers=3,
            monitor_interval_s=0.01,
            worker_idle_timeout_s=0.01,
            join_timeout_s=0.5,
        )
        pool["start"]()
        if index % 20 == 0:
            pool["scale_to"](3)
            time.sleep(0.012)
        snapshot = pool["shutdown"](join_timeout_s=0.5)
        _assert_stopped(snapshot)

        if index % 100 == 0:
            workers_started = snapshot["statistics"]["workers_started"]
            time.sleep(0.02)
            stable = pool["get_status_snapshot"]()
            _assert_stopped(stable)
            assert stable["statistics"]["workers_started"] == workers_started


def run_test():
    _test_result_lifecycle()
    _test_shutdown_with_active_job()
    _test_shutdown_during_scaling()
    _test_repeated_start_stop()
    print("PASS generic thread pool lifecycle and 1000 shutdown cycles")


if __name__ == "__main__":
    run_test()
