# -*- coding: utf-8 -*-

"""Priority, timeout, cancellation and worker recovery regression tests."""

import os
import sys
import threading
import time


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._generic_thread_pool import create_thread_pool


def _identity(value):
    return value


def _wait_then_return(started, release, value):
    started.set()
    release.wait(timeout=3.0)
    return value


def _cooperative_job(started, cancel_event=None):
    started.set()
    while cancel_event is not None and not cancel_event.wait(0.005):
        pass
    return "cancel_seen"


def _assert_stopped(snapshot):
    assert snapshot["shutdown"] is True
    assert snapshot["shutdown_status"] == "stopped", snapshot
    assert snapshot["live_threads"] == (), snapshot
    assert snapshot["manager_alive"] is False, snapshot
    assert not any(
        worker["alive"] for worker in snapshot["workers"].values()
    ), snapshot


def _wait_for_quarantine_release(pool, timeout_s=1.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not pool["get_status_snapshot"]()["quarantined_jobs"]:
            return True
        time.sleep(0.005)
    return False


def _test_stable_priority_and_condition_manager(pools):
    pool = create_thread_pool(
        name="ThreadPoolPriorityTest",
        min_workers=0,
        max_workers=1,
    )
    pools.append(pool)
    pool["start"]()
    expected = ["high", "middle-a", "middle-b", "low"]
    pool["submit_job"]({
        "fn": _identity,
        "args": ("low",),
        "priority": -1,
    })
    pool["submit_job"]({
        "fn": _identity,
        "args": ("middle-a",),
        "priority": 5,
    })
    pool["submit_job"]({
        "fn": _identity,
        "args": ("high",),
        "priority": 10,
    })
    pool["submit_job"]({
        "fn": _identity,
        "args": ("middle-b",),
        "priority": 5,
    })
    assert pool["scale_to"](1)
    results = [
        pool["_out_q"].get(timeout=2.0)
        for unused in range(4)
    ]
    assert [item["result"] for item in results] == expected
    snapshot = pool["get_status_snapshot"]()
    assert snapshot["priority_queue"] is True
    assert snapshot["manager_polling"] is False


def _test_timeout_cancellation_and_recovery(pools, release_events):
    timeout_release = threading.Event()
    cancel_release = threading.Event()
    release_events.extend((timeout_release, cancel_release))
    pool = create_thread_pool(
        name="ThreadPoolTimeoutTest",
        min_workers=1,
        max_workers=1,
        default_job_timeout_s=0.05,
        max_detached_jobs=4,
    )
    pools.append(pool)
    pool["start"]()

    timeout_started = threading.Event()
    timeout_id = pool["submit_job"]({
        "fn": _wait_then_return,
        "args": (timeout_started, timeout_release, "late"),
    })
    assert timeout_started.wait(timeout=1.0)
    timed_out = pool["_out_q"].get(timeout=2.0)
    assert timed_out["job_id"] == timeout_id
    assert timed_out["state"] == "timed_out"
    assert timed_out["timed_out"] is True

    cancel_started = threading.Event()
    cancel_id = pool["submit_job"]({
        "fn": _wait_then_return,
        "args": (cancel_started, cancel_release, "ignored"),
        "timeout_s": 2.0,
    })
    assert cancel_started.wait(timeout=1.0)
    assert pool["cancel_job"](cancel_id, reason="uncooperative-test")
    cancelled = pool["_out_q"].get(timeout=2.0)
    assert cancelled["job_id"] == cancel_id
    assert cancelled["state"] == "cancelled"
    assert cancelled["cancel_reason"] == "uncooperative-test"

    cooperative_started = threading.Event()
    cooperative_id = pool["submit_job"]({
        "fn": _cooperative_job,
        "args": (cooperative_started,),
        "cancel_kwarg": "cancel_event",
        "timeout_s": 2.0,
    })
    assert cooperative_started.wait(timeout=1.0)
    assert pool["cancel_job"](cooperative_id, reason="cooperative-test")
    cooperative = pool["_out_q"].get(timeout=2.0)
    assert cooperative["job_id"] == cooperative_id
    assert cooperative["state"] == "cancelled"

    recovery_id = pool["submit_job"]({
        "fn": _identity,
        "args": ("recovered",),
        "timeout_s": 1.0,
    })
    recovered = pool["_out_q"].get(timeout=2.0)
    assert recovered["job_id"] == recovery_id
    assert recovered["ok"] and recovered["result"] == "recovered"

    timeout_release.set()
    cancel_release.set()
    assert _wait_for_quarantine_release(pool)
    snapshot = pool["get_status_snapshot"]()
    assert snapshot["statistics"]["jobs_timed_out"] == 1
    assert snapshot["statistics"]["jobs_cancelled"] == 2


def _test_queued_cancellation_and_timeout(pools):
    pool = create_thread_pool(
        name="ThreadPoolQueueTimeoutTest",
        min_workers=0,
        max_workers=1,
    )
    pools.append(pool)
    pool["start"]()
    cancelled_id = pool["submit_job"]({
        "fn": _identity,
        "args": ("never",),
    })
    assert pool["cancel_job"](cancelled_id, reason="queued-test")
    cancelled = pool["_out_q"].get(timeout=1.0)
    assert cancelled["job_id"] == cancelled_id
    assert cancelled["state"] == "cancelled"

    timeout_id = pool["submit_job"]({
        "fn": _identity,
        "args": ("expired",),
        "queue_timeout_s": 0.02,
    })
    time.sleep(0.04)
    assert pool["scale_to"](1)
    timed_out = pool["_out_q"].get(timeout=2.0)
    assert timed_out["job_id"] == timeout_id
    assert timed_out["state"] == "queue_timed_out"


def run_test():
    pools = []
    release_events = []
    try:
        _test_stable_priority_and_condition_manager(pools)
        _test_timeout_cancellation_and_recovery(pools, release_events)
        _test_queued_cancellation_and_timeout(pools)
    finally:
        for release_event in release_events:
            release_event.set()
        for pool in reversed(pools):
            _assert_stopped(pool["shutdown"](join_timeout_s=2.0))
    print("PASS thread pool priority, timeout, cancellation and recovery")


if __name__ == "__main__":
    run_test()
