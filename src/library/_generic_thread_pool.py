# -*- coding: utf-8 -*-

# src/library/_generic_thread_pool.py

"""Functional priority thread pool with bounded job isolation.

The manager is condition-driven. Pool workers orchestrate one daemon execution
runner per job so a timed-out or cancelled callable cannot retain a worker or
block shutdown. Python cannot safely terminate an arbitrary thread; runners
that ignore the cooperative cancel event are quarantined and bounded by
``max_detached_jobs``.

No OOP, no decorators, no lambdas, no asyncio.
"""

import copy
import logging
import threading
import time
import traceback
import uuid

from functools import partial
from queue import Empty, Full, PriorityQueue, Queue


LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "generic_thread_pool",
    "library_version": "3.0.0",
    "description": "Condition-driven functional priority and cancellation pool",
    "options": {
        "name": "GenericThreadPool",
        "min_workers": 1,
        "max_workers": 8,
        "worker_idle_timeout_s": 0.2,
        "manager_interval_s": 0.25,
        "join_timeout_s": 5.0,
        "recent_result_limit": 64,
        "retained_job_limit": 256,
        "default_job_timeout_s": 30.0,
        "max_detached_jobs": 32,
        "result_put_timeout_s": 1.0,
        "queue_maxsize": 0,
    },
}

EV_SHUTDOWN = "THREADPOOL_SHUTDOWN"
EV_SCALE_TO = "THREADPOOL_SCALE_TO"
EV_LIMITS = "THREADPOOL_LIMITS"
EV_CFG_REPLACE = "THREADPOOL_CONFIG_REPLACE"
EV_CFG_PATCH = "THREADPOOL_CONFIG_PATCH"
EV_STATUS_REQ = "THREADPOOL_STATUS_REQUEST"
EV_ENQUEUE_JOB = "THREADPOOL_ENQUEUE_JOB"
EV_CANCEL_JOB = "THREADPOOL_CANCEL_JOB"

STOP_JOB = {
    "_type": "__THREADPOOL_STOP__",
    "_sentinel": True,
}

_STOP_PRIORITY = 1000000000.0


def _is_dict(value):
    return isinstance(value, dict)


def _safe_int(value, default, minimum=None, maximum=None):
    try:
        result = int(value)
    except Exception:
        result = int(default)
    if minimum is not None and result < minimum:
        return int(default)
    if maximum is not None and result > maximum:
        return int(default)
    return result


def _safe_float(value, default, minimum=None, maximum=None):
    try:
        result = float(value)
    except Exception:
        result = float(default)
    if minimum is not None and result < minimum:
        return float(default)
    if maximum is not None and result > maximum:
        return float(default)
    return result


def _timeout_value(value, default):
    selected = default if value is None else value
    if selected is None:
        return None
    try:
        result = float(selected)
    except Exception:
        result = float(default) if default is not None else None
    if result is None or result <= 0.0:
        return None
    return result


def _priority_value(value):
    return _safe_float(value, 0.0, -1000000.0, 1000000.0)


def _now():
    return time.time()


def _monotonic():
    return time.monotonic()


def _new_id():
    return str(uuid.uuid4())


def _safe_copy(value, fallback=None):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value if fallback is None else fallback


def _copy_mapping(value):
    if not _is_dict(value):
        return {}
    copied = _safe_copy(value, {})
    return copied if _is_dict(copied) else {}


def _queue_put(queue_object, item, **kwargs):
    if queue_object is None:
        return False
    block = bool(kwargs.get("block", True))
    timeout = kwargs.get("timeout")
    try:
        if timeout is None:
            queue_object.put(item, block=block)
        else:
            queue_object.put(item, block=block, timeout=float(timeout))
        return True
    except Full:
        return False
    except Exception as exc:
        LOGGER.warning("ThreadPool queue put failed: %s", exc)
        return False


def _task_done(queue_object):
    task_done = getattr(queue_object, "task_done", None)
    if not callable(task_done):
        return
    try:
        task_done()
    except (ValueError, AttributeError):
        return


def _is_stop_job(job):
    return (
        _is_dict(job)
        and job.get("_type") == "__THREADPOOL_STOP__"
        and job.get("_sentinel") is True
    )


def _event_type(event):
    if not _is_dict(event):
        return ""
    return str(event.get("event_type") or event.get("type") or "").upper()


def _bounded_append(items, item, limit):
    items.append(item)
    overflow = len(items) - int(limit)
    if overflow > 0:
        del items[:overflow]


def _thread_alive(thread):
    if thread is None:
        return False
    try:
        return bool(thread.is_alive())
    except Exception:
        return False


def _worker_alive(worker):
    return _thread_alive(worker.get("thread") if _is_dict(worker) else None)


def _is_event(value):
    return callable(getattr(value, "is_set", None)) and callable(
        getattr(value, "set", None)
    )


def _resolve_options(kwargs):
    defaults = CONTEXT["options"]
    minimum = _safe_int(kwargs.get("min_workers"), defaults["min_workers"], 0)
    maximum = _safe_int(kwargs.get("max_workers"), defaults["max_workers"], 1)
    if maximum < minimum:
        maximum = minimum
    recent_limit = _safe_int(
        kwargs.get("recent_result_limit"), defaults["recent_result_limit"], 1
    )
    retained_default = max(defaults["retained_job_limit"], recent_limit * 4)
    return {
        "name": str(kwargs.get("name") or defaults["name"]),
        "min_workers": minimum,
        "max_workers": maximum,
        "worker_idle_timeout_s": _safe_float(
            kwargs.get("worker_idle_timeout_s"),
            defaults["worker_idle_timeout_s"],
            0.01,
        ),
        "manager_interval_s": _safe_float(
            kwargs.get("monitor_interval_s"),
            defaults["manager_interval_s"],
            0.01,
        ),
        "join_timeout_s": _safe_float(
            kwargs.get("join_timeout_s"), defaults["join_timeout_s"], 0.1
        ),
        "recent_result_limit": recent_limit,
        "retained_job_limit": _safe_int(
            kwargs.get("retained_job_limit"), retained_default, recent_limit
        ),
        "default_job_timeout_s": _timeout_value(
            kwargs.get("default_job_timeout_s"),
            defaults["default_job_timeout_s"],
        ),
        "max_detached_jobs": _safe_int(
            kwargs.get("max_detached_jobs"),
            defaults["max_detached_jobs"],
            1,
        ),
        "result_put_timeout_s": _safe_float(
            kwargs.get("result_put_timeout_s"),
            defaults["result_put_timeout_s"],
            0.01,
        ),
        "queue_maxsize": _safe_int(
            kwargs.get("queue_maxsize"), defaults["queue_maxsize"], 0
        ),
    }


def create_thread_pool(**kwargs):
    options = _resolve_options(kwargs)
    lock = threading.RLock()
    supplied_queue = kwargs.get("thread_pool_in")
    job_queue = supplied_queue or PriorityQueue(
        maxsize=options["queue_maxsize"]
    )
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "options": options,
        "lock": lock,
        "condition": threading.Condition(lock),
        "shutdown_event": threading.Event(),
        "shutdown_status": "created",
        "shutdown_live_threads": (),
        "started": False,
        "manager_thread": None,
        "workers": {},
        "worker_sequence": 0,
        "desired_workers": options["min_workers"],
        "queue_sequence": 0,
        "job_queue": job_queue,
        "priority_queue": isinstance(job_queue, PriorityQueue),
        "result_queue": kwargs.get("thread_pool_out") or Queue(),
        "control_queue": kwargs.get("queue_event_threadpool") or Queue(),
        "config_cell": {
            "revision": 0,
            "snapshot": _safe_copy(kwargs.get("json_config") or {}, {}),
        },
        "job_registry": {},
        "completed_job_order": [],
        "quarantined_runners": {},
        "recent_results": [],
        "statistics": {
            "jobs_submitted": 0,
            "jobs_started": 0,
            "jobs_ok": 0,
            "jobs_error": 0,
            "jobs_cancelled": 0,
            "jobs_timed_out": 0,
            "jobs_rejected": 0,
            "results_dropped": 0,
            "workers_started": 0,
            "workers_stopped": 0,
            "control_events": 0,
            "condition_wakeups": 0,
            "detached_jobs_total": 0,
            "detached_jobs_completed": 0,
        },
    }
    return _build_api(context)


def _get_context(api_or_context):
    if _is_dict(api_or_context) and _is_dict(api_or_context.get("context")):
        return api_or_context["context"]
    return api_or_context


def get_config_snapshot(api_or_context):
    context = _get_context(api_or_context)
    with context["lock"]:
        return {
            "revision": context["config_cell"]["revision"],
            "snapshot": _safe_copy(context["config_cell"]["snapshot"], {}),
        }


def replace_thread_pool_config(api_or_context, config):
    context = _get_context(api_or_context)
    with context["condition"]:
        context["config_cell"]["revision"] += 1
        context["config_cell"]["snapshot"] = _safe_copy(config or {}, {})
        context["condition"].notify_all()
        return context["config_cell"]["revision"]


def patch_thread_pool_config(api_or_context, patch_ops, **kwargs):
    context = _get_context(api_or_context)
    with context["condition"]:
        working = _safe_copy(context["config_cell"]["snapshot"], {})
        for operation in patch_ops or []:
            if not _is_dict(operation):
                continue
            key = operation.get("key") or operation.get("path")
            if key is None:
                continue
            if operation.get("op") == "delete":
                working.pop(str(key), None)
            else:
                working[str(key)] = _safe_copy(operation.get("value"))
        context["config_cell"]["revision"] += 1
        context["config_cell"]["snapshot"] = working
        context["condition"].notify_all()
        return context["config_cell"]["revision"]


def _normalize_job(context, job):
    if not _is_dict(job):
        return None
    normalized = dict(job)
    normalized["job_id"] = str(normalized.get("job_id") or _new_id())
    normalized["args"] = tuple(normalized.get("args") or ())
    normalized["kwargs"] = dict(normalized.get("kwargs") or {})
    normalized["meta"] = _copy_mapping(normalized.get("meta"))
    normalized["priority"] = _priority_value(normalized.get("priority", 0.0))
    normalized["timeout_s"] = _timeout_value(
        normalized.get("timeout_s"),
        context["options"]["default_job_timeout_s"],
    )
    normalized["queue_timeout_s"] = _timeout_value(
        normalized.get("queue_timeout_s"), None
    )
    cancel_kwarg = str(normalized.get("cancel_kwarg") or "").strip()
    normalized["cancel_kwarg"] = cancel_kwarg
    cancel_event = normalized.get("cancel_event")
    if not _is_event(cancel_event) and cancel_kwarg:
        candidate = normalized["kwargs"].get(cancel_kwarg)
        cancel_event = candidate if _is_event(candidate) else None
    normalized["cancel_event"] = cancel_event or threading.Event()
    normalized["submitted_at"] = _now()
    normalized["submitted_monotonic"] = _monotonic()
    return normalized


def _queue_item_locked(context, job, priority=None):
    context["queue_sequence"] += 1
    sequence = context["queue_sequence"]
    selected_priority = (
        _priority_value(priority)
        if priority is not None
        else _priority_value(job.get("priority", 0.0))
    )
    if context["priority_queue"]:
        return (-selected_priority, sequence, job)
    return job


def _unwrap_queue_item(item):
    if (
        isinstance(item, tuple)
        and len(item) == 3
        and _is_dict(item[2])
    ):
        return item[2]
    return item


def _put_job_queue(context, job, **kwargs):
    with context["condition"]:
        item = _queue_item_locked(context, job)
    return _queue_put(
        context["job_queue"],
        item,
        block=kwargs.get("block", True),
        timeout=kwargs.get("timeout"),
    )


def _put_stop_jobs(context, count):
    written = 0
    for unused in range(max(0, int(count))):
        with context["condition"]:
            item = _queue_item_locked(
                context,
                STOP_JOB,
                priority=_STOP_PRIORITY,
            )
        if _queue_put(context["job_queue"], item, block=False):
            written += 1
    return written


def _make_result(job, worker_id, **kwargs):
    state = str(kwargs.get("state") or "failed")
    started_at = kwargs.get("started_at")
    finished_at = kwargs.get("finished_at") or _now()
    duration = 0.0
    if started_at is not None:
        duration = max(0.0, float(finished_at) - float(started_at))
    return {
        "kind": "result",
        "job_id": job["job_id"],
        "worker_id": worker_id,
        "ok": bool(kwargs.get("ok", False)),
        "result": kwargs.get("result"),
        "error": kwargs.get("error"),
        "traceback": kwargs.get("traceback"),
        "meta": _safe_copy(job.get("meta") or {}, {}),
        "config_revision": kwargs.get("config_revision"),
        "priority": job.get("priority", 0.0),
        "state": state,
        "cancelled": state == "cancelled",
        "timed_out": state in ("timed_out", "queue_timed_out"),
        "cancel_reason": kwargs.get("cancel_reason"),
        "submitted_at": job.get("submitted_at"),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_s": duration,
        "queue_wait_s": max(
            0.0,
            float(started_at or finished_at) - float(job.get("submitted_at") or finished_at),
        ),
    }


def _public_job_state(entry):
    job = entry.get("job") or {}
    return {
        "job_id": entry.get("job_id"),
        "state": entry.get("state"),
        "terminal": bool(entry.get("terminal")),
        "priority": entry.get("priority", job.get("priority", 0.0)),
        "worker_id": entry.get("worker_id"),
        "cancel_requested": bool(entry.get("cancel_requested")),
        "cancel_reason": entry.get("cancel_reason"),
        "submitted_at": entry.get("submitted_at"),
        "started_at": entry.get("started_at"),
        "finished_at": entry.get("finished_at"),
        "timeout_s": entry.get("timeout_s", job.get("timeout_s")),
        "queue_timeout_s": entry.get(
            "queue_timeout_s", job.get("queue_timeout_s")
        ),
    }


def _prune_job_registry_locked(context):
    limit = context["options"]["retained_job_limit"]
    order = context["completed_job_order"]
    while len(order) > limit:
        job_id = order.pop(0)
        if job_id in context["quarantined_runners"]:
            order.append(job_id)
            if all(
                selected in context["quarantined_runners"]
                for selected in order
            ):
                break
            continue
        entry = context["job_registry"].get(job_id)
        if entry is not None and entry.get("terminal"):
            context["job_registry"].pop(job_id, None)


def _finalize_job_locked(context, entry, result):
    if entry.get("terminal"):
        return None
    entry["terminal"] = True
    entry["state"] = result["state"]
    entry["finished_at"] = result["finished_at"]
    entry["cancel_reason"] = result.get("cancel_reason")
    entry["result_summary"] = {
        "ok": result["ok"],
        "error": result.get("error"),
        "state": result["state"],
    }
    entry["job"] = None
    state = result["state"]
    if result["ok"]:
        context["statistics"]["jobs_ok"] += 1
    elif state == "cancelled":
        context["statistics"]["jobs_cancelled"] += 1
    elif state in ("timed_out", "queue_timed_out"):
        context["statistics"]["jobs_timed_out"] += 1
    else:
        context["statistics"]["jobs_error"] += 1
    _bounded_append(
        context["recent_results"],
        _safe_copy(result, result),
        context["options"]["recent_result_limit"],
    )
    context["completed_job_order"].append(entry["job_id"])
    _prune_job_registry_locked(context)
    return result


def _publish_result(context, result):
    if result is None:
        return False
    written = _queue_put(
        context["result_queue"],
        result,
        block=True,
        timeout=context["options"]["result_put_timeout_s"],
    )
    if not written:
        with context["condition"]:
            context["statistics"]["results_dropped"] += 1
    return written


def _runner_loop(context, execution):
    job = execution["job"]
    function = job.get("fn")
    result_value = None
    error = None
    trace = None
    ok = False
    try:
        if not callable(function):
            raise TypeError("thread-pool job fn must be callable")
        call_kwargs = dict(job["kwargs"])
        cancel_kwarg = job.get("cancel_kwarg")
        if cancel_kwarg:
            call_kwargs[cancel_kwarg] = job["cancel_event"]
        if str(job.get("call_style") or "args") == "config":
            result_value = function(
                execution["config_snapshot"],
                *job["args"],
                **call_kwargs
            )
        else:
            result_value = function(*job["args"], **call_kwargs)
        ok = True
    except BaseException as exc:
        error = str(exc)
        trace = traceback.format_exc()
    finally:
        with context["condition"]:
            execution["done"] = True
            execution["ok"] = ok
            execution["result"] = result_value
            execution["error"] = error
            execution["traceback"] = trace
            execution["finished_at"] = _now()
            if execution.get("detached"):
                removed = context["quarantined_runners"].pop(
                    job["job_id"], None
                )
                if removed is not None:
                    context["statistics"]["detached_jobs_completed"] += 1
            context["condition"].notify_all()


def _start_execution_locked(context, worker, entry, job):
    if len(context["quarantined_runners"]) >= context["options"]["max_detached_jobs"]:
        return None
    config = {
        "revision": context["config_cell"]["revision"],
        "snapshot": _safe_copy(context["config_cell"]["snapshot"], {}),
    }
    started_at = _now()
    execution = {
        "job": job,
        "config_revision": config["revision"],
        "config_snapshot": config["snapshot"],
        "started_at": started_at,
        "started_monotonic": _monotonic(),
        "done": False,
        "detached": False,
        "ok": False,
        "result": None,
        "error": None,
        "traceback": None,
        "finished_at": None,
        "thread": None,
    }
    runner = threading.Thread(
        target=partial(_runner_loop, context, execution),
        name="{}-job-{}".format(
            context["options"]["name"], job["job_id"][:8]
        ),
        daemon=True,
    )
    execution["thread"] = runner
    entry["state"] = "running"
    entry["worker_id"] = worker["worker_id"]
    entry["started_at"] = started_at
    worker["busy"] = True
    worker["current_job_id"] = job["job_id"]
    worker["current_priority"] = job["priority"]
    context["statistics"]["jobs_started"] += 1
    runner.start()
    return execution


def _detach_execution_locked(context, execution):
    job = execution["job"]
    job["cancel_event"].set()
    if execution["done"] or not _thread_alive(execution.get("thread")):
        return False
    execution["detached"] = True
    context["quarantined_runners"][job["job_id"]] = execution
    context["statistics"]["detached_jobs_total"] += 1
    return True


def _wait_for_execution(context, worker, entry, execution):
    job = execution["job"]
    timeout_s = job.get("timeout_s")
    deadline = (
        execution["started_monotonic"] + float(timeout_s)
        if timeout_s is not None
        else None
    )
    terminal_state = ""
    cancel_reason = None
    with context["condition"]:
        while not execution["done"]:
            if context["shutdown_event"].is_set():
                terminal_state = "cancelled"
                cancel_reason = "shutdown"
                break
            if entry.get("cancel_requested") or job["cancel_event"].is_set():
                terminal_state = "cancelled"
                cancel_reason = entry.get("cancel_reason") or "cancelled"
                break
            remaining = None if deadline is None else deadline - _monotonic()
            if remaining is not None and remaining <= 0.0:
                terminal_state = "timed_out"
                cancel_reason = "job_timeout"
                break
            context["condition"].wait(timeout=remaining)
            context["statistics"]["condition_wakeups"] += 1

        if execution["done"] and entry.get("cancel_requested"):
            cancel_reason = entry.get("cancel_reason") or "cancelled"
            return _make_result(
                job,
                worker["worker_id"],
                ok=False,
                error="job_cancelled: {}".format(cancel_reason),
                config_revision=execution["config_revision"],
                state="cancelled",
                cancel_reason=cancel_reason,
                started_at=execution["started_at"],
                finished_at=execution["finished_at"],
            )

        if execution["done"]:
            return _make_result(
                job,
                worker["worker_id"],
                ok=execution["ok"],
                result=execution["result"],
                error=execution["error"],
                traceback=execution["traceback"],
                config_revision=execution["config_revision"],
                state="succeeded" if execution["ok"] else "failed",
                started_at=execution["started_at"],
                finished_at=execution["finished_at"],
            )

        _detach_execution_locked(context, execution)
        if terminal_state == "timed_out":
            error = "job_timeout_{}s".format(timeout_s)
        else:
            error = "job_cancelled: {}".format(cancel_reason)
        return _make_result(
            job,
            worker["worker_id"],
            ok=False,
            error=error,
            config_revision=execution["config_revision"],
            state=terminal_state,
            cancel_reason=cancel_reason,
            started_at=execution["started_at"],
        )


def _reset_worker_locked(worker, result):
    worker["busy"] = False
    worker["current_job_id"] = None
    worker["current_priority"] = None
    worker["jobs_total"] += 1
    worker["last_finished_at"] = result["finished_at"]


def _worker_loop(context, worker):
    try:
        while not context["shutdown_event"].is_set():
            if worker["stop_event"].is_set():
                break
            try:
                item = context["job_queue"].get(
                    timeout=context["options"]["worker_idle_timeout_s"]
                )
            except Empty:
                continue
            result = None
            try:
                job = _unwrap_queue_item(item)
                if _is_stop_job(job):
                    break
                if not _is_dict(job):
                    continue

                with context["condition"]:
                    entry = context["job_registry"].get(job.get("job_id"))
                    if entry is None or entry.get("terminal"):
                        continue
                    if context["shutdown_event"].is_set():
                        result = _make_result(
                            job,
                            worker["worker_id"],
                            state="cancelled",
                            error="job_cancelled: shutdown",
                            cancel_reason="shutdown",
                        )
                        result = _finalize_job_locked(context, entry, result)
                        execution = None
                    else:
                        queue_timeout = job.get("queue_timeout_s")
                        queue_age = _monotonic() - job["submitted_monotonic"]
                        if queue_timeout is not None and queue_age >= queue_timeout:
                            result = _make_result(
                                job,
                                worker["worker_id"],
                                state="queue_timed_out",
                                error="queue_timeout_{}s".format(queue_timeout),
                                cancel_reason="queue_timeout",
                            )
                            result = _finalize_job_locked(context, entry, result)
                            execution = None
                        else:
                            execution = _start_execution_locked(
                                context, worker, entry, job
                            )
                            if execution is None:
                                result = _make_result(
                                    job,
                                    worker["worker_id"],
                                    state="failed",
                                    error="detached_job_capacity_exhausted",
                                )
                                result = _finalize_job_locked(
                                    context, entry, result
                                )

                if execution is not None:
                    result = _wait_for_execution(
                        context, worker, entry, execution
                    )
                    with context["condition"]:
                        _reset_worker_locked(worker, result)
                        result = _finalize_job_locked(context, entry, result)
                        context["condition"].notify_all()
                _publish_result(context, result)
            finally:
                _task_done(context["job_queue"])
    finally:
        with context["condition"]:
            worker["state"] = "stopped"
            worker["busy"] = False
            worker["current_job_id"] = None
            worker["current_priority"] = None
            context["statistics"]["workers_stopped"] += 1
            context["condition"].notify_all()


def _new_worker(context):
    with context["condition"]:
        if context["shutdown_event"].is_set():
            return None
        context["worker_sequence"] += 1
        worker_id = context["worker_sequence"]
        worker = {
            "worker_id": worker_id,
            "state": "starting",
            "busy": False,
            "current_job_id": None,
            "current_priority": None,
            "jobs_total": 0,
            "last_finished_at": None,
            "stop_event": threading.Event(),
            "thread": None,
        }
        thread = threading.Thread(
            target=partial(_worker_loop, context, worker),
            name="{}-worker-{}".format(context["options"]["name"], worker_id),
            daemon=False,
        )
        worker["thread"] = thread
        worker["state"] = "running"
        context["workers"][worker_id] = worker
        thread.start()
        context["statistics"]["workers_started"] += 1
        context["condition"].notify_all()
        return worker


def _alive_workers(context):
    with context["lock"]:
        return [
            worker
            for worker in context["workers"].values()
            if _worker_alive(worker) and worker.get("state") != "stopped"
        ]


def _ensure_desired_workers(context):
    if context["shutdown_event"].is_set():
        return 0
    while True:
        alive = _alive_workers(context)
        with context["lock"]:
            desired = min(
                context["options"]["max_workers"],
                max(context["options"]["min_workers"], context["desired_workers"]),
            )
        if len(alive) >= desired:
            return len(alive)
        if _new_worker(context) is None:
            return len(alive)


def _scale_workers(context, target):
    if context["shutdown_event"].is_set():
        return len(_alive_workers(context))
    wanted = _safe_int(target, context["options"]["min_workers"], 0)
    wanted = max(context["options"]["min_workers"], wanted)
    wanted = min(context["options"]["max_workers"], wanted)
    with context["condition"]:
        context["desired_workers"] = wanted
        alive = [
            worker
            for worker in context["workers"].values()
            if _worker_alive(worker) and worker.get("state") != "stopped"
        ]
        alive = sorted(alive, key=_worker_id_sort)
    return wanted


def _worker_id_sort(worker):
    return int(worker.get("worker_id", 0))


def _apply_worker_target(context):
    with context["condition"]:
        wanted = context["desired_workers"]
        alive = [
            worker
            for worker in context["workers"].values()
            if _worker_alive(worker) and worker.get("state") != "stopped"
        ]
        alive = sorted(alive, key=_worker_id_sort)
        for worker in alive:
            worker["stop_event"].clear()
            if worker["state"] == "stopping":
                worker["state"] = "running"
        for worker in reversed(alive[wanted:]):
            worker["state"] = "stopping"
            worker["stop_event"].set()
        context["condition"].notify_all()
    if len(alive) < wanted:
        _ensure_desired_workers(context)
    return wanted


def _handle_control_event(context, event):
    event_name = _event_type(event)
    if context["shutdown_event"].is_set() and event_name != EV_SHUTDOWN:
        return
    payload = event.get("payload") if _is_dict(event) else {}
    payload = payload if _is_dict(payload) else {}
    with context["condition"]:
        context["statistics"]["control_events"] += 1
    handlers = {
        EV_SHUTDOWN: _control_shutdown,
        EV_SCALE_TO: _control_scale,
        EV_LIMITS: _control_limits,
        EV_CFG_REPLACE: _control_config_replace,
        EV_CFG_PATCH: _control_config_patch,
        EV_STATUS_REQ: _control_status,
        EV_ENQUEUE_JOB: _control_enqueue,
        EV_CANCEL_JOB: _control_cancel,
    }
    handler = handlers.get(event_name)
    if callable(handler):
        handler(context, payload)


def _control_shutdown(context, payload):
    with context["condition"]:
        context["shutdown_status"] = "stopping"
        context["shutdown_event"].set()
        results = _cancel_for_shutdown_locked(context)
        workers = [
            worker
            for worker in context["workers"].values()
            if _worker_alive(worker)
        ]
        for worker in workers:
            worker["stop_event"].set()
        context["condition"].notify_all()
    for result in results:
        _publish_result(context, result)
    _drain_job_queue(context)
    _put_stop_jobs(context, len(workers))

def _control_scale(context, payload):
    _scale_workers(context, payload.get("workers"))
    _apply_worker_target(context)


def _control_limits(context, payload):
    _update_limits(context, payload)


def _control_config_replace(context, payload):
    replace_thread_pool_config(context, payload.get("json_config"))


def _control_config_patch(context, payload):
    patch_thread_pool_config(context, payload.get("patch") or [])


def _control_status(context, payload):
    status_event = {
        "kind": "event",
        "event_type": "THREADPOOL_STATUS",
        "payload": get_thread_pool_snapshot(context),
    }
    reply_to = payload.get("reply_to")
    _queue_put(reply_to or context["result_queue"], status_event)


def _control_enqueue(context, payload):
    submit_job(context, payload.get("job"))


def _control_cancel(context, payload):
    cancel_job(
        context,
        payload.get("job_id"),
        reason=payload.get("reason") or "control_event",
    )


def _update_limits(context, payload):
    if context["shutdown_event"].is_set():
        return
    with context["condition"]:
        minimum = _safe_int(
            payload.get("min_workers"), context["options"]["min_workers"], 0
        )
        maximum = _safe_int(
            payload.get("max_workers"), context["options"]["max_workers"], 1
        )
        if maximum < minimum:
            maximum = minimum
        context["options"]["min_workers"] = minimum
        context["options"]["max_workers"] = maximum
        context["desired_workers"] = min(
            maximum, max(minimum, context["desired_workers"])
        )
        context["condition"].notify_all()
    _apply_worker_target(context)


def _manager_wait_needed_locked(context):
    if not context["control_queue"].empty():
        return False
    alive = sum(
        1
        for worker in context["workers"].values()
        if _worker_alive(worker)
        and worker.get("state") not in ("stopped", "stopping")
        and not worker["stop_event"].is_set()
    )
    return alive >= context["desired_workers"]


def _manager_loop(context):
    _ensure_desired_workers(context)
    while not context["shutdown_event"].is_set():
        with context["condition"]:
            while (
                not context["shutdown_event"].is_set()
                and _manager_wait_needed_locked(context)
            ):
                context["condition"].wait()
                context["statistics"]["condition_wakeups"] += 1
            if context["shutdown_event"].is_set():
                break

        handled = False
        while True:
            try:
                event = context["control_queue"].get_nowait()
            except Empty:
                break
            try:
                _handle_control_event(context, event)
                handled = True
            finally:
                _task_done(context["control_queue"])
            if context["shutdown_event"].is_set():
                break
        if not handled and not context["shutdown_event"].is_set():
            _ensure_desired_workers(context)
        elif not context["shutdown_event"].is_set():
            _ensure_desired_workers(context)


def start_thread_pool(api_or_context):
    context = _get_context(api_or_context)
    with context["condition"]:
        if context["shutdown_event"].is_set():
            return None
        if context["started"]:
            return context["manager_thread"]
        context["started"] = True
        context["shutdown_status"] = "running"
        context["shutdown_live_threads"] = ()
        thread = threading.Thread(
            target=partial(_manager_loop, context),
            name=context["options"]["name"] + "-manager",
            daemon=False,
        )
        context["manager_thread"] = thread
        thread.start()
        context["condition"].notify_all()
        return thread


def submit_job(api_or_context, job, **kwargs):
    context = _get_context(api_or_context)
    if context["shutdown_event"].is_set():
        return ""
    normalized = _normalize_job(context, job)
    if normalized is None:
        return ""
    job_id = normalized["job_id"]
    entry = {
        "job_id": job_id,
        "job": normalized,
        "state": "queued",
        "terminal": False,
        "priority": normalized["priority"],
        "worker_id": None,
        "cancel_requested": False,
        "cancel_reason": None,
        "submitted_at": normalized["submitted_at"],
        "started_at": None,
        "finished_at": None,
        "timeout_s": normalized["timeout_s"],
        "queue_timeout_s": normalized["queue_timeout_s"],
    }
    with context["condition"]:
        if context["shutdown_event"].is_set() or job_id in context["job_registry"]:
            context["statistics"]["jobs_rejected"] += 1
            return ""
        context["job_registry"][job_id] = entry

    written = _put_job_queue(
        context,
        normalized,
        block=kwargs.get("block", True),
        timeout=kwargs.get("timeout"),
    )
    if not written:
        with context["condition"]:
            current = context["job_registry"].get(job_id)
            if current is entry and not entry.get("terminal"):
                context["job_registry"].pop(job_id, None)
            context["statistics"]["jobs_rejected"] += 1
        return ""

    with context["condition"]:
        context["statistics"]["jobs_submitted"] += 1
        context["condition"].notify_all()
    return job_id


def submit(api_or_context, function, *args, **kwargs):
    call_style = kwargs.pop("call_style", "args")
    meta = kwargs.pop("meta", None)
    job = {
        "job_id": _new_id(),
        "fn": function,
        "args": args,
        "kwargs": kwargs,
        "call_style": call_style,
        "meta": meta if _is_dict(meta) else {},
        "priority": kwargs.pop("pool_priority", 0.0),
        "timeout_s": kwargs.pop("pool_timeout_s", None),
        "queue_timeout_s": kwargs.pop("pool_queue_timeout_s", None),
        "cancel_kwarg": kwargs.pop("pool_cancel_kwarg", ""),
    }
    return submit_job(api_or_context, job)


def cancel_job(api_or_context, job_id, **kwargs):
    context = _get_context(api_or_context)
    selected_id = str(job_id or "")
    if not selected_id:
        return False
    reason = str(kwargs.get("reason") or "cancelled")
    result = None
    with context["condition"]:
        entry = context["job_registry"].get(selected_id)
        if entry is None or entry.get("terminal"):
            return False
        entry["cancel_requested"] = True
        entry["cancel_reason"] = reason
        job = entry.get("job")
        if _is_dict(job):
            job["cancel_event"].set()
        if entry.get("state") == "queued" and _is_dict(job):
            result = _make_result(
                job,
                None,
                state="cancelled",
                error="job_cancelled: {}".format(reason),
                cancel_reason=reason,
            )
            result = _finalize_job_locked(context, entry, result)
        context["condition"].notify_all()
    _publish_result(context, result)
    return True


def get_job_status(api_or_context, job_id):
    context = _get_context(api_or_context)
    with context["lock"]:
        entry = context["job_registry"].get(str(job_id or ""))
        return _public_job_state(entry) if entry is not None else None


def send_event(api_or_context, event_type, payload=None, **kwargs):
    context = _get_context(api_or_context)
    if context["shutdown_event"].is_set():
        return False
    event = {
        "event_type": str(event_type),
        "payload": payload if _is_dict(payload) else {},
        "timestamp": _now(),
    }
    written = _queue_put(
        context["control_queue"],
        event,
        block=kwargs.get("block", True),
        timeout=kwargs.get("timeout"),
    )
    if written:
        with context["condition"]:
            context["condition"].notify_all()
    return written


def scale_thread_pool(api_or_context, workers):
    return send_event(api_or_context, EV_SCALE_TO, {"workers": workers})


def set_thread_pool_limits(api_or_context, min_workers=None, max_workers=None):
    payload = {}
    if min_workers is not None:
        payload["min_workers"] = min_workers
    if max_workers is not None:
        payload["max_workers"] = max_workers
    return send_event(api_or_context, EV_LIMITS, payload)


def request_thread_pool_status(api_or_context, reply_to=None):
    payload = {}
    if reply_to is not None:
        payload["reply_to"] = reply_to
    return send_event(api_or_context, EV_STATUS_REQ, payload)


def _cancel_for_shutdown_locked(context):
    results = []
    for entry in list(context["job_registry"].values()):
        if entry.get("terminal"):
            continue
        entry["cancel_requested"] = True
        entry["cancel_reason"] = "shutdown"
        job = entry.get("job")
        if _is_dict(job):
            job["cancel_event"].set()
        if entry.get("state") == "queued" and _is_dict(job):
            result = _make_result(
                job,
                None,
                state="cancelled",
                error="job_cancelled: shutdown",
                cancel_reason="shutdown",
            )
            result = _finalize_job_locked(context, entry, result)
            if result is not None:
                results.append(result)
    context["condition"].notify_all()
    return results


def _drain_job_queue(context):
    drained = 0
    while True:
        try:
            context["job_queue"].get_nowait()
        except Empty:
            break
        else:
            drained += 1
            _task_done(context["job_queue"])
    return drained


def _join_thread_until(thread, deadline):
    if thread is None or thread is threading.current_thread():
        return
    remaining = max(0.0, deadline - _monotonic())
    if remaining > 0.0:
        thread.join(timeout=remaining)


def shutdown_thread_pool(api_or_context, **kwargs):
    context = _get_context(api_or_context)
    timeout = _safe_float(
        kwargs.get("join_timeout_s"),
        context["options"]["join_timeout_s"],
        0.1,
    )
    with context["condition"]:
        context["shutdown_status"] = "stopping"
        context["shutdown_event"].set()
        queued_results = _cancel_for_shutdown_locked(context)
        workers = [
            worker
            for worker in context["workers"].values()
            if _worker_alive(worker)
        ]
        for worker in workers:
            worker["stop_event"].set()
        context["condition"].notify_all()

    for result in queued_results:
        _publish_result(context, result)
    _drain_job_queue(context)
    _put_stop_jobs(context, len(workers))

    deadline = _monotonic() + timeout
    manager = context.get("manager_thread")
    _join_thread_until(manager, deadline)
    for worker in workers:
        _join_thread_until(worker.get("thread"), deadline)

    remaining_workers = _alive_workers(context)
    if remaining_workers:
        _put_stop_jobs(context, len(remaining_workers))
        for worker in remaining_workers:
            _join_thread_until(worker.get("thread"), deadline)

    live_threads = []
    if _thread_alive(manager):
        live_threads.append(manager.name)
    for worker in _alive_workers(context):
        thread = worker.get("thread")
        if _thread_alive(thread):
            live_threads.append(thread.name)

    with context["condition"]:
        context["shutdown_live_threads"] = tuple(sorted(set(live_threads)))
        context["shutdown_status"] = "stuck" if live_threads else "stopped"
        context["condition"].notify_all()
    return get_thread_pool_snapshot(context)


def _job_state_counts_locked(context):
    counts = {}
    for entry in context["job_registry"].values():
        state = str(entry.get("state") or "unknown")
        counts[state] = int(counts.get(state, 0)) + 1
    return counts


def get_thread_pool_snapshot(api_or_context):
    context = _get_context(api_or_context)
    with context["lock"]:
        workers = {}
        for worker_id in sorted(context["workers"]):
            worker = context["workers"][worker_id]
            workers[str(worker_id)] = {
                "state": worker["state"],
                "alive": _worker_alive(worker),
                "busy": worker["busy"],
                "current_job_id": worker["current_job_id"],
                "current_priority": worker["current_priority"],
                "jobs_total": worker["jobs_total"],
                "last_finished_at": worker["last_finished_at"],
            }
        quarantined = []
        for job_id in sorted(context["quarantined_runners"]):
            execution = context["quarantined_runners"][job_id]
            thread = execution.get("thread")
            if _thread_alive(thread):
                quarantined.append({
                    "job_id": job_id,
                    "thread_name": thread.name,
                    "started_at": execution.get("started_at"),
                })
        return {
            "module": context["module"],
            "version": context["version"],
            "name": context["options"]["name"],
            "started": context["started"],
            "shutdown": context["shutdown_event"].is_set(),
            "shutdown_status": context["shutdown_status"],
            "live_threads": tuple(context["shutdown_live_threads"]),
            "manager_alive": _thread_alive(context.get("manager_thread")),
            "manager_polling": False,
            "desired_workers": context["desired_workers"],
            "workers": workers,
            "statistics": dict(context["statistics"]),
            "config_revision": context["config_cell"]["revision"],
            "queue_depth": context["job_queue"].qsize(),
            "priority_queue": context["priority_queue"],
            "default_job_timeout_s": context["options"]["default_job_timeout_s"],
            "max_detached_jobs": context["options"]["max_detached_jobs"],
            "quarantined_jobs": quarantined,
            "job_states": _job_state_counts_locked(context),
            "recent_results": _safe_copy(context["recent_results"], []),
        }


def _build_api(context):
    return {
        "context": context,
        "start": partial(start_thread_pool, context),
        "shutdown": partial(shutdown_thread_pool, context),
        "submit_job": partial(submit_job, context),
        "submit": partial(submit, context),
        "cancel_job": partial(cancel_job, context),
        "get_job_status": partial(get_job_status, context),
        "send_event": partial(send_event, context),
        "scale_to": partial(scale_thread_pool, context),
        "set_limits": partial(set_thread_pool_limits, context),
        "config_replace": partial(replace_thread_pool_config, context),
        "config_patch": partial(patch_thread_pool_config, context),
        "request_status": partial(request_thread_pool_status, context),
        "get_status_snapshot": partial(get_thread_pool_snapshot, context),
        "get_config_snapshot": partial(get_config_snapshot, context),
        "_in_q": context["job_queue"],
        "_out_q": context["result_queue"],
        "_ev_q": context["control_queue"],
    }


make_threadpool = create_thread_pool
