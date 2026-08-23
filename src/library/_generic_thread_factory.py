# -*- coding: utf-8 -*-

# src/library/_generic_thread_factory.py


"""
 =============================================================================
 _GENERIC_THREAD_FACTORY
 =============================================================================

 What it is:
     A lightweight, purely functional thread factory that creates, names,
     registers and manages daemon/non-daemon threads in a deterministic way.
     Supports initial bulk creation and dynamic runtime creation/stop.

 Why it is needed:
     • Centralises all thread creation logic (consistent naming, registration,
       shutdown_event injection, parameter binding via partial).
     • Enables loose coupling with _queue_factory (auto-creation of dedicated
       queues when requested).
     • Provides safe dynamic thread lifecycle management for thread_management
       and worker orchestration.
     • Guarantees free-threaded compatibility (Python 3.14+ no-GIL) with
       minimal shared mutable state protected by RLock.
     • Follows all project coding standards: functional only, partials instead
       of lambdas, copy/snapshot where needed, robust error handling.

 How it works:
     1.  Caller calls create_thread() / create_thread_pool() with a target
         function and optional args/kwargs.
     2.  Factory injects shutdown_event (if provided) and binds parameters
         using functools.partial.
     3.  Optional auto-pairing with queues from _queue_factory.
     4.  Thread is registered under a deterministic name in protected registry.
     5.  start_threads() / stop_threads() / join_threads() provide bulk control.
     6.  All registry access is snapshot-based (copy) for thread safety.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import threading
import logging
from functools import partial
import time
import copy

# Loose coupling to queue factory (optional)
try:
    from ._queue_factory import (
        create_queue,
        create_paired_queues,
        get_queue,
        list_all_queue_names,
    )
except ImportError:
    # Fallback for direct testing
    create_queue = None
    create_paired_queues = None
    get_queue = None
    list_all_queue_names = None


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT
# =============================================================================

THREAD_REGISTRY = {}
THREAD_LOCK = threading.RLock()
THREAD_ID_COUNTER = [0]

LOGGER = logging.getLogger(__name__)


# =============================================================================
# HELP FUNCTIONS
# =============================================================================

def _generate_auto_thread_name(prefix="thread"):
    """Deterministic sequential name under lock."""
    with THREAD_LOCK:
        THREAD_ID_COUNTER[0] += 1
        return f"{prefix}_{THREAD_ID_COUNTER[0]:06d}"


def _register_thread(name, thread_obj):
    """Register thread with uniqueness guarantee."""
    with THREAD_LOCK:
        original_name = name
        counter = 1
        while name in THREAD_REGISTRY:
            name = f"{original_name}_{counter}"
            counter += 1
        THREAD_REGISTRY[name] = thread_obj
        LOGGER.debug("Registered thread '%s'", name)
        return name


def _get_registry_snapshot():
    """Return safe shallow copy of registry for external use."""
    with THREAD_LOCK:
        return dict(THREAD_REGISTRY)


def _inject_shutdown_event(target_func, shutdown_event, **kwargs):
    """Inject shutdown_event if target accepts it."""
    if shutdown_event is None:
        return target_func

    try:
        # Check if shutdown_event is already in kwargs or function signature
        if "shutdown_event" not in kwargs:
            kwargs = dict(kwargs)  # copy
            kwargs["shutdown_event"] = shutdown_event
        return partial(target_func, **kwargs)
    except Exception:
        LOGGER.warning("Could not inject shutdown_event to target")
        return target_func


# =============================================================================
# CODEBASE - FUNCTIONS
# =============================================================================

def create_thread(
    name=None,
    target=None,
    daemon=False,
    shutdown_event=None,
    queue_factory_integration=False,
    queue_config=None,
    **kwargs
):
    """
    Create a single managed thread.

    Parameters:
        name                    - explicit name or auto-generated
        target                  - callable (function)
        daemon                  - daemon flag
        shutdown_event          - threading.Event injected automatically
        queue_factory_integration - if True, create associated queue(s)
        queue_config            - dict for queue creation (name, type, etc.)
        **kwargs                - passed to target (via partial)

    Returns:
        tuple (thread_object, final_name, optional_queue_info)
    """
    if target is None or not callable(target):
        raise ValueError("target must be a callable")

    try:
        if name is None:
            name = _generate_auto_thread_name("thread")

        # Optional queue integration
        queue_info = None
        if queue_factory_integration and create_queue is not None:
            qcfg = queue_config or {}
            if qcfg.get("paired", False):
                qpair, qnames = create_paired_queues(
                    base_name=qcfg.get("base_name", f"{name}_q"),
                    queue_type=qcfg.get("type", "fifo"),
                    maxsize=qcfg.get("maxsize", 0)
                )
                queue_info = {"paired": qpair, "names": qnames}
            else:
                qobj, qname = create_queue(
                    name=qcfg.get("name"),
                    queue_type=qcfg.get("type", "fifo"),
                    maxsize=qcfg.get("maxsize", 0)
                )
                queue_info = {"queue": qobj, "name": qname}

        # Bind parameters + shutdown_event
        bound_target = _inject_shutdown_event(target, shutdown_event, **kwargs)

        thread = threading.Thread(
            target=bound_target,
            name=name,
            daemon=daemon
        )

        final_name = _register_thread(name, thread)

        LOGGER.info("Created thread '%s' (daemon=%s)", final_name, daemon)
        return thread, final_name, queue_info

    except Exception as exc:
        LOGGER.error("Failed to create thread '%s': %s", name, exc, exc_info=True)
        raise


def create_thread_pool(
    count=1,
    base_name=None,
    target=None,
    daemon=False,
    shutdown_event=None,
    queue_factory_integration=False,
    **kwargs
):
    """
    Create multiple threads with sequential naming (ideal for worker pools).
    Returns (list_of_threads, list_of_names, optional_queue_infos)
    """
    if count < 1 or not callable(target):
        raise ValueError("count >= 1 and target required")

    threads = []
    names = []
    queue_infos = []

    for i in range(count):
        t_name = f"{base_name or 'worker'}_{i:04d}" if base_name else None

        t, n, qinfo = create_thread(
            name=t_name,
            target=target,
            daemon=daemon,
            shutdown_event=shutdown_event,
            queue_factory_integration=queue_factory_integration,
            **kwargs
        )
        threads.append(t)
        names.append(n)
        if qinfo:
            queue_infos.append(qinfo)

    LOGGER.info("Created thread pool of %d threads with base '%s'", count, base_name)
    return threads, names, queue_infos


def start_threads(threads_or_names):
    """Start one or more threads (accepts list, single thread, or names)."""
    if not isinstance(threads_or_names, (list, tuple)):
        threads_or_names = [threads_or_names]

    started = []
    for item in threads_or_names:
        try:
            if isinstance(item, str):  # name
                t = get_thread(item)
            else:
                t = item
            if t and not t.is_alive():
                t.start()
                started.append(t.name if hasattr(t, "name") else str(t))
        except Exception as e:
            LOGGER.warning("Failed to start thread %s: %s", item, e)

    return started


def stop_threads(threads_or_names, timeout=5.0):
    """Signal shutdown (via event) and join with timeout."""
    if not isinstance(threads_or_names, (list, tuple)):
        threads_or_names = [threads_or_names]

    stopped = []
    for item in threads_or_names:
        try:
            if isinstance(item, str):
                t = get_thread(item)
            else:
                t = item
            if t and t.is_alive():
                # shutdown_event is usually shared - already set externally
                try:
                    t.join(timeout=timeout)
                    stopped.append(t.name)
                except Exception:
                    pass
        except Exception as e:
            LOGGER.warning("Error stopping thread %s: %s", item, e)

    return stopped


def get_thread(name):
    """Retrieve thread by registered name."""
    with THREAD_LOCK:
        return THREAD_REGISTRY.get(name)


def list_all_thread_names():
    """Snapshot of all registered thread names."""
    with THREAD_LOCK:
        return list(THREAD_REGISTRY.keys())


def get_all_threads_snapshot():
    """Safe copy of registry."""
    return _get_registry_snapshot()


def remove_thread(name):
    """Remove from registry (does not kill thread)."""
    with THREAD_LOCK:
        if name in THREAD_REGISTRY:
            del THREAD_REGISTRY[name]
            LOGGER.debug("Removed thread '%s' from registry", name)
            return True
    return False


def join_all_threads(timeout_per_thread=10.0):
    """Convenience: join all registered threads."""
    snapshot = get_all_threads_snapshot()
    joined = []
    for name, t in snapshot.items():
        if t.is_alive():
            try:
                t.join(timeout=timeout_per_thread)
                joined.append(name)
            except Exception:
                pass
    return joined


# =============================================================================
# INTERFACE - EXTERNAL CALLABLE
# =============================================================================

"""
# After imports
from src.library._generic_thread_factory import create_thread, create_thread_pool

# Example replacement:
t_proc_state, _, _ = create_thread(
    name="Process_State_Management",
    target=run_process_state_management,
    resources=resources,
    queue_event_send=queue_event_send,
    queue_event_proc=queue_event_proc,
    shutdown_event=shutdown_event,
)
threads.append(t_proc_state)
"""

"""
Typical usage (in main.py or thread_management):

from src.library._generic_thread_factory import (
    create_thread,
    create_thread_pool,
    start_threads,
    stop_threads,
)

# Single thread (existing pattern compatible)
t_proc, name_proc, _ = create_thread(
    "Process_State_Management",
    run_process_state_management,
    resources=resources,
    queue_event_send=queue_event_send,
    queue_event_proc=queue_event_proc,
    shutdown_event=shutdown_event,
)

threads.append(t_proc)

# Worker pool with integrated queues
workers, w_names, q_infos = create_thread_pool(
    count=4,
    base_name="automation_worker",
    target=worker_target_func,
    daemon=False,
    shutdown_event=shutdown_event,
    queue_factory_integration=True,
    queue_config={"paired": True, "base_name": "worker_q"}
)

# Dynamic runtime
start_threads(workers)
...
stop_threads(workers)
"""

# End of _generic_thread_factory.py