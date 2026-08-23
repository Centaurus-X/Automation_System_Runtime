# -*- coding: utf-8 -*-

# src/library/_queue_factory.py


"""
 =============================================================================
 _QUEUE_FACTORY
 =============================================================================

 What it is:
     A lightweight, purely functional library that provides a **generic thread-safe
     queue factory** for creating, naming, registering and aggregating queues
     used across multiple worker threads, controller threads and automation tasks
     in an event-driven architecture. It supports standard FIFO, LIFO and
     Priority queues from the Python standard library and guarantees deterministic
     behaviour with automatic or user-supplied naming.

 Why it is needed:
     • Centralises queue creation so every thread / component receives properly
       named, registered and isolated queue instances without duplicated logic.
     • Enables simple aggregation of queues into pairs (e.g. in/out) or arbitrary
       sized lists (e.g. one queue per worker) for load distribution and
       event routing.
     • Provides a consistent, lock-protected registry that can be snapshotted
       for safe cross-thread synchronisation exactly as required by the system
       architecture (single-writer + copy-on-sync model).
     • Removes the need for any caller to manage thread-safety, naming collisions
       or queue-type selection.

 How it works:
     1.  A caller invokes create_queue(), create_paired_queues() or
         create_queue_list() with an optional name and queue configuration.
     2.  If no name is supplied a deterministic sequential name is generated
         under a re-entrant lock (e.g. "queue_000001").
     3.  The requested queue type is instantiated (Queue / PriorityQueue /
         LifoQueue) with the supplied maxsize.
     4.  The queue is registered under a guaranteed-unique name inside the
         central protected registry.
     5.  For aggregation the factory returns tuples/lists of queue objects
         together with their final assigned names so callers can distribute
         work without further bookkeeping.
     6.  All read access to the registry returns shallow copies / snapshots
         so that downstream synchronisation can occur without holding locks
         for long periods and without risk of deadlock.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import queue
import threading
import logging


# =============================================================================
# CENTRAL CONTEXT MANAGEMENT
# =============================================================================

"""
Central in-memory registry for all created queues.
Protected by RLock so the same thread may re-enter safely during nested
operations (generate -> register) while still preventing concurrent
modification from different threads. This satisfies the "lock without
deadlocks" requirement and enables deterministic snapshot synchronisation.
"""

_queue_registry = {}
_queue_lock = threading.RLock()
_queue_id_counter = [0]

_logger = logging.getLogger(__name__)


# =============================================================================
# HELP FUNCTIONS
# =============================================================================


def _generate_auto_name(prefix="queue"):
    """
    Generate the next deterministic sequential queue name under lock.
    Format: prefix_000001, prefix_000002, ... (zero-padded, 6 digits).
    The counter is process-wide and monotonically increasing within one run.
    """
    with _queue_lock:
        _queue_id_counter[0] += 1
        return f"{prefix}_{_queue_id_counter[0]:06d}"


def _register_queue(proposed_name, q):
    """
    Register a queue under proposed_name. If the name already exists (race
    condition or duplicate request) a numeric suffix is appended until a
    free name is found. Always executed under lock. Returns the final name
    that was actually used.
    """
    with _queue_lock:
        name = proposed_name
        if name in _queue_registry:
            _logger.warning(
                "Queue name '%s' already in use - generating unique variant",
                name
            )
            base = name
            i = 1
            while name in _queue_registry:
                name = f"{base}_{i}"
                i += 1
        _queue_registry[name] = q
        return name


# =============================================================================
# CODEBASE - FUNCTIONS
# =============================================================================


def create_queue(name=None, queue_type="fifo", maxsize=0):
    """
    Create a single thread-safe queue.

    Parameters (positional count kept <= 4):
        name        - optional explicit name. If None an auto-generated
                      deterministic name is used.
        queue_type  - "fifo" (default), "priority" or "lifo"
        maxsize     - maximum queue size (0 = unlimited)

    Returns:
        tuple (queue_object, final_assigned_name)

    The function is wrapped in try/except so that any failure is logged
    with full context before re-raising. This is one of the most-used entry
    points and therefore must be robust.
    """
    try:
        if name is None:
            name = _generate_auto_name()

        # Instantiate the concrete queue (outside lock - cheap operation)
        qtype = queue_type.lower()
        if qtype in ("fifo", "queue"):
            q = queue.Queue(maxsize=maxsize)
        elif qtype in ("priority", "prio", "priorityqueue"):
            q = queue.PriorityQueue(maxsize=maxsize)
        elif qtype in ("lifo", "stack", "lifoqueue"):
            q = queue.LifoQueue(maxsize=maxsize)
        else:
            raise ValueError(
                f"Unsupported queue_type '{queue_type}'. "
                "Use 'fifo', 'priority' or 'lifo'."
            )

        final_name = _register_queue(name, q)
        _logger.debug(
            "Created %s queue '%s' (maxsize=%s)", qtype, final_name, maxsize
        )
        return q, final_name

    except Exception as exc:
        _logger.error("Failed to create queue (name=%s, type=%s): %s",
                      name, queue_type, exc, exc_info=True)
        raise


def create_paired_queues(base_name=None, queue_type="fifo", maxsize=0):
    """
    Convenience helper that creates exactly two related queues (commonly
    used for request/response or inbound/outbound patterns between threads).

    If base_name is supplied the two queues receive the suffixes "_in" and
    "_out". Otherwise a fresh auto-generated base is used.

    Returns:
        tuple( (q_in, q_out), (name_in, name_out) )
    """
    try:
        if base_name is None:
            base = _generate_auto_name("pair")
            name_in = f"{base}_in"
            name_out = f"{base}_out"
        else:
            name_in = f"{base_name}_in"
            name_out = f"{base_name}_out"

        q_in, n_in = create_queue(name=name_in, queue_type=queue_type, maxsize=maxsize)
        q_out, n_out = create_queue(name=name_out, queue_type=queue_type, maxsize=maxsize)

        _logger.info("Created paired queues base='%s' -> (%s, %s)", base_name, n_in, n_out)
        return (q_in, q_out), (n_in, n_out)

    except Exception as exc:
        _logger.error("Failed to create paired queues (base=%s): %s",
                      base_name, exc, exc_info=True)
        raise


def create_queue_list(count, base_name=None, queue_type="fifo", maxsize=0):
    """
    Create an arbitrary number of queues in one call. Ideal for spinning up
    N worker threads each having its own dedicated input queue.

    The queues are named deterministically:
        <base>_0000, <base>_0001, ...

    Returns:
        tuple( list_of_queue_objects, list_of_assigned_names )
    """
    if not isinstance(count, int) or count < 1:
        raise ValueError("count must be a positive integer")

    try:
        if base_name is None:
            base = _generate_auto_name("list")
        else:
            base = base_name

        queues = []
        names = []
        for i in range(count):
            q_name = f"{base}_{i:04d}"
            q, n = create_queue(name=q_name, queue_type=queue_type, maxsize=maxsize)
            queues.append(q)
            names.append(n)

        _logger.info(
            "Created queue list of length %d with base '%s'", count, base
        )
        return queues, names

    except Exception as exc:
        _logger.error("Failed to create queue list (count=%s, base=%s): %s",
                      count, base_name, exc, exc_info=True)
        raise


def get_queue(name):
    """
    Retrieve a queue by its registered name. Returns None (and logs a
    warning) when the name is unknown. Access is protected by lock but
    the returned queue reference can be used safely afterwards because
    queue.Queue itself is thread-safe.
    """
    with _queue_lock:
        q = _queue_registry.get(name)
        if q is None:
            _logger.warning("Queue '%s' not found in registry", name)
        return q


def list_all_queue_names():
    """
    Return a snapshot (list copy) of all currently registered queue names.
    The snapshot can be passed to other threads or used for
    external synchronisation without holding the registry lock.
    """
    try:
        with _queue_lock:
            return list(_queue_registry.keys())
    except Exception as exc:
        _logger.error("Error while listing queue names: %s", exc, exc_info=True)
        return []


def get_all_queues():
    """
    Return a shallow snapshot (dict copy) of the complete name->queue
    mapping. This is the primary mechanism for "copy/snapshot"
    synchronisation described in the architecture specification.
    Callers must treat the returned dict as read-only.
    """
    try:
        with _queue_lock:
            return dict(_queue_registry)
    except Exception as exc:
        _logger.error("Error while snapshotting all queues: %s", exc, exc_info=True)
        return {}


def aggregate_queues(*queue_names):
    """
    Build a list of queue objects from a variable number of names.
    Missing names are skipped with a warning. The resulting list can be
    used directly for round-robin distribution, broadcasting or any
    other aggregation pattern required by GenericEventRouter or
    worker orchestration logic.

    Because *queue_names is used, any number of arguments (>4) is
    supported without violating the "use args/kwargs for >4 parameters"
    rule.
    """
    if not queue_names:
        return []

    try:
        result = []
        with _queue_lock:
            for nm in queue_names:
                q = _queue_registry.get(nm)
                if q is not None:
                    result.append(q)
                else:
                    _logger.warning(
                        "aggregate_queues: queue '%s' not found - skipped", nm
                    )
        return result
    except Exception as exc:
        _logger.error("Failed to aggregate queues: %s", exc, exc_info=True)
        return []


def remove_queue(name):
    """
    Remove a single queue from the registry (does NOT drain or close the
    queue object itself). Returns True if the queue existed and was removed.
    """
    try:
        with _queue_lock:
            if name in _queue_registry:
                del _queue_registry[name]
                _logger.debug("Removed queue '%s' from registry", name)
                return True
            else:
                _logger.warning("remove_queue: '%s' not present", name)
                return False
    except Exception as exc:
        _logger.error("Error removing queue '%s': %s", name, exc, exc_info=True)
        return False


# =============================================================================
# INTERFACE - EXTERNAL CALLABLE (optional)
# =============================================================================

"""
All public functions above constitute the external interface of the library.
They are intentionally kept free of decorators, classes, dataclasses,
SimpleNamespace, lambdas and type annotations so that the module remains
100 % compatible with the project's coding standard (pure functional,
Python 3.14+ free-threaded, C/Cython convertible, highest readability).

Typical usage pattern (example only - not executed here):

    from src.library._queue_factory import (
        create_queue, create_queue_list, aggregate_queues, get_queue
    )

    # single named queue
    sensor_q, sensor_name = create_queue("sensor_bus")

    # one queue per worker (aggregation use-case)
    worker_qs, worker_names = create_queue_list(4, base_name="worker_in")

    # later in router
    all_sensor_queues = aggregate_queues("sensor_bus", "aux_bus")

    # snapshot for cross-thread sync
    current_state = get_all_queues()
"""

# End of _queue_factory.py
