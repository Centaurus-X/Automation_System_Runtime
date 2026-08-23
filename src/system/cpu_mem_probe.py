# -*- coding: utf-8 -*-

# src/system/cpu_mem_probe.py


"""
 =============================================================================
 CPU_MEM_PROBE
 =============================================================================

 What it is:
     A pure-functional, zero-boilerplate library for periodic, thread-safe
     sampling of per-OS-thread CPU usage PLUS process-level and system-level
     memory metrics. Runs in a single daemon thread, feeds a shared state
     dict (with optional lock) and/or pushes structured events into the
     central Event-Broker queue. Fully compatible with the event-driven,
     multi-threaded automation architecture and the generic_type_library
     patterns (functools.partial, no classes, no type hints, deterministic).

 Why it is needed:
     • Real-time visibility into CPU hot-spots (top-N threads by % load)
       and memory pressure (RSS/VMS/percent for the process + system RAM).
     • Enables rule-engine decisions, logging, alerting and auto-throttling
       based on resource metrics without any blocking in worker/controller
       threads.
     • Single source of truth for resource samples: either pull latest via
       snapshot_cpu_mem_info() or consume via event queue.
     • Backward-compatible extension of the original CPU-only probe;
       existing call sites continue to work while gaining memory data
       "for free".
     • Deterministic, lock-minimized, C/Cython-friendly code style matching
       the project-wide coding standard (Python 3.14+ free-threaded ready).

 How it works:
     1.  start_cpu_mem_probe(shutdown_event, info=..., info_lock=..., **opts)
         spawns ONE daemon thread "CPU-Mem-Probe".
     2.  Inside the tight loop:
         a. enumerate all OS threads of the current process via psutil
         b. diff user_time + system_time against last_times dict → CPU %
            (optionally normalized by cpu_count so 100 % = full machine load)
         c. filter ≥ min_pct, sort by load desc, keep top_n
         d. sample proc.memory_info() + memory_percent() + virtual_memory()
         e. assemble ONE deterministic sample dict containing:
            - ts, interval, cpu_count, normalized
            - top: list of {"tid", "name", "pct"}
            - per_thread: {tid: pct, ...} compact
            - memory: {"process": {...}, "system": {...}}
     3.  If info dict + lock supplied → update under lock (readers always
         get a safe deepcopy via snapshot_*).
     4.  If event_queue supplied → push {"event_type": "...", "timestamp":,
         "sample": sample} via partial-wrapped _safe_put (never blocks hard).
     5.  Optional structured INFO log line with top CPU threads + current
         process RSS MB + % and system RAM %.
     6.  Loop sleeps via shutdown_event.wait(interval) → instant shutdown
         reaction, no lost samples on exit.
     7.  All helpers are pure or partial-bound; no lambdas, no decorators,
         no classes, no type hints anywhere.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import time
import threading
import logging
import copy
from functools import partial

try:
    import psutil
except ImportError:
    psutil = None


# =============================================================================
# CENTRAL MODULE "CONTEXT MANAGEMENT" definition & initialisation
# =============================================================================

LOGGER = logging.getLogger("cpu_mem_probe")

CONTEXT = {
    "library_name": "cpu_mem_probe",
    "library_version": "2.0.0",
    "description": "CPU per-thread + process/system memory probe (event + shared-dict)",
    "options": {
        "default_interval": 1.0,
        "default_top_n": 10,
        "default_min_pct": 0.2,
        "default_history_max": 120,
        "default_normalize_per_cpu": True,
        "default_log_top": True,
        "default_log_level": logging.INFO,
        "default_event_type": "CPU_MEM_PROBE_SAMPLE",
    },
}


# =============================================================================
# HELP-FUNCTIONS
# =============================================================================

def _sorted_by_pct(items):
    """Return list of (tid, pct) tuples sorted descending by pct. No lambda."""
    def _key(kv):
        return kv[1]
    return sorted(items, key=_key, reverse=True)


def _safe_put(q, item):
    """Never-crash put into queue (used via partial)."""
    try:
        q.put_nowait(item)
    except Exception:
        try:
            q.put(item, timeout=0.1)
        except Exception:
            pass


def snapshot_cpu_mem_info(info, info_lock=None):
    """
    Thread-safe snapshot of the shared probe state.
    ALWAYS returns a deep copy so callers can read without holding locks.
    """
    if info is None:
        return {}
    if info_lock is not None:
        with info_lock:
            return copy.deepcopy(info)
    # best-effort without lock (discouraged)
    return copy.deepcopy(info)


def wait_for_first_sample(info, info_lock=None, timeout=5.0):
    """
    Block until the probe has written at least one sample into the shared dict.
    Returns True on success, False on timeout.
    """
    if info is None:
        return False
    t_end = time.time() + float(timeout)
    while time.time() < t_end:
        if info_lock is not None:
            with info_lock:
                if "last_ts" in info:
                    return True
        else:
            if "last_ts" in info:
                return True
        time.sleep(0.05)
    return False


def _update_shared_info(info, sample, history_max):
    """
    Minimal-lock update of the shared state dict.
    Keeps ring-buffer history of CPU top + compact memory snapshots.
    """
    info["last_ts"] = sample["ts"]
    info["interval"] = sample["interval"]
    info["cpu_count"] = sample["cpu_count"]
    info["normalized"] = sample["normalized"]
    info["top"] = sample["top"]
    info["per_thread"] = sample["per_thread"]
    info["memory"] = sample["memory"]

    hist = info.get("history")
    if not isinstance(hist, list):
        hist = []
        info["history"] = hist
    hist.append({
        "ts": sample["ts"],
        "top": sample["top"],
        "mem_rss_mb": sample["memory"]["process"]["rss_mb"],
        "mem_pct": sample["memory"]["process"]["percent"],
    })
    if len(hist) > int(history_max):
        del hist[: len(hist) - int(history_max)]


# =============================================================================
# CODEBASE - FUNCTIONS - PART 1: Core sampling loop (CPU + Memory)
# =============================================================================

def _cpu_mem_probe_loop(
    shutdown_event,
    info,
    info_lock,
    interval,
    top_n,
    min_pct,
    history_max,
    normalize_per_cpu,
    push_event,
    event_type,
    log_top,
    log_level,
):
    """
    Internal tight loop executed by the daemon probe thread.
    Samples BOTH CPU per-thread AND memory (process + system) every interval.
    Completely deterministic and lock-minimized.
    """
    if psutil is None:
        return

    proc = psutil.Process()
    last_times = {}  # tid -> (user_time, system_time)
    cpu_count = psutil.cpu_count(logical=True) or 1

    while not (shutdown_event and shutdown_event.is_set()):
        # Build name map once per sample (cheap)
        name_by_tid = {}
        for t in threading.enumerate():
            if t.native_id:
                name_by_tid[t.native_id] = t.name

        threads = proc.threads()

        # Responsive shutdown wait
        if shutdown_event and shutdown_event.wait(interval):
            break

        now = time.time()

        # --- CPU per-thread calculation (identical algorithm to v1) ---
        stats = {}
        for th in threads:
            tid = th.id
            u = th.user_time
            s = th.system_time
            prev = last_times.get(tid)
            if prev is None:
                last_times[tid] = (u, s)
                continue
            pu, ps = prev
            du = u - pu
            ds = s - ps
            last_times[tid] = (u, s)

            pct = 100.0 * (du + ds) / max(interval, 1e-6)
            if normalize_per_cpu:
                pct = pct / float(cpu_count)
            stats[tid] = pct

        # filter + sort top
        filtered = [(tid, pct) for tid, pct in stats.items() if pct >= float(min_pct)]
        filtered = _sorted_by_pct(filtered)
        top = filtered[: int(top_n)]

        top_struct = [
            {
                "tid": int(tid),
                "name": str(name_by_tid.get(tid, "?")),
                "pct": float(round(pct, 2)),
            }
            for (tid, pct) in top
        ]

        per_thread_compact = {
            int(tid): float(round(pct, 4)) for tid, pct in stats.items()
        }

        # --- Memory sampling (process + system) ---
        mem_info = proc.memory_info()
        mem_pct = proc.memory_percent()
        virtual = psutil.virtual_memory()

        memory_section = {
            "process": {
                "rss_bytes": int(mem_info.rss),
                "vms_bytes": int(mem_info.vms),
                "percent": float(round(mem_pct, 2)),
                "rss_mb": round(mem_info.rss / (1024.0 * 1024.0), 2),
            },
            "system": {
                "total_bytes": int(virtual.total),
                "available_bytes": int(virtual.available),
                "used_bytes": int(virtual.used),
                "percent": float(round(virtual.percent, 2)),
            },
        }

        # --- Unified deterministic sample ---
        sample = {
            "ts": float(now),
            "interval": float(interval),
            "cpu_count": int(cpu_count),
            "normalized": bool(normalize_per_cpu),
            "top": top_struct,
            "per_thread": per_thread_compact,
            "memory": memory_section,
        }

        # --- Publish to shared dict (thread-safe) ---
        if info is not None:
            if info_lock is not None:
                with info_lock:
                    _update_shared_info(info, sample, history_max)
            else:
                _update_shared_info(info, sample, history_max)

        # --- Publish as event (via partial, never blocks) ---
        if push_event is not None:
            evt = {
                "event_type": event_type,
                "timestamp": float(now),
                "sample": sample,
            }
            push_event(evt)

        # --- Optional structured logging ---
        if log_top and top_struct:
            cpu_line = " | ".join(
                f"{d['name']}@{d['tid']}: {d['pct']:.2f}%" for d in top_struct
            )
            mem_p = memory_section["process"]
            mem_s = memory_section["system"]
            mem_line = (
                f"PROC: {mem_p['rss_mb']:.1f}MB ({mem_p['percent']:.1f}%) | "
                f"SYS: {mem_s['percent']:.1f}% used"
            )
            logging.log(log_level, "CPU-MEM [%s] %s || %s", event_type, cpu_line, mem_line)

        # safety sleep in case wait() returned early without flag
        # (should not happen but keeps loop robust)
        time.sleep(0.0)


# =============================================================================
# CODEBASE - FUNCTIONS - PART 2: Public starter & legacy compatibility
# =============================================================================

def start_cpu_mem_probe(
    shutdown_event,
    info=None,
    info_lock=None,
    interval=None,
    top_n=None,
    min_pct=None,
    history_max=None,
    normalize_per_cpu=None,
    event_queue=None,
    event_type=None,
    log_top=None,
    log_level=None,
):
    """
    Start the combined CPU-per-thread + Memory probe thread.

    All parameters are optional except shutdown_event.
    Defaults are taken from CONTEXT["options"] so the probe is fully
    configurable at runtime via CONTEXT if desired.

    Returns the started Thread object or None (when psutil missing).
    """
    if psutil is None:
        LOGGER.warning("CPU-MEM-Probe disabled (psutil not installed).")
        return None

    # resolve defaults from central CONTEXT (single source of truth)
    opts = CONTEXT["options"]
    interval = float(interval) if interval is not None else opts["default_interval"]
    top_n = int(top_n) if top_n is not None else opts["default_top_n"]
    min_pct = float(min_pct) if min_pct is not None else opts["default_min_pct"]
    history_max = int(history_max) if history_max is not None else opts["default_history_max"]
    normalize_per_cpu = (
        bool(normalize_per_cpu)
        if normalize_per_cpu is not None
        else opts["default_normalize_per_cpu"]
    )
    event_type = event_type or opts["default_event_type"]
    log_top = bool(log_top) if log_top is not None else opts["default_log_top"]
    log_level = int(log_level) if log_level is not None else opts["default_log_level"]

    # bind safe event pusher once (partial, no lambda)
    push_event = partial(_safe_put, event_queue) if event_queue is not None else None

    t = threading.Thread(
        target=_cpu_mem_probe_loop,
        name="CPU-Mem-Probe",
        daemon=True,
        args=(
            shutdown_event,
            info,
            info_lock,
            interval,
            top_n,
            min_pct,
            history_max,
            normalize_per_cpu,
            push_event,
            event_type,
            log_top,
            log_level,
        ),
    )
    t.start()
    LOGGER.info(
        "CPU-MEM probe started (interval=%.2fs, top_n=%d, normalize=%s)",
        interval,
        top_n,
        normalize_per_cpu,
    )
    return t


def start_cpu_probe(
    shutdown_event,
    info=None,
    info_lock=None,
    interval=None,
    top_n=None,
    min_pct=None,
    history_max=None,
    normalize_per_cpu=None,
    event_queue=None,
    event_type=None,
    log_top=None,
    log_level=None,
):
    """
    Legacy compatibility wrapper.
    Old call sites using start_cpu_probe(...) continue to work unchanged
    and now automatically receive the extended memory metrics in every sample.
    """
    return start_cpu_mem_probe(
        shutdown_event=shutdown_event,
        info=info,
        info_lock=info_lock,
        interval=interval,
        top_n=top_n,
        min_pct=min_pct,
        history_max=history_max,
        normalize_per_cpu=normalize_per_cpu,
        event_queue=event_queue,
        event_type=event_type,
        log_top=log_top,
        log_level=log_level,
    )


# =============================================================================
# INTERFACE - EXTERNAL CALLABLE (public API)
# =============================================================================

def get_probe_context():
    """Return a snapshot of the library CONTEXT (for diagnostics / config)."""
    return copy.deepcopy(CONTEXT)


def format_latest_sample(info, info_lock=None):
    """Return a human-readable one-line summary of the latest sample (if any)."""
    snap = snapshot_cpu_mem_info(info, info_lock)
    if not snap or "last_ts" not in snap:
        return "no sample yet"
    mem = snap.get("memory", {}).get("process", {})
    top = snap.get("top", [])
    top_str = " ".join(f"{d['name']}:{d['pct']:.1f}%" for d in top[:3])
    return (
        f"ts={snap['last_ts']:.1f} top={top_str} "
        f"rss={mem.get('rss_mb', 0):.1f}MB ({mem.get('percent', 0):.1f}%)"
    )


# Auto-initialise on import (matches generic_type_library pattern)
def _initialize_context():
    """Nothing dynamic to register yet; placeholder for future extensibility."""
    LOGGER.debug(
        "%s v%s initialised (defaults in CONTEXT)",
        CONTEXT["library_name"],
        CONTEXT["library_version"],
    )


_initialize_context()
