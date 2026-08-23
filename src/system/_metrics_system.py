# -*- coding: utf-8 -*-

# src/system/_metrics_system.py


"""
Generic metric system (functional, fully synchronous) for:
- Queue‑Metriken (Laufzeit, Latenz via Payload‑Fingerprint, Durchsatz 1s/60s, Fehler)
- Performance‑Metriken (CPU, Memory, Execution‑Time)
- Code‑Metriken (Cyclomatic Complexity, Duplikationsrate, Stubs für QA)

Kein OOP, keine Decorators. Nutzung von functools.partial.
"""

"""
Legende – Queue-Metriken (Zeiten in ms; Fenster: ~1 s / ~60 s)

Spalten:
  Queue     : Klarname der Queue (falls gesetzt), sonst 'Queue#<id-suffix>'.
  size      : Aktuelle Queue-Länge. Normal: nahe 0; Bursts ok, sollten schnell abklingen.
  inflt     : "In-Flight" im Messfenster ≈ max(0, puts - gets). Dauerhaft >0 ==> Backpressure.
  put/s     : Enqueues pro Sekunde (kurzes Fenster).
  get/s     : Dequeues pro Sekunde (kurzes Fenster).
  put/min   : Enqueues pro Minute (glatt/flankensensitiv über längeres Fenster).
  get/min   : Dequeues pro Minute (glatt/flankensensitiv über längeres Fenster).
  lat_avg   : Durchschnittliche End-to-End-Queue-Wartezeit (put→get), ohne Downstream-Verarbeitung.
  p95       : 95. Perzentil der Wartezeit. Realistisch: deutlich unter Maxima.
  p99       : 99. Perzentil (Tail-Latency). Sensibel auf Kurzspitzen/Outliers.
  max       : Maximale beobachtete Wartezeit im Aggregatfenster.
  put_rt    : Mittlere Laufzeit eines put()-Aufrufs inkl. Serialisierung/Executor-Hop (~0.2–1.0 ms in-memory).
  get_rt    : Mittlere Laufzeit eines get()-Aufrufs. Bei Leerlauf steigt get_rt durch Warten (nicht automatisch schlecht).
  err       : Fehlerzähler 'put_err/get_err/timeout'. Einzelne Timeouts bei selten genutzten Queues sind normal.

Richtwerte (Single-Prozess, In-Memory):
  • size/inflt ≈ 0 im Mittel; Peaks ok, sollten binnen Sekunden/Minuten kollabieren.
  • lat_avg < 10–30 ms typisch; p95 < 100–200 ms; p99 < 300–400 ms (Workload-abhängig).
  • put_rt/get_rt typ. < 1 ms; hoher get_rt bei Leerlauf heißt oft nur: auf Elemente gewartet.
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import os
import gc
import json
import time
import threading
import tracemalloc
import hashlib
import logging
from collections import Counter, deque
from functools import partial

# -------------------------------------------------------
# Logger
logger = logging.getLogger(__name__)
# -------------------------------------------------------

# =============================================================================
# Configuration (overridable via ENV)
# =============================================================================

def _as_bool(x, default):
    try:
        return bool(int(str(x)))
    except Exception:
        return bool(default)

METRICS_ENABLED            = _as_bool(os.getenv("METRICS_ENABLED", "1"), True)
METRICS_TABLE_EVERY_S      = float(os.getenv("METRICS_TABLE_EVERY_S", "10.0"))
METRICS_LATENCY_ENABLED    = _as_bool(os.getenv("METRICS_LATENCY_ENABLED", "1"), True)
METRICS_HASH               = str(os.getenv("METRICS_HASH", "sha1")).lower()   # sha1|sha256
METRICS_MAX_TS_SHORT       = int(os.getenv("METRICS_MAX_TS_SHORT", "2048"))   # put/get timestamps (short)
METRICS_MAX_TS_LONG        = int(os.getenv("METRICS_MAX_TS_LONG",  "12000"))  # put/get timestamps (long ~ 60s+)
METRICS_MAX_SAMPLES        = int(os.getenv("METRICS_MAX_SAMPLES",  "12000"))  # latency/runtime (value,timestamp)
METRICS_LOGGER_NAME        = os.getenv("METRICS_LOGGER_NAME", "_metrics")

# =============================================================================
# Metrik‑Registry (allgemeine Metriken)
# =============================================================================

_metrics_registry = {}

def register_metric(name, metric_func, group="general", description=""):
    _metrics_registry[name] = {
        "func": metric_func,
        "group": group,
        "description": description
    }

def unregister_metric(name):
    if name in _metrics_registry:
        del _metrics_registry[name]

def list_metrics(group=None):
    if group is None:
        return list(_metrics_registry.keys())
    return [k for k, v in _metrics_registry.items() if v["group"] == group]

def get_metric(name):
    return _metrics_registry.get(name, {}).get("func", None)

def metric_info(name):
    e = _metrics_registry.get(name, {})
    return {"group": e.get("group", ""), "description": e.get("description", "")}

def build_metrics_context(**kwargs):
    ctx = {}
    ctx.update(kwargs)
    return ctx

def run_metric(name, *args, context=None, **kwargs):
    m = get_metric(name)
    if m is None:
        raise ValueError(f"Metric '{name}' is not registered!")
    if context is not None:
        return m(*args, context=context, **kwargs)
    return m(*args, **kwargs)

def run_metrics_batch(names, *args, context=None, **kwargs):
    return [run_metric(n, *args, context=context, **kwargs) for n in names]

# =============================================================================
# Queue‑Metriken – State & Engine
# =============================================================================

# globaler Zustand (keine OOP)
_queue_state = {}
_queue_lock  = threading.RLock()
_global_tick = {"last_table_log": 0.0}

def _now():
    return time.perf_counter()

def _utc_iso():
    import datetime
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

def _hash_serialized(serialized):
    b = serialized.encode("utf-8", errors="ignore")
    if METRICS_HASH == "sha256":
        return hashlib.sha256(b).hexdigest()
    return hashlib.sha1(b).hexdigest()

def _ensure_queue_state(q):
    qid = id(q)
    with _queue_lock:
        st = _queue_state.get(qid)
        if st is not None:
            return st
        # lazy‑init
        name = getattr(q, "metrics_name", None)
        if not name:
            # lesbarer Fallback, nur id‑Suffix
            name = f"Queue#{hex(qid)[-5:]}"
        st = {
            "id": qid,
            "ref": q,
            "name": name,
            "first_seen": _now(),
            "last_seen": _now(),
            # Throughput timestamps
            "puts_short": deque(maxlen=METRICS_MAX_TS_SHORT),
            "gets_short": deque(maxlen=METRICS_MAX_TS_SHORT),
            "puts_long":  deque(maxlen=METRICS_MAX_TS_LONG),
            "gets_long":  deque(maxlen=METRICS_MAX_TS_LONG),
            # Inflight matching: hash -> deque[t_put]
            "inflight": {},
            # value samples with timestamps (ts, ms)
            "latency_ts":   deque(maxlen=METRICS_MAX_SAMPLES),
            "put_rt_ts":    deque(maxlen=METRICS_MAX_SAMPLES),
            "get_rt_ts":    deque(maxlen=METRICS_MAX_SAMPLES),
            "ser_ts":       deque(maxlen=METRICS_MAX_SAMPLES),
            "deser_ts":     deque(maxlen=METRICS_MAX_SAMPLES),
            "size_bytes_ts":deque(maxlen=METRICS_MAX_SAMPLES),
            # errors
            "put_errors": 0,
            "get_errors": 0,
            "get_timeouts": 0,
            # logging
            "last_logged": 0.0,
        }
        _queue_state[qid] = st
        return st

def queue_register(q, name=None):
    st = _ensure_queue_state(q)
    if name:
        with _queue_lock:
            st["name"] = str(name)
    return st

def queue_mark_put_error(q, exc=None):
    try:
        st = _ensure_queue_state(q)
        with _queue_lock:
            st["put_errors"] += 1
            st["last_seen"] = _now()
    except Exception:
        pass

def queue_mark_get_error(q, exc=None, timeout=False):
    try:
        st = _ensure_queue_state(q)
        with _queue_lock:
            if timeout:
                st["get_timeouts"] += 1
            else:
                st["get_errors"] += 1
            st["last_seen"] = _now()
    except Exception:
        pass

def queue_track_put(q, serialized, ser_sec, enqueue_sec):
    if not METRICS_ENABLED:
        return
    try:
        st = _ensure_queue_state(q)
        now = _now()
        hb  = _hash_serialized(serialized) if METRICS_LATENCY_ENABLED else None
        with _queue_lock:
            st["last_seen"] = now
            st["puts_short"].append(now)
            st["puts_long"].append(now)
            st["put_rt_ts"].append((now, (ser_sec + enqueue_sec) * 1000.0))
            st["ser_ts"].append((now, ser_sec * 1000.0))
            st["size_bytes_ts"].append((now, float(len(serialized))))
            if hb:
                dq = st["inflight"].setdefault(hb, deque())
                dq.append(now)
    except Exception:
        pass


def queue_health(sample, context=None, **kwargs):
    """
    Bewertet eine Queue-Stichprobe.
    Erwartet dict mit: name, size, inflt, put_s, get_s, p95 (ms), lat_avg (ms), max (ms)
    """
    def _f(x): 
        try: return float(x or 0.0)
        except Exception: return 0.0

    size   = _f(sample.get("size"))
    inflt  = _f(sample.get("inflt"))
    put_s  = _f(sample.get("put_s"))
    get_s  = _f(sample.get("get_s"))
    p95_ms = _f(sample.get("p95"))

    bp     = (size + inflt) / (get_s + 1e-6)
    lag    = max(0.0, put_s - get_s)
    # simple heuristic, robust and explainable
    sev = "OK"
    if (get_s == 0.0 and put_s > 0.0) or size >= 64 or bp > 40.0 or p95_ms > 2000.0:
        sev = "CRITICAL"
    elif bp > 10.0 or lag > 1.0 or p95_ms > 400.0:
        sev = "WARN"

    return {
        "name":      str(sample.get("name") or ""),
        "bp_score":  bp,
        "lag_rate":  lag,
        "p95_ms":    p95_ms,
        "severity":  sev
    }


def queue_track_get(q, serialized, deser_sec, dequeue_sec):
    if not METRICS_ENABLED:
        return
    try:
        st = _ensure_queue_state(q)
        now = _now()
        hb  = _hash_serialized(serialized) if METRICS_LATENCY_ENABLED else None
        with _queue_lock:
            st["last_seen"] = now
            st["gets_short"].append(now)
            st["gets_long"].append(now)
            st["get_rt_ts"].append((now, (deser_sec + dequeue_sec) * 1000.0))
            st["deser_ts"].append((now, deser_sec * 1000.0))
            if hb:
                dq = st["inflight"].get(hb)
                if dq and dq:
                    t_put = dq.popleft()
                    latency_ms = (now - t_put) * 1000.0
                    st["latency_ts"].append((now, latency_ms))
    except Exception:
        pass

def _count_in_window(ts_deque, window_s, now):
    # ts_deque is of floats (perf_counter seconds)
    n = 0
    for t in reversed(ts_deque):
        if now - t <= window_s:
            n += 1
        else:
            break
    return n

def _values_in_window(pair_deque, window_s, now):
    # (ts, value)
    out = []
    for ts, v in reversed(pair_deque):
        if now - ts <= window_s:
            out.append(v)
        else:
            break
    return out

def _avg(lst):
    return (sum(lst) / len(lst)) if lst else 0.0

def _p95(lst):
    if not lst:
        return 0.0
    s = sorted(lst)
    k = int(0.95 * (len(s) - 1))
    return float(s[k])

def _max(lst):
    return float(max(lst)) if lst else 0.0


def _p99(lst):
    if not lst:
        return 0.0
    s = sorted(lst)
    k = int(0.99 * (len(s) - 1))
    return float(s[k])


def queue_snapshot(q):
    st = _ensure_queue_state(q)
    now = _now()
    try:
        qsize = q.qsize() if hasattr(q, "qsize") else None
    except Exception:
        qsize = None
    with _queue_lock:
        puts_1s = _count_in_window(st["puts_short"], 1.0,  now)
        gets_1s = _count_in_window(st["gets_short"], 1.0,  now)
        puts_60 = _count_in_window(st["puts_long"],  60.0, now)
        gets_60 = _count_in_window(st["gets_long"],  60.0, now)

        lat_60   = _values_in_window(st["latency_ts"],   60.0, now)
        putrt_60 = _values_in_window(st["put_rt_ts"],    60.0, now)
        getrt_60 = _values_in_window(st["get_rt_ts"],    60.0, now)
        ser_60   = _values_in_window(st["ser_ts"],       60.0, now)
        deser_60 = _values_in_window(st["deser_ts"],     60.0, now)
        sizes_60 = _values_in_window(st["size_bytes_ts"],60.0, now)

        inflight = sum(len(dq) for dq in st["inflight"].values())

        return {
            "name":          st["name"],
            "qsize":         qsize,
            "inflight":      inflight,
            "puts_per_s":    float(puts_1s),
            "gets_per_s":    float(gets_1s),
            "puts_per_min":  float(puts_60),
            "gets_per_min":  float(gets_60),
            "lat_avg_ms":    _avg(lat_60),
            "lat_p95_ms":    _p95(lat_60),
            "lat_p99_ms":    _p99(lat_60),   # <--- NEU
            "lat_max_ms":    _max(lat_60),
            "put_rt_ms_avg": _avg(putrt_60),
            "get_rt_ms_avg": _avg(getrt_60),
            "ser_ms_avg":    _avg(ser_60),
            "deser_ms_avg":  _avg(deser_60),
            "size_bytes_avg":_avg(sizes_60),
            "put_errors":    st["put_errors"],
            "get_errors":    st["get_errors"],
            "get_timeouts":  st["get_timeouts"],
        }


def _format_table(rows, headers):
    # simple ASCII table without external dependencies
    cols = list(headers)
    # Spaltenbreiten
    w = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    sep = "+".join("-" * (w[c] + 2) for c in cols)
    def _fmt(r):
        return "|".join(" " + str(r.get(c, "")).rjust(w[c]) + " " for c in cols)
    out = []
    out.append("+" + sep + "+")
    out.append("|" + "|".join(" " + c.center(w[c]) + " " for c in cols) + "|")
    out.append("+" + sep + "+")
    for r in rows:
        out.append("|" + _fmt(r) + "|")
    out.append("+" + sep + "+")
    return "\n".join(out)


def format_all_queues_table():
    rows = []
    with _queue_lock:
        qs = list(_queue_state.values())
    for st in qs:
        try:
            snap = queue_snapshot(st["ref"])
        except Exception:
            continue
        rows.append({
            "Queue":   snap["name"],
            "size":    snap["qsize"],
            "inflt":   snap["inflight"],
            "put/s":   f'{snap["puts_per_s"]:.0f}',
            "get/s":   f'{snap["gets_per_s"]:.0f}',
            "put/min": f'{snap["puts_per_min"]:.0f}',
            "get/min": f'{snap["gets_per_min"]:.0f}',
            "lat_avg": f'{snap["lat_avg_ms"]:.1f}ms',
            "p95":     f'{snap["lat_p95_ms"]:.1f}ms',
            "p99":     f'{snap["lat_p99_ms"]:.1f}ms',   # <--- NEU
            "max":     f'{snap["lat_max_ms"]:.1f}ms',
            "put_rt":  f'{snap["put_rt_ms_avg"]:.1f}ms',
            "get_rt":  f'{snap["get_rt_ms_avg"]:.1f}ms',
            "err":     f'{snap["put_errors"]}/{snap["get_errors"]}/{snap["get_timeouts"]}',
        })
    headers = [
        "Queue","size","inflt","put/s","get/s","put/min","get/min",
        "lat_avg","p95","p99","max","put_rt","get_rt","err"  # p99 im Header
    ]
    return _format_table(rows, headers) if rows else "(keine Queues registriert)"


def queue_maybe_log():
    if not METRICS_ENABLED:
        return
    now = _now()
    last = _global_tick["last_table_log"]
    if now - last < METRICS_TABLE_EVERY_S:
        return
    _global_tick["last_table_log"] = now
    try:
        tbl = format_all_queues_table()
        logging.getLogger(METRICS_LOGGER_NAME).info("\n%s", tbl)
    except Exception:
        pass

# =============================================================================
# Standard‑Metriken (bereits vorhanden + bereinigt)
# =============================================================================

# 1) Cyclomatic Complexity
def cyclomatic_complexity(code_str, context=None, **kwargs):
    try:
        import radon.complexity as radon_cc
        result = radon_cc.cc_visit(code_str)
        return [dict(name=o.name, complexity=o.complexity, lineno=o.lineno) for o in result]
    except Exception as ex:
        return {"error": str(ex)}

register_metric("code.cyclomatic_complexity", partial(cyclomatic_complexity),
                group="code_quality",
                description="Zyklomatische Komplexität eines Python‑Code‑Strings")

# 2) Mutation Testing (Stub)
def mutation_testing_score(target_path, context=None, **kwargs):
    return {"mutation_score": None, "detail": "Integration mit mutmut/pytest erforderlich"}

register_metric("test.mutation_score", partial(mutation_testing_score),
                group="test_metrics",
                description="Mutation‑Score (Stub)")

# 3) Duplikationsrate
def duplication_rate(code_str, context=None, **kwargs):
    lines = [l.strip() for l in code_str.splitlines() if l.strip()]
    counts = Counter(lines)
    duplicate_lines = sum(1 for _, v in counts.items() if v > 1)
    total_lines = len(lines)
    ratio = (duplicate_lines / total_lines) if total_lines > 0 else 0.0
    return {"duplicate_lines": duplicate_lines, "total_lines": total_lines, "duplication_rate": ratio}

register_metric("code.duplication_rate", partial(duplication_rate),
                group="code_quality",
                description="Einfache Duplikationsrate (zeilenweise)")

# 4) Execution time
def measure_execution_time(callable_obj, *args, context=None, **kwargs):
    t0 = time.perf_counter()
    callable_obj(*args, **kwargs)
    t1 = time.perf_counter()
    return {"execution_time_sec": t1 - t0}

register_metric("perf.execution_time", partial(measure_execution_time),
                group="performance",
                description="Ausführungszeit einer Funktion/Coroutine")

# 5) Memory usage
def measure_memory_usage(callable_obj, *args, context=None, **kwargs):
    gc.collect()
    tracemalloc.start()
    callable_obj(*args, **kwargs)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"memory_current_bytes": current, "memory_peak_bytes": peak}

register_metric("perf.memory_usage", partial(measure_memory_usage),
                group="performance",
                description="Speicherverbrauch einer Funktion")

# 6) CPU‑Usage
def cpu_usage(context=None, **kwargs):
    try:
        import psutil
        return {"cpu_percent": psutil.cpu_percent(interval=0.2)}
    except Exception as ex:
        return {"error": str(ex)}

register_metric("perf.cpu_usage", partial(cpu_usage),
                group="performance",
                description="Aktuelle CPU‑Auslastung")

# 7) IO‑Zeitanteile (Stub)
def io_time_ratios(context=None, **kwargs):
    return {"io_read_time": None, "io_write_time": None, "detail": "OS‑Integration (psutil/tracing) nötig"}

register_metric("perf.io_time_ratios", partial(io_time_ratios),
                group="performance",
                description="I/O‑Zeitanteile (Stub)")

# 8) Queue snapshot (new, detailed metric)
def queue_snapshot_metric(queue_obj, context=None, **kwargs):
    return queue_snapshot(queue_obj)

register_metric("runtime.queue_snapshot", partial(queue_snapshot_metric),
                group="runtime",
                description="Vollständiger Queue‑Snapshot (size, throughput, latency, runtime)")

# 9) Event/state counting (simple)
def event_state_metrics(event_list, context=None, **kwargs):
    counter = Counter([e.get("event_type", "unknown") for e in event_list])
    return dict(counter)

register_metric("runtime.event_state_metrics", partial(event_state_metrics),
                group="runtime",
                description="Zählt Events/States nach Typ")

# 10) QA (Stub)
def static_analysis_metrics(code_str, context=None, **kwargs):
    return {"lint_errors": None, "detail": "Integration mit pylint/flake8 erforderlich"}

register_metric("qa.static_analysis", partial(static_analysis_metrics),
                group="qa",
                description="Ergebnisse statischer Analyse (Stub)")


register_metric(
                "runtime.queue_health",
                partial(queue_health),
                group="runtime",
                description="Backpressure-Score, Lag-Rate, Heuristik-Schweregrad")



# Automated execution of all registered metrics
def auto_analyze_all_metrics(target, context=None):
    results = {}
    for name in list_metrics():
        m = get_metric(name)
        custom_context = dict(context) if context else {}
        custom_context["target"] = target
        try:
            results[name] = m(target, context=custom_context)
        except Exception as ex:
            results[name] = {"error": str(ex)}
    return results

register_metric("automation.auto_analyze_all", partial(auto_analyze_all_metrics),
                group="automation",
                description="Führt alle registrierten Metriken auf ein Ziel aus")

# Public API
metrics_api = {
    "register_metric": register_metric,
    "unregister_metric": unregister_metric,
    "list_metrics": list_metrics,
    "get_metric": get_metric,
    "metric_info": metric_info,
    "run_metric": run_metric,
    "run_metrics_batch": run_metrics_batch,
    "auto_analyze_all": auto_analyze_all_metrics,
    "build_context": build_metrics_context,
    # Queue‑API
    "queue_register": queue_register,
    "queue_track_put": queue_track_put,
    "queue_track_get": queue_track_get,
    "queue_mark_put_error": queue_mark_put_error,
    "queue_mark_get_error": queue_mark_get_error,
    "queue_snapshot": queue_snapshot,
    "format_all_queues_table": format_all_queues_table,
    "queue_maybe_log": queue_maybe_log,
}
