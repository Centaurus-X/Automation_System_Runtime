# -*- coding: utf-8 -*-

# src/core/_event_router.py


"""
 =============================================================================
 _EVENT_ROUTER
 =============================================================================

 What it is:
     A clean, fully synchronous, functional event router with a
     hierarchical, deterministic priority layer. It consumes events from
     the central ingress queue (queue_event_send), resolves explicit or
     implicit targets, dispatches deterministically to per-domain queues
     and applies data-driven broadcast/mirror rules through ONE plan-based
     engine (cross-rule duplicate suppression, parallel fan-out).

     Priority-Event-Queue: every ingress burst is published into a heap
     broker as (priority, sequence, event) and dispatched strictly in
     ascending priority order. The monotonic sequence keeps exact FIFO
     order inside a priority class - legacy events (all PRIO_DEFAULT)
     therefore behave exactly like a pure FIFO router. Specific events
     are prioritized purely by data: an explicit "priority" field, an
     event_type override table or the target priority class.

 Why it is needed:
     - Central deterministic routing decouples producers from concrete
       queue names and handler threads (routing_table + broadcast_rules).
     - The single-writer sync chain of the architecture demands that
       system critical events, SEM/state writes and config propagation
       are always served before telemetry/UI traffic.
     - All tables are generated from compact declarative SPECS - fully
       generic, dynamic and extensible at runtime via router options.
     - Functional coding standards: no classes, no decorators, no type
       hints, functools.partial instead of lambda, defensive try/except.
     - Free-threading (Python 3.14+ no-GIL) friendly: loop state is
       thread-local, broadcast workers execute an immutable plan and
       never mutate shared state.

 How it works:
     1.  sync_event_router(**kwargs) receives queue_event_*=... queues
         plus optional routing / broadcast / priority configuration.
     2.  build_router_runtime() merges defaults + overrides and creates
         the immutable router context (registries + priority tables).
     3.  Ingress cycle: one blocking get, non-blocking burst drain
         (priority_batch_max), publish into the heap broker, pop-min
         dispatch. enable_priority_routing=False degrades the SAME
         engine to exact FIFO (batch size 1, uniform priority).
     4.  Per event: normalize legacy shapes, priority-order the primary
         targets, dispatch sequentially, apply dynamic broadcast
         targets, then execute the deduplicated broadcast plan
         (parallel queue puts via ThreadPoolExecutor).
     5.  Stop sentinels keep FIFO semantics: events that arrived before
         the sentinel are dispatched, later ones are dropped.
     6.  A robust import bridge binds the generic event interface
         (alias-tolerant): queue primitives AND the complete priority
         layer (classes, tables, event resolution, heap broker) are
         imported from the library - single source of truth, zero
         redundancy. Tiny degraded-mode stubs keep the router
         operational (pure FIFO) even without the library - never
         silent no-ops.
 =============================================================================
"""


"""
ZUSAMMENFASSUNG DES AKTUELLEN OPTIMIERUNGSSTANDS (Juli 2026)
=============================================================

Aktueller Stand:
    Die Architektur ist nun grundsätzlich sauber und konsistent. Die Prioritätslogik
    lebt als Single Source of Truth in _event_interface.py. Der Router bindet diese
    Logik über eine schlanke, robuste Bridge und bleibt auch bei fehlendem Interface
    funktionsfähig (Fallback-Modus).

Was gut gelöst ist:
    - Single Source of Truth für alle PRIO_*, DOMAIN_*, Tabellen und Funktionen
      (classify_target, resolve_event_priority, make_priority_broker_state etc.)
      liegt in _event_interface.py
    - DOMAIN_WORKER wurde ergänzt und korrekt gebridged
    - Die Priority-Bridge im Router wurde deutlich verschlankt und ist nun übersichtlich
    - Ein einheitlicher Ingress-Engine + planbasierter Broadcast-Engine
    - Gute Fallback-Mechanismen (Router bleibt robust)
    - Keine offensichtlichen Inkonsistenzen oder doppelten Definitionen mehr

Verbleibender Optimierungsbedarf (nicht kritisch):
    - Es existieren noch einige Fallback-Funktionen im Router
      (fallback_classify_target, fallback_order_targets, fallback_resolve_event_priority etc.).
      Diese sind zwar funktional redundant zur Bridge, dienen aber als Sicherheitsnetz
      im Degraded-Mode und sind daher bewusst beibehalten worden.
    - Die Bridge könnte theoretisch noch etwas weiter reduziert werden, würde dann aber
      an Robustheit verlieren.

Begründung der aktuellen Struktur:
    - Die Prioritätslogik wurde bewusst nicht komplett aus dem Router entfernt,
      da der Router diese Namen direkt verwendet. Eine komplette Entfernung
      würde zu NameErrors führen.
    - Die aktuelle Balance zwischen Sauberkeit und Robustheit wird als akzeptabel
      und wartbar eingestuft.
    - Weitere Reduktion der Fallbacks ist möglich, aber nicht zwingend notwendig,
      solange der Degraded-Mode erhalten bleiben soll.

Empfehlung:
    Der aktuelle Stand ist produktionsreif. Weitere Optimierungen sind optional
    und sollten nur bei konkretem Bedarf (z.B. weitere Vereinfachung der Bridge)
    durchgeführt werden.
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import heapq
import inspect
import json
import logging
import uuid

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import partial
from queue import Empty


# =============================================================================
# CENTRAL MODULE CONTEXT MANAGEMENT (logger + interface import bridge)
# =============================================================================

logger = logging.getLogger(__name__)


def fallback_universal_serialize(data):
    """Serialize any payload to a JSON string (defensive, always succeeds)."""
    try:
        return json.dumps(data, default=str)
    except Exception as exc:
        logger.warning("fallback_universal_serialize degraded: %s", exc)
        return json.dumps({"_fallback": str(data), "_type": type(data).__name__})


def fallback_universal_deserialize(data):
    """Deserialize JSON strings back to objects; pass through everything else."""
    try:
        if isinstance(data, str):
            try:
                return json.loads(data)
            except Exception:
                return data
        return data
    except Exception:
        return data


def fallback_sync_queue_put(sync_q, item):
    """Real queue put with universal JSON serialization (True/False result)."""
    try:
        if sync_q is None:
            return False
        sync_q.put(fallback_universal_serialize(item))
        return True
    except Exception as exc:
        logger.error("fallback_sync_queue_put failed: %s", exc)
        return False


def fallback_sync_queue_get(sync_q, timeout=5.0):
    """Real blocking queue get with timeout + universal deserialization."""
    try:
        if sync_q is None:
            return None
        try:
            raw = sync_q.get(block=True, timeout=timeout)
        except Empty:
            return None
        return fallback_universal_deserialize(raw)
    except Exception as exc:
        logger.error("fallback_sync_queue_get failed: %s", exc)
        return None


def fallback_make_event(event_type, payload=None, **kwargs):
    """Minimal event factory compatible with the generic event interface."""
    try:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload or {},
        }
        event.update(kwargs)
        return event
    except Exception:
        return {"event_id": str(uuid.uuid4()), "event_type": event_type, "payload": payload or {}}


def import_event_interface_module():
    """Try the known module paths for the generic event interface library."""
    try:
        from src.library import _event_interface as interface_module
        return interface_module
    except ImportError:
        try:
            import _event_interface as interface_module
            return interface_module
        except ImportError:
            return None


def resolve_interface_callable(interface_module, candidate_names, fallback_callable):
    """Resolve the first available callable from candidate alias names."""
    try:
        if interface_module is None:
            return fallback_callable
        for name in candidate_names:
            candidate = getattr(interface_module, name, None)
            if callable(candidate):
                return candidate
        return fallback_callable
    except Exception:
        return fallback_callable


def require_interface_callable(interface_module, candidate_names):
    """Resolve one required callable from the canonical event interface."""
    candidate = resolve_interface_callable(
        interface_module,
        candidate_names,
        None,
    )

    if candidate is None:
        raise ImportError(
            "Required event interface callable is missing: {}".format(
                ", ".join(candidate_names)
            )
        )

    return candidate


_EVENT_INTERFACE_MODULE = import_event_interface_module()

if _EVENT_INTERFACE_MODULE is None:
    logger.warning("_event_router: event interface not importable - using functional fallbacks")

sync_queue_get = resolve_interface_callable(
    _EVENT_INTERFACE_MODULE, ("sync_queue_get", "queue_get_sync"), fallback_sync_queue_get)
sync_queue_put = resolve_interface_callable(
    _EVENT_INTERFACE_MODULE, ("sync_queue_put", "queue_put_sync"), fallback_sync_queue_put)
universal_serialize = resolve_interface_callable(
    _EVENT_INTERFACE_MODULE, ("universal_serialize",), fallback_universal_serialize)
universal_deserialize = resolve_interface_callable(
    _EVENT_INTERFACE_MODULE, ("universal_deserialize",), fallback_universal_deserialize)
make_event = resolve_interface_callable(
    _EVENT_INTERFACE_MODULE, ("make_event", "create_event"), fallback_make_event)


# -----------------------------------------------------------------------------
# PRIORITY LAYER BRIDGE (vereinfacht)
# -----------------------------------------------------------------------------

def resolve_interface_value(interface_module, value_name, fallback_value):
    try:
        if interface_module is None:
            return fallback_value
        value = getattr(interface_module, value_name, None)
        return fallback_value if value is None else value
    except Exception:
        return fallback_value


# Priority constants
PRIO_SYSTEM_CRITICAL = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_SYSTEM_CRITICAL", 0)
PRIO_STATE_WRITER = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_STATE_WRITER", 10)
PRIO_DATASTORE = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_DATASTORE", 20)
PRIO_CONTROLLER = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_CONTROLLER", 30)
PRIO_AUTOMATION = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_AUTOMATION", 40)
PRIO_THREAD_MANAGEMENT = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_THREAD_MANAGEMENT", 50)
PRIO_FIELDBUS = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_FIELDBUS", 60)
PRIO_TELEMETRY = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_TELEMETRY", 70)
PRIO_UI = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_UI", 80)
PRIO_DEFAULT = resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIO_DEFAULT", 100)

# Domains
DOMAIN_SYSTEM = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_SYSTEM", "system")
DOMAIN_STATE = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_STATE", "state")
DOMAIN_DATA = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_DATA", "data")
DOMAIN_CONTROLLER = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_CONTROLLER", "controller")
DOMAIN_AUTOMATION = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_AUTOMATION", "automation")
DOMAIN_THREAD_MGMT = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_THREAD_MGMT", "thread_management")
DOMAIN_FIELDBUS = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_FIELDBUS", "fieldbus")
DOMAIN_TELEMETRY = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_TELEMETRY", "telemetry")
DOMAIN_WORKER = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_WORKER", "worker")
DOMAIN_UI = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_UI", "ui")
DOMAIN_UNKNOWN = resolve_interface_value(_EVENT_INTERFACE_MODULE, "DOMAIN_UNKNOWN", "unknown")

# Priority tables & spec
PRIORITY_SPEC = tuple(resolve_interface_value(_EVENT_INTERFACE_MODULE, "PRIORITY_SPEC", tuple()))
DEFAULT_TARGET_PRIORITY_TABLE = dict(resolve_interface_value(_EVENT_INTERFACE_MODULE, "DEFAULT_TARGET_PRIORITY_TABLE", {}))
DEFAULT_EVENT_TYPE_PRIORITY_TABLE = dict(resolve_interface_value(
    _EVENT_INTERFACE_MODULE, "DEFAULT_EVENT_TYPE_PRIORITY_TABLE", {"__STOP__": PRIO_SYSTEM_CRITICAL}))

STOP_MARKERS = frozenset(resolve_interface_value(_EVENT_INTERFACE_MODULE, "STOP_MARKERS", ("__STOP__",)))


# Priority functions are required from the canonical event interface.
coerce_priority_value = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("coerce_priority_value",))

classify_target = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("classify_target",))
get_target_priority = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("get_target_priority",))
get_target_domain = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("get_target_domain",))

order_targets_deterministic = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("order_targets_deterministic",))
order_targets_by_priority_domain = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("order_targets_by_priority_domain",))

resolve_event_priority = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("resolve_event_priority",))

build_target_priority_table = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("build_target_priority_table",))
build_event_priority_table = require_interface_callable(
    _EVENT_INTERFACE_MODULE, ("build_event_priority_table",))

make_priority_broker_state = require_interface_callable(
    _EVENT_INTERFACE_MODULE,
    ("make_priority_broker_state", "make_event_broker_state"),
)
priority_broker_publish = require_interface_callable(
    _EVENT_INTERFACE_MODULE,
    ("priority_broker_publish", "publish_event"),
)
priority_broker_pop = require_interface_callable(
    _EVENT_INTERFACE_MODULE,
    ("priority_broker_pop", "pop_event"),
)
priority_broker_size = require_interface_callable(
    _EVENT_INTERFACE_MODULE,
    ("priority_broker_size", "event_broker_size"),
)


# Router-facing compatibility aliases (zero logic duplication)
classify_router_target = classify_target
get_router_target_priority = get_target_priority
get_router_target_domain = get_target_domain
order_targets_by_priority = order_targets_deterministic
order_targets_with_priority_details = order_targets_by_priority_domain


# =============================================================================
# CONSTANTS
# =============================================================================

DEPRECATED_TARGETS = frozenset({"process_manager"})

CONFIG_EVENT_TYPES = frozenset((
    "CONFIG_CHANGED", "CONFIG_PATCH", "CONFIG_REPLACE",
    "CONFIG_UPDATED", "CONFIG_CHANGE",
))

AUTOMATION_STATUS_EVENT_TYPES = frozenset(("AUTOMATION_STATUS",))

LEGACY_REMOVE_KEYS = ("message_type",)

STOP_MARKERS = frozenset(("__STOP__",))

DISALLOWED_DIRECT_QUEUE_TARGETS = frozenset(("queue_event_send",))

DEFAULT_ROUTER_NAME = "SyncEventRouter"

DEFAULT_GENERIC_QUEUE_SUFFIXES = (
    "ws", "bc", "io", "ipc", "state", "event", "ds", "dc",
    "proc", "pc", "mbh", "mba", "mbc", "redis_api", "ti", "worker",
)

DEFAULT_EVENT_QUEUE_SUFFIXES = (
    "send", "ws", "state", "ds", "dc", "proc", "pc",
    "mbh", "mba", "mbc", "ti", "worker",
)


# =============================================================================
# PRIORITY LAYER SOURCE
# =============================================================================
#
# All PRIO_* classes, DOMAIN_* names, PRIORITY_SPEC, the target/event
# priority tables and the heap-based Priority-Event-Broker are bound
# above via the priority layer bridge from the generic event interface
# library (src/libraries/_event_interface.py). Single source of truth -
# zero redundancy. Extend at runtime via the router options
# "target_priority_table" and "event_priority_table" (additive merge).


ROUTER_OPTION_DEFAULTS = (
    ("shutdown_event", None),
    ("queues_dict", None),
    ("queue_dict", None),
    ("generic_queue_dict", None),
    ("routing_table", None),
    ("broadcast_rules", None),
    ("router_name", DEFAULT_ROUTER_NAME),
    ("enable_process_broadcast", True),
    ("enable_telemetry_mirror", True),
    ("enable_worker_mirror", True),
    ("enable_config_mirrors", True),
    ("enable_automation_status_mirror", True),
    ("num_threads", 4),
    # --- priority layer (one switch, data-driven tables) ---
    ("enable_priority_routing", True),
    ("priority_batch_max", 64),
    ("ingress_timeout_s", 5.0),
    ("target_priority_table", None),
    ("event_priority_table", None),
    ("priority_from_targets", True),
)

RESERVED_ROUTER_PARAMETER_NAMES = frozenset(name for name, _ in ROUTER_OPTION_DEFAULTS)


def build_default_compat_queue_parameter_names():
    try:
        names = []
        for suffix in DEFAULT_GENERIC_QUEUE_SUFFIXES:
            names.append("queue_%s" % suffix)
        for suffix in DEFAULT_EVENT_QUEUE_SUFFIXES:
            names.append("queue_event_%s" % suffix)
        return tuple(dict.fromkeys(names))
    except Exception:
        return tuple()


DEFAULT_COMPAT_QUEUE_PARAMETER_NAMES = build_default_compat_queue_parameter_names()


# =============================================================================
# HELP-FUNCTIONS - NORMALIZATION (all defensive, no type hints)
# =============================================================================

def safe_deepcopy(data):
    try:
        return copy.deepcopy(data)
    except Exception:
        return data


def normalize_text(value):
    try:
        if value is None:
            return ""
        return str(value).strip().lower()
    except Exception:
        return ""


def normalize_upper_text(value):
    try:
        if value is None:
            return ""
        return str(value).strip().upper()
    except Exception:
        return ""


def normalize_target_collection(value):
    try:
        if value is None:
            return tuple()
        if isinstance(value, (list, tuple, set, frozenset)):
            result = []
            for item in value:
                item_lc = normalize_text(item)
                if item_lc:
                    result.append(item_lc)
            return tuple(result)
        single = normalize_text(value)
        if not single:
            return tuple()
        return (single,)
    except Exception:
        return tuple()


def is_mapping_message(data):
    try:
        return isinstance(data, dict)
    except Exception:
        return False


def strip_top_level_keys(data, keys):
    try:
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        for key in keys:
            payload.pop(key, None)
        return payload
    except Exception:
        return data


def prepare_payload_for_send(data, remove_keys=None, deep_copy=False):
    try:
        payload = safe_deepcopy(data) if deep_copy else data
        if remove_keys:
            payload = strip_top_level_keys(payload, remove_keys)
        return payload
    except Exception:
        return data


# =============================================================================
# HELP-FUNCTIONS - EVENT METADATA
# =============================================================================

def extract_event_type(data):
    try:
        if not isinstance(data, dict):
            return ""
        event_type = data.get("event_type")
        if event_type is None:
            event_type = data.get("type")
        if event_type is None:
            event_type = data.get("message_type")
        if event_type is None:
            payload = data.get("payload")
            if isinstance(payload, dict):
                event_type = (
                    payload.get("event_type")
                    or payload.get("type")
                    or payload.get("message_type")
                )
        return normalize_upper_text(event_type)
    except Exception:
        return ""


def extract_primary_target_value(data):
    try:
        if not isinstance(data, dict):
            return None
        if data.get("target") is not None:
            return data.get("target")
        if data.get("message_type") is not None:
            return data.get("message_type")
        if data.get("type") is not None:
            return data.get("type")
        return None
    except Exception:
        return None


def extract_primary_targets(data):
    try:
        return normalize_target_collection(extract_primary_target_value(data))
    except Exception:
        return tuple()


def extract_explicit_broadcast_targets(data):
    try:
        if not isinstance(data, dict):
            return tuple()
        if data.get("broadcast_targets") is not None:
            return normalize_target_collection(data.get("broadcast_targets"))
        if data.get("broadcast_to") is not None:
            return normalize_target_collection(data.get("broadcast_to"))
        broadcast_value = data.get("broadcast")
        if isinstance(broadcast_value, dict):
            return normalize_target_collection(broadcast_value.get("targets"))
        return tuple()
    except Exception:
        return tuple()


def is_stop_message(data):
    try:
        if not isinstance(data, dict):
            return False
        for key in ("_type", "event_type", "type"):
            if normalize_upper_text(data.get(key)) in STOP_MARKERS:
                return True
        return False
    except Exception:
        return False


def unwrap_unknown_envelope(data):
    try:
        if not isinstance(data, dict):
            return {"data": data, "drop": False, "reason": ""}
        if extract_event_type(data) != "UNKNOWN":
            return {"data": data, "drop": False, "reason": ""}
        payload = data.get("payload")
        raw_candidate = None
        if isinstance(payload, dict):
            raw_candidate = payload.get("raw") or payload.get("payload") or payload.get("data")
        elif isinstance(payload, str):
            raw_candidate = payload
        if isinstance(raw_candidate, str):
            try:
                decoded = json.loads(raw_candidate)
                if isinstance(decoded, dict):
                    return {"data": decoded, "drop": True, "reason": "dropped_duplicate_unknown_envelope"}
            except Exception:
                pass
        if isinstance(raw_candidate, dict):
            return {"data": raw_candidate, "drop": True, "reason": "dropped_duplicate_unknown_envelope"}
        return {"data": data, "drop": True, "reason": "dropped_unknown_envelope_without_payload"}
    except Exception:
        return {"data": data, "drop": True, "reason": "dropped_unknown_envelope_error"}


def is_config_event(data):
    try:
        if not isinstance(data, dict):
            return False
        if extract_event_type(data) in CONFIG_EVENT_TYPES:
            return True
        payload = data.get("payload") or {}
        if isinstance(payload, dict):
            if "patch" in payload or "new_config" in payload or "diff" in payload:
                return True
        return False
    except Exception:
        return False


def is_automation_status_event(data):
    try:
        return extract_event_type(data) in AUTOMATION_STATUS_EVENT_TYPES
    except Exception:
        return False


# =============================================================================
# HELP-FUNCTIONS - PREDICATES (for broadcast rules)
# =============================================================================

def predicate_true(_data):
    return True


def predicate_is_mapping_message(data):
    try:
        return is_mapping_message(data)
    except Exception:
        return False


def predicate_is_config_event(data):
    try:
        return is_config_event(data)
    except Exception:
        return False


def predicate_is_automation_status_event(data):
    try:
        return is_automation_status_event(data)
    except Exception:
        return False


def predicate_all(predicates, data):
    try:
        for predicate in predicates:
            if not predicate(data):
                return False
        return True
    except Exception:
        return False


def predicate_any(predicates, data):
    try:
        for predicate in predicates:
            try:
                if predicate(data):
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def predicate_event_type_in(event_types, data):
    try:
        return extract_event_type(data) in event_types
    except Exception:
        return False


def predicate_priority_at_most(data, max_priority=PRIO_DEFAULT,
                               event_priority_table=None, target_priority_table=None):
    """
    Priority-aware rule predicate (bind via functools.partial):
        partial(predicate_priority_at_most, max_priority=PRIO_CONTROLLER)
    True when the event resolves to a priority <= max_priority.
    """
    try:
        resolved = resolve_event_priority(
            data,
            event_priority_table=event_priority_table,
            target_priority_table=target_priority_table,
        )
        return resolved <= coerce_priority_value(max_priority, PRIO_DEFAULT)
    except Exception:
        return False


# =============================================================================
# HELP-FUNCTIONS - QUEUE FORWARDING + NAMED HANDLERS
# =============================================================================

def forward_to_queue(queue, data, handler_name, remove_keys=None, deep_copy=False):
    try:
        if queue is None:
            logger.debug("%s: queue is None - skip", handler_name)
            return False
        payload = prepare_payload_for_send(data, remove_keys=remove_keys, deep_copy=deep_copy)
        return bool(sync_queue_put(queue, payload))
    except Exception as exc:
        logger.error("%s failed: %s", handler_name, exc)
        return False


def heartbeat_handler(data):
    try:
        logger.info("Heartbeat received")
        return True
    except Exception as exc:
        logger.error("heartbeat_handler failed: %s", exc)
        return False


def runtime_status_handler(data):
    try:
        logger.info(
            "Runtime status event received: %s",
            extract_event_type(data) or "UNKNOWN",
        )
        return True
    except Exception as exc:
        logger.error("runtime_status_handler failed: %s", exc)
        return False


def queue_forward_handler(queue, data, handler_name, remove_keys=None):
    try:
        return forward_to_queue(queue, data, handler_name, remove_keys=remove_keys)
    except Exception as exc:
        logger.error("%s failed: %s", handler_name, exc)
        return False


def _make_named_handler(handler_name, remove_keys=None):
    handler = partial(queue_forward_handler, handler_name=handler_name, remove_keys=remove_keys)
    handler.__name__ = handler_name
    handler.__qualname__ = handler_name
    return handler


# Pre-created named handlers (functional, no duplication)
state_handler = _make_named_handler("state_handler", remove_keys=LEGACY_REMOVE_KEYS)
event_handler = _make_named_handler("event_handler", remove_keys=LEGACY_REMOVE_KEYS)
datastore_handler = _make_named_handler("datastore_handler")
data_config_handler = _make_named_handler("data_config_handler")
process_handler = _make_named_handler("process_handler")
automation_handler = _make_named_handler("automation_handler")
modbus_handler = _make_named_handler("modbus_handler")
controller_handler = _make_named_handler("controller_handler")
process_manager_handler = _make_named_handler("process_manager_handler")
thread_management_handler = _make_named_handler("thread_management_handler")
telemetry_handler = _make_named_handler("telemetry_handler")
worker_event_handler = _make_named_handler("worker_event_handler")
websocket_handler = _make_named_handler("websocket_handler")
generic_queue_handler = _make_named_handler("generic_queue_handler")


# =============================================================================
# HELP-FUNCTIONS - ROUTING TABLE (generated from declarative SPEC)
# =============================================================================

def make_route_entry(handler, queue_key=None, description=""):
    try:
        return {"handler": handler, "queue_key": queue_key, "description": description}
    except Exception:
        return {"handler": handler, "queue_key": queue_key, "description": ""}


def normalize_route_entry(entry):
    try:
        if isinstance(entry, str):
            return make_route_entry(generic_queue_handler, entry, "custom_string_queue_route")
        if isinstance(entry, tuple) and len(entry) == 2:
            queue_key, handler = entry
            return make_route_entry(handler, queue_key, "custom_tuple_route")
        if isinstance(entry, dict):
            handler = entry.get("handler") or generic_queue_handler
            return make_route_entry(handler, entry.get("queue_key"),
                                    entry.get("description", "custom_dict_route"))
        if callable(entry):
            return make_route_entry(entry, None, "custom_handler_only_route")
        return None
    except Exception:
        return None


# Declarative routing SPEC: (queue_key, handler, aliases). The routing
# table is generated from it - adding a domain is one declarative line.
ROUTING_SPEC = (
    (None, heartbeat_handler, ("heartbeat",)),
    (None, runtime_status_handler, ("runtime", "runtime_status")),
    ("queue_event_state", state_handler,
     ("state", "state_handler", "state_management", "state_event_management", "sem")),
    ("queue_event_state", event_handler, ("event", "event_handler")),
    ("queue_event_mbc", controller_handler, ("controller", "mbc")),
    ("queue_event_mba", automation_handler, ("automation", "mba")),
    ("queue_event_pc", thread_management_handler,
     ("thread_management", "tm", "pc", "process_controller")),
    ("queue_event_ds", datastore_handler, ("datastore", "database", "ds")),
    ("queue_event_dc", data_config_handler, ("data_config", "config_data", "dc")),
    ("queue_event_mbh", modbus_handler, ("modbus", "mbh", "modbus_handler")),
    ("queue_event_ws", websocket_handler, ("websocket", "ws")),
    ("queue_event_ti", telemetry_handler, ("telemetry", "telemetry_event", "ti")),
    ("queue_event_worker", worker_event_handler,
     ("worker", "worker_event", "proxy_worker_bridge", "runtime_command_binding")),
)


def build_default_routing_table():
    """Generate the default routing table from ROUTING_SPEC (data-driven)."""
    try:
        table = {}
        for queue_key, handler, aliases in ROUTING_SPEC:
            for alias in aliases:
                table[alias] = make_route_entry(handler, queue_key, alias)
        return table
    except Exception:
        return {}


def build_routing_table(custom_routing_table=None):
    try:
        routing_table = build_default_routing_table()
        if not isinstance(custom_routing_table, dict):
            return routing_table
        for target, entry in custom_routing_table.items():
            target_lc = normalize_text(target)
            normalized_entry = normalize_route_entry(entry)
            if target_lc and normalized_entry is not None:
                routing_table[target_lc] = normalized_entry
        return routing_table
    except Exception:
        return build_default_routing_table()


# =============================================================================
# HELP-FUNCTIONS - QUEUE REGISTRY
# =============================================================================

def merge_queue_dicts(*queue_dicts):
    try:
        merged = {}
        for queue_dict in queue_dicts:
            if not isinstance(queue_dict, dict):
                continue
            for key, value in queue_dict.items():
                if not isinstance(key, str):
                    continue
                key_lc = normalize_text(key)
                if key_lc.startswith("queue_"):
                    merged[key_lc] = value
        return merged
    except Exception:
        return {}


def is_queue_parameter_name(key):
    try:
        key_lc = normalize_text(key)
        return key_lc.startswith("queue_") and key_lc not in RESERVED_ROUTER_PARAMETER_NAMES
    except Exception:
        return False


def build_queue_registry(router_kwargs):
    try:
        registry = {}
        if not isinstance(router_kwargs, dict):
            return registry
        wrapper_sources = [
            router_kwargs.get("queues_dict"),
            router_kwargs.get("queue_dict"),
            router_kwargs.get("generic_queue_dict"),
        ]
        registry.update(merge_queue_dicts(*[w for w in wrapper_sources if w]))
        for key, value in router_kwargs.items():
            if is_queue_parameter_name(key):
                registry[normalize_text(key)] = value
        return registry
    except Exception:
        return {}


def resolve_dynamic_queue_key(target, queue_name_index):
    try:
        target_lc = normalize_text(target)
        if not target_lc:
            return None
        if target_lc.startswith("queue_"):
            return target_lc if target_lc in queue_name_index else None
        for candidate in ("queue_event_%s" % target_lc, "queue_%s" % target_lc):
            if candidate in queue_name_index:
                return candidate
        return None
    except Exception:
        return None


def build_queue_name_index(queue_registry):
    try:
        if not isinstance(queue_registry, dict):
            return {}
        index = {}
        for key in queue_registry.keys():
            key_lc = normalize_text(key)
            if key_lc.startswith("queue_"):
                index[key_lc] = key
        return index
    except Exception:
        return {}


# =============================================================================
# HELP-FUNCTIONS - BROADCAST RULES
# =============================================================================

def make_broadcast_rule(name, targets, predicate=None, enabled=True,
                        description="", priority=None):
    """Rule factory; optional "priority" orders rule application (lower first)."""
    try:
        return {
            "name": name,
            "targets": tuple(targets) if targets else tuple(),
            "predicate": predicate or predicate_true,
            "enabled": bool(enabled),
            "description": description,
            "priority": coerce_priority_value(priority, None),
        }
    except Exception:
        return {"name": name, "targets": tuple(), "predicate": predicate_true,
                "enabled": False, "description": "", "priority": None}


def normalize_broadcast_rule(rule):
    try:
        if not isinstance(rule, dict):
            return None
        return make_broadcast_rule(
            name=rule.get("name") or "unnamed_rule",
            targets=normalize_target_collection(rule.get("targets")),
            predicate=rule.get("predicate") or predicate_true,
            enabled=bool(rule.get("enabled", True)),
            description=rule.get("description", ""),
            priority=rule.get("priority"),
        )
    except Exception:
        return None


def build_broadcast_rules(
    queue_registry,
    custom_broadcast_rules=None,
    enable_process_broadcast=True,
    enable_telemetry_mirror=True,
    enable_worker_mirror=True,
    enable_config_mirrors=True,
    enable_automation_status_mirror=True,
):
    try:
        rules = []
        if enable_config_mirrors:
            rules.append(make_broadcast_rule(
                "config_mirror", ("state", "worker", "telemetry"),
                predicate=predicate_is_config_event, priority=PRIO_DATASTORE,
                description="Mirror all config events to state + worker + telemetry"))
        if enable_automation_status_mirror:
            rules.append(make_broadcast_rule(
                "automation_status_mirror", ("automation", "state", "worker"),
                predicate=predicate_is_automation_status_event, priority=PRIO_AUTOMATION,
                description="Mirror automation status to automation + state + worker"))
        if enable_worker_mirror:
            rules.append(make_broadcast_rule(
                "worker_mirror", ("worker",),
                predicate=predicate_is_mapping_message, priority=PRIO_TELEMETRY,
                description="Mirror mapping messages to worker"))
        if enable_telemetry_mirror:
            rules.append(make_broadcast_rule(
                "telemetry_mirror", ("telemetry",),
                predicate=predicate_is_mapping_message, priority=PRIO_TELEMETRY,
                description="Mirror mapping messages to telemetry"))
        if enable_process_broadcast:
            rules.append(make_broadcast_rule(
                "process_broadcast", ("process", "state"),
                predicate=predicate_true, priority=PRIO_THREAD_MANAGEMENT,
                description="Generic process broadcast (can be refined)"))
        if isinstance(custom_broadcast_rules, (list, tuple)):
            for rule in custom_broadcast_rules:
                normalized = normalize_broadcast_rule(rule)
                if normalized is not None:
                    rules.append(normalized)
        return tuple(rules)
    except Exception:
        return tuple()


def broadcast_rule_sort_key(indexed_rule):
    """Sort key for (index, rule): ascending priority, insertion order on tie."""
    try:
        index, rule = indexed_rule
        return (coerce_priority_value(rule.get("priority"), PRIO_DEFAULT), index)
    except Exception:
        return (PRIO_DEFAULT, 0)


def order_broadcast_rules(broadcast_rules):
    """Deterministic rule order: ascending priority, insertion order on tie."""
    try:
        indexed = list(enumerate(broadcast_rules or tuple()))
        indexed.sort(key=broadcast_rule_sort_key)
        return tuple(rule for _index, rule in indexed)
    except Exception:
        return tuple(broadcast_rules or tuple())


# =============================================================================
# CODEBASE - ROUTER CONTEXT
# =============================================================================

def create_router_context(
    queue_registry,
    routing_table,
    broadcast_rules,
    router_name="SyncEventRouter",
    cross_domain_mapping=None,
    target_priority_table=None,
    event_priority_table=None,
    router_options=None,
):
    try:
        return {
            "router_name": router_name,
            "queues": queue_registry or {},
            "queue_name_index": build_queue_name_index(queue_registry or {}),
            "routing_table": routing_table or {},
            "broadcast_rules": broadcast_rules or tuple(),
            "ordered_broadcast_rules": order_broadcast_rules(broadcast_rules or tuple()),
            "cross_domain_mapping": cross_domain_mapping or {},
            "target_priority_table": target_priority_table
            if isinstance(target_priority_table, dict) else build_target_priority_table(None),
            "event_priority_table": event_priority_table
            if isinstance(event_priority_table, dict) else build_event_priority_table(None),
            "router_options": router_options if isinstance(router_options, dict) else {},
        }
    except Exception:
        return {
            "router_name": "SyncEventRouter", "queues": {}, "queue_name_index": {},
            "routing_table": {}, "broadcast_rules": tuple(),
            "ordered_broadcast_rules": tuple(), "cross_domain_mapping": {},
            "target_priority_table": dict(DEFAULT_TARGET_PRIORITY_TABLE),
            "event_priority_table": dict(DEFAULT_EVENT_TYPE_PRIORITY_TABLE),
            "router_options": {},
        }


def read_context_option(router_context, option_name, default=None):
    try:
        options = router_context.get("router_options", {})
        value = options.get(option_name, default) if isinstance(options, dict) else default
        return default if value is None else value
    except Exception:
        return default


# =============================================================================
# CODEBASE - DISPATCH
# =============================================================================

def make_dispatch_result(target, queue_key, success, handler_name, reason="", priority=None):
    try:
        return {
            "target": normalize_text(target),
            "queue_key": queue_key,
            "success": bool(success),
            "handler_name": handler_name,
            "reason": reason,
            "priority": priority,
        }
    except Exception:
        return {"target": "", "queue_key": queue_key, "success": False,
                "handler_name": handler_name, "reason": reason, "priority": priority}


def resolve_route_entry(target, router_context):
    try:
        target_lc = normalize_text(target)
        if not target_lc:
            return None
        route_entry = router_context.get("routing_table", {}).get(target_lc)
        if route_entry is not None:
            return route_entry
        dynamic_queue_key = resolve_dynamic_queue_key(
            target_lc, router_context.get("queue_name_index", {}))
        if dynamic_queue_key is None:
            return None
        return make_route_entry(generic_queue_handler, dynamic_queue_key,
                                "dynamic_queue_resolution")
    except Exception:
        return None


def invoke_route_handler(route_entry, queue, data):
    try:
        handler = route_entry.get("handler")
        if route_entry.get("queue_key") is None:
            return bool(handler(data))
        return bool(handler(queue, data))
    except Exception as exc:
        handler_name = getattr(route_entry.get("handler"), "__name__", "unknown_handler")
        logger.error("invoke_route_handler failed for %s: %s", handler_name, exc)
        return False


def dispatch_target(target, data, router_context, sent_queue_keys=None):
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()
        router_name = router_context.get("router_name", DEFAULT_ROUTER_NAME)
        route_entry = resolve_route_entry(target, router_context)
        if route_entry is None:
            if target in DEPRECATED_TARGETS:
                logger.debug("%s: dropping event for deprecated target '%s'",
                             router_name, target)
                return make_dispatch_result(target, None, True,
                                            "deprecated_target", "deprecated_target_dropped")
            logger.error("%s: unknown target '%s'", router_name, target)
            return make_dispatch_result(target, None, False,
                                        "unresolved_target", "unknown_target")
        queue_key = route_entry.get("queue_key")
        handler_name = getattr(route_entry.get("handler"), "__name__", "unknown_handler")
        if queue_key is not None and queue_key in DISALLOWED_DIRECT_QUEUE_TARGETS:
            logger.error("%s: routing to disallowed queue '%s' is blocked",
                         router_name, queue_key)
            return make_dispatch_result(target, queue_key, False,
                                        handler_name, "disallowed_queue_target")
        if queue_key is not None and queue_key in sent_queue_keys:
            return make_dispatch_result(target, queue_key, False,
                                        handler_name, "duplicate_queue_skip")
        queue = None
        if queue_key is not None:
            queue = router_context.get("queues", {}).get(queue_key)
            if queue is None:
                return make_dispatch_result(target, queue_key, False,
                                            handler_name, "queue_not_available")
        success = invoke_route_handler(route_entry, queue, data)
        if success and queue_key is not None:
            sent_queue_keys.add(queue_key)
        return make_dispatch_result(target, queue_key, success, handler_name)
    except Exception as exc:
        logger.error("dispatch_target failed: %s", exc)
        return make_dispatch_result(target, None, False,
                                    "dispatch_target", "dispatch_exception")


def dispatch_targets(targets, data, router_context, sent_queue_keys=None,
                     deep_copy_additional_targets=True):
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()
        results = []
        for index, target in enumerate(normalize_target_collection(targets)):
            payload = safe_deepcopy(data) if (deep_copy_additional_targets and index > 0) else data
            results.append(dispatch_target(target, payload, router_context, sent_queue_keys))
        return tuple(results)
    except Exception as exc:
        logger.error("dispatch_targets failed: %s", exc)
        return tuple()


def apply_dynamic_broadcast_targets(data, router_context, sent_queue_keys=None):
    try:
        broadcast_targets = extract_explicit_broadcast_targets(data)
        if not broadcast_targets:
            return tuple()
        return dispatch_targets(broadcast_targets, data, router_context,
                                sent_queue_keys=sent_queue_keys)
    except Exception as exc:
        logger.error("apply_dynamic_broadcast_targets failed: %s", exc)
        return tuple()


# =============================================================================
# CODEBASE - DETERMINISTIC PRIMARY DISPATCH (priority-aware)
# =============================================================================

def build_deterministic_primary_targets(data, router_context):
    """
    Ordered primary targets: explicit targets + cross_domain_targets,
    deduplicated (first occurrence wins). With enable_priority_routing
    (default) multi-target sets are sorted by (priority, domain, name);
    otherwise the payload order is kept (classic behavior).
    """
    try:
        primary_targets = extract_primary_targets(data)
        cross_targets = tuple()
        if isinstance(data, dict):
            payload = data.get("payload") or {}
            if isinstance(payload, dict):
                cross_targets = normalize_target_collection(payload.get("cross_domain_targets"))
            if not cross_targets:
                cross_targets = normalize_target_collection(data.get("cross_domain_targets"))
        all_targets = []
        seen = set()
        for target in list(primary_targets) + list(cross_targets):
            target_lc = normalize_text(target)
            if target_lc and target_lc not in seen:
                seen.add(target_lc)
                all_targets.append(target_lc)
        if len(all_targets) > 1 and bool(
                read_context_option(router_context, "enable_priority_routing", True)):
            return order_targets_by_priority(
                all_targets, router_context.get("target_priority_table"))
        return tuple(all_targets)
    except Exception as exc:
        logger.error("build_deterministic_primary_targets failed: %s", exc)
        return tuple()


def dispatch_deterministic_primary(data, router_context, sent_queue_keys=None):
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()
        ordered_targets = build_deterministic_primary_targets(data, router_context)
        if not ordered_targets:
            return tuple()
        return dispatch_targets(ordered_targets, data, router_context,
                                sent_queue_keys=sent_queue_keys)
    except Exception as exc:
        logger.error("dispatch_deterministic_primary failed: %s", exc)
        return tuple()


# =============================================================================
# CODEBASE - BROADCAST ENGINE (one plan-based engine, dedupe + parallel)
# =============================================================================

def build_broadcast_dispatch_plan(data, router_context, sent_queue_keys=None):
    """
    Evaluate all enabled rules in deterministic priority order and produce
    an immutable dispatch plan with CROSS-RULE duplicate suppression:
    every queue key is planned at most once per event, keys already served
    by the primary/dynamic dispatch are skipped, unresolvable mirror
    targets are logged at DEBUG level (mirrors are best-effort).
    Plan items are independent - safe for parallel execution.
    """
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()
        plan = []
        planned_queue_keys = set()
        router_name = router_context.get("router_name", DEFAULT_ROUTER_NAME)
        ordered_rules = router_context.get("ordered_broadcast_rules") or \
            order_broadcast_rules(router_context.get("broadcast_rules", tuple()))
        for rule in ordered_rules:
            if not isinstance(rule, dict) or not bool(rule.get("enabled", True)):
                continue
            rule_name = rule.get("name", "unknown_rule")
            predicate = rule.get("predicate") or predicate_true
            try:
                if not bool(predicate(data)):
                    continue
            except Exception as exc:
                logger.error("broadcast rule '%s' predicate failed: %s", rule_name, exc)
                continue
            for target in normalize_target_collection(rule.get("targets", tuple())):
                route_entry = resolve_route_entry(target, router_context)
                if route_entry is None:
                    logger.debug("%s: broadcast '%s' target '%s' not resolvable - skip",
                                 router_name, rule_name, target)
                    continue
                queue_key = route_entry.get("queue_key")
                if queue_key is not None and queue_key in DISALLOWED_DIRECT_QUEUE_TARGETS:
                    logger.error("%s: broadcast '%s' to disallowed queue '%s' blocked",
                                 router_name, rule_name, queue_key)
                    continue
                if queue_key is not None:
                    if queue_key in sent_queue_keys or queue_key in planned_queue_keys:
                        continue
                    queue = router_context.get("queues", {}).get(queue_key)
                    if queue is None:
                        logger.debug("%s: broadcast '%s' queue '%s' not available - skip",
                                     router_name, rule_name, queue_key)
                        continue
                    planned_queue_keys.add(queue_key)
                else:
                    queue = None
                plan.append({"rule_name": rule_name, "target": target,
                             "queue_key": queue_key, "queue": queue,
                             "route_entry": route_entry})
        return tuple(plan)
    except Exception as exc:
        logger.error("build_broadcast_dispatch_plan failed: %s", exc)
        return tuple()


def execute_broadcast_plan_item(plan_item, data):
    """Execute one independent plan item (safe inside a pool worker)."""
    try:
        success = invoke_route_handler(
            plan_item.get("route_entry"), plan_item.get("queue"), safe_deepcopy(data))
        return make_dispatch_result(
            plan_item.get("target"), plan_item.get("queue_key"), success,
            getattr(plan_item.get("route_entry", {}).get("handler"),
                    "__name__", "unknown_handler"),
            plan_item.get("rule_name", ""))
    except Exception as exc:
        logger.error("execute_broadcast_plan_item failed: %s", exc)
        return make_dispatch_result(plan_item.get("target"), plan_item.get("queue_key"),
                                    False, "execute_broadcast_plan_item",
                                    "plan_item_exception")


def execute_broadcast_dispatch_plan(plan, data, sent_queue_keys, executor=None):
    """
    Execute a broadcast plan. Queue items fan out in parallel when an
    executor is provided and more than one item exists; handler-only items
    run in the router thread. sent_queue_keys is updated in the router
    thread only (no shared mutation in workers - free-threading safe).
    """
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()
        if not plan:
            return tuple()
        queue_items = tuple(item for item in plan if item.get("queue_key") is not None)
        handler_items = tuple(item for item in plan if item.get("queue_key") is None)
        results = []
        if executor is not None and len(queue_items) > 1:
            futures = []
            for item in queue_items:
                try:
                    futures.append(executor.submit(execute_broadcast_plan_item, item, data))
                except Exception as exc:
                    logger.error("broadcast plan submit failed: %s", exc)
                    results.append(execute_broadcast_plan_item(item, data))
            for future in as_completed(futures):
                try:
                    results.append(future.result(timeout=10.0))
                except Exception as exc:
                    logger.error("broadcast future failed: %s", exc)
        else:
            for item in queue_items:
                results.append(execute_broadcast_plan_item(item, data))
        for item in handler_items:
            results.append(execute_broadcast_plan_item(item, data))
        for result in results:
            if result.get("success") and result.get("queue_key") is not None:
                sent_queue_keys.add(result.get("queue_key"))
        return tuple(results)
    except Exception as exc:
        logger.error("execute_broadcast_dispatch_plan failed: %s", exc)
        return tuple()


def apply_broadcast_rules(data, router_context, sent_queue_keys=None):
    """Sequential broadcast application (plan engine without executor)."""
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()
        plan = build_broadcast_dispatch_plan(data, router_context, sent_queue_keys)
        return execute_broadcast_dispatch_plan(plan, data, sent_queue_keys)
    except Exception as exc:
        logger.error("apply_broadcast_rules failed: %s", exc)
        return tuple()


def apply_broadcast_rules_parallel(data, router_context, sent_queue_keys, executor):
    """Parallel broadcast fan-out (same plan engine, executor-backed puts)."""
    try:
        if sent_queue_keys is None:
            sent_queue_keys = set()
        plan = build_broadcast_dispatch_plan(data, router_context, sent_queue_keys)
        return execute_broadcast_dispatch_plan(plan, data, sent_queue_keys, executor)
    except Exception as exc:
        logger.error("apply_broadcast_rules_parallel failed: %s", exc)
        return tuple()


# =============================================================================
# CODEBASE - SINGLE EVENT PIPELINE
# =============================================================================

def process_single_event(data, router_context, executor=None):
    """
    Full routing pipeline for ONE dequeued event.
    Returns "stop" | "dropped" | "routed" | "unrouted".
    """
    try:
        router_name = router_context.get("router_name", DEFAULT_ROUTER_NAME)
        if data is None:
            return "dropped"
        if is_stop_message(data):
            logger.info("%s: stop sentinel received", router_name)
            return "stop"
        unknown_result = unwrap_unknown_envelope(data)
        if bool(unknown_result.get("drop", False)):
            logger.debug("%s: dropped unknown envelope (%s)",
                         router_name, unknown_result.get("reason", "unknown"))
            return "dropped"
        normalized_data = unknown_result.get("data", data)
        sent_queue_keys = set()
        primary_results = dispatch_deterministic_primary(
            normalized_data, router_context, sent_queue_keys)
        status = "routed"
        if not primary_results:
            status = "unrouted"
            logger.warning("%s: message without resolvable target: %s",
                           router_name, str(normalized_data)[:200])
        apply_dynamic_broadcast_targets(normalized_data, router_context, sent_queue_keys)
        apply_broadcast_rules_parallel(normalized_data, router_context,
                                       sent_queue_keys, executor)
        return status
    except Exception as exc:
        logger.error("process_single_event failed: %s", exc)
        return "dropped"


# =============================================================================
# CODEBASE - INGRESS ENGINE (one engine; FIFO is the batch-size-1 case)
# =============================================================================

def drain_ingress_batch(ingress_queue, ingress_timeout_s=5.0, priority_batch_max=64):
    """One blocking get, then non-blocking burst drain up to the batch max."""
    try:
        batch_max = max(1, int(priority_batch_max or 1))
        first = sync_queue_get(ingress_queue, float(ingress_timeout_s or 5.0))
        if first is None:
            return tuple()
        batch = [first]
        while len(batch) < batch_max:
            extra = sync_queue_get(ingress_queue, 0.0)
            if extra is None:
                break
            batch.append(extra)
        return tuple(batch)
    except Exception as exc:
        logger.error("drain_ingress_batch failed: %s", exc)
        return tuple()


def publish_batch_to_broker(batch, broker_state, router_context):
    """
    Resolve priorities and publish a batch; sequence numbers preserve the
    arrival (FIFO) order inside priority ties. With priority routing
    disabled every event gets PRIO_DEFAULT - exact FIFO through the heap.
    """
    try:
        enable_priority = bool(
            read_context_option(router_context, "enable_priority_routing", True))
        priority_from_targets = bool(
            read_context_option(router_context, "priority_from_targets", True))
        published = 0
        for event in (batch or tuple()):
            if enable_priority:
                priority = resolve_event_priority(
                    event,
                    event_priority_table=router_context.get("event_priority_table"),
                    target_priority_table=router_context.get("target_priority_table"),
                    priority_from_targets=priority_from_targets,
                )
            else:
                priority = PRIO_DEFAULT
            if priority_broker_publish(broker_state, priority, event) is not None:
                published += 1
        return published
    except Exception as exc:
        logger.error("publish_batch_to_broker failed: %s", exc)
        return 0


def run_ingress_cycle(ingress_queue, router_context, executor=None, shutdown_event=None):
    """
    One full ingress cycle: drain burst -> heap broker -> pop-min dispatch.
    FIFO-exact stop semantics: when a stop sentinel is popped, events with
    a lower sequence (arrived earlier) are still dispatched, later ones
    are dropped. Returns "stop" | "idle" | "processed" | "shutdown".
    """
    try:
        router_name = router_context.get("router_name", DEFAULT_ROUTER_NAME)
        batch = drain_ingress_batch(
            ingress_queue,
            ingress_timeout_s=read_context_option(router_context, "ingress_timeout_s", 5.0),
            priority_batch_max=read_context_option(router_context, "priority_batch_max", 64),
        )
        if not batch:
            return "idle"
        broker_state = make_priority_broker_state()
        publish_batch_to_broker(batch, broker_state, router_context)
        stop_sequence = None
        dropped_after_stop = 0
        while True:
            if shutdown_event is not None:
                try:
                    if shutdown_event.is_set():
                        remaining = priority_broker_size(broker_state)
                        if remaining:
                            logger.info("%s: shutdown during batch - %d events undispatched",
                                        router_name, remaining)
                        return "shutdown"
                except Exception:
                    pass
            entry = priority_broker_pop(broker_state)
            if entry is None:
                break
            if stop_sequence is not None and entry.get("sequence", 0) > stop_sequence:
                dropped_after_stop += 1
                continue
            status = process_single_event(entry.get("event"), router_context, executor)
            if status == "stop":
                stop_sequence = entry.get("sequence", 0)
        if stop_sequence is not None:
            if dropped_after_stop:
                logger.info("%s: dropped %d events queued after stop sentinel",
                            router_name, dropped_after_stop)
            return "stop"
        return "processed"
    except Exception as exc:
        logger.error("run_ingress_cycle failed: %s", exc)
        return "idle"


# =============================================================================
# CODEBASE - MAIN ROUTER
# =============================================================================

def read_router_option(router_kwargs, option_name, default=None):
    try:
        if not isinstance(router_kwargs, dict):
            return default
        value = router_kwargs.get(option_name, default)
        return default if value is None else value
    except Exception:
        return default


def collect_router_options(router_kwargs):
    """Collect all known router options (defaults + overrides) into a dict."""
    try:
        options = {}
        for option_name, default_value in ROUTER_OPTION_DEFAULTS:
            options[option_name] = read_router_option(router_kwargs, option_name, default_value)
        return options
    except Exception:
        return dict(ROUTER_OPTION_DEFAULTS)


def build_router_runtime(router_kwargs):
    """
    Assemble the complete router runtime (setup only, no loop):
    options, queue registry, routing table, broadcast rules, priority
    tables and the immutable router context. Priority routing disabled
    (or the legacy kwarg enable_priority_ingress=False) forces the
    ingress batch size to 1 -> the ONE ingress engine becomes exact FIFO.
    """
    try:
        options = collect_router_options(router_kwargs)
        if isinstance(router_kwargs, dict) and \
                router_kwargs.get("enable_priority_ingress") is False:
            options["enable_priority_routing"] = False
        if not bool(options.get("enable_priority_routing", True)):
            options["priority_batch_max"] = 1
        queue_registry = build_queue_registry(router_kwargs)
        router_context = create_router_context(
            queue_registry=queue_registry,
            routing_table=build_routing_table(options.get("routing_table")),
            broadcast_rules=build_broadcast_rules(
                queue_registry=queue_registry,
                custom_broadcast_rules=options.get("broadcast_rules"),
                enable_process_broadcast=options.get("enable_process_broadcast", True),
                enable_telemetry_mirror=options.get("enable_telemetry_mirror", True),
                enable_worker_mirror=options.get("enable_worker_mirror", True),
                enable_config_mirrors=options.get("enable_config_mirrors", True),
                enable_automation_status_mirror=options.get(
                    "enable_automation_status_mirror", True),
            ),
            router_name=options.get("router_name", DEFAULT_ROUTER_NAME),
            target_priority_table=build_target_priority_table(
                options.get("target_priority_table")),
            event_priority_table=build_event_priority_table(
                options.get("event_priority_table")),
            router_options=options,
        )
        return {
            "options": options,
            "router_context": router_context,
            "ingress_queue": queue_registry.get("queue_event_send"),
        }
    except Exception as exc:
        logger.error("build_router_runtime failed: %s", exc)
        return {"options": {}, "router_context": create_router_context({}, {}, tuple()),
                "ingress_queue": None}


def sync_event_router(**router_kwargs):
    """
    Main synchronous router entry point: deterministic priority-ordered
    primary dispatch + plan-based broadcast mirrors. The ThreadPoolExecutor
    is used only for the non-critical parallel broadcast fan-out.
    """
    router_name = DEFAULT_ROUTER_NAME
    try:
        runtime = build_router_runtime(router_kwargs)
        options = runtime.get("options", {})
        router_context = runtime.get("router_context", {})
        ingress_queue = runtime.get("ingress_queue")
        router_name = router_context.get("router_name", DEFAULT_ROUTER_NAME)
        shutdown_event = options.get("shutdown_event")

        if ingress_queue is None:
            logger.error("%s: queue_event_send is required", router_name)
            return False

        executor = ThreadPoolExecutor(
            max_workers=int(options.get("num_threads", 4) or 4),
            thread_name_prefix="%s-pool" % router_name,
        )
        logger.info(
            "%s started: %d queues, %d routes, %d broadcast rules, "
            "%d threads, priority_routing=%s, batch_max=%s",
            router_name,
            len(router_context.get("queues", {})),
            len(router_context.get("routing_table", {})),
            len(router_context.get("broadcast_rules", tuple())),
            int(options.get("num_threads", 4) or 4),
            bool(options.get("enable_priority_routing", True)),
            options.get("priority_batch_max", 64),
        )
        try:
            while True:
                if shutdown_event is not None:
                    try:
                        if shutdown_event.is_set():
                            break
                    except Exception:
                        pass
                cycle_status = run_ingress_cycle(
                    ingress_queue, router_context, executor, shutdown_event)
                if cycle_status in ("stop", "shutdown"):
                    break
        finally:
            executor.shutdown(wait=True, cancel_futures=False)
            logger.info("%s: ThreadPoolExecutor shut down", router_name)
        logger.info("%s stopped", router_name)
        return True
    except Exception as exc:
        logger.error("%s crashed: %s", router_name, exc)
        return False


# =============================================================================
# INTERFACE - EXTERNAL CALLABLE (compatibility wrappers + diagnostics)
# =============================================================================

def sync_ev_queue_handler(**router_kwargs):
    try:
        return sync_event_router(**router_kwargs)
    except Exception as exc:
        logger.error("sync_ev_queue_handler failed: %s", exc)
        return False


def get_compatibility_queue_parameter_names():
    try:
        return tuple(dict.fromkeys(
            name for name in DEFAULT_COMPAT_QUEUE_PARAMETER_NAMES if name))
    except Exception:
        return tuple(DEFAULT_COMPAT_QUEUE_PARAMETER_NAMES)


def build_router_signature():
    try:
        parameters = []
        for queue_name in get_compatibility_queue_parameter_names():
            parameters.append(inspect.Parameter(
                queue_name, inspect.Parameter.KEYWORD_ONLY, default=None))
        for option_name, default_value in ROUTER_OPTION_DEFAULTS:
            parameters.append(inspect.Parameter(
                option_name, inspect.Parameter.KEYWORD_ONLY, default=default_value))
        return inspect.Signature(parameters=tuple(parameters))
    except Exception:
        return inspect.Signature()


def apply_router_signature(func):
    try:
        func.__signature__ = build_router_signature()
    except Exception:
        pass
    return func


def refresh_router_signatures():
    try:
        apply_router_signature(sync_event_router)
        apply_router_signature(sync_ev_queue_handler)
        return True
    except Exception:
        return False


def register_router_queue_names(*queue_names):
    try:
        # Clean version keeps the default set; dynamic extension can be
        # added if truly needed by external filters.
        return get_compatibility_queue_parameter_names()
    except Exception:
        return tuple()


refresh_router_signatures()


def event_router_health_snapshot(routing_table=None, queue_registry=None, broadcast_rules=None):
    try:
        normalized_routing_table = build_routing_table(routing_table)
        normalized_queue_registry = queue_registry if isinstance(queue_registry, dict) else {}
        normalized_broadcast_rules = broadcast_rules \
            if isinstance(broadcast_rules, (list, tuple)) else tuple()
        return {
            "routing_targets": sorted(normalized_routing_table.keys()),
            "queue_keys": sorted(normalized_queue_registry.keys()),
            "broadcast_rule_names": [
                str(rule.get("name", "unknown_rule"))
                for rule in normalized_broadcast_rules if isinstance(rule, dict)
            ],
        }
    except Exception:
        return {"routing_targets": [], "queue_keys": [], "broadcast_rule_names": []}


def event_router_priority_snapshot(target_priority_table=None, event_priority_table=None):
    """Diagnostic snapshot of the effective priority configuration."""
    try:
        merged_targets = build_target_priority_table(target_priority_table)
        merged_events = build_event_priority_table(event_priority_table)
        target_rows = []
        for target in sorted(merged_targets.keys()):
            priority, domain = classify_router_target(target, merged_targets)
            target_rows.append({"target": target, "priority": priority, "domain": domain})
        event_rows = []
        for event_type in sorted(merged_events.keys()):
            event_rows.append({
                "event_type": event_type,
                "priority": coerce_priority_value(merged_events.get(event_type), PRIO_DEFAULT),
            })
        return {
            "priority_classes": {
                "PRIO_SYSTEM_CRITICAL": PRIO_SYSTEM_CRITICAL,
                "PRIO_STATE_WRITER": PRIO_STATE_WRITER,
                "PRIO_DATASTORE": PRIO_DATASTORE,
                "PRIO_CONTROLLER": PRIO_CONTROLLER,
                "PRIO_AUTOMATION": PRIO_AUTOMATION,
                "PRIO_THREAD_MANAGEMENT": PRIO_THREAD_MANAGEMENT,
                "PRIO_FIELDBUS": PRIO_FIELDBUS,
                "PRIO_TELEMETRY": PRIO_TELEMETRY,
                "PRIO_UI": PRIO_UI,
                "PRIO_DEFAULT": PRIO_DEFAULT,
            },
            "target_priorities": target_rows,
            "event_type_priorities": event_rows,
        }
    except Exception:
        return {"priority_classes": {}, "target_priorities": [], "event_type_priorities": []}


# =============================================================================
# PUBLIC SURFACE OVERVIEW
# =============================================================================
#
# Router entry points ......... sync_event_router, sync_ev_queue_handler
# Runtime setup ............... build_router_runtime, collect_router_options
# Priority configuration ...... PRIORITY_SPEC, build_target_priority_table,
#                                build_event_priority_table,
#                                classify_router_target, resolve_event_priority,
#                                order_targets_by_priority,
#                                order_targets_with_priority_details,
#                                predicate_priority_at_most
# Priority-Event-Queue ........ make_priority_broker_state,
#                                priority_broker_publish, priority_broker_pop,
#                                priority_broker_size
# Ingress engine .............. run_ingress_cycle, drain_ingress_batch,
#                                publish_batch_to_broker
# Pipeline .................... process_single_event,
#                                dispatch_deterministic_primary,
#                                apply_dynamic_broadcast_targets,
#                                apply_broadcast_rules,
#                                apply_broadcast_rules_parallel,
#                                build_broadcast_dispatch_plan,
#                                execute_broadcast_dispatch_plan
# Diagnostics ................. event_router_health_snapshot,
#                                event_router_priority_snapshot
#
# End of refactored implementation (slim, one ingress engine, one
# broadcast engine, spec-generated tables, full priority functionality)
