# -*- coding: utf-8 -*-

# src/library/_event_interface.py


"""
 =============================================================================
 _EVENT_INTERFACE
 =============================================================================

 What it is:
     A lightweight library that provides a **robust event‑communication** layer
     built on top of plain Python data structures.  Events are represented as
     dictionaries and are transported between producers and consumers using a
     consistent JSON‑based serialisation format.

 Why it is needed:
     • Guarantees that every event can be safely persisted, logged or sent
       across process boundaries (JSON is universally understood).
     • Decouples event producers from consumers – no hard‑coded references,
       only loosely‑coupled message passing.
     • Allows functional‑programming style handlers via ``functools.partial``,
       making it easy to pre‑bind context, configuration or state.

 How it works:
     1.  An **event** is created as a plain ``dict`` that follows a tiny schema
         (``type``, ``payload`` and optional ``metadata``).
     2.  The event is handed to the **Context Manager** which attaches a unique
         identifier, timestamps and any global context information.
     3.  The enriched event is serialised to a JSON string via ``json.dumps``.
     4.  Consumers receive the JSON, deserialize it and invoke the appropriate
         handler that has been registered with the **Interface** layer.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import json
import time
import uuid

import heapq
from heapq import heappop, heappush

from queue import Queue, Empty


from functools import partial
from datetime import datetime, timezone

import logging


# -------------------------------------------------------
# Init Logger
logger = logging.getLogger(__name__)
# -------------------------------------------------------


# ThreadPoolExecutor - Async Operation
_executor = None
# _executor = ThreadPoolExecutor(max_workers=2)


# MetricQueue
# _metrics.queue_register(q, name="queue_event_send")


# =============================================================================
# EVENT CATALOG
# =============================================================================

# Static system events (published by long-lived authority threads)
STATIC_EVENTS = {
    "config_epoch": {
        "publisher": "central_config_authority_thread",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "runtime_signal": {
        "publisher": "worker_thread",
        "subscribers": ("event_management_thread", "central_config_authority_thread"),
    },
}

# Request events (task submissions towards the generic thread pool / authority)
REQUESTED_EVENTS = {
    "config_commit_requested": {
        "publisher": "mqtt_ingress",
        "subscribers": ("central_config_authority_thread",),
    },
    "worker_task_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
    "rule_evaluation_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
    "automation_task_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
    "control_task_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
}

# Result events (task results flowing back from the generic thread pool)
RESULT_EVENTS = {
    "worker_task_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "rule_evaluation_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "automation_task_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "control_task_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
}

EVENT_CATALOG = {
    **STATIC_EVENTS,
    **REQUESTED_EVENTS,
    **RESULT_EVENTS,
}

"""
EVENT_CATALOG = {
    "config_epoch": {
        "publisher": "central_config_authority_thread",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "config_commit_requested": {
        "publisher": "mqtt_ingress",
        "subscribers": ("central_config_authority_thread",),
    },
    "runtime_signal": {
        "publisher": "worker_thread",
        "subscribers": ("event_management_thread", "central_config_authority_thread"),
    },
    "worker_task_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
    "worker_task_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "rule_evaluation_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
    "rule_evaluation_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "automation_task_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
    "automation_task_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
    "control_task_requested": {
        "publisher": "worker_thread",
        "subscribers": ("generic_thread_pool",),
    },
    "control_task_result": {
        "publisher": "generic_thread_pool",
        "subscribers": ("event_management_thread", "worker_thread"),
    },
}
"""

# =============================================================================
#
# =============================================================================

"""
 =============================================================================
 Hierarchical, deterministic event priority for the sync event router.
 =============================================================================

 Goals
 =====
- Targets resolve to a **priority class** (small integer, lower = higher).
- Routing is deterministic: same input -> same dispatch order.
- No OOP, pure functions, pure dicts.
- ``functools.partial`` instead of ``lambda``.
- The module is stand-alone and importable without side effects.

 Priority classes
 ===============
   0  SYSTEM_CRITICAL     heartbeat, stop, shutdown, watchdog
  10  STATE_WRITER        SEM / state event management (single writer)
  20  DATASTORE           worker datastore / config patches
  30  CONTROLLER          controller / automation tasks (per-worker)
  40  AUTOMATION          automation threads
  50  THREAD_MANAGEMENT   process controller / tm
  60  FIELDBUS            modbus / fieldbus / hardware bus
  70  TELEMETRY           telemetry / worker mirror
  80  UI                  websocket / mqtt bridge
 100  DEFAULT             unclassified

Events are primary-dispatched strictly in ascending priority order.
Broadcast mirrors run **after** the deterministic primary pipeline.
 =============================================================================
"""


# =============================================================================
# Priority class constants
# =============================================================================

PRIO_SYSTEM_CRITICAL = 0
PRIO_STATE_WRITER = 10
PRIO_DATASTORE = 20
PRIO_CONTROLLER = 30
PRIO_AUTOMATION = 40
PRIO_THREAD_MANAGEMENT = 50
PRIO_FIELDBUS = 60
PRIO_TELEMETRY = 70
PRIO_MQTT = PRIO_TELEMETRY
PRIO_UI = 80
PRIO_DEFAULT = 100


# Central stop sentinel markers (single source for router + consumers)
STOP_MARKERS = ("__STOP__",)


# =============================================================================
# Domain classification
# =============================================================================

DOMAIN_SYSTEM = "system"
DOMAIN_STATE = "state"
DOMAIN_DATA = "data"
DOMAIN_CONTROLLER = "controller"
DOMAIN_AUTOMATION = "automation"
DOMAIN_THREAD_MGMT = "thread_management"
DOMAIN_FIELDBUS = "fieldbus"
DOMAIN_TELEMETRY = "telemetry"
DOMAIN_WORKER = "worker"
DOMAIN_UI = "ui"
DOMAIN_UNKNOWN = "unknown"


# =============================================================================
# Target -> (priority, domain) mapping
#
# Declarative PRIORITY_SPEC: (priority, domain, targets). The effective
# lookup table is generated once by build_priority_table_from_spec() and
# stays consistent with all target names used by the sync event router.
# Runtime extension is additive via build_target_priority_table(custom).
# =============================================================================

PRIORITY_SPEC = (
    (PRIO_SYSTEM_CRITICAL, DOMAIN_SYSTEM,
     ("heartbeat",)),
    (PRIO_STATE_WRITER, DOMAIN_STATE,
     ("state", "state_handler", "state_management", "state_event_management",
      "sem", "event", "event_handler")),
    (PRIO_DATASTORE, DOMAIN_DATA,
     ("datastore", "database", "ds", "data_config", "config_data", "dc",
      "data_center_thread")),
    (PRIO_CONTROLLER, DOMAIN_CONTROLLER,
     ("controller", "mbc", "controller_thread")),
    (PRIO_AUTOMATION, DOMAIN_AUTOMATION,
     ("automation", "mba", "automation_thread")),
    (PRIO_THREAD_MANAGEMENT, DOMAIN_THREAD_MGMT,
     ("thread_management", "tm", "pc", "process_controller", "process")),
    (PRIO_FIELDBUS, DOMAIN_FIELDBUS,
     ("modbus", "mbh", "modbus_handler")),
    (PRIO_TELEMETRY, DOMAIN_TELEMETRY,
     ("telemetry", "telemetry_event", "ti", "mqtt", "worker", "worker_event",
      "proxy_worker_bridge", "runtime_command_binding")),
    (PRIO_UI, DOMAIN_UI,
     ("websocket", "ws")),
)


def build_priority_table_from_spec(priority_spec):
    """Generate a target -> (priority, domain) lookup dict from a SPEC."""
    try:
        table = {}
        for priority, domain, targets in priority_spec:
            for target in targets:
                table[target] = (priority, domain)
        return table
    except Exception:
        return {}


TARGET_PRIORITY_TABLE = build_priority_table_from_spec(PRIORITY_SPEC)

# Canonical alias used by the sync event router bridge
DEFAULT_TARGET_PRIORITY_TABLE = TARGET_PRIORITY_TABLE


# -----------------------------------------------------------------------------
# event_type -> priority override table. THE hook to prioritize specific
# events: extend additively via build_event_priority_table(custom), e.g.
# {"SENSOR_ALARM": 0}. Values here beat target-class priorities.
# -----------------------------------------------------------------------------

DEFAULT_EVENT_TYPE_PRIORITY_TABLE = {
    "__STOP__": PRIO_SYSTEM_CRITICAL,
    "HEARTBEAT": PRIO_SYSTEM_CRITICAL,
    "SHUTDOWN": PRIO_SYSTEM_CRITICAL,
    "WATCHDOG": PRIO_SYSTEM_CRITICAL,
    "CONFIG_CHANGED": PRIO_DATASTORE,
    "CONFIG_PATCH": PRIO_DATASTORE,
    "CONFIG_REPLACE": PRIO_DATASTORE,
    "CONFIG_UPDATED": PRIO_DATASTORE,
    "CONFIG_CHANGE": PRIO_DATASTORE,
    "AUTOMATION_STATUS": PRIO_AUTOMATION,
}


def coerce_priority_value(value, default=None):
    """Coerce arbitrary input to a non-negative integer priority or default."""
    try:
        if value is None or isinstance(value, bool):
            return default
        priority = int(value)
        return priority if priority >= 0 else default
    except Exception:
        return default


def build_target_priority_table(custom_target_priority_table=None):
    """
    Effective target -> (priority, domain) table: SPEC defaults merged
    with additive custom overrides. Custom values accept (priority,
    domain) tuples or a bare priority int.
    """
    try:
        table = dict(DEFAULT_TARGET_PRIORITY_TABLE)
        if isinstance(custom_target_priority_table, dict):
            for key, value in custom_target_priority_table.items():
                key_lc = str(key).strip().lower() if key is not None else ""
                if not key_lc:
                    continue
                if isinstance(value, (tuple, list)) and len(value) >= 2:
                    priority = coerce_priority_value(value[0], PRIO_DEFAULT)
                    domain = str(value[1]).strip().lower() or DOMAIN_UNKNOWN
                    table[key_lc] = (priority, domain)
                else:
                    priority = coerce_priority_value(value, None)
                    if priority is not None:
                        table[key_lc] = (priority, DOMAIN_UNKNOWN)
        return table
    except Exception:
        return dict(DEFAULT_TARGET_PRIORITY_TABLE)


def build_event_priority_table(custom_event_priority_table=None):
    """Effective event_type -> priority overrides (defaults + custom merge)."""
    try:
        table = dict(DEFAULT_EVENT_TYPE_PRIORITY_TABLE)
        if isinstance(custom_event_priority_table, dict):
            for key, value in custom_event_priority_table.items():
                key_uc = str(key).strip().upper() if key is not None else ""
                priority = coerce_priority_value(value, None)
                if key_uc and priority is not None:
                    table[key_uc] = priority
        return table
    except Exception:
        return dict(DEFAULT_EVENT_TYPE_PRIORITY_TABLE)



# =============================================================================
# Public helpers
# =============================================================================

def classify_target(target_name, target_priority_table=None):
    """
    Return (priority, domain) for a target name.

    Optional target_priority_table overrides the module default table
    (fully backwards compatible - single-argument calls keep working).
    Unknown targets map to (PRIO_DEFAULT, DOMAIN_UNKNOWN) - that still
    preserves determinism because the sort is by (priority, target).
    """
    try:
        table = target_priority_table if isinstance(target_priority_table, dict) \
            else TARGET_PRIORITY_TABLE
        if target_name is None:
            return (PRIO_DEFAULT, DOMAIN_UNKNOWN)
        key = str(target_name).strip().lower()
        if not key:
            return (PRIO_DEFAULT, DOMAIN_UNKNOWN)
        entry = table.get(key)
        if isinstance(entry, (tuple, list)) and len(entry) >= 2:
            priority = coerce_priority_value(entry[0], PRIO_DEFAULT)
            domain = str(entry[1]).strip().lower() or DOMAIN_UNKNOWN
            return (priority, domain)
        priority = coerce_priority_value(entry, None)
        if priority is not None:
            return (priority, DOMAIN_UNKNOWN)
        return (PRIO_DEFAULT, DOMAIN_UNKNOWN)
    except Exception:
        return (PRIO_DEFAULT, DOMAIN_UNKNOWN)


def get_target_priority(target_name, target_priority_table=None):
    return classify_target(target_name, target_priority_table)[0]


def get_target_domain(target_name, target_priority_table=None):
    return classify_target(target_name, target_priority_table)[1]


def target_priority_sort_key(target_priority_table, target_name):
    """Named sort-key function, bound via functools.partial (never lambda)."""
    try:
        priority, domain = classify_target(target_name, target_priority_table)
        return (priority, str(domain), str(target_name or ""))
    except Exception:
        return (PRIO_DEFAULT, DOMAIN_UNKNOWN, str(target_name or ""))


def _priority_sort_key(target_name):
    """Legacy single-argument sort key (module default table)."""
    return target_priority_sort_key(None, target_name)


def order_targets_deterministic(targets, target_priority_table=None):
    """
    Return a deterministic, priority-sorted tuple of targets.

    - Stable even when priorities tie (secondary sort by domain, then
      by target name).
    - Duplicates removed, preserving the first occurrence.
    - Optional target_priority_table overrides the module default.
    """
    try:
        if not targets:
            return tuple()

        seen = set()
        unique = []
        for target in targets:
            if target is None:
                continue
            key = str(target).strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(key)

        if not unique:
            return tuple()

        unique.sort(key=partial(target_priority_sort_key, target_priority_table))
        return tuple(unique)
    except Exception:
        return tuple()


def order_targets_by_priority_domain(targets, target_priority_table=None):
    """
    Same as ``order_targets_deterministic`` but also returns the
    ``(priority, domain, target)`` tuples for diagnostic use.
    """
    try:
        ordered_names = order_targets_deterministic(targets, target_priority_table)
        result = []
        for target in ordered_names:
            priority, domain = classify_target(target, target_priority_table)
            result.append((priority, domain, target))
        return tuple(result)
    except Exception:
        return tuple()


# -----------------------------------------------------------------------------
# Event priority resolution (single source of truth for all consumers)
# -----------------------------------------------------------------------------

def _read_explicit_event_priority(data):
    """Explicit numeric priority from top level or payload (None if absent)."""
    try:
        if not isinstance(data, dict):
            return None
        priority = coerce_priority_value(data.get("priority"), None)
        if priority is not None:
            return priority
        payload = data.get("payload")
        if isinstance(payload, dict):
            return coerce_priority_value(payload.get("priority"), None)
        return None
    except Exception:
        return None


def _read_event_type_upper(data):
    """Upper-cased event type from event_type / type / message_type fields."""
    try:
        if not isinstance(data, dict):
            return ""
        event_type = (
            data.get("event_type")
            or data.get("type")
            or data.get("message_type")
        )
        if event_type is None:
            payload = data.get("payload")
            if isinstance(payload, dict):
                event_type = (
                    payload.get("event_type")
                    or payload.get("type")
                    or payload.get("message_type")
                )
        if event_type is None:
            return ""
        return str(event_type).strip().upper()
    except Exception:
        return ""


def _read_primary_target_names(data):
    """Primary target names (target / message_type / type legacy chain)."""
    try:
        if not isinstance(data, dict):
            return tuple()
        value = data.get("target")
        if value is None:
            value = data.get("message_type")
        if value is None:
            value = data.get("type")
        return normalize_event_target(value)
    except Exception:
        return tuple()


def _is_stop_marker_event(data):
    """True when any of the marker fields carries a stop sentinel."""
    try:
        if not isinstance(data, dict):
            return False
        for key in ("_type", "event_type", "type"):
            value = data.get(key)
            if value is not None and str(value).strip().upper() in STOP_MARKERS:
                return True
        return False
    except Exception:
        return False


def resolve_event_priority(data, event_priority_table=None,
                           target_priority_table=None, priority_from_targets=True):
    """
    Deterministic priority precedence:
      1. explicit "priority" field (top level or payload)
      2. stop sentinel -> PRIO_SYSTEM_CRITICAL
      3. event_type override table
      4. best (minimum) target class priority (if priority_from_targets)
      5. PRIO_DEFAULT (legacy events keep pure FIFO semantics)
    """
    try:
        explicit_priority = _read_explicit_event_priority(data)
        if explicit_priority is not None:
            return explicit_priority
        if _is_stop_marker_event(data):
            return PRIO_SYSTEM_CRITICAL
        event_type = _read_event_type_upper(data)
        if event_type:
            table = event_priority_table if isinstance(event_priority_table, dict) \
                else DEFAULT_EVENT_TYPE_PRIORITY_TABLE
            override = coerce_priority_value(table.get(event_type), None)
            if override is not None:
                return override
        if priority_from_targets:
            best_priority = None
            for target in _read_primary_target_names(data):
                candidate = get_target_priority(target, target_priority_table)
                if candidate == PRIO_DEFAULT:
                    continue
                if best_priority is None or candidate < best_priority:
                    best_priority = candidate
            if best_priority is not None:
                return best_priority
        return PRIO_DEFAULT
    except Exception:
        return PRIO_DEFAULT



# =============================================================================
# Universal serialization - core of robust communication
# =============================================================================

def universal_serialize(data):
    """
    Universal serialization for ALL data types.
    Pragmatic: everything becomes a JSON string.
    """
    try:
        # Special handling for well-known shapes
        if hasattr(data, 'dict'):  # pydantic-like models
            return json.dumps(data.dict(), default=str)
        elif hasattr(data, '__dict__'):  # plain objects
            return json.dumps(data.__dict__, default=str)
        else:
            # Standard JSON serialization with fallback
            return json.dumps(data, default=str)
    except Exception as e:
        # Last resort: string representation
        logger.warning("serialization failed, using str(): %s", e)
        return json.dumps({"_fallback": str(data), "_type": type(data).__name__})


def universal_deserialize(data):
    """
    Universal deserialization.
    Pragmatic: try JSON, otherwise return the original value.
    """
    if isinstance(data, str):
        try:
            return json.loads(data)
        except Exception:
            # JSON parsing failed - treat it as a plain string
            return data
    else:
        # Already deserialized
        return data


# =============================================================================
# Event Factory
# =============================================================================

def make_event(event_type, payload=None, **kwargs):
    """Create an event carrying the minimal required fields."""
    evt = {
        'event_id': str(uuid.uuid4()),
        'event_type': event_type,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'payload': payload or {}
    }
    evt.update(kwargs)
    return evt


# Compatibility alias
create_event = make_event


# =============================================================================
# Synchronous Queue-Wrapper
# =============================================================================

def queue_put_sync(sync_q, item):
    """
    Put *item* into ``sync_q`` (after universal JSON serialization).
    Returns True on success, False otherwise (e.g. queue is None or the
    serialization failed).
    """
    if sync_q is None:
        logger.error("queue_put_sync: given queue is None")
        return False

    try:
        payload = universal_serialize(item)
    except Exception as exc:                             # serialization failed
        logger.error("queue_put_sync: serialization error %s", exc)
        return False

    try:
        # queue.Queue.put is already blocking - no executor required.
        sync_q.put(payload)
        logger.debug(
            "queue_put_sync: %s (size %d bytes)",
            type(item).__name__,
            len(payload),
        )
        return True
    except Exception as exc:                               # unexpected put error
        logger.error("queue_put_sync: put error %s", exc)
        return False


def queue_get_sync(sync_q, timeout=5.0):
    """
    Read one element from ``sync_q`` (waiting up to ``timeout`` seconds).

    *   On a normal timeout (no data within *timeout*) None is returned.
    *   On success the JSON-deserialized object is returned.
    *   On other errors (e.g. queue is None) None is returned as well.
    """
    if sync_q is None:
        logger.error("queue_get_sync: given queue is None")
        return None

    try:
        raw = sync_q.get(block=True, timeout=timeout)
    except Empty:                                       # timeout - expected behavior
        logger.debug("queue_get_sync: timeout after %.1f s", timeout)
        return None
    except Exception as exc:                             # unexpected get error
        logger.error("queue_get_sync: get error %s", exc)
        return None

    try:
        data = universal_deserialize(raw)
        logger.debug("queue_get_sync: %s", type(data).__name__)
        return data
    except Exception as exc:                            # deserialization damage
        logger.error("queue_get_sync: deserialization error %s", exc)
        return None


# Canonical alias exports (names used by the sync event router bridge)
sync_queue_put = queue_put_sync
sync_queue_get = queue_get_sync


# =============================================================================
# Event Handler Registry
# =============================================================================

def make_event_handler_registry(default_handler=None):
    return {
        "handlers": {},
        "default_handler": default_handler,
        "registered": 0,
        "handled": 0,
        "failed": 0,
    }


def register_event_handler(registry, event_type, handler):
    if not callable(handler):
        raise TypeError("event handler must be callable")

    registry["handlers"][event_type] = handler
    registry["registered"] += 1
    return registry


def unregister_event_handler(registry, event_type):
    return registry["handlers"].pop(event_type, None)


def get_event_handler(registry, event_type):
    return registry.get("handlers", {}).get(event_type, registry.get("default_handler"))


def handle_event(registry, event, context=None):
    handler = get_event_handler(registry, event.get("type"))

    if handler is None:
        registry["failed"] += 1
        return {
            "status": "unhandled",
            "event": event,
        }

    try:
        result = handler(event, context)
        registry["handled"] += 1
        return {
            "status": "handled",
            "result": result,
            "event": event,
        }
    except Exception as error:
        registry["failed"] += 1
        return {
            "status": "failed",
            "error": str(error),
            "event": event,
        }


def handler_registry_snapshot(registry):
    return {
        "registered": int(registry.get("registered", 0)),
        "handled": int(registry.get("handled", 0)),
        "failed": int(registry.get("failed", 0)),
        "event_types": tuple(sorted(registry.get("handlers", {}).keys())),
    }


# =============================================================================
# Event Broker
# =============================================================================

def make_event_broker_state():
    return {
        "sequence": 0,
        "queue": [],
        "dead_letters": [],
    }


def publish_event(broker_state, priority, event):
    broker_state["sequence"] += 1
    heappush(
        broker_state["queue"],
        (priority, broker_state["sequence"], event),
    )
    return broker_state["sequence"]


def pop_event(broker_state):
    if not broker_state["queue"]:
        return None

    priority, sequence, event = heappop(broker_state["queue"])

    return {
        "priority": priority,
        "sequence": sequence,
        "event": event,
    }


def event_broker_size(broker_state):
    """Number of events currently waiting inside the broker heap."""
    try:
        return len(broker_state.get("queue", []))
    except Exception:
        return 0


# Priority alias exports (canonical names used by the sync event router):
# the priority features live HERE inside the event broker - one heap,
# one sequence counter, zero redundancy across consumers.
make_priority_broker_state = make_event_broker_state
priority_broker_publish = publish_event
priority_broker_pop = pop_event
priority_broker_size = event_broker_size


# =============================================================================
# Event Validation
# =============================================================================

def validate_event_shape(event):
    errors = []

    if type(event) is not dict:
        return ("event must be a dict",)

    if not event.get("type"):
        errors.append("event type is required")

    if not event.get("source"):
        errors.append("event source is required")

    if "payload" not in event:
        errors.append("event payload is required")

    return tuple(errors)


def validate_event_catalog(event, catalog=None):
    errors = list(validate_event_shape(event))

    if errors:
        return tuple(errors)

    event_catalog = catalog or EVENT_CATALOG
    event_type = event.get("type")
    source = event.get("source")
    contract = event_catalog.get(event_type)

    if contract is None:
        errors.append(f"unknown event type: {event_type}")
        return tuple(errors)

    publisher = contract.get("publisher")
    if publisher is not None and source != publisher:
        errors.append(f"invalid publisher for {event_type}: {source}")

    return tuple(errors)


def make_event_validation_result(event, catalog=None):
    errors = validate_event_catalog(event, catalog=catalog)
    return {
        "ok": not errors,
        "errors": errors,
        "event": event,
    }


# =============================================================================
# Event Envelope
# =============================================================================

def monotonic_s():
    return time.monotonic()


def make_event_id(prefix="evt"):
    return f"{prefix}:{uuid.uuid4()}"


def make_event_envelope(event_type, payload, source, target=None, priority=100, sequence=None, event_id=None, epoch=None, created_s=None, metadata=None):
    event = {
        "event_id": event_id or make_event_id(),
        "type": event_type,
        "payload": payload,
        "source": source,
        "target": target,
        "priority": int(priority),
        "sequence": sequence,
        "epoch": epoch,
        "created_s": monotonic_s() if created_s is None else float(created_s),
        "metadata": dict(metadata or {}),
    }
    return event


def copy_event_with_target(event, target):
    copied = dict(event)
    copied["target"] = target
    return copied


def event_identity(event):
    return (
        event.get("event_id"),
        event.get("type"),
        event.get("source"),
        event.get("target"),
    )


# =============================================================================
# Event Routing Helpers
# =============================================================================

def make_routing_rule(event_type, targets=None, priority=None):
    return {
        "event_type": event_type,
        "targets": tuple(targets or ()),
        "priority": priority,
    }


def catalog_targets_for_event(event, catalog=None):
    event_catalog = catalog or EVENT_CATALOG
    contract = event_catalog.get(event.get("type"), {})
    return tuple(contract.get("subscribers", ()))


def normalize_event_target(target):
    if target is None:
        return ()

    if type(target) is tuple:
        return target

    if type(target) is list:
        return tuple(target)

    return (target,)


def resolve_event_targets(event, routing_rules=None, catalog=None):
    explicit_targets = normalize_event_target(event.get("target"))

    if explicit_targets:
        return explicit_targets

    rules = routing_rules or {}
    rule = rules.get(event.get("type"))

    if rule is not None and rule.get("targets"):
        return tuple(rule.get("targets"))

    return catalog_targets_for_event(event, catalog=catalog)


def build_event_routes(event, routing_rules=None, catalog=None):
    routes = []

    for target in resolve_event_targets(event, routing_rules=routing_rules, catalog=catalog):
        routes.append({
            "target": target,
            "event": copy_event_with_target(event, target),
        })

    return tuple(routes)


def route_is_broadcast(route):
    return route.get("target") in ("*", "broadcast")



# =============================================================================
# Dead Letter
# =============================================================================

def append_dead_letter(dead_letters, event, reason):
    dead_letters.append({
        "event": event,
        "reason": reason,
    })

