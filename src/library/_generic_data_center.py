# -*- coding: utf-8 -*-

# src/library/_generic_data_center.py


"""
 =============================================================================
 _GENERIC_DATA_CENTER
 =============================================================================

 GENERIC DATA CENTER - Central event-driven data hub (HEAP | JSON | SQLITE)

 WHAT IT IS:
     A pure functional, generic and dynamic data-management library that
     acts as the central, event-driven data node of the automation system.
     It loads the verified "device_state_norm" configuration completely
     into the MEM_HEAP, exposes deterministic single-writer commits,
     persists the heap atomically back to JSON (canonical or human
     readable table layout) and - when enabled - mirrors the complete
     dataset into a SQLite database. Worker threads receive their data
     exclusively through isolated deep-copy shard snapshots.

 WHY IT IS NEEDED:
     - Config-Master authority: exactly ONE writer mutates the central
       heap - every state change flows through request_write_commit().
     - Runtime performance: all data lives in the heap; readers obtain
       consistent deep-copy snapshots (snapshot isolation, lock-free
       consumption after copy).
     - Event-driven synchronisation: every load / commit / save / sync
       publishes a well-defined event so the GenericEventRouter and the
       downstream sync chain can react deterministically.
     - Persistence: deterministic, atomic JSON round-trips plus optional
       full SQLite mirroring (generic per-section tables, lossless).
     - Sharding: sections are generically partitioned by a shard field
       (default "controller_uuid") so every worker/controller thread can
       be updated and rolled out separately, consistently and
       deterministically.

 HOW IT WORKS:
     1.  create_data_center_context() builds a plain dict context holding
         the heap, one re-entrant lock (single lock => deadlock free),
         the revision counter, options and the event subscriber registry.
     2.  load_json_into_heap() reads the config file through the verified
         read path of _generic_type_library - nothing unverified enters
         the heap - and publishes EVENT_HEAP_LOADED.
     3.  request_write_commit() is the ONLY mutation entry point
         (Single-Writer). It applies a generic path mutation
         (set | update | append | delete), bumps the revision, optionally
         auto-persists and publishes EVENT_HEAP_COMMIT only AFTER the
         central heap is fully up to date - the caller returns when the
         central write and the synchronous downstream dispatch completed.
     4.  save_heap_to_json() verifies the snapshot and writes it
         atomically (temp file + os.replace) either in canonical
         byte-stable form or in the human readable table layout produced
         by _json_formator (numeric_pk pre-sort + one record per line).
     5.  sync_heap_to_sqlite() rebuilds one table per section inside a
         single transaction: dynamically inferred typed columns for SQL
         queries plus a lossless "_row_json" column for exact
         round-trips via load_heap_from_sqlite().
     6.  build_shard_index() / get_shard_snapshot() partition every
         section generically by the shard field; rows without the field
         belong to the "__global__" shard. Worker threads consume these
         isolated snapshots for their local rule engines and FSMs.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import json
import logging
import os
import re
import sqlite3
import tempfile
import threading
import time
import uuid
from functools import partial

from src.library import _generic_type_library as type_library
from src.library import _json_formator as json_formator
from src.library._generic_system_contract_library import (
    config_sharding_contracts as config_graph,
)

config_compatibility = type_library

# =============================================================================
# CENTRAL MODULE "CONTEXT MANAGEMENT" - definition & initialisation
# =============================================================================

LOGGER = logging.getLogger("_generic_data_center")

GLOBAL_SHARD_KEY = "__global__"

EVENT_HEAP_LOADED = "data_center.heap_loaded"
EVENT_HEAP_COMMIT = "data_center.heap_commit"
EVENT_HEAP_SAVED = "data_center.heap_saved"
EVENT_SQLITE_SYNCED = "data_center.sqlite_synced"
EVENT_SHARD_SYNC = "data_center.shard_sync"
EVENT_WRITE_REJECTED = "data_center.write_rejected"

DEFAULT_OPTIONS = {
    "json_path": None,               # source / target config file
    "json_receipt_path": None,       # crash-safe provenance for internal saves
    "json_style": "table",           # "table" (formator) | "canonical" (type lib)
    "indent_size": 2,                # indent for the table layout
    "sort_mode": "fingerprint_safe",  # object order only; arrays keep identity
    "sqlite_enabled": False,         # master switch for SQLite mirroring
    "sqlite_path": None,             # target database file
    "shard_field": "controller_uuid",  # generic shard partition field
    "verify_on_load": True,          # full verification on every load
    "verify_on_save": True,          # full verification before every save
    "preserve_config_identity_on_save": False,  # reject formatter identity drift
    "allow_legacy_import": False,     # Contract 5 requires explicit profile opt-in
    "legacy_import_profile": "production_v2",  # fail closed on safety migrations
    "autosave_json": False,          # persist JSON after every commit
    "autosync_sqlite": False,        # mirror SQLite after every commit
    "encoding": "utf-8",
}

CONTEXT_TEMPLATE = {
    "name": "generic_data_center",
    "library_version": "1.1.2",
    "created_at": 0.0,
    "heap": {},                      # MEM_HEAP - the single source of truth
    "heap_lock": None,               # one re-entrant lock => deadlock free
    "persistence_lock": None,        # serialises JSON + provenance writes
    "revision": 0,                   # bumped on every successful mutation
    "dirty": False,                  # heap differs from last persisted state
    "fingerprint": "",               # cached canonical SHA-256 of the heap
    "fingerprint_revision": -1,      # revision the cached fingerprint belongs to
    "last_persisted_fingerprint": "",  # exact snapshot last loaded/saved
    "event_sequence": 0,             # monotonic event counter
    "subscribers": {},               # event_type -> list of handler callables
    "options": {},                   # runtime options (see DEFAULT_OPTIONS)
    "statistics": {},                # deterministic operation counters
}

DATA_CENTER_CONTEXT = None           # default module instance, set at import

SQL_IDENTIFIER_PATTERN = re.compile(r"[^A-Za-z0-9_]")

SQL_META_TABLE = "__datacenter_meta__"
SQL_REGISTRY_TABLE = "__section_registry__"

# =============================================================================
# HELP-FUNCTIONS
# =============================================================================

def current_timestamp():
    """Wall clock timestamp in seconds (float, UTC based epoch)."""
    return time.time()


def new_event_id():
    """Unique event identifier for transport and logging."""
    return str(uuid.uuid4())


def new_statistics():
    """Fresh deterministic operation counter block."""
    return {
        "loads": 0,
        "commits": 0,
        "rejected_commits": 0,
        "json_saves": 0,
        "json_receipts": 0,
        "sqlite_syncs": 0,
        "events_published": 0,
        "shard_snapshots": 0,
    }


def make_report(status, message):
    """Uniform result envelope used by every public operation."""
    return {"status": status, "message": message, "details": {}}


def report_ok(message):
    """Success report shortcut."""
    return make_report("OK", message)


def report_error(message):
    """Error report shortcut (operations never raise to the caller)."""
    return make_report("ERROR", message)


def log_event(logger, event):
    """Default subscriber: minimalistic structured event logging."""
    logger.info("event %s seq=%s rev=%s path=%s",
                event["event_type"], event["sequence"],
                event["payload"].get("revision", "-"),
                event["payload"].get("path", "-"))


def parse_data_path(path):
    """
    Parse a generic data path into plain tokens.
    Accepted forms (all equivalent):
        "controllers[0].host"
        "controllers.0.host"
        "controllers/0/host"
        ["controllers", 0, "host"]
    Bracket indices always become int tokens; bare digit segments are
    resolved as list indices at walk time when the container is a list.
    """
    if isinstance(path, (list, tuple)):
        return list(path)
    if path is None:
        return []
    text = str(path).strip()
    if text == "":
        return []
    tokens = []
    for segment in text.replace("/", ".").split("."):
        if segment == "":
            continue
        while "[" in segment:
            head, _, rest = segment.partition("[")
            if head != "":
                tokens.append(head)
            index_text, _, segment = rest.partition("]")
            tokens.append(int(index_text))
        if segment != "":
            tokens.append(segment)
    return tokens


def resolve_token(container, token):
    """Resolve one path token against a dict or list container."""
    if isinstance(container, dict):
        key = token
        if key not in container and isinstance(token, int):
            key = str(token)
        if key in container:
            return True, container[key]
        return False, None
    if isinstance(container, list):
        index = token
        if isinstance(index, str) and index.lstrip("+-").isdigit():
            index = int(index)
        if isinstance(index, int) and -len(container) <= index < len(container):
            return True, container[index]
        return False, None
    return False, None


def walk_path(root, tokens):
    """Walk all tokens; returns (found, value)."""
    node = root
    for token in tokens:
        found, node = resolve_token(node, token)
        if not found:
            return False, None
    return True, node


def normalize_list_index(container, token):
    """Normalize a token into a valid list index or None."""
    index = token
    if isinstance(index, str) and index.lstrip("+-").isdigit():
        index = int(index)
    if isinstance(index, int) and -len(container) <= index < len(container):
        return index
    return None


def atomic_write_text(file_path, text, encoding):
    """
    Deterministic atomic file write: temp file in the target directory,
    fsync, then os.replace() - a reader never observes a partial file.
    """
    target = os.path.abspath(file_path)
    directory = os.path.dirname(target) or "."
    os.makedirs(directory, exist_ok=True)
    handle, temp_path = tempfile.mkstemp(
        prefix=os.path.basename(target) + ".tmp.", dir=directory, text=False)
    try:
        payload = text if text.endswith("\n") else text + "\n"
        os.write(handle, payload.encode(encoding))
        os.fsync(handle)
        os.close(handle)
        handle = -1
        os.replace(temp_path, target)
        temp_path = None
        if hasattr(os, "O_DIRECTORY"):
            directory_handle = os.open(
                directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_handle)
            finally:
                os.close(directory_handle)
    finally:
        if handle >= 0:
            os.close(handle)
        if temp_path is not None and os.path.exists(temp_path):
            os.remove(temp_path)


def sanitize_sql_identifier(name):
    """Reduce any section/field name to a safe SQL identifier."""
    cleaned = SQL_IDENTIFIER_PATTERN.sub("_", str(name))
    if cleaned == "" or cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned


def quote_sql_identifier(name):
    """Double-quote an identifier for SQLite statements."""
    return '"' + sanitize_sql_identifier(name) + '"'


def infer_sql_type(value):
    """Map a JSON value onto a SQLite column affinity."""
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, (dict, list)):
        return "TEXT"
    return "TEXT"


def encode_sql_value(value):
    """Encode a JSON value into its SQLite cell representation."""
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float, str)):
        return value
    return type_library.canonical_dumps(value)


def infer_section_columns(rows):
    """
    Dynamic, deterministic column inference for one section:
    union of all row keys in first-seen order plus a type per column.
    """
    columns = []
    column_types = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key not in column_types:
                columns.append(key)
                column_types[key] = infer_sql_type(row[key])
            elif column_types[key] == "TEXT":
                continue
            elif infer_sql_type(row[key]) != column_types[key]:
                column_types[key] = "TEXT"
    return columns, column_types

# =============================================================================
# CODEBASE - FUNCTIONS - PART 1: CONTEXT LIFECYCLE
# =============================================================================

def create_data_center_context(**options):
    """
    Build one independent, fully initialised data-center context.
    The context is a plain dict - generic, serialisation friendly and
    free-threaded safe through its single re-entrant heap lock.
    """
    context = copy.deepcopy(CONTEXT_TEMPLATE)
    context["created_at"] = current_timestamp()
    context["heap_lock"] = threading.RLock()
    context["persistence_lock"] = threading.RLock()
    context["options"] = dict(DEFAULT_OPTIONS)
    context["statistics"] = new_statistics()
    configure_data_center(context, **options)
    LOGGER.debug("data center context created: %s", context["name"])
    return context


def configure_data_center(context, **options):
    """
    Update runtime options; unknown keys are rejected deterministically
    so configuration typos can never silently disable behaviour.
    """
    report = report_ok("options applied")
    for key in options.keys():
        if key == "name":
            context["name"] = str(options[key])
        elif key == "legacy_import_profile":
            profile = str(options[key] or "").strip().lower()
            if profile not in ("lab_v2", "production_v2"):
                return report_error(
                    "legacy_import_profile must be lab_v2 or production_v2")
            context["options"][key] = profile
        elif key in DEFAULT_OPTIONS:
            context["options"][key] = options[key]
        else:
            report = report_error("unknown option: " + str(key))
            LOGGER.error("configure rejected unknown option: %s", key)
            return report
    report["details"]["options"] = dict(context["options"])
    return report


def describe_data_center(context):
    """Deterministic status snapshot (no heap payload, safe to log)."""
    with context["heap_lock"]:
        return {
            "name": context["name"],
            "library_version": context["library_version"],
            "revision": context["revision"],
            "dirty": context["dirty"],
            "sections": len(context["heap"]),
            "fingerprint": get_fingerprint(context),
            "options": dict(context["options"]),
            "statistics": dict(context["statistics"]),
        }

# =============================================================================
# CODEBASE - FUNCTIONS - PART 2: EVENT LAYER (PUBLISH / SUBSCRIBE)
# =============================================================================

def subscribe_event(context, event_type, handler):
    """
    Register a handler callable for one event type. Handlers receive the
    complete event dict; pre-bind extra arguments with functools.partial.
    """
    with context["heap_lock"]:
        handlers = context["subscribers"].setdefault(event_type, [])
        if handler not in handlers:
            handlers.append(handler)
    return report_ok("subscribed: " + str(event_type))


def unsubscribe_event(context, event_type, handler):
    """Remove a previously registered handler (idempotent)."""
    with context["heap_lock"]:
        handlers = context["subscribers"].get(event_type, [])
        if handler in handlers:
            handlers.remove(handler)
    return report_ok("unsubscribed: " + str(event_type))


def build_event(context, event_type, payload):
    """Assemble one enriched, transport-ready event dict."""
    with context["heap_lock"]:
        context["event_sequence"] += 1
        context["statistics"]["events_published"] += 1
        sequence = context["event_sequence"]
    return {
        "event_id": new_event_id(),
        "event_type": event_type,
        "sequence": sequence,
        "timestamp": current_timestamp(),
        "source": context["name"],
        "payload": payload,
    }


def publish_event(context, event_type, payload):
    """
    Build and synchronously dispatch one event to every subscriber in
    registration order. Dispatch runs OUTSIDE the heap lock and every
    handler is shielded - one failing consumer never breaks the chain.
    """
    event = build_event(context, event_type, payload)
    with context["heap_lock"]:
        handlers = list(context["subscribers"].get(event_type, []))
    for handler in handlers:
        try:
            handler(event)
        except Exception as exc:
            LOGGER.error("event handler failed for %s: %s", event_type, exc)
    return event

# =============================================================================
# CODEBASE - FUNCTIONS - PART 3: MEM_HEAP READ ACCESS (SNAPSHOT ISOLATION)
# =============================================================================

def heap_snapshot(context, path=None):
    """
    Consistent deep-copy snapshot of the whole heap or one sub path.
    Returns (found, snapshot). The copy is fully isolated - consumers
    can never mutate the central heap through a snapshot.
    """
    tokens = parse_data_path(path)
    with context["heap_lock"]:
        found, value = walk_path(context["heap"], tokens)
        if not found:
            return False, None
        return True, copy.deepcopy(value)


def heap_snapshot_envelope(context):
    """Capture one full snapshot and its revision as a single lock epoch.

    The fingerprint is computed from the isolated copy after releasing the
    lock, so it can never describe another revision even when persistence and
    commits overlap.
    """
    with context["heap_lock"]:
        snapshot = copy.deepcopy(context["heap"])
        revision = int(context["revision"])
        dirty = bool(context["dirty"])
    if not isinstance(snapshot, dict) or not snapshot:
        return False, None
    fingerprint = type_library.config_fingerprint(snapshot)
    return True, {
        "snapshot": snapshot,
        "revision": revision,
        "fingerprint": fingerprint,
        "dirty": dirty,
    }


def heap_get(context, path, default=None):
    """Isolated read of one value; returns default when path is absent."""
    found, value = heap_snapshot(context, path)
    if not found:
        return default
    return value


def heap_exists(context, path):
    """Check whether a data path currently resolves inside the heap."""
    tokens = parse_data_path(path)
    with context["heap_lock"]:
        found, _ = walk_path(context["heap"], tokens)
    return found


def list_sections(context):
    """Deterministic sorted list of all top-level section names."""
    with context["heap_lock"]:
        return sorted(context["heap"].keys())


def get_revision(context):
    """Current heap revision (bumps on every successful mutation)."""
    with context["heap_lock"]:
        return context["revision"]


def get_fingerprint(context):
    """
    Canonical SHA-256 fingerprint of the current heap. Cached per
    revision so repeated calls stay cheap and deterministic.
    """
    with context["heap_lock"]:
        if context["fingerprint_revision"] == context["revision"]:
            return context["fingerprint"]
        fingerprint = type_library.config_fingerprint(context["heap"])
        context["fingerprint"] = fingerprint
        context["fingerprint_revision"] = context["revision"]
        return fingerprint

# =============================================================================
# CODEBASE - FUNCTIONS - PART 4: SINGLE-WRITER MUTATION LAYER
# =============================================================================

def ensure_parent_containers(root, tokens):
    """
    Walk to the parent of the final token, creating missing dict levels
    on the way (generic auto-vivification for "set" commits).
    Returns (parent_or_none, error_message).
    """
    node = root
    for position, token in enumerate(tokens[:-1]):
        found, child = resolve_token(node, token)
        if found:
            node = child
            continue
        if isinstance(node, dict):
            node[token] = {}
            node = node[token]
            continue
        joined = ".".join(str(item) for item in tokens[:position + 1])
        return None, "cannot create parent at: " + joined
    return node, ""


def apply_set(parent, token, value):
    """Set a dict key or replace a list slot (append when index == len)."""
    if isinstance(parent, dict):
        parent[token] = value
        return ""
    if isinstance(parent, list):
        index = normalize_list_index(parent, token)
        if index is not None:
            parent[index] = value
            return ""
        if token in (len(parent), str(len(parent))):
            parent.append(value)
            return ""
        return "list index out of range: " + str(token)
    return "parent is not a container"


def apply_update(parent, token, value):
    """Shallow-merge a dict patch into an existing dict target."""
    found, target = resolve_token(parent, token)
    if not found:
        return "update target missing: " + str(token)
    if not isinstance(target, dict) or not isinstance(value, dict):
        return "update requires dict target and dict patch"
    target.update(copy.deepcopy(value))
    return ""


def apply_append(parent, token, value):
    """Append one element to an existing list target."""
    found, target = resolve_token(parent, token)
    if not found or not isinstance(target, list):
        return "append target is not a list: " + str(token)
    target.append(value)
    return ""


def apply_delete(parent, token):
    """Delete a dict key or list element."""
    if isinstance(parent, dict):
        if token in parent:
            del parent[token]
            return ""
        return "delete key missing: " + str(token)
    if isinstance(parent, list):
        index = normalize_list_index(parent, token)
        if index is not None:
            del parent[index]
            return ""
        return "delete index out of range: " + str(token)
    return "parent is not a container"


def apply_mutation(root, tokens, mode, value):
    """Dispatch one generic mutation; returns "" on success."""
    if not tokens:
        return "empty path is not mutable"
    parent, error = ensure_parent_containers(root, tokens)
    if error:
        return error
    token = tokens[-1]
    if mode == "set":
        return apply_set(parent, token, copy.deepcopy(value))
    if mode == "update":
        return apply_update(parent, token, value)
    if mode == "append":
        return apply_append(parent, token, copy.deepcopy(value))
    if mode == "delete":
        return apply_delete(parent, token)
    return "unknown mutation mode: " + str(mode)


def request_write_commit(context, path, value=None, **kwargs):
    """
    THE single mutation entry point (Single-Writer authority).

    kwargs:
        mode    "set" | "update" | "append" | "delete"   (default "set")
        source  free-form origin marker                  (default "internal")
        verify  full config verification after mutation  (default False)

    Protocol (spec compliant): the mutation is applied to an isolated
    working copy under the central lock (copy-on-write), optionally
    verified, then swapped in atomically and the revision is bumped -
    a rejected commit therefore NEVER touches the heap. Optional
    auto-persistence runs and ONLY THEN the EVENT_HEAP_COMMIT sync chain
    is dispatched. The calling thread returns after central write +
    downstream dispatch completed.
    """
    mode = kwargs.get("mode", "set")
    source = kwargs.get("source", "internal")
    verify = kwargs.get("verify", False)
    tokens = parse_data_path(path)
    result = {"status": "OK", "message": "", "revision": 0, "path": path}
    with context["heap_lock"]:
        working = copy.deepcopy(context["heap"])
        error = apply_mutation(working, tokens, mode, value)
        if error == "" and verify:
            verify_report = type_library.verify_runtime_config_data(working)
            if verify_report["status"] != "OK":
                error = "verification failed after mutation"
        if error:
            context["statistics"]["rejected_commits"] += 1
            result["status"] = "ERROR"
            result["message"] = error
            result["revision"] = context["revision"]
        else:
            context["heap"] = working
            context["revision"] += 1
            context["dirty"] = True
            context["statistics"]["commits"] += 1
            result["message"] = "commit applied"
            result["revision"] = context["revision"]
    payload = {"path": str(path), "mode": mode, "source": source,
               "revision": result["revision"],
               "section": str(tokens[0]) if tokens else ""}
    if result["status"] != "OK":
        payload["error"] = result["message"]
        LOGGER.warning("write rejected (%s): %s", source, result["message"])
        publish_event(context, EVENT_WRITE_REJECTED, payload)
        return result
    autosync_after_commit(context)
    publish_event(context, EVENT_HEAP_COMMIT, payload)
    return result


def _legacy_storage_allowed(context, config, explicit=False):
    version = config_compatibility.get_config_version(config)
    if version != 5:
        return True
    return bool(explicit or context["options"].get("allow_legacy_import"))


def _prepare_full_config(context, config, representation, allow_legacy=False):
    representation = str(representation or "").strip().lower()
    if representation not in ("storage", "runtime", "legacy_storage"):
        raise ValueError(
            "full config commit requires explicit storage or runtime representation"
        )
    if representation in ("storage", "legacy_storage"):
        version = config_compatibility.get_config_version(config)
        if representation == "legacy_storage" and version != 5:
            raise ValueError("legacy_storage accepts only schema contract 5")
        if not _legacy_storage_allowed(
                context, config, explicit=allow_legacy or representation == "legacy_storage"):
            raise ValueError("schema contract 5 requires explicit legacy import")
        storage_report = type_library.verify_storage_config_data(config)
        if storage_report["status"] != "OK":
            raise ValueError("storage verification failed")
        runtime = type_library.normalize_config_for_runtime(
            config, representation="storage")
    else:
        runtime = type_library.deep_copy_config(config)
    runtime_report = type_library.verify_runtime_config_data(runtime)
    if runtime_report["status"] != "OK":
        raise ValueError("runtime verification failed")
    return runtime


def commit_full_config(context, config, source="external_sync", **kwargs):
    """
    Replace the complete heap in one verified atomic step (external or
    internal master synchronisation). Rejects unverified data.
    """
    representation = kwargs.get("representation")
    try:
        runtime_config = _prepare_full_config(
            context,
            config,
            representation,
            allow_legacy=kwargs.get("allow_legacy", False),
        )
    except (TypeError, ValueError) as exc:
        LOGGER.error("full commit rejected from %s", source)
        publish_event(context, EVENT_WRITE_REJECTED,
                      {"path": "", "mode": "replace", "source": source,
                       "revision": get_revision(context),
                       "error": str(exc)})
        return report_error("full commit rejected: " + str(exc))
    with context["heap_lock"]:
        context["heap"] = type_library.deep_copy_config(runtime_config)
        context["revision"] += 1
        context["dirty"] = True
        context["statistics"]["commits"] += 1
        revision = context["revision"]
    autosync_after_commit(context)
    publish_event(context, EVENT_HEAP_COMMIT,
                  {"path": "", "mode": "replace", "source": source,
                   "revision": revision, "section": ""})
    report = report_ok("full config committed")
    report["details"]["revision"] = revision
    report["details"]["fingerprint"] = get_fingerprint(context)
    return report


def autosync_after_commit(context):
    """Optional synchronous persistence directly after a commit."""
    if context["options"]["autosave_json"] and context["options"]["json_path"]:
        save_heap_to_json(context)
    if context["options"]["autosync_sqlite"] and context["options"]["sqlite_path"]:
        sync_heap_to_sqlite(context)

# =============================================================================
# CODEBASE - FUNCTIONS - PART 5: JSON PERSISTENCE (LOAD / SAVE / UPDATE)
# =============================================================================

def read_json_file(file_path, encoding):
    """
    Raw JSON read with best-effort repair fallback from _json_formator.
    Returns (data_or_none, error_message).
    """
    try:
        handle = open(file_path, "r", encoding=encoding)
        try:
            raw_text = handle.read()
        finally:
            handle.close()
    except OSError as exc:
        return None, "failed to read " + str(file_path) + ": " + str(exc)
    try:
        return json.loads(raw_text), ""
    except ValueError:
        pass
    try:
        repaired = json_formator.json_repair(raw_text)
        LOGGER.warning("json repaired on load: %s", file_path)
        return json.loads(repaired), ""
    except ValueError as exc:
        return None, "failed to parse " + str(file_path) + ": " + str(exc)


def _load_config_with_explicit_legacy_import(target, options):
    """Load strict storage or run the one explicit V5-to-V9 import boundary."""
    config, verify_report = type_library.load_config_file(target)
    if config is not None or not options.get("allow_legacy_import"):
        return config, verify_report

    source, read_error = read_json_file(target, options["encoding"])
    meta_rows = source.get("meta") if isinstance(source, dict) else []
    meta = meta_rows[0] if isinstance(meta_rows, list) and meta_rows else {}
    source_version = meta.get("version") if isinstance(meta, dict) else None
    if read_error or source_version != type_library.LEGACY_VERSION:
        return config, verify_report

    try:
        migration = config_graph.migrate_legacy_config(
            source,
            profile=options.get("legacy_import_profile") or "production_v2",
        )
        if not migration.get("ok"):
            failed = dict(verify_report or {})
            failed["status"] = "FAILED"
            failed.setdefault("errors", []).append({
                "path": "rules",
                "error": "explicit legacy graph migration is incomplete",
            })
            failed.setdefault("stats", {})["migration_report"] = (
                copy.deepcopy(migration.get("report") or {}))
            return None, failed
        migrated = migration["config"]
        verify_report = type_library.verify_storage_config_data(migrated)
        if verify_report.get("status") != "OK":
            return None, verify_report
        config = type_library.normalize_config_for_runtime(
            migrated,
            representation="storage",
        )
        verify_report["stats"]["fingerprint"] = (
            type_library.config_fingerprint(migrated)
        )
        verify_report["stats"]["explicit_legacy_import"] = True
        verify_report["stats"]["source_schema_version"] = (
            type_library.LEGACY_VERSION
        )
        verify_report["stats"]["migration_report"] = copy.deepcopy(
            migration.get("report") or {})
        return config, verify_report
    except (TypeError, ValueError) as exc:
        failed = dict(verify_report or {})
        failed["status"] = "FAILED"
        failed.setdefault("errors", []).append({
            "path": "meta[0].version",
            "error": "explicit legacy import failed: " + str(exc),
        })
        return None, failed


def load_json_into_heap(context, file_path=None):
    """
    Load the complete config JSON into the MEM_HEAP.

    _generic_type_library.load_config_file() strictly validates the declared
    storage representation before normalization. ``verify_on_load`` remains a
    compatibility option but can no longer bypass this trust boundary.
    Publishes EVENT_HEAP_LOADED on success.
    """
    options = context["options"]
    target = file_path if file_path else options["json_path"]
    if not target:
        return report_error("no json_path configured for load")
    config, verify_report = _load_config_with_explicit_legacy_import(
        target, options)
    if verify_report.get("stats", {}).get("explicit_legacy_import"):
        LOGGER.warning("explicit V5 import migrated config on load: %s", target)
    if config is None:
        LOGGER.error("verified load failed: %s", target)
        report = report_error("verified load failed: " + str(target))
        report["details"]["verification"] = verify_report
        return report
    version = int(verify_report.get("stats", {}).get("schema_version") or 0)
    if version == 5 and not options.get("allow_legacy_import"):
        report = report_error(
            "verified load rejected: schema contract 5 requires explicit legacy import")
        report["details"]["verification"] = verify_report
        return report
    with context["heap_lock"]:
        context["heap"] = config
        context["revision"] += 1
        context["dirty"] = False
        context["statistics"]["loads"] += 1
        revision = context["revision"]
        if not options["json_path"]:
            options["json_path"] = target
    fingerprint = get_fingerprint(context)
    with context["heap_lock"]:
        context["last_persisted_fingerprint"] = fingerprint
    publish_event(context, EVENT_HEAP_LOADED,
                  {"path": str(target), "revision": revision,
                   "fingerprint": fingerprint,
                   "sections": len(list_sections(context))})
    LOGGER.info("heap loaded: %s (revision %d)", target, revision)
    report = report_ok("heap loaded: " + str(target))
    report["details"]["revision"] = revision
    report["details"]["fingerprint"] = fingerprint
    report["details"]["verification"] = verify_report
    return report


def render_heap_json(context, snapshot):
    """
    Render one heap snapshot into its deterministic textual JSON form.
        "table"     numeric_pk pre-sort + one record per line (formator)
        "canonical" byte-stable minimal form (type library)
    """
    storage_snapshot = config_compatibility.project_config_for_storage(snapshot)
    style = context["options"]["json_style"]
    if style == "canonical":
        return type_library.canonical_dumps(storage_snapshot)
    sorted_data, _ = json_formator.sort_json_data(
        storage_snapshot, sort_mode=context["options"]["sort_mode"])
    return json_formator.format_json(
        sorted_data, indent_size=context["options"]["indent_size"])


def _json_receipt_path(context, target):
    """Return the provenance sidecar only for the configured live JSON."""
    receipt_path = context["options"].get("json_receipt_path")
    configured_path = context["options"].get("json_path")
    if not receipt_path or not configured_path:
        return None
    if os.path.abspath(str(target)) != os.path.abspath(str(configured_path)):
        return None
    return os.path.abspath(str(receipt_path))


def _write_json_receipt(context, receipt_path, target, fingerprint,
                        previous_fingerprint, revision, state):
    payload = {
        "version": 1,
        "state": str(state),
        "target_path": os.path.abspath(str(target)),
        "target_fingerprint": str(fingerprint),
        "previous_fingerprint": str(previous_fingerprint or ""),
        "revision": int(revision),
        "saved_at_s": current_timestamp(),
    }
    atomic_write_text(
        receipt_path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")),
        "utf-8",
    )
    with context["heap_lock"]:
        context["statistics"]["json_receipts"] += 1
    return payload


def _external_config_fingerprint(
        context, target, target_fingerprint,
        expected_external_fingerprint=""):
    """Return a conflicting external file identity without overwriting it."""
    if not os.path.exists(target):
        return ""
    current, report = _load_config_with_explicit_legacy_import(
        target, context["options"])
    if current is None or report.get("status") != "OK":
        raise ValueError("existing config file is not a valid config")
    current_fingerprint = type_library.config_fingerprint(current)
    previous_fingerprint = str(
        context.get("last_persisted_fingerprint") or "")
    accepted_fingerprints = {
        previous_fingerprint,
        str(target_fingerprint or ""),
    }
    expected_fingerprint = str(expected_external_fingerprint or "")
    if expected_fingerprint:
        accepted_fingerprints.add(expected_fingerprint)
    if current_fingerprint in accepted_fingerprints:
        return ""
    return current_fingerprint


def _external_edit_report(target, fingerprint):
    report = report_error("save deferred: external config edit is pending")
    report["details"]["path"] = str(target)
    report["details"]["external_fingerprint"] = str(fingerprint)
    return report


def save_heap_to_json(context, file_path=None, **kwargs):
    """
    Persist the complete heap atomically to JSON. The snapshot is taken
    under the lock, verification and file IO run outside the lock so
    readers and writers are never blocked by disk latency.
    Publishes EVENT_HEAP_SAVED on success.
    """
    options = context["options"]
    target = file_path if file_path else options["json_path"]
    expected_external_fingerprint = str(
        kwargs.get("expected_external_fingerprint") or "")
    if not target:
        return report_error("no json_path configured for save")
    found, envelope = heap_snapshot_envelope(context)
    if not found:
        return report_error("heap is empty - nothing to save")
    snapshot = envelope["snapshot"]
    revision = envelope["revision"]
    snapshot_fingerprint = envelope["fingerprint"]
    try:
        rendered_json = render_heap_json(context, snapshot)
        persisted_snapshot = json.loads(rendered_json)
    except (TypeError, ValueError) as exc:
        return report_error("failed to render " + str(target) + ": " + str(exc))
    if options["verify_on_save"]:
        runtime_report = type_library.verify_runtime_config_data(snapshot)
        verify_report = type_library.verify_storage_config_data(persisted_snapshot)
        if runtime_report["status"] != "OK" or verify_report["status"] != "OK":
            LOGGER.error("refusing to save unverified heap: %s", target)
            report = report_error("save rejected: verification failed")
            report["details"]["runtime_verification"] = runtime_report
            report["details"]["verification"] = verify_report
            return report
    fingerprint = type_library.config_fingerprint(persisted_snapshot)
    if (
        options.get("preserve_config_identity_on_save")
        and fingerprint != snapshot_fingerprint
    ):
        LOGGER.error(
            "refusing identity-changing JSON render: %s (%s -> %s)",
            target,
            snapshot_fingerprint,
            fingerprint,
        )
        report = report_error(
            "save rejected: rendered JSON changes config identity")
        report["details"]["snapshot_fingerprint"] = snapshot_fingerprint
        report["details"]["rendered_fingerprint"] = fingerprint
        return report
    # The generic formatter may intentionally reorder record collections when
    # the strict identity guard is disabled. Attest the exact validated JSON
    # that will reach disk so the receipt remains a crash-recovery boundary.
    receipt_path = _json_receipt_path(context, target)
    try:
        with context["persistence_lock"]:
            previous_fingerprint = str(
                context.get("last_persisted_fingerprint") or "")
            external_fingerprint = _external_config_fingerprint(
                context, target, fingerprint,
                expected_external_fingerprint)
            if external_fingerprint:
                return _external_edit_report(target, external_fingerprint)
            if receipt_path:
                _write_json_receipt(
                    context, receipt_path, target, fingerprint,
                    previous_fingerprint, revision, "PREPARED")
                external_fingerprint = _external_config_fingerprint(
                    context, target, fingerprint,
                    expected_external_fingerprint)
                if external_fingerprint:
                    return _external_edit_report(target, external_fingerprint)
            atomic_write_text(target, rendered_json, options["encoding"])
            if receipt_path:
                _write_json_receipt(
                    context, receipt_path, target, fingerprint,
                    previous_fingerprint, revision, "COMMITTED")
            with context["heap_lock"]:
                context["last_persisted_fingerprint"] = fingerprint
    except (OSError, ValueError) as exc:
        return report_error("failed to write " + str(target) + ": " + str(exc))
    with context["heap_lock"]:
        if context["revision"] == revision:
            context["dirty"] = False
        context["statistics"]["json_saves"] += 1
    publish_event(context, EVENT_HEAP_SAVED,
                  {"path": str(target), "revision": revision,
                   "fingerprint": fingerprint,
                   "style": options["json_style"]})
    LOGGER.info("heap saved: %s (revision %d)", target, revision)
    report = report_ok("heap saved: " + str(target))
    report["details"]["revision"] = revision
    report["details"]["fingerprint"] = fingerprint
    return report


def update_json_value(context, path, value, **kwargs):
    """
    Convenience write-through: one commit followed by one JSON save.
    kwargs are forwarded to request_write_commit (mode, source, verify).
    """
    result = request_write_commit(context, path, value, **kwargs)
    if result["status"] != "OK":
        return result
    save_report = save_heap_to_json(context, kwargs.get("file_path"))
    result["message"] = "commit applied + " + save_report["message"]
    result["status"] = save_report["status"]
    return result

# =============================================================================
# CODEBASE - FUNCTIONS - PART 6: SQLITE MIRRORING (FULL DATASET)
# =============================================================================

def section_rows_for_sql(section_value):
    """Normalize one section into (rows, is_list) for table storage."""
    if isinstance(section_value, list):
        return section_value, 1
    return [section_value], 0


def section_kind(section_name):
    """Structural kind from the type library registry (meta information)."""
    registry = type_library.CONTEXT.get("config_schema_registry", {})
    entry = registry.get(section_name, {})
    return str(entry.get("kind", "table"))


def build_section_ddl(table_name, columns, column_types):
    """CREATE TABLE statement with inferred typed columns + row json."""
    parts = ['"_row_index" INTEGER PRIMARY KEY']
    for column in columns:
        parts.append(quote_sql_identifier(column) + " " + column_types[column])
    parts.append('"_row_json" TEXT NOT NULL')
    return ("CREATE TABLE " + quote_sql_identifier(table_name) +
            " (" + ", ".join(parts) + ")")


def build_section_insert(table_name, columns):
    """Parametrised INSERT statement matching build_section_ddl()."""
    names = ['"_row_index"']
    for column in columns:
        names.append(quote_sql_identifier(column))
    names.append('"_row_json"')
    marks = ", ".join(["?"] * len(names))
    return ("INSERT INTO " + quote_sql_identifier(table_name) +
            " (" + ", ".join(names) + ") VALUES (" + marks + ")")


def build_section_row_values(row, row_index, columns):
    """Cell tuple for one row: index, typed cells, lossless row json."""
    cells = [row_index]
    source = row if isinstance(row, dict) else {}
    for column in columns:
        cells.append(encode_sql_value(source.get(column)))
    cells.append(type_library.canonical_dumps(row))
    return tuple(cells)


def write_section_table(cursor, section_name, rows):
    """Rebuild one section table deterministically inside the open tx."""
    columns, column_types = infer_section_columns(rows)
    cursor.execute("DROP TABLE IF EXISTS " + quote_sql_identifier(section_name))
    cursor.execute(build_section_ddl(section_name, columns, column_types))
    insert_sql = build_section_insert(section_name, columns)
    build_values = partial(build_section_row_values, columns=columns)
    cursor.executemany(insert_sql, [
        build_values(row, row_index) for row_index, row in enumerate(rows)])
    return len(rows)


def write_registry_tables(cursor, context, snapshot, revision, fingerprint):
    """Rebuild meta + section registry tables for the mirrored dataset."""
    cursor.execute("DROP TABLE IF EXISTS " + quote_sql_identifier(SQL_META_TABLE))
    cursor.execute("CREATE TABLE " + quote_sql_identifier(SQL_META_TABLE) +
                   ' ("key" TEXT PRIMARY KEY, "value" TEXT NOT NULL)')
    meta_rows = [
        ("library", context["name"]),
        ("library_version", context["library_version"]),
        ("revision", str(revision)),
        ("fingerprint", str(fingerprint)),
        ("synced_at", repr(current_timestamp())),
        ("section_count", str(len(snapshot))),
    ]
    cursor.executemany("INSERT INTO " + quote_sql_identifier(SQL_META_TABLE) +
                       " VALUES (?, ?)", meta_rows)
    cursor.execute("DROP TABLE IF EXISTS " + quote_sql_identifier(SQL_REGISTRY_TABLE))
    cursor.execute("CREATE TABLE " + quote_sql_identifier(SQL_REGISTRY_TABLE) +
                   ' ("section" TEXT PRIMARY KEY, "table_name" TEXT NOT NULL,'
                   ' "kind" TEXT NOT NULL, "is_list" INTEGER NOT NULL,'
                   ' "row_count" INTEGER NOT NULL)')
    registry_rows = []
    for section_name in snapshot.keys():
        rows, is_list = section_rows_for_sql(snapshot[section_name])
        registry_rows.append((section_name,
                              sanitize_sql_identifier(section_name),
                              section_kind(section_name), is_list, len(rows)))
    cursor.executemany("INSERT INTO " + quote_sql_identifier(SQL_REGISTRY_TABLE) +
                       " VALUES (?, ?, ?, ?, ?)", registry_rows)


def sync_heap_to_sqlite(context, db_path=None):
    """
    Mirror the COMPLETE heap into SQLite: one dynamically typed table per
    section (queryable columns + lossless "_row_json") plus meta and
    registry tables - rebuilt deterministically in ONE transaction.
    Requires sqlite_enabled or an explicit db_path (master switch).
    Publishes EVENT_SQLITE_SYNCED on success.
    """
    options = context["options"]
    target = db_path if db_path else options["sqlite_path"]
    if not options["sqlite_enabled"] and not db_path:
        return report_error("sqlite mirroring is disabled")
    if not target:
        return report_error("no sqlite_path configured")
    found, envelope = heap_snapshot_envelope(context)
    if not found:
        return report_error("heap is empty - nothing to sync")
    snapshot = envelope["snapshot"]
    if not isinstance(snapshot, dict) or not snapshot:
        return report_error("heap is empty - nothing to sync")
    revision = envelope["revision"]
    fingerprint = envelope["fingerprint"]
    row_totals = {}
    try:
        connection = sqlite3.connect(target)
        try:
            cursor = connection.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            for section_name in snapshot.keys():
                rows, _ = section_rows_for_sql(snapshot[section_name])
                row_totals[section_name] = write_section_table(
                    cursor, section_name, rows)
            write_registry_tables(
                cursor, context, snapshot, revision, fingerprint)
            connection.commit()
        finally:
            connection.close()
    except (sqlite3.Error, OSError, ValueError) as exc:
        LOGGER.error("sqlite sync failed: %s", exc)
        return report_error("sqlite sync failed: " + str(exc))
    with context["heap_lock"]:
        context["statistics"]["sqlite_syncs"] += 1
    publish_event(context, EVENT_SQLITE_SYNCED,
                  {"path": str(target), "revision": revision,
                   "tables": len(row_totals),
                   "rows": sum(row_totals.values())})
    LOGGER.info("sqlite synced: %s (%d tables)", target, len(row_totals))
    report = report_ok("sqlite synced: " + str(target))
    report["details"]["revision"] = revision
    report["details"]["tables"] = row_totals
    return report


def load_heap_from_sqlite(context, db_path=None):
    """
    Lossless restore of the complete dataset from a mirrored database:
    sections are rebuilt from "_row_json" in original row order, then
    committed through the verified full-config path.
    """
    options = context["options"]
    target = db_path if db_path else options["sqlite_path"]
    if not target:
        return report_error("no sqlite_path configured")
    config = {}
    try:
        connection = sqlite3.connect(target)
        try:
            cursor = connection.cursor()
            cursor.execute('SELECT "section", "table_name", "is_list" FROM ' +
                           quote_sql_identifier(SQL_REGISTRY_TABLE) +
                           ' ORDER BY "section"')
            for section_name, table_name, is_list in cursor.fetchall():
                row_cursor = connection.cursor()
                row_cursor.execute('SELECT "_row_json" FROM ' +
                                   quote_sql_identifier(table_name) +
                                   ' ORDER BY "_row_index"')
                rows = [json.loads(item[0]) for item in row_cursor.fetchall()]
                config[section_name] = rows if is_list else rows[0]
        finally:
            connection.close()
    except (sqlite3.Error, OSError, ValueError) as exc:
        LOGGER.error("sqlite load failed: %s", exc)
        return report_error("sqlite load failed: " + str(exc))
    return commit_full_config(
        context, config, source="sqlite:" + str(target),
        representation="runtime")

# =============================================================================
# CODEBASE - FUNCTIONS - PART 7: GENERIC SHARDING & WORKER SYNCHRONISATION
# =============================================================================

def row_shard_key(row, shard_field):
    """
    Generic partition rule: a dict row carrying the shard field belongs
    to that shard; every other row belongs to the global shard.
    """
    if isinstance(row, dict) and shard_field in row:
        return str(row[shard_field])
    return GLOBAL_SHARD_KEY


def build_shard_index_from(config, shard_field):
    """
    Pure index computation over any config mapping (no locking, no copy).
    Deterministic structure:
        { shard_key: { section_name: [row_index, ...] } }
    Sections that are no lists are treated as global singletons.
    """
    index = {GLOBAL_SHARD_KEY: {}}
    for section_name in config.keys():
        section_value = config[section_name]
        if not isinstance(section_value, list):
            index[GLOBAL_SHARD_KEY].setdefault(section_name, []).append(0)
            continue
        for row_position, row in enumerate(section_value):
            shard_key = row_shard_key(row, shard_field)
            shard_sections = index.setdefault(shard_key, {})
            shard_sections.setdefault(section_name, []).append(row_position)
    return index


def build_shard_index(context, shard_field=None):
    """
    Partition every section generically by the shard field (default from
    options, normally "controller_uuid") on a consistent heap snapshot.
    """
    field = shard_field if shard_field else context["options"]["shard_field"]
    found, snapshot = heap_snapshot(context)
    if not found:
        return {}
    return build_shard_index_from(snapshot, field)


def list_shard_keys(context, shard_field=None):
    """Deterministic sorted list of all shard keys (global shard first)."""
    index = build_shard_index(context, shard_field)
    keys = sorted(key for key in index.keys() if key != GLOBAL_SHARD_KEY)
    return [GLOBAL_SHARD_KEY] + keys


def get_shard_snapshot(context, shard_key, **kwargs):
    """
    Isolated deep-copy dataset for ONE worker/controller thread.

    kwargs:
        shard_field     partition field override
        include_global  attach the global reference shard (default True)

    Returns a self-describing shard package:
        { "shard_key", "shard_field", "revision", "fingerprint",
          "snapshot_at", "sections": { section: [rows...] } }
    Workers consume this copy lock-free; the central heap stays isolated.
    """
    field = kwargs.get("shard_field") or context["options"]["shard_field"]
    include_global = kwargs.get("include_global", True)
    with context["heap_lock"]:
        index = build_shard_index_from(context["heap"], field)
        revision = context["revision"]
        fingerprint = get_fingerprint(context)
        wanted = {}
        for source_key in (GLOBAL_SHARD_KEY, str(shard_key)):
            if source_key == GLOBAL_SHARD_KEY and not include_global:
                continue
            for section_name, positions in index.get(source_key, {}).items():
                wanted.setdefault(section_name, []).extend(positions)
        sections = {}
        for section_name in sorted(wanted.keys()):
            section_value = context["heap"][section_name]
            if not isinstance(section_value, list):
                sections[section_name] = copy.deepcopy(section_value)
                continue
            positions = sorted(set(wanted[section_name]))
            sections[section_name] = [
                copy.deepcopy(section_value[position]) for position in positions]
        context["statistics"]["shard_snapshots"] += 1
    return {
        "shard_key": str(shard_key),
        "shard_field": field,
        "revision": revision,
        "fingerprint": fingerprint,
        "snapshot_at": current_timestamp(),
        "sections": sections,
    }


def publish_shard_sync(context, shard_key, **kwargs):
    """
    Push-style downstream synchronisation: build the shard snapshot and
    publish it as EVENT_SHARD_SYNC. Runs strictly AFTER the central heap
    is up to date - the natural continuation of the commit sync chain.
    """
    package = get_shard_snapshot(context, shard_key, **kwargs)
    payload = {"path": "shard:" + package["shard_key"],
               "revision": package["revision"],
               "shard_key": package["shard_key"],
               "package": package}
    publish_event(context, EVENT_SHARD_SYNC, payload)
    return package

# =============================================================================
# INTERFACE - EXTERNAL CALLABLE
# =============================================================================

def initialize_data_center(**options):
    """
    Central initialisation: (re)build the default module context - the
    one Config-Master authority instance of the running system - and
    attach the minimalistic default event logger for commits.
    """
    global DATA_CENTER_CONTEXT
    DATA_CENTER_CONTEXT = create_data_center_context(**options)
    log_handler = partial(log_event, LOGGER)
    subscribe_event(DATA_CENTER_CONTEXT, EVENT_HEAP_COMMIT, log_handler)
    LOGGER.debug("default data center context initialised")
    return DATA_CENTER_CONTEXT


def get_data_center():
    """Access the default module context (Config-Master authority)."""
    return DATA_CENTER_CONTEXT


initialize_data_center()




