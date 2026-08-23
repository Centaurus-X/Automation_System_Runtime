# -*- coding: utf-8 -*-

# src/library/_distributed_operation_store.py


"""
 =============================================================================
 DISTRIBUTED_OPERATION_STORE
 =============================================================================

 Durable SQLite journal for distributed inbox, outbox, node registry and
 actuator-operation state. Every public read returns detached JSON-compatible
 data. The store never retries a hardware operation by itself.
 =============================================================================
"""


import copy
import os
import sqlite3
import threading
import time

from src.library import _generic_type_library as type_authority
from src.library._generic_system_contract_library import authority_flow_contracts
from src.library._generic_system_contract_library import persistence_contracts


CONTEXT = {
    "library_name": "distributed_operation_store",
    "library_version": "2.0.0",
}


def _is_dict(value):
    return isinstance(value, dict)


def _json_text(value):
    return type_authority.canonical_text(type_authority.json_ready(value))


def _json_value(value, default=None):
    if value in (None, ""):
        return copy.deepcopy(default)
    try:
        return type_authority.decode_json_bytes(str(value).encode("utf-8"))
    except Exception:
        return copy.deepcopy(default)


def payload_fingerprint(value):
    return type_authority.fingerprint(value)


def _ensure_parent(path):
    if path == ":memory:":
        return
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def _configure_connection(connection):
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    if str(connection.execute("PRAGMA database_list").fetchone()[2]):
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")


def _create_schema(connection):
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS inbox (
            message_id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            state TEXT NOT NULL,
            result_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS outbox (
            message_id TEXT PRIMARY KEY,
            target_node TEXT NOT NULL,
            command TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            next_attempt_at REAL NOT NULL,
            result_json TEXT,
            last_error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS outbox_due_idx
            ON outbox(state, next_attempt_at);
        CREATE TABLE IF NOT EXISTS actuator_operations (
            operation_id TEXT PRIMARY KEY,
            direction TEXT NOT NULL,
            source_node TEXT NOT NULL,
            target_node TEXT NOT NULL,
            controller_uuid TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            task_json TEXT NOT NULL,
            state TEXT NOT NULL,
            fencing_term INTEGER NOT NULL,
            result_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS actuator_state_idx
            ON actuator_operations(direction, state);
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            role TEXT NOT NULL,
            state TEXT NOT NULL,
            authority_revision INTEGER NOT NULL,
            fingerprint TEXT NOT NULL,
            capabilities_json TEXT NOT NULL,
            last_seen_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scope_heads (
            scope TEXT PRIMARY KEY,
            authority_mode TEXT NOT NULL,
            authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0),
            scope_revision INTEGER NOT NULL CHECK (scope_revision >= 0),
            execution_epoch INTEGER NOT NULL CHECK (execution_epoch >= 0),
            fingerprint TEXT,
            locked INTEGER NOT NULL CHECK (locked IN (0, 1)),
            node_authoritative_committed INTEGER NOT NULL CHECK (
                node_authoritative_committed IN (0, 1)),
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS authority_journal (
            record_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            scope TEXT NOT NULL,
            authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0),
            scope_revision INTEGER NOT NULL CHECK (scope_revision >= 0),
            execution_epoch INTEGER NOT NULL CHECK (execution_epoch >= 0),
            fingerprint TEXT,
            record_json TEXT NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS authority_journal_scope_idx
            ON authority_journal(scope, authority_epoch, scope_revision);
        CREATE TABLE IF NOT EXISTS mirror_outbox (
            mirror_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0),
            scope_revision INTEGER NOT NULL CHECK (scope_revision >= 0),
            fingerprint TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL CHECK (attempts >= 0),
            record_json TEXT NOT NULL,
            next_attempt_at REAL NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(scope, authority_epoch, scope_revision, fingerprint)
        );
        CREATE INDEX IF NOT EXISTS mirror_outbox_due_idx
            ON mirror_outbox(state, next_attempt_at);
        CREATE TABLE IF NOT EXISTS authority_transfers (
            transfer_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            state TEXT NOT NULL,
            transfer_json TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS restore_intents (
            restore_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            state TEXT NOT NULL,
            restore_json TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS registry_view (
            registry_id TEXT PRIMARY KEY,
            authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0),
            execution_epoch INTEGER NOT NULL CHECK (execution_epoch >= 0),
            fingerprint TEXT NOT NULL,
            registry_json TEXT NOT NULL,
            accepted_at REAL NOT NULL
        );
        """
    )


def create_operation_store(path):
    target = str(path or ":memory:")
    _ensure_parent(target)
    connection = sqlite3.connect(
        target, timeout=5.0, check_same_thread=False, isolation_level=None)
    _configure_connection(connection)
    _create_schema(connection)
    return {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "path": target,
        "connection": connection,
        "lock": threading.RLock(),
        "closed": False,
    }


def _begin(context):
    context["connection"].execute("BEGIN IMMEDIATE")


def _commit(context):
    context["connection"].execute("COMMIT")


def _rollback(context):
    try:
        context["connection"].execute("ROLLBACK")
    except Exception:
        pass


def get_metadata(context, key, default=None):
    with context["lock"]:
        row = context["connection"].execute(
            "SELECT value_json FROM metadata WHERE key=?", (str(key),)
        ).fetchone()
    return _json_value(row["value_json"], default) if row else copy.deepcopy(default)


def set_metadata(context, key, value):
    now = time.time()
    with context["lock"]:
        context["connection"].execute(
            """
            INSERT INTO metadata(key, value_json, updated_at) VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json, updated_at=excluded.updated_at
            """,
            (str(key), _json_text(value), now),
        )
    return copy.deepcopy(value)


def increment_metadata(context, key, initial=0):
    with context["lock"]:
        _begin(context)
        try:
            row = context["connection"].execute(
                "SELECT value_json FROM metadata WHERE key=?", (str(key),)
            ).fetchone()
            current = _json_value(row["value_json"], initial) if row else initial
            value = int(current) + 1
            context["connection"].execute(
                """
                INSERT INTO metadata(key, value_json, updated_at) VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json=excluded.value_json, updated_at=excluded.updated_at
                """,
                (str(key), _json_text(value), time.time()),
            )
            _commit(context)
            return value
        except Exception:
            _rollback(context)
            raise


def register_inbox(context, message_id, command, **kwargs):
    payload = kwargs.get("payload")
    identity = str(message_id or "")
    fingerprint = payload_fingerprint(payload)
    now = time.time()
    with context["lock"]:
        _begin(context)
        try:
            row = context["connection"].execute(
                "SELECT * FROM inbox WHERE message_id=?", (identity,)
            ).fetchone()
            if row is not None:
                _commit(context)
                conflict = (
                    row["command"] != str(command)
                    or row["payload_hash"] != fingerprint
                )
                return {
                    "status": "conflict" if conflict else "duplicate",
                    "state": row["state"],
                    "result": _json_value(row["result_json"]),
                }
            context["connection"].execute(
                """
                INSERT INTO inbox(
                    message_id, command, payload_hash, state,
                    result_json, created_at, updated_at
                ) VALUES(?, ?, ?, 'RECEIVED', NULL, ?, ?)
                """,
                (identity, str(command), fingerprint, now, now),
            )
            _commit(context)
            return {"status": "new", "state": "RECEIVED", "result": None}
        except Exception:
            _rollback(context)
            raise


def complete_inbox(context, message_id, result, **kwargs):
    state = kwargs.get("state", "COMPLETED")
    with context["lock"]:
        cursor = context["connection"].execute(
            """
            UPDATE inbox SET state=?, result_json=?, updated_at=?
            WHERE message_id=?
            """,
            (str(state), _json_text(result), time.time(), str(message_id)),
        )
    return cursor.rowcount == 1


def enqueue_outbox(context, message_id, **kwargs):
    target_node = kwargs.get("target_node")
    command = kwargs.get("command")
    payload = kwargs.get("payload")
    identity = str(message_id or "")
    fingerprint = payload_fingerprint(payload)
    now = time.time()
    with context["lock"]:
        _begin(context)
        try:
            row = context["connection"].execute(
                "SELECT * FROM outbox WHERE message_id=?", (identity,)
            ).fetchone()
            if row is not None:
                _commit(context)
                conflict = (
                    row["target_node"] != str(target_node)
                    or row["command"] != str(command)
                    or row["payload_hash"] != fingerprint
                )
                return {
                    "status": "conflict" if conflict else "duplicate",
                    "state": row["state"],
                }
            context["connection"].execute(
                """
                INSERT INTO outbox(
                    message_id, target_node, command, payload_hash,
                    payload_json, state, attempts, next_attempt_at,
                    result_json, last_error, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, 'PENDING', 0, ?, NULL, NULL, ?, ?)
                """,
                (identity, str(target_node), str(command), fingerprint,
                 _json_text(payload), now, now, now),
            )
            _commit(context)
            return {"status": "new", "state": "PENDING"}
        except Exception:
            _rollback(context)
            raise


def _outbox_record(row):
    if row is None:
        return None
    result = dict(row)
    result["payload"] = _json_value(result.pop("payload_json"), {})
    result["result"] = _json_value(result.pop("result_json"), None)
    return result


def get_outbox(context, message_id):
    with context["lock"]:
        row = context["connection"].execute(
            "SELECT * FROM outbox WHERE message_id=?", (str(message_id),)
        ).fetchone()
    return _outbox_record(row)


def get_due_outbox(context, limit=64, now=None):
    current = time.time() if now is None else float(now)
    maximum = max(1, int(limit))
    with context["lock"]:
        rows = context["connection"].execute(
            """
            SELECT * FROM outbox
            WHERE state IN ('PENDING', 'AWAITING_ACK')
              AND next_attempt_at <= ?
              AND (
                command != 'distributed.config.propose'
                OR message_id = (
                    SELECT proposal.message_id FROM outbox AS proposal
                    WHERE proposal.command='distributed.config.propose'
                      AND proposal.state IN (
                        'PENDING', 'AWAITING_ACK', 'MASTER_ACKED',
                        'REBASE_REQUIRED'
                      )
                    ORDER BY proposal.created_at, proposal.message_id
                    LIMIT 1
                )
              )
            ORDER BY created_at, message_id LIMIT ?
            """,
            (current, maximum),
        ).fetchall()
    return [_outbox_record(row) for row in rows]


def update_unattempted_outbox_payload(context, message_id, payload):
    """Refresh an unsent proposal base revision without changing its identity."""
    encoded = _json_text(payload)
    fingerprint = payload_fingerprint(payload)
    with context["lock"]:
        cursor = context["connection"].execute(
            """
            UPDATE outbox SET payload_hash=?, payload_json=?, updated_at=?
            WHERE message_id=? AND state='PENDING' AND attempts=0
            """,
            (fingerprint, encoded, time.time(), str(message_id)),
        )
    return cursor.rowcount == 1


def mark_outbox_attempt(context, message_id, **kwargs):
    accepted = kwargs.get("accepted")
    retry_after_s = kwargs.get("retry_after_s")
    error = kwargs.get("error")
    state = "AWAITING_ACK" if accepted else "PENDING"
    now = time.time()
    with context["lock"]:
        cursor = context["connection"].execute(
            """
            UPDATE outbox SET state=?, attempts=attempts+1,
                next_attempt_at=?, last_error=?, updated_at=?
            WHERE message_id=? AND state IN ('PENDING', 'AWAITING_ACK')
            """,
            (state, now + max(0.05, float(retry_after_s)),
             None if error is None else str(error), now, str(message_id)),
        )
    return cursor.rowcount == 1


def complete_outbox(context, message_id, result, **kwargs):
    state = kwargs.get("state")
    if state is None:
        state = "CONFIRMED" if _is_dict(result) and result.get("ok") else "REJECTED"
    with context["lock"]:
        cursor = context["connection"].execute(
            """
            UPDATE outbox SET state=?, result_json=?, updated_at=?
            WHERE message_id=?
            """,
            (state, _json_text(result), time.time(), str(message_id)),
        )
    return cursor.rowcount == 1


def converge_config_proposals(context, authority_revision, fingerprint=""):
    """Mark Master-ACKed config proposals as authoritatively replicated.

    A proposal has two independent application acknowledgements: the Master
    first confirms its durable commit, then the Worker confirms the replicated
    authority revision through ``distributed.config.apply``.  Keeping both
    states in the durable outbox makes a crash between these stages visible and
    recoverable without giving the Worker database access.
    """
    revision = int(authority_revision or 0)
    replicated_fingerprint = str(fingerprint or "")
    changed = 0
    with context["lock"]:
        rows = context["connection"].execute(
            """
            SELECT message_id, result_json FROM outbox
            WHERE command='distributed.config.propose'
              AND state='MASTER_ACKED'
            ORDER BY created_at, message_id
            """
        ).fetchall()
        for row in rows:
            result = _json_value(row["result_json"], {})
            post_commit = result.get("post_commit") if _is_dict(result) else {}
            post_commit = post_commit if _is_dict(post_commit) else {}
            expected_revision = int(
                post_commit.get("authority_revision") or 0)
            expected_fingerprint = str(post_commit.get("fingerprint") or "")
            if expected_revision <= 0 or expected_revision > revision:
                continue
            if (
                expected_revision == revision
                and expected_fingerprint
                and replicated_fingerprint
                and expected_fingerprint != replicated_fingerprint
            ):
                continue
            cursor = context["connection"].execute(
                """
                UPDATE outbox SET state='CONVERGED', updated_at=?
                WHERE message_id=? AND state='MASTER_ACKED'
                """,
                (time.time(), row["message_id"]),
            )
            changed += cursor.rowcount
    return changed


def rebase_config_proposals(context, authority_revision):
    """Requeue stale local-first proposals against a replicated Master base."""
    revision = int(authority_revision or 0)
    changed = 0
    if revision <= 0:
        return changed
    with context["lock"]:
        rows = context["connection"].execute(
            """
            SELECT * FROM outbox
            WHERE command='distributed.config.propose'
              AND state='REBASE_REQUIRED'
            ORDER BY created_at, message_id
            """
        ).fetchall()
        for row in rows:
            payload = _json_value(row["payload_json"], {})
            if payload.get("authority_scope") != "worker_local_runtime":
                continue
            rebase_attempt = int(payload.get("rebase_attempt") or 0) + 1
            payload["authority_revision"] = revision
            payload["rebase_attempt"] = rebase_attempt
            proposal_id = str(
                payload.get("proposal_id") or row["message_id"])
            payload["proposal_id"] = proposal_id
            message_id = "{}:rebase:{}".format(proposal_id, rebase_attempt)
            cursor = context["connection"].execute(
                """
                UPDATE outbox SET message_id=?, payload_hash=?, payload_json=?,
                    state='PENDING', attempts=0, next_attempt_at=?,
                    last_error=NULL, updated_at=?
                WHERE message_id=? AND state='REBASE_REQUIRED'
                """,
                (message_id, payload_fingerprint(payload), _json_text(payload),
                 time.time(), time.time(), row["message_id"]),
            )
            changed += cursor.rowcount
    return changed


def register_actuator_operation(context, operation_id, task, **kwargs):
    identity = str(operation_id or "")
    fingerprint = payload_fingerprint(task)
    now = time.time()
    record = (
        identity,
        str(kwargs.get("direction") or "outbound"),
        str(kwargs.get("source_node") or ""),
        str(kwargs.get("target_node") or ""),
        str(kwargs.get("controller_uuid") or task.get("controller_uuid") or ""),
        fingerprint,
        _json_text(task),
        int(kwargs.get("fencing_term") or 0),
        now,
        now,
    )
    with context["lock"]:
        _begin(context)
        try:
            row = context["connection"].execute(
                "SELECT * FROM actuator_operations WHERE operation_id=?",
                (identity,),
            ).fetchone()
            if row is not None:
                _commit(context)
                conflict = row["payload_hash"] != fingerprint
                return {
                    "status": "conflict" if conflict else "duplicate",
                    "state": row["state"],
                    "result": _json_value(row["result_json"]),
                }
            context["connection"].execute(
                """
                INSERT INTO actuator_operations(
                    operation_id, direction, source_node, target_node,
                    controller_uuid, payload_hash, task_json, state,
                    fencing_term, result_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'RECEIVED', ?, NULL, ?, ?)
                """,
                record,
            )
            _commit(context)
            return {"status": "new", "state": "RECEIVED", "result": None}
        except Exception:
            _rollback(context)
            raise


def _actuator_record(row):
    if row is None:
        return None
    result = dict(row)
    result["task"] = _json_value(result.pop("task_json"), {})
    result["result"] = _json_value(result.pop("result_json"), None)
    return result


def get_actuator_operation(context, operation_id):
    with context["lock"]:
        row = context["connection"].execute(
            "SELECT * FROM actuator_operations WHERE operation_id=?",
            (str(operation_id),),
        ).fetchone()
    return _actuator_record(row)


def list_actuator_operations(context, **kwargs):
    conditions = []
    parameters = []
    if kwargs.get("direction"):
        conditions.append("direction=?")
        parameters.append(str(kwargs["direction"]))
    if kwargs.get("states"):
        states = tuple(str(value) for value in kwargs["states"])
        conditions.append("state IN ({})".format(",".join("?" for _ in states)))
        parameters.extend(states)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    statement = "SELECT * FROM actuator_operations" + where + " ORDER BY created_at"
    with context["lock"]:
        rows = context["connection"].execute(statement, tuple(parameters)).fetchall()
    return [_actuator_record(row) for row in rows]


def transition_actuator_operation(context, operation_id, state, **kwargs):
    allowed = tuple(str(value) for value in kwargs.get("allowed_states") or ())
    parameters = [str(state), _json_text(kwargs.get("result")), time.time(),
                  str(operation_id)]
    clause = ""
    if allowed:
        clause = " AND state IN ({})".format(",".join("?" for _ in allowed))
        parameters.extend(allowed)
    statement = (
        "UPDATE actuator_operations SET state=?, result_json=?, updated_at=? "
        "WHERE operation_id=?" + clause
    )
    with context["lock"]:
        cursor = context["connection"].execute(statement, tuple(parameters))
    return cursor.rowcount == 1


def recover_actuator_operations(context, direction="inbound"):
    with context["lock"]:
        context["connection"].execute(
            """
            UPDATE actuator_operations SET state='UNCERTAIN', updated_at=?
            WHERE direction=? AND state='DISPATCHED'
            """,
            (time.time(), str(direction)),
        )
    return list_actuator_operations(
        context, direction=direction, states=("RECEIVED", "UNCERTAIN"))


def upsert_node(context, node_id, **kwargs):
    now = time.time()
    values = (
        str(node_id), str(kwargs.get("role") or "worker"),
        str(kwargs.get("state") or "online"),
        int(kwargs.get("authority_revision") or 0),
        str(kwargs.get("fingerprint") or ""),
        _json_text(kwargs.get("capabilities") or {}), now, now,
    )
    with context["lock"]:
        context["connection"].execute(
            """
            INSERT INTO nodes(
                node_id, role, state, authority_revision, fingerprint,
                capabilities_json, last_seen_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                role=excluded.role, state=excluded.state,
                authority_revision=excluded.authority_revision,
                fingerprint=excluded.fingerprint,
                capabilities_json=excluded.capabilities_json,
                last_seen_at=excluded.last_seen_at,
                updated_at=excluded.updated_at
            """,
            values,
        )
    return get_node(context, node_id)


def _node_record(row):
    if row is None:
        return None
    result = dict(row)
    result["capabilities"] = _json_value(
        result.pop("capabilities_json"), {})
    return result


def get_node(context, node_id):
    with context["lock"]:
        row = context["connection"].execute(
            "SELECT * FROM nodes WHERE node_id=?", (str(node_id),)
        ).fetchone()
    return _node_record(row)


def list_nodes(context):
    with context["lock"]:
        rows = context["connection"].execute(
            "SELECT * FROM nodes ORDER BY node_id"
        ).fetchall()
    return [_node_record(row) for row in rows]


def _scope_head_record(row):
    if row is None:
        return None
    result = dict(row)
    result["locked"] = bool(result["locked"])
    result["node_authoritative_committed"] = bool(
        result["node_authoritative_committed"])
    result.pop("updated_at", None)
    return result


def ensure_scope_head(context, scope, **kwargs):
    """Create a durable scope head once without overwriting authority state."""
    prepared = authority_flow_contracts.make_scope_head(scope, **kwargs)
    now = time.time()
    with context["lock"]:
        context["connection"].execute(
            """
            INSERT OR IGNORE INTO scope_heads(
                scope, authority_mode, authority_epoch, scope_revision,
                execution_epoch, fingerprint, locked,
                node_authoritative_committed, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prepared["scope"], prepared["authority_mode"],
                prepared["authority_epoch"], prepared["scope_revision"],
                prepared["execution_epoch"], prepared["fingerprint"],
                int(prepared["locked"]),
                int(prepared["node_authoritative_committed"]), now,
            ),
        )
    return get_scope_head(context, prepared["scope"])


def get_scope_head(context, scope):
    """Return one detached persistent authority head."""
    with context["lock"]:
        row = context["connection"].execute(
            "SELECT * FROM scope_heads WHERE scope=?", (str(scope),)
        ).fetchone()
    return _scope_head_record(row)


def _insert_journal(connection, record):
    connection.execute(
        """
        INSERT INTO authority_journal(
            record_id, kind, scope, authority_epoch, scope_revision,
            execution_epoch, fingerprint, record_json, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["record_id"], record["kind"], record["scope"],
            record["authority_epoch"], record["scope_revision"],
            record["execution_epoch"], record.get("fingerprint"),
            _json_text(record), record["created_at_s"],
        ),
    )


def _insert_mirror(connection, record):
    now = time.time()
    connection.execute(
        """
        INSERT INTO mirror_outbox(
            mirror_id, scope, authority_epoch, scope_revision, fingerprint,
            state, attempts, record_json, next_attempt_at, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scope, authority_epoch, scope_revision, fingerprint)
        DO NOTHING
        """,
        (
            record["mirror_id"], record["scope"], record["authority_epoch"],
            record["scope_revision"], record["fingerprint"], record["state"],
            record["attempts"], _json_text(record), now,
            record["created_at_s"], now,
        ),
    )


def commit_scope_state(context, scope_head, journal_record, **kwargs):
    """Atomically persist the head, audit record and optional pending mirror."""
    head = authority_flow_contracts.validate_scope_head(scope_head)
    record = persistence_contracts.validate_journal_record(journal_record)
    mirror = kwargs.get("mirror_record")
    if mirror is not None:
        mirror = persistence_contracts.make_mirror_record(
            mirror.get("command"),
            mirror.get("snapshot"),
            mirror_id=mirror.get("mirror_id"),
            state=mirror.get("state"),
            attempts=mirror.get("attempts"),
            created_at_s=mirror.get("created_at_s"),
            updated_at_s=mirror.get("updated_at_s"),
        )
    if record["scope"] != head["scope"]:
        raise ValueError("journal and scope head differ")
    now = time.time()
    with context["lock"]:
        try:
            _begin(context)
            current_row = context["connection"].execute(
                "SELECT * FROM scope_heads WHERE scope=?", (head["scope"],)
            ).fetchone()
            current = _scope_head_record(current_row)
            if current is not None:
                same_epoch = head["authority_epoch"] == current["authority_epoch"]
                next_revision = head["scope_revision"] == current["scope_revision"] + 1
                transfer_epoch = head["authority_epoch"] == current["authority_epoch"] + 1
                same_revision = head["scope_revision"] == current["scope_revision"]
                if not ((same_epoch and next_revision) or (transfer_epoch and same_revision)):
                    raise ValueError("scope head transition is not monotonic")
            context["connection"].execute(
                """
                INSERT INTO scope_heads(
                    scope, authority_mode, authority_epoch, scope_revision,
                    execution_epoch, fingerprint, locked,
                    node_authoritative_committed, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope) DO UPDATE SET
                    authority_mode=excluded.authority_mode,
                    authority_epoch=excluded.authority_epoch,
                    scope_revision=excluded.scope_revision,
                    execution_epoch=excluded.execution_epoch,
                    fingerprint=excluded.fingerprint,
                    locked=excluded.locked,
                    node_authoritative_committed=excluded.node_authoritative_committed,
                    updated_at=excluded.updated_at
                """,
                (
                    head["scope"], head["authority_mode"], head["authority_epoch"],
                    head["scope_revision"], head["execution_epoch"],
                    head.get("fingerprint"), int(head["locked"]),
                    int(head["node_authoritative_committed"]), now,
                ),
            )
            _insert_journal(context["connection"], record)
            if mirror is not None:
                _insert_mirror(context["connection"], mirror)
            _commit(context)
        except Exception:
            _rollback(context)
            raise
    return copy.deepcopy(head)


def list_authority_journal(context, **kwargs):
    """Return ordered durable evidence for one scope or the complete node."""
    scope = kwargs.get("scope")
    limit = max(1, min(10000, int(kwargs.get("limit", 1000))))
    with context["lock"]:
        if scope is None:
            rows = context["connection"].execute(
                "SELECT record_json FROM authority_journal "
                "ORDER BY created_at, record_id LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = context["connection"].execute(
                "SELECT record_json FROM authority_journal WHERE scope=? "
                "ORDER BY created_at, record_id LIMIT ?", (str(scope), limit)
            ).fetchall()
    return [_json_value(row["record_json"], {}) for row in rows]


def stage_mirror_record(context, mirror_record):
    """Persist one current-scope Mirror transfer without changing its authority head."""
    if not isinstance(mirror_record, dict):
        raise ValueError("mirror record must be an object")
    mirror = persistence_contracts.make_mirror_record(
        mirror_record.get("command"),
        mirror_record.get("snapshot"),
        mirror_id=mirror_record.get("mirror_id"),
        state=mirror_record.get("state"),
        attempts=mirror_record.get("attempts"),
        created_at_s=mirror_record.get("created_at_s"),
        updated_at_s=mirror_record.get("updated_at_s"),
    )
    with context["lock"]:
        try:
            _begin(context)
            _insert_mirror(context["connection"], mirror)
            _commit(context)
        except Exception:
            _rollback(context)
            raise
    return copy.deepcopy(mirror)


def get_mirror_record(context, mirror_id):
    """Return one detached pending or acknowledged mirror record."""
    with context["lock"]:
        row = context["connection"].execute(
            "SELECT * FROM mirror_outbox WHERE mirror_id=?",
            (str(mirror_id),),
        ).fetchone()
    if row is None:
        return None
    result = _json_value(row["record_json"], {})
    result["state"] = row["state"]
    result["attempts"] = row["attempts"]
    return result


def get_due_mirrors(context, **kwargs):
    """Return restart-safe unacknowledged mirrors due for delivery."""
    limit = max(1, min(512, int(kwargs.get("limit", 64))))
    now = float(kwargs.get("now_s", time.time()))
    with context["lock"]:
        rows = context["connection"].execute(
            """
            SELECT * FROM mirror_outbox
            WHERE state IN ('pending', 'announced', 'requested', 'sending')
              AND next_attempt_at <= ?
            ORDER BY authority_epoch, scope_revision, created_at
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()
    result = []
    for row in rows:
        item = _json_value(row["record_json"], {})
        item["state"] = row["state"]
        item["attempts"] = row["attempts"]
        result.append(item)
    return result


def mark_mirror_attempt(context, mirror_id, **kwargs):
    """Persist an attempted announcement or content transfer."""
    state = str(kwargs.get("state") or "announced")
    if state not in ("announced", "requested", "sending"):
        raise ValueError("mirror attempt state is invalid")
    now = time.time()
    retry_after = max(0.05, float(kwargs.get("retry_after_s", 2.0)))
    with context["lock"]:
        context["connection"].execute(
            """
            UPDATE mirror_outbox SET state=?, attempts=attempts+1,
                next_attempt_at=?, updated_at=? WHERE mirror_id=?
            """,
            (state, now + retry_after, now, str(mirror_id)),
        )
    return True


def acknowledge_mirror(context, mirror_id, ack):
    """Compact a mirror only after exact persisted epoch/revision/fingerprint ACK."""
    if not isinstance(ack, dict):
        raise ValueError("mirror ACK must be an object")
    with context["lock"]:
        row = context["connection"].execute(
            "SELECT * FROM mirror_outbox WHERE mirror_id=?", (str(mirror_id),)
        ).fetchone()
        if row is None:
            return False
        if (
            int(ack.get("authority_epoch", -1)) != row["authority_epoch"]
            or int(ack.get("scope_revision", -1)) != row["scope_revision"]
            or str(ack.get("fingerprint") or "") != row["fingerprint"]
        ):
            raise ValueError("mirror ACK differs from pending record")
        context["connection"].execute(
            "UPDATE mirror_outbox SET state='acked', updated_at=? "
            "WHERE mirror_id=?", (time.time(), str(mirror_id))
        )
    return True


def persist_authority_transfer(context, transfer):
    """Persist one controlled transfer state."""
    if not isinstance(transfer, dict):
        raise ValueError("transfer must be an object")
    now = time.time()
    identity = str(transfer.get("transfer_id") or "")
    with context["lock"]:
        context["connection"].execute(
            """
            INSERT INTO authority_transfers(
                transfer_id, scope, state, transfer_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(transfer_id) DO UPDATE SET
                state=excluded.state,
                transfer_json=excluded.transfer_json,
                updated_at=excluded.updated_at
            """,
            (
                identity, str(transfer.get("scope") or ""),
                str(transfer.get("state") or ""), _json_text(transfer), now, now,
            ),
        )
    return copy.deepcopy(transfer)


def persist_restore_intent(context, restore):
    """Persist one quarantined restore state."""
    if not isinstance(restore, dict):
        raise ValueError("restore must be an object")
    now = time.time()
    identity = str(restore.get("restore_id") or "")
    with context["lock"]:
        context["connection"].execute(
            """
            INSERT INTO restore_intents(
                restore_id, scope, state, restore_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(restore_id) DO UPDATE SET
                state=excluded.state,
                restore_json=excluded.restore_json,
                updated_at=excluded.updated_at
            """,
            (
                identity, str(restore.get("scope") or ""),
                str(restore.get("state") or ""), _json_text(restore), now, now,
            ),
        )
    return copy.deepcopy(restore)


def get_operation_store_snapshot(context):
    tables = (
        "inbox", "outbox", "actuator_operations", "nodes",
        "mirror_outbox", "authority_transfers", "restore_intents",
    )
    counts = {}
    with context["lock"]:
        for table in tables:
            rows = context["connection"].execute(
                "SELECT state, COUNT(*) AS count FROM {} GROUP BY state".format(table)
            ).fetchall()
            counts[table] = {row["state"]: row["count"] for row in rows}
        for table in ("scope_heads", "authority_journal", "registry_view"):
            row = context["connection"].execute(
                "SELECT COUNT(*) AS count FROM {}".format(table)
            ).fetchone()
            counts[table] = {"TOTAL": row["count"]}
    return {
        "module": context["module"],
        "version": context["version"],
        "path": context["path"],
        "closed": context["closed"],
        "counts": counts,
    }


def close_operation_store(context):
    with context["lock"]:
        if context["closed"]:
            return True
        context["connection"].close()
        context["closed"] = True
    return True




