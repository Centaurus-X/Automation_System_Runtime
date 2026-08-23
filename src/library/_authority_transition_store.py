# -*- coding: utf-8 -*-

# src/library/_authority_transition_store.py

"""Atomic persistence adapter for Authority V5 transfer and restore flows."""

import copy
import time

from src.library import _generic_type_library as type_authority
from src.library._generic_system_contract_library import authority_flow_contracts
from src.library._generic_system_contract_library import persistence_contracts


CONTEXT = {
    "library_name": "authority_transition_store",
    "library_version": "1.0.0",
}


def _json_text(value):
    return type_authority.canonical_text(type_authority.json_ready(value))


def _json_value(value, default=None):
    if value in (None, ""):
        return copy.deepcopy(default)
    try:
        return type_authority.decode_json_bytes(str(value).encode("utf-8"))
    except Exception:
        return copy.deepcopy(default)


def _required_text(value, label):
    result = str(value or "").strip()
    if not result:
        raise ValueError(str(label) + " must not be empty")
    return result


def _begin(store):
    store["connection"].execute("BEGIN IMMEDIATE")


def _commit(store):
    store["connection"].execute("COMMIT")


def _rollback(store):
    try:
        store["connection"].execute("ROLLBACK")
    except Exception:
        pass


def _upsert_scope_head(connection, scope_head):
    head = authority_flow_contracts.validate_scope_head(scope_head)
    connection.execute(
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
            int(head["node_authoritative_committed"]), time.time(),
        ),
    )
    return head


def _insert_journal(connection, journal_record):
    record = persistence_contracts.validate_journal_record(journal_record)
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
    return record


def _upsert_transfer(connection, transfer):
    identity = _required_text(transfer.get("transfer_id"), "transfer_id")
    scope = _required_text(transfer.get("scope"), "scope")
    state = _required_text(transfer.get("state"), "state")
    now = time.time()
    connection.execute(
        """
        INSERT INTO authority_transfers(
            transfer_id, scope, state, transfer_json, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(transfer_id) DO UPDATE SET
            state=excluded.state,
            transfer_json=excluded.transfer_json,
            updated_at=excluded.updated_at
        """,
        (identity, scope, state, _json_text(transfer), now, now),
    )


def _upsert_restore(connection, restore):
    identity = _required_text(restore.get("restore_id"), "restore_id")
    scope = _required_text(restore.get("scope"), "scope")
    state = _required_text(restore.get("state"), "state")
    now = time.time()
    connection.execute(
        """
        INSERT INTO restore_intents(
            restore_id, scope, state, restore_json, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(restore_id) DO UPDATE SET
            state=excluded.state,
            restore_json=excluded.restore_json,
            updated_at=excluded.updated_at
        """,
        (identity, scope, state, _json_text(restore), now, now),
    )


def persist_transfer(store, transfer, **kwargs):
    """Atomically persist transfer state, the write lock/head and optional audit."""
    if not isinstance(transfer, dict):
        raise ValueError("transfer must be an object")
    head = authority_flow_contracts.validate_scope_head(transfer.get("scope_head"))
    if transfer.get("scope") != head["scope"]:
        raise ValueError("transfer and scope head differ")
    journal = kwargs.get("journal_record")
    with store["lock"]:
        _begin(store)
        try:
            _upsert_scope_head(store["connection"], head)
            _upsert_transfer(store["connection"], transfer)
            if journal is not None:
                record = _insert_journal(store["connection"], journal)
                if record["scope"] != head["scope"]:
                    raise ValueError("transfer journal and scope head differ")
            _commit(store)
        except Exception:
            _rollback(store)
            raise
    return copy.deepcopy(transfer)


def get_transfer(store, transfer_id):
    with store["lock"]:
        row = store["connection"].execute(
            "SELECT transfer_json FROM authority_transfers WHERE transfer_id=?",
            (str(transfer_id or ""),),
        ).fetchone()
    return _json_value(row["transfer_json"], None) if row else None


def get_active_transfer(store, scope):
    with store["lock"]:
        row = store["connection"].execute(
            """
            SELECT transfer_json FROM authority_transfers
            WHERE scope=? AND state NOT IN ('completed', 'failed')
            ORDER BY updated_at DESC LIMIT 1
            """,
            (str(scope or ""),),
        ).fetchone()
    return _json_value(row["transfer_json"], None) if row else None


def persist_restore(store, restore, **kwargs):
    """Atomically persist restore state, optional scope head and audit record."""
    if not isinstance(restore, dict):
        raise ValueError("restore must be an object")
    head = kwargs.get("scope_head")
    journal = kwargs.get("journal_record")
    with store["lock"]:
        _begin(store)
        try:
            validated_head = None
            if head is not None:
                validated_head = _upsert_scope_head(store["connection"], head)
                if restore.get("scope") != validated_head["scope"]:
                    raise ValueError("restore and scope head differ")
            _upsert_restore(store["connection"], restore)
            if journal is not None:
                record = _insert_journal(store["connection"], journal)
                if record["scope"] != restore.get("scope"):
                    raise ValueError("restore journal and intent differ")
            _commit(store)
        except Exception:
            _rollback(store)
            raise
    return copy.deepcopy(restore)


def get_restore(store, restore_id):
    with store["lock"]:
        row = store["connection"].execute(
            "SELECT restore_json FROM restore_intents WHERE restore_id=?",
            (str(restore_id or ""),),
        ).fetchone()
    return _json_value(row["restore_json"], None) if row else None


def get_latest_restore(store, scope):
    with store["lock"]:
        row = store["connection"].execute(
            """
            SELECT restore_json FROM restore_intents
            WHERE scope=? ORDER BY updated_at DESC LIMIT 1
            """,
            (str(scope or ""),),
        ).fetchone()
    return _json_value(row["restore_json"], None) if row else None


def persist_registry_view(store, registry_id, registry, **kwargs):
    """Persist the last accepted ownership view for masterless continuity."""
    identity = _required_text(registry_id, "registry_id")
    fingerprint = type_authority.fingerprint(registry)
    supplied = kwargs.get("fingerprint")
    if supplied is not None and supplied != fingerprint:
        raise ValueError("registry fingerprint differs")
    authority_epoch = int(kwargs.get("authority_epoch", 0))
    execution_epoch = int(kwargs.get("execution_epoch", 0))
    if authority_epoch < 0 or execution_epoch < 0:
        raise ValueError("registry epochs must be non-negative")
    with store["lock"]:
        store["connection"].execute(
            """
            INSERT INTO registry_view(
                registry_id, authority_epoch, execution_epoch, fingerprint,
                registry_json, accepted_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(registry_id) DO UPDATE SET
                authority_epoch=excluded.authority_epoch,
                execution_epoch=excluded.execution_epoch,
                fingerprint=excluded.fingerprint,
                registry_json=excluded.registry_json,
                accepted_at=excluded.accepted_at
            """,
            (
                identity, authority_epoch, execution_epoch, fingerprint,
                _json_text(registry), time.time(),
            ),
        )
    return {
        "registry_id": identity,
        "authority_epoch": authority_epoch,
        "execution_epoch": execution_epoch,
        "fingerprint": fingerprint,
        "registry": copy.deepcopy(registry),
    }


def get_registry_view(store, registry_id):
    with store["lock"]:
        row = store["connection"].execute(
            "SELECT * FROM registry_view WHERE registry_id=?",
            (str(registry_id or ""),),
        ).fetchone()
    if row is None:
        return None
    return {
        "registry_id": row["registry_id"],
        "authority_epoch": row["authority_epoch"],
        "execution_epoch": row["execution_epoch"],
        "fingerprint": row["fingerprint"],
        "registry": _json_value(row["registry_json"], {}),
        "accepted_at": row["accepted_at"],
    }


__all__ = (
    "CONTEXT",
    "get_active_transfer",
    "get_latest_restore",
    "get_registry_view",
    "get_restore",
    "get_transfer",
    "persist_registry_view",
    "persist_restore",
    "persist_transfer",
)
