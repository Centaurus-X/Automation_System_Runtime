# -*- coding: utf-8 -*-

"""Durable inbox/outbox identity and crash-state recovery test."""

import os
import sys
import tempfile


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core import distributed_coordinator
from src.library._distributed_operation_store import (
    close_operation_store,
    complete_inbox,
    complete_outbox,
    converge_config_proposals,
    create_operation_store,
    enqueue_outbox,
    get_actuator_operation,
    get_due_outbox,
    get_outbox,
    increment_metadata,
    rebase_config_proposals,
    recover_actuator_operations,
    register_actuator_operation,
    register_inbox,
    transition_actuator_operation,
    update_unattempted_outbox_payload,
)


def _run_test():
    with tempfile.TemporaryDirectory(prefix="distributed_store_") as directory:
        path = os.path.join(directory, "operations.sqlite3")
        store = create_operation_store(path)
        assert increment_metadata(store, "coordinator_term", 0) == 1
        first = register_inbox(
            store, "message-1", "test.command", payload={"value": 1})
        assert first["status"] == "new"
        complete_inbox(store, "message-1", {"ok": True, "value": 1})
        duplicate = register_inbox(
            store, "message-1", "test.command", payload={"value": 1})
        conflict = register_inbox(
            store, "message-1", "test.command", payload={"value": 2})
        assert duplicate["status"] == "duplicate"
        assert duplicate["result"]["ok"] is True
        assert conflict["status"] == "conflict"
        assert enqueue_outbox(
            store, "outbox-1", target_node="node-2",
            command="distributed.config.propose", payload={"value": 1}
        )["status"] == "new"
        assert enqueue_outbox(
            store, "outbox-2", target_node="node-2",
            command="distributed.config.propose", payload={"value": 2}
        )["status"] == "new"
        assert len(get_due_outbox(store)) == 1
        assert get_due_outbox(store)[0]["message_id"] == "outbox-1"
        proposal_result = {
            "ok": True,
            "post_commit": {
                "authority_revision": 7,
                "fingerprint": "authority-fingerprint-7",
            },
        }
        assert complete_outbox(
            store, "outbox-1", proposal_result, state="MASTER_ACKED")
        assert get_outbox(store, "outbox-1")["state"] == "MASTER_ACKED"
        assert converge_config_proposals(
            store, 7, "authority-fingerprint-7") == 1
        assert get_outbox(store, "outbox-1")["state"] == "CONVERGED"
        assert get_due_outbox(store)[0]["message_id"] == "outbox-2"
        rebase_payload = {
            "authority_scope": "worker_local_runtime",
            "authority_revision": 7,
            "proposal_id": "outbox-2",
            "operation_id": "operation-2",
            "actions": [],
        }
        assert update_unattempted_outbox_payload(
            store, "outbox-2", rebase_payload)
        assert complete_outbox(
            store, "outbox-2", {"ok": False, "code": "stale_revision"},
            state="REBASE_REQUIRED")
        assert rebase_config_proposals(store, 8) == 1
        rebased = get_due_outbox(store)[0]
        assert rebased["message_id"] == "outbox-2:rebase:1"
        assert int(rebased["payload"]["authority_revision"]) == 8
        assert complete_outbox(
            store, rebased["message_id"], {"ok": True},
            state="CONVERGED")
        assert enqueue_outbox(
            store, "action-plan-stale", target_node="node-2",
            command="distributed.config.propose",
            payload={
                "authority_scope": "worker_local_runtime",
                "operation": "action_plan",
                "operation_id": "rule:old",
            },
        )["status"] == "new"
        assert enqueue_outbox(
            store, "file-proposal-next", target_node="node-2",
            command="distributed.config.propose",
            payload={
                "authority_scope": "worker_file_master_first",
                "operation": "scope_operations",
                "operation_id": "file:new",
            },
        )["status"] == "new"
        stale_record = get_outbox(store, "action-plan-stale")
        stale_result = {"ok": False, "code": "stale_revision"}
        state = distributed_coordinator._config_proposal_completion_state(
            stale_record, stale_result)
        assert state == "REJECTED"
        assert complete_outbox(
            store, "action-plan-stale", stale_result, state=state)
        assert get_due_outbox(store)[0]["message_id"] == "file-proposal-next"
        task = {"controller_uuid": "controller-1", "control_value": 7}
        assert register_actuator_operation(
            store, "operation-1", task, direction="inbound"
        )["status"] == "new"
        assert transition_actuator_operation(
            store, "operation-1", "DISPATCHED",
            allowed_states=("RECEIVED",))
        close_operation_store(store)
        reopened = create_operation_store(path)
        assert increment_metadata(reopened, "coordinator_term", 0) == 2
        assert register_inbox(
            reopened, "message-1", "test.command", payload={"value": 1}
        )["status"] == "duplicate"
        assert len(get_due_outbox(reopened)) == 1
        recovered = recover_actuator_operations(reopened, direction="inbound")
        assert recovered[0]["state"] == "UNCERTAIN"
        operation = get_actuator_operation(reopened, "operation-1")
        assert operation["state"] == "UNCERTAIN"
        assert not transition_actuator_operation(
            reopened, "operation-1", "DISPATCHED",
            allowed_states=("RECEIVED",))
        close_operation_store(reopened)


if __name__ == "__main__":
    _run_test()
    print("PASS durable distributed journal and UNCERTAIN crash recovery")
