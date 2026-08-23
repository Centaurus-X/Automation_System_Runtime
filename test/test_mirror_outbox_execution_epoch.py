# -*- coding: utf-8 -*-

"""Regression test for the durable Worker Mirror execution-epoch fence."""

import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library import _generic_type_library as type_authority
from src.library._distributed_operation_store import close_operation_store
from src.library._distributed_operation_store import create_operation_store
from src.library._distributed_operation_store import get_due_mirrors
from src.library._distributed_operation_store import stage_mirror_record
from src.library._generic_system_contract_library import persistence_contracts
from src.library._generic_system_contract_library import protocol_contracts


FIXTURE_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "device_state_data_v9_node_fn-01.json",
)


def _load_config():
    with open(FIXTURE_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def test_execution_epoch_survives_mirror_outbox_roundtrip():
    storage_config = _load_config()
    config = type_authority.normalize_config_for_runtime(
        storage_config, representation="storage")
    fingerprint = type_authority.config_fingerprint(config)
    snapshot = persistence_contracts.make_scope_snapshot(
        config,
        scope="node:fn-01",
        authority_epoch=0,
        scope_revision=0,
        execution_epoch=0,
        fingerprint=fingerprint,
    )
    assert snapshot["config"] == storage_config
    assert all("rows" not in row for row in snapshot["config"]["sensors"])
    assert all("rows" not in row for row in snapshot["config"]["actuators"])
    assert snapshot["fingerprint"] == type_authority.config_fingerprint(storage_config)
    mirror = persistence_contracts.make_mirror_record(
        protocol_contracts.MIRROR_SNAPSHOT,
        snapshot,
        state="pending",
    )
    assert mirror["execution_epoch"] == 0

    store = create_operation_store(":memory:")
    try:
        stage_mirror_record(store, mirror)
        due = get_due_mirrors(store)
        assert len(due) == 1
        assert due[0]["execution_epoch"] == 0
        assert due[0]["execution_epoch"] == due[0]["snapshot"]["execution_epoch"]
    finally:
        close_operation_store(store)


def main():
    test_execution_epoch_survives_mirror_outbox_roundtrip()
    print("PASS durable Worker Mirror execution-epoch roundtrip")


if __name__ == "__main__":
    main()
