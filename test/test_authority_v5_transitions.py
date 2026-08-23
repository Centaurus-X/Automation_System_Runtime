# -*- coding: utf-8 -*-

"""Authority V5 transfer, restore quarantine and stale-mirror regression tests."""

import copy
import json
import os
import sys


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.authority_transition_service import create_authority_transition_service
from src.library import _generic_type_library as type_authority
from src.library._distributed_operation_store import (
    create_operation_store,
    ensure_scope_head,
    get_operation_store_snapshot,
    get_scope_head,
)
from src.library._generic_system_contract_library import persistence_contracts


CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "_config",
    "config_data",
    "device_state_data_v9_node_fn-01.json",
)

MODE_ACTIVATIONS = []
SEM_WRITES = []
LIVE_CONFIG = {}


def _load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _safety_status():
    return {
        "enabled": True,
        "gates": {
            "actuator_tasks": True,
            "external_commits": True,
        },
    }


def _activate_mode(mode):
    MODE_ACTIVATIONS.append(str(mode))
    return True


def _revision_reader():
    return 5


def _sem_replace_writer(config, **kwargs):
    LIVE_CONFIG["config"] = copy.deepcopy(config)
    SEM_WRITES.append({"config": copy.deepcopy(config), "kwargs": dict(kwargs)})
    return {
        "ok": True,
        "operation_id": kwargs.get("operation_id"),
        "revision": 6,
        "fingerprint": type_authority.config_fingerprint(config),
    }


def _config_snapshot_reader():
    return copy.deepcopy(LIVE_CONFIG["config"])


def _service(store, scope, initial_config=None):
    LIVE_CONFIG["config"] = copy.deepcopy(initial_config or _load_config())
    return create_authority_transition_service(
        store=store,
        scope=scope,
        sem_replace_writer=_sem_replace_writer,
        config_snapshot_reader=_config_snapshot_reader,
        revision_reader=_revision_reader,
        safety_status_reader=_safety_status,
        authority_mode_activator=_activate_mode,
    )


def test_controlled_authority_transfer_requires_exact_ack():
    MODE_ACTIVATIONS.clear()
    config = _load_config()
    fingerprint = type_authority.config_fingerprint(config)
    store = create_operation_store(":memory:")
    ensure_scope_head(
        store,
        "node:fn-01",
        authority_mode="master_global",
        authority_epoch=3,
        scope_revision=9,
        execution_epoch=0,
        fingerprint=fingerprint,
    )
    service = _service(store, "node:fn-01")
    prepared = service["begin_transfer"](
        "node_local",
        operator_confirmed=True,
        source_truth="node_local",
        master_snapshot=config,
        node_snapshot=config,
    )
    assert prepared["ok"] is True, prepared
    transfer = prepared["transfer"]
    assert transfer["state"] == "awaiting_target_ack"
    locked = get_scope_head(store, "node:fn-01")
    assert locked["locked"] is True
    wrong = service["ack_transfer"](
        transfer["transfer_id"],
        {
            "accepted": True,
            "authority_epoch": transfer["target_authority_epoch"],
            "fingerprint": "0" * 64,
        },
    )
    assert wrong["ok"] is False and wrong["error"]["code"] == "invalid_ack"
    accepted = service["ack_transfer"](
        transfer["transfer_id"],
        {
            "accepted": True,
            "authority_epoch": transfer["target_authority_epoch"],
            "fingerprint": transfer["selected_fingerprint"],
        },
    )
    assert accepted["ok"] is True, accepted
    head = get_scope_head(store, "node:fn-01")
    assert head["authority_mode"] == "node_local"
    assert head["authority_epoch"] == 4
    assert head["locked"] is False
    assert MODE_ACTIVATIONS == ["node_local"]
    counts = get_operation_store_snapshot(store)["counts"]
    assert counts["authority_journal"]["TOTAL"] == 2


def test_authority_transfer_applies_selected_truth_through_sem():
    MODE_ACTIVATIONS.clear()
    SEM_WRITES.clear()
    master_config = _load_config()
    node_config = copy.deepcopy(master_config)
    node_config["meta"][0]["description"] = "Node-selected authority truth"
    master_fingerprint = type_authority.config_fingerprint(master_config)
    node_fingerprint = type_authority.config_fingerprint(node_config)
    store = create_operation_store(":memory:")
    ensure_scope_head(
        store,
        "node:fn-01",
        authority_mode="master_global",
        authority_epoch=4,
        scope_revision=10,
        execution_epoch=0,
        fingerprint=master_fingerprint,
    )
    service = _service(store, "node:fn-01", initial_config=master_config)
    prepared = service["begin_transfer"](
        "node_local",
        operator_confirmed=True,
        source_truth="node_local",
        master_snapshot=master_config,
        node_snapshot=node_config,
    )
    assert prepared["ok"] is True, prepared
    transfer = prepared["transfer"]
    accepted = service["ack_transfer"](
        transfer["transfer_id"],
        {
            "accepted": True,
            "authority_epoch": transfer["target_authority_epoch"],
            "fingerprint": node_fingerprint,
        },
    )
    assert accepted["ok"] is True, accepted
    assert len(SEM_WRITES) == 1
    assert SEM_WRITES[0]["kwargs"]["source"] == "authority_transfer"
    assert SEM_WRITES[0]["kwargs"]["transfer_id"] == transfer["transfer_id"]
    assert type_authority.config_fingerprint(
        _config_snapshot_reader()) == node_fingerprint
    assert accepted["transfer"].get("sem_commit_id")
    head = get_scope_head(store, "node:fn-01")
    assert head["authority_mode"] == "node_local"
    assert head["authority_epoch"] == 5
    assert head["fingerprint"] == node_fingerprint

def test_disaster_restore_stays_quarantined_until_preflight():
    SEM_WRITES.clear()
    config = _load_config()
    fingerprint = type_authority.config_fingerprint(config)
    store = create_operation_store(":memory:")
    ensure_scope_head(
        store,
        "node:fn-01",
        authority_mode="node_local",
        authority_epoch=7,
        scope_revision=5,
        execution_epoch=0,
        fingerprint=fingerprint,
        node_authoritative_committed=True,
    )
    service = _service(store, "node:fn-01")
    started = service["begin_restore"](identity="fn-01")
    assert started["ok"] is True and started["code"] == "quarantined"
    assert service["hardware_writes_allowed"]() is False
    restore_id = started["restore"]["restore_id"]
    selected = service["select_restore"](
        restore_id,
        "local_backup",
        config,
        operator_confirmed=True,
        identity_confirmed=True,
    )
    assert selected["ok"] is True, selected
    validated = service["validate_restore"](restore_id)
    assert validated["ok"] is True, validated
    activated = service["activate_restore"](restore_id)
    assert activated["ok"] is True, activated
    assert len(SEM_WRITES) == 1
    assert SEM_WRITES[0]["kwargs"]["source"] == "disaster_restore"
    assert service["hardware_writes_allowed"]() is False
    completed = service["pass_restore_preflight"](
        restore_id, preflight_ok=True)
    assert completed["ok"] is True, completed
    assert completed["restore"]["state"] == "hardware_enabled"
    assert service["hardware_writes_allowed"]() is True
    head = get_scope_head(store, "node:fn-01")
    assert head["authority_epoch"] == 8
    counts = get_operation_store_snapshot(store)["counts"]
    assert counts["restore_intents"]["hardware_enabled"] == 1
    assert counts["authority_journal"]["TOTAL"] == 2


def test_older_master_mirror_needs_explicit_override():
    config = _load_config()
    fingerprint = type_authority.config_fingerprint(config)
    store = create_operation_store(":memory:")
    ensure_scope_head(
        store,
        "node:fn-01",
        authority_mode="node_local",
        authority_epoch=2,
        scope_revision=12,
        execution_epoch=0,
        fingerprint=fingerprint,
        node_authoritative_committed=True,
    )
    service = _service(store, "node:fn-01")
    started = service["begin_restore"](identity="fn-01")
    restore_id = started["restore"]["restore_id"]
    old_config = copy.deepcopy(config)
    old_config["meta"][0]["description"] = "Older mirror snapshot"
    old_snapshot = persistence_contracts.make_scope_snapshot(
        old_config,
        scope="node:fn-01",
        authority_epoch=2,
        scope_revision=11,
        execution_epoch=0,
    )
    rejected = service["select_restore"](
        restore_id,
        "master_mirror",
        old_snapshot,
        operator_confirmed=True,
        identity_confirmed=True,
    )
    assert rejected["ok"] is False
    assert rejected["error"]["code"] == "stale_revision"


def main():
    tests = (
        test_controlled_authority_transfer_requires_exact_ack,
        test_authority_transfer_applies_selected_truth_through_sem,
        test_disaster_restore_stays_quarantined_until_preflight,
        test_older_master_mirror_needs_explicit_override,
    )
    for test_function in tests:
        test_function()
        print("PASS " + test_function.__name__)
    print("PASS Authority V5 transition suite ({} checks)".format(len(tests)))


if __name__ == "__main__":
    main()
