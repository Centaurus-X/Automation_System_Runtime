# -*- coding: utf-8 -*-

"""Legacy Master-authority JSON file-sync regression suite."""

import copy
import json
import os
import sys
import tempfile
import threading


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core import config_file_sync as config_file_sync_module
from src.library._generic_system_contract_library.config_sharding_contracts import apply_operations, diff_configs
from src.core.config_file_sync import (
    _idle_wal,
    _internal_persistence_receipt,
    _local_runtime_actions,
    _observe_master_baseline,
    _poll_pending,
    _process_candidate,
    _record_processing_error,
    _stage_prepared,
    create_config_file_sync,
    prepare_startup_config_file_sync,
)
from src.library import _generic_type_library as type_library


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data",
    "device_state_data_v9_node_fn-01.json")


def _state(config, state_id):
    return next(
        row for row in config["process_states"]
        if str(row.get("state_id")) == str(state_id))


def _write(path, config):
    storage = type_library.project_config_for_storage(config) \
        if isinstance(config, dict) and all(
            section in config for section in ("meta", "sensors", "actuators")) \
        else config
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(storage, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _fake_runtime(config_path, initial):
    state = {
        "config": copy.deepcopy(initial), "revision": 1,
        "scope_revision": 1,
        "accepted_fingerprint": type_library.config_fingerprint(initial),
        "proposals": {},
        "persistence_expectations": [],
        "preapplied_calls": 0,
        "replace_calls": 0,
        "request_actions_no_change_once": False,
    }

    def snapshot():
        return copy.deepcopy(state["config"])

    def revision():
        return int(state["revision"])

    def replace(config, **_kwargs):
        state["replace_calls"] += 1
        state["config"] = copy.deepcopy(config)
        state["revision"] += 1
        _write(config_path, state["config"])
        return {
            "ok": True, "changed": True, "revision": state["revision"],
            "fingerprint": type_library.config_fingerprint(state["config"]),
        }

    def persist_json(**kwargs):
        state["persistence_expectations"].append(
            str(kwargs.get("expected_external_fingerprint") or ""))
        snapshot = copy.deepcopy(state["config"])
        storage = type_library.project_config_for_storage(snapshot)
        fingerprint = type_library.config_fingerprint(storage)
        _write(config_path, snapshot)
        return {
            "status": "OK",
            "details": {"fingerprint": fingerprint},
        }

    def request_actions(actions, **kwargs):
        for action in actions:
            for state_id in action.get("state_ids") or []:
                row = _state(state["config"], state_id)
                if "value_id" in action:
                    row["value_id"] = copy.deepcopy(action["value_id"])
                if isinstance(action.get("parameters"), dict):
                    row.setdefault("parameters", {}).update(copy.deepcopy(action["parameters"]))
        state["revision"] += 1
        state["scope_revision"] += 1
        state["accepted_fingerprint"] = type_library.config_fingerprint(state["config"])
        _write(config_path, state["config"])
        if state["request_actions_no_change_once"]:
            state["request_actions_no_change_once"] = False
            return {
                "ok": True, "changed": False,
                "sync": {"state": "NO_CHANGE", "durable": True},
            }
        message_id = "local:" + str(kwargs.get("operation_id"))
        state["proposals"][message_id] = {"state": "CONVERGED", "result": {"ok": True}}
        return {
            "ok": True, "changed": True,
            "sync": {"message_id": message_id, "durable": True},
        }

    def stage_applied(actions, **kwargs):
        state["preapplied_calls"] += 1
        message_id = "preapplied:" + str(kwargs.get("operation_id"))
        state["proposals"][message_id] = {
            "state": "CONVERGED", "result": {"ok": True}}
        return {
            "ok": True, "changed": True, "startup_preapplied": True,
            "sync": {"message_id": message_id, "durable": True},
        }

    def request_scope(operations, **kwargs):
        state["config"] = apply_operations(
            state["config"], operations, representation="runtime")
        state["revision"] += 1
        state["scope_revision"] += 1
        state["accepted_fingerprint"] = type_library.config_fingerprint(state["config"])
        _write(config_path, state["config"])
        message_id = "scope:" + str(kwargs.get("operation_id"))
        state["proposals"][message_id] = {"state": "CONVERGED", "result": {"ok": True}}
        return {
            "ok": True, "changed": True,
            "sync": {"message_id": message_id, "durable": True},
        }

    def distributed_snapshot():
        return {
            "node_scope_revision": state["scope_revision"],
            "node_shard_fingerprint": state["accepted_fingerprint"],
        }

    def proposal(message_id):
        return copy.deepcopy(state["proposals"].get(message_id))

    return state, {
        "get_snapshot": snapshot, "get_revision": revision,
        "request_replace": replace, "persist_json": persist_json,
    }, {
        "request_action_plan": request_actions,
        "stage_applied_action_plan": stage_applied,
        "request_scope_operations": request_scope,
        "snapshot": distributed_snapshot,
        "get_proposal": proposal,
    }


def main():
    with open(SOURCE_CONFIG, "r", encoding="utf-8") as handle:
        storage = json.load(handle)
    baseline = type_library.normalize_config_for_runtime(
        storage, representation="storage")
    expected_target = _state(baseline, 1)["parameters"]["target_temp_c"] + 1.0
    runtime_candidate = copy.deepcopy(baseline)
    _state(runtime_candidate, 1)["parameters"]["target_temp_c"] += 1.0
    runtime_operations = diff_configs(
        baseline, runtime_candidate, representation="runtime")
    actions = _local_runtime_actions(baseline, runtime_candidate, runtime_operations)
    assert actions and actions[0]["parameters"]["target_temp_c"] == expected_target
    mixed_candidate = copy.deepcopy(runtime_candidate)
    _state(mixed_candidate, 1)["mode_id"] = 1
    assert _local_runtime_actions(
        baseline, mixed_candidate, diff_configs(
            baseline, mixed_candidate, representation="runtime")) is None

    with tempfile.TemporaryDirectory(prefix="config-file-sync-") as directory:
        config_path = os.path.join(directory, "config.json")
        wal_path = os.path.join(directory, "proposal-wal.json")
        _write(config_path, baseline)
        state, config_api, distributed_api = _fake_runtime(config_path, baseline)
        api = create_config_file_sync(
            options={
                "enabled": True, "node_id": "fn-01",
                "config_path": config_path, "wal_path": wal_path,
                "config_authority": "master_global",
            },
            config_capabilities=config_api, distributed_api=distributed_api)
        context = api["context"]

        _write(config_path, runtime_candidate)
        assert _process_candidate(
            context, runtime_candidate,
            type_library.config_fingerprint(runtime_candidate))
        assert context["wal"]["mode"] == "local_first"
        first_runtime_operation_id = context["wal"].get("operation_id")
        assert first_runtime_operation_id
        assert _poll_pending(context)
        assert context["wal"]["state"] == "IDLE"
        assert _state(state["config"], 1)["parameters"]["target_temp_c"] \
            == expected_target

        # Repeated local runtime edits must converge exactly and remain durable.
        # This reproduces the reported state-21 threshold transition.
        for target_temp_c in (800.0, 1800.0):
            target_candidate = copy.deepcopy(state["config"])
            _state(target_candidate, 21).setdefault("parameters", {})[
                "target_temp_c"] = target_temp_c
            _write(config_path, target_candidate)
            assert _process_candidate(
                context, target_candidate,
                type_library.config_fingerprint(target_candidate))
            assert context["wal"]["mode"] == "local_first"
            assert _poll_pending(context)
            assert context["wal"]["state"] == "IDLE"
            assert _state(state["config"], 21)["parameters"][
                "target_temp_c"] == target_temp_c
            with open(config_path, "r", encoding="utf-8") as handle:
                persisted_target = json.load(handle)
            assert _state(persisted_target, 21)["parameters"][
                "target_temp_c"] == target_temp_c

        structural_candidate = copy.deepcopy(state["config"])
        structural_candidate["controllers"][0]["tags"] = ["file-sync", "structural"]
        _write(config_path, structural_candidate)
        assert _process_candidate(
            context, structural_candidate,
            type_library.config_fingerprint(structural_candidate))
        assert context["wal"]["mode"] == "master_first"
        assert _poll_pending(context)
        assert context["wal"]["state"] == "IDLE"
        assert state["config"]["controllers"][0]["tags"] == [
            "file-sync", "structural"]
        assert context["statistics"]["local_first"] == 3
        assert context["statistics"]["master_first"] == 1
        assert context["statistics"]["converged"] == 4

        # Fresh may preserve the proposal WAL while deliberately resetting the
        # distributed SQLite replica journal.  The durable operation must be
        # restaged with the same identity instead of remaining PENDING forever.
        orphan_baseline = copy.deepcopy(state["config"])
        orphan_candidate = copy.deepcopy(orphan_baseline)
        orphan_candidate["controllers"][0]["tags"] = ["orphan-recovery"]
        _write(config_path, orphan_candidate)
        assert _process_candidate(
            context, orphan_candidate,
            type_library.config_fingerprint(orphan_candidate))
        orphan_operation_id = context["wal"]["operation_id"]
        orphan_message_id = context["wal"]["message_id"]
        state["proposals"].pop(orphan_message_id)
        state["config"] = copy.deepcopy(orphan_baseline)
        state["accepted_fingerprint"] = type_library.config_fingerprint(
            orphan_baseline)
        _write(config_path, orphan_baseline)
        prepare_startup_config_file_sync(context)
        assert context["wal"]["state"] == "PREPARED"
        assert context["wal"]["operation_id"] == orphan_operation_id
        assert "message_id" not in context["wal"]
        assert context["statistics"]["pending_recovered"] == 1
        assert _stage_prepared(context)
        assert context["wal"]["operation_id"] == orphan_operation_id
        assert _poll_pending(context)
        assert context["wal"]["state"] == "IDLE"
        assert state["config"]["controllers"][0]["tags"] == [
            "orphan-recovery"]

        # A proposal is still converged when an unrelated runtime value moves
        # after the authoritative live ACK but before the WAL polling cycle.
        moving_candidate = copy.deepcopy(state["config"])
        _state(moving_candidate, 1)["parameters"]["target_temp_c"] += 1.0
        _write(config_path, moving_candidate)
        assert _process_candidate(
            context, moving_candidate,
            type_library.config_fingerprint(moving_candidate))
        moving_candidate_fingerprint = type_library.config_fingerprint(
            moving_candidate)
        _state(state["config"], 10)["parameters"]["flow_rate"] += 1.0
        assert _poll_pending(context)
        assert context["wal"]["state"] == "IDLE"
        assert state["persistence_expectations"][-1] == (
            moving_candidate_fingerprint)
        with open(config_path, "r", encoding="utf-8") as handle:
            materialized = json.load(handle)
        assert _state(materialized, 10)["parameters"]["flow_rate"] == \
            _state(state["config"], 10)["parameters"]["flow_rate"]

        # Another writer may apply the same action between the live overlay
        # check and request_action_plan. A durable NO_CHANGE must be converted
        # into one pre-applied journal entry instead of rolling back the config.
        race_baseline = copy.deepcopy(state["config"])
        race_candidate = copy.deepcopy(race_baseline)
        _state(race_candidate, 1)["parameters"]["target_temp_c"] += 1.0
        _write(config_path, race_candidate)
        state["request_actions_no_change_once"] = True
        replace_calls = state["replace_calls"]
        preapplied_calls = state["preapplied_calls"]
        assert _process_candidate(
            context, race_candidate,
            type_library.config_fingerprint(race_candidate))
        assert context["wal"]["state"] == "PENDING"
        assert state["replace_calls"] == replace_calls
        assert state["preapplied_calls"] == preapplied_calls + 1
        assert _poll_pending(context)
        assert context["wal"]["state"] == "IDLE"
        assert _state(state["config"], 1)["parameters"]["target_temp_c"] == \
            _state(race_candidate, 1)["parameters"]["target_temp_c"]

        # A concurrent Master patch has strict precedence over a local file
        # proposal. The worker waits for the authoritative shard, discards the
        # local candidate once, materializes Master, and never rolls back.
        precedence_baseline = copy.deepcopy(state["config"])
        local_candidate = copy.deepcopy(precedence_baseline)
        local_value = _state(local_candidate, 21)["parameters"][
            "target_temp_c"] + 100.0
        _state(local_candidate, 21)["parameters"][
            "target_temp_c"] = local_value
        _write(config_path, local_candidate)
        assert _process_candidate(
            context, local_candidate,
            type_library.config_fingerprint(local_candidate))
        precedence_message_id = context["wal"]["message_id"]
        master_authority = copy.deepcopy(precedence_baseline)
        master_value = local_value + 100.0
        _state(master_authority, 21)["parameters"][
            "target_temp_c"] = master_value
        master_fingerprint = type_library.config_fingerprint(
            master_authority)
        state["proposals"][precedence_message_id] = {
            "state": "REBASE_REQUIRED",
            "result": {
                "ok": False, "code": "rebase_required",
                "current_scope_fingerprint": master_fingerprint,
            },
        }
        state["accepted_fingerprint"] = master_fingerprint
        warning_messages = []

        def capture_warning(message, *args):
            warning_messages.append(message % args if args else message)

        original_warning = config_file_sync_module.LOGGER.warning
        config_file_sync_module.LOGGER.warning = capture_warning
        rollback_count = context["statistics"]["rollbacks"]
        try:
            assert _poll_pending(context) is False
            assert context["wal"]["state"] == "PENDING"
            assert context["statistics"]["authority_waits"] == 1
            assert warning_messages == []
            state["config"] = copy.deepcopy(master_authority)
            assert _poll_pending(context)
            assert _poll_pending(context) is False
        finally:
            config_file_sync_module.LOGGER.warning = original_warning
        assert context["wal"]["state"] == "IDLE"
        assert context["statistics"]["rollbacks"] == rollback_count
        assert context["statistics"]["master_precedence_discards"] == 1
        assert len(warning_messages) == 1
        assert "Master patch has priority" in warning_messages[0]
        assert _state(state["config"], 21)["parameters"][
            "target_temp_c"] == master_value
        with open(config_path, "r", encoding="utf-8") as handle:
            persisted_master = json.load(handle)
        assert _state(persisted_master, 21)["parameters"][
            "target_temp_c"] == master_value

        # Repeating an already used A -> B transition at a later Master
        # revision must receive a fresh durable identity, not conflict with
        # the completed outbox entry from the first cycle.
        state["config"] = copy.deepcopy(baseline)
        _write(config_path, baseline)
        _idle_wal(
            context, baseline, type_library.config_fingerprint(baseline))
        assert _process_candidate(
            context, runtime_candidate,
            type_library.config_fingerprint(runtime_candidate))
        repeated_operation_id = context["wal"].get("operation_id")
        assert repeated_operation_id != first_runtime_operation_id
        assert _poll_pending(context)

        # A runtime-only file edit made while stopped is already loaded by the
        # DataCenter. Startup must journal it without applying it twice.
        stopped_runtime = copy.deepcopy(state["config"])
        stopped_runtime_value = (
            2 if _state(stopped_runtime, 1).get("value_id") != 2 else 1)
        _state(stopped_runtime, 1)["value_id"] = stopped_runtime_value
        state["config"] = copy.deepcopy(stopped_runtime)
        _write(config_path, stopped_runtime)
        prepare_startup_config_file_sync(context)
        assert context["wal"]["state"] == "STARTUP_CANDIDATE"
        assert context["wal"]["mode"] == "local_first"
        # Controller activity can advance unrelated runtime parameters before
        # the prepared file proposal is staged. Only action-owned fields define
        # whether the candidate or baseline overlay is active.
        _state(state["config"], 10)["parameters"]["flow_rate"] += 0.5
        assert _stage_prepared(context)
        assert context["wal"]["stage_result"]["startup_preapplied"] is True
        pending_baseline = copy.deepcopy(context["wal"]["baseline_config"])
        state["config"] = pending_baseline
        _write(config_path, pending_baseline)
        prepare_startup_config_file_sync(context)
        assert _state(state["config"], 1)["value_id"] == stopped_runtime_value
        if context["wal"]["state"] == "PENDING":
            assert _poll_pending(context)
        assert context["wal"]["state"] == "IDLE"
        assert _state(state["config"], 1)["value_id"] == stopped_runtime_value

        # A structural edit made while stopped must not reach controller
        # startup. It is preserved in the WAL, the confirmed baseline is
        # restored through SEM, and only the Master-first round-trip activates
        # the candidate.
        stopped_structural = copy.deepcopy(state["config"])
        stopped_structural["controllers"][0]["tags"] = ["startup-structural"]
        state["config"] = copy.deepcopy(stopped_structural)
        _write(config_path, stopped_structural)
        prepare_startup_config_file_sync(context)
        assert context["wal"]["state"] == "STARTUP_CANDIDATE"
        assert context["wal"]["mode"] == "master_first"
        assert state["config"]["controllers"][0]["tags"] != [
            "startup-structural"]
        assert _stage_prepared(context)
        assert _poll_pending(context)
        assert state["config"]["controllers"][0]["tags"] == [
            "startup-structural"]


    # A stable retry cause is counted every time but logged only once per
    # bounded interval. The next emitted record summarizes suppressed retries.
    error_context = {
        "lock": threading.RLock(),
        "options": {"error_log_interval_s": 30.0},
        "error_log_state": {},
        "statistics": {
            "errors": 0, "error_logs": 0,
            "error_logs_suppressed": 0,
        },
        "last_error": None,
    }
    error_messages = []

    def capture_error(message, *args):
        error_messages.append(message % args if args else message)

    original_error = config_file_sync_module.LOGGER.error
    config_file_sync_module.LOGGER.error = capture_error
    try:
        for _attempt in range(20):
            _record_processing_error(
                error_context, RuntimeError("stable persistence conflict"))
        assert error_context["statistics"]["errors"] == 20
        assert error_context["statistics"]["error_logs"] == 1
        assert error_context["statistics"]["error_logs_suppressed"] == 19
        assert len(error_messages) == 1
        error_context["error_log_state"]["last_logged_s"] -= 31.0
        _record_processing_error(
            error_context, RuntimeError("stable persistence conflict"))
    finally:
        config_file_sync_module.LOGGER.error = original_error
    assert error_context["statistics"]["errors"] == 21
    assert error_context["statistics"]["error_logs"] == 2
    assert len(error_messages) == 2
    assert "19 identical retries suppressed" in error_messages[1]
    # Internal DataCenter persistence is distinguished solely by its stable
    # fingerprint receipt. PREPARED accepts either side of the atomic replace;
    # COMMITTED accepts only the exact written target so a later operator edit
    # back to an old value is never suppressed.
    with tempfile.TemporaryDirectory(prefix="config-origin-receipt-") as directory:
        config_path = os.path.join(directory, "config.json")
        wal_path = os.path.join(directory, "proposal-wal.json")
        receipt_path = os.path.join(directory, "persistence-receipt.json")
        _write(config_path, baseline)
        _state_data, config_api, distributed_api = _fake_runtime(
            config_path, baseline)
        api = create_config_file_sync(
            options={
                "enabled": True, "node_id": "fn-01",
                "config_path": config_path, "wal_path": wal_path,
                "persistence_receipt_path": receipt_path,
            },
            config_capabilities=config_api, distributed_api=distributed_api)
        context = api["context"]
        target = copy.deepcopy(baseline)
        _state(target, 10)["parameters"]["flow_rate"] += 2.0
        previous_fingerprint = type_library.config_fingerprint(baseline)
        target_fingerprint = type_library.config_fingerprint(target)
        receipt = {
            "version": 1,
            "state": "PREPARED",
            "target_path": os.path.abspath(config_path),
            "target_fingerprint": target_fingerprint,
            "previous_fingerprint": previous_fingerprint,
            "revision": 2,
            "saved_at_s": 1.0,
        }
        _write(receipt_path, receipt)
        assert _internal_persistence_receipt(
            context, previous_fingerprint) is not None
        _write(config_path, target)
        assert _internal_persistence_receipt(
            context, target_fingerprint) is not None
        receipt["state"] = "COMMITTED"
        _write(receipt_path, receipt)
        assert _internal_persistence_receipt(
            context, target_fingerprint) is not None
        assert _internal_persistence_receipt(
            context, previous_fingerprint) is None
        _state_data["config"] = copy.deepcopy(target)
        prepare_startup_config_file_sync(context)
        assert context["wal"]["state"] == "IDLE"
        assert context["wal"]["baseline_fingerprint"] == target_fingerprint
        assert context["statistics"]["internal_writes"] == 1
        assert context["statistics"]["proposals"] == 0

    # A first authoritative Master shard supersedes the packaged V9 baseline
    # by materialization only. It must never be reflected as a local file
    # proposal, regardless of how large or structural the shard difference is.
    with tempfile.TemporaryDirectory(prefix="config-master-baseline-") as directory:
        config_path = os.path.join(directory, "config.json")
        wal_path = os.path.join(directory, "proposal-wal.json")
        authority = copy.deepcopy(baseline)
        authority["controllers"][0]["tags"] = ["master-authority"]
        _write(config_path, baseline)
        _write(wal_path, {
            "version": 1, "state": "IDLE", "node_id": "fn-01",
            "baseline_config": baseline,
            "baseline_fingerprint": type_library.config_fingerprint(baseline),
            "updated_at_s": 1.0,
        })
        _state_data, config_api, distributed_api = _fake_runtime(
            config_path, authority)
        api = create_config_file_sync(
            options={
                "enabled": True, "node_id": "fn-01",
                "config_path": config_path, "wal_path": wal_path,
                "config_authority": "master_global",
            },
            config_capabilities=config_api, distributed_api=distributed_api)
        context = api["context"]
        assert _observe_master_baseline(context) is True
        assert context["wal"]["state"] == "IDLE"
        assert context["wal"]["baseline_fingerprint"] \
            == type_library.config_fingerprint(authority)
        assert context["statistics"]["proposals"] == 0
        assert context["statistics"]["local_first"] == 0
        assert context["statistics"]["master_first"] == 0
    print(
        "PASS config file paths; master_precedence_one_warning; "
        "master_precedence_zero_rollback; stable_error_log_coalescing")


if __name__ == "__main__":
    main()


