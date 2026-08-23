# -*- coding: utf-8 -*-

"""Validated JSON-file proposals for legacy and node-local authority modes."""

import copy
import json
import logging
import os
import stat
import tempfile
import threading
import time
import uuid

from functools import partial

from src.library._generic_system_contract_library.config_sharding_contracts import diff_configs

from src.library import _generic_type_library as type_library


LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "config_file_sync",
    "library_version": "2.0.0",
    "options": {
        "enabled": False,
        "poll_interval_s": 0.5,
        "settle_time_s": 0.5,
        "max_file_bytes": 32 * 1024 * 1024,
        "join_timeout_s": 5.0,
        "proposal_timeout_s": 15.0,
        "error_log_interval_s": 30.0,
        "allow_local_runtime": True,
        "allow_structural_proposals": True,
        "config_authority": "node_local",
        "persistence_receipt_path": "",
    },
}

TERMINAL_REJECTED = ("REJECTED", "FAILED", "REBASE_REQUIRED")
PENDING_STATES = ("PENDING", "AWAITING_ACK", "MASTER_ACKED")
RECOVERABLE_STATES = ("PREPARED", "STARTUP_CANDIDATE")


def _is_dict(value):
    return isinstance(value, dict)


def _safe_bool(value, default):
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in ("1", "true", "yes", "on", "enabled"):
        return True
    if normalized in ("0", "false", "no", "off", "disabled"):
        return False
    return bool(default)


def _safe_float(value, default, minimum=0.05):
    try:
        result = float(value)
    except Exception:
        return float(default)
    return result if result >= minimum else float(default)


def _safe_int(value, default, minimum=1):
    try:
        result = int(value)
    except Exception:
        return int(default)
    return result if result >= minimum else int(default)


def _atomic_json(path, payload):
    target = os.path.abspath(str(path))
    parent = os.path.dirname(target)
    if parent:
        os.makedirs(parent, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=parent or None,
        prefix=".config_file_sync_", suffix=".tmp", delete=False,
    )
    temporary_path = handle.name
    try:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary_path, target)
        if parent and hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except Exception:
        handle.close()
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


def _validated_config_object(value, label):
    if not _is_dict(value):
        raise RuntimeError(label + " is missing from the config file proposal WAL")
    report = type_library.verify_config_data(value)
    if not _is_dict(report) or report.get("status") != "OK":
        raise RuntimeError(label + " in the config file proposal WAL is invalid")
    return value


def _read_wal(path, maximum):
    if not os.path.exists(path):
        return None
    _file_signature(path, maximum)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("config file proposal WAL is unreadable") from exc
    if not _is_dict(payload) or int(payload.get("version") or 0) != 1:
        raise RuntimeError("config file proposal WAL version is unsupported")
    if str(payload.get("state") or "") not in (
        "IDLE", "PREPARED", "STARTUP_CANDIDATE", "PENDING",
    ):
        raise RuntimeError("config file proposal WAL state is unsupported")
    baseline = _validated_config_object(
        payload.get("baseline_config"), "baseline configuration")
    if type_library.config_fingerprint(baseline) != str(
            payload.get("baseline_fingerprint") or ""):
        raise RuntimeError("config file proposal WAL baseline fingerprint differs")
    if payload.get("candidate_config") is not None:
        candidate = _validated_config_object(
            payload.get("candidate_config"), "candidate configuration")
        if type_library.config_fingerprint(candidate) != str(
                payload.get("candidate_fingerprint") or ""):
            raise RuntimeError("config file proposal WAL candidate fingerprint differs")
    return payload


def _file_signature(path, maximum):
    details = os.lstat(path)
    if stat.S_ISLNK(details.st_mode):
        raise ValueError("config file symlinks are not allowed")
    if not stat.S_ISREG(details.st_mode):
        raise ValueError("config path is not a regular file")
    if int(details.st_size) > int(maximum):
        raise ValueError("config file exceeds the configured size limit")
    return (int(details.st_mtime_ns), int(details.st_size), int(details.st_ino))


def _verified_file(path, maximum):
    before = _file_signature(path, maximum)
    config, report = type_library.load_config_file(path)
    after = _file_signature(path, maximum)
    if before != after:
        raise ValueError("config file changed during validation")
    if config is None or report.get("status") != "OK":
        raise ValueError("config file failed schema and reference validation")
    return config, str(report.get("stats", {}).get("fingerprint") or "")


def _valid_fingerprint(value, allow_empty=False):
    text = str(value or "")
    if allow_empty and not text:
        return True
    return type_library.is_fingerprint(text)


def _persistence_receipt(context):
    """Read one stable, validated internal-persistence provenance record."""
    path = context["options"].get("persistence_receipt_path")
    if not path or not os.path.exists(path):
        return None
    maximum = min(context["options"]["max_file_bytes"], 1024 * 1024)
    before = _file_signature(path, maximum)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            receipt = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("config persistence receipt is unreadable") from exc
    after = _file_signature(path, maximum)
    if before != after:
        raise RuntimeError("config persistence receipt changed during validation")
    if not _is_dict(receipt) or int(receipt.get("version") or 0) != 1:
        raise RuntimeError("config persistence receipt version is unsupported")
    state = str(receipt.get("state") or "")
    if state not in ("PREPARED", "COMMITTED"):
        raise RuntimeError("config persistence receipt state is unsupported")
    expected_path = os.path.normcase(os.path.abspath(
        context["options"]["config_path"]))
    receipt_path = os.path.normcase(os.path.abspath(str(
        receipt.get("target_path") or "")))
    if receipt_path != expected_path:
        raise RuntimeError("config persistence receipt targets another file")
    if not _valid_fingerprint(receipt.get("target_fingerprint")):
        raise RuntimeError("config persistence receipt fingerprint is invalid")
    if not _valid_fingerprint(
            receipt.get("previous_fingerprint"), allow_empty=True):
        raise RuntimeError("config persistence receipt predecessor is invalid")
    return receipt


def _internal_persistence_receipt(context, fingerprint):
    receipt = _persistence_receipt(context)
    if not receipt:
        return None
    accepted = {str(receipt.get("target_fingerprint") or "")}
    # While PREPARED, the atomic config replace may be either immediately
    # before or immediately after the receipt write. Once COMMITTED, an
    # intentional operator edit back to the previous value must not be hidden.
    if str(receipt.get("state") or "") == "PREPARED":
        accepted.add(str(receipt.get("previous_fingerprint") or ""))
    return receipt if str(fingerprint) in accepted else None


def _states_by_id(config):
    result = {}
    for row in config.get("process_states") or []:
        if _is_dict(row) and row.get("state_id") is not None:
            result[str(row.get("state_id"))] = row
    return result


def _runtime_action(before, after):
    prior = copy.deepcopy(before)
    target = copy.deepcopy(after)
    prior_parameters = prior.pop("parameters", {})
    target_parameters = target.pop("parameters", {})
    prior_value = prior.pop("value_id", None)
    target_value = target.pop("value_id", None)
    if prior != target:
        return None
    if not _is_dict(prior_parameters) or not _is_dict(target_parameters):
        return None
    if set(prior_parameters) - set(target_parameters):
        return None
    changed_parameters = {
        key: copy.deepcopy(target_parameters[key])
        for key in sorted(target_parameters)
        if key not in prior_parameters or prior_parameters[key] != target_parameters[key]
    }
    action = {"state_ids": [copy.deepcopy(after.get("state_id"))]}
    if prior_value != target_value:
        action["value_id"] = copy.deepcopy(target_value)
    if changed_parameters:
        action["parameters"] = changed_parameters
    return action if len(action) > 1 else None


def _local_runtime_actions(before, after, operations):
    if not operations:
        return ()
    if any(operation.get("section") != "process_states" for operation in operations):
        return None
    prior = _states_by_id(before)
    target = _states_by_id(after)
    if set(prior) != set(target):
        return None
    actions = []
    for state_id in sorted(prior):
        if prior[state_id] == target[state_id]:
            continue
        action = _runtime_action(prior[state_id], target[state_id])
        if action is None:
            return None
        actions.append(action)
    return tuple(actions) if actions else None


def _runtime_state_overlay_matches(
        live_state, reference_state, action, missing):
    """Compare one state's fields owned by one runtime action."""
    if not _is_dict(live_state) or not _is_dict(reference_state):
        return False
    if "value_id" in action and live_state.get(
            "value_id", missing) != reference_state.get(
                "value_id", missing):
        return False
    parameters = action.get("parameters")
    if not _is_dict(parameters):
        return True
    live_parameters = live_state.get("parameters") or {}
    reference_parameters = reference_state.get("parameters") or {}
    if (not _is_dict(live_parameters)
            or not _is_dict(reference_parameters)):
        return False
    for key in parameters:
        if live_parameters.get(key, missing) != (
                reference_parameters.get(key, missing)):
            return False
    return True


def _runtime_overlay_matches(config, reference, actions):
    """Compare only fields owned by one runtime action plan."""
    if not _is_dict(config) or not _is_dict(reference):
        return False
    live_states = _states_by_id(config)
    reference_states = _states_by_id(reference)
    missing = object()
    for action in actions or ():
        if not _is_dict(action):
            return False
        state_ids = action.get("state_ids") or ()
        if not isinstance(state_ids, (list, tuple, set)):
            state_ids = (state_ids,)
        for state_id in state_ids:
            live_state = live_states.get(str(state_id))
            reference_state = reference_states.get(str(state_id))
            if not _runtime_state_overlay_matches(
                    live_state, reference_state, action, missing):
                return False
    return True


def _wal_record(context, state, **kwargs):
    record = {
        "version": 1,
        "state": str(state),
        "node_id": context["options"]["node_id"],
        "updated_at_s": time.time(),
    }
    record.update(copy.deepcopy(kwargs))
    _atomic_json(context["options"]["wal_path"], record)
    with context["lock"]:
        context["wal"] = record
    return record


def _idle_wal(context, config, fingerprint, **kwargs):
    return _wal_record(
        context, "IDLE", baseline_config=copy.deepcopy(config),
        baseline_fingerprint=str(fingerprint),
        last_result=copy.deepcopy(kwargs.get("last_result") or {}),
        last_operation=copy.deepcopy(kwargs.get("last_operation") or {}),
    )


def _distributed_snapshot(context):
    reader = context["distributed_api"].get("snapshot")
    return reader() if callable(reader) else {}


def _proposal_record(context, message_id):
    reader = context["distributed_api"].get("get_proposal")
    return reader(message_id) if callable(reader) else None


def _master_precedence_fingerprint(context, wal, result):
    """Resolve the newest known authoritative node-shard fingerprint."""
    snapshot = _distributed_snapshot(context)
    baseline = str(wal.get("baseline_fingerprint") or "")
    candidate = str(wal.get("candidate_fingerprint") or "")
    result_fingerprint = str(result.get("current_scope_fingerprint") or "") \
        if _is_dict(result) else ""
    snapshot_fingerprint = str(snapshot.get("node_shard_fingerprint") or "") \
        if _is_dict(snapshot) else ""
    if (_valid_fingerprint(snapshot_fingerprint)
            and snapshot_fingerprint not in (
                baseline, candidate, result_fingerprint)):
        return snapshot_fingerprint
    if _valid_fingerprint(result_fingerprint):
        return result_fingerprint
    if _valid_fingerprint(snapshot_fingerprint):
        return snapshot_fingerprint
    return ""


def _complete_master_precedence(context, wal, record):
    """Discard one raced local proposal after the Master snapshot is live."""
    result = record.get("result") if _is_dict(record) else {}
    result = result if _is_dict(result) else {}
    authority = _master_precedence_fingerprint(context, wal, result)
    reader = context["config_capabilities"].get("get_snapshot")
    live = reader() if callable(reader) else {}
    live_fingerprint = type_library.config_fingerprint(live) \
        if _is_dict(live) else ""
    if not authority or live_fingerprint != authority:
        with context["lock"]:
            context["statistics"]["authority_waits"] += 1
        return False
    persisted, persisted_fingerprint = _materialize_live_file(
        context,
        expected_external_fingerprint=str(
            wal.get("candidate_fingerprint") or ""),
    )
    if persisted_fingerprint != authority:
        raise RuntimeError("materialized Master authority fingerprint differs")
    _idle_wal(
        context, persisted, persisted_fingerprint,
        last_result={
            "ok": False, "code": "local_discarded_master_precedence",
            "master_result": copy.deepcopy(result),
        })
    with context["lock"]:
        context["statistics"]["master_precedence_discards"] += 1
    LOGGER.warning(
        "Local config proposal %s was not written because a concurrent "
        "Master patch has priority",
        str(wal.get("operation_id") or "unknown"))
    return True


def _restore_baseline(context, wal, result):
    writer = context["config_capabilities"].get("request_replace")
    baseline = wal.get("baseline_config")
    if not callable(writer) or not _is_dict(baseline):
        raise RuntimeError("file proposal baseline cannot be restored")
    restored = writer(
        baseline, source="worker_file_rollback", external=False,
        operation_id="file-rollback:" + str(wal.get("operation_id") or uuid.uuid4()),
        timeout_s=context["options"]["proposal_timeout_s"],
    )
    if not _is_dict(restored) or not restored.get("ok"):
        raise RuntimeError("file proposal rollback failed")
    persisted, fingerprint = _materialize_live_file(
        context,
        expected_external_fingerprint=str(
            wal.get("candidate_fingerprint") or ""),
    )
    _idle_wal(context, persisted, fingerprint, last_result=result)
    with context["lock"]:
        context["statistics"]["rollbacks"] += 1


def _activate_config(
        context, config, operation_id, expected_external_fingerprint=""):
    """Materialize one already validated config through the SEM single writer."""
    writer = context["config_capabilities"].get("request_replace")
    if not callable(writer):
        raise RuntimeError("config replacement capability is unavailable")
    result = writer(
        copy.deepcopy(config), source="worker_file_rollback", external=False,
        operation_id=str(operation_id),
        timeout_s=context["options"]["proposal_timeout_s"],
    )
    if not _is_dict(result) or not result.get("ok"):
        raise RuntimeError("validated config materialization failed")
    _materialize_live_file(
        context,
        expected_external_fingerprint=expected_external_fingerprint,
    )
    return result


def _materialization_error(report):
    """Describe a failed persistence report without discarding its cause."""
    if not _is_dict(report):
        return RuntimeError(
            "live configuration materialization failed: invalid persistence report")
    values = ["status=" + str(report.get("status") or "UNKNOWN")]
    message = str(report.get("message") or "").strip()
    if message:
        values.append("message=" + message)
    details = report.get("details") if _is_dict(report.get("details")) else {}
    external_fingerprint = str(details.get("external_fingerprint") or "")
    if external_fingerprint:
        values.append("external_fingerprint=" + external_fingerprint)
    return RuntimeError(
        "live configuration materialization failed: " + "; ".join(values))


def _materialize_live_file(context, expected_external_fingerprint=""):
    """Persist and re-read the exact current SEM snapshot without a commit."""
    writer = context["config_capabilities"].get("persist_json")
    if not callable(writer):
        raise RuntimeError("config JSON persistence capability is unavailable")
    report = writer(
        expected_external_fingerprint=str(
            expected_external_fingerprint or ""))
    if not _is_dict(report) or report.get("status") != "OK":
        raise _materialization_error(report)
    persisted, fingerprint = _verified_file(
        context["options"]["config_path"],
        context["options"]["max_file_bytes"],
    )
    reported = str((report.get("details") or {}).get("fingerprint") or "")
    if reported and reported != fingerprint:
        raise RuntimeError("materialized config fingerprint differs from receipt")
    with context["lock"]:
        context["statistics"]["materializations"] += 1
    return persisted, fingerprint


def _complete_if_converged(context, wal, record):
    # CONVERGED is emitted only after the authoritative Master revision was
    # activated and live-ACKed by this runtime. Runtime values may legitimately
    # move again before this polling loop runs, so a stale whole-config equality
    # check would deadlock the WAL. Materialise the current live snapshot and
    # close the proposal against that exact, fsync-protected file instead.
    persisted, fingerprint = _materialize_live_file(
        context,
        expected_external_fingerprint=str(
            wal.get("candidate_fingerprint") or ""),
    )
    _idle_wal(
        context, persisted, fingerprint,
        last_result=(record or {}).get("result"),
    )
    with context["lock"]:
        context["statistics"]["converged"] += 1
    return True


def _complete_if_rejected(context, wal, record):
    """Resolve a rejected proposal without overriding newer Master state."""
    result = record.get("result") if _is_dict(record) else {}
    result = result if _is_dict(result) else {}
    state = str(record.get("state") or "") if _is_dict(record) else ""
    code = str(result.get("code") or result.get("status") or "").lower()
    if state == "REBASE_REQUIRED" or code == "rebase_required":
        return _complete_master_precedence(context, wal, record)
    _restore_baseline(context, wal, result)
    return True


def _poll_pending(context):
    wal = copy.deepcopy(context.get("wal") or {})
    if wal.get("state") != "PENDING":
        return False
    record = _proposal_record(context, wal.get("message_id"))
    if not _is_dict(record):
        return False
    state = str(record.get("state") or "")
    if state == "CONVERGED":
        return _complete_if_converged(context, wal, record)
    if state in TERMINAL_REJECTED:
        if not _complete_if_rejected(context, wal, record):
            return False
        with context["lock"]:
            context["statistics"]["rejected"] += 1
        return True
    return state in PENDING_STATES


def _scope_baseline(context):
    snapshot = _distributed_snapshot(context)
    return {
        "scope": "node:" + context["options"]["node_id"],
        "authority_epoch": int(snapshot.get("authority_epoch") or 0),
        "revision": int(snapshot.get("node_scope_revision") or 0),
        "fingerprint": str(snapshot.get("node_shard_fingerprint") or ""),
    }


def _stage_runtime_actions(context, actions, operation_id):
    writer = context["distributed_api"].get("request_action_plan")
    revision_reader = context["config_capabilities"].get("get_revision")
    if not callable(writer) or not callable(revision_reader):
        raise RuntimeError("local runtime proposal capability is unavailable")
    return writer(
        list(actions), operation_id=operation_id,
        base_revision=revision_reader(),
        timeout_s=context["options"]["proposal_timeout_s"],
    )


def _stage_preapplied_runtime_actions(context, actions, operation_id):
    writer = context["distributed_api"].get("stage_applied_action_plan")
    if not callable(writer):
        raise RuntimeError("pre-applied runtime proposal capability is unavailable")
    return writer(list(actions), operation_id=operation_id)


def _stage_structural_operations(
        context, operations, operation_id, scope_baseline=None):
    writer = context["distributed_api"].get("request_scope_operations")
    if not callable(writer):
        raise RuntimeError("structural file proposal capability is unavailable")
    baseline = copy.deepcopy(scope_baseline) \
        if _is_dict(scope_baseline) else _scope_baseline(context)
    return writer(
        list(operations), scope=baseline["scope"],
        base_authority_epoch=baseline["authority_epoch"],
        base_scope_revision=baseline["revision"],
        base_scope_fingerprint=baseline["fingerprint"],
        principal="worker_file_sync",
        operation_id=operation_id,
        proposal_origin="worker_config_file",
        reason="validated Worker JSON file edit",
        timeout_s=context["options"]["proposal_timeout_s"],
    )


def _proposal_message_id(result):
    sync = result.get("sync") if _is_dict(result) else None
    return str((sync or {}).get("message_id") or result.get("message_id") or "")


def _node_local_result_matches(context, wal, result, commit_kind):
    """Validate the exact durable Worker-authority receipt for one file edit."""
    if not _is_dict(result) or not result.get("ok"):
        return None
    post_commit = result.get("post_commit")
    post_commit = post_commit if _is_dict(post_commit) else {}
    if not post_commit.get("ok"):
        raise RuntimeError("node-local file commit has no durable authority receipt")
    scope_baseline = wal.get("scope_baseline")
    scope_baseline = scope_baseline if _is_dict(scope_baseline) else {}
    expected_scope = "node:" + context["options"]["node_id"]
    if str(scope_baseline.get("scope") or "") != expected_scope:
        raise RuntimeError("node-local file commit scope baseline is invalid")
    epoch = post_commit.get("authority_epoch")
    revision = post_commit.get("scope_revision")
    base_epoch = scope_baseline.get("authority_epoch")
    base_revision = scope_baseline.get("revision")
    values = (epoch, revision, base_epoch, base_revision)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        raise RuntimeError("node-local file commit epoch or revision is invalid")
    if epoch != base_epoch or revision < base_revision:
        raise RuntimeError("node-local file commit does not extend its WAL baseline")
    changed = bool(result.get("changed"))
    status = str(post_commit.get("status") or "")
    mirror_id = str(post_commit.get("mirror_id") or "")
    if changed:
        if revision <= base_revision:
            raise RuntimeError("node-local file commit did not advance scope revision")
        if status != "node_authoritative_committed" or not mirror_id:
            raise RuntimeError("node-local file commit has no durable mirror record")
    elif status != "unchanged":
        raise RuntimeError("node-local no-change receipt is invalid")
    fingerprint = str(post_commit.get("fingerprint") or "")
    if not _valid_fingerprint(fingerprint):
        raise RuntimeError("node-local file commit fingerprint is invalid")
    live_reader = context["config_capabilities"].get("get_snapshot")
    live = live_reader() if callable(live_reader) else {}
    if not _is_dict(live):
        raise RuntimeError("node-local committed config snapshot is unavailable")
    live_fingerprint = type_library.config_fingerprint(live)
    actions = list(wal.get("actions") or ())
    candidate = wal.get("candidate_config")
    candidate_matches = (
        _runtime_overlay_matches(live, candidate, actions)
        if commit_kind == "action_plan"
        else live_fingerprint == str(wal.get("candidate_fingerprint") or "")
    )
    if not candidate_matches or fingerprint != live_fingerprint:
        raise RuntimeError("node-local file commit differs from the validated candidate")
    return {
        "authority_mode": "node_local",
        "scope": expected_scope,
        "authority_epoch": epoch,
        "observed_base_scope_revision": base_revision,
        "scope_revision": revision,
        "fingerprint": fingerprint,
        "mirror_id": mirror_id,
        "status": status,
        "changed": changed,
        "commit_kind": str(commit_kind),
    }


def _complete_node_local_result(context, wal, result, commit_kind):
    proof = _node_local_result_matches(context, wal, result, commit_kind)
    if proof is None:
        return False
    persisted, fingerprint = _materialize_live_file(
        context,
        expected_external_fingerprint=str(
            wal.get("candidate_fingerprint") or ""),
    )
    if fingerprint != proof["fingerprint"]:
        raise RuntimeError("node-local file materialization differs from authority")
    last_operation = {
        "operation_id": str(wal.get("operation_id") or ""),
        "mode": "node_local",
        "commit_kind": str(commit_kind),
        "baseline_fingerprint": str(wal.get("baseline_fingerprint") or ""),
        "candidate_fingerprint": str(wal.get("candidate_fingerprint") or ""),
        "operations": copy.deepcopy(wal.get("operations") or []),
        "actions": copy.deepcopy(wal.get("actions") or []),
        "scope_baseline": copy.deepcopy(wal.get("scope_baseline") or {}),
        "authority_commit": proof,
        "sem_result": copy.deepcopy(result),
    }
    _idle_wal(
        context, persisted, fingerprint,
        last_result=result, last_operation=last_operation)
    with context["lock"]:
        context["statistics"]["proposals"] += 1
        context["statistics"]["node_local"] += 1
        context["statistics"]["converged"] += 1
    return True


FILE_MASTER_FIRST_POLICY_CODES = (
    "engine_missing",
    "required_mode",
    "writable_fields",
)


def _staging_failure_details(result):
    """Extract one stable code and message from a failed staging result."""
    if not _is_dict(result):
        return {"code": "", "message": ""}
    commit = result.get("commit") if _is_dict(result.get("commit")) else {}
    details = commit.get("details") if _is_dict(commit.get("details")) else {}
    safety = result.get("safety") if _is_dict(result.get("safety")) else {}
    return {
        "code": str(
            result.get("code")
            or details.get("code")
            or safety.get("code")
            or ""
        ).strip().lower(),
        "message": str(
            result.get("message")
            or commit.get("message")
            or safety.get("message")
            or result.get("error")
            or ""
        ).strip(),
    }


def _staging_failure_reason(result):
    """Format the precise refusal cause without hiding technical failures."""
    details = _staging_failure_details(result)
    values = []
    for label in ("code", "message"):
        value = details.get(label)
        if value:
            values.append(label + "=" + value)
    return "; ".join(values)


def _file_master_first_policy_fallback(result):
    """Separate operator file authority from automatic RuleEngine policy."""
    return _staging_failure_details(result).get(
        "code") in FILE_MASTER_FIRST_POLICY_CODES


def _candidate_plan(context, before, candidate):
    operations = diff_configs(before, candidate, representation="runtime")
    actions = _local_runtime_actions(before, candidate, operations)
    local_runtime = actions is not None and context["options"]["allow_local_runtime"]
    if not local_runtime and not context["options"]["allow_structural_proposals"]:
        raise ValueError("structural file proposals are disabled")
    return list(operations), list(actions or ()), local_runtime


def _prepared_record(context, before, candidate, candidate_fingerprint, **kwargs):
    before_fingerprint = type_library.config_fingerprint(before)
    operations, actions, local_runtime = _candidate_plan(context, before, candidate)
    # The WAL persists this identity across retries/restarts. A fresh edit of
    # the same semantic value must receive a distinct auditable operation ID.
    operation_id = "file:{}:{}:{}:{}".format(
        context["options"]["node_id"], before_fingerprint[:12],
        candidate_fingerprint[:12], uuid.uuid4().hex)
    node_local = context["options"]["config_authority"] == "node_local"
    mode = "node_local" if node_local else (
        "local_first" if local_runtime else "master_first")
    commit_kind = "action_plan" if local_runtime else "scope_operations"
    return _wal_record(
        context, str(kwargs.get("state") or "PREPARED"),
        operation_id=operation_id,
        mode=mode, commit_kind=commit_kind,
        baseline_config=before, baseline_fingerprint=before_fingerprint,
        candidate_config=candidate, candidate_fingerprint=candidate_fingerprint,
        operations=operations, actions=actions,
        scope_baseline=_scope_baseline(context) if node_local else {},
        startup_preapplied=bool(kwargs.get("startup_preapplied")),
    )


def _stage_node_local_prepared(context, wal):
    operation_id = str(wal.get("operation_id") or "")
    baseline = wal.get("baseline_config")
    candidate = wal.get("candidate_config")
    if not operation_id or not _is_dict(baseline) or not _is_dict(candidate):
        raise RuntimeError("prepared node-local file proposal is incomplete")
    live_reader = context["config_capabilities"].get("get_snapshot")
    live = live_reader() if callable(live_reader) else {}
    if not _is_dict(live):
        raise RuntimeError("live configuration snapshot is unavailable")
    baseline_fingerprint = str(wal.get("baseline_fingerprint") or "")
    live_fingerprint = type_library.config_fingerprint(live)
    actions = list(wal.get("actions") or ())
    commit_kind = str(wal.get("commit_kind") or "scope_operations")
    if commit_kind == "action_plan":
        baseline_active = _runtime_overlay_matches(live, baseline, actions)
    else:
        baseline_active = live_fingerprint == baseline_fingerprint
    if not baseline_active:
        raise RuntimeError("node-local file proposal baseline is not active")
    result = None
    if commit_kind == "action_plan":
        result = _stage_runtime_actions(context, actions, operation_id)
        if (
            (not _is_dict(result) or not result.get("ok"))
            and _file_master_first_policy_fallback(result)
        ):
            local_result = copy.deepcopy(result)
            result = _stage_structural_operations(
                context, wal.get("operations") or [],
                operation_id + ":scope-fallback",
                scope_baseline=wal.get("scope_baseline"),
            )
            commit_kind = "scope_operations"
            reason = _staging_failure_reason(local_result)
            LOGGER.warning(
                "Local runtime gate delegated config file operation %s to "
                "canonical Worker node authority (%s)",
                operation_id, reason or "operator file authority required")
    elif commit_kind == "scope_operations":
        result = _stage_structural_operations(
            context, wal.get("operations") or [], operation_id,
            scope_baseline=wal.get("scope_baseline"),
        )
    else:
        raise RuntimeError("prepared node-local file proposal has an invalid commit kind")
    if _complete_node_local_result(context, wal, result, commit_kind):
        return True
    reason = _staging_failure_reason(result)
    LOGGER.warning(
        "Node-local config file operation %s was rejected (%s); restoring "
        "the last confirmed Worker baseline",
        operation_id, reason or "no reason reported")
    _restore_baseline(context, wal, result)
    with context["lock"]:
        context["statistics"]["rejected"] += 1
    raise RuntimeError(
        "node-local file proposal was rejected"
        + ((" (" + reason + ")") if reason else ""))


def _stage_prepared(context):
    wal = copy.deepcopy(context.get("wal") or {})
    if wal.get("state") not in RECOVERABLE_STATES:
        return False
    if str(wal.get("mode") or "") == "node_local":
        return _stage_node_local_prepared(context, wal)
    operation_id = str(wal.get("operation_id") or "")
    mode = str(wal.get("mode") or "")
    baseline = wal.get("baseline_config")
    candidate = wal.get("candidate_config")
    if not operation_id or not _is_dict(baseline) or not _is_dict(candidate):
        raise RuntimeError("prepared file proposal is incomplete")

    live_reader = context["config_capabilities"].get("get_snapshot")
    live = live_reader() if callable(live_reader) else {}
    if not _is_dict(live):
        raise RuntimeError("live configuration snapshot is unavailable")
    live_fingerprint = type_library.config_fingerprint(live)
    baseline_fingerprint = str(wal.get("baseline_fingerprint") or "")
    candidate_fingerprint = str(wal.get("candidate_fingerprint") or "")
    actions = list(wal.get("actions") or ())

    if mode == "local_first":
        if _runtime_overlay_matches(live, candidate, actions):
            result = _stage_preapplied_runtime_actions(
                context, actions, operation_id)
        elif _runtime_overlay_matches(live, baseline, actions):
            result = _stage_runtime_actions(
                context, actions, operation_id)
        else:
            raise RuntimeError("local-first startup proposal conflicts with live config")
    elif mode == "master_first":
        if live_fingerprint != baseline_fingerprint:
            raise RuntimeError("master-first proposal baseline is not active")
        result = _stage_structural_operations(
            context, wal.get("operations") or [], operation_id)
    else:
        raise RuntimeError("prepared file proposal has an invalid authority mode")

    message_id = _proposal_message_id(result)
    if not message_id and mode == "local_first":
        latest_live = live_reader() if callable(live_reader) else {}
        if _runtime_overlay_matches(latest_live, candidate, actions):
            result = _stage_preapplied_runtime_actions(
                context, actions, operation_id)
            message_id = _proposal_message_id(result)
        elif (
            _runtime_overlay_matches(latest_live, baseline, actions)
            and _file_master_first_policy_fallback(result)
        ):
            local_result = copy.deepcopy(result)
            result = _stage_structural_operations(
                context, wal.get("operations") or [], operation_id)
            message_id = _proposal_message_id(result)
            if message_id:
                reason = _staging_failure_reason(local_result)
                mode = "master_first"
                LOGGER.warning(
                    "Local runtime gate deferred config file operation %s "
                    "to Master authority (%s)",
                    operation_id, reason or "operator file authority required")
    if not message_id:
        reason = _staging_failure_reason(result)
        if mode == "local_first":
            LOGGER.warning(
                "Local config file operation %s was rejected before durable "
                "staging (%s); restoring the last confirmed baseline",
                operation_id, reason or "no reason reported")
            _restore_baseline(context, wal, result)
        raise RuntimeError(
            "file proposal was not durably journaled"
            + ((" (" + reason + ")") if reason else ""))
    _wal_record(
        context, "PENDING", operation_id=operation_id,
        message_id=message_id, mode=mode,
        baseline_config=baseline, baseline_fingerprint=baseline_fingerprint,
        candidate_config=candidate, candidate_fingerprint=candidate_fingerprint,
        operations=list(wal.get("operations") or ()),
        actions=actions,
        stage_result=copy.deepcopy(result),
        startup_preapplied=bool(wal.get("startup_preapplied")),
    )
    with context["lock"]:
        context["statistics"]["proposals"] += 1
        context["statistics"]["local_first"] += int(mode == "local_first")
        context["statistics"]["master_first"] += int(mode == "master_first")
    return True


def _process_candidate(context, candidate, candidate_fingerprint):
    snapshot_reader = context["config_capabilities"].get("get_snapshot")
    before = snapshot_reader() if callable(snapshot_reader) else {}
    if not _is_dict(before):
        raise RuntimeError("live configuration snapshot is unavailable")
    before_fingerprint = type_library.config_fingerprint(before)
    if before_fingerprint == candidate_fingerprint:
        _idle_wal(context, before, before_fingerprint)
        return True
    _prepared_record(context, before, candidate, candidate_fingerprint)
    return _stage_prepared(context)


def _prepare_node_local_recovery(context, wal):
    """Restore the confirmed baseline before replaying a crash-visible edit."""
    baseline = _validated_config_object(
        wal.get("baseline_config"), "node-local recovery baseline")
    candidate = _validated_config_object(
        wal.get("candidate_config"), "node-local recovery candidate")
    baseline_fingerprint = type_library.config_fingerprint(baseline)
    candidate_fingerprint = type_library.config_fingerprint(candidate)
    if str(wal.get("baseline_fingerprint") or "") != baseline_fingerprint:
        raise RuntimeError("node-local recovery baseline fingerprint differs")
    if str(wal.get("candidate_fingerprint") or "") != candidate_fingerprint:
        raise RuntimeError("node-local recovery candidate fingerprint differs")
    live_reader = context["config_capabilities"].get("get_snapshot")
    live = live_reader() if callable(live_reader) else {}
    live_fingerprint = type_library.config_fingerprint(live) \
        if _is_dict(live) else ""
    if live_fingerprint != baseline_fingerprint:
        _activate_config(
            context, baseline,
            "file-node-local-recovery:" + str(
                wal.get("operation_id") or uuid.uuid4()),
            expected_external_fingerprint=candidate_fingerprint,
        )
    preserved = {
        key: copy.deepcopy(value)
        for key, value in wal.items()
        if key not in (
            "version", "state", "node_id", "updated_at_s",
            "message_id", "stage_result",
        )
    }
    preserved["mode"] = "node_local"
    preserved["commit_kind"] = str(
        preserved.get("commit_kind")
        or ("action_plan" if preserved.get("actions") else "scope_operations"))
    preserved["scope_baseline"] = _scope_baseline(context)
    preserved["startup_preapplied"] = False
    _wal_record(context, "STARTUP_CANDIDATE", **preserved)
    if str(wal.get("state") or "") == "PENDING":
        with context["lock"]:
            context["statistics"]["pending_recovered"] += 1
    context["state"] = "prepared"
    return get_config_file_sync_snapshot(context)


def prepare_startup_config_file_sync(api_or_context):
    """Classify a stopped edit before any controller thread can observe it.

    Node-local candidates are retained in the WAL while the confirmed Worker
    baseline is rematerialized through SEM. The candidate is committed only by
    the Worker authority path after startup preparation has completed.
    """
    context = api_or_context.get("context") \
        if "context" in api_or_context else api_or_context
    if not context["options"]["enabled"]:
        context["state"] = "disabled"
        return get_config_file_sync_snapshot(context)
    wal = copy.deepcopy(context.get("wal") or {})
    state = str(wal.get("state") or "")
    if (
        context["options"]["config_authority"] == "node_local"
        and state in (("PENDING",) + RECOVERABLE_STATES)
    ):
        return _prepare_node_local_recovery(context, wal)
    if state == "PENDING":
        proposal = _proposal_record(context, wal.get("message_id")) or {}
        if not proposal:
            # The proposal WAL and the distributed SQLite outbox are separate
            # crash domains.  A Fresh rollout deliberately resets the replica
            # journal while preserving this operator-facing WAL.  Recreate the
            # missing outbox entry from the original durable operation ID; the
            # Master remains authoritative and deduplicates that identity.
            preserved = {
                key: copy.deepcopy(value)
                for key, value in wal.items()
                if key not in (
                    "version", "state", "node_id", "updated_at_s",
                    "message_id", "stage_result",
                )
            }
            _wal_record(context, "PREPARED", **preserved)
            with context["lock"]:
                context["statistics"]["pending_recovered"] += 1
            context["state"] = "prepared"
            return get_config_file_sync_snapshot(context)
        proposal_state = str(proposal.get("state") or "")
        if proposal_state == "CONVERGED":
            live_reader = context["config_capabilities"].get("get_snapshot")
            live = live_reader() if callable(live_reader) else {}
            live_fingerprint = type_library.config_fingerprint(live) \
                if _is_dict(live) else ""
            baseline_fingerprint = str(
                wal.get("baseline_fingerprint") or "")
            candidate = wal.get("candidate_config")
            # A crash can occur after the authoritative live ACK but before
            # the candidate is durably materialised. Only the exact old
            # baseline is advanced here; a later, legitimately evolved live
            # state is never rolled back to a stale whole-file candidate.
            if (live_fingerprint == baseline_fingerprint
                    and _is_dict(candidate)):
                _activate_config(
                    context, candidate,
                    "file-startup-converged:" + str(
                        wal.get("operation_id") or uuid.uuid4()),
                    expected_external_fingerprint=str(
                        wal.get("candidate_fingerprint") or ""),
                )
            _complete_if_converged(context, wal, proposal)
            context["state"] = "prepared"
            return get_config_file_sync_snapshot(context)
        if proposal_state in TERMINAL_REJECTED:
            completed = _complete_if_rejected(context, wal, proposal)
            context["state"] = "prepared" if completed else "awaiting_master"
            return get_config_file_sync_snapshot(context)
        live_reader = context["config_capabilities"].get("get_snapshot")
        live = live_reader() if callable(live_reader) else {}
        live_fingerprint = type_library.config_fingerprint(live) \
            if _is_dict(live) else ""
        baseline = wal.get("baseline_config")
        candidate = wal.get("candidate_config")
        baseline_fingerprint = str(wal.get("baseline_fingerprint") or "")
        candidate_fingerprint = str(wal.get("candidate_fingerprint") or "")
        mode = str(wal.get("mode") or "")
        target = None
        if mode == "local_first" and live_fingerprint != candidate_fingerprint:
            target = candidate
        elif mode == "master_first":
            proposal = _proposal_record(context, wal.get("message_id")) or {}
            accepted = str(proposal.get("state") or "") in (
                "MASTER_ACKED", "CONVERGED")
            if not accepted and live_fingerprint != baseline_fingerprint:
                target = baseline
            elif accepted and live_fingerprint not in (
                    baseline_fingerprint, candidate_fingerprint):
                target = baseline
        if target is not None:
            _activate_config(
                context, target,
                "file-startup-pending:" + str(
                    wal.get("operation_id") or uuid.uuid4()),
                expected_external_fingerprint=candidate_fingerprint,
            )
        context["state"] = "prepared"
        return get_config_file_sync_snapshot(context)

    candidate, candidate_fingerprint = _verified_file(
        context["options"]["config_path"],
        context["options"]["max_file_bytes"],
    )
    if state in RECOVERABLE_STATES:
        candidate = copy.deepcopy(wal.get("candidate_config") or candidate)
        candidate_fingerprint = str(
            wal.get("candidate_fingerprint") or candidate_fingerprint)
        baseline = copy.deepcopy(wal.get("baseline_config") or {})
        mode = str(wal.get("mode") or "")
    else:
        receipt = _internal_persistence_receipt(
            context, candidate_fingerprint)
        if receipt is not None:
            _idle_wal(
                context, candidate, candidate_fingerprint,
                last_result={
                    "ok": True,
                    "code": "internal_persistence",
                    "receipt_state": receipt.get("state"),
                },
            )
            with context["lock"]:
                context["statistics"]["internal_writes"] += 1
            context["state"] = "prepared"
            return get_config_file_sync_snapshot(context)
        baseline = copy.deepcopy(wal.get("baseline_config") or {})
        baseline_fingerprint = str(wal.get("baseline_fingerprint") or "")
        if not baseline or candidate_fingerprint == baseline_fingerprint:
            context["state"] = "prepared"
            return get_config_file_sync_snapshot(context)
        record = _prepared_record(
            context, baseline, candidate, candidate_fingerprint,
            state="STARTUP_CANDIDATE", startup_preapplied=True,
        )
        mode = str(record.get("mode") or "")

    if not _is_dict(baseline):
        raise RuntimeError("startup file proposal has no confirmed baseline")
    if mode in ("master_first", "node_local"):
        baseline_fingerprint = type_library.config_fingerprint(baseline)
        live_reader = context["config_capabilities"].get("get_snapshot")
        live = live_reader() if callable(live_reader) else {}
        if type_library.config_fingerprint(live) != baseline_fingerprint:
            _activate_config(
                context, baseline,
                "file-startup-baseline:" + str(
                    (context.get("wal") or {}).get("operation_id") or uuid.uuid4()),
                expected_external_fingerprint=candidate_fingerprint,
            )
        # Preserve the classified candidate after SEM materialized the baseline.
        preserved = copy.deepcopy(context.get("wal") or wal)
        preserved["state"] = "STARTUP_CANDIDATE"
        preserved["startup_preapplied"] = False
        if mode == "node_local":
            preserved["scope_baseline"] = _scope_baseline(context)
        _wal_record(
            context, "STARTUP_CANDIDATE",
            **{key: value for key, value in preserved.items()
               if key not in ("version", "state", "node_id", "updated_at_s")}
        )
    context["state"] = "prepared"
    return get_config_file_sync_snapshot(context)


def _observe_master_baseline(context):
    if context["options"]["config_authority"] == "node_local":
        return False
    if (context.get("wal") or {}).get("state") != "IDLE":
        return False
    snapshot = _distributed_snapshot(context)
    accepted = str(snapshot.get("node_shard_fingerprint") or "")
    live_reader = context["config_capabilities"].get("get_snapshot")
    live = live_reader() if callable(live_reader) else {}
    if not accepted or not _is_dict(live):
        return False
    live_fingerprint = type_library.config_fingerprint(live)
    if accepted != live_fingerprint:
        return False
    current = str((context.get("wal") or {}).get("baseline_fingerprint") or "")
    if current == live_fingerprint:
        return False
    persisted, persisted_fingerprint = _materialize_live_file(context)
    if persisted_fingerprint != accepted:
        return False
    _idle_wal(context, persisted, persisted_fingerprint)
    return True


def _record_processing_error(context, error):
    """Count every retry but emit one bounded log stream per stable cause."""
    now_s = time.monotonic()
    signature = type(error).__name__ + ":" + str(error)
    should_log = False
    suppressed = 0
    with context["lock"]:
        state = context["error_log_state"]
        if state.get("signature") != signature:
            state.update({
                "signature": signature, "last_logged_s": 0.0,
                "suppressed": 0})
        elapsed_s = now_s - float(state.get("last_logged_s") or 0.0)
        if not state.get("last_logged_s") or elapsed_s >= (
                context["options"]["error_log_interval_s"]):
            should_log = True
            suppressed = int(state.get("suppressed") or 0)
            state["last_logged_s"] = now_s
            state["suppressed"] = 0
            context["statistics"]["error_logs"] += 1
        else:
            state["suppressed"] = int(state.get("suppressed") or 0) + 1
            context["statistics"]["error_logs_suppressed"] += 1
        context["statistics"]["errors"] += 1
        context["last_error"] = str(error)
    if not should_log:
        return
    if suppressed:
        LOGGER.error(
            "Config file proposal processing failed: %s "
            "(%d identical retries suppressed)", error, suppressed)
        return
    LOGGER.error("Config file proposal processing failed: %s", error)


def _watch_loop(context):
    context["state"] = "running"
    pending_signature = None
    pending_since = None
    while not context["stop_event"].wait(context["options"]["poll_interval_s"]):
        try:
            if (context.get("wal") or {}).get("state") == "PENDING":
                _poll_pending(context)
                continue
            if (context.get("wal") or {}).get("state") in RECOVERABLE_STATES:
                _stage_prepared(context)
                continue
            _observe_master_baseline(context)
            signature = _file_signature(
                context["options"]["config_path"],
                context["options"]["max_file_bytes"],
            )
            if signature != pending_signature:
                pending_signature = signature
                pending_since = time.monotonic()
                continue
            if pending_since is None or time.monotonic() - pending_since < context["options"]["settle_time_s"]:
                continue
            candidate, fingerprint = _verified_file(
                context["options"]["config_path"],
                context["options"]["max_file_bytes"],
            )
            baseline = str((context.get("wal") or {}).get("baseline_fingerprint") or "")
            if fingerprint != baseline:
                receipt = _internal_persistence_receipt(context, fingerprint)
                if receipt is not None:
                    _idle_wal(
                        context, candidate, fingerprint,
                        last_result={
                            "ok": True,
                            "code": "internal_persistence",
                            "receipt_state": receipt.get("state"),
                        },
                    )
                    with context["lock"]:
                        context["statistics"]["internal_writes"] += 1
                else:
                    _process_candidate(context, candidate, fingerprint)
            pending_since = time.monotonic()
        except Exception as exc:
            _record_processing_error(context, exc)
    context["state"] = "stopped"


def create_config_file_sync(**kwargs):
    supplied = copy.deepcopy(kwargs.get("options") or {})
    options = copy.deepcopy(CONTEXT["options"])
    options.update(supplied)
    options["enabled"] = _safe_bool(options.get("enabled"), False)
    options["node_id"] = str(options.get("node_id") or "node-unknown")
    options["config_authority"] = str(
        options.get("config_authority") or "node_local").strip().lower()
    if options["config_authority"] not in ("master_global", "node_local"):
        raise ValueError("config_authority is invalid")
    options["config_path"] = os.path.abspath(str(options.get("config_path") or ""))
    options["wal_path"] = os.path.abspath(str(options.get("wal_path") or ""))
    receipt_path = str(options.get("persistence_receipt_path") or "")
    options["persistence_receipt_path"] = (
        os.path.abspath(receipt_path) if receipt_path else "")
    options["poll_interval_s"] = _safe_float(options.get("poll_interval_s"), 0.5)
    options["settle_time_s"] = _safe_float(options.get("settle_time_s"), 0.5)
    options["proposal_timeout_s"] = _safe_float(options.get("proposal_timeout_s"), 15.0)
    options["error_log_interval_s"] = _safe_float(
        options.get("error_log_interval_s"), 30.0)
    options["join_timeout_s"] = _safe_float(options.get("join_timeout_s"), 5.0)
    options["max_file_bytes"] = _safe_int(options.get("max_file_bytes"), 32 * 1024 * 1024)
    options["allow_local_runtime"] = _safe_bool(options.get("allow_local_runtime"), True)
    options["allow_structural_proposals"] = _safe_bool(
        options.get("allow_structural_proposals"), True)
    if options["enabled"] and (not options["config_path"] or not options["wal_path"]):
        raise ValueError("config file sync requires config_path and wal_path")
    configured_paths = [
        item for item in (
            options["config_path"], options["wal_path"],
            options["persistence_receipt_path"],
        ) if item
    ]
    if len({os.path.normcase(item) for item in configured_paths}) != len(configured_paths):
        raise ValueError("config file sync paths must be distinct")
    wal = _read_wal(
        options["wal_path"], options["max_file_bytes"] * 2,
    ) if options["enabled"] else None
    if wal is not None and str(wal.get("node_id") or "") != options["node_id"]:
        raise RuntimeError("config file proposal WAL belongs to another Worker")
    if wal is None and options["enabled"]:
        reader = kwargs["config_capabilities"].get("get_snapshot")
        baseline = reader() if callable(reader) else {}
        wal = {
            "version": 1, "state": "IDLE", "node_id": options["node_id"],
            "baseline_config": copy.deepcopy(baseline),
            "baseline_fingerprint": type_library.config_fingerprint(baseline),
            "updated_at_s": time.time(),
        }
        _atomic_json(options["wal_path"], wal)
    context = {
        "module": CONTEXT["library_name"], "version": CONTEXT["library_version"],
        "state": "created", "options": options,
        "config_capabilities": kwargs.get("config_capabilities") or {},
        "distributed_api": kwargs.get("distributed_api") or {},
        "wal": wal or {}, "thread": None, "stop_event": threading.Event(),
        "lock": threading.RLock(), "last_error": None,
        "error_log_state": {
            "signature": "", "last_logged_s": 0.0, "suppressed": 0},
        "statistics": {
            "proposals": 0, "node_local": 0,
            "local_first": 0, "master_first": 0,
            "converged": 0, "rejected": 0, "rollbacks": 0, "errors": 0,
            "error_logs": 0, "error_logs_suppressed": 0,
            "authority_waits": 0,
            "master_precedence_discards": 0,
            "internal_writes": 0, "materializations": 0,
            "pending_recovered": 0,
        },
    }
    return {
        "context": context,
        "prepare_startup": partial(prepare_startup_config_file_sync, context),
        "start": partial(start_config_file_sync, context),
        "stop": partial(stop_config_file_sync, context),
        "snapshot": partial(get_config_file_sync_snapshot, context),
    }


def start_config_file_sync(api_or_context):
    context = api_or_context.get("context") if "context" in api_or_context else api_or_context
    if not context["options"]["enabled"]:
        context["state"] = "disabled"
        return None
    thread = context.get("thread")
    if thread is not None and thread.is_alive():
        return thread
    context["stop_event"].clear()
    thread = threading.Thread(
        target=partial(_watch_loop, context), name="ConfigFileSync", daemon=False)
    context["thread"] = thread
    thread.start()
    return thread


def stop_config_file_sync(api_or_context, **kwargs):
    context = api_or_context.get("context") if "context" in api_or_context else api_or_context
    context["stop_event"].set()
    thread = context.get("thread")
    timeout = float(kwargs.get("timeout_s") or context["options"]["join_timeout_s"])
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=timeout)
    if thread is None or not thread.is_alive():
        context["state"] = "stopped"
    return get_config_file_sync_snapshot(context)


def get_config_file_sync_snapshot(api_or_context):
    context = api_or_context.get("context") if "context" in api_or_context else api_or_context
    thread = context.get("thread")
    with context["lock"]:
        return {
            "module": context["module"], "version": context["version"],
            "state": context["state"], "enabled": context["options"]["enabled"],
            "thread_alive": bool(thread and thread.is_alive()),
            "config_path": context["options"]["config_path"],
            "wal_path": context["options"]["wal_path"],
            "persistence_receipt_path": context["options"][
                "persistence_receipt_path"],
            "wal_state": str((context.get("wal") or {}).get("state") or ""),
            "baseline_fingerprint": str(
                (context.get("wal") or {}).get("baseline_fingerprint") or ""),
            "candidate_fingerprint": str(
                (context.get("wal") or {}).get("candidate_fingerprint") or ""),
            "last_error": context.get("last_error"),
            "statistics": copy.deepcopy(context["statistics"]),
        }




