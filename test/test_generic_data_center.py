# -*- coding: utf-8 -*-

# tests/test_generic_data_center.py

"""
Full functional verification of _generic_data_center against the real
device_state_data_v5_multi_worker_norm.json dataset. Functional style,
no test framework - deterministic assert based checks with a summary.
"""

import json
import os
import atexit
import shutil
import sqlite3
import threading
import logging
import sys
import tempfile
from functools import partial

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library import _generic_data_center as dc
from src.library import _generic_type_library as type_library

logging.basicConfig(level=logging.WARNING)

SOURCE_JSON = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json")
TEST_DIRECTORY = tempfile.mkdtemp(prefix="automation_dc_test_")
atexit.register(partial(shutil.rmtree, TEST_DIRECTORY, ignore_errors=True))
WORK_JSON = os.path.join(TEST_DIRECTORY, "config.json")
CANON_JSON = os.path.join(TEST_DIRECTORY, "canonical.json")
WORK_DB = os.path.join(TEST_DIRECTORY, "config.sqlite")

RESULTS = []


def check(name, condition, detail=""):
    RESULTS.append((name, bool(condition), detail))
    marker = "PASS" if condition else "FAIL"
    print(f"[{marker}] {name}" + (f"  ({detail})" if detail else ""))


def collect_event(bucket, event):
    bucket.append(event)


# --- 1. context + verified load -----------------------------------------
context = dc.create_data_center_context(
    name="test_center", json_path=SOURCE_JSON,
    sqlite_enabled=True, sqlite_path=WORK_DB,
    allow_legacy_import=True, legacy_import_profile="lab_v2", sort_mode="none",
    preserve_config_identity_on_save=True)

events = []
for event_type in (dc.EVENT_HEAP_LOADED, dc.EVENT_HEAP_COMMIT,
                   dc.EVENT_HEAP_SAVED, dc.EVENT_SQLITE_SYNCED,
                   dc.EVENT_SHARD_SYNC, dc.EVENT_WRITE_REJECTED):
    dc.subscribe_event(context, event_type, partial(collect_event, events))

load_report = dc.load_json_into_heap(context)
check("verified load OK", load_report["status"] == "OK")
check("26 sections loaded", len(dc.list_sections(context)) == 26,
      str(len(dc.list_sections(context))))
check("revision == 1 after load", dc.get_revision(context) == 1)
check("EVENT_HEAP_LOADED published",
      any(e["event_type"] == dc.EVENT_HEAP_LOADED for e in events))
source_fp = dc.get_fingerprint(context)
check("fingerprint present", len(source_fp) == 64, source_fp[:16])

# --- 2. generic path reads (all path syntaxes) ---------------------------
host_a = dc.heap_get(context, "controllers[0].host")
host_b = dc.heap_get(context, "controllers.0.host")
host_c = dc.heap_get(context, ["controllers", 0, "host"])
host_d = dc.heap_get(context, "controllers/0/host")
check("path syntax equivalence", host_a == host_b == host_c == host_d == "192.168.0.51")
check("missing path -> default", dc.heap_get(context, "controllers[0].nope", "X") == "X")
check("heap_exists true/false",
      dc.heap_exists(context, "rules[0].when") and
      not dc.heap_exists(context, "rules[99]"))
snap_found, sensors_snap = dc.heap_snapshot(context, "sensors")
sensor_group_index = next(
    index
    for index, group in enumerate(sensors_snap)
    if group.get("rows") and group["rows"][0].get("address") == 1
)
sensors_snap[sensor_group_index]["rows"][0]["address"] = 99999
check("snapshot isolation (no heap mutation)",
      dc.heap_get(
          context,
          "sensors[{}].rows[0].address".format(sensor_group_index),
      ) == 1)

# --- 3. single-writer commits --------------------------------------------
r_set = dc.request_write_commit(context, "controllers[0].port", 503,
                                source="worker_thread_1")
check("commit set OK", r_set["status"] == "OK" and
      dc.heap_get(context, "controllers[0].port") == 503)
r_upd = dc.request_write_commit(
    context, "process_states[0].parameters",
    {"target_temp_c": 555.5}, mode="update", source="rule_engine")
check("commit update (dict merge) OK", r_upd["status"] == "OK" and
      dc.heap_get(context, "process_states[0].parameters.target_temp_c") == 555.5)
r_app = dc.request_write_commit(context, "controllers[0].tags", "line2",
                                mode="append")
check("commit append OK", r_app["status"] == "OK" and
      "line2" in dc.heap_get(context, "controllers[0].tags"))
r_del = dc.request_write_commit(context, "controllers[0].tags[2]",
                                mode="delete")
check("commit delete OK", r_del["status"] == "OK" and
      len(dc.heap_get(context, "controllers[0].tags")) == 2)
r_bad = dc.request_write_commit(context, "controllers[99].port", 1,
                                mode="update")
check("invalid commit rejected + event", r_bad["status"] == "ERROR" and
      any(e["event_type"] == dc.EVENT_WRITE_REJECTED for e in events))
r_verify = dc.request_write_commit(context, "controllers[0].port", "bad_type",
                                   verify=True)
check("verify=True blocks schema-invalid value", r_verify["status"] == "ERROR"
      and dc.heap_get(context, "controllers[0].port") == 503)
commit_events = [e for e in events if e["event_type"] == dc.EVENT_HEAP_COMMIT]
check("commit events carry revision+section",
      commit_events and all("revision" in e["payload"] and
                            "section" in e["payload"] for e in commit_events))

# --- 4. JSON save (table style) + verified reload round-trip -------------
save_report = dc.save_heap_to_json(context, WORK_JSON)
check("table-style save OK", save_report["status"] == "OK")
reloaded, reload_verify = type_library.load_config_file(WORK_JSON)
check("saved file passes full verification", reloaded is not None and
      reload_verify["status"] == "OK")
check("round-trip fingerprint identical",
      type_library.config_fingerprint(reloaded) == dc.get_fingerprint(context))
with open(WORK_JSON, "r", encoding="utf-8") as fh:
    saved_text = fh.read()
check("table layout: one record per line",
      any(
          '"controller_id"' in line and '"ctrl_wago_1"' in line
          and line.lstrip().startswith("{")
          for line in saved_text.splitlines()
      ))

# --- 5. canonical style save ----------------------------------------------
dc.configure_data_center(context, json_style="canonical")
canon_report = dc.save_heap_to_json(context, CANON_JSON)
with open(CANON_JSON, "r", encoding="utf-8") as fh:
    canon_text = fh.read().strip()
canonical_snapshot = type_library.project_config_for_storage(
    dc.heap_snapshot(context)[1])
check("canonical save byte-stable",
      canon_report["status"] == "OK" and
      canon_text == type_library.canonical_dumps(canonical_snapshot))
dc.configure_data_center(context, json_style="table")

# --- 6. SQLite full mirror + lossless restore -----------------------------
sql_report = dc.sync_heap_to_sqlite(context)
check("sqlite sync OK", sql_report["status"] == "OK")
check("sqlite mirrored all 26 sections",
      len(sql_report["details"]["tables"]) == 26)
connection = sqlite3.connect(WORK_DB)
cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM rules")
rule_count = cursor.fetchone()[0]
cursor.execute("SELECT host, port FROM controllers WHERE controller_key = ?",
               ("ctrl_wago_1",))
host, port = cursor.fetchone()
cursor.execute('SELECT "value" FROM "__datacenter_meta__" WHERE "key"=?',
               ("revision",))
db_revision = int(cursor.fetchone()[0])
cursor.execute('SELECT COUNT(*) FROM "__section_registry__"')
registry_count = cursor.fetchone()[0]
connection.close()
check("typed SQL queries work", rule_count == 14 and host == "192.168.0.51"
      and port == 503, f"rules={rule_count} host={host} port={port}")
check("meta + registry tables written",
      db_revision == dc.get_revision(context) and registry_count == 26)

restore_context = dc.create_data_center_context(name="restore_center")
restore_report = dc.load_heap_from_sqlite(restore_context, WORK_DB)
check("lossless sqlite restore (verified)",
      restore_report["status"] == "OK" and
      dc.get_fingerprint(restore_context) == dc.get_fingerprint(context))

# --- 7. generic sharding (controller_uuid) --------------------------------
shard_keys = dc.list_shard_keys(context)
check("shard keys: global + 5 controller uuids", len(shard_keys) == 6 and
      shard_keys[0] == dc.GLOBAL_SHARD_KEY, str(len(shard_keys)))
wago2_uuid = dc.heap_get(context, "controllers[1].controller_uuid")
package = dc.get_shard_snapshot(context, wago2_uuid)
sections = package["sections"]
check("shard package self-describing",
      package["shard_key"] == wago2_uuid and
      package["revision"] == dc.get_revision(context) and
      package["fingerprint"] == dc.get_fingerprint(context))
check("shard: exactly 1 controller row",
      len(sections["controllers"]) == 1 and
      sections["controllers"][0]["controller_key"] == "ctrl_wago_2")
check("shard: sensor group of wago_2 (6 rows)",
      len(sections["sensors"]) == 1 and
      len(sections["sensors"][0]["rows"]) == 6)
check("shard: routing row included",
      any(row["controller_uuid"] == wago2_uuid
          for row in sections["site_controller_map"]))
check("shard: global reference data attached",
      len(sections["scaling_profiles"]) == 4 and
      len(sections["rules"]) == 14 and len(sections["meta"]) == 1)
local_only = dc.get_shard_snapshot(context, wago2_uuid, include_global=False)
check("shard without global layer",
      "rules" not in local_only["sections"] and
      "controllers" in local_only["sections"])
site_shards = dc.list_shard_keys(context, "site_id")
check("dynamic shard field (site_id)", "site-main" in site_shards)

# --- 8. event driven downstream sync chain ---------------------------------
sync_bucket = []
dc.subscribe_event(context, dc.EVENT_SHARD_SYNC, partial(collect_event, sync_bucket))
dc.publish_shard_sync(context, wago2_uuid)
check("EVENT_SHARD_SYNC delivers full package",
      sync_bucket and
      sync_bucket[0]["payload"]["package"]["shard_key"] == wago2_uuid)

chain = []


def downstream_worker_sync(data_context, shard_key, bucket, event):
    """Simulated downstream worker: pulls its shard AFTER central commit."""
    shard = dc.get_shard_snapshot(data_context, shard_key)
    bucket.append((event["payload"]["revision"], shard["revision"],
                   shard["sections"]["controllers"][0]["port"]))


handler = partial(downstream_worker_sync, context, wago2_uuid, chain)
dc.subscribe_event(context, dc.EVENT_HEAP_COMMIT, handler)
dc.request_write_commit(context, "controllers[1].port", 504,
                        source="worker_thread_2")
dc.unsubscribe_event(context, dc.EVENT_HEAP_COMMIT, handler)
check("sync chain runs AFTER central write (revisions match)",
      len(chain) == 1 and chain[0][0] == chain[0][1] and chain[0][2] == 504,
      str(chain))

# --- 9. free-threaded hammering (single-writer under contention) -----------
start_revision = dc.get_revision(context)
THREADS, COMMITS = 8, 25


def hammer(data_context, thread_number):
    for i in range(COMMITS):
        dc.request_write_commit(
            data_context, f"meta[0].hammer_{thread_number}", i,
            source=f"thread_{thread_number}")


workers = [threading.Thread(target=partial(hammer, context, n))
           for n in range(THREADS)]
for worker in workers:
    worker.start()
for worker in workers:
    worker.join()
delta = dc.get_revision(context) - start_revision
check("thread-safe: every commit counted exactly once",
      delta == THREADS * COMMITS, f"delta={delta}")
for n in range(THREADS):
    dc.request_write_commit(context, f"meta[0].hammer_{n}", mode="delete")

# --- 10. write-through convenience + autosync -------------------------------
wt = dc.update_json_value(context, "controllers[0].port", 502,
                          file_path=WORK_JSON)
reread = json.load(open(WORK_JSON, "r", encoding="utf-8"))
check("update_json_value write-through", wt["status"] == "OK" and
      reread["controllers"][0]["port"] == 502)
dc.configure_data_center(context, autosync_sqlite=True)
before = context["statistics"]["sqlite_syncs"]
dc.request_write_commit(context, "controllers[1].port", 502)
check("autosync_sqlite after commit",
      context["statistics"]["sqlite_syncs"] == before + 1)
dc.configure_data_center(context, autosync_sqlite=False)

bad_option = dc.configure_data_center(context, no_such_option=1)
check("unknown option rejected", bad_option["status"] == "ERROR")

status = dc.describe_data_center(context)
check("describe_data_center consistent",
      status["sections"] == 26 and status["revision"] == dc.get_revision(context))

# --- summary ----------------------------------------------------------------
failed = [name for name, ok, _ in RESULTS if not ok]
print("\n" + "=" * 60)
print(f"TOTAL: {len(RESULTS)}  PASS: {len(RESULTS) - len(failed)}  "
      f"FAIL: {len(failed)}")
if failed:
    print("FAILED:", failed)
    raise SystemExit(1)
print("ALL CHECKS PASSED - _generic_data_center is complete and correct")
