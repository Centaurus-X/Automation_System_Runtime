# -*- coding: utf-8 -*-

# test/test_runtime_value_store.py


"""
Deterministic concurrency and lifecycle smoke test for the central runtime
sensor-value store. The test uses one writer per controller shard and a
concurrent full-snapshot reader.
"""


import os
import sys
import threading
import time

from functools import partial


PROJECT_ROOT = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._runtime_value_store import (
    claim_controller_writer,
    close_runtime_value_store,
    create_runtime_value_store,
    create_writer_token,
    drain_value_changes,
    get_all_sensor_values_snapshot,
    get_sensor_value_snapshot,
    get_value_store_health_snapshot,
    release_controller_writer,
    write_sensor_value,
)


CONTROLLER_UUIDS = (
    "06ab4528-f8a3-5ec7-a91f-eea79d60761e",
    "62671112-766c-59ed-b0ff-30a49d48f747",
    "82fb9eb0-3fd9-5db5-a04e-7f06e405cfdf",
    "d11032fa-33db-56de-a4ed-7e16cca3ded7",
)

WRITES_PER_CONTROLLER = 250
SENSORS_PER_CONTROLLER = 7


def _check(condition, message):
    if not condition:
        raise AssertionError(message)


def _write_controller_values(
    store,
    controller_uuid,
    writer_token,
    failures,
    failure_lock,
):
    for index in range(WRITES_PER_CONTROLLER):
        result = write_sensor_value(
            store,
            controller_uuid=controller_uuid,
            sensor_id=(index % SENSORS_PER_CONTROLLER) + 1,
            value={
                "iteration": index,
                "controller_uuid": controller_uuid,
            },
            writer_token=writer_token,
            quality="good",
            source="runtime_value_store_test",
        )

        if not result.get("ok"):
            with failure_lock:
                failures.append(result)


def _read_full_snapshots(store, stop_event, observations):
    while not stop_event.is_set():
        snapshot = get_all_sensor_values_snapshot(
            store
        )
        controller_keys = list(
            snapshot["controllers"]
        )

        _check(
            controller_keys == sorted(controller_keys),
            "controller snapshots must be deterministically ordered",
        )

        observations["reads"] += 1
        time.sleep(0.0005)


def _claim_writers(store):
    tokens = {}

    for controller_uuid in CONTROLLER_UUIDS:
        token = create_writer_token(
            "test_controller"
        )
        result = claim_controller_writer(
            store,
            {
                "controller_uuid": controller_uuid,
                "writer_label": "test_controller",
            },
            token,
        )
        _check(result.get("ok"), result.get("message"))
        tokens[controller_uuid] = token

    return tokens


def _verify_writer_isolation(store, tokens):
    controller_uuid = CONTROLLER_UUIDS[0]
    intruder_token = create_writer_token(
        "intruder"
    )
    claim = claim_controller_writer(
        store,
        controller_uuid,
        intruder_token,
    )
    _check(
        not claim.get("ok"),
        "a second controller writer must be rejected",
    )

    write = write_sensor_value(
        store,
        controller_uuid=controller_uuid,
        sensor_id=1,
        value="invalid",
        writer_token=intruder_token,
    )
    _check(
        not write.get("ok"),
        "a foreign writer token must not mutate the shard",
    )
    _check(
        tokens[controller_uuid] != intruder_token,
        "writer tokens must be unique",
    )


def _run_concurrency_test(store, tokens):
    failures = []
    failure_lock = threading.Lock()
    stop_reader = threading.Event()
    observations = {
        "reads": 0,
    }
    reader = threading.Thread(
        target=partial(
            _read_full_snapshots,
            store,
            stop_reader,
            observations,
        ),
        name="RuntimeValueSnapshotReader",
        daemon=False,
    )
    writers = []

    for controller_uuid in CONTROLLER_UUIDS:
        writer = threading.Thread(
            target=partial(
                _write_controller_values,
                store,
                controller_uuid,
                tokens[controller_uuid],
                failures,
                failure_lock,
            ),
            name="RuntimeValueWriter-" + controller_uuid[:8],
            daemon=False,
        )
        writers.append(writer)

    reader.start()

    for writer in writers:
        writer.start()

    for writer in writers:
        writer.join(timeout=10.0)
        _check(not writer.is_alive(), "writer thread did not terminate")

    stop_reader.set()
    reader.join(timeout=5.0)

    _check(not reader.is_alive(), "snapshot reader did not terminate")
    _check(not failures, "sensor writes failed: {}".format(failures[:3]))
    _check(observations["reads"] > 0, "snapshot reader was not scheduled")


def _verify_snapshots(store):
    snapshot = get_all_sensor_values_snapshot(
        store
    )
    expected_writes = len(CONTROLLER_UUIDS) * WRITES_PER_CONTROLLER

    _check(
        snapshot["global_sequence"] == expected_writes,
        "global sequence does not match successful writes",
    )

    for controller_uuid in CONTROLLER_UUIDS:
        controller = snapshot["controllers"][controller_uuid]
        _check(
            controller["revision"] == WRITES_PER_CONTROLLER,
            "controller revision does not match writes",
        )
        _check(
            len(controller["values"]) == SENSORS_PER_CONTROLLER,
            "unexpected sensor count in controller shard",
        )

    sensor = snapshot[
        "controllers"
    ][CONTROLLER_UUIDS[0]]["values"]["1"]
    sensor["value"]["iteration"] = -1
    fresh = get_sensor_value_snapshot(
        store,
        CONTROLLER_UUIDS[0],
        1,
    )
    _check(
        fresh["value"]["iteration"] != -1,
        "external snapshot mutated the internal store",
    )

    changes = drain_value_changes(
        store,
        expected_writes + 10,
    )
    sequences = [
        item["sequence"]
        for item in changes
    ]
    _check(
        len(changes) == expected_writes,
        "persistence outbox lost changes",
    )
    _check(
        sequences == sorted(sequences),
        "persistence batches must be sequence ordered",
    )


def _verify_shutdown(store, tokens):
    for controller_uuid in CONTROLLER_UUIDS:
        result = release_controller_writer(
            store,
            controller_uuid,
            tokens[controller_uuid],
        )
        _check(result.get("ok"), result.get("message"))

    _check(close_runtime_value_store(store), "store close failed")

    rejected = write_sensor_value(
        store,
        controller_uuid=CONTROLLER_UUIDS[0],
        sensor_id=1,
        value="after-close",
        writer_token=tokens[CONTROLLER_UUIDS[0]],
    )
    _check(not rejected.get("ok"), "closed store accepted a write")
    _check(
        get_sensor_value_snapshot(
            store,
            CONTROLLER_UUIDS[0],
            1,
        ) is not None,
        "closed store must remain snapshot-readable",
    )
    health = get_value_store_health_snapshot(
        store
    )
    _check(health["closed"], "health snapshot did not report close")


def run_test():
    store = create_runtime_value_store(
        name="runtime-value-store-test",
        change_queue_maxsize=2048,
    )
    tokens = _claim_writers(store)

    _verify_writer_isolation(store, tokens)
    _run_concurrency_test(store, tokens)
    _verify_snapshots(store)
    _verify_shutdown(store, tokens)

    print(
        "PASS runtime value store: {} writes, {} controller shards".format(
            len(CONTROLLER_UUIDS) * WRITES_PER_CONTROLLER,
            len(CONTROLLER_UUIDS),
        )
    )


if __name__ == "__main__":
    run_test()
