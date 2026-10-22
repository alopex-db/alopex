#!/usr/bin/env python3
"""Manual, instrumented-build WAL boundary acceptance; never a load test."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback

from alopex import AlopexError, Database, TxnMode, WalOverflowOptions, _alopex


STAGES = {
    1: "spool_write", 2: "spool_sync", 3: "spool_publish",
    4: "before_reference", 5: "reference_append_sync", 8: "reference_force_sync",
}


def open_db(path):
    return Database.open(str(path), wal_overflow=WalOverflowOptions(
        payload_bytes=16384, spool_bytes=32768, directory_bytes=131072,
        replay_bytes=32768, key_bytes=1024, value_bytes=4096,
        fragment_bytes=256, wal_segment_bytes=92, wal_segments=1,
        inline_bytes=1024, activation_bytes=4096, container_fence_bytes=16384,
    ))


def verify(db, surface, committed):
    if surface == "sql":
        expected = [{"id": 1, "code": "old", "value": "ok"}]
        if committed:
            expected += [{"id": 2, "code": "a", "value": "x" * 65},
                         {"id": 3, "code": "b", "value": "y"}]
        assert db.execute_sql("SELECT id, code, value FROM items ORDER BY id") == expected
    else:
        tx = db.begin(TxnMode.READ_ONLY)
        assert tx.get(b"old") == b"ok"
        assert tx.get(b"pending-a") == (bytes([7]) * 65 if committed else None)
        assert tx.get(b"pending-b") == (bytes([8]) if committed else None)
        assert tx.get(b"retry") is None
        tx.rollback()


def verify_indexes(db, committed):
    for statement in ("INSERT INTO items VALUES (2, 'unused', 'z')",
                      "INSERT INTO items VALUES (9, 'a', 'z')"):
        tx = db.begin(TxnMode.READ_WRITE)
        failed = False
        try:
            tx.execute_sql(statement)
        except AlopexError:
            failed = True
        finally:
            tx.rollback()
        assert failed == committed, statement


def expected_error(action, message):
    try:
        action()
    except AlopexError as error:
        assert error.code == "ALOPEX-PY999", repr(error)
        assert message in str(error), repr(error)
    else:
        raise AssertionError("expected persistence error was not returned")


def child(path, surface, mode, stage):
    db = open_db(path)
    verify(db, surface, False)
    tx = db.begin(TxnMode.READ_WRITE)
    if surface == "sql":
        tx.execute_sql("INSERT INTO items VALUES (2, 'a', '" + "x" * 65 + "'), (3, 'b', 'y')")
    else:
        tx.put(b"pending-a", bytes([7]) * 65)
        tx.put(b"pending-b", bytes([8]))
    _alopex._arm_wal_test_fault(stage, mode == "exit")
    print("ARMED", flush=True)
    expected_error(tx.commit, "injected overflow persistence failure")
    assert mode == "error", "exit hook was not reached"
    verify(db, surface, False)
    if stage >= 4:
        retry = db.begin(TxnMode.READ_WRITE)
        retry.put(b"retry", b"no")
        expected_error(retry.commit, "WAL outcome uncertain; reopen before further writes")
    print("ERROR_RETURNED", flush=True)
    os._exit(0)  # Bypass Database cleanup/converge, preserving the fault boundary.


def case(output, surface, mode, stage):
    name = f"{surface}-{mode}-{STAGES[stage]}"
    fixture = output / name
    fixture.mkdir()
    path = fixture / "db.alopex"
    result = {"case": name, "stage": stage, "status": "failed"}
    try:
        db = open_db(path)
        if surface == "sql":
            db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE, value TEXT)")
            db.execute_sql("INSERT INTO items VALUES (1, 'old', 'ok')")
        else:
            tx = db.begin(TxnMode.READ_WRITE)
            tx.put(b"old", b"ok")
            tx.commit()
        db.close()
        del db
        log_path = output / (name + ".log")
        with log_path.open("w") as log:
            process = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--child", str(path),
                 surface, mode, str(stage)], stdout=log, stderr=subprocess.STDOUT,
                timeout=30, check=False,
            )
        log = log_path.read_text()
        assert process.returncode == (80 + stage if mode == "exit" else 0), log
        assert "ARMED" in log, log
        if mode == "error":
            assert "ERROR_RETURNED" in log, log
        # One previous key/row, two pending keys/rows; cap physical fixture too.
        size = sum(p.stat().st_size for p in fixture.rglob("*") if p.is_file())
        assert size <= 1024 * 1024, size
        for _ in range(2):
            db = open_db(path)
            try:
                verify(db, surface, stage >= 5)
                assert db.wal_overflow_stats()["gc_failures"] == 0
                if surface == "sql":
                    verify_indexes(db, stage >= 5)
            finally:
                db.close()
            del db
        result.update(status="passed", fixture_bytes=size, reopen_cycles=2)
    except Exception:
        result["failure"] = traceback.format_exc()
    finally:
        shutil.rmtree(fixture)
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--child", nargs=4, metavar=("PATH", "SURFACE", "MODE", "STAGE"))
    args = parser.parse_args()
    if sys.flags.optimize:
        parser.error("assertions must be enabled; do not use Python -O")
    if sys.platform == "win32" or not hasattr(_alopex, "_arm_wal_test_fault"):
        parser.error("requires a non-Windows debug extension built with --cfg alopex_wal_test_hooks")
    if args.child:
        path, surface, mode, stage = args.child
        child(Path(path), surface, mode, int(stage))
        return
    if args.output is None:
        parser.error("--output is required")
    args.output.mkdir(parents=True, exist_ok=False)
    results = [case(args.output, surface, mode, stage)
               for surface in ("kv", "sql") for stage in STAGES for mode in ("error", "exit")]
    passed = all(r["status"] == "passed" for r in results)
    report = {
        "status": "passed" if passed else "failed", "cases": results,
        "native_sha256": hashlib.sha256(Path(_alopex.__file__).read_bytes()).hexdigest(),
        "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python": sys.version,
        "scope": "instrumented process exit/error boundaries, not power-loss or release-wheel qualification",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
