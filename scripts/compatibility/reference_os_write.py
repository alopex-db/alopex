#!/usr/bin/env python3
"""Manual Linux reference short-write/recovery with an instrumented timing barrier."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import time
import traceback
import zlib

from alopex import TxnMode, _alopex
from publication_python import expected_error, open_db, verify, verify_indexes
from spool_os_write import pending

MARKER = b"ALOPEX_BEFORE_REFERENCE\n"
REFERENCE_OFFSET = 20 + 28


def child(path, surface, mode):
    import resource

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    previous = resource.getrlimit(resource.RLIMIT_FSIZE)
    signal.signal(signal.SIGXFSZ, signal.SIG_DFL if mode == "signal" else signal.SIG_IGN)
    db = open_db(path)
    verify(db, surface, False)
    tx = pending(db, surface)
    _alopex._arm_wal_test_reference_pause()
    expected_error(tx.commit, f"os error {errno.EFBIG}")
    assert mode == "error", "expected SIGXFSZ termination"
    resource.setrlimit(resource.RLIMIT_FSIZE, previous)
    verify(db, surface, False)
    retry = db.begin(TxnMode.READ_WRITE)
    retry.put(b"retry", b"no")
    expected_error(retry.commit, "WAL outcome uncertain; reopen before further writes")
    print("EFBIG_RETURNED_WRITER_POISONED", flush=True)
    os._exit(0)  # Keep the failed WAL and sealed spool, without convergence.


def wait_for_marker(process, deadline):
    data = b""
    while not data.endswith(b"\n"):
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
            raise TimeoutError("reference barrier was not reached within 30 seconds")
        block = os.read(process.stdout.fileno(), 256)
        if not block:
            raise AssertionError(f"child exited before barrier: {data!r}")
        data += block
        assert len(data) <= 256, data
    assert data == MARKER, data
    return data


def reference_from_spool(data):
    # Build the expected prefix from the independently persisted spool identity.
    assert data[:8] == b"ALPXOVFL" and data[8:12] == b"\x01\x00\x00\x00"
    assert zlib.crc32(data[:52]) == int.from_bytes(data[52:56], "little")
    assert data[-16:-8] == b"ALPXSEAL"
    assert zlib.crc32(data[-16:-4]) == int.from_bytes(data[-4:], "little")
    reference = (data[28:36] + (41).to_bytes(4, "little") + b"\x03\x01\x00\x00\x00"
                 + data[12:28] + data[36:44] + data[44:48] + data[-8:-4])
    assert len(reference) == 49 and int.from_bytes(reference[:8], "little") > 0
    return reference + zlib.crc32(reference).to_bytes(4, "little")


def case(output, surface, mode, written):
    import resource

    name = f"{surface}-{mode}-{written}"
    fixture = output / name
    fixture.mkdir()
    path = fixture / "db.alopex"
    result = {"case": name, "status": "failed", "reference_bytes_written": written}
    process = None
    log = b""
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
        deadline = time.monotonic() + 30
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--child", str(path), surface, mode],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0,
        )
        log = wait_for_marker(process, deadline)
        sidecar = Path(str(path) + ".d")
        spool_dir = sidecar / "overflow"
        spools = list(spool_dir.iterdir())
        assert len(spools) == 1 and spools[0].suffix != ".tmp", spools
        spool = spools[0]
        spool_bytes = spool.read_bytes()
        reference = reference_from_spool(spool_bytes)
        wal = sidecar / "lsm.wal"
        before = wal.read_bytes()
        assert len(before) == REFERENCE_OFFSET + 64
        assert before[20:26] == b"AWAL\x03\x00"
        assert before[REFERENCE_OFFSET:] == bytes(64), "reference already written before barrier"
        # Change this child only, after the complete spool is durably published.
        previous = resource.prlimit(process.pid, resource.RLIMIT_FSIZE)
        limit = REFERENCE_OFFSET + written
        resource.prlimit(process.pid, resource.RLIMIT_FSIZE, (limit, previous[1]))
        assert resource.prlimit(process.pid, resource.RLIMIT_FSIZE) == (limit, previous[1])
        remaining = deadline - time.monotonic()
        assert remaining > 0
        rest, _ = process.communicate(input=b"G", timeout=remaining)
        log += rest
        assert process.returncode == (-signal.SIGXFSZ if mode == "signal" else 0), log.decode()
        if mode == "error":
            assert b"EFBIG_RETURNED_WRITER_POISONED" in log, log.decode()
        after = wal.read_bytes()
        assert len(after) == len(before)  # RLIMIT applies to writes inside a preallocated file too.
        assert after[REFERENCE_OFFSET:limit] == reference[:written]
        assert after[limit:] == before[limit:], "bytes beyond the OS limit changed"
        assert spool.read_bytes() == spool_bytes, "uncertain outcome must retain the sealed spool"
        size = sum(p.stat().st_size for p in fixture.rglob("*") if p.is_file())
        assert size <= 1024 * 1024, size
        for cycle in range(2):
            db = open_db(path)
            try:
                verify(db, surface, False)
                if surface == "sql":
                    verify_indexes(db, False)
                assert db.wal_overflow_stats()["gc_failures"] == 0
                if cycle == 1:
                    assert list(spool_dir.iterdir()) == [], "unreferenced spool survived complete replay"
            finally:
                db.close()
            del db
        db = open_db(path)
        pending(db, surface).commit()
        db.close()
        db = open_db(path)
        try:
            verify(db, surface, True)
            if surface == "sql":
                verify_indexes(db, True)
        finally:
            db.close()
        del db
        result.update(status="passed", fixture_bytes=size, reopen_cycles=2, os_limit_bytes=limit)
    except Exception:
        result["failure"] = traceback.format_exc()
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
            rest, _ = process.communicate(timeout=10)
            # communicate() may return the same already collected output on a second call.
            if not log.endswith(rest):
                log += rest
        (output / (name + ".log")).write_bytes(log)
        shutil.rmtree(fixture)
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--case", choices=[f"{surface}-{mode}-{written}"
                        for surface in ("kv", "sql") for written in (0, 26)
                        for mode in ("error", "signal")], help="run one isolated acceptance case")
    parser.add_argument("--child", nargs=3, metavar=("PATH", "SURFACE", "MODE"))
    args = parser.parse_args()
    if sys.platform != "linux" or sys.flags.optimize:
        parser.error("requires Linux Python with assertions enabled")
    if not hasattr(_alopex, "_arm_wal_test_reference_pause"):
        parser.error("requires an explicitly instrumented debug extension")
    if args.child:
        child(Path(args.child[0]), args.child[1], args.child[2])
        return
    if args.output is None:
        parser.error("--output is required")
    args.output.mkdir(parents=True, exist_ok=False)
    results = [case(args.output, surface, mode, written)
               for surface in ("kv", "sql") for written in (0, 26) for mode in ("error", "signal")
               if args.case is None or args.case == f"{surface}-{mode}-{written}"]
    passed = all(r["status"] == "passed" for r in results)
    report = {
        "status": "passed" if passed else "failed", "cases": results,
        "native_sha256": hashlib.sha256(Path(_alopex.__file__).read_bytes()).hexdigest(),
        "source_sha256": {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                          for name in (Path(__file__).name, "publication_python.py", "spool_os_write.py")},
        "python": sys.version, "platform": sys.platform,
        "scope": "real Linux reference write refusal/SIGXFSZ with a timing-only debug barrier; not fsync failure or power loss",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
