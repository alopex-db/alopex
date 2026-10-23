#!/usr/bin/env python3
"""Manual Unix spool write refusal/recovery using real per-child OS limits."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import traceback

from alopex import AlopexError, TxnMode, _alopex
from publication_python import open_db, verify, verify_indexes


def pending(db, surface):
    tx = db.begin(TxnMode.READ_WRITE)
    if surface == "sql":
        tx.execute_sql("INSERT INTO items VALUES (2, 'a', '" + "x" * 65 + "'), (3, 'b', 'y')")
    else:
        tx.put(b"pending-a", bytes([7]) * 65)
        tx.put(b"pending-b", bytes([8]))
    return tx


def child(path, surface, mode, limit):
    import resource

    # No core files, including when the OS terminates the child with SIGXFSZ.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    db = open_db(path)
    verify(db, surface, False)
    tx = pending(db, surface)
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    previous = resource.getrlimit(resource.RLIMIT_FSIZE)
    resource.setrlimit(resource.RLIMIT_FSIZE, (limit, previous[1]))
    # Confirm this host's short-write/EFBIG behavior in the same process first.
    probe = path.parent / "limit-probe"
    fd = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        if limit:
            assert os.write(fd, b"x" * (limit + 1)) == limit
        try:
            os.write(fd, b"x")
        except OSError as error:
            assert error.errno == errno.EFBIG, error
        else:
            raise AssertionError("OS file-size limit did not reject the write")
        assert os.fstat(fd).st_size == limit
    finally:
        os.close(fd)
        probe.unlink()
    if mode == "signal":
        signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
    print(f"OS_LIMIT_CONFIRMED {limit}", flush=True)  # stdout is a pipe, not a file.
    try:
        tx.commit()
    except AlopexError as error:
        assert mode == "error", "expected OS signal termination"
        assert error.code == "ALOPEX-PY999", repr(error)
        assert f"os error {errno.EFBIG}" in str(error), repr(error)
    else:
        raise AssertionError("commit unexpectedly succeeded under the OS limit")
    resource.setrlimit(resource.RLIMIT_FSIZE, previous)
    verify(db, surface, False)
    if surface == "sql":
        verify_indexes(db, False)
    print("EFBIG_RETURNED", flush=True)
    os._exit(0)  # Preserve the error outcome without destructor convergence.


def case(output, surface, mode, limit):
    name = f"{surface}-{mode}-{limit}"
    fixture = output / name
    fixture.mkdir()
    path = fixture / "db.alopex"
    result = {"case": name, "limit_bytes": limit, "status": "failed"}
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
        spool = Path(str(path) + ".d") / "overflow"
        # Closing the container may remove the converged sidecar altogether.
        assert not spool.exists() or list(spool.iterdir()) == []
        process = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", str(path),
             surface, mode, str(limit)], capture_output=True, text=True,
            timeout=30, check=False,
        )
        log = process.stdout + process.stderr
        (output / (name + ".log")).write_text(log)
        assert process.returncode == (-signal.SIGXFSZ if mode == "signal" else 0), log
        assert f"OS_LIMIT_CONFIRMED {limit}" in log, log
        files = list(spool.iterdir())
        if mode == "signal":
            assert len(files) == 1 and files[0].suffix == ".tmp", files
            data = files[0].read_bytes()
            assert len(data) == limit, (len(data), limit)
            if limit:
                assert data.startswith(b"ALPXOVFL"), data
            result["partial_spool_bytes"] = len(data)
        else:
            assert "EFBIG_RETURNED" in log, log
            assert files == [], files
        size = sum(p.stat().st_size for p in fixture.rglob("*") if p.is_file())
        assert size <= 1024 * 1024, size
        for _ in range(2):
            db = open_db(path)
            try:
                verify(db, surface, False)
                assert db.wal_overflow_stats()["gc_failures"] == 0
                assert list(spool.iterdir()) == [], "unreferenced partial spool was not reclaimed"
                if surface == "sql":
                    verify_indexes(db, False)
            finally:
                db.close()
            del db
        # A subsequent normal commit must still succeed after recovery.
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
        result.update(status="passed", fixture_bytes=size, recovery_cycles=2)
    except Exception:
        result["failure"] = traceback.format_exc()
    finally:
        shutil.rmtree(fixture)
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--child", nargs=4, metavar=("PATH", "SURFACE", "MODE", "LIMIT"))
    args = parser.parse_args()
    if sys.platform not in ("darwin", "linux") or sys.flags.optimize:
        parser.error("requires Linux/macOS Python with assertions enabled")
    if hasattr(_alopex, "_arm_wal_test_fault"):
        parser.error("requires the ordinary extension without test hooks")
    if args.child:
        path, surface, mode, limit = args.child
        child(Path(path), surface, mode, int(limit))
        return
    if args.output is None:
        parser.error("--output is required")
    args.output.mkdir(parents=True, exist_ok=False)
    results = [case(args.output, surface, mode, limit)
               for surface in ("kv", "sql") for limit in (0, 32) for mode in ("error", "signal")]
    passed = all(r["status"] == "passed" for r in results)
    report = {
        "status": "passed" if passed else "failed", "cases": results,
        "native_sha256": hashlib.sha256(Path(_alopex.__file__).read_bytes()).hexdigest(),
        "source_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in
                          (Path(__file__), Path(__file__).with_name("publication_python.py"))},
        "python": sys.version, "platform": sys.platform,
        "scope": "real OS spool write refusal/SIGXFSZ; not fsync failure, reference short write or power loss",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
