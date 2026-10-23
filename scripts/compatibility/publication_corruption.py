#!/usr/bin/env python3
"""Manual public KV/SQL recovery of damaged overflow references and spools."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
import zlib

from alopex import AlopexError, TxnMode, _alopex
from publication_python import open_db, verify, verify_indexes
from spool_os_write import pending


KINDS = ("reference_crc", "reference_tail", "spool_crc", "spool_truncated")


def child(path, surface):
    db = open_db(path)
    verify(db, surface, False)
    pending(db, surface).commit()
    verify(db, surface, True)
    print("COMMITTED", flush=True)
    os._exit(0)  # Keep the live WAL; close would converge it into the container.


def write_synced(path, data):
    with path.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def case(output, surface, kind):
    name = f"{surface}-{kind}"
    fixture = output / name
    fixture.mkdir()
    path = fixture / "db.alopex"
    result = {"case": name, "status": "failed"}
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
        process = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", str(path), surface],
            capture_output=True, text=True, timeout=30, check=False,
        )
        log = process.stdout + process.stderr
        (output / (name + ".log")).write_text(log)
        assert process.returncode == 0 and "COMMITTED" in log, log
        sidecar = Path(str(path) + ".d")
        wal = sidecar / "lsm.wal"
        spools = list((sidecar / "overflow").iterdir())
        assert len(spools) == 1 and spools[0].suffix != ".tmp", spools
        spool = spools[0]
        wal_bytes, spool_bytes = wal.read_bytes(), spool.read_bytes()
        # One 92-byte segment plus its separate 20-byte section header.
        reference_offset = 20 + 28
        assert len(wal_bytes) == reference_offset + 64, len(wal_bytes)
        assert wal_bytes[20:24] == b"AWAL" and wal_bytes[24:26] == b"\x03\x00"
        record_length = 12 + int.from_bytes(wal_bytes[reference_offset + 8:reference_offset + 12], "little")
        assert record_length == 53
        reference_end = reference_offset + record_length
        reference = wal_bytes[reference_offset:reference_end]
        assert reference[12] == 3
        assert zlib.crc32(reference[:-4]) == int.from_bytes(reference[-4:], "little")
        assert spool_bytes.startswith(b"ALPXOVFL")
        size = sum(p.stat().st_size for p in fixture.rglob("*") if p.is_file())
        assert size <= 1024 * 1024, size
        if kind.startswith("reference"):
            damaged = bytearray(wal_bytes)
            if kind == "reference_crc":
                damaged[reference_end - 1] ^= 1
            else:
                # Model a missing second half in a preallocated ring, not a short syscall.
                cut = record_length // 2
                damaged[reference_offset + cut:reference_end] = bytes(record_length - cut)
            write_synced(wal, damaged)
            expected = False
        else:
            damaged = bytearray(spool_bytes)
            if kind == "spool_crc":
                damaged[-1] ^= 1  # Seal CRC, after all fragments.
            else:
                del damaged[-1:]  # Incomplete seal, after all fragments.
            write_synced(spool, damaged)
            # A referenced bad spool must fail closed, not become a recoverable tail.
            for _ in range(2):
                try:
                    unexpected = open_db(path)
                except AlopexError as error:
                    assert error.code == "ALOPEX-PY999", repr(error)
                    message = "checksum mismatch" if kind == "spool_crc" else "failed to fill whole buffer"
                    assert message in str(error), repr(error)
                    result["rejection"] = str(error)
                else:
                    unexpected.close()
                    raise AssertionError("corrupt referenced spool unexpectedly opened")
                assert wal.read_bytes() == wal_bytes
                assert spool.read_bytes() == damaged
            # Restore exact known-good bytes to prove failed opens did not lose/duplicate data.
            write_synced(spool, spool_bytes)
            expected = True
        for cycle in range(2):
            db = open_db(path)
            try:
                verify(db, surface, expected)
                if surface == "sql":
                    verify_indexes(db, expected)
                assert db.wal_overflow_stats()["gc_failures"] == 0
                if not expected and cycle == 0:
                    assert spool.read_bytes() == spool_bytes, "uncertain-tail evidence must be retained"
            finally:
                db.close()
            del db
        if not expected:
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
    parser.add_argument("--child", nargs=2, metavar=("PATH", "SURFACE"))
    args = parser.parse_args()
    if sys.platform not in ("darwin", "linux") or sys.flags.optimize:
        parser.error("requires Linux/macOS Python with assertions enabled")
    if hasattr(_alopex, "_arm_wal_test_fault"):
        parser.error("requires the ordinary extension without test hooks")
    if args.child:
        child(Path(args.child[0]), args.child[1])
        return
    if args.output is None:
        parser.error("--output is required")
    args.output.mkdir(parents=True, exist_ok=False)
    results = [case(args.output, surface, kind) for surface in ("kv", "sql") for kind in KINDS]
    passed = all(r["status"] == "passed" for r in results)
    report = {
        "status": "passed" if passed else "failed", "cases": results,
        "native_sha256": hashlib.sha256(Path(_alopex.__file__).read_bytes()).hexdigest(),
        "source_sha256": {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                          for name in (Path(__file__).name, "publication_python.py", "spool_os_write.py")},
        "python": sys.version, "platform": sys.platform,
        "scope": "offline damaged-byte recovery; not syscall failures, power loss or release qualification",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
