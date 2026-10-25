#!/usr/bin/env python3
"""Manual Linux inline WAL sync failure/exit acceptance, with an ordinary extension."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import traceback

from alopex import AlopexError, Database, TxnMode, WalOverflowOptions, _alopex
from publication_python import expected_error

SYNC = re.compile(r"^(fsync|fdatasync)\(\d+<([^>]+)>\)\s+= (.+)$")


def open_db(path):
    return Database.open(str(path), wal_overflow=WalOverflowOptions(
        payload_bytes=16384, spool_bytes=32768, directory_bytes=131072,
        replay_bytes=32768, key_bytes=1024, value_bytes=4096,
        fragment_bytes=256, wal_segment_bytes=4124, wal_segments=1,
        inline_bytes=1024, activation_bytes=8192, container_fence_bytes=16384,
    ))


def verify(db, surface, committed):
    if surface == "sql":
        expected = [{"id": 1, "v": 10}]
        if committed:
            expected += [{"id": 2, "v": 20}, {"id": 3, "v": 30}]
        assert db.execute_sql("SELECT id, v FROM items ORDER BY id") == expected
        # Check the primary-key index without retaining any attempted insertion.
        for key in (2, 3):
            tx = db.begin(TxnMode.READ_WRITE)
            refused = False
            try:
                tx.execute_sql(f"INSERT INTO items VALUES ({key}, 99)")
            except AlopexError:
                refused = True
            finally:
                tx.rollback()
            assert refused == committed, key
    else:
        tx = db.begin(TxnMode.READ_ONLY)
        assert tx.get(b"old") == b"ok"
        assert tx.get(b"a") == (b"x" if committed else None)
        assert tx.get(b"b") == (b"y" if committed else None)
        tx.rollback()
    tx = db.begin(TxnMode.READ_ONLY)
    assert tx.get(b"retry") is None
    tx.rollback()


def seed(path, surface):
    db = open_db(path)
    if surface == "sql":
        db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, v INT)")
        db.execute_sql("INSERT INTO items VALUES (1, 10)")
    else:
        tx = db.begin(TxnMode.READ_WRITE)
        tx.put(b"old", b"ok")
        tx.commit()
    db.close()


def child(path, surface, mode):
    import resource

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    db = open_db(path)
    verify(db, surface, False)
    tx = db.begin(TxnMode.READ_WRITE)
    if surface == "sql":
        tx.execute_sql("INSERT INTO items VALUES (2, 20), (3, 30)")
    else:
        tx.put(b"a", b"x")
        tx.put(b"b", b"y")
    os.write(2, b"ALOPEX_COMMIT_BEGIN\n")
    if mode == "baseline":
        tx.commit()
    else:
        expected_error(tx.commit, f"os error {errno.EIO}")
        assert mode == "eio", "SIGKILL did not terminate the commit"
    os.write(2, b"ALOPEX_COMMIT_END\n")
    verify(db, surface, mode == "baseline")
    if mode == "eio":
        retry = db.begin(TxnMode.READ_WRITE)
        retry.put(b"retry", b"no")
        expected_error(retry.commit, "WAL outcome uncertain; reopen before further writes")
    print("COMMIT_OK" if mode == "baseline" else "EIO_RETURNED", flush=True)
    os._exit(0)  # Preserve WAL/replay, bypassing destructor checkpoint/convergence.


def sync_events(trace, mode):
    counts = {"fsync": 0, "fdatasync": 0}
    active = False
    begin = end = 0
    events = []
    for line in trace.splitlines():
        if line.startswith("write(2<") and '"ALOPEX_COMMIT_BEGIN\\n"' in line:
            begin += 1
            active = True
        elif line.startswith("write(2<") and '"ALOPEX_COMMIT_END\\n"' in line:
            end += 1
            active = False
        match = SYNC.fullmatch(line)
        if match:
            syscall, path, result = match.groups()
            counts[syscall] += 1
            events.append(dict(syscall=syscall, path=path, result=result,
                               ordinal=counts[syscall], during_commit=active))
    assert begin == 1 and end == int(mode != "kill"), (begin, end)
    return events


def target_event(events, path):
    commit = [e for e in events if e["during_commit"]]
    # A spool path would add fsyncs and a second WAL fdatasync. Fail closed.
    assert len(commit) == 1, commit
    target = commit[0]
    assert target["syscall"] == "fdatasync"
    assert target["path"] == str(path) + ".d/lsm.wal", target
    return target


def traced_child(path, surface, mode, output, name, injection=None):
    trace_path = output / (name + ".trace")
    log_path = output / (name + ".log")
    command = ["strace", "-qq", "-yy", "-s", "128", "-o", str(trace_path),
               "-e", "trace=fsync,fdatasync,write"]
    if injection:
        extra = ":signal=SIGKILL" if mode == "kill" else ""
        command += ["-e", f"inject=fdatasync:error=EIO{extra}:when={injection['ordinal']}"]
    command += [sys.executable, str(Path(__file__).resolve()), "--child", str(path), surface, mode]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               start_new_session=True)
    try:
        log, _ = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        log, _ = process.communicate(timeout=10)
        log_path.write_bytes(log)
        raise
    log_path.write_bytes(log)
    trace = trace_path.read_text()
    assert trace_path.stat().st_size < 1024 * 1024
    if mode == "kill":
        assert process.returncode in (-signal.SIGKILL, 128 + signal.SIGKILL), log
        assert "+++ killed by SIGKILL +++" in trace
        assert b"EIO_RETURNED" not in log and b"COMMIT_OK" not in log
    else:
        assert process.returncode == 0, log.decode(errors="replace")
        assert (b"COMMIT_OK" if mode == "baseline" else b"EIO_RETURNED") in log
    return sync_events(trace, mode)


def case(output, surface, mode):
    name = f"{surface}-{mode}"
    fixture = output / name
    fixture.mkdir()
    result = dict(case=name, status="failed")
    try:
        baseline = fixture / "baseline.alopex"
        seed(baseline, surface)
        events = traced_child(baseline, surface, "baseline", output, name + ".baseline")
        target = target_event(events, baseline)
        assert target["result"] == "0"
        assert list(Path(str(baseline) + ".d/overflow").iterdir()) == []
        db = open_db(baseline)
        verify(db, surface, True)
        db.close()

        path = fixture / "fault.alopex"
        seed(path, surface)
        events = traced_child(path, surface, mode, output, name + ".fault", target)
        hit = target_event(events, path)
        injected = [e for e in events if "INJECTED" in e["result"]]
        assert hit["ordinal"] == target["ordinal"], hit
        if mode == "eio":
            assert injected == [hit] and hit["result"].startswith("-1 EIO "), hit
        else:
            # SIGKILL prevents the attempted syscall from returning. strace
            # reports '?' rather than an EIO return/INJECTED annotation. The
            # exact path/ordinal, open commit interval and signal are required.
            assert injected == [] and hit["result"] == "?", hit
        spool_dir = Path(str(path) + ".d/overflow")
        assert list(spool_dir.iterdir()) == [], "case did not stay on inline path"
        size = sum(p.stat().st_size for p in fixture.rglob("*") if p.is_file())
        assert size <= 1024 * 1024, size
        # Injected sync failure leaves complete writes in the page cache. Recovery
        # may therefore expose the whole transaction despite no successful return.
        for _ in range(2):
            db = open_db(path)
            verify(db, surface, True)
            assert db.wal_overflow_stats()["gc_failures"] == 0
            db.close()
            # Successful close may converge the sidecar and remove its empty
            # overflow directory. Both absent and empty mean no retained spool.
            assert not spool_dir.exists() or list(spool_dir.iterdir()) == []
        result.update(status="passed", fixture_bytes=size, syscall=hit["syscall"],
                      ordinal=hit["ordinal"], target_suffix="lsm.wal", replay="complete",
                      reopen_cycles=2, spool_bytes=0, baseline="synced commit then process exit")
        result["sync_outcome"] = "EIO returned" if mode == "eio" else "SIGKILL before observed syscall return"
    except Exception:
        result["failure"] = traceback.format_exc()
    finally:
        shutil.rmtree(fixture)
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--case", choices=[f"{s}-{m}" for s in ("kv", "sql") for m in ("eio", "kill")])
    parser.add_argument("--child", nargs=3, metavar=("PATH", "SURFACE", "MODE"))
    args = parser.parse_args()
    if sys.platform != "linux" or sys.flags.optimize:
        parser.error("requires Linux Python with assertions enabled")
    if any(hasattr(_alopex, name) for name in ("_arm_wal_test_fault", "_arm_wal_test_reference_pause")):
        parser.error("requires the ordinary extension without debug hooks")
    if args.child:
        child(Path(args.child[0]), args.child[1], args.child[2])
        return
    if args.output is None or not shutil.which("strace"):
        parser.error("--output and strace with syscall injection are required")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    results = [case(args.output, s, m) for s in ("kv", "sql") for m in ("eio", "kill")
               if args.case is None or args.case == f"{s}-{m}"]
    passed = all(r["status"] == "passed" for r in results)
    report = dict(status="passed" if passed else "failed", cases=results,
                  native_sha256=hashlib.sha256(Path(_alopex.__file__).read_bytes()).hexdigest(),
                  source_sha256={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                 for name in (Path(__file__).name, "publication_python.py")},
                  strace=subprocess.check_output(["strace", "--version"], text=True).splitlines()[0],
                  python=sys.version, scope="inline syscall EIO/SIGKILL with page cache retained; not device power loss")
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
