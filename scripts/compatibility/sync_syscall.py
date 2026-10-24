#!/usr/bin/env python3
"""Manual Linux sync-syscall EIO acceptance using an ordinary Python extension."""
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

from alopex import TxnMode, _alopex
from publication_python import expected_error, open_db, verify, verify_indexes
from spool_os_write import pending

STAGES = ("spool", "directory", "reference_append", "reference_force")
SYNC = re.compile(r"^(fsync|fdatasync)\(\d+<([^>]+)>\)\s+= (.+)$")


def child(path, surface, stage):
    import resource

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    db = open_db(path)
    verify(db, surface, False)
    tx = pending(db, surface)
    os.write(2, b"ALOPEX_COMMIT_BEGIN\n")
    if stage == "baseline":
        tx.commit()
    else:
        expected_error(tx.commit, f"os error {errno.EIO}")
    os.write(2, b"ALOPEX_COMMIT_END\n")
    verify(db, surface, stage == "baseline")
    if stage.startswith("reference"):
        retry = db.begin(TxnMode.READ_WRITE)
        retry.put(b"retry", b"no")
        expected_error(retry.commit, "WAL outcome uncertain; reopen before further writes")
    print("COMMIT_OK" if stage == "baseline" else "EIO_RETURNED", flush=True)
    os._exit(0)  # Preserve the exact outcome without destructor convergence.


def seed(path, surface):
    db = open_db(path)
    if surface == "sql":
        db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE, value TEXT)")
        db.execute_sql("INSERT INTO items VALUES (1, 'old', 'ok')")
    else:
        tx = db.begin(TxnMode.READ_WRITE)
        tx.put(b"old", b"ok")
        tx.commit()
    db.close()


def sync_events(trace):
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
    assert begin == end == 1, (begin, end)
    return events


def select_target(events, path, stage):
    sidecar = str(path) + ".d"
    commit = [e for e in events if e["during_commit"]]
    if stage == "spool":
        matches = [e for e in commit if e["syscall"] == "fsync"
                   and e["path"].startswith(sidecar + "/overflow/")
                   and e["path"].endswith(".tmp")]
    elif stage == "directory":
        matches = [e for e in commit if e["syscall"] == "fsync"
                   and e["path"] == sidecar + "/overflow"]
    else:
        matches = [e for e in commit if e["syscall"] == "fdatasync"
                   and e["path"] == sidecar + "/lsm.wal"]
        assert len(matches) == 2, matches
        matches = [matches[0 if stage == "reference_append" else 1]]
    assert len(matches) == 1 and matches[0]["result"] == "0", matches
    return matches[0]


def traced_child(path, surface, stage, trace_path, log_path, injection=None):
    command = ["strace", "-qq", "-yy", "-s", "128", "-o", str(trace_path),
               "-e", "trace=fsync,fdatasync,write"]
    if injection:
        command += ["-e", f"inject={injection['syscall']}:error=EIO:when={injection['ordinal']}"]
    command += [sys.executable, str(Path(__file__).resolve()), "--child", str(path), surface, stage]
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
    assert process.returncode == 0, log.decode(errors="replace")
    assert (b"COMMIT_OK" if stage == "baseline" else b"EIO_RETURNED") in log
    assert trace_path.stat().st_size < 1024 * 1024, "trace budget exceeded"
    return sync_events(trace_path.read_text())


def case(output, surface, stage):
    name = f"{surface}-{stage}"
    fixture = output / name
    fixture.mkdir()
    result = dict(case=name, status="failed")
    try:
        baseline = fixture / "baseline.alopex"
        seed(baseline, surface)
        events = traced_child(baseline, surface, "baseline", output / (name + ".baseline.trace"),
                              output / (name + ".baseline.log"))
        target = select_target(events, baseline, stage)
        # The calibration itself must produce the complete transaction and indexes.
        db = open_db(baseline)
        verify(db, surface, True)
        if surface == "sql":
            verify_indexes(db, True)
        db.close()

        path = fixture / "fault.alopex"
        seed(path, surface)
        events = traced_child(path, surface, stage, output / (name + ".fault.trace"),
                              output / (name + ".fault.log"), target)
        injected = [e for e in events if "INJECTED" in e["result"]]
        expected_path = target["path"].replace(str(baseline), str(path), 1)
        assert len(injected) == 1, injected
        hit = injected[0]
        assert hit["during_commit"] and hit["path"] == expected_path, hit
        assert (hit["syscall"], hit["ordinal"]) == (target["syscall"], target["ordinal"]), hit
        assert hit["result"].startswith("-1 EIO "), hit
        committed = stage.startswith("reference")
        spool_dir = Path(str(path) + ".d") / "overflow"
        spools = list(spool_dir.iterdir())
        assert len(spools) == int(committed), spools
        assert all(p.suffix != ".tmp" for p in spools), spools
        size = sum(p.stat().st_size for p in fixture.rglob("*") if p.is_file())
        assert size <= 1024 * 1024, size
        # Syscall injection does not evict the page cache. Complete written references
        # replay atomically even though commit returned an uncertain failure.
        for cycle in range(2):
            db = open_db(path)
            try:
                verify(db, surface, committed)
                if surface == "sql":
                    verify_indexes(db, committed)
                assert db.wal_overflow_stats()["gc_failures"] == 0
                if cycle == 1:
                    assert list(spool_dir.iterdir()) == [], "spool remained after convergence"
            finally:
                db.close()
        if not committed:
            db = open_db(path)
            pending(db, surface).commit()
            db.close()
            db = open_db(path)
            verify(db, surface, True)
            if surface == "sql":
                verify_indexes(db, True)
            db.close()
        result.update(status="passed", fixture_bytes=size, syscall=hit["syscall"],
                      ordinal=hit["ordinal"], target_suffix=hit["path"].split("fault.alopex.d/", 1)[1],
                      replay="complete" if committed else "prior-only", reopen_cycles=2)
    except Exception:
        result["failure"] = traceback.format_exc()
    finally:
        shutil.rmtree(fixture)
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--case", choices=[f"{s}-{t}" for s in ("kv", "sql") for t in STAGES])
    parser.add_argument("--child", nargs=3, metavar=("PATH", "SURFACE", "STAGE"))
    args = parser.parse_args()
    if sys.platform != "linux" or sys.flags.optimize:
        parser.error("requires Linux Python with assertions enabled")
    if any(hasattr(_alopex, name) for name in ("_arm_wal_test_fault", "_arm_wal_test_reference_pause")):
        parser.error("requires the ordinary extension without debug hooks")
    if args.child:
        child(Path(args.child[0]), args.child[1], args.child[2])
        return
    if args.output is None or not shutil.which("strace"):
        parser.error("--output and strace with syscall injection support are required")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    results = [case(args.output, surface, stage) for surface in ("kv", "sql") for stage in STAGES
               if args.case is None or args.case == f"{surface}-{stage}"]
    passed = all(r["status"] == "passed" for r in results)
    report = dict(status="passed" if passed else "failed", cases=results,
                  native_sha256=hashlib.sha256(Path(_alopex.__file__).read_bytes()).hexdigest(),
                  source_sha256={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                 for name in (Path(__file__).name, "publication_python.py", "spool_os_write.py")},
                  strace=subprocess.check_output(["strace", "--version"], text=True).splitlines()[0],
                  python=sys.version, scope="Linux syscall-boundary EIO injection; not real device failure or power loss")
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
