#!/usr/bin/env python3
"""Manual tiny Python KV/SQL overflow comparison, excluded from CI.

Each writer exits without destructors; two fresh readers verify full contents
and the SQL index. Timings are observations, not an automatic acceptance gate.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time

import storage

OPTIONS = dict(payload_bytes=65536, spool_bytes=131072, directory_bytes=524288,
               replay_bytes=131072, key_bytes=1024, value_bytes=8192,
               fragment_bytes=256, wal_segment_bytes=4124, wal_segments=1,
               inline_bytes=1024, activation_bytes=8192, container_fence_bytes=32768)
SHAPES = (("kv", 16), ("kv", 2048), ("sql", 16), ("sql", 2048))
METRICS = ("prepare_ns", "open_ns", "schema_ns", "index_ns", "prior_ns", "begin_ns",
           "stage_ns", "commit_ns", "peak_rss_bytes", "wal_bytes", "spool_bytes",
           "fixture_bytes", "allocated_bytes")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def measure(action):
    start = time.perf_counter_ns()
    result = action()
    return result, time.perf_counter_ns() - start


def sizes(directory):
    paths = [directory, *directory.rglob("*")]
    require(not any(p.is_symlink() for p in paths), "unexpected fixture symlink")
    result = {"wal_bytes": 0, "spool_bytes": 0, "fixture_bytes": 0, "allocated_bytes": 0}
    for path in paths:
        info = path.stat()
        result["allocated_bytes"] += info.st_blocks * 512
        if path.is_file():
            result["fixture_bytes"] += info.st_size
            if path.name == "lsm.wal":
                result["wal_bytes"] += info.st_size
            if "overflow" in path.relative_to(directory).parts:
                result["spool_bytes"] += info.st_size
    require(result["fixture_bytes"] <= 1_048_576, "tiny fixture exceeded 1 MiB")
    return result


def child(phase, directory, surface, size, expected_native):
    # Import only in the isolated process, from that source's own Python package.
    from alopex import Database, TxnMode, WalOverflowOptions, _alopex
    require(digest(_alopex.__file__) == expected_native, "wrong native extension")
    require(not hasattr(_alopex, "_arm_wal_test_fault"), "ordinary extension required")
    path = directory / "db.alopex"
    options = WalOverflowOptions(**OPTIONS)
    record = {"phase": phase, "surface": surface, "value_bytes": size,
              "native_sha256": expected_native, "status": "passed"}
    if phase == "write":
        start = time.perf_counter_ns()
        values = (b"x" * size, b"y" * size)
        sql = "INSERT INTO items VALUES (2, 'a', '" + "x" * size + "'), (3, 'b', '" + "y" * size + "')"
        record["prepare_ns"] = time.perf_counter_ns() - start
        db, record["open_ns"] = measure(lambda: Database.open(str(path), wal_overflow=options))
        record.update(schema_ns=0, index_ns=0)
        if surface == "sql":
            _, record["schema_ns"] = measure(lambda: db.execute_sql(
                "CREATE TABLE items (id INT PRIMARY KEY, code TEXT, value TEXT)"))
            _, record["index_ns"] = measure(lambda: db.execute_sql("CREATE INDEX items_code ON items(code)"))
        start = time.perf_counter_ns()
        tx = db.begin(TxnMode.READ_WRITE)
        if surface == "kv":
            tx.put(b"old", b"ok")
        else:
            tx.execute_sql("INSERT INTO items VALUES (1, 'old', 'ok')")
        tx.commit()
        record["prior_ns"] = time.perf_counter_ns() - start
        tx, record["begin_ns"] = measure(lambda: db.begin(TxnMode.READ_WRITE))
        start = time.perf_counter_ns()
        if surface == "kv":
            tx.put(b"a", values[0])
            tx.put(b"b", values[1])
        else:
            tx.execute_sql(sql)
        record["stage_ns"] = time.perf_counter_ns() - start
        _, record["commit_ns"] = measure(tx.commit)
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        record["peak_rss_bytes"] = peak if sys.platform == "darwin" else peak * 1024
        require(record["peak_rss_bytes"] <= 1_073_741_824, "writer exceeded 1 GiB RSS bound")
        record.update(sizes(directory))
        record["overflow_stats"] = db.wal_overflow_stats()
        print(json.dumps(record), flush=True)
        os._exit(0)  # Do not include destructor/checkpoint work in commit or erase live WAL.
    db, record["open_ns"] = measure(lambda: Database.open(str(path), wal_overflow=options))
    try:
        if surface == "kv":
            tx = db.begin(TxnMode.READ_ONLY)
            require(tx.get(b"old") == b"ok" and tx.get(b"a") == b"x" * size
                    and tx.get(b"b") == b"y" * size, "recovered KV differs")
            tx.rollback()
        else:
            expected = [{"id": 1, "code": "old", "value": "ok"},
                        {"id": 2, "code": "a", "value": "x" * size},
                        {"id": 3, "code": "b", "value": "y" * size}]
            require(db.execute_sql("SELECT id, code, value FROM items ORDER BY id") == expected,
                    "recovered SQL rows differ")
            require(db.execute_sql("SELECT id, code, value FROM items WHERE code = 'a'") == expected[1:2],
                    "recovered indexed query differs")
            require(db.execute_sql("SELECT id, code, value FROM items WHERE code = 'absent'") == [],
                    "indexed miss differs")
        require(db.wal_overflow_stats()["gc_failures"] == 0, "overflow GC failed")
    finally:
        db.close()
    record.update(sizes(directory))
    record["verified"] = True
    print(json.dumps(record), flush=True)


def summarize(rows):
    expected = {(engine, surface, size, round_id) for engine in ("before", "after")
                for surface, size in SHAPES for round_id in range(6)}
    identities = [(r["engine"], r["surface"], r["value_bytes"], r["round"]) for r in rows]
    require(len(identities) == len(expected) and set(identities) == expected, "incomplete/duplicate matrix")
    require(all(r["status"] == "passed" and r["reopen_cycles"] == 2 for r in rows),
            "failed or unverified measurement")
    medians = {}
    for engine in ("before", "after"):
        for surface, size in SHAPES:
            cell = [r for r in rows if r["engine"] == engine and r["surface"] == surface
                    and r["value_bytes"] == size and r["round"] > 0]
            key = f"{engine}-{surface}-{size}"
            medians[key] = {metric: statistics.median(r[metric] for r in cell) for metric in METRICS}
            center = medians[key]["commit_ns"]
            medians[key]["commit_mad_ns"] = statistics.median(abs(r["commit_ns"] - center) for r in cell)
            medians[key]["reopen_ns"] = statistics.median(n for r in cell for n in r["reopen_ns"])
    ratios = {f"{surface}-{size}": medians[f"after-{surface}-{size}"]["commit_ns"] /
              medians[f"before-{surface}-{size}"]["commit_ns"] for surface, size in SHAPES}
    return {"status": "measured", "medians": medians, "commit_after_before_ratios": ratios,
            "investigate": [k for k, v in ratios.items() if v > 1.20],
            "writer_processes": 48, "reader_processes": 96,
            "scope": "tiny Python KV/SQL observations; not throughput or release performance qualification"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", choices=("write", "verify"))
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--surface", choices=("kv", "sql"))
    parser.add_argument("--size", type=int, choices=(16, 2048))
    parser.add_argument("--native-sha")
    parser.add_argument("--before-package", type=Path)
    parser.add_argument("--after-package", type=Path)
    parser.add_argument("--before-sha")
    parser.add_argument("--after-sha")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if os.environ.get("CI") or sys.flags.optimize or sys.platform not in ("darwin", "linux"):
        parser.error("manual macOS/Linux only, outside CI, assertions enabled")
    if args.child:
        require(args.fixture is not None and args.surface and args.size and args.native_sha,
                "missing child identity/shape")
        child(args.child, args.fixture, args.surface, args.size, args.native_sha)
        return
    require(args.output is not None and args.before_package and args.after_package
            and args.before_sha and args.after_sha, "both source packages, SHAs and output are required")
    packages = {name: getattr(args, name + "_package").resolve() for name in ("before", "after")}
    hashes = {}
    builds = {}
    for name, package in packages.items():
        native, = package.glob("alopex/_alopex*.so")
        hashes[name] = digest(native)
        builds[name] = json.loads((package / "build-identity.json").read_text())
        require(builds[name]["source_sha"] == getattr(args, name + "_sha")
                and builds[name]["native_sha256"] == hashes[name], "package build identity mismatch")
    require(all(builds["before"][key] == builds["after"][key]
                for key in ("profile", "python", "parser_sha256", "rustc")),
            "before/after toolchain, profile or parser differs")
    require(hashes["before"] != hashes["after"], "before/after binaries must be distinct")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    initial = storage.check(output / "storage.jsonl", [output])["allocated_bytes"]
    require(initial + 200_000_000 < storage.STOP_BYTES, "insufficient measured storage headroom")
    plan = {"before_sha": args.before_sha, "after_sha": args.after_sha, "native_sha256": hashes,
            "builds": builds,
            "runner_sha256": digest(__file__), "options": OPTIONS, "shapes": SHAPES,
            "smoke": args.smoke, "warmups": 1, "samples": 5, "prior": 1, "pending": 2,
            "cache": "fresh DB/process; warm OS cache; no cache eviction",
            "durability": "public overflow options use EveryWrite; default public checkpoint/converge",
            "peak_scope": "writer process lifetime high water through commit, includes import/preparation",
            "criteria": "retain all samples, no outlier removal; commit ratio>1.20 requires investigation",
            "limits": "each child30s, writer RSS1GiB, fixture1MiB, job growth200MB, host stop40GB",
            "cleanup": "each fixture after write+two verified reopens; raw receipts retained"}
    (output / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    rows = []
    for round_id in range(1 if args.smoke else 6):
        for surface, size in SHAPES:
            for engine in (("before", "after") if round_id % 2 == 0 else ("after", "before")):
                name = f"{round_id}-{surface}-{size}-{engine}"
                fixture = output / name
                fixture.mkdir()
                record = {"engine": engine, "round": round_id, "surface": surface,
                          "value_bytes": size, "status": "failed", "reopen_cycles": 0}
                try:
                    env = dict(os.environ, PYTHONPATH=str(packages[engine]), PYTHONDONTWRITEBYTECODE="1",
                               RAYON_NUM_THREADS="2", OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="1")
                    for index, phase in enumerate(("write", "verify", "verify")):
                        run = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--child", phase,
                            "--fixture", str(fixture), "--surface", surface, "--size", str(size),
                            "--native-sha", hashes[engine]], env=env, capture_output=True, text=True, timeout=30)
                        (output / f"{name}-{index}-{phase}.log").write_text(run.stdout + run.stderr)
                        require(run.returncode == 0, f"{name}/{phase} failed; see saved log")
                        data = json.loads(run.stdout)
                        require(data["native_sha256"] == hashes[engine] and data["phase"] == phase
                                and data["surface"] == surface and data["value_bytes"] == size
                                and data["status"] == "passed", "child receipt mismatch")
                        if phase == "write":
                            record.update(data, status="failed", reopen_ns=[])
                        else:
                            require(data["verified"], "reopen verification missing")
                            record["reopen_ns"].append(data["open_ns"])
                            record["reopen_cycles"] += 1
                    record["status"] = "passed"
                except BaseException as error:
                    record["failure"] = repr(error)
                    raise
                finally:
                    with (output / "rows.jsonl").open("a") as stream:
                        stream.write(json.dumps(record) + "\n")
                    shutil.rmtree(fixture)
                storage.check(output / "storage.jsonl", [output], limit=initial + 200_000_000)
                rows.append(record)
    report = {"status": "smoke-passed", "cases": len(rows)} if args.smoke else summarize(rows)
    report["plan"] = plan
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "plan"}), flush=True)


if __name__ == "__main__":
    main()
