"""Measure independent existing SQL operations through both public server APIs."""
from subject_identity import verify_subject, worker_command
from sql_resources import ENV as RESOURCE_ENV, OperationResources

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import resource
import statistics
import subprocess
import sys
import time

CASES = ("drop-sequence", "aggregate", "ordered", "select-all", "insert-rollback")
ROWS = 34250


def selected_cases(requested):
    return (requested,) if requested else tuple(case for case in CASES if case != "drop-sequence")


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def validate(case, result, rows):
    if case == "aggregate":
        expected = [{"count": (rows + 1) // 2}]
    elif case == "ordered":
        expected = [{"id": i, "value": float(i)} for i in range(min(rows, 16))]
    elif case == "select-all":
        result = sorted(result, key=lambda row: row["id"])
        expected = [{"id": i, "value": float(i)} for i in range(0, rows, 2)]
    elif case == "insert-rollback":
        expected = [{"id": 3}]
    else:
        expected = []
    assert result == expected, f"Incorrect complete result for {case}"
    for actual, wanted in zip(result, expected):
        assert all(type(actual[key]) is type(value) for key, value in wanted.items()), "Result type mismatch"
    return hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()


def run_process(command, output, name, metadata):
    started = time.monotonic()
    with (output / f"{name}.stdout.txt").open("w") as stdout, (output / f"{name}.stderr.txt").open("w") as stderr:
        try:
            process = subprocess.run(worker_command(command, metadata.get("engine") == "alopex-server"), stdout=stdout, stderr=stderr, timeout=300)
            exit_code = process.returncode
        except subprocess.TimeoutExpired:
            exit_code = 124
            stderr.write("Case process exceeded 300 second budget\n")
    status = dict(metadata, exit_code=exit_code, elapsed_seconds=time.monotonic() - started)
    save(output / f"{name}.status.json", status)
    return status


def relative(results, cases, engines=("alopex-server", "postgresql")):
    summary = []
    for case in cases:
        cells = [row for row in results if row["case"] == case]
        expected = {(engine, trial) for engine in engines for trial in range(3)}
        row = {"case": case, "comparison_status": "failed"}
        if (len(cells) == 6 and {(r["engine"], r["trial"]) for r in cells} == expected
                and all(r["exit_code"] == 0 and math.isfinite(r.get("latency_p50_ms", 0)) and r.get("latency_p50_ms", 0) > 0 for r in cells)
                and len({r["result_sha256"] for r in cells}) == 1):
            times = {engine: [next(r["latency_p50_ms"] for r in cells if r["engine"] == engine and r["trial"] == trial) for trial in range(3)] for engine in engines}
            ratio = statistics.median(times[engines[0]]) / statistics.median(times[engines[1]])
            paired = [a / b for a, b in zip(times[engines[0]], times[engines[1]])]
            row.update(comparison_status="valid", latency_ratio=ratio, throughput_ratio=1 / ratio,
                       paired_latency_ratio_range=[min(paired), max(paired)])
        summary.append(row)
    return summary


def measure(module, engine, case, raw_path):
    if engine == "alopex-server":
        import alopex
        connection = alopex.connect(os.environ["ALOPEX_PERF_SERVER_URL"], keep_alive=True)
        assert isinstance(connection, alopex.RemoteDatabase), "Embedded fallback forbidden"
        driver = "alopex"
    else:
        import psycopg
        from psycopg.rows import dict_row
        connection = psycopg.connect(os.environ["ALOPEX_PERF_POSTGRES_DSN"], autocommit=True, row_factory=dict_row, prepare_threshold=None)
        assert connection.execute("SHOW server_version").fetchone()["server_version"].startswith("16.14")
        driver = "postgresql"

    def execute(sql, target=None):
        target = connection if target is None else target
        if driver == "alopex":
            return target.execute_sql(sql) or []
        cursor = target.execute(sql)
        return cursor.fetchall() if cursor.description else []

    try:
        if case in ("aggregate", "ordered", "select-all"):
            execute("DROP TABLE IF EXISTS bench")
            module._populate_sql(connection, driver, ROWS)
            sql = {"aggregate": module.SQL_AGGREGATE_QUERY,
                   "ordered": "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16",
                   "select-all": module.SQL_STREAM_QUERY}[case]
            operation = lambda: execute(sql)
        elif case == "insert-rollback":
            execute("DROP TABLE IF EXISTS perf_items")
            execute("CREATE TABLE perf_items (id INTEGER PRIMARY KEY, value TEXT)")
            execute("INSERT INTO perf_items VALUES (1, 'old')")
            sql = "INSERT INTO perf_items VALUES (3, 'returning') RETURNING id"

            def operation():
                if driver == "alopex":
                    target = connection.begin(alopex.TxnMode.READ_WRITE)
                else:
                    connection.execute("BEGIN")
                    target = connection
                try:
                    return execute(sql, target)
                finally:
                    target.rollback()
        else:
            sql = "DROP SEQUENCE IF EXISTS perf_sequence"
            operation = lambda: execute(sql)
        checksum = validate(case, operation(), ROWS)
        for _ in range(3):
            validate(case, operation(), ROWS)
        observer = OperationResources.from_environment(
            engine, connection.info.backend_pid if driver == "postgresql" and os.environ.get(RESOURCE_ENV) else None)
        samples = []
        with raw_path.open("x") as raw:
            for index in range(21):
                resource_before = observer.begin() if observer else None
                started = time.perf_counter()
                result = operation()
                elapsed = time.perf_counter() - started
                row = {"sample": index, "seconds": elapsed}
                try:
                    if observer:
                        row["resources"] = observer.end(resource_before)
                except Exception as error:
                    row["resources"] = {"status": "failed", "error": str(error)}
                    raw.write(json.dumps(row) + "\n")
                    raw.flush()
                    raise
                raw.write(json.dumps(row) + "\n")
                raw.flush()
                assert validate(case, result, ROWS) == checksum
                samples.append(elapsed)
        if case == "insert-rollback":
            assert execute("SELECT id, value FROM perf_items ORDER BY id") == [{"id": 1, "value": "old"}], "Rollback changed fixture"
        return {"case": case, "engine": engine, "server_client": True, "sql": sql,
                "fixture_rows": 1 if case == "insert-rollback" else (0 if case == "drop-sequence" else ROWS),
                "warmups": 3, "samples": 21, "result_sha256": checksum,
                "latency_p50_ms": statistics.median(samples) * 1000,
                "client_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
    finally:
        connection.close()


def measure_diagnostic(module, case, mode, trial, raw_path):
    """Separate paired diagnostic, copied from the frozen measure boundary.

    Only aggregate/ordered are admitted. The SQL, fixture, oracle, warmups,
    operation timer, resource observation, and 21 raw samples retain their
    archived placement. Adapter callbacks are joined after operation().
    """
    from diagnostic import CellAdapter

    assert case in ("aggregate", "ordered") and mode in ("on", "off")
    import alopex
    adapter = CellAdapter(case, trial, mode, validate)
    options = {"keep_alive": True}
    if mode == "on":
        options["sql_timing_sink"] = adapter.sink
    connection = alopex.connect(os.environ["ALOPEX_PERF_SERVER_URL"], **options)
    assert isinstance(connection, alopex.RemoteDatabase), "Embedded fallback forbidden"

    class SetupConnection:
        def execute_sql(self, sql):
            return adapter.setup(lambda: connection.execute_sql(sql))

    try:
        setup = SetupConnection()
        setup.execute_sql("DROP TABLE IF EXISTS bench")
        module._populate_sql(setup, "alopex", ROWS)
        sql = (module.SQL_AGGREGATE_QUERY if case == "aggregate" else
               "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16")
        operation = lambda: connection.execute_sql(sql) or []
        adapter.call("precheck", operation)
        for index in range(3):
            adapter.call("warmup", operation, sample=index)
        observer = OperationResources.from_environment("alopex-server", None)
        samples = []
        with raw_path.open("x") as raw:
            for index in range(21):
                resource_before = observer.begin() if observer else None
                result, elapsed = adapter.call("measured", operation, sample=index,
                                               measure=True, defer_oracle=True)
                row = {"sample": index, "seconds": elapsed}
                try:
                    if observer:
                        row["resources"] = observer.end(resource_before)
                except Exception:
                    row["resources"] = {"status": "failed", "error_code": "resource_capture_failed"}
                    raw.write(json.dumps(row) + "\n")
                    raw.flush()
                    raise
                raw.write(json.dumps(row) + "\n")
                raw.flush()
                assert adapter.finalize_result(result) == adapter.expected_checksum
                samples.append(elapsed)
        adapter.finish()
        result = {"case": case, "engine": "alopex-server", "server_client": True,
                  "fixture_rows": ROWS, "warmups": 3, "samples": 21,
                  "result_sha256": adapter.expected_checksum,
                  "latency_p50_ms": statistics.median(samples) * 1000,
                  "client_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
        return result, adapter.calls
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--engine", choices=("alopex-server", "postgresql"))
    parser.add_argument("--case", choices=CASES)
    args = parser.parse_args()
    subject = verify_subject(require_server=True)
    os.environ.update({key: "1" for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS")})
    from subject_identity import cpu_assignment
    allocation = cpu_assignment(subject)
    os.sched_setaffinity(0, set(allocation["client"]))
    assert os.sched_getaffinity(0) == set(allocation["client"]), "Client CPU affinity differs from manifest"
    root, output = args.harness_root.resolve(), args.output.resolve()
    sys.path.insert(0, str(root / "scripts/performance"))
    import parity_v0811_measure as module
    if args.engine:
        if not args.case:
            parser.error("worker requires --case")
        print(json.dumps(measure(module, args.engine, args.case, Path(os.environ["ALOPEX_PERF_RAW_PATH"])), allow_nan=False))
        return
    output.mkdir(parents=True, exist_ok=True)
    if (output / "environment.json").exists() or any(output.glob("case-*")):
        raise RuntimeError("Use a new run directory; partial runs must remain immutable")
    save(output / "environment.json", {"subject_identity": subject, "python": platform.python_version(), "platform": platform.platform(),
         "versions": {key: importlib.metadata.version(key) for key in ("alopex", "psycopg", "psycopg-binary")},
         "client_cpu_affinity": sorted(os.sched_getaffinity(0)), "server_cpu_affinity": allocation["server"],
         "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
         "harness_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
         "runner_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], text=True).strip(),
         "harness_sha256": hashlib.sha256((root / "scripts/performance/parity_v0811_measure.py").read_bytes()).hexdigest(),
         "resource_observer_sha256": hashlib.sha256(Path(__file__).with_name("sql_resources.py").read_bytes()).hexdigest(),
         "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    results = []
    cases = selected_cases(args.case)
    for case in cases:
        for trial in range(3):
            engines = ("alopex-server", "postgresql") if trial % 2 == 0 else ("postgresql", "alopex-server")
            for engine in engines:
                name = f"case-{case}-{engine}-trial{trial}"
                os.environ["ALOPEX_PERF_RAW_PATH"] = str(output / f"{name}.raw.ndjson")
                command = [sys.executable, "-B", str(Path(__file__).resolve()), "--harness-root", str(root),
                           "--output", str(output), "--engine", engine, "--case", case]
                status = run_process(command, output, name, {"case": case, "engine": engine, "trial": trial})
                print(json.dumps(status), flush=True)
                row = dict(status)
                if status["exit_code"] == 0:
                    row.update(json.loads((output / f"{name}.stdout.txt").read_text()))
                    save(output / f"{name}.json", row)
                results.append(row)
    save(output / "relative.json", {"schema": "alopex.isolated-server-sql/v1", "reference": "PostgreSQL 16.14 = 1",
         "subject": f"AlopexDB server {subject['version']} via RemoteDatabase", "cases": relative(results, cases),
         "default_skips": {"drop-sequence": "Known server defect, alopex-db/alopex#720; explicit --case required"}})


if __name__ == "__main__":
    main()
