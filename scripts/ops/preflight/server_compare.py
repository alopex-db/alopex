"""Repeat the existing PostgreSQL case with two real server/client APIs."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def require_new_run(output):
    if (output / "environment.json").exists() or any(output.glob("measure-*")) or any(output.glob("preflight-*")):
        raise RuntimeError("Use a new run directory; partial and completed attempts must remain immutable")


def connect(engine):
    if engine == "alopex-server":
        import alopex
        connection = alopex.connect(os.environ["ALOPEX_PERF_SERVER_URL"], keep_alive=True)
        if not isinstance(connection, alopex.RemoteDatabase):
            raise RuntimeError("Server comparison must not use an embedded Database")
        return connection, "alopex"
    import psycopg
    return psycopg.connect(os.environ["ALOPEX_PERF_POSTGRES_DSN"], autocommit=True), "postgresql"


def preflight(module, engine):
    connection, driver = connect(engine)
    checks = []

    def rows(target, sql):
        result = module._execute(target, driver, sql)
        return [tuple(row.values()) if isinstance(row, dict) else tuple(row) for row in result]

    def transaction():
        if driver == "alopex":
            import alopex
            return connection.begin(alopex.TxnMode.READ_WRITE)
        connection.execute("BEGIN")
        return connection

    try:
        module._execute(connection, driver, "DROP TABLE IF EXISTS bench")
        module._populate_sql(connection, driver, 20)
        module._prepare_sql_workload(connection, driver, "sql-postgresql-curated")
        target = transaction()
        try:
            assert rows(target, "INSERT INTO perf_items VALUES (3, 'returning') RETURNING id") == [(3,)]
            checks.append("insert_returning")
            assert rows(target, "INSERT INTO perf_items VALUES (1, 'conflict') ON CONFLICT (id) DO UPDATE SET value = 'updated' RETURNING value") == [("updated",)]
            checks.append("upsert_returning")
        finally:
            target.rollback()
        target = transaction()
        try:
            module._execute(target, driver, "MERGE INTO perf_items USING perf_source ON perf_items.id = perf_source.id WHEN MATCHED THEN UPDATE SET value = perf_source.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (perf_source.id, perf_source.value)")
            assert rows(target, "SELECT id, value FROM perf_items ORDER BY id") == [(1, "new"), (2, "second")]
            checks.append("merge_results")
            for sql in ("CREATE TABLE perf_schema (id INTEGER)", "ALTER TABLE perf_schema ADD COLUMN value TEXT", "TRUNCATE TABLE perf_schema", "CREATE VIEW perf_view AS SELECT id, value FROM perf_schema"):
                module._execute(target, driver, sql)
        finally:
            target.rollback()
        assert rows(connection, "SELECT table_name FROM information_schema.tables WHERE table_name = 'perf_schema'") == []
        checks.append("ddl_rollback")
        assert rows(connection, "SELECT nextval('perf_sequence')") == [(1,)]
        assert rows(connection, "SELECT nextval('perf_sequence')") == [(2,)]
        checks.append("sequence_advances")
        module._sql_workload(connection, driver, "sql-postgresql-curated", 20)
        checksum = module._validate_sql_state(connection, driver, "sql-postgresql-curated", 20)
        checks.append("complete_existing_workload_and_rollback_state")
        return {"engine": engine, "server_client": True, "checks": checks, "result_sha256": checksum}
    finally:
        connection.close()


def worker(module, engine, preflight_only):
    if preflight_only:
        return preflight(module, engine)
    if engine == "alopex-server":
        connection, driver = connect(engine)
        connection.execute_sql("DROP TABLE IF EXISTS bench")
        result = module._sql(driver, "sql-postgresql-curated", 34250, 3, 21, connection=connection)
    else:
        result = module._sql("postgresql", "sql-postgresql-curated", 34250, 3, 21)
    result.update({"engine": engine, "server_client": True, "rows": 34250, "warmups": 3, "samples": 21})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--engine", choices=["alopex-server", "postgresql"])
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    os.environ.update({key: "1" for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS")})
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.sched_setaffinity(0, {1})
    root, output = args.harness_root.resolve(), args.output.resolve()
    sys.path.insert(0, str(root / "scripts/performance"))
    import parity_v0811_measure as module
    if args.engine:
        print(json.dumps(worker(module, args.engine, args.preflight), allow_nan=False))
        return
    output.mkdir(parents=True, exist_ok=True)
    require_new_run(output)
    save(output / "environment.json", {
        "python": platform.python_version(), "platform": platform.platform(),
        "client_cpu_affinity": sorted(os.sched_getaffinity(0)), "server_cpu_affinity": [0],
        "cpu_model": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),
        "versions": {name: importlib.metadata.version(name) for name in ("alopex", "psycopg", "psycopg-binary")},
        "harness_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "harness_sha256": hashlib.sha256((root / "scripts/performance/parity_v0811_measure.py").read_bytes()).hexdigest(),
    })
    for phase in ("preflight", "measure"):
        for trial in range(1 if phase == "preflight" else 3):
            engines = ("alopex-server", "postgresql") if trial % 2 == 0 else ("postgresql", "alopex-server")
            checksums = []
            for engine in engines:
                name = f"{phase}-{engine}-trial{trial}"
                path = output / f"{name}.json"
                status_path = output / f"{name}.status.json"
                if status_path.exists():
                    raise RuntimeError("Use a new run directory; previous attempts must remain immutable")
                command = [sys.executable, "-B", str(Path(__file__).resolve()), "--harness-root", str(root), "--output", str(output), "--engine", engine]
                if phase == "preflight":
                    command.append("--preflight")
                env = dict(os.environ, ALOPEX_PERF_RAW_PATH=str(output / f"{name}.raw.ndjson"))
                started = time.monotonic()
                with (output / f"{name}.stdout.txt").open("w") as stdout, (output / f"{name}.stderr.txt").open("w") as stderr:
                    completed = subprocess.run(command, env=env, cwd=root, stdout=stdout, stderr=stderr)
                status = {"phase": phase, "engine": engine, "trial": trial, "exit_code": completed.returncode, "elapsed_seconds": time.monotonic() - started}
                save(status_path, status)
                print(json.dumps(status), flush=True)
                if completed.returncode:
                    raise RuntimeError(f"Failed {name}; raw and failure status retained")
                value = json.loads((output / f"{name}.stdout.txt").read_text())
                if not value["server_client"]:
                    raise RuntimeError("An embedded measurement is not a server/client result")
                save(path, value)
                checksums.append(value["result_sha256"])
            if len(set(checksums)) != 1:
                raise RuntimeError("Server/client results differ between products")
    times = {}
    for engine in ("alopex-server", "postgresql"):
        times[engine] = [json.loads((output / f"measure-{engine}-trial{trial}.json").read_text())["latency_p50_ms"] for trial in range(3)]
    ratio = statistics.median(times["alopex-server"]) / statistics.median(times["postgresql"])
    paired = [a / b for a, b in zip(times["alopex-server"], times["postgresql"])]
    save(output / "relative.json", {"schema": "alopex.server-client-comparison/v1", "comparison_status": "valid", "subject": "alopex-server via RemoteDatabase", "reference": "PostgreSQL via psycopg", "case": "sql-postgresql-curated", "latency_ratio": ratio, "throughput_ratio": 1 / ratio, "paired_latency_ratio_range": [min(paired), max(paired)], "previous_embedded_comparison": "invalid; retained separately"})


if __name__ == "__main__":
    main()
