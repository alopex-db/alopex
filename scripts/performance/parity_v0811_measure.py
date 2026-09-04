"""Produce paired Polars/SQL measurements for the v0.8.11 parity gate."""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from .parity_v0811_gate import percentile
except ImportError:  # Direct script execution in the performance workflow.
    from parity_v0811_gate import percentile


SQL_FIXTURE_PATH = Path(__file__).parent / "fixtures/sql-v0.8.11-curated.json"
SQL_FIXTURE = json.loads(SQL_FIXTURE_PATH.read_text(encoding="utf-8"))
SQL_AGGREGATE_QUERY = SQL_FIXTURE["queries"]["aggregate"]
SQL_STREAM_QUERY = SQL_FIXTURE["queries"]["stream"]
POLARS_EAGER_CLAIMS = [
    "DataFrame.__init__", "DataFrame.explode", "DataFrame.height", "DataFrame.lazy",
    "DataFrame.to_dict", "DataFrame.width", "Expr.add", "Expr.alias", "Expr.and_",
    "Expr.eq", "Expr.ge", "Expr.gt", "Expr.le", "Expr.lt", "Expr.mul", "Expr.not_",
    "Expr.or_", "Expr.sub", "LazyFrame.filter", "LazyFrame.select",
    "LazyFrame.with_columns", "Series.name", "Series.to_list", "col", "concat",
    "concat_str", "lit",
]
POLARS_STREAMING_CLAIMS = ["LazyFrame.collect", "LazyFrame.collect_batches"]
SQL_SQLITE_CLAIMS = [
    "portable SELECT/null/order/coercion", "parser/lexer", "transactions/savepoints",
    "prepared statements", "CHECK/FK constraints",
]
SQL_POSTGRESQL_CLAIMS = [
    "EXPLAIN JSON", "RETURNING", "ON CONFLICT", "SERIAL/IDENTITY/SEQUENCE",
    "VIEW/ALTER TABLE/TRUNCATE", "MERGE/UPDATE FROM/DELETE USING",
    "information_schema",
]
SQL_STREAMING_CLAIMS = ["streaming query"]
WORKLOAD_COVERAGE = {
    "polars-eager-api": POLARS_EAGER_CLAIMS,
    "polars-csv-streaming": POLARS_STREAMING_CLAIMS,
    "polars-parquet-streaming": POLARS_STREAMING_CLAIMS,
    "sql-sqlite-curated": SQL_SQLITE_CLAIMS,
    "sql-postgresql-curated": SQL_POSTGRESQL_CLAIMS,
    "sql-streaming": SQL_STREAMING_CLAIMS,
}


def summarize_latencies(samples: list[float], rows: int) -> dict[str, float]:
    median = percentile(samples, 0.50)
    return {
        "latency_p50_ms": median * 1000,
        "latency_p95_ms": percentile(samples, 0.95) * 1000,
        "rows_per_second": rows / median,
    }


def normalize_hnsw(payload: dict[str, object], checksum: str) -> dict[str, object]:
    summaries = {
        (row["engine"], row["ef_search"]): row for row in payload.get("summary", [])
    }
    builds = {row["engine"]: row for row in payload.get("builds", [])}

    def metrics(engine: str) -> dict[str, float]:
        search = summaries[(engine, 64)]
        build = builds[engine]
        return {
            "recall_at_10": float(search["median_tie_aware_recall_at_10"]),
            "build_latency_ms": float(build["build_time_seconds"]) * 1000,
            "query_latency_p50_ms": float(search["median_query_latency_p50_us"]) / 1000,
            "query_latency_p95_ms": float(search["median_query_latency_p95_us"]) / 1000,
            "query_latency_p99_ms": float(search["median_query_latency_p99_us"]) / 1000,
            "queries_per_second": float(search["median_queries_per_second"]),
            "index_size_bytes": float(build["index_size_bytes"]),
            "peak_rss_bytes": float(build["peak_rss_bytes"]),
            "update_latency_ms": float(build["update_latency_ms"]),
            "delete_latency_ms": float(build["delete_latency_ms"]),
            "reopen_latency_ms": float(build["reopen_latency_ms"]),
        }

    return {
        "contract": "hnsw-pareto-v1",
        "evidence_id": "hnsw-pareto",
        "reference_revision": "nmslib/hnswlib@3f3429661187e4c24a490a0f148fc6bc89042b3d;facebookresearch/faiss@20f14b31a6d54e243a3d1de6ae193fc4c3ec18ed",
        "dataset_sha256": checksum,
        "subject": metrics("alopex-hnsw"),
        "reference": metrics("hnswlib"),
    }


def _rss_bytes() -> float:
    return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)


def _io_bytes() -> float:
    try:
        values = {
            key: int(value)
            for key, value in (
                line.split(":", 1)
                for line in Path("/proc/self/io").read_text(encoding="utf-8").splitlines()
            )
        }
        return float(values.get("read_bytes", 0) + values.get("write_bytes", 0))
    except OSError:
        return 0.0


def _environment() -> dict[str, object]:
    cpu_model = next(
        (
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines()
            if line.startswith("model name")
        ),
        platform.processor(),
    )
    memory_bytes = next(
        int(line.split()[1]) * 1024
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines()
        if line.startswith("MemTotal:")
    )
    revision = os.environ.get("GITHUB_SHA")
    if not revision:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    source_files = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    source_hash = hashlib.sha256()
    for relative in sorted(path for path in source_files if path):
        source_hash.update(relative)
        source_hash.update(b"\0")
        source_hash.update(Path(os.fsdecode(relative)).read_bytes())
        source_hash.update(b"\0")
    return {
        "alopex_version": importlib.metadata.version("alopex"),
        "alopex_revision": revision,
        "alopex_tree_sha256": source_hash.hexdigest(),
        "os": "ubuntu-24.04",
        "kernel": platform.release(),
        "cpu_model": cpu_model,
        "logical_cpu_count": os.cpu_count(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "memory_bytes": memory_bytes,
        "build_profile": "release",
        "thread_count": 1,
        "python_version": platform.python_version(),
    }


def _samples(operation, warmups: int, runs: int) -> list[float]:
    for _ in range(warmups):
        operation()
    durations = []
    for _ in range(runs):
        started = time.perf_counter()
        operation()
        durations.append(time.perf_counter() - started)
    return durations


def _eager(engine: str, rows: int, warmups: int, runs: int) -> dict[str, float]:
    columns = {
        "id": list(range(rows)),
        "label": [None if index % 10 == 0 else f"row-{index}" for index in range(rows)],
    }
    if engine == "alopex":
        from alopex import DataFrame, col, concat, concat_str, lit

        def operation():
            frame = DataFrame(
                {**columns, "items": [[str(index), str(index + 1)] for index in range(rows)]}
            )
            series = frame.to_dict()
            result = (
                frame.lazy()
                .filter(col("id").ge(lit(0)))
                .with_columns(col("id").add(lit(1)).alias("next_id"))
                .select(
                    col("id").add(1).alias("add"),
                    col("id").sub(1).alias("sub"),
                    col("id").mul(2).alias("mul"),
                    col("id").eq(0).alias("eq"),
                    col("id").ge(0).alias("ge"),
                    col("id").gt(-1).alias("gt"),
                    col("id").le(rows).alias("le"),
                    col("id").lt(rows + 1).alias("lt"),
                    col("id").ge(0).and_(col("id").lt(rows)).alias("and"),
                    col("id").lt(0).or_(col("id").ge(0)).alias("or"),
                    col("id").lt(0).not_().alias("not"),
                    concat_str("label", lit("!"), separator="").alias("joined"),
                )
                .collect()
            )
            return (
                frame.height,
                frame.width,
                frame.explode("items").height,
                next(iter(series.values())).name,
                next(iter(series.values())).to_list(),
                concat([frame]).height,
                result.to_dict(as_series=False),
            )

    else:
        import polars as pl

        if pl.__version__ != "1.43.2":
            raise RuntimeError(f"expected Polars 1.43.2, got {pl.__version__}")

        def operation():
            frame = pl.DataFrame(
                {**columns, "items": [[str(index), str(index + 1)] for index in range(rows)]}
            )
            series = frame.to_dict()
            result = (
                frame.lazy()
                .filter(pl.col("id") >= 0)
                .with_columns((pl.col("id") + 1).alias("next_id"))
                .select(
                    pl.col("id").add(1).alias("add"),
                    pl.col("id").sub(1).alias("sub"),
                    pl.col("id").mul(2).alias("mul"),
                    pl.col("id").eq(0).alias("eq"),
                    pl.col("id").ge(0).alias("ge"),
                    pl.col("id").gt(-1).alias("gt"),
                    pl.col("id").le(rows).alias("le"),
                    pl.col("id").lt(rows + 1).alias("lt"),
                    pl.col("id").ge(0).and_(pl.col("id").lt(rows)).alias("and"),
                    pl.col("id").lt(0).or_(pl.col("id").ge(0)).alias("or"),
                    pl.col("id").lt(0).not_().alias("not"),
                    pl.concat_str("label", pl.lit("!"), separator="").alias("joined"),
                )
                .collect()
            )
            return (
                frame.height,
                frame.width,
                frame.explode("items").height,
                next(iter(series.values())).name,
                next(iter(series.values())).to_list(),
                pl.concat([frame]).height,
                result.to_dict(as_series=False),
            )

    metrics = summarize_latencies(_samples(operation, warmups, runs), rows)
    metrics["peak_rss_bytes"] = _rss_bytes()
    return metrics


def _streaming(
    engine: str, fixture: Path, rows: int, warmups: int, runs: int
) -> dict[str, float]:
    parquet = fixture.suffix == ".parquet"
    if engine == "alopex":
        from alopex import LazyFrame

        scan = LazyFrame.scan_parquet if parquet else LazyFrame.scan_csv
        build = lambda: scan(str(fixture))
        materialize = lambda: build().collect().height

        def consume():
            started = time.perf_counter()
            with build().collect_batches(chunk_size=1024) as stream:
                first = next(stream)
                first_elapsed = time.perf_counter() - started
                count = first.height + sum(batch.height for batch in stream)
                return first_elapsed, time.perf_counter() - started, count, first.height

    else:
        import polars as pl

        if pl.__version__ != "1.43.2":
            raise RuntimeError(f"expected Polars 1.43.2, got {pl.__version__}")

        scan = pl.scan_parquet if parquet else pl.scan_csv
        build = lambda: scan(fixture)
        materialize = lambda: build().collect().height

        def consume():
            started = time.perf_counter()
            batches = iter(build().collect_batches(chunk_size=8192))
            first = next(batches)
            first_elapsed = time.perf_counter() - started
            count = first.height + sum(batch.height for batch in batches)
            return first_elapsed, time.perf_counter() - started, count, first.height

    plan = _samples(build, warmups, runs)
    collect_samples = _samples(materialize, warmups, runs)
    for _ in range(warmups):
        consume()
    first_samples = []
    total_samples = []
    steady_samples = []
    for _ in range(runs):
        first, total, count, first_rows = consume()
        if count != rows:
            raise RuntimeError(f"{engine} streaming returned {count} rows, expected {rows}")
        first_samples.append(first)
        total_samples.append(total)
        steady_samples.append((count - first_rows) / max(total - first, 1e-9))
    median_total = statistics.median(total_samples)
    return {
        "plan_build_p50_ms": statistics.median(plan) * 1000,
        "collect_p50_ms": statistics.median(collect_samples) * 1000,
        "time_to_first_batch_p50_ms": statistics.median(first_samples) * 1000,
        "total_p50_ms": median_total * 1000,
        "steady_state_rows_per_second": statistics.median(steady_samples),
        "peak_rss_bytes": _rss_bytes(),
    }


def _populate_sql(connection, engine: str, rows: int) -> None:
    if engine == "alopex":
        connection.execute_sql(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value DOUBLE, enabled BOOLEAN)"
        )
    elif engine == "sqlite":
        connection.execute(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value REAL, enabled INTEGER)"
        )
    else:
        connection.execute("DROP TABLE IF EXISTS bench")
        connection.execute(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value DOUBLE PRECISION, enabled BOOLEAN)"
        )
    for start in range(0, rows, 100):
        values = ",".join(
            f"({index},{float(index)},"
            f"{int(index % 2 == 0) if engine == 'sqlite' else str(index % 2 == 0).lower()})"
            for index in range(start, min(start + 100, rows))
        )
        sql = f"INSERT INTO bench VALUES {values}"
        connection.execute_sql(sql) if engine == "alopex" else connection.execute(sql)
    if engine != "alopex":
        connection.commit()


def _sql_script(connection, engine: str, sql: str) -> None:
    if engine == "alopex":
        connection.execute_sql(sql)
    elif engine == "sqlite":
        connection.executescript(sql)
    else:
        connection.execute(sql)


def _sql_transaction(connection, engine: str, statements: list[str]) -> None:
    if engine == "alopex":
        from alopex import TxnMode

        transaction = connection.begin(TxnMode.READ_WRITE)
        try:
            for statement in statements:
                transaction.execute_sql(statement)
        finally:
            transaction.rollback()
        return
    _sql_script(connection, engine, f"BEGIN; {'; '.join(statements)}; ROLLBACK")


def _prepare_sql_workload(connection, engine: str, workload: str) -> None:
    if workload == "sql-sqlite-curated":
        statements = (
            "CREATE TABLE perf_parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE perf_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES perf_parent(id), quantity INTEGER CHECK (quantity > 0))",
            "INSERT INTO perf_parent VALUES (1)",
        )
    else:
        statements = (
            "DROP TABLE IF EXISTS perf_source",
            "DROP TABLE IF EXISTS perf_items",
            "DROP SEQUENCE IF EXISTS perf_sequence",
            "CREATE TABLE perf_items (id INTEGER PRIMARY KEY, value TEXT)",
            "CREATE TABLE perf_source (id INTEGER, value TEXT)",
            "INSERT INTO perf_items VALUES (1, 'old')",
            "INSERT INTO perf_source VALUES (1, 'new'), (2, 'second')",
            "CREATE SEQUENCE perf_sequence START WITH 1",
        )
    for statement in statements:
        if engine == "alopex":
            connection.execute_sql(statement)
        else:
            connection.execute(statement)
    if engine != "alopex":
        connection.commit()


def _sql_workload(connection, engine: str, workload: str, rows: int) -> None:
    execute = connection.execute_sql if engine == "alopex" else connection.execute
    if workload == "sql-sqlite-curated":
        query = SQL_AGGREGATE_QUERY.replace("true", "1") if engine == "sqlite" else SQL_AGGREGATE_QUERY
        execute(query)
        execute("SELECT id, COALESCE(value, 0) FROM bench WHERE id >= 0 ORDER BY id LIMIT 16")
        if engine == "alopex":
            _sql_transaction(
                connection,
                engine,
                [
                    f"INSERT INTO bench VALUES ({rows + 1}, 1.0, true)",
                    "UPDATE bench SET value = 2.0 WHERE id = 0",
                ],
            )
        else:
            _sql_script(
                connection,
                engine,
                f"BEGIN; INSERT INTO bench VALUES ({rows + 1}, 1.0, true); "
                "SAVEPOINT perf_savepoint; UPDATE bench SET value = 2.0 WHERE id = 0; "
                "ROLLBACK TO perf_savepoint; RELEASE perf_savepoint; ROLLBACK",
            )
        if engine == "alopex":
            statement = connection.prepare("SELECT id FROM bench WHERE id = ?")
            statement.bind(1, 0)
            statement.execute()
            statement.finalize()
        else:
            connection.execute("SELECT id FROM bench WHERE id = ?", (0,)).fetchall()
        _sql_transaction(
            connection, engine, ["INSERT INTO perf_child VALUES (1, 1, 1)"]
        )
        return

    execute(f"EXPLAIN (FORMAT JSON) {SQL_AGGREGATE_QUERY}")
    _sql_transaction(
        connection,
        engine,
        ["INSERT INTO perf_items VALUES (3, 'returning') RETURNING id"],
    )
    _sql_transaction(
        connection,
        engine,
        [
            "INSERT INTO perf_items VALUES (1, 'conflict') "
            "ON CONFLICT (id) DO UPDATE SET value = 'updated' RETURNING value"
        ],
    )
    execute("SELECT nextval('perf_sequence')")
    _sql_transaction(
        connection,
        engine,
        [
            "CREATE TABLE perf_schema (id INTEGER)",
            "ALTER TABLE perf_schema ADD COLUMN value TEXT",
            "TRUNCATE TABLE perf_schema",
            "CREATE VIEW perf_view AS SELECT id, value FROM perf_schema",
        ],
    )
    _sql_transaction(
        connection,
        engine,
        [
            "MERGE INTO perf_items USING perf_source "
            "ON perf_items.id = perf_source.id "
            "WHEN MATCHED THEN UPDATE SET value = perf_source.value "
            "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (perf_source.id, perf_source.value)",
            "UPDATE perf_items SET value = perf_source.value FROM perf_source "
            "WHERE perf_items.id = perf_source.id",
            "DELETE FROM perf_items USING perf_source "
            "WHERE perf_items.id = perf_source.id AND perf_source.id = 2",
        ],
    )
    execute("SELECT table_name FROM information_schema.tables ORDER BY table_name LIMIT 16")


def _sql(
    engine: str, workload: str, rows: int, warmups: int, runs: int
) -> dict[str, float]:
    if engine == "alopex":
        from alopex import Database

        connection = Database.new()
        plan = lambda: connection.execute_sql(
            f"EXPLAIN (FORMAT JSON) {SQL_AGGREGATE_QUERY}"
        )
    elif engine == "sqlite":
        from pysqlite3 import dbapi2 as sqlite3

        if sqlite3.sqlite_version != "3.46.1":
            raise RuntimeError(f"expected SQLite 3.46.1, got {sqlite3.sqlite_version}")
        connection = sqlite3.connect(":memory:", isolation_level=None)
        plan = lambda: connection.execute(
            f"EXPLAIN QUERY PLAN {SQL_AGGREGATE_QUERY.replace('true', '1')}"
        ).fetchall()
    else:
        import psycopg

        connection = psycopg.connect(
            os.environ["ALOPEX_PERF_POSTGRES_DSN"], autocommit=True
        )
        version = connection.execute("SHOW server_version").fetchone()[0]
        if not version.startswith("16.14"):
            raise RuntimeError(f"expected PostgreSQL 16.14, got {version}")
        plan = lambda: connection.execute(
            f"EXPLAIN (FORMAT JSON) {SQL_AGGREGATE_QUERY}"
        ).fetchall()
    _populate_sql(connection, engine, rows)
    _prepare_sql_workload(connection, engine, workload)
    before_io = _io_bytes()
    plan_samples = _samples(plan, warmups, runs)
    execution_samples = _samples(
        lambda: _sql_workload(connection, engine, workload, rows), warmups, runs
    )
    metrics = summarize_latencies(execution_samples, rows)
    metrics.update(
        {
            "plan_latency_p50_ms": statistics.median(plan_samples) * 1000,
            "execution_latency_p50_ms": metrics["latency_p50_ms"],
            "queries_per_second": 1 / statistics.median(execution_samples),
            "peak_rss_bytes": _rss_bytes(),
            "temporary_io_bytes": max(1.0, _io_bytes() - before_io),
        }
    )
    connection.close()
    return metrics


def _sql_streaming(engine: str, rows: int, warmups: int, runs: int) -> dict[str, float]:
    returned_rows = (rows + 1) // 2
    if engine == "alopex":
        from alopex import Database

        connection = Database.new()
        _populate_sql(connection, engine, rows)

        def open_stream():
            return connection.execute_sql_stream(
                SQL_STREAM_QUERY,
                resource_limit_bytes=1024 * 1024 * 1024,
            )

        plan = lambda: connection.execute_sql(f"EXPLAIN (FORMAT JSON) {SQL_STREAM_QUERY}")

        def consume():
            started = time.perf_counter()
            with open_stream() as stream:
                next(stream)
                first_elapsed = time.perf_counter() - started
                count = 1 + sum(1 for _ in stream)
                return first_elapsed, time.perf_counter() - started, count, 1

    else:
        import datafusion
        import pyarrow as pa
        from datafusion import SessionContext

        if datafusion.__version__ != "50.0.0":
            raise RuntimeError(f"expected DataFusion 50.0.0, got {datafusion.__version__}")
        context = SessionContext()
        batches = [
            pa.record_batch(
                {
                    "id": range(start, min(start + 8192, rows)),
                    "value": [
                        float(index) for index in range(start, min(start + 8192, rows))
                    ],
                    "enabled": [
                        index % 2 == 0
                        for index in range(start, min(start + 8192, rows))
                    ],
                }
            )
            for start in range(0, rows, 8192)
        ]
        context.register_record_batches("bench", [batches])
        build = lambda: context.sql(SQL_STREAM_QUERY)
        plan = lambda: build().logical_plan()

        def consume():
            started = time.perf_counter()
            stream = iter(build().execute_stream())
            first = next(stream)
            first_elapsed = time.perf_counter() - started
            first_rows = first.to_pyarrow().num_rows
            count = first_rows + sum(
                batch.to_pyarrow().num_rows for batch in stream
            )
            return first_elapsed, time.perf_counter() - started, count, first_rows

    plan_samples = _samples(plan, warmups, runs)
    for _ in range(warmups):
        consume()
    first_samples = []
    total_samples = []
    steady_samples = []
    for _ in range(runs):
        first, total, count, first_rows = consume()
        if count != returned_rows:
            raise RuntimeError(
                f"{engine} SQL streaming returned {count} rows, expected {returned_rows}"
            )
        first_samples.append(first)
        total_samples.append(total)
        steady_samples.append((count - first_rows) / max(total - first, 1e-9))
    median_total = statistics.median(total_samples)
    if engine == "alopex":
        connection.close()
    return {
        "plan_build_p50_ms": statistics.median(plan_samples) * 1000,
        "time_to_first_batch_p50_ms": statistics.median(first_samples) * 1000,
        "total_p50_ms": median_total * 1000,
        "steady_state_rows_per_second": statistics.median(steady_samples),
        "peak_rss_bytes": _rss_bytes(),
    }


def _worker(args) -> int:
    if args.worker == "polars-eager-api":
        result = _eager(args.engine, args.rows, args.warmups, args.runs)
    elif args.worker in ("polars-csv-streaming", "polars-parquet-streaming"):
        result = _streaming(args.engine, args.fixture, args.rows, args.warmups, args.runs)
    elif args.worker == "sql-streaming":
        result = _sql_streaming(args.engine, args.rows, args.warmups, args.runs)
    else:
        result = _sql(args.engine, args.worker, args.rows, args.warmups, args.runs)
    print(json.dumps(result, sort_keys=True))
    return 0


def _run_worker(
    workload: str,
    engine: str,
    fixture: Path,
    rows: int,
    warmups: int,
    runs: int,
) -> dict[str, float]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        workload,
        "--engine",
        engine,
        "--fixture",
        str(fixture),
        "--rows",
        str(rows),
        "--warmups",
        str(warmups),
        "--runs",
        str(runs),
    ]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip())
    return json.loads(completed.stdout)


def _write_csv(path: Path, rows: int) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("id", "label"))
        writer.writerows(
            (
                index,
                "" if index % 10 == 0 else f"row-{index}",
            )
            for index in range(rows)
        )


def _write_parquet(path: Path, rows: int) -> None:
    import polars as pl

    pl.DataFrame(
        {
            "id": range(rows),
            "label": [None if index % 10 == 0 else f"row-{index}" for index in range(rows)],
        }
    ).write_parquet(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", choices=("curated", "full"), default="curated")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--hnsw-measurements", type=Path)
    parser.add_argument(
        "--worker",
        choices=(
            "polars-eager-api",
            "polars-csv-streaming",
            "polars-parquet-streaming",
            "sql-sqlite-curated",
            "sql-postgresql-curated",
            "sql-streaming",
        ),
    )
    parser.add_argument(
        "--engine", choices=("alopex", "polars", "sqlite", "postgresql", "datafusion")
    )
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--rows", type=int, default=34250)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--runs", type=int, default=21)
    args = parser.parse_args(argv)
    if args.worker:
        return _worker(args)
    if args.output is None:
        parser.error("--output is required")
    if args.rows != SQL_FIXTURE["rows"]:
        parser.error(f"--rows must match the fixed SQL fixture ({SQL_FIXTURE['rows']})")
    with tempfile.TemporaryDirectory() as directory:
        fixture = Path(directory) / "rows.csv"
        _write_csv(fixture, args.rows)
        results = []
        workloads = [
            ("polars-eager-v1", "polars-eager-api", ("alopex", "polars")),
            ("polars-lazy-streaming-v1", "polars-csv-streaming", ("alopex", "polars")),
            ("sql-sqlite-v1", "sql-sqlite-curated", ("alopex", "sqlite")),
            (
                "sql-postgresql-v1",
                "sql-postgresql-curated",
                ("alopex", "postgresql"),
            ),
            (
                "sql-datafusion-streaming-v1",
                "sql-streaming",
                ("alopex", "datafusion"),
            ),
        ]
        if args.suite == "full":
            workloads.append(
                ("polars-lazy-streaming-v1", "polars-parquet-streaming", ("alopex", "polars"))
            )
        for contract, evidence, engines in workloads:
            workload_fixture = fixture
            if evidence == "polars-parquet-streaming":
                workload_fixture = Path(directory) / "rows.parquet"
                _write_parquet(workload_fixture, args.rows)
            subject = _run_worker(
                evidence, engines[0], workload_fixture, args.rows, args.warmups, args.runs
            )
            reference = _run_worker(
                evidence, engines[1], workload_fixture, args.rows, args.warmups, args.runs
            )
            revision = {
                "polars-eager-v1": "pola-rs/polars@ae588a9f2c91171f45bace43a99fb7b80b90847b",
                "polars-lazy-streaming-v1": "pola-rs/polars@ae588a9f2c91171f45bace43a99fb7b80b90847b",
                "sql-sqlite-v1": "sqlite/sqlite@f3d536d37825302e31ed0eddd811c689f38f85a3",
                "sql-postgresql-v1": "postgres/postgres@0d1c00c624fa7367d4a895f44381887757289682",
                "sql-datafusion-streaming-v1": "apache/datafusion@d0a0c5a7d5867da949161b6065642d15293806de",
            }[contract]
            checksum = (
                "eec24557dcbdf71821b2e7afad1a7083dfb3256b71b57adf2e7ec387e1fd20ff"
                if contract.startswith("polars")
                else "23147656b2336db08e37cb876d26490bac40ef5b3d84077f8a76397e3232b3f3"
            )
            results.append(
                {
                    "contract": contract,
                    "evidence_id": evidence,
                    "covered_claims": WORKLOAD_COVERAGE[evidence],
                    "reference_revision": revision,
                    "dataset_sha256": checksum,
                    "subject": subject,
                    "reference": reference,
                }
            )
        if args.hnsw_measurements:
            hnsw = json.loads(args.hnsw_measurements.read_text(encoding="utf-8"))
            results.append(
                normalize_hnsw(
                    hnsw,
                    "ec7ad773a70e654d65ee3757748fe99d614be24ad087330e437bcc84cda85291",
                )
            )
    payload = {
        "schema": "alopex.performance-measurement/v1",
        "suite": args.suite,
        "environment": _environment(),
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
