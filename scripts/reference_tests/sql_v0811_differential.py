"""Run the curated v0.8.11 SQL overlap against pinned reference engines."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from scripts.parity.runner.normalize import normalize_scalar


ROOT = Path(__file__).resolve().parents[2]
CORPUS_PATH = Path(__file__).with_name("sql_v0811_curated.json")
CORPUS = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


def normalize_rows(rows):
    return [
        {str(name): normalize_scalar(value) for name, value in row.items()}
        for row in rows
    ]


def alopex_results():
    from alopex import Database

    database = Database.new()
    database.execute_sql(
        "CREATE TABLE parity_rows ("
        "id INTEGER PRIMARY KEY, value DOUBLE, label TEXT, enabled BOOLEAN)"
    )
    database.execute_sql(
        "INSERT INTO parity_rows VALUES "
        "(1, 10.5, 'one', true), (2, NULL, 'βeta', false), (3, 30.25, NULL, true)"
    )
    results = {
        case["id"]: normalize_rows(database.execute_sql(case["sql"]))
        for case in CORPUS["cases"]
    }
    errors = {}
    for case in CORPUS["error_cases"]:
        try:
            database.execute_sql(case["sql"])
        except Exception:
            errors[case["id"]] = "error"
        else:
            errors[case["id"]] = "success"
    database.close()
    return {"results": results, "errors": errors}


def dbapi_results(connection, engine: str):
    connection.execute("DROP TABLE IF EXISTS parity_rows")
    boolean = "INTEGER" if engine == "sqlite" else "BOOLEAN"
    connection.execute(
        "CREATE TABLE parity_rows ("
        f"id INTEGER PRIMARY KEY, value DOUBLE PRECISION, label TEXT, enabled {boolean})"
    )
    enabled = ("1", "0", "1") if engine == "sqlite" else ("true", "false", "true")
    connection.execute(
        "INSERT INTO parity_rows VALUES "
        f"(1, 10.5, 'one', {enabled[0]}), "
        f"(2, NULL, 'βeta', {enabled[1]}), "
        f"(3, 30.25, NULL, {enabled[2]})"
    )
    results = {}
    for case in CORPUS["cases"]:
        sql = case["sql"].replace("enabled = true", "enabled = 1") if engine == "sqlite" else case["sql"]
        cursor = connection.execute(sql)
        names = [column.name if hasattr(column, "name") else column[0] for column in cursor.description]
        results[case["id"]] = normalize_rows(
            [dict(zip(names, row)) for row in cursor.fetchall()]
        )
    errors = {}
    for case in CORPUS["error_cases"]:
        try:
            connection.execute(case["sql"]).fetchall()
        except Exception:
            errors[case["id"]] = "error"
            if engine == "postgresql":
                connection.rollback()
        else:
            errors[case["id"]] = "success"
    connection.close()
    return {"results": results, "errors": errors}


def reference_results(engine: str):
    if engine == "sqlite":
        from pysqlite3 import dbapi2 as sqlite3

        if sqlite3.sqlite_version != CORPUS["references"][engine]["version"]:
            raise RuntimeError(f"expected SQLite 3.46.1, got {sqlite3.sqlite_version}")
        return dbapi_results(sqlite3.connect(":memory:"), engine)
    if engine == "postgresql":
        import psycopg

        connection = psycopg.connect(os.environ["ALOPEX_REFERENCE_POSTGRES_DSN"], autocommit=True)
        version = connection.execute("SHOW server_version").fetchone()[0]
        if not version.startswith(CORPUS["references"][engine]["version"]):
            raise RuntimeError(f"expected PostgreSQL 16.14, got {version}")
        return dbapi_results(connection, engine)
    if engine == "duckdb":
        import duckdb

        if duckdb.__version__ != CORPUS["references"][engine]["version"]:
            raise RuntimeError(f"expected DuckDB 1.4.0, got {duckdb.__version__}")
        return dbapi_results(duckdb.connect(":memory:"), engine)
    import datafusion
    import pyarrow as pa
    from datafusion import SessionContext

    if datafusion.__version__ != CORPUS["references"][engine]["version"]:
        raise RuntimeError(f"expected DataFusion 50.0.0, got {datafusion.__version__}")
    context = SessionContext()
    setup = CORPUS["setup"]
    columns = dict(zip(setup["columns"], zip(*setup["rows"])))
    context.register_record_batches("parity_rows", [[pa.record_batch(columns)]])
    results = {}
    for case in CORPUS["cases"]:
        rows = []
        for batch in context.sql(case["sql"]).collect():
            rows.extend(batch.to_pylist())
        results[case["id"]] = normalize_rows(rows)
    errors = {}
    for case in CORPUS["error_cases"]:
        try:
            context.sql(case["sql"]).collect()
        except Exception:
            errors[case["id"]] = "error"
        else:
            errors[case["id"]] = "success"
    return {"results": results, "errors": errors}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--engine",
        action="append",
        choices=tuple(CORPUS["references"]),
        required=True,
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    subject = alopex_results()
    evidence = {}
    for engine in args.engine:
        reference = reference_results(engine)
        if subject != reference:
            raise AssertionError(
                json.dumps({"engine": engine, "alopex": subject, "reference": reference}, indent=2, ensure_ascii=False)
            )
        evidence[engine] = {"reference": CORPUS["references"][engine], "passed": True}
    payload = {"schema": "alopex.sql-reference-evidence/v1", "corpus": str(CORPUS_PATH.relative_to(ROOT)), "engines": evidence}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
