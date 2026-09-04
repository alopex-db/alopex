"""Compare v0.8.11 advanced DML with pinned PostgreSQL and DuckDB."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from scripts.reference_tests.sql_v0811_differential import normalize_rows


ROOT = Path(__file__).resolve().parents[2]
CORPUS_PATH = Path(__file__).with_name("sql_v0811_advanced_dml.json")
CORPUS = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


def _alopex_rows(database, sql):
    return normalize_rows(database.execute_sql(sql))


def alopex_results():
    from alopex import Database

    evidence = {}
    for case in CORPUS["cases"]:
        database = Database.new()
        for statement in case["setup"]:
            database.execute_sql(statement)
        try:
            returned = _alopex_rows(database, case["statement"]) if case["result"] else []
            if not case["result"]:
                database.execute_sql(case["statement"])
        except Exception as error:
            raise RuntimeError(f"advanced DML case {case['id']} failed: {error}") from error
        evidence[case["id"]] = {
            "returned": returned,
            "rows": _alopex_rows(database, case["verify"]),
        }
        database.close()
    return evidence


def _dbapi_rows(cursor):
    if cursor.description is None:
        return []
    names = [column.name if hasattr(column, "name") else column[0] for column in cursor.description]
    return normalize_rows([dict(zip(names, row)) for row in cursor.fetchall()])


def dbapi_results(connection):
    evidence = {}
    for case in CORPUS["cases"]:
        connection.execute("DROP TABLE IF EXISTS dml_source")
        connection.execute("DROP TABLE IF EXISTS dml_target")
        for statement in case["setup"]:
            connection.execute(statement)
        cursor = connection.execute(case["statement"])
        returned = _dbapi_rows(cursor) if case["result"] else []
        evidence[case["id"]] = {
            "returned": returned,
            "rows": _dbapi_rows(connection.execute(case["verify"])),
        }
    connection.close()
    return evidence


def reference_results(engine):
    if engine == "postgresql":
        import psycopg

        connection = psycopg.connect(os.environ["ALOPEX_REFERENCE_POSTGRES_DSN"], autocommit=True)
        version = connection.execute("SHOW server_version").fetchone()[0]
        if not version.startswith(CORPUS["references"][engine]["version"]):
            raise RuntimeError(f"expected PostgreSQL 16.14, got {version}")
        return dbapi_results(connection)
    import duckdb

    if duckdb.__version__ != CORPUS["references"][engine]["version"]:
        raise RuntimeError(f"expected DuckDB 1.4.0, got {duckdb.__version__}")
    return dbapi_results(duckdb.connect(":memory:"))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", action="append", choices=tuple(CORPUS["references"]), required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    subject = alopex_results()
    engines = {}
    for engine in args.engine:
        reference = reference_results(engine)
        if subject != reference:
            raise AssertionError(json.dumps({"engine": engine, "alopex": subject, "reference": reference}, indent=2, ensure_ascii=False))
        engines[engine] = {"reference": CORPUS["references"][engine], "passed": True}
    payload = {"schema": "alopex.sql-advanced-dml-evidence/v1", "corpus": str(CORPUS_PATH.relative_to(ROOT)), "engines": engines}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
