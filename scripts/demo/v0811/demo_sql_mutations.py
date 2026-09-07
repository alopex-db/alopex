#!/usr/bin/env python3
"""Public-package smoke for the v0.8.11 SQL mutation contracts."""
from __future__ import annotations

import io
import tempfile
from pathlib import Path

from alopex import AlopexError, Database


def expect_error(db: Database, sql: str, expected: str) -> None:
    try:
        db.execute_sql(sql)
    except AlopexError as error:
        assert expected.casefold() in str(error).casefold(), error
    else:
        raise AssertionError(f"expected {expected!r}: {sql}")


def main() -> int:
    db = Database.new()
    db.execute_sql("CREATE TABLE parent (id BIGINT PRIMARY KEY, quota BIGINT CHECK (quota >= 0))")
    db.execute_sql(
        "CREATE TABLE child (id BIGINT PRIMARY KEY, parent_id BIGINT, "
        "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE)"
    )
    db.execute_sql("INSERT INTO parent VALUES (1, 2)")
    db.execute_sql("INSERT INTO child VALUES (1, 1)")
    expect_error(db, "INSERT INTO child VALUES (2, 99)", "constraint")

    db.execute_sql("CREATE TABLE jobs (id SERIAL PRIMARY KEY, label TEXT UNIQUE)")
    assert db.execute_sql("INSERT INTO jobs (label) VALUES ('first') RETURNING id") == [{"id": 1}]
    assert db.execute_sql(
        "INSERT INTO jobs (id, label) VALUES (1, 'duplicate') "
        "ON CONFLICT DO NOTHING RETURNING id"
    ) == []
    db.execute_sql("CREATE SEQUENCE bounded START WITH 2 MINVALUE 2 MAXVALUE 3 CYCLE")
    assert db.execute_sql("SELECT nextval('bounded') AS value") == [{"value": 2}]
    assert db.execute_sql("SELECT currval('bounded') AS value") == [{"value": 2}]
    assert any(sequence["name"] == "bounded" for sequence in db.list_sequences())

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "jobs.csv"
        db.execute_sql(f"COPY jobs TO '{path}' WITH (FORMAT CSV, HEADER TRUE)")
        restored = Database.new()
        restored.execute_sql("CREATE TABLE jobs (id INTEGER PRIMARY KEY, label TEXT UNIQUE)")
        restored.execute_sql(f"COPY jobs FROM '{path}' WITH (FORMAT CSV, HEADER TRUE)")
        assert restored.execute_sql("SELECT id, label FROM jobs") == [{"id": 1, "label": "first"}]
        expect_error(db, f"COPY jobs TO '{path}' WITH (FORMAT JSON)", "COPY FORMAT JSON")

    stream = io.BytesIO()
    assert db.copy_to_csv("jobs", stream, header=True) == 1
    streamed = Database.new()
    streamed.execute_sql("CREATE TABLE jobs (id INTEGER PRIMARY KEY, label TEXT UNIQUE)")
    stream.seek(0)
    assert streamed.copy_from_csv("jobs", stream, header=True) == 1

    constraints = db.execute_sql(
        "SELECT constraint_type FROM information_schema.table_constraints "
        "WHERE table_name = 'child' ORDER BY constraint_type"
    )
    assert constraints
    print("v0.8.11 SQL mutation contracts passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
