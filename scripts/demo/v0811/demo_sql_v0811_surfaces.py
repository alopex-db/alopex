#!/usr/bin/env python3
"""Public-package smoke for v0.8.11 SQL surfaces not covered by
demo_sql_mutations.py.

v0.8.11 (#166, #167, #169, #170, #171) added portable metadata
introspection, positional `?` parameters, EXPLAIN/EXPLAIN (FORMAT JSON),
dynamic views, transactional ALTER TABLE/TRUNCATE, and MERGE. This script
was written because no release verification report since v0.8.10 has
exercised any of them -- issue alopex-db/alopex#428 stage 3 requires this
gap closed before v0.8.13's post-release report is treated as complete.
"""
from __future__ import annotations

import json

from alopex import Database

CHECKS = 0


def check(condition: bool, message: str) -> None:
    global CHECKS
    assert condition, message
    CHECKS += 1


def portable_metadata(db: Database) -> None:
    db.execute_sql('CREATE TABLE "Order Items" (id BIGINT, label TEXT)')

    check(
        db.execute_sql("SHOW TABLES") == [{"table_name": "Order Items"}],
        "SHOW TABLES must list the created table",
    )
    check(
        db.execute_sql('DESC "Order Items"')
        == [
            {
                "column_name": "id",
                "column_type": "BIGINT",
                "null": "YES",
                "key": "",
                "default": None,
                "extra": "",
            },
            {
                "column_name": "label",
                "column_type": "TEXT",
                "null": "YES",
                "key": "",
                "default": None,
                "extra": "",
            },
        ],
        "DESC must report both columns in declaration order",
    )
    check(
        db.execute_sql(
            "SELECT table_name, column_name, ordinal_position "
            "FROM information_schema.columns ORDER BY ordinal_position"
        )
        == [
            {"table_name": "Order Items", "column_name": "id", "ordinal_position": 1},
            {"table_name": "Order Items", "column_name": "label", "ordinal_position": 2},
        ],
        "information_schema.columns must mirror DESC",
    )


def positional_parameters_and_explain(db: Database) -> None:
    db.execute_sql("CREATE TABLE secrets (id INTEGER PRIMARY KEY, value TEXT)")
    db.execute_sql("INSERT INTO secrets (id, value) VALUES (1, 'top-secret')")

    check(
        db.execute_sql("SELECT id FROM secrets WHERE value = ?", ["top-secret"])
        == [{"id": 1}],
        "a `?` positional parameter must bind by value, not by SQL text",
    )

    rows = db.execute_sql(
        "EXPLAIN (FORMAT JSON) SELECT id FROM secrets WHERE value = ?",
        ["never-show-this"],
    )
    payload = rows[0]["query_plan"]
    check("never-show-this" not in payload, "EXPLAIN JSON must redact bound parameter values")
    document = json.loads(payload)
    check(document["schema"] == "alopex.explain", f"unexpected EXPLAIN schema: {document}")


def dynamic_view(db: Database) -> None:
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
    db.execute_sql("INSERT INTO items (id, label) VALUES (1, 'first')")
    db.execute_sql("CREATE VIEW IF NOT EXISTS labels (name) AS SELECT label FROM items")

    check(
        db.execute_sql("SELECT name FROM labels") == [{"name": "first"}],
        "a fresh view must reflect the underlying table at creation time",
    )
    db.execute_sql("INSERT INTO items (id, label) VALUES (2, 'second')")
    check(
        db.execute_sql("SELECT name FROM labels ORDER BY name")
        == [{"name": "first"}, {"name": "second"}],
        "a view must stay dynamic: it must reflect rows inserted after creation",
    )
    db.execute_sql("DROP VIEW IF EXISTS labels")


def alter_table_and_truncate(db: Database) -> None:
    db.execute_sql("CREATE TABLE inventory (id INTEGER PRIMARY KEY, label TEXT)")
    db.execute_sql("INSERT INTO inventory (id, label) VALUES (1, 'widget')")

    db.execute_sql("ALTER TABLE inventory ADD COLUMN IF NOT EXISTS score BIGINT DEFAULT 0")
    check(
        db.execute_sql("SELECT id, label, score FROM inventory")
        == [{"id": 1, "label": "widget", "score": 0}],
        "ADD COLUMN ... DEFAULT must backfill every existing row",
    )

    db.execute_sql("ALTER TABLE inventory RENAME COLUMN label TO name")
    check(
        db.execute_sql("SELECT id, name, score FROM inventory")
        == [{"id": 1, "name": "widget", "score": 0}],
        "RENAME COLUMN must be visible to subsequent SELECTs",
    )

    db.execute_sql("TRUNCATE TABLE inventory")
    check(
        db.execute_sql("SELECT id FROM inventory") == [],
        "TRUNCATE must remove every row while keeping the table itself",
    )
    db.execute_sql("INSERT INTO inventory (id, name, score) VALUES (2, 'gadget', 5)")
    check(
        db.execute_sql("SELECT id FROM inventory") == [{"id": 2}],
        "the table must remain usable after TRUNCATE",
    )


def merge_into(db: Database) -> None:
    db.execute_sql("CREATE TABLE target (id BIGINT PRIMARY KEY, value TEXT)")
    db.execute_sql("CREATE TABLE source (id BIGINT, value TEXT)")
    db.execute_sql("INSERT INTO target VALUES (1, 'old')")
    db.execute_sql("INSERT INTO source VALUES (1, 'updated'), (2, 'inserted')")

    affected = db.execute_sql(
        "MERGE INTO target USING source ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET value = source.value "
        "WHEN NOT MATCHED THEN INSERT (id, value) "
        "VALUES (source.id, source.value)"
    )
    check(affected == 2, f"MERGE must report 2 affected rows (1 update + 1 insert), got {affected}")
    check(
        db.execute_sql("SELECT id, value FROM target ORDER BY id")
        == [{"id": 1, "value": "updated"}, {"id": 2, "value": "inserted"}],
        "MERGE must update matches and insert non-matches in one statement",
    )


def main() -> int:
    db = Database.new()
    portable_metadata(db)
    positional_parameters_and_explain(db)
    dynamic_view(db)
    alter_table_and_truncate(db)
    merge_into(db)

    if CHECKS != 14:
        raise AssertionError(f"v0.8.11 surfaces demo check count changed: {CHECKS} != 14")
    print("v0.8.11 SQL surfaces demo completed: 14 checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
