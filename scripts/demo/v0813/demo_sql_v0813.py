#!/usr/bin/env python3
"""Public-package smoke for the v0.8.13 SQL correctness contracts.

v0.8.13 adds no new Nim SQL grammar (the parser source is byte-identical to
v0.8.12); every change is a planning/execution behavior on existing syntax.
This script exercises the four scenarios that are new in v0.8.13 and were
not covered by any earlier demo_sql_*.py:

- #411 atomic batch upsert: a multi-row `INSERT ... ON CONFLICT DO UPDATE`
  applies EXCLUDED values, and duplicate ids within one statement are
  rejected without a partial write.
- #412 transactional CSV vector ingestion via COPY: a clean batch inserts
  every row and maps CSV text into a VECTOR column; a batch with one bad
  row anywhere inserts nothing (all-or-nothing).
- #425 B-tree index plan selection: after `CREATE INDEX`, `EXPLAIN` reports
  an IndexScan access path for equality and range filters instead of a
  full scan.
- #424 NULL-safe equi-join: NULL join keys never match themselves in
  INNER/LEFT joins.

See docs/parity/vector-upsert-v0.8.13.json, docs/parity/copy-v0.8.13.json,
and docs/parity/btree-index-v0.8.13.json for the corresponding reference
compatibility inventory rows.
"""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from alopex import AlopexError, Database

CHECKS = 0


def check(condition: bool, message: str) -> None:
    global CHECKS
    assert condition, message
    CHECKS += 1


def expect_error(db: Database, sql: str, expected: str) -> None:
    try:
        db.execute_sql(sql)
    except AlopexError as error:
        check(expected.casefold() in str(error).casefold(), f"{error} (expected {expected!r})")
    else:
        raise AssertionError(f"expected {expected!r}: {sql}")


def atomic_batch_upsert(db: Database) -> None:
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2))")
    db.execute_sql("INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0])")

    db.execute_sql(
        "INSERT INTO items (id, embedding) VALUES (1, [1.0, 0.0]), (2, [0.0, 1.0]) "
        "ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding"
    )
    check(
        db.execute_sql("SELECT id, embedding FROM items ORDER BY id")
        == [
            {"id": 1, "embedding": [1.0, 0.0]},
            {"id": 2, "embedding": [0.0, 1.0]},
        ],
        "multi-row upsert must apply EXCLUDED values",
    )

    expect_error(
        db,
        "INSERT INTO items (id, embedding) VALUES (1, [9.0, 9.0]), (1, [8.0, 8.0]) "
        "ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding",
        "constraint",
    )
    check(
        db.execute_sql("SELECT id, embedding FROM items ORDER BY id")
        == [
            {"id": 1, "embedding": [1.0, 0.0]},
            {"id": 2, "embedding": [0.0, 1.0]},
        ],
        "a rejected duplicate-id batch must not partially apply",
    )


def transactional_copy_vector_ingestion(db: Database) -> None:
    db.execute_sql("CREATE TABLE vectors (id INT PRIMARY KEY, embedding VECTOR(2, L2))")

    with tempfile.TemporaryDirectory() as directory:
        good = Path(directory) / "good.csv"
        good.write_text('id,embedding\n1,"[1.0,0.0]"\n2,"[0.0,1.0]"\n', encoding="utf-8")
        db.execute_sql(
            f"COPY vectors (id, embedding) FROM '{good}' WITH (FORMAT CSV, HEADER TRUE)"
        )
        check(
            db.execute_sql("SELECT id FROM vectors ORDER BY id") == [{"id": 1}, {"id": 2}],
            "a clean CSV batch must insert every row",
        )

        bad = Path(directory) / "bad.csv"
        # id=4 carries a 1-dimensional vector against a VECTOR(2, L2) column.
        bad.write_text('id,embedding\n3,"[1.0,0.0]"\n4,"[1.0]"\n', encoding="utf-8")
        expect_error(
            db,
            f"COPY vectors (id, embedding) FROM '{bad}' WITH (FORMAT CSV, HEADER TRUE)",
            "dimension mismatch",
        )
        check(
            db.execute_sql("SELECT id FROM vectors ORDER BY id") == [{"id": 1}, {"id": 2}],
            "a failing COPY batch must not partially insert its valid rows",
        )


def btree_index_plan_selection(db: Database) -> None:
    db.execute_sql("CREATE TABLE scores (id INT PRIMARY KEY, score INT)")
    for row_id, score in ((1, 10), (2, 20), (3, 30)):
        db.execute_sql(f"INSERT INTO scores (id, score) VALUES ({row_id}, {score})")
    db.execute_sql("CREATE INDEX idx_scores_score ON scores(score)")

    for predicate, expected_rows in (
        ("score = 20", [{"id": 2}]),
        ("score >= 20", [{"id": 2}, {"id": 3}]),
    ):
        # No ORDER BY here: a Sort node wraps the Filter/Scan and the plan
        # no longer names an access path at the top, so it must not be
        # part of the EXPLAIN'd statement.
        plan = db.execute_sql(f"EXPLAIN SELECT id FROM scores WHERE {predicate}")
        plan_text = plan[0]["QUERY PLAN"]
        check(
            "IndexScan index=idx_scores_score" in plan_text,
            f"expected an IndexScan access path, got: {plan_text}",
        )
        rows = db.execute_sql(f"SELECT id FROM scores WHERE {predicate}")
        check(
            sorted(rows, key=lambda row: row["id"]) == expected_rows,
            f"index-backed filter returned wrong rows for {predicate!r}: {rows}",
        )


def null_safe_equi_join(db: Database) -> None:
    db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, customer_ref INT)")
    db.execute_sql("CREATE TABLE customers (id INT PRIMARY KEY, ref INT)")
    db.execute_sql("INSERT INTO orders (id, customer_ref) VALUES (1, NULL), (2, 5)")
    db.execute_sql("INSERT INTO customers (id, ref) VALUES (10, NULL), (20, 5)")

    inner = db.execute_sql(
        "SELECT orders.id AS order_id, customers.id AS customer_id FROM orders "
        "JOIN customers ON orders.customer_ref = customers.ref ORDER BY orders.id"
    )
    check(
        inner == [{"order_id": 2, "customer_id": 20}],
        f"NULL=NULL must not satisfy an INNER JOIN, got: {inner}",
    )

    left = db.execute_sql(
        "SELECT orders.id AS order_id, customers.id AS customer_id FROM orders "
        "LEFT JOIN customers ON orders.customer_ref = customers.ref ORDER BY orders.id"
    )
    check(
        left
        == [
            {"order_id": 1, "customer_id": None},
            {"order_id": 2, "customer_id": 20},
        ],
        f"NULL=NULL must not satisfy a LEFT JOIN either, got: {left}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        action="append",
        choices=(
            "atomic-batch-upsert",
            "csv-vector-copy",
            "btree-index-plan",
            "null-equi-join",
        ),
        help="run one named v0.8.13 contract; repeat to select several",
    )
    selected = parser.parse_args().scenario
    scenarios = {
        "atomic-batch-upsert": atomic_batch_upsert,
        "csv-vector-copy": transactional_copy_vector_ingestion,
        "btree-index-plan": btree_index_plan_selection,
        "null-equi-join": null_safe_equi_join,
    }

    for name in selected or scenarios:
        before = CHECKS
        scenarios[name](Database.new())
        print(f"v0.8.13 {name}: {CHECKS - before} checks passed")

    if selected is None and CHECKS != 12:
        raise AssertionError(f"v0.8.13 SQL demo check count changed: {CHECKS} != 12")
    print(f"v0.8.13 SQL correctness demo completed: {CHECKS} checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
