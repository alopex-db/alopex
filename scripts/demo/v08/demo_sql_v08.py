#!/usr/bin/env python3
"""Exercise SQL correctness shipped across the v0.8 release line."""

from __future__ import annotations

import sys
from typing import Any


Row = dict[str, Any]


def show(sql: str, result: Any) -> None:
    print(f"call> db.execute_sql({sql!r})")
    print(f"   -> {result!r}")


def check_ordered(db: Any, sql: str, expected_rows: list[Row]) -> None:
    """Compare rows in SQL-defined order, including Python value types."""
    actual = db.execute_sql(sql)
    show(sql, actual)
    if [canonical_row(row) for row in actual] != [
        canonical_row(row) for row in expected_rows
    ]:
        raise AssertionError(
            f"ordered SQL result mismatch for {sql!r}: "
            f"actual={actual!r}, expected={expected_rows!r}"
        )


def canonical_value(value: Any) -> tuple[Any, ...]:
    """Return a sortable, type-sensitive representation of a Python value."""
    if isinstance(value, list):
        return ("list", tuple(canonical_value(item) for item in value))
    if isinstance(value, dict):
        return (
            "dict",
            tuple(sorted((key, canonical_value(item)) for key, item in value.items())),
        )
    return (type(value).__name__, repr(value))


def canonical_rows(rows: list[Row]) -> list[tuple[Any, ...]]:
    normalized = [canonical_row(row) for row in rows]
    return sorted(normalized, key=repr)


def canonical_row(row: Row) -> tuple[Any, ...]:
    return tuple(sorted((key, canonical_value(value)) for key, value in row.items()))


def check_unordered(db: Any, sql: str, expected_rows: list[Row]) -> None:
    """Compare a row multiset without inventing an order for set operations."""
    actual = db.execute_sql(sql)
    show(sql, actual)
    if canonical_rows(actual) != canonical_rows(expected_rows):
        raise AssertionError(
            f"unordered SQL result mismatch for {sql!r}: "
            f"actual={actual!r}, expected={expected_rows!r}"
        )


def expect_error(db: Any, sql: str, code_or_substring: str) -> None:
    """Require a fail-closed SQL error that identifies the rejected contract."""
    try:
        db.execute_sql(sql)
    except Exception as exc:  # AlopexError is an extension-defined Exception.
        print(f"call> db.execute_sql({sql!r})")
        print(f"   -> ERROR {type(exc).__name__}: {exc}")
        if code_or_substring.casefold() not in str(exc).casefold():
            raise AssertionError(
                f"SQL error for {sql!r} did not contain {code_or_substring!r}: {exc}"
            ) from exc
    else:
        raise AssertionError(f"SQL expected to fail was accepted: {sql!r}")


def main() -> int:
    from alopex import Database

    db = Database.new()
    try:
        statements = [
            "CREATE TABLE metrics (id INTEGER PRIMARY KEY, n INTEGER, f FLOAT, ts TIMESTAMP)",
            "INSERT INTO metrics VALUES (1, 2, 1.5, '2025-01-15 10:30:00')",
            "INSERT INTO metrics VALUES (2, 3, 2.5, '2025-01-15 10:30:01.25')",
            "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL, qty INTEGER, bonus REAL)",
            "INSERT INTO sales VALUES (1, 'east', 100.0, 3, 10.0)",
            "INSERT INTO sales VALUES (2, 'east', 200.0, 1, NULL)",
            "INSERT INTO sales VALUES (3, 'west', 150.0, 5, 20.0)",
            "INSERT INTO sales VALUES (4, 'west', 150.0, 2, NULL)",
            "INSERT INTO sales VALUES (5, 'north', 50.0, 0, 5.0)",
            "CREATE TABLE ints (id INTEGER PRIMARY KEY)",
            "CREATE TABLE doubles (id DOUBLE PRIMARY KEY)",
            "INSERT INTO ints VALUES (1)",
            "INSERT INTO doubles VALUES (1.0)",
        ]
        for sql in statements:
            show(sql, db.execute_sql(sql))

        # These 30 queries have an explicit or single-row order contract.
        ordered_checks: list[tuple[str, list[Row]]] = [
            ("SELECT SUM(n) AS total FROM metrics", [{"total": 5}]),
            (
                "SELECT id, n * 2.0 AS doubled FROM metrics ORDER BY doubled",
                [{"id": 1, "doubled": 4.0}, {"id": 2, "doubled": 6.0}],
            ),
            (
                "SELECT id FROM metrics WHERE n IN (2, 4) AND n BETWEEN 1 AND 3",
                [{"id": 1}],
            ),
            (
                "SELECT CAST(n AS DOUBLE) AS converted FROM metrics WHERE id = 1",
                [{"converted": 2.0}],
            ),
            (
                "SELECT ints.id FROM ints JOIN doubles ON ints.id = doubles.id",
                [{"id": 1}],
            ),
            (
                "SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY total DESC, region",
                [
                    {"region": "east", "total": 300.0},
                    {"region": "west", "total": 300.0},
                    {"region": "north", "total": 50.0},
                ],
            ),
            (
                "SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING total >= 300 ORDER BY region",
                [
                    {"region": "east", "total": 300.0},
                    {"region": "west", "total": 300.0},
                ],
            ),
            (
                "SELECT amount AS id FROM sales ORDER BY id",
                [{"id": 50.0}, {"id": 100.0}, {"id": 150.0}, {"id": 150.0}, {"id": 200.0}],
            ),
            (
                "SELECT id, amount FROM sales ORDER BY id",
                [
                    {"id": 1, "amount": 100.0},
                    {"id": 2, "amount": 200.0},
                    {"id": 3, "amount": 150.0},
                    {"id": 4, "amount": 150.0},
                    {"id": 5, "amount": 50.0},
                ],
            ),
            ("SELECT pg_typeof(amount) AS kind FROM sales WHERE id = 1", [{"kind": "real"}]),
            ("SELECT amount + 0.5 AS adjusted FROM sales WHERE id = 1", [{"adjusted": 100.5}]),
            (
                "SELECT id, CASE WHEN qty > 2 THEN 'bulk' ELSE 'small' END AS band FROM sales ORDER BY id",
                [
                    {"id": 1, "band": "bulk"},
                    {"id": 2, "band": "small"},
                    {"id": 3, "band": "bulk"},
                    {"id": 4, "band": "small"},
                    {"id": 5, "band": "small"},
                ],
            ),
            (
                "SELECT id, CASE region WHEN 'east' THEN 1 WHEN 'west' THEN 2 ELSE 3 END AS code FROM sales ORDER BY id",
                [
                    {"id": 1, "code": 1},
                    {"id": 2, "code": 1},
                    {"id": 3, "code": 2},
                    {"id": 4, "code": 2},
                    {"id": 5, "code": 3},
                ],
            ),
            (
                "SELECT id, CASE WHEN bonus > 10 THEN 'large' END AS bonus_band FROM sales ORDER BY id",
                [
                    {"id": 1, "bonus_band": None},
                    {"id": 2, "bonus_band": None},
                    {"id": 3, "bonus_band": "large"},
                    {"id": 4, "bonus_band": None},
                    {"id": 5, "bonus_band": None},
                ],
            ),
            (
                "SELECT id, CASE WHEN qty = 0 THEN 1 ELSE 2.5 END AS numeric_case FROM sales ORDER BY id",
                [
                    {"id": 1, "numeric_case": 2.5},
                    {"id": 2, "numeric_case": 2.5},
                    {"id": 3, "numeric_case": 2.5},
                    {"id": 4, "numeric_case": 2.5},
                    {"id": 5, "numeric_case": 1.0},
                ],
            ),
            (
                "SELECT id, CASE WHEN region = 'east' THEN CASE WHEN qty > 2 THEN 'east-bulk' ELSE 'east-small' END ELSE 'other' END AS bucket FROM sales ORDER BY id",
                [
                    {"id": 1, "bucket": "east-bulk"},
                    {"id": 2, "bucket": "east-small"},
                    {"id": 3, "bucket": "other"},
                    {"id": 4, "bucket": "other"},
                    {"id": 5, "bucket": "other"},
                ],
            ),
            (
                "SELECT id, CASE WHEN qty = 0 THEN TRUE ELSE FALSE END AS is_zero FROM sales ORDER BY id",
                [
                    {"id": 1, "is_zero": False},
                    {"id": 2, "is_zero": False},
                    {"id": 3, "is_zero": False},
                    {"id": 4, "is_zero": False},
                    {"id": 5, "is_zero": True},
                ],
            ),
            (
                "WITH high AS (SELECT id, amount FROM sales WHERE amount >= 150) SELECT id, amount FROM high ORDER BY id",
                [{"id": 2, "amount": 200.0}, {"id": 3, "amount": 150.0}, {"id": 4, "amount": 150.0}],
            ),
            (
                "WITH renamed(identifier, territory) AS (SELECT id, region FROM sales WHERE id = 1) SELECT territory, identifier FROM renamed",
                [{"territory": "east", "identifier": 1}],
            ),
            (
                "WITH a AS (SELECT id FROM sales WHERE id <= 2), b AS (SELECT id FROM sales WHERE id >= 4) SELECT a.id AS left_id, b.id AS right_id FROM a, b ORDER BY left_id, right_id",
                [
                    {"left_id": 1, "right_id": 4},
                    {"left_id": 1, "right_id": 5},
                    {"left_id": 2, "right_id": 4},
                    {"left_id": 2, "right_id": 5},
                ],
            ),
            (
                "WITH totals AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) SELECT region, total FROM totals ORDER BY region",
                [
                    {"region": "east", "total": 300.0},
                    {"region": "north", "total": 50.0},
                    {"region": "west", "total": 300.0},
                ],
            ),
            (
                "WITH chosen AS (SELECT id, region FROM sales WHERE amount >= 150) SELECT sales.id AS sales_id, chosen.id AS chosen_id FROM sales JOIN chosen ON sales.region = chosen.region ORDER BY sales_id, chosen_id",
                [
                    {"sales_id": 1, "chosen_id": 2},
                    {"sales_id": 2, "chosen_id": 2},
                    {"sales_id": 3, "chosen_id": 3},
                    {"sales_id": 3, "chosen_id": 4},
                    {"sales_id": 4, "chosen_id": 3},
                    {"sales_id": 4, "chosen_id": 4},
                ],
            ),
            (
                "WITH sales AS (SELECT id + 100 AS id FROM sales WHERE id = 1) SELECT id FROM sales",
                [{"id": 101}],
            ),
            (
                "SELECT id, SUM(amount) OVER () AS grand FROM sales ORDER BY id",
                [{"id": idx, "grand": 650.0} for idx in range(1, 6)],
            ),
            (
                "SELECT id, SUM(amount) OVER (PARTITION BY region) AS regional FROM sales ORDER BY id",
                [
                    {"id": 1, "regional": 300.0},
                    {"id": 2, "regional": 300.0},
                    {"id": 3, "regional": 300.0},
                    {"id": 4, "regional": 300.0},
                    {"id": 5, "regional": 50.0},
                ],
            ),
            (
                "SELECT id, SUM(amount) OVER (ORDER BY id) AS running FROM sales ORDER BY id",
                [
                    {"id": 1, "running": 100.0},
                    {"id": 2, "running": 300.0},
                    {"id": 3, "running": 450.0},
                    {"id": 4, "running": 600.0},
                    {"id": 5, "running": 650.0},
                ],
            ),
            (
                "SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, id) AS rn FROM sales ORDER BY id",
                [
                    {"id": 1, "rn": 2},
                    {"id": 2, "rn": 1},
                    {"id": 3, "rn": 1},
                    {"id": 4, "rn": 2},
                    {"id": 5, "rn": 1},
                ],
            ),
            (
                "SELECT id, RANK() OVER (ORDER BY amount) AS rank_value, DENSE_RANK() OVER (ORDER BY amount) AS dense_value FROM sales ORDER BY id",
                [
                    {"id": 1, "rank_value": 2, "dense_value": 2},
                    {"id": 2, "rank_value": 5, "dense_value": 4},
                    {"id": 3, "rank_value": 3, "dense_value": 3},
                    {"id": 4, "rank_value": 3, "dense_value": 3},
                    {"id": 5, "rank_value": 1, "dense_value": 1},
                ],
            ),
            (
                "SELECT id, COUNT(*) OVER (PARTITION BY region) AS region_count, AVG(amount) OVER (PARTITION BY region) AS region_mean FROM sales ORDER BY id",
                [
                    {"id": 1, "region_count": 2, "region_mean": 150.0},
                    {"id": 2, "region_count": 2, "region_mean": 150.0},
                    {"id": 3, "region_count": 2, "region_mean": 150.0},
                    {"id": 4, "region_count": 2, "region_mean": 150.0},
                    {"id": 5, "region_count": 1, "region_mean": 50.0},
                ],
            ),
            (
                "SELECT id, SUM(bonus) OVER (PARTITION BY region) AS region_bonus FROM sales ORDER BY id",
                [
                    {"id": 1, "region_bonus": 10.0},
                    {"id": 2, "region_bonus": 10.0},
                    {"id": 3, "region_bonus": 20.0},
                    {"id": 4, "region_bonus": 20.0},
                    {"id": 5, "region_bonus": 5.0},
                ],
            ),
            (
                "SELECT region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM sales ORDER BY region, rn",
                [
                    {"region": "east", "rn": 1},
                    {"region": "east", "rn": 2},
                    {"region": "north", "rn": 1},
                    {"region": "west", "rn": 1},
                    {"region": "west", "rn": 2},
                ],
            ),
        ]

        # These 10 queries assert row multisets; their SQL does not promise order.
        unordered_checks: list[tuple[str, list[Row]]] = [
            (
                "SELECT region FROM sales WHERE region = 'west' UNION SELECT region FROM sales WHERE region = 'north'",
                [{"region": "west"}, {"region": "north"}],
            ),
            (
                "SELECT id FROM sales WHERE amount >= 150 UNION SELECT id FROM sales WHERE qty <= 2",
                [{"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}],
            ),
            (
                "SELECT id FROM sales WHERE amount >= 150 UNION ALL SELECT id FROM sales WHERE qty <= 2",
                [
                    {"id": 2},
                    {"id": 3},
                    {"id": 4},
                    {"id": 2},
                    {"id": 4},
                    {"id": 5},
                ],
            ),
            (
                "SELECT id FROM sales WHERE amount >= 150 INTERSECT SELECT id FROM sales WHERE qty <= 2",
                [{"id": 2}, {"id": 4}],
            ),
            (
                "SELECT id FROM sales WHERE amount >= 150 EXCEPT SELECT id FROM sales WHERE qty <= 2",
                [{"id": 3}],
            ),
            (
                "SELECT id FROM sales WHERE qty <= 2 EXCEPT SELECT id FROM sales WHERE amount >= 150",
                [{"id": 5}],
            ),
            (
                "SELECT 1 AS value UNION ALL SELECT 1 UNION SELECT 1",
                [{"value": 1}],
            ),
            (
                "SELECT bonus FROM sales UNION SELECT bonus FROM sales",
                [{"bonus": None}, {"bonus": 5.0}, {"bonus": 10.0}, {"bonus": 20.0}],
            ),
            (
                "SELECT 1 AS value UNION SELECT 1 UNION ALL SELECT 1",
                [{"value": 1}, {"value": 1}],
            ),
            (
                "SELECT 1 AS value UNION SELECT 2 INTERSECT SELECT 2",
                [{"value": 1}, {"value": 2}],
            ),
        ]

        error_checks = [
            ("SELECT * FROM ints AS x JOIN doubles AS x ON x.id = x.id", "ALOPEX-C004"),
            ("SELECT amount AS ident FROM sales WHERE ident > 100", "ALOPEX-C003"),
            ("SELECT region AS area, COUNT(*) FROM sales GROUP BY area", "ALOPEX-C003"),
            ("SELECT CASE WHEN TRUE THEN 1 ELSE 'text' END", "type mismatch"),
            ("SELECT id FROM sales UNION SELECT id, region FROM sales", "column count mismatch"),
            ("SELECT id FROM sales UNION SELECT region FROM sales", "type mismatch"),
            ("SELECT LAG(amount) OVER (ORDER BY id) FROM sales", "LAG"),
            ("SELECT LEAD(amount) OVER (ORDER BY id) FROM sales", "LEAD"),
            (
                "SELECT SUM(qty) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM sales",
                "ROWS",
            ),
            (
                "SELECT SUM(qty) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales",
                "RANGE",
            ),
            (
                "WITH RECURSIVE c AS (SELECT 1 AS id) SELECT id FROM c",
                "recursive common table expression",
            ),
            ("WITH defined AS (SELECT 1 AS id) SELECT id FROM missing", "missing"),
        ]

        completed = 0
        for sql, expected in ordered_checks:
            check_ordered(db, sql, expected)
            completed += 1
        for sql, expected in unordered_checks:
            check_unordered(db, sql, expected)
            completed += 1
        for sql, expected_error in error_checks:
            expect_error(db, sql, expected_error)
            completed += 1

        if completed != 53:
            raise AssertionError(f"v0.8 SQL demo check count changed: {completed} != 53")
    finally:
        db.close()

    print("v0.8 SQL correctness demo completed: 53 checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
