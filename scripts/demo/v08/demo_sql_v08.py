#!/usr/bin/env python3
"""Exercise SQL correctness shipped across the v0.8 release line."""

from __future__ import annotations

import sys
from typing import Any


def show(sql: str, result: Any) -> None:
    print(f"call> db.execute_sql({sql!r})")
    print(f"   -> {result!r}")


def main() -> int:
    from alopex import Database

    db = Database.new()
    try:
        statements = [
            "CREATE TABLE metrics (id INTEGER PRIMARY KEY, n INTEGER, f FLOAT, ts TIMESTAMP)",
            "INSERT INTO metrics VALUES (1, 2, 1.5, '2025-01-15 10:30:00')",
            "INSERT INTO metrics VALUES (2, 3, 2.5, '2025-01-15 10:30:01.25')",
        ]
        for sql in statements:
            show(sql, db.execute_sql(sql))

        checks = [
            (
                "SELECT SUM(n) AS total FROM metrics",
                [{"total": 5}],
            ),
            (
                "SELECT id, n * 2.0 AS doubled FROM metrics ORDER BY id",
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
        ]
        for sql, expected in checks:
            actual = db.execute_sql(sql)
            show(sql, actual)
            if actual != expected:
                raise AssertionError(f"unexpected v0.8 SQL result: {actual!r} != {expected!r}")

        for sql in [
            "CREATE TABLE ints (id INTEGER PRIMARY KEY)",
            "CREATE TABLE doubles (id DOUBLE PRIMARY KEY)",
            "INSERT INTO ints VALUES (1)",
            "INSERT INTO doubles VALUES (1.0)",
        ]:
            show(sql, db.execute_sql(sql))
        sql = "SELECT ints.id FROM ints JOIN doubles ON ints.id = doubles.id"
        rows = db.execute_sql(sql)
        show(sql, rows)
        if rows != [{"id": 1}]:
            raise AssertionError(f"mixed numeric join did not match: {rows!r}")

        invalid = "SELECT * FROM ints AS x JOIN doubles AS x ON x.id = x.id"
        try:
            db.execute_sql(invalid)
        except Exception as exc:  # AlopexError is an extension-defined Exception.
            print(f"call> db.execute_sql({invalid!r})")
            print(f"   -> ERROR {type(exc).__name__}: {exc}")
        else:
            raise AssertionError("duplicate range-variable alias was accepted")
    finally:
        db.close()

    print("v0.8 SQL correctness demo completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
