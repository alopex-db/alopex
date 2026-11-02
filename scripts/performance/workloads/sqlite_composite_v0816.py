"""Frozen SQL helper bodies from the corrected original v0.8.16 harness.

Source SHA256: 7ba8a8e9da92987d2f0dbdc6ef3d98f2a12e5c4dc087651ae279a1dd6fbf1800
Used only for the explicit original-contract diagnostic lane.
"""
import hashlib
import json
import os
from pathlib import Path

SQL_AGGREGATE_QUERY = "SELECT COUNT(*) AS count FROM bench WHERE enabled = true"


def _record_raw(record: dict[str, object]) -> None:
    path = os.environ.get("ALOPEX_PERF_RAW_PATH")
    if path:
        with Path(path).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
            stream.flush()


def _result_checksum(result) -> str:
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"))
    checksum = hashlib.sha256(encoded.encode()).hexdigest()
    _record_raw({"phase": "validation", "result_sha256": checksum})
    return checksum


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


def _execute(connection, engine: str, sql: str):
    if engine == "alopex":
        return connection.execute_sql(sql)
    cursor = connection.execute(sql)
    return cursor.fetchall() if cursor.description else None


def _sql_script(connection, engine: str, sql: str) -> None:
    if engine == "alopex":
        connection.execute_sql(sql)
    elif engine == "sqlite":
        connection.executescript(sql)
    else:
        cursor = connection.execute(sql)
        while True:
            if cursor.description:
                cursor.fetchall()
            if not cursor.nextset():
                break


def _sql_transaction(connection, engine: str, statements: list[str]) -> None:
    if engine == "alopex":
        from alopex import TxnMode

        transaction = connection.begin(TxnMode.READ_WRITE)
        try:
            for statement in statements:
                _execute(transaction, engine, statement)
        finally:
            transaction.rollback()
        return
    connection.execute("BEGIN")
    try:
        for statement in statements:
            _execute(connection, engine, statement)
    finally:
        connection.rollback()


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
    execute = lambda sql: _execute(connection, engine, sql)
    if workload == "sql-sqlite-curated":
        query = SQL_AGGREGATE_QUERY.replace("true", "1") if engine == "sqlite" else SQL_AGGREGATE_QUERY
        execute(query)
        execute("SELECT id, COALESCE(value, 0) FROM bench WHERE id >= 0 ORDER BY id LIMIT 16")
        if engine == "alopex":
            from alopex import TxnMode

            transaction = connection.begin(TxnMode.READ_WRITE)
            try:
                transaction.execute_sql(f"INSERT INTO bench VALUES ({rows + 1}, 1.0, true)")
                transaction.savepoint("perf_savepoint")
                transaction.execute_sql("UPDATE bench SET value = 2.0 WHERE id = 0")
                transaction.rollback_to("perf_savepoint")
                transaction.release("perf_savepoint")
            finally:
                transaction.rollback()
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


def _validate_sql_state(connection, engine: str, workload: str, rows: int) -> str:
    def tuples(sql):
        result = _execute(connection, engine, sql)
        return [tuple(row.values()) if isinstance(row, dict) else tuple(row) for row in result]

    state = tuples("SELECT id, value, enabled FROM bench ORDER BY id")
    expected = [(index, float(index), index % 2 == 0) for index in range(rows)]
    count_query = SQL_AGGREGATE_QUERY.replace("true", "1") if engine == "sqlite" else SQL_AGGREGATE_QUERY
    if state != expected or tuples(count_query) != [((rows + 1) // 2,)]:
        raise RuntimeError(f"{engine} bench result or rollback state differs from fixture")
    if workload == "sql-sqlite-curated":
        auxiliary = tuples("SELECT id, parent_id, quantity FROM perf_child ORDER BY id")
        if auxiliary:
            raise RuntimeError(f"{engine} child insert was not rolled back")
    else:
        auxiliary = tuples("SELECT id, value FROM perf_items ORDER BY id")
        if auxiliary != [(1, "old")]:
            raise RuntimeError(f"{engine} DML was not rolled back")
        if tuples("SELECT id, value FROM perf_source ORDER BY id") != [(1, "new"), (2, "second")]:
            raise RuntimeError(f"{engine} source fixture changed")
    # Normalize SQLite's integer booleans before comparing cross-engine hashes.
    return _result_checksum({"bench": [(a, float(b), bool(c)) for a, b, c in state],
                             "auxiliary": auxiliary})
