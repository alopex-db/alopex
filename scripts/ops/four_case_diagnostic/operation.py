"""Four-case SQL operation adapter for an already identity-qualified harness.

The caller must pin and verify the exact harness/module and product assets before
calling this module. This module deliberately contains no asset acquisition or
fallback source identity.
"""
from . import oracle


ORDERED_SQL = ("SELECT id, COALESCE(value, 0) AS value FROM bench "
               "WHERE id >= 0 ORDER BY id LIMIT 16")
INSERT_SQL = "INSERT INTO perf_items VALUES (3, 'returning') RETURNING id"
ROLLBACK_SQL = "SELECT id, value FROM perf_items ORDER BY id"


def execute(connection, sql):
    return connection.execute_sql(sql) or []


def prepare(case, module, connection, traced_setup, *, txn_mode=None):
    """Create one shared fixture; traced_setup wraps each setup SQL request."""
    if case in ("aggregate", "ordered", "select-all"):
        traced_setup(lambda: execute(connection, "DROP TABLE IF EXISTS bench"))

        class SetupConnection:
            def execute_sql(self, sql):
                return traced_setup(lambda: execute(connection, sql))

        module._populate_sql(SetupConnection(), "alopex", oracle.ROWS)
        sql = {"aggregate": module.SQL_AGGREGATE_QUERY,
               "ordered": ORDERED_SQL,
               "select-all": module.SQL_STREAM_QUERY}[case]
        return lambda target: execute(target, sql)
    if case == "insert-rollback":
        if txn_mode is None:
            raise ValueError("TXN_MODE")
        for sql in ("DROP TABLE IF EXISTS perf_items",
                    "CREATE TABLE perf_items (id INTEGER PRIMARY KEY, value TEXT)",
                    "INSERT INTO perf_items VALUES (1, 'old')"):
            traced_setup(lambda sql=sql: execute(connection, sql))

        def insert(target):
            transaction = target.begin(txn_mode)
            try:
                return execute(transaction, INSERT_SQL)
            finally:
                transaction.rollback()

        return insert
    raise ValueError("CASE")


def postcheck(case, connection):
    if case != "insert-rollback":
        raise ValueError("POSTCHECK")
    value = execute(connection, ROLLBACK_SQL)
    oracle.rollback(value)
    return value
