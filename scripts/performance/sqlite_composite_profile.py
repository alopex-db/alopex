#!/usr/bin/env python3
"""Manual SQLite composite diagnosis, preserving an explicit original lane."""
import argparse
import cProfile
import json
import os
from pathlib import Path
import pstats
import resource
import time

from sql_delete_profile import digest, emit, require
from sql_stream_profile import file_hash
from workloads import sqlite_composite_v0816 as original

OPERATIONS = ("alopex-original", "sqlite-original", "alopex-checked", "sqlite-checked")
PHASES = ("aggregate", "ordered", "savepoint", "prepared", "constraints")
ORDERED = "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16"


def tuples(actual):
    return [tuple(row.values()) if isinstance(row, dict) else tuple(row) for row in actual]


def validate_rows(actual, expected):
    rows = tuples(actual)
    require(len(rows) == len(expected), "row count mismatch")
    for row, wanted in zip(rows, expected):
        require(len(row) == len(wanted), "column count mismatch")
        require(all(type(a) is type(b) and a == b for a, b in zip(row, wanted)),
                "typed value/order mismatch")


def validate_result(actual, rows):
    require(list(actual) == ["aggregate", "ordered", "prepared"], "result phases differ")
    for phase, columns in [("aggregate", ["count"]), ("ordered", ["id", "value"]), ("prepared", ["id"])]:
        for row in actual[phase]:
            if isinstance(row, dict):
                require(list(row) == columns, "named result columns differ")
    validate_rows(actual["aggregate"], [((rows + 1) // 2,)])
    validate_rows(actual["ordered"], [(i, float(i)) for i in range(min(rows, 16))])
    validate_rows(actual["prepared"], [(0,)])


def savepoint_cycle(connection, engine, rows):
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
        original._sql_script(connection, engine,
            f"BEGIN; INSERT INTO bench VALUES ({rows + 1}, 1.0, true); "
            "SAVEPOINT perf_savepoint; UPDATE bench SET value = 2.0 WHERE id = 0; "
            "ROLLBACK TO perf_savepoint; RELEASE perf_savepoint; ROLLBACK")


def prepared(connection, engine):
    if engine == "alopex":
        statement = connection.prepare("SELECT id FROM bench WHERE id = ?")
        try:
            statement.bind(1, 0)
            return statement.execute()
        finally:
            statement.finalize()
    return connection.execute("SELECT id FROM bench WHERE id = ?", (0,)).fetchall()


def checked_composite(connection, engine, rows, phase_clock=None):
    intervals = {}
    def run(name, operation):
        started = phase_clock() if phase_clock else None
        result = operation()
        if phase_clock:
            intervals[name] = phase_clock() - started
        return result
    query = original.SQL_AGGREGATE_QUERY.replace("true", "1") if engine == "sqlite" else original.SQL_AGGREGATE_QUERY
    result = {}
    result["aggregate"] = run("aggregate", lambda: original._execute(connection, engine, query))
    result["ordered"] = run("ordered", lambda: original._execute(connection, engine, ORDERED))
    run("savepoint", lambda: savepoint_cycle(connection, engine, rows))
    result["prepared"] = run("prepared", lambda: prepared(connection, engine))
    run("constraints", lambda: original._sql_transaction(connection, engine,
        ["INSERT INTO perf_child VALUES (1, 1, 1)"]))
    return result, intervals


def validate_state(connection, engine, rows):
    state = original._execute(connection, engine, "SELECT id, value, enabled FROM bench ORDER BY id")
    expected = [(i, float(i), (i % 2 == 0) if engine == "alopex" else int(i % 2 == 0)) for i in range(rows)]
    validate_rows(state, expected)
    validate_rows(original._execute(connection, engine, "SELECT id FROM perf_parent ORDER BY id"), [(1,)])
    validate_rows(original._execute(connection, engine, "SELECT id, parent_id, quantity FROM perf_child ORDER BY id"), [])
    return digest({"bench": [(a, b, bool(c)) for a, b, c in tuples(state)], "auxiliary": []})


def validate_constraints(connection, engine):
    # Independent negative preflight, outside every timing/profile interval.
    for sql, marker in [("INSERT INTO perf_child VALUES (1, 999, 1)", "foreign key"),
                        ("INSERT INTO perf_child VALUES (1, 1, 0)", "check constraint")]:
        rejected = False
        try:
            original._sql_transaction(connection, engine, [sql])
        except Exception as error:
            require(marker in str(error).lower(), "unexpected constraint-preflight error: " + str(error))
            rejected = True
        require(rejected, "constraint violation was accepted")
        validate_rows(original._execute(connection, engine,
            "SELECT id, parent_id, quantity FROM perf_child ORDER BY id"), [])


def validate_savepoint(connection, engine, rows):
    if engine == "alopex":
        from alopex import TxnMode
        transaction = connection.begin(TxnMode.READ_WRITE)
    else:
        connection.execute("BEGIN")
        transaction = connection
    try:
        original._execute(transaction, engine, f"INSERT INTO bench VALUES ({rows + 1}, 1.0, true)")
        if engine == "alopex":
            transaction.savepoint("check_savepoint")
        else:
            connection.execute("SAVEPOINT check_savepoint")
        original._execute(transaction, engine, "UPDATE bench SET value = 2.0 WHERE id = 0")
        validate_rows(original._execute(transaction, engine, "SELECT id, value FROM bench WHERE id = 0"), [(0, 2.0)])
        if engine == "alopex":
            transaction.rollback_to("check_savepoint")
        else:
            connection.execute("ROLLBACK TO check_savepoint")
        validate_rows(original._execute(transaction, engine, "SELECT id, value FROM bench WHERE id = 0"), [(0, 0.0)])
        validate_rows(original._execute(transaction, engine,
            f"SELECT id, value FROM bench WHERE id = {rows + 1}"), [(rows + 1, 1.0)])
        if engine == "alopex":
            transaction.release("check_savepoint")
        else:
            connection.execute("RELEASE check_savepoint")
    finally:
        transaction.rollback()


def measure(operation, validate, state, *, warmups=3, samples=21,
            clock=time.perf_counter_ns, record=emit):
    for iteration in range(warmups + samples):
        usage = resource.getrusage(resource.RUSAGE_SELF)
        started = clock()
        actual = operation()
        elapsed = clock() - started
        after = resource.getrusage(resource.RUSAGE_SELF)
        validate(actual)
        state()
        record(dict(event="sample", phase="warmup" if iteration < warmups else "measure",
                    iteration=iteration, total_ns=elapsed,
                    operation_cpu_seconds=after.ru_utime + after.ru_stime - usage.ru_utime - usage.ru_stime,
                    operation_input_blocks=after.ru_inblock - usage.ru_inblock,
                    operation_output_blocks=after.ru_oublock - usage.ru_oublock))
        actual = None
    return warmups + samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", required=True)
    parser.add_argument("--surface", choices=("embedded",), required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=(3, 34250), required=True)
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    require(not os.environ.get("CI") and not os.environ.get("GITHUB_ACTIONS"), "manual diagnosis forbidden in CI")
    require(not os.environ.get("ALOPEX_PERF_RAW_PATH"), "original raw logging must be disabled")
    engine, lane = args.operation.split("-")
    require(not args.profile or lane == "checked", "profile only the checked composite")
    args.work.mkdir(parents=True, exist_ok=False)
    if engine == "alopex":
        import alopex
        connection = alopex.Database.new()
        native = {"alopex": file_hash(alopex._alopex.__file__)}
        settings = {"storage": "memory", "native_source": "current; identified by hash"}
    else:
        import pysqlite3 as sqlite3
        require(sqlite3.sqlite_version == "3.46.1", "SQLite version mismatch")
        connection = sqlite3.connect(":memory:", isolation_level=None)
        initial_fk = connection.execute("PRAGMA foreign_keys").fetchone()[0]
        if lane == "checked":
            connection.execute("PRAGMA foreign_keys=ON")
        settings = dict(version=sqlite3.sqlite_version, source_id=connection.execute("SELECT sqlite_source_id()").fetchone()[0],
                        storage="memory", initial_foreign_keys=initial_fk,
                        foreign_keys=connection.execute("PRAGMA foreign_keys").fetchone()[0],
                        compile_options=[row[0] for row in connection.execute("PRAGMA compile_options")],
                        isolation_level=connection.isolation_level)
        require(lane != "checked" or settings["foreign_keys"] == 1, "foreign keys must be enforced")
        native = {"pysqlite3": file_hash(sqlite3._sqlite3.__file__)}
    try:
        original._populate_sql(connection, engine, args.rows)
        original._prepare_sql_workload(connection, engine, "sql-sqlite-curated")
        initial = validate_state(connection, engine, args.rows)
        require(initial == original._validate_sql_state(connection, engine, "sql-sqlite-curated", args.rows),
                "original and typed state digests differ")
        direct = checked_composite(connection, engine, args.rows)[0]
        validate_result(direct, args.rows)
        direct = None
        if lane == "checked":
            validate_constraints(connection, engine)
            validate_savepoint(connection, engine, args.rows)
        state = lambda: require(validate_state(connection, engine, args.rows) == initial, "fixture/rollback changed")
        operation = (lambda: original._sql_workload(connection, engine, "sql-sqlite-curated", args.rows)) if lane == "original" else (
            lambda: checked_composite(connection, engine, args.rows)[0])
        validate = (lambda actual: require(actual is None, "original workload return changed")) if lane == "original" else (
            lambda actual: validate_result(actual, args.rows))
        query = original.SQL_AGGREGATE_QUERY.replace("true", "1") if engine == "sqlite" else original.SQL_AGGREGATE_QUERY
        for iteration in range(24):
            started = time.perf_counter_ns()
            original._execute(connection, engine, ("EXPLAIN QUERY PLAN " if engine == "sqlite" else "EXPLAIN (FORMAT JSON) ") + query)
            elapsed = time.perf_counter_ns() - started
            emit(dict(event="plan_sample", phase="warmup" if iteration < 3 else "measure", total_ns=elapsed))
        emit(dict(event="ready", pid=os.getpid(), rows=args.rows, surface="embedded", operation=args.operation,
                  settings=settings, native=native, driver_sha256=file_hash(__file__),
                  original_sha256=file_hash(original.__file__), input_sha256=digest([(i, float(i), i % 2 == 0) for i in range(args.rows)]),
                  result_sha256=initial, thread_environment={k: os.environ.get(k) for k in ("RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS")}))
        if args.profile:
            profiler = cProfile.Profile()
            time.sleep(1)
            started = time.monotonic()
            iterations = 0
            while True:
                actual = profiler.runcall(operation)
                iterations += 1
                if time.monotonic() - started >= 4:
                    break
                actual = None
            validate(actual)
            actual = None
            state()
            with (args.work / "client.profile.txt").open("x") as stream:
                pstats.Stats(profiler, stream=stream).sort_stats("cumulative").print_stats()
        else:
            iterations = measure(operation, validate, state)
            if lane == "checked":
                for iteration in range(24):
                    result, intervals = checked_composite(connection, engine, args.rows, time.perf_counter_ns)
                    validate_result(result, args.rows)
                    state()
                    emit(dict(event="phase_sample", phase="warmup" if iteration < 3 else "measure", intervals_ns=intervals))
                    result = None
        validate_result(checked_composite(connection, engine, args.rows)[0], args.rows)
        state()
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        connection.close()
        connection = None
        emit(dict(event="complete", iterations=iterations, final_rows=args.rows, result_sha256=initial,
                  connection_closed=True, process_peak_rss_bytes=peak,
                  validation="typed full fixture after every timed sample; checked lane validates actual SELECT results and negative constraints; original lane retains original discarded query returns",
                  scope="manual current-source diagnostic; original lane may lack SQLite FK enforcement; no release qualification"))
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
