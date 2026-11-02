#!/usr/bin/env python3
"""Isolated original SQL cases for #712, with full Python result validation."""
import argparse
import cProfile
import hashlib
import json
import os
from pathlib import Path
import pstats
import resource
import time

from sql_delete_profile import digest, emit, require, start_server, stop, validate_rows

OPERATIONS = ("aggregate", "ordered", "select-all", "insert-rollback")
FIXTURE = Path(__file__).parent / "fixtures/sql-v0.8.11-curated.json"
QUERIES = json.loads(FIXTURE.read_text())["queries"]
SQL = {
    "aggregate": QUERIES["aggregate"],
    "ordered": "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16",
    "select-all": QUERIES["stream"],
    "insert-rollback": "INSERT INTO perf_items VALUES (3,'returning') RETURNING id",
}


def fixture_rows(operation, rows):
    return 1 if operation == "insert-rollback" else rows


def expected_result(operation, rows):
    if operation == "aggregate":
        return [dict(count=(rows+1)//2)]
    if operation == "ordered":
        return [dict(id=i, value=float(i)) for i in range(min(rows, 16))]
    if operation == "select-all":
        return [dict(id=i, value=float(i)) for i in range(0, rows, 2)]
    return [dict(id=3)]


def validate_result(actual, operation, expected):
    # Only the original unordered SELECT permits sorting, outside timing.
    if operation == "select-all":
        actual = sorted(actual, key=lambda row: row["id"])
    validate_rows(actual, expected)


def timed_operation(client, mode, operation, clock=time.perf_counter_ns):
    sql = SQL[operation]
    started = clock()
    if operation == "insert-rollback":
        transaction = client.begin(mode)
        begun = clock()
        try:
            result = transaction.execute_sql(sql)
            executed = clock()
        finally:
            try:
                transaction.rollback()
                rolled_back = clock()
            finally:
                del transaction
        finished = clock()
        return result, dict(begin_ns=begun-started, execute_ns=executed-begun,
                            rollback_ns=rolled_back-executed, release_ns=finished-rolled_back,
                            total_ns=finished-started)
    result = client.execute_sql(sql)
    finished = clock()
    return result, dict(execute_ns=finished-started, total_ns=finished-started)


def populate(client, operation, rows):
    if operation == "insert-rollback":
        client.execute_sql("CREATE TABLE perf_items (id INTEGER PRIMARY KEY, value TEXT)")
        client.execute_sql("INSERT INTO perf_items VALUES (1,'old')")
        return
    client.execute_sql("CREATE TABLE bench (id INTEGER PRIMARY KEY, value DOUBLE, enabled BOOLEAN)")
    # Preserve the original fixture's 100-row autocommit batches outside timing.
    for start in range(0, rows, 100):
        values = ",".join(f"({i},{float(i)},{str(i % 2 == 0).lower()})"
                          for i in range(start, min(start+100, rows)))
        client.execute_sql("INSERT INTO bench VALUES "+values)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", required=True)
    parser.add_argument("--surface", choices=("embedded", "server"), required=True)
    parser.add_argument("--server-bin", type=Path)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=(1, 3, 34250), required=True)
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    require(not os.environ.get("CI") and not os.environ.get("GITHUB_ACTIONS"), "manual diagnosis forbidden in CI")
    require(args.rows == fixture_rows(args.operation, args.rows), "rollback fixture must have exactly one row")
    require(args.operation == "insert-rollback" or args.rows in (3, 34250), "read fixture must have 3 or 34250 rows")
    require(args.surface != "server" or args.server_bin is not None, "server binary required")
    args.work.mkdir(parents=True, exist_ok=False)
    import alopex
    server = None
    client = None
    profiler = cProfile.Profile() if args.profile else None
    try:
        if args.surface == "server":
            server, url = start_server(args.server_bin.resolve(), args.work)
            client = alopex.connect(url, keep_alive=True, timeout=30)
            require(isinstance(client, alopex.RemoteDatabase), "embedded fallback forbidden")
        else:
            client = alopex.Database.open(str(args.work / "database"))
        emit(dict(event="started", pid=os.getpid(), server_pid=server.pid if server else None))
        populate(client, args.operation, args.rows)
        expected = expected_result(args.operation, args.rows)
        result, _ = timed_operation(client, alopex.TxnMode.READ_WRITE, args.operation)
        validate_result(result, args.operation, expected)  # Original one-operation preflight.
        emit(dict(event="ready", pid=os.getpid(), server_pid=server.pid if server else None,
                  surface=args.surface, rows=args.rows, operation=args.operation, profile=args.profile,
                  sql=SQL[args.operation], expected_result_sha256=digest(expected),
                  native_sha256=hashlib.sha256(Path(alopex._alopex.__file__).read_bytes()).hexdigest(),
                  driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  fixture_sha256=hashlib.sha256(FIXTURE.read_bytes()).hexdigest()))
        if args.profile:
            time.sleep(1)
        start = time.monotonic()
        iteration = 0
        while True:
            if profiler:
                result, intervals = profiler.runcall(timed_operation, client, alopex.TxnMode.READ_WRITE, args.operation)
            else:
                result, intervals = timed_operation(client, alopex.TxnMode.READ_WRITE, args.operation)
                emit(dict(event="sample", iteration=iteration, phase="warmup" if iteration < 3 else "measure", **intervals))
            validate_result(result, args.operation, expected)
            iteration += 1
            if (args.profile and time.monotonic()-start >= 4) or (not args.profile and iteration == 24):
                break
        if profiler:
            with (args.work / "client.profile.txt").open("w") as stream:
                pstats.Stats(profiler, stream=stream).sort_stats("cumulative").print_stats()
        client.close()
        del client
        client = None
        client = (alopex.connect(url, keep_alive=True, timeout=30) if args.surface == "server" else
                  alopex.Database.open(str(args.work / "database")))
        if args.operation == "insert-rollback":
            final = client.execute_sql("SELECT id, value FROM perf_items ORDER BY id")
            wanted = [dict(id=1, value="old")]
        else:
            final = client.execute_sql("SELECT id, value, enabled FROM bench ORDER BY id")
            wanted = [dict(id=i, value=float(i), enabled=i % 2 == 0) for i in range(args.rows)]
        validate_rows(final, wanted)
        emit(dict(event="complete", iterations=iteration, final_rows=len(final), result_sha256=digest(final),
                  client_cpu_lifetime_seconds=sum(resource.getrusage(resource.RUSAGE_SELF)[:2]),
                  validation="every full result with Python types and complete unchanged state after reconnect/reopen",
                  scope="manual internal diagnosis; no published-version/reference comparison; profile timings not measured latency"))
    finally:
        try:
            if client is not None:
                client.close()
        finally:
            if server is not None:
                stop(server)
                emit(dict(event="server_stopped", returncode=server.returncode,
                          server_cpu_lifetime_seconds=sum(resource.getrusage(resource.RUSAGE_CHILDREN)[:2])))


if __name__ == "__main__":
    main()
