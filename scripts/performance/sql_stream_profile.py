#!/usr/bin/env python3
"""Bounded manual SQL stream diagnosis at two explicit result boundaries."""
import argparse
import hashlib
import os
from pathlib import Path
import resource
import sys
import time

from dataframe_profile import profile
import parity_v0811_measure as original_helpers
from sql_delete_profile import digest, emit, require

QUERY = original_helpers.SQL_STREAM_QUERY
OPERATIONS = ("alopex-original", "datafusion-original", "alopex-rows", "datafusion-rows")


def consume_original_alopex(open_stream):
    started = time.perf_counter()
    with open_stream() as stream:
        next(stream)
        first_elapsed = time.perf_counter() - started
        count = 1 + sum(1 for _ in stream)
        return first_elapsed, time.perf_counter() - started, count, 1


def consume_original_datafusion(build):
    started = time.perf_counter()
    stream = iter(build().execute_stream())
    first = next(stream)
    first_elapsed = time.perf_counter() - started
    first_rows = first.to_pyarrow().num_rows
    count = first_rows + sum(
        batch.to_pyarrow().num_rows for batch in stream
    )
    return first_elapsed, time.perf_counter() - started, count, first_rows


def consume_rows_alopex(open_stream):
    started = time.perf_counter()
    with open_stream() as stream:
        first = next(stream)
        first_elapsed = time.perf_counter() - started
        rows = [first, *stream]
        return first_elapsed, time.perf_counter() - started, len(rows), 1, rows


def consume_rows_datafusion(build):
    started = time.perf_counter()
    stream = iter(build().execute_stream())
    first = next(stream).to_pyarrow().to_pylist()
    first_elapsed = time.perf_counter() - started
    first_rows = len(first)
    rows = first
    for batch in stream:
        rows.extend(batch.to_pyarrow().to_pylist())
    return first_elapsed, time.perf_counter() - started, len(rows), first_rows, rows


def canonical_rows(rows, expected):
    require(type(rows) is list, "expected materialized Python rows")
    for row in rows:
        require(type(row) is dict and list(row) == ["id", "value"], "row column order/type")
        require(type(row["id"]) is int and type(row["value"]) is float, "row scalar type")
    actual = sorted(rows, key=lambda row: row["id"])
    require(actual == expected, "complete row values or multiplicity mismatch")
    return actual


def validate_result(result, expected, rows_lane):
    first, total, count, first_count = result[:4]
    require(0 <= first <= total and total > 0, "invalid stream timings")
    require(count == len(expected) and 0 < first_count <= count, "incomplete stream consumption")
    return canonical_rows(result[4], expected) if rows_lane else count


def measure(consume, expected, rows_lane, *, warmups=3, samples=21, record=emit):
    for index in range(warmups + samples):
        usage_before = resource.getrusage(resource.RUSAGE_SELF)
        started = time.perf_counter_ns()
        result = consume()
        elapsed = time.perf_counter_ns() - started
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
        validate_result(result, expected, rows_lane)
        first_ns, total_ns = round(result[0] * 1e9), round(result[1] * 1e9)
        record(dict(event="sample", iteration=index, phase="warmup" if index < warmups else "measure",
                    first_ns=first_ns, remaining_ns=total_ns-first_ns, total_ns=total_ns,
                    outside_original_total_ns=elapsed-total_ns, rows=result[2], first_chunk_rows=result[3],
                    cpu_including_close_seconds=(usage_after.ru_utime + usage_after.ru_stime -
                                                 usage_before.ru_utime - usage_before.ru_stime),
                    input_blocks_including_close=usage_after.ru_inblock-usage_before.ru_inblock,
                    output_blocks_including_close=usage_after.ru_oublock-usage_before.ru_oublock,
                    validation="full typed rows" if rows_lane else "count; full values before/after case"))
        result = None
    return warmups + samples


def file_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", required=True)
    parser.add_argument("--surface", choices=("embedded",), required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=(3, 34250), required=True)
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    require(not os.environ.get("CI") and not os.environ.get("GITHUB_ACTIONS"),
            "manual SQL stream diagnosis forbidden in CI")
    args.work.mkdir(parents=True, exist_ok=False)
    engine, lane = args.operation.split("-")
    expected = [{"id": i, "value": float(i)} for i in range(0, args.rows, 2)]
    connection = None
    if engine == "alopex":
        import alopex
        connection = alopex.Database.new()
        try:
            original_helpers._populate_sql(connection, "alopex", args.rows)
        except BaseException:
            connection.close()
            raise
        open_stream = lambda: connection.execute_sql_stream(QUERY, resource_limit_bytes=1024**3)
        build_plan = lambda: connection.execute_sql(f"EXPLAIN (FORMAT JSON) {QUERY}")
        consume = (lambda: (*consume_original_alopex(open_stream), None)) if lane == "original" else lambda: consume_rows_alopex(open_stream)
        read_all = lambda: consume_rows_alopex(open_stream)[4]
        native = {"alopex": file_hash(alopex._alopex.__file__)}
        versions = {"alopex": "current-source; identified by native hash"}
    else:
        import datafusion
        import pyarrow as pa
        require(datafusion.__version__ == "50.0.0" and pa.__version__ == "26.0.0", "reference version mismatch")
        context = datafusion.SessionContext(datafusion.SessionConfig().with_target_partitions(1))
        batches = [pa.record_batch({"id": range(start, min(start+8192, args.rows)),
                                   "value": [float(i) for i in range(start, min(start+8192, args.rows))],
                                   "enabled": [i % 2 == 0 for i in range(start, min(start+8192, args.rows))]})
                   for start in range(0, args.rows, 8192)]
        context.register_record_batches("bench", [batches])
        build = lambda: context.sql(QUERY)
        build_plan = lambda: build().logical_plan()
        consume = (lambda: (*consume_original_datafusion(build), None)) if lane == "original" else lambda: consume_rows_datafusion(build)
        read_all = lambda: consume_rows_datafusion(build)[4]
        native = {module.__name__ + "/" + str(f.relative_to(Path(module.__file__).parent)): file_hash(f)
                  for module in [datafusion, pa] for f in Path(module.__file__).parent.rglob("*")
                  if f.is_file() and f.suffix in (".so", ".dylib")}
        versions = {"datafusion": datafusion.__version__, "pyarrow": pa.__version__}
    try:
        canonical_rows(read_all(), expected)
        # Preserve the original three warmups / 21 separate plan observations.
        # EXPLAIN JSON and logical_plan are different APIs, not a plan-time product ratio.
        for index in range(24):
            started = time.perf_counter_ns()
            build_plan()
            elapsed = time.perf_counter_ns() - started
            emit(dict(event="plan_sample", phase="warmup" if index < 3 else "measure", total_ns=elapsed))
        emit(dict(event="ready", pid=os.getpid(), surface="embedded", rows=args.rows,
                  operation=args.operation, profile=args.profile, versions=versions, native=native,
                  driver_sha256=file_hash(__file__), helper_sha256=file_hash(original_helpers.__file__),
                  original_function_sha256=file_hash(Path(__file__).parent / "workloads/sql_streaming_v0816.py"),
                  input_sha256=digest([[i, float(i), i % 2 == 0] for i in range(args.rows)]),
                  result_sha256=digest(expected), thread_environment={k: os.environ.get(k) for k in
                      ("RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS")},
                  datafusion_target_partitions=1 if engine == "datafusion" else None))
        rows_lane = lane == "rows"
        iterations = (profile(consume, lambda result: validate_result(result, expected, rows_lane),
                              expected if rows_lane else len(expected), args.work / "client.profile.txt")
                      if args.profile else measure(consume, expected, rows_lane))
        canonical_rows(read_all(), expected)
    finally:
        if connection is not None:
            connection.close()
    usage = resource.getrusage(resource.RUSAGE_SELF)
    emit(dict(event="complete", iterations=iterations, final_rows=len(expected), result_sha256=digest(expected),
              process_peak_rss_bytes=usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss*1024,
              client_cpu_lifetime_seconds=usage.ru_utime+usage.ru_stime, connection_closed=True,
              validation="full typed rows before/after case; each timed sample full rows only in rows lane",
              scope="current-source versus pinned reference diagnosis; not published/release qualification"))


if __name__ == "__main__":
    main()
