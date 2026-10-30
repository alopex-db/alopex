#!/usr/bin/env python3
"""Manual full-consumption diagnosis of the original plain CSV streaming case."""
import argparse
import importlib
import importlib.metadata
import os
from pathlib import Path
import resource
import sys
import time

from dataframe_profile import profile
from sql_delete_profile import digest, emit, require
from sql_stream_profile import file_hash, measure, validate_result

OPERATIONS = ("alopex", "polars")


def consume_alopex(build):
    started = time.perf_counter()
    with build().collect_batches(chunk_size=8192) as stream:
        first = next(stream)
        first_elapsed = time.perf_counter() - started
        count = first.height + sum(batch.height for batch in stream)
        return first_elapsed, time.perf_counter() - started, count, first.height


def consume_polars(build):
    started = time.perf_counter()
    batches = iter(build().collect_batches(chunk_size=8192))
    first = next(batches)
    first_elapsed = time.perf_counter() - started
    count = first.height + sum(batch.height for batch in batches)
    return first_elapsed, time.perf_counter() - started, count, first.height


def oracle(rows):
    return {"id": list(range(rows)),
            "label": [None if i % 10 == 0 else f"row-{i}" for i in range(rows)]}


def validate_values(actual, expected):
    require(type(actual) is dict and list(actual) == ["id", "label"], "column order/type")
    for name in expected:
        require(type(actual[name]) is list and len(actual[name]) == len(expected[name]), "column length/type")
        require(all(type(a) is type(b) and a == b for a, b in zip(actual[name], expected[name])),
                "CSV values/types/NULL/order mismatch")
    return digest(actual)


def read_stream(build):
    stream = build().collect_batches(chunk_size=8192)
    result = {"id": [], "label": []}
    try:
        for batch in stream:
            columns = batch.to_dict(as_series=False)
            require(list(columns) == list(result), "batch column order mismatch")
            for name, values in columns.items():
                result[name].extend(values)
    finally:
        # This explicit validation stream is outside the original timed consumer.
        # Close the validation iterator when the engine exposes a close hook.
        close = getattr(stream, "close", None)
        if close is not None:
            close()
    return result


def observe_phase(operation, event, *, warmups=3, samples=21, record=emit):
    for index in range(warmups + samples):
        started = time.perf_counter()
        operation()  # Preserve disposal before the end clock, as original _samples.
        elapsed = time.perf_counter() - started
        record(dict(event=event, phase="warmup" if index < warmups else "measure",
                    total_ns=round(elapsed*1e9)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", required=True)
    parser.add_argument("--surface", choices=("embedded",), required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=(3, 34250), required=True)
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    require(not os.environ.get("CI") and not os.environ.get("GITHUB_ACTIONS"), "manual CSV diagnosis forbidden in CI")
    args.work.mkdir(parents=True, exist_ok=False)
    fixture = Path(os.environ["ALOPEX_PERF_INPUT_DIRECTORY"]) / str(args.rows) / "rows.csv"
    input_hash = file_hash(fixture)  # Same warmed immutable input, outside timing.
    expected = oracle(args.rows)
    if args.operation == "alopex":
        import alopex
        build = lambda: alopex.LazyFrame.scan_csv(str(fixture))
        consume = lambda: (*consume_alopex(build), None)
        native = {"alopex": file_hash(alopex._alopex.__file__)}
        versions = {"alopex": "current-source; identified by native hash"}
        pool = None
    else:
        require(os.environ.get("POLARS_MAX_THREADS") == "1", "set POLARS_MAX_THREADS=1 before reference import")
        import polars as pl
        runtime = importlib.import_module("_polars_runtime_32")
        require(pl.__version__ == "1.43.2" and importlib.metadata.version("polars-runtime-32") == "1.43.2", "reference version mismatch")
        pool = pl.thread_pool_size()
        require(pool == 1, "reference worker count mismatch")
        build = lambda: pl.scan_csv(fixture)
        consume = lambda: (*consume_polars(build), None)
        native = {str(p.relative_to(Path(runtime.__file__).parent.parent)): file_hash(p)
                  for p in Path(runtime.__file__).parent.rglob("*") if p.is_file() and p.suffix in (".so", ".dylib")}
        require(native, "missing reference native identity")
        versions = {"polars": pl.__version__, "polars-runtime-32": importlib.metadata.version("polars-runtime-32")}

    def validate_all():
        validate_values(build().collect().to_dict(as_series=False), expected)
        validate_values(read_stream(build), expected)

    validate_all()
    observe_phase(build, "plan_sample")
    observe_phase(lambda: build().collect().height, "collect_sample")
    emit(dict(event="ready", pid=os.getpid(), operation=args.operation, surface="embedded", rows=args.rows,
              profile=args.profile, versions=versions, native=native, polars_thread_pool_size=pool,
              input_sha256=input_hash, result_sha256=digest(expected), driver_sha256=file_hash(__file__),
              original_function_sha256=file_hash(Path(__file__).parent / "workloads/csv_streaming_v0816.py"),
              thread_environment={k: os.environ.get(k) for k in ("RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "POLARS_MAX_THREADS")}))
    iterations = (profile(consume, lambda result: validate_result(result, expected["id"], False),
                          args.rows, args.work / "client.profile.txt") if args.profile else
                  measure(consume, expected["id"], False))
    validate_all()
    require(file_hash(fixture) == input_hash, "fixture changed")
    usage = resource.getrusage(resource.RUSAGE_SELF)
    emit(dict(event="complete", iterations=iterations, final_rows=args.rows, input_sha256=input_hash,
              result_sha256=digest(expected), process_peak_rss_bytes=usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss*1024,
              client_cpu_lifetime_seconds=usage.ru_utime+usage.ru_stime,
              validation="count every timed sample, full typed/NULL/ordered collect and stream values before/after; profile preflight/final count",
              scope="diagnostic current test build/reference wheels; not published v0.8.16 or release qualification"))


if __name__ == "__main__":
    main()
