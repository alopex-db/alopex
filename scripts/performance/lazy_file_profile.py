#!/usr/bin/env python3
"""Manual diagnosis of the pinned CSV expressions and numeric Parquet guards."""
import argparse
import hashlib
import os
from pathlib import Path
import resource
import sys

from dataframe_profile import CurrentOperations, measure, profile
from sql_delete_profile import emit, require

# The archived module's absolute sibling import is preserved byte-for-byte.
sys.path.insert(0, str(Path(__file__).parent / "workloads"))
import polars_entrypoints_v0816 as workload

OPERATIONS = workload.DEFAULT_CASES


def prepare(operations, case, fixture):
    if case not in OPERATIONS:
        raise ValueError("unknown or excluded lazy-file case")
    extension, *_, mode = case.split("-")
    scan = getattr(operations.api.LazyFrame, f"scan_{extension}")
    expected = workload.oracle(workload.inputs(operations.rows))["result"]
    if "-numeric-" in case:
        expected.pop("joined")

    def operation():
        source = operations.next_plan(scan(str(fixture)))
        plan = (source.select(*operations.expressions()[:-1]) if "-numeric-" in case
                else operations.select_plan(source))
        if mode == "collect":
            return [plan.collect()]
        with plan.collect_batches(chunk_size=8192) as stream:
            return list(stream)

    def snapshot(batches):
        actual = {name: [] for name in expected}
        for batch in batches:
            values = batch.to_dict(as_series=False)
            require(list(values) == list(expected), "batch column order mismatch")
            for name, column in values.items():
                actual[name].extend(column)
        return actual

    return operation, snapshot, expected


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
            "manual lazy-file diagnosis forbidden in CI")
    args.work.mkdir(parents=True, exist_ok=False)
    import alopex
    directory = Path(os.environ["ALOPEX_PERF_INPUT_DIRECTORY"]) / str(args.rows)
    fixture = directory / ("rows." + args.operation.split("-")[0])
    input_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()  # Warm outside timing.
    operations = CurrentOperations(alopex, args.rows)
    operation, snapshot, expected = prepare(operations, args.operation, fixture)
    actual = operation()
    workload.assert_equal(snapshot(actual), expected)
    actual = None
    emit(dict(event="ready", pid=os.getpid(), surface="embedded", rows=args.rows,
              operation=args.operation, profile=args.profile, input_sha256=input_hash,
              native_sha256=hashlib.sha256(Path(alopex._alopex.__file__).read_bytes()).hexdigest(),
              driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              workload_sha256=hashlib.sha256(Path(workload.__file__).read_bytes()).hexdigest(),
              thread_environment={k: os.environ.get(k) for k in
                                  ("RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS")},
              setup="fixed warmed file; scan/plan/load/full collect inside timer; conversion/oracle/disposal outside"))
    iterations = (profile(operation, snapshot, expected, args.work / "client.profile.txt")
                  if args.profile else measure(operation, snapshot, expected))
    require(hashlib.sha256(fixture.read_bytes()).hexdigest() == input_hash,
            "input fixture changed")
    usage = resource.getrusage(resource.RUSAGE_SELF)
    emit(dict(event="complete", iterations=iterations, final_rows=args.rows,
              input_sha256=input_hash, result_sha256=workload._hash(expected),
              client_cpu_lifetime_seconds=usage.ru_utime + usage.ru_stime,
              # macOS ru_maxrss is bytes, unlike the archived Linux runner.
              process_peak_rss_bytes=usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024,
              validation="timed: every warmup/sample; profile: preflight and last; all values/types/NULL/order",
              scope="current-source internal comparison; not published/reference qualification"))


if __name__ == "__main__":
    main()
