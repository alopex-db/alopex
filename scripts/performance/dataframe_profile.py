#!/usr/bin/env python3
"""Manual current-source diagnosis of the pinned #712 DataFrame phase workload."""
import argparse
import cProfile
import hashlib
import os
from pathlib import Path
import pstats
import resource
import time

from sql_delete_profile import emit, require
from workloads import dataframe_phases_v0816 as workload

OPERATIONS = workload.CASES


class CurrentOperations(workload.Operations):
    def __init__(self, api, rows):
        # The archived runner's version gate intentionally stays unchanged. This
        # adapter uses an explicitly hashed current native package, not v0.8.16.
        self.api = api
        self.rows = rows


def prepare(operations, case, columns):
    if case not in OPERATIONS:
        raise ValueError("unknown DataFrame phase")
    frame = operations.frame(columns) if case in ("compute", "convert") else None
    result = operations.compute(frame) if case == "convert" else None

    def operation():
        if case == "construct":
            return operations.frame(columns)
        if case == "compute":
            return operations.compute(frame)
        if case == "convert":
            return result.to_dict(as_series=False)
        complete_columns = {
            "id": columns["id"], "label": columns["label"],
            "items": [[str(i), str(i + 1)] for i in range(len(columns["id"]))],
        }
        return operations.snapshot(operations.frame(complete_columns))

    def snapshot(actual):
        if case == "construct":
            return operations.snapshot(actual)
        if case == "compute":
            return operations.snapshot(frame, result=actual)
        if case == "convert":
            return operations.snapshot(frame, result=result, converted=actual)
        return actual

    return operation, snapshot


def measure(operation, snapshot, expected, *, warmups=3, samples=21,
            clock=time.perf_counter_ns, record=emit):
    for iteration in range(warmups + samples):
        usage_before = resource.getrusage(resource.RUSAGE_SELF)
        started = clock()
        actual = operation()
        elapsed = clock() - started
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
        record(dict(event="sample", iteration=iteration,
                    phase="warmup" if iteration < warmups else "measure", total_ns=elapsed,
                    operation_cpu_seconds=(usage_after.ru_utime + usage_after.ru_stime -
                                           usage_before.ru_utime - usage_before.ru_stime),
                    operation_input_blocks=usage_after.ru_inblock - usage_before.ru_inblock,
                    operation_output_blocks=usage_after.ru_oublock - usage_before.ru_oublock))
        workload.assert_equal(snapshot(actual), expected)
        # Both the result and validation temporaries must be gone before the next timer.
        actual = None
    return warmups + samples


def profile(operation, snapshot, expected, destination):
    profiler = cProfile.Profile()
    time.sleep(1)  # Allow the supervisor's native sampler to start after ready.
    started = time.monotonic()
    iterations = 0
    while True:
        actual = profiler.runcall(operation)
        iterations += 1
        if time.monotonic() - started >= 4:
            break
        actual = None
    # The preflight and this last output are validated. Unlike timed samples,
    # intermediate profiling outputs are not validated in the sampling window.
    workload.assert_equal(snapshot(actual), expected)
    with destination.open("x") as stream:
        pstats.Stats(profiler, stream=stream).sort_stats("cumulative").print_stats()
    return iterations


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
            "manual DataFrame diagnosis forbidden in CI")
    args.work.mkdir(parents=True, exist_ok=False)
    import alopex
    columns = workload.inputs(args.rows)
    expected = workload.oracle(columns)
    operations = CurrentOperations(alopex, args.rows)
    operation, snapshot = prepare(operations, args.operation, columns)
    actual = operation()
    workload.assert_equal(snapshot(actual), expected)
    actual = None
    emit(dict(event="ready", pid=os.getpid(), surface="embedded", rows=args.rows,
              operation=args.operation, profile=args.profile,
              input_sha256=workload._hash(columns),
              native_sha256=hashlib.sha256(Path(alopex._alopex.__file__).read_bytes()).hexdigest(),
              driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              workload_sha256=hashlib.sha256(Path(workload.__file__).read_bytes()).hexdigest(),
              thread_environment={k: os.environ.get(k) for k in
                                  ("RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS")},
              setup="Python inputs/oracle outside timers; frame preconstructed for compute/convert; result precomputed for convert"))
    iterations = (profile(operation, snapshot, expected, args.work / "client.profile.txt")
                  if args.profile else measure(operation, snapshot, expected))
    emit(dict(event="complete", iterations=iterations, final_rows=args.rows,
              input_sha256=workload._hash(columns), result_sha256=workload._hash(expected),
              client_cpu_lifetime_seconds=sum(resource.getrusage(resource.RUSAGE_SELF)[:2]),
              validation="profile: preflight and last output; timed: preflight and every warmup/sample; full values/types/NULL/order and original full snapshot",
              scope="current-source internal diagnosis only; no reference/cross-version ratio; native profile includes result disposal but not full snapshot validation in loop"))


if __name__ == "__main__":
    main()
