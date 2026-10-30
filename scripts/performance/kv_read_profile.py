#!/usr/bin/env python3
"""Manual public KV read diagnosis with the fixed #712 byte/transaction workload."""
import argparse
import cProfile
import hashlib
import json
import os
from pathlib import Path
import pstats
import random
import resource
import time

from sql_delete_profile import emit, require

OPERATIONS = ("begin-rollback", "random-get-hit", "random-get-miss", "range-scan")


def key_for(index):
    return b"k:" + index.to_bytes(8, "big")


def value_for(index):
    return index.to_bytes(8, "little") * 4


def plans(operation, rows, count=24):
    # Same seed, key requests and range bounds as docs/write_kv_native.py.
    rng = random.Random(7)
    if operation == "range-scan":
        result = []
        for _ in range(count):
            start = rng.randrange(rows-min(rows, 1000)+1)
            result.append([(key_for(start), key_for(start+min(rows, 1000)))])
        return result
    return [[(key_for(rng.randrange(rows)+(rows if operation == "random-get-miss" else 0)), b"")
             for _ in range(100)] for _ in range(count)]


def wanted(operation, requests, rows):
    if operation == "begin-rollback":
        return []
    if operation == "random-get-miss":
        return [None] * 100
    if operation == "random-get-hit":
        return [value_for(int.from_bytes(key[2:], "big")) for key, _ in requests]
    start, end = requests[0]
    return [(key_for(i), value_for(i)) for i in range(rows) if start <= key_for(i) < end]


def validate(actual, expected):
    require(type(actual) is list and len(actual) == len(expected), "KV result count/type mismatch")
    for a, b in zip(actual, expected):
        require(type(a) is type(b) and a == b, "KV value/type mismatch")
        if isinstance(b, tuple):
            require(all(type(v) is bytes for v in a), "KV range must return copied bytes")


def result_digest(rows):
    return hashlib.sha256(b"".join(key+value for key, value in rows)).hexdigest()


def timed_operation(client, mode, operation, requests, clock=time.perf_counter_ns):
    started = clock()
    result = []
    if operation == "range-scan":
        txn = client.begin(mode)
        try:
            result = list(txn.scan_range(*requests[0]))
        finally:
            txn.rollback()
    else:
        for key, _ in requests:
            txn = client.begin(mode)
            try:
                if operation != "begin-rollback":
                    result.append(txn.get(key))
            finally:
                txn.rollback()
    finished = clock()
    # Match the original boundary: last handle destruction is outside timing;
    # every session's rollback is inside it. No per-get timing instrumentation.
    return result, dict(total_ns=finished-started, execute_ns=finished-started)


def populate(client, mode, rows):
    txn = client.begin(mode)
    try:
        for i in range(rows):
            txn.put(key_for(i), value_for(i))
        txn.commit()
    except BaseException:
        txn.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", required=True)
    parser.add_argument("--surface", choices=("embedded",), required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=(3, 10000), required=True)
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    require(not os.environ.get("CI") and not os.environ.get("GITHUB_ACTIONS"), "manual KV diagnosis forbidden in CI")
    args.work.mkdir(parents=True, exist_ok=False)
    import alopex
    location = str(args.work / "store.alopex")
    client = None
    try:
        client = alopex.Database.open(location)
        emit(dict(event="started", pid=os.getpid()))
        populate(client, alopex.TxnMode.READ_WRITE, args.rows)
        client.flush()
        client.close()
        client = alopex.Database.open(location)
        requests = plans(args.operation, args.rows)
        expected = [wanted(args.operation, plan, args.rows) for plan in requests]
        # One unmeasured preflight, separate from the original24 measured/warmup plans.
        actual, _ = timed_operation(client, alopex.TxnMode.READ_ONLY, args.operation, requests[0])
        validate(actual, expected[0])
        plan_bytes = b"".join(key+value for plan in requests for key, value in plan)
        emit(dict(event="ready", pid=os.getpid(), surface="embedded", rows=args.rows, operation=args.operation,
                  profile=args.profile, input_sha256=hashlib.sha256(plan_bytes).hexdigest(),
                  native_sha256=hashlib.sha256(Path(alopex._alopex.__file__).read_bytes()).hexdigest(),
                  driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  operations_per_sample=min(args.rows, 1000) if args.operation == "range-scan" else 100,
                  setup="one durable fixture commit, flush, close, reopen; warm-cache public reads"))
        profiler = cProfile.Profile() if args.profile else None
        if profiler:
            time.sleep(1)
        iteration = 0
        started = time.monotonic()
        while True:
            index = iteration % len(requests)
            call = (timed_operation, client, alopex.TxnMode.READ_ONLY, args.operation, requests[index])
            actual, intervals = profiler.runcall(*call) if profiler else call[0](*call[1:])
            if not profiler:
                emit(dict(event="sample", iteration=iteration, phase="warmup" if iteration < 3 else "measure", **intervals))
            validate(actual, expected[index])
            iteration += 1
            if (profiler and time.monotonic()-started >= 4) or (not profiler and iteration == 24):
                break
        if profiler:
            with (args.work / "client.profile.txt").open("w") as stream:
                pstats.Stats(profiler, stream=stream).sort_stats("cumulative").print_stats()
        client.close()
        client = alopex.Database.open(location)
        txn = client.begin(alopex.TxnMode.READ_ONLY)
        try:
            final = list(txn.scan_range(b"k:", b"l:"))
        finally:
            txn.rollback()
        validate(final, [(key_for(i), value_for(i)) for i in range(args.rows)])
        emit(dict(event="complete", iterations=iteration, final_rows=len(final), result_sha256=result_digest(final),
                  client_cpu_lifetime_seconds=sum(resource.getrusage(resource.RUSAGE_SELF)[:2]),
                  validation="all samples copied bytes/order and complete unchanged state after close/reopen",
                  scope="internal manual diagnosis, not published/reference comparison;100-get sample is not one-request latency"))
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
