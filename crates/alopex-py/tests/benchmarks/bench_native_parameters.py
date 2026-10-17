"""Emit reproducible native-parameter timings; run each arm in a fresh process.

PYTHONPATH=<wheel package parent> python bench_native_parameters.py
Use the same Python/build profile, alternate arms, and compare at least six
process medians. Each recorded sample contains 1,000 checked SQL executions.
"""

import json
import statistics
import time
from decimal import Decimal

from alopex import Database


def measure(value):
    db = Database.new()
    samples = []
    for repeat in range(24):
        start = time.perf_counter_ns()
        for _ in range(1000):
            assert db.execute_sql("SELECT ? AS value", [value]) == [{"value": value}]
        elapsed = time.perf_counter_ns() - start
        if repeat >= 3:
            samples.append(elapsed)
    return {
        "operations": len(samples) * 1000,
        "latency_ns_per_1000": samples,
        "median_ns_per_1000": statistics.median(samples),
    }


if __name__ == "__main__":
    print(json.dumps({
        "decimal": measure(Decimal("12345678901234567.89")),
        "integer": measure(123),
    }))
