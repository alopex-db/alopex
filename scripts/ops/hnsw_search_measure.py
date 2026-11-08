"""Preserve the pinned HNSW timer; retain already-collected query latencies.

Resource observation wraps the whole original function, including its percentile
aggregation. It is intentionally wider than the query timer. Native core timing
is a separate quantized observation, not a measured Python-only duration.
"""
from __future__ import annotations
import statistics
import time

def _measure_prepared_run(
    engine: SearchEngine,
    queries,
    prepared: tuple[float, float, float | None],
    *,
    ef_search: int,
    min_queries: int,
    run: int,
    k: int,
    min_duration_seconds: float,
    dataset_size: int,
) -> dict[str, object]:
    recall, tie_aware_recall, native_latency_us = prepared
    started = time.perf_counter()
    executed = 0
    latencies = []
    while executed < min_queries or time.perf_counter() - started < min_duration_seconds:
        query_started = time.perf_counter_ns()
        engine.search(queries[executed % len(queries)], k, ef_search)
        latencies.append((time.perf_counter_ns() - query_started) / 1000)
        executed += 1
    elapsed = time.perf_counter() - started
    return {
        "query_latencies_us": latencies,
        "engine": engine.name,
        "ef_search": ef_search,
        "run": run,
        "dataset_size": dataset_size,
        "dimension": queries.shape[1],
        "query_count": executed,
        "duration_seconds": elapsed,
        "queries_per_second": executed / elapsed,
        "latency_us": elapsed * 1_000_000 / executed,
        "native_search_latency_us": native_latency_us,
        "python_binding_residual_us": (
            max(0.0, elapsed * 1_000_000 / executed - native_latency_us)
            if native_latency_us is not None
            else None
        ),
        "query_latency_p50_us": statistics.median(latencies),
        "query_latency_p95_us": sorted(latencies)[
            max(0, (95 * len(latencies) + 99) // 100 - 1)
        ],
        "query_latency_p99_us": sorted(latencies)[
            max(0, (99 * len(latencies) + 99) // 100 - 1)
        ],
        "recall_at_10": recall,
        "tie_aware_recall_at_10": tie_aware_recall,
    }
