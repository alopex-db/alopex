"""Deterministic, post-release HNSW diagnostic artifact generator (issues #298/#299)."""
from __future__ import annotations

import argparse
import csv
import json
import random
import statistics
import time
from pathlib import Path


DATASET_SIZE = 9171
DIMENSION = 128
QUERY_COUNT = 10_000
SEED = 42
WARMUP_SECONDS = 2.0
EF_SEARCH_VALUES = (16, 64, 256, 512, 1024, DATASET_SIZE)


def _measure_scalar(duration: float, query_count: int) -> dict[str, object]:
    rng = random.Random(SEED)
    vectors = [[rng.random() for _ in range(DIMENSION)] for _ in range(DATASET_SIZE)]
    queries = [vectors[i % DATASET_SIZE] for i in range(query_count)]
    started = time.perf_counter()
    checksum = 0.0
    while time.perf_counter() - started < duration:
        for query in queries:
            checksum += sum(value * value for value in query)
    elapsed = time.perf_counter() - started
    return {
        "engine": "scalar-baseline",
        "dataset_size": DATASET_SIZE,
        "dimension": DIMENSION,
        "query_count": query_count,
        "seed": SEED,
        "duration_seconds": elapsed,
        "queries_per_second": query_count / elapsed,
        "checksum": checksum,
        "recall_at_10": None,
        "latency_ms": elapsed * 1000 / query_count,
        "recall_by_ef_search": {},
        "latency_by_ef_search_ms": {},
        "fixed_latency_ms": None,
        "exploration_latency_ms": None,
    }


def _measure_alopex(duration: float, query_count: int) -> dict[str, object]:
    import numpy as np
    import alopex

    rng = np.random.default_rng(SEED)
    vectors = rng.standard_normal((DATASET_SIZE, DIMENSION)).astype(np.float32)
    db = alopex.Database.new()
    index_name = "v0811_diagnostic"
    config = alopex.HnswConfig(
        dim=DIMENSION,
        m=16,
        ef_construction=200,
        metric=alopex.Metric.COSINE,
    )
    db.create_hnsw_index(index_name, config)
    try:
        with db.begin(alopex.TxnMode.READ_WRITE) as txn:
            for index, vector in enumerate(vectors):
                txn.upsert_to_hnsw(
                    index_name, key=f"hnsw_{index}".encode(), vector=vector
                )
            txn.commit()

        queries = vectors[: min(64, DATASET_SIZE)]
        warmup_started = time.perf_counter()
        while time.perf_counter() - warmup_started < WARMUP_SECONDS:
            db.search_hnsw(index_name, queries[0], k=10, ef_search=64)

        recall_by_ef_search: dict[str, float] = {}
        latency_by_ef_search_ms: dict[str, float] = {}
        for ef_search in EF_SEARCH_VALUES:
            hits = 0
            started = time.perf_counter()
            for index, query in enumerate(queries):
                results, _ = db.search_hnsw(
                    index_name, query, k=10, ef_search=min(ef_search, DATASET_SIZE)
                )
                hits += any(result.key == f"hnsw_{index}".encode() for result in results)
            elapsed = time.perf_counter() - started
            key = str(ef_search)
            recall_by_ef_search[key] = hits / len(queries)
            latency_by_ef_search_ms[key] = elapsed * 1000 / len(queries)

        started = time.perf_counter()
        executed = 0
        while executed < query_count or time.perf_counter() - started < duration:
            query = queries[executed % len(queries)]
            db.search_hnsw(index_name, query, k=10, ef_search=64)
            executed += 1
        elapsed = time.perf_counter() - started
        return {
            "engine": "alopex-hnsw",
            "dataset_size": DATASET_SIZE,
            "dimension": DIMENSION,
            "query_count": executed,
            "seed": SEED,
            "duration_seconds": elapsed,
            "queries_per_second": executed / elapsed,
            "checksum": None,
            "recall_at_10": recall_by_ef_search["64"],
            "latency_ms": elapsed * 1000 / executed,
            "ef_search": 64,
            "recall_by_ef_search": recall_by_ef_search,
            "latency_by_ef_search_ms": latency_by_ef_search_ms,
            "fixed_latency_ms": latency_by_ef_search_ms["16"],
            "exploration_latency_ms": max(
                0.0,
                latency_by_ef_search_ms["64"] - latency_by_ef_search_ms["16"],
            ),
        }
    finally:
        db.drop_hnsw_index(index_name)
        db.close()


def measure(duration: float, query_count: int = QUERY_COUNT) -> dict[str, object]:
    try:
        import numpy  # noqa: F401
        import alopex  # noqa: F401
    except ImportError:
        return _measure_scalar(duration, query_count)
    return _measure_alopex(duration, query_count)


def write_artifacts(
    output: Path, runs: list[dict[str, object]], release_version: str | None = None
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "alopex.hnsw-diagnostic/v1",
        "contract": {
            "dataset_size": DATASET_SIZE,
            "dimension": DIMENSION,
            "warmup_seconds": WARMUP_SECONDS,
            "min_queries": QUERY_COUNT,
            "runs": 3,
            "metrics": [
                "recall_at_10",
                "latency_ms",
                "queries_per_second",
                "recall_by_ef_search",
                "latency_by_ef_search_ms",
            ],
        },
        "runs": runs,
        "median_queries_per_second": statistics.median(float(run["queries_per_second"]) for run in runs),
    }
    if release_version is not None:
        payload["release_version"] = release_version
    (output / "hnsw-diagnostic.raw.json").write_text(
        json.dumps({"schema": "alopex.hnsw-diagnostic-raw/v1", "runs": runs}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    (output / "hnsw-diagnostic.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (output / "hnsw-diagnostic.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=runs[0].keys())
        writer.writeheader()
        writer.writerows(
            {
                key: json.dumps(value, sort_keys=True)
                if isinstance(value, (dict, list))
                else value
                for key, value in run.items()
            }
            for run in runs
        )
    (output / "hnsw-diagnostic.md").write_text(
        "# HNSW diagnostic\n\n"
        f"Release: `{release_version or 'local'}`. Dataset: `{DATASET_SIZE} x {DIMENSION}`; queries/run: `{QUERY_COUNT}`; seed: `{SEED}`.\n\n"
        f"Median queries/s: **{payload['median_queries_per_second']:.2f}**\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=2.0)
    parser.add_argument("--release-version")
    args = parser.parse_args()
    if args.duration_seconds < 2:
        parser.error("duration must be at least 2 seconds")
    write_artifacts(
        args.output,
        [measure(args.duration_seconds) for _ in range(3)],
        release_version=args.release_version,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
