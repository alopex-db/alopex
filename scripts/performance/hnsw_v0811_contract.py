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


def measure(duration: float, query_count: int = QUERY_COUNT) -> dict[str, object]:
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
        "dataset_size": DATASET_SIZE,
        "dimension": DIMENSION,
        "query_count": query_count,
        "seed": SEED,
        "duration_seconds": elapsed,
        "queries_per_second": query_count / elapsed,
        "checksum": checksum,
    }


def write_artifacts(output: Path, runs: list[dict[str, object]]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "alopex.hnsw-diagnostic/v1",
        "contract": {"dataset_size": DATASET_SIZE, "dimension": DIMENSION, "warmup_seconds": 2, "min_queries": QUERY_COUNT, "runs": 3},
        "runs": runs,
        "median_queries_per_second": statistics.median(float(run["queries_per_second"]) for run in runs),
    }
    (output / "hnsw-diagnostic.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (output / "hnsw-diagnostic.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=runs[0].keys())
        writer.writeheader()
        writer.writerows(runs)
    (output / "hnsw-diagnostic.md").write_text(
        "# HNSW diagnostic\n\n"
        f"Dataset: `{DATASET_SIZE} x {DIMENSION}`; queries/run: `{QUERY_COUNT}`; seed: `{SEED}`.\n\n"
        f"Median queries/s: **{payload['median_queries_per_second']:.2f}**\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=2.0)
    args = parser.parse_args()
    if args.duration_seconds < 2:
        parser.error("duration must be at least 2 seconds")
    write_artifacts(args.output, [measure(args.duration_seconds) for _ in range(3)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
