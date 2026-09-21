"""Exact #448 flat-vector ingest acceptance measurement."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import time
from pathlib import Path


DIMENSION = 128
VECTOR_COUNT = 16_000
WINDOW_SIZE = 2_000
RUNS = 3


def normalized_vectors():
    import numpy as np

    rng = np.random.default_rng(0)
    vectors = rng.standard_normal((VECTOR_COUNT, DIMENSION)).astype(np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    return vectors


def measure_upserts(vectors, *, commit_per_window: bool) -> list[dict[str, object]]:
    import alopex

    db = alopex.Database.new()
    rows = []
    try:
        if commit_per_window:
            for start in range(0, len(vectors), WINDOW_SIZE):
                started = time.perf_counter_ns()
                with db.begin(alopex.TxnMode.READ_WRITE) as txn:
                    for index, vector in enumerate(vectors[start : start + WINDOW_SIZE], start):
                        txn.upsert_vector(
                            f"v{index}".encode(), None, vector, alopex.Metric.COSINE
                        )
                    txn.commit()
                rows.append(
                    {
                        "stored": start + WINDOW_SIZE,
                        "us_per_vector": (time.perf_counter_ns() - started)
                        / WINDOW_SIZE
                        / 1_000,
                        "includes_commit": True,
                    }
                )
        else:
            with db.begin(alopex.TxnMode.READ_WRITE) as txn:
                for start in range(0, len(vectors), WINDOW_SIZE):
                    started = time.perf_counter_ns()
                    for index, vector in enumerate(vectors[start : start + WINDOW_SIZE], start):
                        txn.upsert_vector(
                            f"v{index}".encode(), None, vector, alopex.Metric.COSINE
                        )
                    rows.append(
                        {
                            "stored": start + WINDOW_SIZE,
                            "us_per_vector": (time.perf_counter_ns() - started)
                            / WINDOW_SIZE
                            / 1_000,
                            "includes_commit": False,
                        }
                    )
                txn.commit()
    finally:
        db.close()
    return rows


def measure_kv_control() -> list[dict[str, object]]:
    import alopex

    db = alopex.Database.new()
    rows = []
    try:
        with db.begin(alopex.TxnMode.READ_WRITE) as txn:
            for start in range(0, VECTOR_COUNT, WINDOW_SIZE):
                started = time.perf_counter_ns()
                for index in range(start, start + WINDOW_SIZE):
                    txn.put(f"k{index}".encode(), b"x" * 512)
                rows.append(
                    {
                        "stored": start + WINDOW_SIZE,
                        "us_per_value": (time.perf_counter_ns() - started)
                        / WINDOW_SIZE
                        / 1_000,
                    }
                )
            txn.commit()
    finally:
        db.close()
    return rows


def median_growth(rows_by_run: list[list[dict[str, object]]], field: str) -> float:
    if not rows_by_run or any(len(rows) != VECTOR_COUNT // WINDOW_SIZE for rows in rows_by_run):
        raise ValueError("every run must contain the complete window sequence")
    first = statistics.median(float(rows[0][field]) for rows in rows_by_run)
    last = statistics.median(float(rows[-1][field]) for rows in rows_by_run)
    if first <= 0:
        raise ValueError("first-window latency must be positive")
    return last / first


def validate(payload: dict[str, object]) -> None:
    measurements = payload["measurements"]
    assert isinstance(measurements, dict)
    for name in ("single_transaction", "commit_per_window"):
        growth = median_growth(measurements[name], "us_per_vector")
        if growth > 4.0:
            raise ValueError(
                f"{name} grew {growth:.2f}x from the first to the final window; "
                "flat vector insertion must not grow with stored vector count"
            )


def source_commit(root: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    vectors = normalized_vectors()
    measurements = {
        "single_transaction": [
            measure_upserts(vectors, commit_per_window=False) for _ in range(RUNS)
        ],
        "commit_per_window": [
            measure_upserts(vectors, commit_per_window=True) for _ in range(RUNS)
        ],
        "kv_control": [measure_kv_control() for _ in range(RUNS)],
    }
    root = Path(__file__).resolve().parents[2]
    payload: dict[str, object] = {
        "schema": "alopex.flat-vector-ingest/v1",
        "source_commit": source_commit(root),
        "contract": {
            "dimension": DIMENSION,
            "vector_count": VECTOR_COUNT,
            "window_size": WINDOW_SIZE,
            "runs": RUNS,
            "vector_metric": "cosine",
            "maximum_final_to_first_window_growth": 4.0,
        },
        "measurements": measurements,
        "status": "success",
    }
    try:
        validate(payload)
    except ValueError as error:
        payload["status"] = "failure"
        payload["failure"] = str(error)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if payload["status"] != "success":
        raise SystemExit(str(payload["failure"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
