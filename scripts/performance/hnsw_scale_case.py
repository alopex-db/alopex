"""Run exactly one GloVe engine x N scale case and emit one raw JSON result."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from hnsw_v0811_contract import (
    EF_SEARCH_VALUES,
    GLOVE_SHA256,
    build_alopex,
    build_faiss_flat,
    build_faiss_hnsw,
    build_hnswlib,
    measure_setting,
)


BUILDERS = {
    "alopex-hnsw": build_alopex,
    "faiss-hnsw": build_faiss_hnsw,
    "hnswlib": build_hnswlib,
    "flat": build_faiss_flat,
}


def run_case(dataset: Path, engine_name: str, size: int, query_count: int) -> dict:
    import h5py
    import numpy as np

    digest = hashlib.sha256(dataset.read_bytes()).hexdigest()
    if digest != GLOVE_SHA256:
        raise ValueError(f"unexpected glove-100-angular checksum: {digest}")
    builder = BUILDERS[engine_name]
    with h5py.File(dataset) as source:
        if size > len(source["train"]):
            raise ValueError(f"requested N={size} exceeds dataset rows")
        vectors = np.asarray(source["train"][:size], dtype=np.float32)
        vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
        queries = np.asarray(source["test"][:200], dtype=np.float32)
        queries /= np.linalg.norm(queries, axis=1, keepdims=True)
        oracle = build_faiss_flat(vectors)
        try:
            truth = [oracle.search(query, 10, size) for query in queries]
        finally:
            oracle.close()
        engine = builder(vectors)
        try:
            # Scale acceptance needs one comparable recall-qualified operating
            # point, not a full ef sweep.  The maximum ANN ef is the configured
            # candidate; the three fixed-query runs below provide the evidence.
            ef_values = (size,) if engine_name == "flat" else (max(EF_SEARCH_VALUES),)
            runs = []
            for ef_search in ef_values:
                runs.extend(
                    measure_setting(
                        engine,
                        queries,
                        truth,
                        truth,
                        ef_search=ef_search,
                        min_queries=query_count,
                        run_count=3,
                        dataset_size=size,
                    )
                )
            summary = {}
            for row in runs:
                summary.setdefault(row["ef_search"], []).append(row)
            curve = [
                {
                    "ef_search": ef,
                    "median_queries_per_second": float(
                        np.median([row["queries_per_second"] for row in rows])
                    ),
                    "median_recall_at_10": float(
                        np.median([row["recall_at_10"] for row in rows])
                    ),
                }
                for ef, rows in sorted(summary.items())
            ]
            eligible = [row for row in curve if row["median_recall_at_10"] >= 0.95]
            fastest = max(eligible, key=lambda row: row["median_queries_per_second"], default=None)
            return {
                "schema": "alopex.hnsw-scale-case/v1",
                "dataset": "glove-100-angular",
                "dataset_sha256": digest,
                "engine": engine_name,
                "dataset_size": size,
                "query_count_per_run": query_count,
                "run_count": 3,
                "build": {
                    "build_time_seconds": engine.build_time_seconds,
                    "index_size_bytes": engine.index_size_bytes,
                    "peak_rss_bytes": engine.peak_rss_bytes,
                    "node_count": engine.node_count,
                },
                "search": {
                    "qps_at_recall_095": fastest["median_queries_per_second"] if fastest else None,
                    "ef_search_at_recall_095": fastest["ef_search"] if fastest else None,
                    "recall_at_selected_setting": fastest["median_recall_at_10"] if fastest else None,
                    "curve": curve,
                    "ef_search_policy": "flat_N_or_max_configured_ann_ef",
                },
                "runs": runs,
            }
        finally:
            engine.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--engine", choices=sorted(BUILDERS), required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--query-count", type=int, default=10_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_case(args.dataset, args.engine, args.size, args.query_count)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
