"""Reproducible Amazon-product HNSW comparison for issues #298/#299."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


DATASET_SIZE = 9171
DIMENSION = 128
QUERY_COUNT = 10_000
SEED = 42
WARMUP_SECONDS = 2.0
EF_SEARCH_VALUES = (16, 32, 64, 128, 256)
RECALL_CEILING_VALUES = (512, 1024, 4096, DATASET_SIZE)


@dataclass
class SearchEngine:
    name: str
    build_time_seconds: float
    search: Callable[[object, int, int], list[int]]
    close: Callable[[], None]


def load_amazon_products(path: Path):
    """Apply the reference notebook's product cleanup exactly."""
    import numpy as np
    import pandas as pd

    raw = pd.read_csv(path, low_memory=False)
    required = {"Product Name", "Category", "Selling Price"}
    missing = sorted(required.difference(raw.columns))
    if missing:
        raise ValueError(f"dataset is missing required columns: {', '.join(missing)}")

    def top_level_category(value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            return "Unknown"
        return re.split(r"[|>]", value)[0].strip()[:40]

    def parse_price(value: object) -> float:
        if not isinstance(value, str):
            return np.nan
        match = re.search(r"[\d,]+\.?\d*", value.replace(",", ""))
        return float(match.group()) if match else np.nan

    products = pd.DataFrame()
    products["id"] = np.arange(len(raw))
    products["name"] = raw["Product Name"].fillna("")
    products["category"] = raw["Category"].apply(top_level_category)
    products["price"] = raw["Selling Price"].apply(parse_price)
    about = raw.get("About Product", "").fillna("")
    description = raw.get("Product Description", "").fillna("")
    products["text"] = (
        products["name"] + ". " + about + ". " + description
    ).str.slice(0, 2000)
    products = products[
        (products["text"].str.len() > 20) & (products["category"] != "Unknown")
    ].reset_index(drop=True)
    products["id"] = np.arange(len(products))
    return products


def build_embeddings(products):
    """Build the reference TF-IDF -> SVD -> L2-normalized 128-d vectors."""
    import numpy as np
    from sklearn.decomposition import TruncatedSVD
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.preprocessing import normalize

    tfidf = TfidfVectorizer(max_features=50_000, stop_words="english", min_df=2)
    sparse = tfidf.fit_transform(products["text"])
    svd = TruncatedSVD(n_components=DIMENSION, random_state=SEED)
    vectors = normalize(svd.fit_transform(sparse)).astype(np.float32)
    return np.ascontiguousarray(vectors), float(svd.explained_variance_ratio_.sum())


def recall_at_k(predicted, expected) -> float:
    if len(predicted) != len(expected) or not predicted:
        raise ValueError("predicted and expected rows must have the same non-zero length")
    return statistics.fmean(
        len(set(prediction).intersection(truth)) / len(truth)
        for prediction, truth in zip(predicted, expected)
    )


def tie_aware_recall_at_k(predicted, acceptable, k: int) -> float:
    if len(predicted) != len(acceptable) or not predicted:
        raise ValueError("predicted and acceptable rows must have the same non-zero length")
    return statistics.fmean(
        len(set(prediction).intersection(valid_ids)) / k
        for prediction, valid_ids in zip(predicted, acceptable)
    )


def exact_neighbors(vectors, queries, k: int):
    import numpy as np

    similarities = queries @ vectors.T
    return np.argsort(-similarities, axis=1)[:, :k]


def exact_ground_truth(vectors, queries, k: int):
    import numpy as np

    similarities = queries @ vectors.T
    neighbors = np.argsort(-similarities, axis=1)[:, :k]
    cutoffs = np.take_along_axis(similarities, neighbors[:, -1:], axis=1)[:, 0]
    acceptable = [
        np.flatnonzero(row >= cutoff - 1e-7).tolist()
        for row, cutoff in zip(similarities, cutoffs)
    ]
    return neighbors.tolist(), acceptable


def build_alopex(vectors, *, m: int = 16, ef_construction: int = 200) -> SearchEngine:
    import alopex

    db = alopex.Database.new()
    name = f"products_m{m}_efc{ef_construction}"
    config = alopex.HnswConfig(
        DIMENSION,
        m=m,
        ef_construction=ef_construction,
        metric=alopex.Metric.COSINE,
    )
    db.create_hnsw_index(name, config)
    started = time.perf_counter()
    with db.begin(alopex.TxnMode.READ_WRITE) as transaction:
        for index, vector in enumerate(vectors):
            transaction.upsert_to_hnsw(name, str(index).encode(), vector, None)
        transaction.commit()
    build_time = time.perf_counter() - started

    def search(query, k: int, ef_search: int) -> list[int]:
        results, _ = db.search_hnsw(name, query, k, ef_search=ef_search)
        return [int(result.key.decode()) for result in results]

    def close() -> None:
        db.drop_hnsw_index(name)
        db.close()

    return SearchEngine("alopex-hnsw", build_time, search, close)


def build_faiss_flat(vectors) -> SearchEngine:
    import faiss

    faiss.omp_set_num_threads(1)
    started = time.perf_counter()
    index = faiss.IndexFlatIP(DIMENSION)
    index.add(vectors)
    build_time = time.perf_counter() - started

    def search(query, k: int, _ef_search: int) -> list[int]:
        _, labels = index.search(query.reshape(1, -1), k)
        return labels[0].tolist()

    return SearchEngine("faiss-flat-exact", build_time, search, lambda: None)


def build_faiss_hnsw(vectors) -> SearchEngine:
    import faiss

    faiss.omp_set_num_threads(1)
    index = faiss.IndexHNSWFlat(DIMENSION, 16, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = 200
    started = time.perf_counter()
    index.add(vectors)
    build_time = time.perf_counter() - started
    configured_ef = None

    def search(query, k: int, ef_search: int) -> list[int]:
        nonlocal configured_ef
        if configured_ef != ef_search:
            index.hnsw.efSearch = ef_search
            configured_ef = ef_search
        _, labels = index.search(query.reshape(1, -1), k)
        return labels[0].tolist()

    return SearchEngine("faiss-hnsw", build_time, search, lambda: None)


def build_hnswlib(vectors) -> SearchEngine:
    import hnswlib
    import numpy as np

    index = hnswlib.Index(space="ip", dim=DIMENSION)
    index.init_index(
        max_elements=len(vectors), ef_construction=200, M=16, random_seed=SEED
    )
    index.set_num_threads(1)
    started = time.perf_counter()
    index.add_items(vectors, np.arange(len(vectors)))
    build_time = time.perf_counter() - started
    configured_ef = None

    def search(query, k: int, ef_search: int) -> list[int]:
        nonlocal configured_ef
        requested_ef = max(ef_search, k)
        if configured_ef != requested_ef:
            index.set_ef(requested_ef)
            configured_ef = requested_ef
        labels, _ = index.knn_query(query.reshape(1, -1), k=k)
        return labels[0].tolist()

    return SearchEngine("hnswlib", build_time, search, lambda: None)


def measure_setting(
    engine: SearchEngine,
    queries,
    ground_truth,
    acceptable_truth,
    *,
    ef_search: int,
    duration_seconds: float,
    min_queries: int,
    run_count: int,
    k: int = 10,
) -> list[dict[str, object]]:
    for query in queries:
        engine.search(query, k, ef_search)
    predicted = [engine.search(query, k, ef_search) for query in queries]
    recall = recall_at_k(predicted, ground_truth)
    tie_aware_recall = tie_aware_recall_at_k(predicted, acceptable_truth, k)
    rows = []
    for run in range(1, run_count + 1):
        started = time.perf_counter()
        executed = 0
        while executed < min_queries or time.perf_counter() - started < duration_seconds:
            engine.search(queries[executed % len(queries)], k, ef_search)
            executed += 1
        elapsed = time.perf_counter() - started
        rows.append(
            {
                "engine": engine.name,
                "ef_search": ef_search,
                "run": run,
                "dataset_size": DATASET_SIZE,
                "dimension": DIMENSION,
                "query_count": executed,
                "duration_seconds": elapsed,
                "queries_per_second": executed / elapsed,
                "latency_us": elapsed * 1_000_000 / executed,
                "recall_at_10": recall,
                "tie_aware_recall_at_10": tie_aware_recall,
                "build_time_seconds": engine.build_time_seconds,
            }
        )
    return rows


def recall_sweep(
    engine: SearchEngine, queries, ground_truth, acceptable_truth
) -> list[dict[str, object]]:
    rows = []
    for ef_search in RECALL_CEILING_VALUES:
        predicted = [engine.search(query, 10, ef_search) for query in queries]
        rows.append(
            {
                "engine": engine.name,
                "ef_search": ef_search,
                "query_count": len(queries),
                "recall_at_10": recall_at_k(predicted, ground_truth),
                "tie_aware_recall_at_10": tie_aware_recall_at_k(
                    predicted, acceptable_truth, 10
                ),
            }
        )
    return rows


def summarize_by_engine(runs: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, object], list[dict[str, object]]] = defaultdict(list)
    for run in runs:
        grouped[(str(run["engine"]), run.get("ef_search"))].append(run)
    return [
        {
            "engine": engine,
            "ef_search": ef_search,
            "median_queries_per_second": statistics.median(
                float(row["queries_per_second"]) for row in rows
            ),
            "median_latency_us": statistics.median(
                float(
                    row.get(
                        "latency_us", 1_000_000 / float(row["queries_per_second"])
                    )
                )
                for row in rows
            ),
            "median_recall_at_10": statistics.median(
                float(row["recall_at_10"]) for row in rows
            ),
            "median_tie_aware_recall_at_10": statistics.median(
                float(row.get("tie_aware_recall_at_10", row["recall_at_10"]))
                for row in rows
            ),
        }
        for (engine, ef_search), rows in sorted(grouped.items())
    ]


def best_at_recall(summary: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    answer: dict[str, dict[str, object]] = {}
    engines = sorted({str(row["engine"]) for row in summary})
    for threshold in (0.95, 0.99):
        winners = {}
        for engine in engines:
            eligible = [
                row
                for row in summary
                if row["engine"] == engine
                and float(row["median_recall_at_10"]) >= threshold
            ]
            if eligible:
                winners[engine] = max(
                    eligible, key=lambda row: float(row["median_queries_per_second"])
                )
        answer[str(threshold)] = winners
    return answer


def run_benchmark(
    dataset: Path,
    *,
    duration_seconds: float,
    min_queries: int,
    run_count: int,
) -> tuple[
    list[dict[str, object]],
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    import numpy as np

    products = load_amazon_products(dataset)
    if len(products) != DATASET_SIZE:
        raise ValueError(f"expected {DATASET_SIZE} cleaned products, found {len(products)}")
    vectors, explained_variance = build_embeddings(products)
    rng = np.random.default_rng(SEED)
    query_indexes = rng.choice(len(vectors), size=200, replace=False)
    queries = vectors[query_indexes]
    truth, acceptable_truth = exact_ground_truth(vectors, queries, 10)
    runs: list[dict[str, object]] = []
    builds = []
    recall_ceiling = []
    engines = [
        build_alopex(vectors),
        build_faiss_flat(vectors),
        build_faiss_hnsw(vectors),
        build_hnswlib(vectors),
    ]
    try:
        for engine in engines:
            builds.append(
                {
                    "engine": engine.name,
                    "build_time_seconds": engine.build_time_seconds,
                }
            )
            ef_values = (
                (DATASET_SIZE,)
                if engine.name == "faiss-flat-exact"
                else EF_SEARCH_VALUES
            )
            for ef_search in ef_values:
                runs.extend(
                    measure_setting(
                        engine,
                        queries,
                        truth,
                        acceptable_truth,
                        ef_search=ef_search,
                        duration_seconds=duration_seconds,
                        min_queries=min_queries,
                        run_count=run_count,
                    )
                )
            if engine.name != "faiss-flat-exact":
                recall_ceiling.extend(
                    recall_sweep(engine, queries, truth, acceptable_truth)
                )
    finally:
        for engine in engines:
            engine.close()
    metadata = {
        "source": "Amazon Product Dataset 2020",
        "source_file": dataset.name,
        "source_sha256": hashlib.sha256(dataset.read_bytes()).hexdigest(),
        "raw_rows": 10_002,
        "clean_rows": len(products),
        "dimension": vectors.shape[1],
        "query_rows": len(queries),
        "seed": SEED,
        "embedding": "TF-IDF(max_features=50000,min_df=2) + TruncatedSVD(128) + L2",
        "explained_variance": explained_variance,
    }
    return runs, metadata, builds, recall_ceiling


def write_artifacts(
    output: Path,
    runs: list[dict[str, object]],
    release_version: str | None = None,
    *,
    dataset: dict[str, object] | None = None,
    builds: list[dict[str, object]] | None = None,
    recall_ceiling: list[dict[str, object]] | None = None,
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    summary = summarize_by_engine(runs)
    payload = {
        "schema": "alopex.hnsw-diagnostic/v2",
        "contract": {
            "dataset_size": DATASET_SIZE,
            "dimension": DIMENSION,
            "warmup": "one complete query cycle per engine/setting",
            "min_duration_seconds": WARMUP_SECONDS,
            "min_queries": QUERY_COUNT,
            "runs": 3,
            "metrics": [
                "recall_at_10",
                "tie_aware_recall_at_10",
                "latency_us",
                "queries_per_second",
            ],
        },
        "dataset": dataset or {},
        "builds": builds or [],
        "recall_ceiling": recall_ceiling or [],
        "runs": runs,
        "summary": summary,
        "best_at_recall": best_at_recall(summary),
        "median_queries_per_second": statistics.median(
            float(run["queries_per_second"]) for run in runs
        ),
    }
    if release_version is not None:
        payload["release_version"] = release_version
    raw = {"schema": "alopex.hnsw-diagnostic-raw/v2", "runs": runs}
    (output / "hnsw-diagnostic.raw.json").write_text(
        json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "hnsw-diagnostic.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    fieldnames = sorted({key for run in runs for key in run})
    with (output / "hnsw-diagnostic.csv").open(
        "w", newline="", encoding="utf-8"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(runs)
    table = [
        "| engine | ef_search | recall@10 | tie-aware recall@10 | QPS | us/query |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    table.extend(
        "| {engine} | {ef_search} | {median_recall_at_10:.4f} | "
        "{median_tie_aware_recall_at_10:.4f} | "
        "{median_queries_per_second:.1f} | {median_latency_us:.1f} |".format(**row)
        for row in summary
    )
    (output / "hnsw-diagnostic.md").write_text(
        "# HNSW diagnostic\n\n"
        f"Release: `{release_version or 'local'}`. Dataset: `{DATASET_SIZE} x "
        f"{DIMENSION}`; minimum queries/run: `{QUERY_COUNT}`; seed: `{SEED}`.\n\n"
        + "\n".join(table)
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=WARMUP_SECONDS)
    parser.add_argument("--min-queries", type=int, default=QUERY_COUNT)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--release-version")
    args = parser.parse_args()
    if args.duration_seconds < WARMUP_SECONDS:
        parser.error("duration must be at least 2 seconds")
    if args.min_queries < QUERY_COUNT:
        parser.error("min-queries must be at least 10000")
    if args.runs != 3:
        parser.error("runs must be exactly 3")
    benchmark_runs, dataset, builds, recall_ceiling = run_benchmark(
        args.dataset,
        duration_seconds=args.duration_seconds,
        min_queries=args.min_queries,
        run_count=args.runs,
    )
    write_artifacts(
        args.output,
        benchmark_runs,
        release_version=args.release_version,
        dataset=dataset,
        builds=builds,
        recall_ceiling=recall_ceiling,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
