"""Reproducible Amazon-product HNSW comparison for issues #298/#299."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import resource
import statistics
import tempfile
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
    index_size_bytes: int
    peak_rss_bytes: int
    update_latency_ms: float | None = None
    delete_latency_ms: float | None = None
    reopen_latency_ms: float | None = None
    node_count: int | None = None
    native_search_time_us: Callable[[object, int, int], int] | None = None


def peak_rss_bytes() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


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
    products["text"] = (products["name"] + ". " + about + ". " + description).str.slice(
        0, 2000
    )
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
        raise ValueError(
            "predicted and expected rows must have the same non-zero length"
        )
    return statistics.fmean(
        len(set(prediction).intersection(truth)) / len(truth)
        for prediction, truth in zip(predicted, expected)
    )


def tie_aware_recall_at_k(predicted, acceptable, k: int) -> float:
    if len(predicted) != len(acceptable) or not predicted:
        raise ValueError(
            "predicted and acceptable rows must have the same non-zero length"
        )
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

    directory = tempfile.TemporaryDirectory()
    database_path = Path(directory.name) / "index"
    db = alopex.Database.open(str(database_path))
    name = f"products_m{m}_efc{ef_construction}"
    config = alopex.HnswConfig(
        vectors.shape[1],
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
    del transaction
    db.flush()
    build_time = time.perf_counter() - started
    db.close()
    started = time.perf_counter()
    db = alopex.Database.open(str(database_path))
    reopen_latency_ms = (time.perf_counter() - started) * 1000
    index_size_bytes = sum(
        path.stat().st_blocks * 512
        for path in database_path.rglob("*")
        if path.is_file()
    )
    measured_rss = peak_rss_bytes()
    started = time.perf_counter()
    with db.begin(alopex.TxnMode.READ_WRITE) as transaction:
        transaction.upsert_to_hnsw(name, b"0", vectors[0], None)
        transaction.commit()
    del transaction
    update_latency_ms = (time.perf_counter() - started) * 1000
    started = time.perf_counter()
    with db.begin(alopex.TxnMode.READ_WRITE) as transaction:
        transaction.delete_from_hnsw(name, b"1")
        transaction.commit()
    del transaction
    delete_latency_ms = (time.perf_counter() - started) * 1000
    with db.begin(alopex.TxnMode.READ_WRITE) as transaction:
        transaction.upsert_to_hnsw(name, b"1", vectors[1], None)
        transaction.commit()
    del transaction
    node_count = db.get_hnsw_stats(name).node_count

    def search(query, k: int, ef_search: int) -> list[int]:
        results, _ = db.search_hnsw(name, query, k, ef_search=ef_search)
        return [int(result.key.decode()) for result in results]

    def native_search_time_us(query, k: int, ef_search: int) -> int:
        _, stats = db.search_hnsw(name, query, k, ef_search=ef_search)
        return stats.search_time_us

    def close() -> None:
        db.drop_hnsw_index(name)
        db.close()
        directory.cleanup()

    return SearchEngine(
        "alopex-hnsw",
        build_time,
        search,
        close,
        index_size_bytes,
        measured_rss,
        update_latency_ms,
        delete_latency_ms,
        reopen_latency_ms,
        node_count,
        native_search_time_us,
    )


def build_faiss_flat(vectors) -> SearchEngine:
    import faiss

    faiss.omp_set_num_threads(1)
    started = time.perf_counter()
    index = faiss.IndexFlatIP(vectors.shape[1])
    index.add(vectors)
    build_time = time.perf_counter() - started

    def search(query, k: int, _ef_search: int) -> list[int]:
        _, labels = index.search(query.reshape(1, -1), k)
        return labels[0].tolist()

    return SearchEngine(
        "faiss-flat-exact",
        build_time,
        search,
        lambda: None,
        len(faiss.serialize_index(index)),
        peak_rss_bytes(),
        node_count=int(index.ntotal),
    )


def build_faiss_hnsw(vectors) -> SearchEngine:
    import faiss

    faiss.omp_set_num_threads(1)
    index = faiss.IndexHNSWFlat(vectors.shape[1], 16, faiss.METRIC_INNER_PRODUCT)
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

    return SearchEngine(
        "faiss-hnsw",
        build_time,
        search,
        lambda: None,
        len(faiss.serialize_index(index)),
        peak_rss_bytes(),
        node_count=int(index.ntotal),
    )


def build_hnswlib(vectors) -> SearchEngine:
    import hnswlib
    import numpy as np

    index = hnswlib.Index(space="ip", dim=vectors.shape[1])
    index.init_index(
        max_elements=len(vectors), ef_construction=200, M=16, random_seed=SEED
    )
    index.set_num_threads(1)
    started = time.perf_counter()
    index.add_items(vectors, np.arange(len(vectors)))
    build_time = time.perf_counter() - started
    started = time.perf_counter()
    index.add_items(vectors[0].reshape(1, -1), np.array([0]))
    update_latency_ms = (time.perf_counter() - started) * 1000
    started = time.perf_counter()
    index.mark_deleted(1)
    index.unmark_deleted(1)
    delete_latency_ms = (time.perf_counter() - started) * 1000
    directory = tempfile.TemporaryDirectory()
    index_path = Path(directory.name) / "index.bin"
    index.save_index(str(index_path))
    index_size_bytes = index_path.stat().st_size
    reopened = hnswlib.Index(space="ip", dim=DIMENSION)
    started = time.perf_counter()
    reopened.load_index(str(index_path), max_elements=len(vectors))
    reopen_latency_ms = (time.perf_counter() - started) * 1000
    reopened.set_num_threads(1)
    index = reopened
    configured_ef = None

    def search(query, k: int, ef_search: int) -> list[int]:
        nonlocal configured_ef
        requested_ef = max(ef_search, k)
        if configured_ef != requested_ef:
            index.set_ef(requested_ef)
            configured_ef = requested_ef
        labels, _ = index.knn_query(query.reshape(1, -1), k=k)
        return labels[0].tolist()

    return SearchEngine(
        "hnswlib",
        build_time,
        search,
        directory.cleanup,
        index_size_bytes,
        peak_rss_bytes(),
        update_latency_ms,
        delete_latency_ms,
        reopen_latency_ms,
        int(index.get_current_count()),
    )


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
    dataset_size: int = DATASET_SIZE,
) -> list[dict[str, object]]:
    for query in queries:
        engine.search(query, k, ef_search)
    predicted = [engine.search(query, k, ef_search) for query in queries]
    recall = recall_at_k(predicted, ground_truth)
    tie_aware_recall = tie_aware_recall_at_k(predicted, acceptable_truth, k)
    native_latency_us = (
        statistics.median(
            engine.native_search_time_us(query, k, ef_search) for query in queries
        )
        if engine.native_search_time_us
        else None
    )
    rows = []
    for run in range(1, run_count + 1):
        started = time.perf_counter()
        executed = 0
        latencies = []
        while (
            executed < min_queries or time.perf_counter() - started < duration_seconds
        ):
            query_started = time.perf_counter_ns()
            engine.search(queries[executed % len(queries)], k, ef_search)
            latencies.append((time.perf_counter_ns() - query_started) / 1000)
            executed += 1
        elapsed = time.perf_counter() - started
        rows.append(
            {
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
                "build_time_seconds": engine.build_time_seconds,
            }
        )
    return rows


def measure_call_floor(
    vectors,
    *,
    duration_seconds: float,
    min_queries: int,
    run_count: int,
) -> list[dict[str, object]]:
    """Measure the 10-vector, k=1, ef=1 lower-bound requested by #298."""
    floors = []
    for builder in (build_hnswlib, build_alopex, build_faiss_flat, build_faiss_hnsw):
        engine = builder(vectors[:10])
        try:
            for query in vectors[:10]:
                engine.search(query, 1, 1)
            latencies = []
            for run in range(1, run_count + 1):
                started = time.perf_counter()
                executed = 0
                while (
                    executed < min_queries
                    or time.perf_counter() - started < duration_seconds
                ):
                    engine.search(vectors[executed % 10], 1, 1)
                    executed += 1
                elapsed = time.perf_counter() - started
                latencies.append(elapsed * 1_000_000 / executed)
                floors.append(
                    {
                        "engine": engine.name,
                        "run": run,
                        "vectors": 10,
                        "k": 1,
                        "ef_search": 1,
                        "query_count": executed,
                        "duration_seconds": elapsed,
                        "latency_us": latencies[-1],
                    }
                )
        finally:
            engine.close()
    return floors


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


def exact_metric_agreement(vectors, limit: int = 512) -> dict[str, object]:
    """Verify the normalized-vector L2/IP ordering used by the oracle."""
    import numpy as np

    sample = vectors[: min(limit, len(vectors))]
    queries = sample[: min(32, len(sample))]
    inner_product = np.argsort(-(queries @ sample.T), axis=1, kind="stable")
    squared_l2 = np.argsort(
        ((queries[:, None, :] - sample[None, :, :]) ** 2).sum(axis=2),
        axis=1,
        kind="stable",
    )
    return {
        "rows": len(sample),
        "queries": len(queries),
        "normalized": bool(np.allclose(np.linalg.norm(sample, axis=1), 1.0)),
        "l2_inner_product_top_10_equal": bool(
            np.array_equal(inner_product[:, :10], squared_l2[:, :10])
        ),
    }


def mismatch_reproduction(
    vectors, queries, predicted, truth, acceptable
) -> dict[str, object]:
    """Return the first strict mismatch with enough scores to reproduce it."""
    for query_index, (actual, expected, valid) in enumerate(
        zip(predicted, truth, acceptable)
    ):
        if set(actual) == set(expected):
            continue
        missing = sorted(set(expected).difference(actual))
        substituted = sorted(set(actual).difference(expected))
        return {
            "query_index": query_index,
            "missing_ids": missing,
            "substituted_ids": substituted,
            "missing_scores": [
                float(queries[query_index] @ vectors[index]) for index in missing
            ],
            "substituted_scores": [
                float(queries[query_index] @ vectors[index]) for index in substituted
            ],
            "all_substitutions_tie_acceptable": all(
                index in valid for index in substituted
            ),
        }
    return {"query_index": None, "all_substitutions_tie_acceptable": True}


def investigate_recall_contract(
    vectors, queries, truth, acceptable_truth, base_alopex: SearchEngine
) -> dict[str, object]:
    base_predictions = [
        base_alopex.search(query, 10, DATASET_SIZE) for query in queries
    ]
    self_matches = sum(
        base_alopex.search(vector, 1, len(vectors)) == [index]
        for index, vector in enumerate(vectors)
    )
    configurations = []
    for ef_construction, m in ((50, 16), (200, 16), (800, 16), (200, 32)):
        if ef_construction == 200 and m == 16:
            engine = base_alopex
            owned = False
        else:
            engine = build_alopex(vectors, m=m, ef_construction=ef_construction)
            owned = True
        try:
            predicted = [engine.search(query, 10, 64) for query in queries]
            configurations.append(
                {
                    "engine": "alopex-hnsw",
                    "m": m,
                    "ef_construction": ef_construction,
                    "ef_search": 64,
                    "recall_at_10": recall_at_k(predicted, truth),
                    "tie_aware_recall_at_10": tie_aware_recall_at_k(
                        predicted, acceptable_truth, 10
                    ),
                }
            )
        finally:
            if owned:
                engine.close()
    tie_aware = tie_aware_recall_at_k(base_predictions, acceptable_truth, 10)
    return {
        "index_count": base_alopex.node_count,
        "input_count": len(vectors),
        "index_count_matches_input": base_alopex.node_count == len(vectors),
        "self_match_rate": self_matches / len(vectors),
        "ef_construction_and_m": configurations,
        "exact_metric_agreement": exact_metric_agreement(vectors),
        "minimal_reproduction": mismatch_reproduction(
            vectors, queries, base_predictions, truth, acceptable_truth
        ),
        "conclusion": (
            "strict top-k boundary tie"
            if tie_aware == 1.0 and recall_at_k(base_predictions, truth) < 1.0
            else "no strict recall ceiling"
            if recall_at_k(base_predictions, truth) == 1.0
            else "non-tie recall divergence"
        ),
    }


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
                    row.get("latency_us", 1_000_000 / float(row["queries_per_second"]))
                )
                for row in rows
            ),
            "median_query_latency_p50_us": statistics.median(
                float(row.get("query_latency_p50_us", row.get("latency_us", 0.0)))
                for row in rows
            ),
            "median_query_latency_p95_us": statistics.median(
                float(row.get("query_latency_p95_us", row.get("latency_us", 0.0)))
                for row in rows
            ),
            "median_query_latency_p99_us": statistics.median(
                float(row.get("query_latency_p99_us", row.get("latency_us", 0.0)))
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


def decompose_latency(
    summary: list[dict[str, object]], fixed_cost_us: dict[str, float]
) -> list[dict[str, object]]:
    """Fit latency against ef_search and retain the measured call-floor."""
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in summary:
        grouped[str(row["engine"])].append(row)
    decomposed = []
    for engine, rows in grouped.items():
        points = [
            (float(row["ef_search"]), float(row["median_latency_us"]))
            for row in rows
            if row.get("ef_search") is not None
        ]
        if len(points) > 1:
            mean_x = statistics.fmean(x for x, _ in points)
            mean_y = statistics.fmean(y for _, y in points)
            denominator = sum((x - mean_x) ** 2 for x, _ in points)
            slope = sum((x - mean_x) * (y - mean_y) for x, y in points) / denominator
            intercept = mean_y - slope * mean_x
        else:
            slope = 0.0
            intercept = points[0][1]
        floor = fixed_cost_us.get(engine, max(0.0, intercept))
        for row in rows:
            actual = float(row["median_latency_us"])
            decomposed.append(
                {
                    "engine": engine,
                    "ef_search": row.get("ef_search"),
                    "actual_latency_us": actual,
                    "fixed_lower_bound_us": floor,
                    "exploration_residual_us": max(0.0, actual - floor),
                    "regression_intercept_us": intercept,
                    "slope_us_per_ef": slope,
                }
            )
    return sorted(
        decomposed, key=lambda row: (str(row["engine"]), int(row["ef_search"]))
    )


def analyze_scale(
    rows: list[dict[str, object]], *, requested_sizes: tuple[int, ...]
) -> dict[str, object]:
    by_size = {
        (int(row["dataset_size"]), str(row["engine"])): float(row["qps_at_recall_095"])
        for row in rows
    }
    measured_sizes = sorted({size for size, _ in by_size})
    engines = sorted({engine for _, engine in by_size if engine != "flat"})
    crossover = {}
    trends = {}
    for engine in engines:
        comparable = [
            size
            for size in measured_sizes
            if (size, engine) in by_size and (size, "flat") in by_size
        ]
        crossover[engine] = next(
            (
                size
                for size in comparable
                if by_size[(size, engine)] > by_size[(size, "flat")]
            ),
            None,
        )
        ratios = [
            by_size[(size, engine)] / by_size[(size, "flat")] for size in comparable
        ]
        if len(ratios) < 2 or 0.95 <= ratios[-1] / ratios[0] <= 1.05:
            trends[engine] = "constant"
        elif ratios[-1] > ratios[0]:
            trends[engine] = "widens"
        else:
            trends[engine] = "narrows"
    return {
        "brute_force_crossover": crossover,
        "gap_trend_vs_flat": trends,
        "limits": [
            {"dataset_size": size, "reason": "configured execution limit"}
            for size in requested_sizes
            if size not in measured_sizes
        ],
    }


def _alopex_hybrid(vectors):
    import alopex

    db = alopex.Database.new()
    db.create_hnsw_index(
        "hybrid_vectors",
        alopex.HnswConfig(
            vectors.shape[1], m=16, ef_construction=200, metric=alopex.Metric.COSINE
        ),
    )
    db.execute_sql("CREATE TABLE hybrid_rows (id INT PRIMARY KEY, bucket INT)")
    started = time.perf_counter()
    with db.begin(alopex.TxnMode.READ_WRITE) as transaction:
        for index, vector in enumerate(vectors):
            transaction.upsert_to_hnsw(
                "hybrid_vectors", str(index).encode(), vector, None
            )
        transaction.commit()
    for start in range(0, len(vectors), 500):
        values = ",".join(
            f"({index},{index % 1000})"
            for index in range(start, min(start + 500, len(vectors)))
        )
        db.execute_sql(f"INSERT INTO hybrid_rows VALUES {values}")

    def search(query, k: int, ef_search: int) -> list[int]:
        results, _ = db.search_hnsw(
            "hybrid_vectors", query, k, ef_search=max(k, ef_search)
        )
        return [int(result.key.decode()) for result in results]

    def select_ids(threshold: int) -> list[int]:
        return [
            int(row["id"])
            for row in db.execute_sql(
                f"SELECT id FROM hybrid_rows WHERE bucket < {threshold}"
            )
        ]

    def close() -> None:
        db.close()

    return (
        SearchEngine(
            "alopex-hybrid",
            time.perf_counter() - started,
            search,
            close,
            0,
            peak_rss_bytes(),
            node_count=len(vectors),
        ),
        select_ids,
    )


def _sqlite_filter_catalog(row_count: int):
    import sqlite3

    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE hybrid_rows (id INTEGER PRIMARY KEY, bucket INTEGER)"
    )
    connection.executemany(
        "INSERT INTO hybrid_rows VALUES (?, ?)",
        ((index, index % 1000) for index in range(row_count)),
    )

    def select_ids(threshold: int) -> list[int]:
        return [
            int(row[0])
            for row in connection.execute(
                "SELECT id FROM hybrid_rows WHERE bucket < ?", (threshold,)
            )
        ]

    return connection, select_ids


def _hybrid_search(
    search: Callable[[object, int, int], list[int]],
    select_ids: Callable[[int], list[int]],
    query,
    selectivity: float,
    row_count: int,
    *,
    k: int = 10,
) -> tuple[list[int], int, int]:
    allowed = set(select_ids(max(1, round(selectivity * 1000))))
    requested = min(k, row_count)
    cap = min(row_count, k * 100)
    selected = []
    while requested <= cap:
        selected = [
            index
            for index in search(query, requested, max(64, requested))
            if index in allowed
        ][:k]
        if len(selected) >= min(k, len(allowed)) or requested == cap:
            break
        requested = min(cap, requested * 2)
    return selected, requested, len(allowed)


def measure_hybrid(
    vectors,
    queries,
    *,
    duration_seconds: float,
    min_queries: int,
    run_count: int,
) -> list[dict[str, object]]:
    import numpy as np

    alopex_engine, alopex_filter = _alopex_hybrid(vectors)
    hnsw_engine = build_hnswlib(vectors)
    sqlite_connection, sqlite_filter = _sqlite_filter_catalog(len(vectors))

    def exact(query, _k: int, _ef: int, selectivity: float) -> list[int]:
        allowed = sqlite_filter(max(1, round(selectivity * 1000)))
        if not allowed:
            return []
        scores = vectors[allowed] @ query
        return [allowed[index] for index in np.argsort(-scores, kind="stable")[:10]]

    rows = []
    try:
        for selectivity in (0.001, 0.01, 0.05, 0.2, 1.0):
            threshold = max(1, round(selectivity * 1000))
            allowed = sqlite_filter(threshold)
            expected = [exact(query, 10, 0, selectivity) for query in queries]
            arms = (
                (
                    "alopex-sql-hnsw-postfilter",
                    lambda query: _hybrid_search(
                        alopex_engine.search,
                        alopex_filter,
                        query,
                        selectivity,
                        len(vectors),
                    ),
                ),
                (
                    "sqlite-hnswlib",
                    lambda query: _hybrid_search(
                        hnsw_engine.search,
                        sqlite_filter,
                        query,
                        selectivity,
                        len(vectors),
                    ),
                ),
                (
                    "filtered-exact",
                    lambda query: (
                        exact(query, 10, 0, selectivity),
                        len(allowed),
                        len(allowed),
                    ),
                ),
            )
            for arm, operation in arms:
                predictions = []
                requested = []
                for query in queries:
                    result, overfetch, _ = operation(query)
                    predictions.append(result)
                    requested.append(overfetch)
                denominator = max(1, min(10, len(allowed)))
                accuracy = statistics.fmean(
                    len(set(actual).intersection(reference)) / denominator
                    for actual, reference in zip(predictions, expected)
                )
                for run in range(1, run_count + 1):
                    latencies = []
                    started = time.perf_counter()
                    executed = 0
                    while (
                        executed < min_queries
                        or time.perf_counter() - started < duration_seconds
                    ):
                        query_started = time.perf_counter_ns()
                        operation(queries[executed % len(queries)])
                        latencies.append(
                            (time.perf_counter_ns() - query_started) / 1000
                        )
                        executed += 1
                    elapsed = time.perf_counter() - started
                    ordered = sorted(latencies)
                    rows.append(
                        {
                            "arm": arm,
                            "selectivity": selectivity,
                            "run": run,
                            "query_count": executed,
                            "duration_seconds": elapsed,
                            "latency_p50_us": statistics.median(latencies),
                            "latency_p95_us": ordered[
                                (95 * len(ordered) + 99) // 100 - 1
                            ],
                            "filtered_top_k_accuracy": accuracy,
                            "median_overfetch_amplification": statistics.median(
                                requested
                            )
                            / 10,
                            "eligible_rows": len(allowed),
                            "returns_k": all(
                                len(result) == min(10, len(allowed))
                                for result in predictions
                            ),
                        }
                    )
    finally:
        alopex_engine.close()
        hnsw_engine.close()
        sqlite_connection.close()
    return rows


def summarize_hybrid(rows: list[dict[str, object]]) -> dict[str, object]:
    grouped: dict[tuple[str, float], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["arm"]), float(row["selectivity"]))].append(row)
    summary = [
        {
            "arm": arm,
            "selectivity": selectivity,
            "median_latency_p50_us": statistics.median(
                float(row["latency_p50_us"]) for row in values
            ),
            "median_latency_p95_us": statistics.median(
                float(row["latency_p95_us"]) for row in values
            ),
            "filtered_top_k_accuracy": statistics.median(
                float(row["filtered_top_k_accuracy"]) for row in values
            ),
            "median_overfetch_amplification": statistics.median(
                float(row["median_overfetch_amplification"]) for row in values
            ),
            "returns_k": all(bool(row["returns_k"]) for row in values),
        }
        for (arm, selectivity), values in sorted(grouped.items())
    ]
    advantageous = []
    for selectivity in sorted({float(row["selectivity"]) for row in summary}):
        values = [row for row in summary if row["selectivity"] == selectivity]
        fastest = min(values, key=lambda row: float(row["median_latency_p50_us"]))
        if fastest["arm"] == "alopex-sql-hnsw-postfilter":
            advantageous.append(selectivity)
    return {
        "runs": rows,
        "summary": summary,
        "alopex_advantageous_selectivities": advantageous,
        "breakpoints": [
            {"arm": row["arm"], "selectivity": row["selectivity"]}
            for row in summary
            if not row["returns_k"]
        ],
        "filter_aware_traversal": False,
    }


GLOVE_SHA256 = "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4"
SCALE_SIZES = (10_000, 50_000, 200_000, 1_000_000)


def run_scale_benchmark(
    dataset: Path,
    *,
    max_n: int,
    duration_seconds: float,
    min_queries: int,
    run_count: int,
) -> dict[str, object]:
    import h5py
    import numpy as np

    digest = hashlib.sha256(dataset.read_bytes()).hexdigest()
    if digest != GLOVE_SHA256:
        raise ValueError(f"unexpected glove-100-angular checksum: {digest}")
    results = []
    raw_runs = []
    with h5py.File(dataset) as source:
        queries = np.asarray(source["test"][:200], dtype=np.float32)
        queries /= np.linalg.norm(queries, axis=1, keepdims=True)
        for size in SCALE_SIZES:
            if size > max_n or size > len(source["train"]):
                continue
            vectors = np.asarray(source["train"][:size], dtype=np.float32)
            vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
            oracle = build_faiss_flat(vectors)
            try:
                truth = [oracle.search(query, 10, size) for query in queries]
            finally:
                oracle.close()
            acceptable = [list(row) for row in truth]
            for builder in (
                build_alopex,
                build_faiss_hnsw,
                build_hnswlib,
                build_faiss_flat,
            ):
                engine = builder(vectors)
                try:
                    ef_values = (
                        (size,)
                        if engine.name == "faiss-flat-exact"
                        else EF_SEARCH_VALUES
                    )
                    runs = []
                    for ef_search in ef_values:
                        runs.extend(
                            measure_setting(
                                engine,
                                queries,
                                truth,
                                acceptable,
                                ef_search=ef_search,
                                duration_seconds=duration_seconds,
                                min_queries=min_queries,
                                run_count=run_count,
                                dataset_size=size,
                            )
                        )
                    summary = summarize_by_engine(runs)
                    raw_runs.extend(runs)
                    eligible = [
                        row
                        for row in summary
                        if float(row["median_recall_at_10"]) >= 0.95
                    ]
                    fastest = max(
                        eligible,
                        key=lambda row: float(row["median_queries_per_second"]),
                        default=None,
                    )
                    results.append(
                        {
                            "dataset_size": size,
                            "engine": (
                                "flat"
                                if engine.name == "faiss-flat-exact"
                                else engine.name
                            ),
                            "build_time_seconds": engine.build_time_seconds,
                            "index_size_bytes": engine.index_size_bytes,
                            "peak_rss_bytes": engine.peak_rss_bytes,
                            "qps_at_recall_095": (
                                float(fastest["median_queries_per_second"])
                                if fastest
                                else 0.0
                            ),
                            "ef_search_at_recall_095": (
                                fastest["ef_search"] if fastest else None
                            ),
                            "recall_at_selected_setting": (
                                fastest["median_recall_at_10"] if fastest else None
                            ),
                            "curve": summary,
                        }
                    )
                finally:
                    engine.close()
                    del engine
            del vectors
    analysis = analyze_scale(results, requested_sizes=SCALE_SIZES)
    for limit in analysis["limits"]:
        limit["reason"] = (
            f"configured max_n={max_n} for the hosted runner"
            if int(limit["dataset_size"]) > max_n
            else "dataset contains fewer rows"
        )
    return {
        "dataset": "glove-100-angular",
        "sha256": digest,
        "requested_sizes": list(SCALE_SIZES),
        "max_n": max_n,
        "results": results,
        "runs": raw_runs,
        **analysis,
    }


def run_benchmark(
    dataset: Path,
    *,
    duration_seconds: float,
    min_queries: int,
    run_count: int,
    extended: bool = True,
) -> tuple[
    list[dict[str, object]],
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    dict[str, object],
]:
    import numpy as np

    products = load_amazon_products(dataset)
    if len(products) != DATASET_SIZE:
        raise ValueError(
            f"expected {DATASET_SIZE} cleaned products, found {len(products)}"
        )
    vectors, explained_variance = build_embeddings(products)
    rng = np.random.default_rng(SEED)
    query_indexes = rng.choice(len(vectors), size=200, replace=False)
    queries = vectors[query_indexes]
    truth, acceptable_truth = exact_ground_truth(vectors, queries, 10)
    runs: list[dict[str, object]] = []
    builds = []
    recall_ceiling = []
    recall_investigation = {}
    builders = (build_hnswlib, build_alopex, build_faiss_flat, build_faiss_hnsw)
    for builder in builders:
        engine = builder(vectors)
        try:
            builds.append(
                {
                    "engine": engine.name,
                    "build_time_seconds": engine.build_time_seconds,
                    "index_size_bytes": engine.index_size_bytes,
                    "peak_rss_bytes": engine.peak_rss_bytes,
                    "update_latency_ms": engine.update_latency_ms,
                    "delete_latency_ms": engine.delete_latency_ms,
                    "reopen_latency_ms": engine.reopen_latency_ms,
                    "node_count": engine.node_count,
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
            if extended and engine.name == "alopex-hnsw":
                recall_investigation = investigate_recall_contract(
                    vectors, queries, truth, acceptable_truth, engine
                )
        finally:
            engine.close()
            del engine
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
    if not extended:
        return runs, metadata, builds, recall_ceiling, {}
    fixed_cost_runs = measure_call_floor(
        vectors,
        duration_seconds=duration_seconds,
        min_queries=min_queries,
        run_count=run_count,
    )
    fixed_cost_us = {
        engine: statistics.median(float(row["latency_us"]) for row in rows)
        for engine, rows in (
            (
                engine,
                [row for row in fixed_cost_runs if row["engine"] == engine],
            )
            for engine in sorted({str(row["engine"]) for row in fixed_cost_runs})
        )
    }
    hybrid_runs = measure_hybrid(
        vectors,
        queries,
        duration_seconds=duration_seconds,
        min_queries=min_queries,
        run_count=run_count,
    )
    base_summary = summarize_by_engine(runs)
    recall_investigation["m_semantics"] = {
        "alopex_m16_layer_zero_capacity": 32,
        "alopex_m32_layer_zero_capacity": 64,
        "competitor_m16_layer_zero_capacity": 32,
        "note": "Alopex and the references cap layer zero at 2*M; the M=32 arm therefore has twice the layer-zero capacity of competitor M=16.",
        "alopex_m32_result": next(
            (
                row
                for row in recall_investigation["ef_construction_and_m"]
                if row["m"] == 32
            ),
            None,
        ),
        "ef64_results": [
            row
            for row in base_summary
            if row["ef_search"] == 64 and row["engine"] != "faiss-flat-exact"
        ],
    }
    diagnostics = {
        "recall_investigation": recall_investigation,
        "fixed_cost_runs": fixed_cost_runs,
        "latency_decomposition": decompose_latency(base_summary, fixed_cost_us),
        "hybrid": summarize_hybrid(hybrid_runs),
    }
    return runs, metadata, builds, recall_ceiling, diagnostics


def write_artifacts(
    output: Path,
    runs: list[dict[str, object]],
    release_version: str | None = None,
    *,
    dataset: dict[str, object] | None = None,
    builds: list[dict[str, object]] | None = None,
    recall_ceiling: list[dict[str, object]] | None = None,
    diagnostics: dict[str, object] | None = None,
    scale: dict[str, object] | None = None,
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    summary = summarize_by_engine(runs)
    payload = {
        "schema": "alopex.hnsw-diagnostic/v3",
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
                "query_latency_p50_us",
                "query_latency_p95_us",
                "query_latency_p99_us",
                "native_search_latency_us",
                "python_binding_residual_us",
                "queries_per_second",
                "build_time_seconds",
                "index_size_bytes",
                "peak_rss_bytes",
                "update_latency_ms",
                "delete_latency_ms",
                "reopen_latency_ms",
            ],
        },
        "dataset": dataset or {},
        "builds": builds or [],
        "recall_ceiling": recall_ceiling or [],
        "recall_investigation": (diagnostics or {}).get("recall_investigation", {}),
        "fixed_cost_runs": (diagnostics or {}).get("fixed_cost_runs", []),
        "latency_decomposition": (diagnostics or {}).get("latency_decomposition", []),
        "hybrid": (diagnostics or {}).get("hybrid", {}),
        "scale": scale or {},
        "runs": runs,
        "summary": summary,
        "best_at_recall": best_at_recall(summary),
        "median_queries_per_second": statistics.median(
            float(run["queries_per_second"]) for run in runs
        ),
    }
    if release_version is not None:
        payload["release_version"] = release_version
    raw = {
        "schema": "alopex.hnsw-diagnostic-raw/v3",
        "runs": runs,
        "fixed_cost_runs": payload["fixed_cost_runs"],
        "hybrid_runs": payload["hybrid"].get("runs", []),
        "scale_runs": payload["scale"].get("runs", []),
    }
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
    for filename, rows in (
        ("hnsw-latency-decomposition.csv", payload["latency_decomposition"]),
        ("hnsw-hybrid.csv", payload["hybrid"].get("runs", [])),
        ("hnsw-scale.csv", payload["scale"].get("results", [])),
    ):
        if not rows:
            continue
        columns = sorted({key for row in rows for key in row if key != "curve"})
        with (output / filename).open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
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
        + "\n\n## Recall ceiling conclusion\n\n"
        + str(payload["recall_investigation"].get("conclusion", "not measured"))
        + "\n\n## Hybrid\n\nAlopex advantageous selectivities: `"
        + json.dumps(payload["hybrid"].get("alopex_advantageous_selectivities", []))
        + "`. Filter-aware traversal: `false`.\n\n## Scale\n\n"
        + "Brute-force crossovers: `"
        + json.dumps(payload["scale"].get("brute_force_crossover", {}), sort_keys=True)
        + "`. Limits: `"
        + json.dumps(payload["scale"].get("limits", []), sort_keys=True)
        + "`.\n",
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
    parser.add_argument("--glove-dataset", type=Path)
    parser.add_argument("--max-scale-n", type=int, default=50_000)
    parser.add_argument("--baseline-only", action="store_true")
    args = parser.parse_args()
    if args.duration_seconds < WARMUP_SECONDS:
        parser.error("duration must be at least 2 seconds")
    if args.min_queries < QUERY_COUNT:
        parser.error("min-queries must be at least 10000")
    if args.runs != 3:
        parser.error("runs must be exactly 3")
    benchmark_runs, dataset, builds, recall_ceiling, diagnostics = run_benchmark(
        args.dataset,
        duration_seconds=args.duration_seconds,
        min_queries=args.min_queries,
        run_count=args.runs,
        extended=not args.baseline_only,
    )
    scale = (
        run_scale_benchmark(
            args.glove_dataset,
            max_n=args.max_scale_n,
            duration_seconds=args.duration_seconds,
            min_queries=args.min_queries,
            run_count=args.runs,
        )
        if args.glove_dataset and not args.baseline_only
        else {}
    )
    write_artifacts(
        args.output,
        benchmark_runs,
        release_version=args.release_version,
        dataset=dataset,
        builds=builds,
        recall_ceiling=recall_ceiling,
        diagnostics=diagnostics,
        scale=scale,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
