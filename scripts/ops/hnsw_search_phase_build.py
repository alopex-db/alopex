"""Pinned builders with diagnostics retained only outside the query timer."""
from __future__ import annotations
from pathlib import Path
import statistics
import tempfile
import time
from hnsw_v0811_contract import (SearchEngine, peak_rss_bytes, recall_at_k,
    tie_aware_recall_at_k)

def build_alopex(vectors, *, m: int = 16, ef_construction: int = 200) -> SearchEngine:
    import alopex

    # The parent removes this cell's scratch directory only after wait().
    # Worker-side rmtree races the strict live storage census.
    directory = Path(tempfile.mkdtemp(prefix="alopex-index-"))
    database_path = directory / "index"
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
        transaction.upsert_to_hnsw_batch(
            name, [str(index).encode() for index in range(len(vectors))], vectors
        )
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

    native_observations = []

    def native_search_time_us(query, k: int, ef_search: int) -> int:
        _, stats = db.search_hnsw(name, query, k, ef_search=ef_search)
        native_observations.append(dict(nodes_visited=stats.nodes_visited,
            distance_computations=stats.distance_computations, search_time_us=stats.search_time_us))
        return stats.search_time_us

    def close() -> None:
        db.drop_hnsw_index(name)
        db.close()

    engine = SearchEngine(
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
    engine.native_observations = native_observations
    engine.diagnostic_search = lambda query, k, ef: db.search_hnsw(name, query, k, ef_search=ef)
    return engine


def _prepare_setting(
    engine: SearchEngine,
    queries,
    ground_truth,
    acceptable_truth,
    *,
    ef_search: int,
    k: int = 10,
) -> tuple[float, float, float | None]:
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
    return (recall, tie_aware_recall, native_latency_us), predicted
