#!/usr/bin/env python3
"""Exercise vector, HNSW, and streaming APIs from an installed wheel."""

from __future__ import annotations

import numpy as np

from alopex import Database, HnswConfig, LocalScan, Metric, Transaction, TxnMode


DATABASE_METHODS = (
    "create_hnsw_index",
    "search_hnsw",
    "drop_hnsw_index",
    "get_hnsw_stats",
    "execute_sql_stream",
    "query_stream",
)
TRANSACTION_METHODS = (
    "upsert_vector",
    "search_similar",
    "get_vector",
    "upsert_to_hnsw",
    "delete_from_hnsw",
    "execute_sql_stream",
    "query_stream",
)


def main() -> None:
    missing = [name for name in DATABASE_METHODS if not hasattr(Database, name)]
    missing.extend(name for name in TRANSACTION_METHODS if not hasattr(Transaction, name))
    if missing:
        raise AssertionError(f"installed wheel is missing callable APIs: {missing}")

    db = Database.new()
    try:
        db.execute_sql("CREATE TABLE stream_smoke (id INTEGER PRIMARY KEY)")
        db.execute_sql("INSERT INTO stream_smoke VALUES (1), (2)")
        assert list(db.execute_sql_stream("SELECT id FROM stream_smoke")) == [
            {"id": 1},
            {"id": 2},
        ]
        assert list(db.query_stream(LocalScan.table("stream_smoke"))) == [
            {"id": 1},
            {"id": 2},
        ]

        tx = db.begin(TxnMode.READ_ONLY)
        assert list(tx.execute_sql_stream("SELECT id FROM stream_smoke")) == [
            {"id": 1},
            {"id": 2},
        ]
        tx.rollback()
        tx = db.begin(TxnMode.READ_ONLY)
        assert list(tx.query_stream(LocalScan.table("stream_smoke"))) == [
            {"id": 1},
            {"id": 2},
        ]
        tx.rollback()

        db.create_hnsw_index("wheel_vector_smoke", HnswConfig(2, metric=Metric.L2))
        tx = db.begin(TxnMode.READ_WRITE)
        tx.upsert_vector(
            b"flat", None, np.array([1.0, 0.0], dtype=np.float32), Metric.L2
        )
        assert tx.search_similar(
            np.array([1.0, 0.0], dtype=np.float32), Metric.L2, 1
        )[0].key == b"flat"
        assert tx.get_vector(b"flat", Metric.L2).tolist() == [1.0, 0.0]
        tx.upsert_to_hnsw(
            "wheel_vector_smoke", b"point", np.array([1.0, 0.0], dtype=np.float32)
        )
        tx.upsert_to_hnsw(
            "wheel_vector_smoke", b"quarter", np.array([0.75, 0.0], dtype=np.float32)
        )
        tx.commit()

        results, stats = db.search_hnsw(
            "wheel_vector_smoke", np.array([1.0, 0.0], dtype=np.float32), 2
        )
        assert results[0].key == b"point" and results[0].score == 0.0
        assert results[1].key == b"quarter"
        assert abs(results[1].score - 0.25) < np.finfo(np.float32).eps
        assert stats.node_count == 2
        tx = db.begin(TxnMode.READ_WRITE)
        tx.delete_from_hnsw("wheel_vector_smoke", b"quarter")
        tx.commit()
        db.drop_hnsw_index("wheel_vector_smoke")
    finally:
        db.close()

    print("installed wheel exposes and executes vector/HNSW/streaming APIs")
    print("nearest=b'point' distance=0.0")
    print("second=b'quarter' distance=0.25")
    print("node_count=2")


if __name__ == "__main__":
    main()
