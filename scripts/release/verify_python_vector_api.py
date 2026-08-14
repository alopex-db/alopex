#!/usr/bin/env python3
"""Verify that an installed alopex wheel exposes and executes its vector API."""

from __future__ import annotations

import numpy as np

from alopex import Database, HnswConfig, Metric, Transaction, TxnMode


DATABASE_METHODS = (
    "create_hnsw_index",
    "search_hnsw",
    "drop_hnsw_index",
    "get_hnsw_stats",
)
TRANSACTION_METHODS = (
    "upsert_vector",
    "search_similar",
    "get_vector",
    "upsert_to_hnsw",
    "delete_from_hnsw",
)


def main() -> None:
    missing = [name for name in DATABASE_METHODS if not hasattr(Database, name)]
    missing.extend(name for name in TRANSACTION_METHODS if not hasattr(Transaction, name))
    if missing:
        raise AssertionError(f"installed wheel is missing vector APIs: {missing}")

    db = Database.new()
    try:
        db.create_hnsw_index("wheel_vector_smoke", HnswConfig(2, metric=Metric.L2))
        with db.begin(TxnMode.READ_WRITE) as txn:
            txn.upsert_to_hnsw(
                "wheel_vector_smoke",
                b"point",
                np.array([1.0, 0.0], dtype=np.float32),
            )
            txn.upsert_to_hnsw(
                "wheel_vector_smoke",
                b"quarter",
                np.array([0.75, 0.0], dtype=np.float32),
            )
            txn.commit()

        results, stats = db.search_hnsw(
            "wheel_vector_smoke",
            np.array([1.0, 0.0], dtype=np.float32),
            2,
        )
        assert len(results) == 2
        assert results[0].key == b"point"
        assert results[0].score == 0.0
        assert results[1].key == b"quarter"
        assert abs(results[1].score - 0.25) < np.finfo(np.float32).eps
        assert stats.node_count == 2
        print("installed wheel exposes all Database and Transaction vector APIs")
        print(f"nearest={results[0].key!r} score={results[0].score}")
        print(f"second={results[1].key!r} score={results[1].score}")
        print(f"node_count={stats.node_count}")
        db.drop_hnsw_index("wheel_vector_smoke")
    finally:
        db.close()


if __name__ == "__main__":
    main()
