import pytest

from alopex import Database, HnswConfig, TxnMode


@pytest.mark.requires_numpy
def test_hnsw_create_search_delete():
    import numpy as np

    db = Database.new()
    db.create_hnsw_index("idx", HnswConfig(2))
    with db.begin(TxnMode.READ_WRITE) as txn:
        vec = np.array([1.0, 0.0], dtype=np.float32)
        txn.upsert_to_hnsw("idx", b"k1", vec, None)
        txn.commit()

    results, stats = db.search_hnsw("idx", np.array([1.0, 0.0], dtype=np.float32), 1)
    assert len(results) >= 1
    assert stats.node_count >= 1

    with db.begin(TxnMode.READ_WRITE) as txn:
        txn.delete_from_hnsw("idx", b"k1")
        txn.commit()

    db.drop_hnsw_index("idx")
