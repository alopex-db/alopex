import pytest

from alopex import Database, Metric, TxnMode


@pytest.mark.requires_numpy
def test_upsert_vector_and_search_similar():
    import numpy as np

    db = Database.new()
    with db.begin(TxnMode.READ_WRITE) as txn:
        vec1 = np.array([1.0, 0.0, 0.0], dtype=np.float64)
        vec2 = np.array([0.0, 1.0, 0.0], dtype=np.float32)
        txn.upsert_vector(b"k1", None, vec1, Metric.COSINE)
        txn.upsert_vector(b"k2", b"meta", vec2, Metric.COSINE)
        results = txn.search_similar(vec1, Metric.COSINE, 2)

    assert len(results) >= 1
    keys = {result.key for result in results}
    assert b"k1" in keys
