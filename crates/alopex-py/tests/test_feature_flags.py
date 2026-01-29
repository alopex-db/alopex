import importlib.util

import pytest

from alopex import AlopexError, Database, Metric, Transaction, TxnMode


def test_numpy_feature_flags():
    has_numpy = importlib.util.find_spec("numpy") is not None
    has_vector_api = hasattr(Transaction, "upsert_vector")

    if not has_vector_api:
        assert not hasattr(Transaction, "search_similar")
        assert not hasattr(Database, "search_hnsw")
        assert not hasattr(Transaction, "upsert_to_hnsw")
        assert not hasattr(Transaction, "delete_from_hnsw")
        assert not hasattr(Database, "create_hnsw_index")
        assert not hasattr(Database, "drop_hnsw_index")
        assert not hasattr(Database, "get_hnsw_stats")
        return

    if not has_numpy:
        db = Database.new()
        txn = db.begin(TxnMode.READ_WRITE)
        with pytest.raises(AlopexError):
            txn.upsert_vector(b"k1", None, [1.0], Metric.COSINE)
        return

    assert has_vector_api is True
