import pytest

from alopex import Database, Metric, TxnMode


def _make_strided_f32(np, base):
    arr = np.asarray(base, dtype=np.float32)
    buf = np.zeros(arr.size * 2, dtype=np.float32)
    buf[::2] = arr
    return buf[::2]


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


@pytest.mark.requires_numpy
@pytest.mark.parametrize(
    "query_factory",
    [
        pytest.param(
            lambda np, base: np.array(base, dtype=np.float32),
            id="f32_c_contig",
        ),
        pytest.param(
            lambda np, base: np.array(base, dtype=np.float64),
            id="f64_cast",
        ),
        pytest.param(
            lambda np, base: _make_strided_f32(np, base),
            id="f32_strided_view",
        ),
    ],
)
def test_search_similar_accepts_dtype_and_layout_variations(query_factory):
    import numpy as np

    db = Database.new()
    with db.begin(TxnMode.READ_WRITE) as txn:
        vec_a = np.array([1.0, 0.0, 0.0], dtype=np.float32)
        vec_b = np.array([0.0, 1.0, 0.0], dtype=np.float32)
        txn.upsert_vector(b"a", None, vec_a, Metric.COSINE)
        txn.upsert_vector(b"b", None, vec_b, Metric.COSINE)

        query = query_factory(np, [1.0, 0.0, 0.0])
        assert query.dtype in (np.float32, np.float64)
        results = txn.search_similar(query, Metric.COSINE, 2)

    keys = {r.key for r in results}
    assert b"a" in keys


@pytest.mark.requires_numpy
@pytest.mark.parametrize(
    "zero_copy_return",
    [pytest.param(True, id="zero_copy_return_true"), pytest.param(False, id="zero_copy_return_false")],
)
def test_search_similar_return_vectors_includes_ndarray(zero_copy_return):
    import numpy as np

    db = Database.new()
    with db.begin(TxnMode.READ_WRITE) as txn:
        vec = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        txn.upsert_vector(b"k", None, vec, Metric.COSINE)
        results = txn.search_similar(
            vec.astype(np.float64),
            Metric.COSINE,
            1,
            return_vectors=True,
            zero_copy_return=zero_copy_return,
        )

    assert len(results) == 1
    assert results[0].key == b"k"
    assert results[0].vector is not None
    assert isinstance(results[0].vector, np.ndarray)
    assert results[0].vector.dtype == np.float32
    np.testing.assert_allclose(results[0].vector, vec, rtol=0, atol=0)


@pytest.mark.requires_numpy
@pytest.mark.parametrize(
    "vector_factory",
    [
        pytest.param(lambda np: np.array([4.0, 5.0, 6.0], dtype=np.float32), id="f32_c_contig"),
        pytest.param(lambda np: np.array([4.0, 5.0, 6.0], dtype=np.float64), id="f64_cast"),
        pytest.param(
            lambda np: _make_strided_f32(np, [4.0, 5.0, 6.0]),
            id="f32_strided_view",
        ),
    ],
)
def test_upsert_vector_accepts_dtype_and_layout_variations(vector_factory):
    import numpy as np

    db = Database.new()
    with db.begin(TxnMode.READ_WRITE) as txn:
        vec = vector_factory(np)
        txn.upsert_vector(b"k", None, vec, Metric.COSINE)
        out = txn.get_vector(b"k", Metric.COSINE, zero_copy_return=True)

    assert isinstance(out, np.ndarray)
    assert out.dtype == np.float32
    np.testing.assert_allclose(out, np.array([4.0, 5.0, 6.0], dtype=np.float32), rtol=0, atol=0)
