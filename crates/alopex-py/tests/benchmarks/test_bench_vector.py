import pytest

pytest.importorskip("pytest_benchmark")
np = pytest.importorskip("numpy")

import alopex

from .baseline_numpy import cosine_similarity_loop, cosine_similarity_vectorized


pytestmark = [
    pytest.mark.requires_numpy,
    pytest.mark.requires_pytest_benchmark,
]


SCENARIOS = {
    "small": {"dim": 128, "count": 1_000, "k": 10},
    "medium": {"dim": 768, "count": 10_000, "k": 50},
    "large": {"dim": 1536, "count": 100_000, "k": 100},
}


def _make_vectors(count: int, dim: int, seed: int):
    rng = np.random.default_rng(seed)
    vectors = rng.standard_normal((count, dim)).astype(np.float32, copy=False)
    query = rng.standard_normal((dim,)).astype(np.float32, copy=False)
    return vectors, query


def _populate_vector_store(db: "alopex.Database", vectors):
    with db.begin(alopex.TxnMode.READ_WRITE) as txn:
        for i in range(vectors.shape[0]):
            txn.upsert_vector(
                key=f"vec_{i}".encode(),
                metadata=None,
                vector=vectors[i],
                metric=alopex.Metric.COSINE,
            )
        txn.commit()


@pytest.fixture(scope="module")
def vector_env(request):
    scenario = request.param
    cfg = SCENARIOS[scenario]
    dim, count, k = cfg["dim"], cfg["count"], cfg["k"]

    vectors, query = _make_vectors(count=count, dim=dim, seed=1)
    db = alopex.Database.new()
    _populate_vector_store(db, vectors)
    txn = db.begin()

    yield {
        "scenario": scenario,
        "dim": dim,
        "count": count,
        "k": k,
        "vectors": vectors,
        "query": query,
        "db": db,
        "txn": txn,
    }

    try:
        txn.rollback()
    finally:
        db.close()


@pytest.mark.parametrize(
    "vector_env",
    [
        pytest.param("small", id="small"),
        pytest.param("medium", id="medium"),
        pytest.param("large", marks=pytest.mark.large, id="large"),
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "query_kind",
    [
        pytest.param("f32_c", id="f32_c"),
        pytest.param("f64_c", id="f64_c"),
        pytest.param("strided", id="strided"),
    ],
)
def test_bench_vector_array_conversion_overhead(benchmark, vector_env, query_kind):
    dim, k = vector_env["dim"], vector_env["k"]
    txn = vector_env["txn"]

    if query_kind == "f32_c":
        query = vector_env["query"]
        iterations = 200
    elif query_kind == "f64_c":
        query = vector_env["query"].astype(np.float64, copy=True)
        iterations = 50
    elif query_kind == "strided":
        query = np.arange(dim * 2, dtype=np.float32)[::2]
        iterations = 50
    else:
        raise ValueError(f"unknown query_kind: {query_kind}")

    def run():
        _ = txn.search_similar(query, alopex.Metric.COSINE, k=k)

    benchmark.pedantic(
        run,
        iterations=iterations,
        rounds=1,
        warmup_rounds=1,
    )


@pytest.mark.parametrize(
    "vector_env",
    [
        pytest.param("small", id="small"),
        pytest.param("medium", id="medium"),
        pytest.param("large", marks=pytest.mark.large, id="large"),
    ],
    indirect=True,
)
def test_bench_vector_search_latency(benchmark, vector_env):
    k = vector_env["k"]
    query = vector_env["query"]
    txn = vector_env["txn"]

    def run():
        _ = txn.search_similar(query, alopex.Metric.COSINE, k=k)

    benchmark.pedantic(run, iterations=10, rounds=1, warmup_rounds=1)


@pytest.mark.parametrize(
    "vector_env",
    [
        pytest.param("small", id="small"),
        pytest.param("medium", id="medium"),
        pytest.param("large", marks=pytest.mark.large, id="large"),
    ],
    indirect=True,
)
def test_bench_numpy_loop_baseline(benchmark, vector_env):
    def run():
        _ = cosine_similarity_loop(vector_env["query"], vector_env["vectors"])

    benchmark.pedantic(run, iterations=1, rounds=1, warmup_rounds=0)


@pytest.mark.parametrize(
    "vector_env",
    [
        pytest.param("small", id="small"),
        pytest.param("medium", id="medium"),
        pytest.param("large", marks=pytest.mark.large, id="large"),
    ],
    indirect=True,
)
def test_bench_numpy_vectorized_reference(benchmark, vector_env):
    def run():
        _ = cosine_similarity_vectorized(vector_env["query"], vector_env["vectors"])

    benchmark.pedantic(run, iterations=1, rounds=1, warmup_rounds=0)
