import threading

import pytest

pytest.importorskip("pytest_benchmark")
np = pytest.importorskip("numpy")

import alopex


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


def _populate_hnsw(db: "alopex.Database", index_name: str, vectors):
    cfg = alopex.HnswConfig(dim=int(vectors.shape[1]), metric=alopex.Metric.COSINE)
    db.create_hnsw_index(index_name, cfg)

    with db.begin(alopex.TxnMode.READ_WRITE) as txn:
        for i in range(vectors.shape[0]):
            txn.upsert_to_hnsw(index_name, key=f"hnsw_{i}".encode(), vector=vectors[i])
        txn.commit()


@pytest.fixture(scope="module")
def hnsw_env(request):
    scenario = request.param
    cfg = SCENARIOS[scenario]
    dim, count, k = cfg["dim"], cfg["count"], cfg["k"]

    vectors, query = _make_vectors(count=count, dim=dim, seed=2)
    db = alopex.Database.new()
    index_name = f"bench_{scenario}"
    _populate_hnsw(db, index_name, vectors)

    yield {
        "scenario": scenario,
        "dim": dim,
        "count": count,
        "k": k,
        "vectors": vectors,
        "query": query,
        "db": db,
        "index_name": index_name,
    }

    try:
        db.drop_hnsw_index(index_name)
    finally:
        db.close()


@pytest.mark.parametrize(
    "hnsw_env",
    [
        pytest.param("small", id="small"),
        pytest.param("medium", id="medium"),
        pytest.param("large", marks=pytest.mark.large, id="large"),
    ],
    indirect=True,
)
def test_bench_hnsw_search_latency(benchmark, hnsw_env):
    db = hnsw_env["db"]
    index_name = hnsw_env["index_name"]
    query = hnsw_env["query"]
    k = hnsw_env["k"]

    def run():
        _ = db.search_hnsw(index_name, query, k=k)

    benchmark.pedantic(run, iterations=10, rounds=1, warmup_rounds=1)


@pytest.mark.parametrize(
    "hnsw_env",
    [
        pytest.param("small", id="small"),
        pytest.param("medium", id="medium"),
        pytest.param("large", marks=pytest.mark.large, id="large"),
    ],
    indirect=True,
)
def test_bench_hnsw_multithread_throughput_4_threads(benchmark, hnsw_env):
    db = hnsw_env["db"]
    index_name = hnsw_env["index_name"]
    query = hnsw_env["query"]
    k = hnsw_env["k"]

    threads = 4
    total_queries = 200
    per_thread = total_queries // threads

    def worker():
        for _ in range(per_thread):
            _ = db.search_hnsw(index_name, query, k=k)

    def run():
        ts = [threading.Thread(target=worker) for _ in range(threads)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    benchmark.pedantic(run, iterations=1, rounds=5, warmup_rounds=1)
