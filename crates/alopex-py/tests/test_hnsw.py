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


@pytest.mark.requires_numpy
def test_hnsw_multithreaded_search_releases_gil():
    import time
    import threading
    from concurrent.futures import ThreadPoolExecutor

    import numpy as np

    db = Database.new()
    name = "idx_mt"
    dim = 64
    count = 500
    k = 10
    ef_search = 100

    db.create_hnsw_index(name, HnswConfig(dim))
    rng = np.random.default_rng(0)
    vectors = rng.standard_normal((count, dim)).astype(np.float32, copy=False)
    query = rng.standard_normal((dim,)).astype(np.float32, copy=False)

    with db.begin(TxnMode.READ_WRITE) as txn:
        for i in range(count):
            txn.upsert_to_hnsw(name, f"k{i}".encode(), vectors[i], None)
        txn.commit()

    threads = 4
    repeats = 3
    barrier = threading.Barrier(threads)

    def worker():
        barrier.wait()
        started = time.perf_counter()
        for _ in range(repeats):
            results, _stats = db.search_hnsw(name, query, k, ef_search=ef_search)
            assert len(results) == k
        finished = time.perf_counter()
        return started, finished

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker) for _ in range(threads)]
        intervals = [future.result() for future in futures]

    overlap = False
    for i in range(len(intervals)):
        s1, e1 = intervals[i]
        for j in range(i + 1, len(intervals)):
            s2, e2 = intervals[j]
            if max(s1, s2) < min(e1, e2):
                overlap = True
                break
        if overlap:
            break

    assert overlap, "search_hnsw calls did not overlap; GIL may not be released during search"
