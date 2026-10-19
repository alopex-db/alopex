"""Gate 3 (#672): kNN inside db.begin() does not use the HNSW cache and reloads the whole
graph on every statement; the 256 MiB cache limit is not configurable from Python.

Two phases so the slow index build can be done once:
  python gate3_hnsw_cache.py build   DB_PATH [ROWS]   # create table + HNSW index (100k x 384: ~11 min)
  python gate3_hnsw_cache.py measure DB_PATH          # measurements below (~1 min)

Exit code (measure): 1 = defect reproduced, 0 = fixed.
"""
import resource
import sys
import time

import numpy as np

from alopex import Database, TxnMode

D = 384


def rss_mib():
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)


def timed(label, fn):
    t0 = time.perf_counter()
    r = fn()
    ms = (time.perf_counter() - t0) * 1000
    print(f"{label}: {ms:.1f} ms  maxRSS={rss_mib()} MiB", flush=True)
    return r, ms


def knn_sql(seed):
    v = np.random.default_rng(seed).standard_normal(D)
    v /= np.linalg.norm(v)
    lit = "[" + ",".join(f"{x:.6f}" for x in v) + "]"
    return f"SELECT id FROM ra ORDER BY vector_distance(e, {lit}, 'cosine') ASC LIMIT 10"


def build(path, rows):
    db = Database.open(path)
    q = db.execute_sql
    q(f"CREATE TABLE ra (id INTEGER PRIMARY KEY, e VECTOR({D}, COSINE))")
    V = np.random.default_rng(0).standard_normal((rows, D)).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True)
    ps = db.prepare("INSERT INTO ra VALUES (?, ?)")
    timed("insert", lambda: [ps.execute_many([[i, V[i].tolist()] for i in range(b, min(rows, b + 2000))])
                             for b in range(0, rows, 2000)])
    ps.finalize()
    timed("CREATE INDEX USING HNSW", lambda: q(
        "CREATE INDEX ra_h ON ra (e) USING HNSW WITH (m=16, ef_construction=100)"))
    timed("close", db.close)


def measure(path):
    db, _ = timed("open", lambda: Database.open(path))
    q = db.execute_sql
    print("HNSW memory_bytes:", db.get_hnsw_stats("ra_h").memory_bytes)
    print("Python cache API:", [m for m in dir(db) if "cache" in m.lower()] or "none")
    print("EXPLAIN:", q("EXPLAIN " + knn_sql(0))[0]["QUERY PLAN"].replace("\n", " | "))
    auto = [timed(f"auto-commit SELECT #{i + 1}", lambda: q(knn_sql(i)))[1] for i in range(5)]
    warm = min(auto[1:])
    in_txn = []
    for mode in ("READ_ONLY", "READ_WRITE"):
        tx = db.begin(getattr(TxnMode, mode))
        in_txn += [timed(f"inside db.begin({mode}) SELECT #{i + 1}", lambda: tx.execute_sql(knn_sql(i)))[1]
                   for i in range(3)]
        tx.rollback()
        del tx
    timed("close", db.close)

    problems = []
    if min(in_txn) > 20 * max(warm, 1.0):
        problems.append(f"kNN inside db.begin() takes {min(in_txn):.0f}-{max(in_txn):.0f} ms vs "
                        f"{warm:.1f} ms warm auto-commit (graph reloaded per statement)")
    if not any("cache" in m.lower() for m in dir(Database.open_in_memory())):
        problems.append("no Python API to set/inspect the HNSW cache limit (default 256 MiB)")
    for p in problems:
        print("  -> DEFECT:", p)
    print("RESULT:", "REPRODUCED (#672)" if problems else "not reproduced / fixed")
    return 1 if problems else 0


if __name__ == "__main__":
    mode, path = sys.argv[1], sys.argv[2]
    if mode == "build":
        build(path, int(sys.argv[3]) if len(sys.argv) > 3 else 100_000)
    else:
        sys.exit(measure(path))
