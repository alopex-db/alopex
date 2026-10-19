"""TRUNCATE inside db.begin() leaves the old HNSW graph behind (regression from #676).

Exit 1 if reproduced, 0 if fixed.
"""
import sys
import tempfile

import numpy as np
from alopex import Database, TxnMode

D, N, KEEP = 64, 3000, 500


def lit(v):
    return "[" + ",".join(f"{x:.5f}" for x in v) + "]"


def knn(v, k=5, opt="enable_hnsw = true"):
    return f"SELECT id FROM it ORDER BY vector_distance(e, {lit(v)}, 'l2') ASC LIMIT {k} WITH ({opt})"


rng = np.random.default_rng(3)
old = rng.standard_normal((N, D)).astype(np.float32)
new = rng.standard_normal((N, D)).astype(np.float32) * 3 + 20

failed = False
for mode in ("txn", "autocommit"):
    path = tempfile.mkdtemp() + "/a.alopex"
    db = Database.open(path)
    q = db.execute_sql
    q(f"CREATE TABLE it (id INTEGER PRIMARY KEY, e VECTOR({D}, L2))")
    for b in range(0, N, 100):
        q("INSERT INTO it VALUES " + ",".join(f"({i},{lit(old[i])})" for i in range(b, b + 100)))
    q("CREATE INDEX ih ON it (e) USING HNSW")
    q(knn(old[0]))  # warm the DB-level HNSW cache

    if mode == "txn":
        tx = db.begin(TxnMode.READ_WRITE)
        run = tx.execute_sql
    else:
        run = q
    run("TRUNCATE it")
    for b in range(0, KEEP, 100):
        run("INSERT INTO it VALUES " + ",".join(f"({i},{lit(new[i])})" for i in range(b, b + 100)))
    if mode == "txn":
        tx.commit()
        del tx

    rows = q("SELECT COUNT(*) AS n FROM it")[0]["n"]
    st = db.get_hnsw_stats("ih")
    wrong = 0
    for j in range(0, KEEP, 25):
        for src in (old, new):
            h = {r["id"] for r in q(knn(src[j]))}
            e = {r["id"] for r in q(knn(src[j], opt="enable_hnsw = false"))}
            wrong += len(h - e) > 1  # allow normal ANN recall loss
    db.close()
    db = Database.open(path)
    reopened = db.get_hnsw_stats("ih").node_count
    db.close()
    print(f"[{mode}] rows={rows} node_count={st.node_count} memory_bytes={st.memory_bytes} "
          f"node_count_after_reopen={reopened} knn_mismatch={wrong}/40", flush=True)
    if st.node_count != rows or reopened != rows or wrong:
        print(f"  -> DEFECT: HNSW graph does not match the table after TRUNCATE ({mode})")
        failed = True

print("RESULT:", "REPRODUCED" if failed else "OK")
sys.exit(1 if failed else 0)
