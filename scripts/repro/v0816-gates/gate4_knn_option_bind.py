"""Gate 4 (#671): per-query HNSW options WITH (ef_search = ?, enable_hnsw = ?) cannot be bound.

Exit code: 1 = defect reproduced, 0 = fixed.
"""
import sys

import numpy as np

from alopex import Database

db = Database.open_in_memory()
q = db.execute_sql
D, N = 8, 300
V = np.random.default_rng(0).standard_normal((N, D)).astype(np.float32)
q(f"CREATE TABLE it (id INTEGER PRIMARY KEY, e VECTOR({D}, L2))")
for b in range(0, N, 100):
    q("INSERT INTO it VALUES " + ",".join(
        f"({i},[{','.join(f'{x:.4f}' for x in V[i])}])" for i in range(b, b + 100)))
q("CREATE INDEX hi ON it (e) USING HNSW")
base = "SELECT id FROM it ORDER BY vector_distance(e, ?, 'l2') ASC LIMIT 5"
query = V[0].tolist()

problems = []
print("literal ef_search:", q(base + " WITH (ef_search = 64)", [query]))
for label, sql, params in [
    ("execute_sql WITH (ef_search = ?)", base + " WITH (ef_search = ?)", [query, 64]),
    ("execute_sql WITH (enable_hnsw = ?)", base + " WITH (enable_hnsw = ?)", [query, True]),
]:
    try:
        print(label, "->", q(sql, params))
    except Exception as e:  # noqa: BLE001
        print(label, "ERROR:", str(e).replace("\n", " ")[:200])
        problems.append(label)
try:
    ps = db.prepare(base + " WITH (ef_search = ?)")
    print("prepare WITH (ef_search = ?) -> OK", ps)
except Exception as e:  # noqa: BLE001
    print("prepare WITH (ef_search = ?) ERROR:", str(e).replace("\n", " ")[:200])
    problems.append("prepare with bound ef_search")

for p in problems:
    print("  -> DEFECT: cannot bind", p)
print("RESULT:", "REPRODUCED (#671)" if problems else "not reproduced / fixed")
sys.exit(1 if problems else 0)
