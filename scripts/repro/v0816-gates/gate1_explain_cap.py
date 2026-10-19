"""Gate 1 (#669): EXPLAIN / EXPLAIN ANALYZE claim IndexScan when the B-tree path
falls back to a full scan above its runtime candidate cap (1024 row ids / 8 MiB).

Exit code: 1 = defect reproduced, 0 = fixed.
"""
import statistics
import sys
import tempfile
import time

from alopex import Database

N = 40_000
db = Database.open(tempfile.mkdtemp() + "/gate1.alopex")
q = db.execute_sql
q("CREATE TABLE t (id INTEGER PRIMARY KEY, small INTEGER, big INTEGER, pad TEXT)")
for b in range(0, N, 500):
    q("INSERT INTO t VALUES " + ",".join(f"({i},{i % 40},{i % 4},'x{i}')" for i in range(b, b + 500)))


def median_ms(sql, n=7):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        q(sql)
        ts.append(time.perf_counter() - t0)
    return statistics.median(ts) * 1000


cases = {
    "under_cap_1000_rows": "SELECT * FROM t WHERE small = 3",
    "over_cap_10000_rows": "SELECT * FROM t WHERE big = 3",
}
no_index = {k: median_ms(s) for k, s in cases.items()}
q("CREATE INDEX t_small ON t (small)")
q("CREATE INDEX t_big ON t (big)")

reproduced = False
for name, sql in cases.items():
    with_index = median_ms(sql)
    plan = q("EXPLAIN " + sql)[0]["QUERY PLAN"].replace("\n", " | ")
    analyze = q("EXPLAIN ANALYZE " + sql)[0]["QUERY PLAN"].replace("\n", " | ")
    rows = len(q(sql))
    print(f"[{name}] rows={rows} no_index={no_index[name]:.1f}ms index={with_index:.1f}ms")
    print(f"  EXPLAIN        : {plan}")
    print(f"  EXPLAIN ANALYZE: {analyze}")
    if name.startswith("over_cap"):
        ran_full_scan = with_index > 0.5 * no_index[name]
        claims_index = "IndexScan" in analyze and "fallback" not in analyze and "actual_path=Scan" not in analyze
        if ran_full_scan and claims_index:
            reproduced = True
            print("  -> DEFECT: ran as a full scan but EXPLAIN ANALYZE reports IndexScan without a fallback marker")

print("RESULT:", "REPRODUCED (#669)" if reproduced else "not reproduced / fixed")
sys.exit(1 if reproduced else 0)
