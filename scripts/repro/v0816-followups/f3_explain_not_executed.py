"""EXPLAIN ANALYZE still names an unexecuted B-tree node `IndexScan` (follow-up of #673).

Exit 1 if reproduced, 0 if fixed.
"""
import sys

from alopex import Database

db = Database.open_in_memory()
q = db.execute_sql
q("CREATE TABLE k (id INTEGER PRIMARY KEY, g INTEGER, e VECTOR(4, L2))")
q("CREATE INDEX kg ON k (g)")
q("INSERT INTO k VALUES " + ",".join(f"({i},{i % 5},[{i},0,0,1])" for i in range(200)))

sql = ("EXPLAIN ANALYZE SELECT id FROM k WHERE g = 2 "
       "ORDER BY vector_distance(e, [1,0,0,1], 'l2') LIMIT 3")
lines = "\n".join(str(list(r.values())[0]) for r in q(sql)).splitlines()
print("\n".join(lines))
# A node may be named IndexScan only when the index path actually ran.
bad = [l for l in lines if l.strip().startswith("IndexScan") and "actual_path=IndexScan" not in l]
if bad:
    print("  -> DEFECT: node is rendered as IndexScan although it did not run")
print("RESULT:", "REPRODUCED" if bad else "OK")
sys.exit(1 if bad else 0)
