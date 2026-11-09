"""Check that a migrated Parquet VECTOR table uses the indexed HNSW path."""
from __future__ import annotations

from contextlib import closing
import importlib.metadata
import json
import os
from pathlib import Path
import shutil

import alopex
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    assert importlib.metadata.version("alopex") == "0.8.17"
    work = Path(os.environ["ALOPEX_HNSW_WORK"])
    dataset = work / "dataset"
    dataset.mkdir(mode=0o700)
    parquet = dataset / "vectors.parquet"
    database = dataset / "vectors.db"
    expected = [{"id": 1, "e": [0.0, 1.0, 2.0]},
                {"id": 2, "e": [10.0, 11.0, 12.0]},
                {"id": 3, "e": None}]
    pq.write_table(pa.table({"id": pa.array([1, 2, 3], pa.int32()),
                             "e": pa.array([row["e"] for row in expected],
                                           pa.list_(pa.float32()))}), parquet)
    with closing(alopex.Database.open(str(database))) as db:
        db.execute_sql("CREATE TABLE v(id INTEGER PRIMARY KEY,e VECTOR(3,L2))")
        assert db.execute_sql(f"COPY v FROM '{parquet}' WITH (FORMAT PARQUET)") == 3
        db.flush()
    with closing(alopex.Database.open(str(database))) as db:
        assert db.execute_sql("SELECT * FROM v ORDER BY id") == expected
        db.execute_sql("CREATE INDEX ih ON v(e) USING HNSW")
        query = ("SELECT id FROM v ORDER BY "
                 "vector_distance(e,[0.0,1.0,2.0],'l2') ASC LIMIT 1 "
                 "WITH (enable_hnsw=true)")
        plan = db.execute_sql("EXPLAIN " + query)[0]["QUERY PLAN"]
        assert "HnswSearch index=ih" in plan, plan
        observed = db.execute_sql(query)
        assert observed == [{"id": 1}], observed
        analyzed = next(iter(db.execute_sql("EXPLAIN ANALYZE " + query)[0].values()))
        assert "HnswSearch index=ih" in analyzed and "nodes_visited=" in analyzed
        assert "nodes_visited=0" not in analyzed, analyzed
        report = {"schema": "alopex.parquet-hnsw-path/v1", "status": "passed", "rows": 3,
                  "parquet_reopened": True, "index_rebuilt": True,
                  "explain_hnsw_selected": True, "analyze_visited_nodes": True,
                  "nearest_id": observed[0]["id"]}
    shutil.rmtree(dataset)
    (work / "hnsw-path.json").write_text(json.dumps(report, sort_keys=True) + "\n")
    print(json.dumps({"status": "passed", "rows": 3, "hnsw_selected": True}))


if __name__ == "__main__":
    main()
