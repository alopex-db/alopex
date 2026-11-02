#!/usr/bin/env python3
"""Manual three-row legacy SELECT -> PyArrow -> current COPY acceptance.

Invoke export in a process importing the requested public old wheel, then import
in a separate process importing the candidate extension. This never upgrades a DB
in place. Dependencies are installed by the operator into an isolated environment.
"""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import shutil


DDL = "CREATE TABLE vectors(id INTEGER PRIMARY KEY,e VECTOR(3,L2))"
QUERY = "SELECT id,e FROM vectors ORDER BY id"
EXPECTED = [
    {"id": 1, "e": [1.0, 2.0, 3.0]},
    {"id": 2, "e": [10.0, 11.0, 12.0]},
    {"id": 3, "e": None},
]


def remove_owned_database(path):
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["export", "import"])
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()
    import alopex
    import pyarrow as pa
    import pyarrow.parquet as pq

    version = importlib.metadata.version("alopex")
    if version != args.expected_version:
        raise RuntimeError(f"expected alopex {args.expected_version}, imported {version}")
    root = args.directory.resolve()
    parquet = root / "vectors.parquet"
    dbpath = root / ("old.db" if args.mode == "export" else "new.db")
    if args.mode == "export":
        root.mkdir(parents=True, exist_ok=False)
    elif not parquet.is_file() or dbpath.exists():
        raise RuntimeError("import requires exported data and a fresh destination")
    db = None
    direct_copy_error = None
    try:
        if args.mode == "export":
            db = alopex.Database.open(str(dbpath))
            db.execute_sql(DDL)
            db.execute_sql("INSERT INTO vectors VALUES (1,[1,2,3]),(2,[10,11,12]),(3,NULL)")
            db.flush()
            db.close()
            db = alopex.Database.open(str(dbpath))
            rows = db.execute_sql(QUERY)
            if rows != EXPECTED:
                raise RuntimeError("old SELECT differs from expected values")
            try:
                db.execute_sql(f"COPY vectors TO '{root / 'unsupported.parquet'}' WITH (FORMAT PARQUET)")
            except alopex.AlopexError as error:
                direct_copy_error = str(error)
            else:
                raise RuntimeError("expected the old native VECTOR COPY refusal")
            if "does not support" not in direct_copy_error:
                raise RuntimeError(f"unexpected old COPY error: {direct_copy_error}")
            pq.write_table(pa.table({
                "id": pa.array([row["id"] for row in rows], pa.int32()),
                "e": pa.array([row["e"] for row in rows], pa.list_(pa.float32())),
            }), parquet)
        else:
            db = alopex.Database.open(str(dbpath))
            db.execute_sql(DDL)
            if db.execute_sql(f"COPY vectors FROM '{parquet}' WITH (FORMAT PARQUET)") != 3:
                raise RuntimeError("wrong imported row count")
            db.flush()
            db.close()
            db = alopex.Database.open(str(dbpath))
            if db.execute_sql(QUERY) != EXPECTED:
                raise RuntimeError("candidate rows differ after reopen")
            db.execute_sql("CREATE INDEX ih ON vectors(e) USING HNSW")
            result = db.execute_sql("SELECT id FROM vectors ORDER BY vector_distance(e,[1,2,3],'l2') LIMIT 1 WITH (enable_hnsw=true)")
            if result != [{"id": 1}] or db.hnsw_cache_stats()["entries"] != 1:
                raise RuntimeError("recreated HNSW result/cache differs")
        if pq.read_table(parquet).to_pylist() != EXPECTED:
            raise RuntimeError("independent PyArrow read differs")
        record = {
            "mode": args.mode, "version": version, "module": str(alopex.__file__),
            "pyarrow": pa.__version__, "rows": EXPECTED,
            "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
            "old_native_copy_error": direct_copy_error, "outcome": "passed",
        }
        (root / f"{args.mode}.json").write_text(json.dumps(record, indent=2) + "\n")
        print(json.dumps(record))
    finally:
        if db is not None:
            db.close()
        remove_owned_database(dbpath)


if __name__ == "__main__":
    main()
