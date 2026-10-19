import hashlib
from importlib.metadata import version
import io
import json
from pathlib import Path
import sys
import os
from common import seed

import alopex
import numpy as np

ROOT = Path(os.environ["ALOPEX_COMPAT_OUTPUT"]).resolve()
OUT = ROOT / "native"
DB = OUT / "old-created-db"
DATA = {str(d): {"rows": seed(d)} for d in [3, 384]}


def quoted(s):
    return "'" + s.replace("'", "''") + "'"


def literal(v):
    return "NULL" if v is None else "[" + ",".join(repr(float(x)) for x in v) + "]"


def canonical(rows):
    return [
        {
            "id": r["id"],
            "note": r["note"],
            "vector_bits": None
            if r["embedding"] is None
            else np.asarray(r["embedding"], dtype="<f4").tobytes().hex(),
        }
        for r in rows
    ]


def snapshot(db):
    sql = {}
    raw = {}
    for dim in [3, 384]:
        rows = db.execute_sql(f"SELECT * FROM vec{dim} ORDER BY id")
        assert canonical(rows) == canonical(
            [r for r in DATA[str(dim)]["rows"] if r["embedding"] is not None]
        )
        sql[str(dim)] = canonical(rows)
        nulls = db.execute_sql(f"SELECT * FROM vecnull{dim} ORDER BY id")
        assert canonical(nulls) == canonical(
            [r for r in DATA[str(dim)]["rows"] if r["embedding"] is None]
        )
        sql[f"null{dim}"] = canonical(nulls)
    for dim in [3]:
        with db.begin() as tx:
            value = tx.get_vector(f"compat:{dim}".encode(), alopex.Metric.L2)
            assert value is not None
            raw[str(dim)] = np.asarray(value, dtype="<f4").tobytes().hex()
            assert raw[str(dim)] == canonical(DATA[str(dim)]["rows"])[0]["vector_bits"]
    return {"sql": sql, "raw": raw}


def legacy_rows():
    return [{"id": i, "label": f"prefix-{i:04d}"} for i in range(1, 1001)] + [
        {"id": 1000 + j, "label": f"tail-{1001 - j:04d}"} for j in range(1, 1001)
    ]


def columnar(db):
    try:
        return {"rows": db.execute_sql("SELECT * FROM legacy_text ORDER BY id")}
    except Exception as e:
        return {"error": str(e)}


def files():
    return [
        {
            "name": str(p.relative_to(DB)),
            "bytes": p.stat().st_size,
            "sha256": hashlib.file_digest(p.open("rb"), "sha256").hexdigest(),
        }
        for p in sorted(DB.rglob("*"))
        if p.is_file() and p.name != ".alopex.lock"
    ]


def old():
    assert version("alopex") == "0.8.15"
    db = alopex.Database.open(str(DB))
    for dim in [3, 384]:
        db.execute_sql(
            f"CREATE TABLE vec{dim} (id INTEGER PRIMARY KEY, embedding VECTOR({dim},L2), note TEXT)"
        )
        db.execute_sql(
            f"CREATE TABLE vecnull{dim} (id INTEGER PRIMARY KEY, embedding VECTOR({dim},L2), note TEXT)"
        )
        for r in DATA[str(dim)]["rows"]:
            note = "NULL" if r["note"] is None else quoted(r["note"])
            table = f"vecnull{dim}" if r["embedding"] is None else f"vec{dim}"
            db.execute_sql(
                f"INSERT INTO {table} VALUES ({r['id']},{literal(r['embedding'])},{note})"
            )
        db.execute_sql(f"CREATE INDEX idx_vec{dim} ON vec{dim}(embedding) USING HNSW")
    with db.begin(alopex.TxnMode.READ_WRITE) as tx:
        for dim in [3]:
            tx.upsert_vector(
                f"compat:{dim}".encode(),
                b"metadata",
                np.asarray(DATA[str(dim)]["rows"][0]["embedding"], dtype=np.float32),
                alopex.Metric.L2,
            )
        tx.commit()
    # v0.8.15's completed transaction object still retains the DB allocation.
    # Drop it before closing/reopening the same directory in this process.
    del tx
    db.execute_sql(
        "CREATE TABLE legacy_text (id INTEGER PRIMARY KEY, label TEXT) WITH (storage='columnar', row_group_size=1000)"
    )
    csv_data = "id,label\n" + "".join(
        f"{r['id']},{r['label']}\n" for r in legacy_rows()
    )
    db.copy_from_csv("legacy_text", io.BytesIO(csv_data.encode()), header=True)
    db.flush()
    db.close()
    db = alopex.Database.open(str(DB))
    receipt = {
        "version": version("alopex"),
        "snapshot": snapshot(db),
        "columnar": columnar(db),
    }
    db.close()
    receipt["files_before_new_open"] = files()
    (OUT / "old.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
    print("Old persisted/reopened SQL and raw vectors verified", flush=True)


def new():
    assert version("alopex") == os.environ["ALOPEX_COMPAT_TARGET_VERSION"]
    old = json.loads((OUT / "old.json").read_text())
    assert files() == old["files_before_new_open"], "old files changed before new open"
    db = alopex.Database.open(str(DB))
    actual = snapshot(db)
    assert actual == old["snapshot"]
    col = columnar(db)
    assert col.get("rows") == legacy_rows(), col
    queries = {}
    for dim in [3, 384]:
        target = DATA[str(dim)]["rows"][0]["embedding"]
        query = f"SELECT id FROM vec{dim} ORDER BY vector_distance(embedding,{literal(target)},'l2') LIMIT 1 WITH (enable_hnsw=true)"
        result = db.execute_sql(query)
        plan = db.execute_sql("EXPLAIN " + query)
        assert result == [{"id": 1}] and "HnswSearch" in json.dumps(plan)
        queries[str(dim)] = {"rows": result, "explain": plan}
    db.flush()
    db.close()
    db = alopex.Database.open(str(DB))
    assert snapshot(db) == old["snapshot"]
    db.close()
    report = {
        "source": "published alopex 0.8.15",
        "destination": "alopex " + version("alopex"),
        "direct_same_database_open": True,
        "export_import_used": False,
        "sql_vectors_bits_equal": True,
        "raw_get_vector_bits_equal": True,
        "raw_vector_dimensions": [3],
        "dimensions": [3, 384],
        "sql_rows_per_dimension": 5,
        "fixture_note": "Four non-NULL indexed rows and one NULL row in a separate table per dimension, because v0.8.15 rejects HNSW DDL on NULL vectors.",
        "old_existing_hnsw_indexes_recreated_by_harness": False,
        "hnsw_queries": queries,
        "old_columnar_result": old["columnar"],
        "new_columnar_result": {"rows": len(col["rows"]), "matches_seed": True},
        "second_target_reopen_pass": True,
        "scope": "Synthetic row SQL vectors, one raw 3D vector and one 2000-row/2-row-group columnar TEXT fixture. Does not certify every legacy DB or vector segment layout.",
    }
    (OUT / "result.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    old() if sys.argv[1] == "old" else new()
