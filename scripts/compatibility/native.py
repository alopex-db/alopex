"""One bounded old-writer/new-reader contract per process and database.

Usage: native.py old|new sql-3|sql-384|raw-3|columnar
The columnar case never invokes the broken old decoder as a diagnostic.
"""

import hashlib
from importlib.metadata import version
import io
import json
import os
from pathlib import Path
import sys

import alopex
import numpy as np
from common import seed

CASES = ("sql-3", "sql-384", "raw-3", "columnar")
MODE, CASE = sys.argv[1:]
assert MODE in ("old", "new") and CASE in CASES
OUT = Path(os.environ["ALOPEX_COMPAT_OUTPUT"]).resolve() / "native" / CASE
OUT.mkdir(parents=True, exist_ok=True)
DB = OUT / "old-created-db"
DIM = int(CASE.split("-")[1]) if CASE != "columnar" else None
ROWS = [
    dict(id=i, label=text)
    for i, text in enumerate(("apple", "apricot", "zebra", "yak"), 1)
]


def operation(name, call):
    print(f"{CASE} {MODE} {name}: start", flush=True)
    result = call()
    print(f"{CASE} {MODE} {name}: passed", flush=True)
    return result


def quoted(value):
    return "'" + value.replace("'", "''") + "'"


def literal(vector):
    return (
        "NULL"
        if vector is None
        else "[" + ",".join(repr(float(x)) for x in vector) + "]"
    )


def canonical(rows):
    return [
        dict(
            id=r["id"],
            note=r["note"],
            vector_bits=None
            if r["embedding"] is None
            else np.asarray(r["embedding"], dtype="<f4").tobytes().hex(),
        )
        for r in rows
    ]


def snapshot(db):
    if CASE == "columnar":
        rows = db.execute_sql("SELECT * FROM legacy_text ORDER BY id")
        assert rows == ROWS
        return rows
    if CASE == "raw-3":
        with db.begin() as tx:
            value = tx.get_vector(b"compat:3", alopex.Metric.L2)
            assert value is not None
            bits = np.asarray(value, dtype="<f4").tobytes().hex()
            assert bits == canonical(seed(3))[0]["vector_bits"]
            return bits
    result = {}
    for table, null in [("vectors", False), ("null_vectors", True)]:
        rows = db.execute_sql(f"SELECT * FROM {table} ORDER BY id")
        result[table] = canonical(rows)
        assert result[table] == canonical(
            [r for r in seed(DIM) if (r["embedding"] is None) == null]
        )
    return result


def files():
    result = []
    for path in sorted(DB.rglob("*")):
        if path.is_file() and path.name != ".alopex.lock":
            with path.open("rb") as stream:
                result.append(
                    dict(
                        name=str(path.relative_to(DB)),
                        bytes=path.stat().st_size,
                        sha256=hashlib.file_digest(stream, "sha256").hexdigest(),
                    )
                )
    return result


def write_fixture(db):
    if CASE == "columnar":
        db.execute_sql(
            "CREATE TABLE legacy_text (id INTEGER PRIMARY KEY, label TEXT) WITH (storage='columnar', row_group_size=1000)"
        )
        csv = "id,label\n" + "".join(f"{r['id']},{r['label']}\n" for r in ROWS)
        db.copy_from_csv("legacy_text", io.BytesIO(csv.encode()), header=True)
    elif CASE == "raw-3":
        with db.begin(alopex.TxnMode.READ_WRITE) as tx:
            tx.upsert_vector(
                b"compat:3",
                b"metadata",
                np.asarray(seed(3)[0]["embedding"], dtype=np.float32),
                alopex.Metric.L2,
            )
            tx.commit()
    else:
        for table in ("vectors", "null_vectors"):
            db.execute_sql(
                f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR({DIM},L2), note TEXT)"
            )
        for row in seed(DIM):
            table = "null_vectors" if row["embedding"] is None else "vectors"
            note = "NULL" if row["note"] is None else quoted(row["note"])
            db.execute_sql(
                f"INSERT INTO {table} VALUES ({row['id']},{literal(row['embedding'])},{note})"
            )
        db.execute_sql("CREATE INDEX idx_vectors ON vectors(embedding) USING HNSW")


def save(name, data):
    (OUT / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def old():
    assert version("alopex") == "0.8.15"
    db = operation("create", lambda: alopex.Database.open(str(DB)))
    operation("write", lambda: write_fixture(db))
    operation("flush", db.flush)
    operation("close", db.close)
    # Writer->candidate reader is the contract. Old self-read is unnecessary and
    # historically unsafe for mixed binary row groups; do not execute/catch it.
    save(
        "old.json",
        dict(version=version("alopex"), case=CASE, files_before_new_open=files()),
    )


def new():
    assert version("alopex") == os.environ["ALOPEX_COMPAT_TARGET_VERSION"]
    receipt = json.loads((OUT / "old.json").read_text())
    assert receipt["case"] == CASE and files() == receipt["files_before_new_open"]
    db = operation("open", lambda: alopex.Database.open(str(DB)))
    operation("read", lambda: snapshot(db))
    if CASE.startswith("sql-"):
        query = f"SELECT id FROM vectors ORDER BY vector_distance(embedding,{literal(seed(DIM)[0]['embedding'])},'l2') LIMIT 1 WITH (enable_hnsw=true)"

        def check_index():
            assert db.execute_sql(query) == [{"id": 1}]
            assert "HnswSearch" in json.dumps(db.execute_sql("EXPLAIN " + query))

        operation("existing-hnsw", check_index)
    operation("flush", db.flush)
    operation("close", db.close)
    db = operation("reopen", lambda: alopex.Database.open(str(DB)))
    operation("reread", lambda: snapshot(db))
    operation("close", db.close)
    save(
        "result.json",
        dict(
            case=CASE,
            direct_same_database_open=True,
            second_target_reopen_pass=True,
            values_match_seed=True,
            index_recreated=False,
            rows=4 if CASE == "columnar" else 1 if CASE == "raw-3" else 5,
        ),
    )


if MODE == "old":
    old()
else:
    new()
