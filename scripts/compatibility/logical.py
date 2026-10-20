"""Published-package migration probe. No native database bytes are copied.

DDL/index definitions are supplied explicitly. Python 0.8.15 KV export needs
an application-owned key manifest; it has no public raw-key enumeration API.
"""

import datetime as dt
import decimal
import csv
import json
from pathlib import Path
import sys
import os
import alopex
from importlib.metadata import version

VERSION = version("alopex")

ROOT = Path(os.environ["ALOPEX_COMPAT_OUTPUT"]).resolve()
DDL = {
    "scalars": "CREATE TABLE scalars (id INTEGER PRIMARY KEY, note TEXT, n BIGINT, score DOUBLE, active BOOLEAN)",
    "typed": "CREATE TABLE typed (id INTEGER PRIMARY KEY, amount DECIMAL(18,6), day DATE, clock TIME, stamp TIMESTAMP)",
    "vectors": "CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(3, L2), note TEXT)",
    "col": "CREATE TABLE col (id INTEGER PRIMARY KEY, note TEXT, score DOUBLE) WITH (storage='columnar')",
}
SEED = [
    "INSERT INTO scalars VALUES (1, '', 9223372036854775807, 1.25, TRUE), (2, NULL, -9223372036854775807, -0.5, FALSE), (3, '日本語,''quote''\nnext', 0, 0.0, TRUE)",
    "INSERT INTO typed VALUES (1, CAST('123.456789' AS DECIMAL(18,6)), DATE '2024-02-29', TIME '12:34:56.123456', TIMESTAMP '2024-02-29 12:34:56.123456')",
    "INSERT INTO vectors VALUES (1, [0.0,1.0,2.0], 'nearest'), (2, [10.0,11.0,12.0], 'far')",
]
INDEX = "CREATE INDEX vec_idx ON vectors(embedding) USING HNSW"
KNN = "SELECT id FROM vectors ORDER BY vector_distance(embedding, [0.0,1.0,2.0], 'l2') ASC LIMIT 1"
KV = [(b"migration:\x00key", b"\x00\xffvalue"), (b"migration:empty", b"")]


def dump(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def quote(s):
    return "'" + s.replace("'", "''") + "'"


def literal(v, name):
    if v is None:
        return "NULL"
    if name == "stamp":
        stamp = dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=v)
        return "TIMESTAMP " + quote(stamp.isoformat(sep=" ", timespec="microseconds"))
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, decimal.Decimal):
        return "CAST(" + quote(str(v)) + " AS DECIMAL(18,6))"
    if isinstance(v, dt.date):
        return "DATE " + quote(v.isoformat())
    if isinstance(v, dt.time):
        return "TIME " + quote(v.isoformat(timespec="microseconds"))
    if isinstance(v, str):
        return quote(v)
    if isinstance(v, list):
        return "[" + ",".join(repr(x) for x in v) + "]"
    return repr(v)


def norm(v):
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.date):
        return (v - dt.date(1970, 1, 1)).days
    if isinstance(v, dt.time):
        return ((v.hour * 60 + v.minute) * 60 + v.second) * 1000000 + v.microsecond
    return v


def snapshot(db, keys):
    result = {}
    for table in DDL:
        result[table] = [
            [norm(v) for v in row.values()]
            for row in db.execute_sql(f"SELECT * FROM {table} ORDER BY id")
        ]
    with db.begin(alopex.TxnMode.READ_ONLY) as tx:
        result["kv"] = [[k.hex(), tx.get(k).hex()] for k in keys]
    result["knn"] = db.execute_sql(KNN)[0]["id"]
    return result


def seed(db):
    for ddl in DDL.values():
        db.execute_sql(ddl)
    for sql in SEED:
        db.execute_sql(sql)
    import io

    db.copy_from_csv(
        "col",
        io.BytesIO(b"id,note,score\n1,columnar,1.5\n2,second,-2.0\n"),
        header=True,
    )
    with db.begin(alopex.TxnMode.READ_WRITE) as tx:
        for k, v in KV:
            tx.put(k, v)
        tx.commit()
    db.execute_sql(INDEX)


def export(label):
    assert VERSION == "0.8.15", VERSION
    path = ROOT / (label + "-source-db")
    db = alopex.Database.open(str(path))
    seed(db)
    db.flush()
    db.close()
    db = alopex.Database.open(str(path))
    expected = snapshot(db, [k for k, _ in KV])
    statements = []
    for table in ["scalars", "typed", "vectors"]:
        for row in db.execute_sql(f"SELECT * FROM {table} ORDER BY id"):
            statements.append(
                f"INSERT INTO {table} VALUES ("
                + ",".join(literal(v, k) for k, v in row.items())
                + ")"
            )
    parquet = ROOT / (label + "-col.parquet")
    parquet_export_count = db.execute_sql(
        f"COPY col TO {quote(str(parquet))} WITH (FORMAT PARQUET)"
    )
    columnar_csv = ROOT / (label + "-col-logical.csv")
    with columnar_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "note", "score"])
        writer.writerows(expected["col"])
    scalar_csv = ROOT / (label + "-scalars.csv")
    db.execute_sql(
        f"COPY scalars TO {quote(str(scalar_csv))} WITH (FORMAT CSV, HEADER TRUE)"
    )
    scalar_parquet = ROOT / (label + "-scalars.parquet")
    db.execute_sql(
        f"COPY scalars TO {quote(str(scalar_parquet))} WITH (FORMAT PARQUET)"
    )
    try:
        db.execute_sql(
            f"COPY vectors TO {quote(str(ROOT / (label + '-vectors.parquet')))} WITH (FORMAT PARQUET)"
        )
        vector_parquet = "unexpected success"
    except Exception as e:
        vector_parquet = str(e)
    bundle = {
        "source": "Python " + VERSION,
        "ddl": list(DDL.values()),
        "statements": statements,
        "index": INDEX,
        "knn_query": KNN,
        "columnar_parquet": str(parquet),
        "scalar_csv": str(scalar_csv),
        "columnar_csv": str(columnar_csv),
        "scalar_parquet": str(scalar_parquet),
        "kv": expected["kv"],
        "expected": expected,
        "limitations": {
            "schema": "explicit supplied DDL/index recreation",
            "kv": "application supplied key manifest",
            "columnar_parquet_export_count": parquet_export_count,
            "vector_parquet": vector_parquet,
        },
    }
    dump(ROOT / (label + "-bundle.json"), bundle)
    db.close()
    print(
        json.dumps(
            {
                "phase": "export",
                "label": label,
                "version": VERSION,
                "snapshot": expected,
            },
            ensure_ascii=False,
        )
    )


def load(label, source):
    assert VERSION == os.environ["ALOPEX_COMPAT_TARGET_VERSION"], VERSION
    bundle = json.loads((ROOT / (source + "-bundle.json")).read_text())
    path = ROOT / (label + "-destination-db")
    db = alopex.Database.open(str(path))
    for sql in bundle["ddl"] + bundle["statements"]:
        db.execute_sql(sql)
    with open(bundle["columnar_csv"], "rb") as f:
        db.copy_from_csv("col", f, header=True)
    with db.begin(alopex.TxnMode.READ_WRITE) as tx:
        for k, v in bundle["kv"]:
            tx.put(bytes.fromhex(k), bytes.fromhex(v))
        tx.commit()
    db.execute_sql(bundle["index"])
    db.flush()
    db.close()
    # Preserve the stock columnar Parquet result independently.
    db = alopex.Database.new()
    db.execute_sql(DDL["col"])
    parquet_result = {}
    try:
        count = db.execute_sql(
            f"COPY col FROM {quote(bundle['columnar_parquet'])} WITH (FORMAT PARQUET)"
        )
        rows = [
            [norm(v) for v in row.values()]
            for row in db.execute_sql("SELECT * FROM col ORDER BY id")
        ]
        parquet_result = {
            "preserved": rows == bundle["expected"]["col"],
            "import_count": count,
            "actual": rows,
        }
    except Exception as e:
        parquet_result = {"preserved": False, "error": str(e)}
    db.close()
    db = alopex.Database.open(str(path))
    actual = snapshot(db, [bytes.fromhex(k) for k, _ in bundle["kv"]])
    assert actual == bundle["expected"], (actual, bundle["expected"])
    db.execute_sql("INSERT INTO scalars VALUES (4, 'new write', 42, 2.5, TRUE)")
    db.execute_sql("UPDATE scalars SET note='updated' WHERE id=4")
    assert db.execute_sql("SELECT note FROM scalars WHERE id=4")[0]["note"] == "updated"
    db.flush()
    db.close()
    # Probe the stock CSV path separately, without hiding empty-string loss.
    db = alopex.Database.new()
    db.execute_sql(DDL["scalars"])
    csv_result = {}
    try:
        db.execute_sql(
            f"COPY scalars FROM {quote(bundle['scalar_csv'])} WITH (FORMAT CSV, HEADER TRUE)"
        )
        rows = [
            [norm(v) for v in row.values()]
            for row in db.execute_sql("SELECT * FROM scalars ORDER BY id")
        ]
        csv_result = {
            "preserved": rows == bundle["expected"]["scalars"],
            "actual": rows,
        }
    except Exception as e:
        csv_result = {"preserved": False, "error": str(e)}
    db.close()
    scalar_parquet_result = {"tested": False}
    if bundle.get("scalar_parquet"):
        db = alopex.Database.new()
        db.execute_sql(DDL["scalars"])
        try:
            count = db.execute_sql(
                f"COPY scalars FROM {quote(bundle['scalar_parquet'])} WITH (FORMAT PARQUET)"
            )
            rows = [
                [norm(v) for v in row.values()]
                for row in db.execute_sql("SELECT * FROM scalars ORDER BY id")
            ]
            scalar_parquet_result = {
                "tested": True,
                "preserved": rows == bundle["expected"]["scalars"],
                "import_count": count,
                "actual": rows,
            }
        except Exception as e:
            scalar_parquet_result = {
                "tested": True,
                "preserved": False,
                "error": str(e),
            }
        db.close()
    if scalar_parquet_result["tested"]:
        assert scalar_parquet_result["preserved"], scalar_parquet_result
    report = {
        "source": bundle["source"],
        "destination": "Python " + VERSION,
        "logical_roundtrip_pass": True,
        "post_reopen_snapshot": actual,
        "new_write_update_pass": True,
        "stock_csv_probe": csv_result,
        "stock_columnar_parquet_probe": parquet_result,
        "stock_scalar_parquet_probe": scalar_parquet_result,
        "limitations": bundle["limitations"],
    }
    dump(ROOT / (label + "-result.json"), report)
    print(json.dumps(report, ensure_ascii=False))


def direct(source):
    """Open a copy of an old writer's DB, without recreating DDL/indexes."""
    import shutil

    bundle = json.loads((ROOT / (source + "-bundle.json")).read_text())
    path = ROOT / (source + "-python-direct-db")
    shutil.copytree(ROOT / (source + "-source-db"), path)
    db = alopex.Database.open(str(path))
    actual = snapshot(db, [bytes.fromhex(k) for k, _ in bundle["kv"]])
    assert actual == bundle["expected"], (actual, bundle["expected"])
    query = bundle["knn_query"] + " WITH (enable_hnsw = true)"
    assert db.execute_sql(query)[0]["id"] == bundle["expected"]["knn"]
    plan = db.execute_sql("EXPLAIN " + query)
    assert "HnswSearch" in json.dumps(plan), plan
    db.flush()
    db.close()
    db = alopex.Database.open(str(path))
    assert snapshot(db, [bytes.fromhex(k) for k, _ in bundle["kv"]]) == actual
    db.close()
    dump(
        ROOT / (source + "-python-direct-result.json"),
        {
            "direct_open_pass": True,
            "index_recreated": False,
            "source": bundle["source"],
            "destination": "Python " + VERSION,
            "hnsw_plan": plan,
            "second_reopen_pass": True,
        },
    )


if __name__ == "__main__":
    if sys.argv[1] == "export":
        export(sys.argv[2])
    elif sys.argv[1] == "direct":
        direct(sys.argv[2])
    else:
        load(sys.argv[2], sys.argv[3])
