"""Actual published-wheel VECTOR migration through external PyArrow.

Schema/indexes are explicitly recreated. This is a bounded Python route,
not native Alopex COPY support or arbitrary database migration certification.
"""

import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import shutil
import sys
import traceback

import alopex
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import os
from common import seed


def check(label):
    # The runner owns the host budget monitor; child phases retain labels.
    print(label, flush=True)


ROOT = Path(os.environ["ALOPEX_COMPAT_OUTPUT"]).resolve()
OUT = ROOT / "arrow"
FORMATS = ["list_f32", "list_f64", "fixed_f32", "fixed_f64", "json_utf8"]
DIMS = [3, 384]


def save(name, value):
    (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def quoted(s):
    return "'" + s.replace("'", "''") + "'"


def vector_sql(vector):
    return (
        "NULL"
        if vector is None
        else "[" + ",".join(repr(float(x)) for x in vector) + "]"
    )


def ddl(name, dim):
    return f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, embedding VECTOR({dim}, L2), note TEXT)"


def insert(db, name, row):
    note = "NULL" if row["note"] is None else quoted(row["note"])
    db.execute_sql(
        f"INSERT INTO {name} VALUES ({row['id']},{vector_sql(row['embedding'])},{note})"
    )


def canonical(rows):
    return [
        {
            "id": r["id"],
            "note": r["note"],
            "vector_float32_le_hex": None
            if r["embedding"] is None
            else np.asarray(r["embedding"], dtype="<f4").tobytes().hex(),
        }
        for r in rows
    ]


def digest(rows):
    return hashlib.sha256(
        json.dumps(canonical(rows), ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()


def source_export():
    assert version("alopex") == "0.8.15"
    check("before-arrow-old-export")
    path = OUT / "source-db"
    db = alopex.Database.open(str(path))
    for dim in DIMS:
        db.execute_sql(ddl(f"vec{dim}", dim))
        for row in seed(dim):
            insert(db, f"vec{dim}", row)
    db.flush()
    db.close()
    db = alopex.Database.open(str(path))
    expected = {}
    files = []
    for dim in DIMS:
        rows = db.execute_sql(f"SELECT * FROM vec{dim} ORDER BY id")
        assert canonical(rows) == canonical(seed(dim)), (
            "old persisted fixture differs from seed"
        )
        expected[str(dim)] = {"rows": rows, "digest": digest(rows)}
        for representation in FORMATS:
            kind = pa.float32() if representation.endswith("f32") else pa.float64()
            field_type = (
                pa.string()
                if representation == "json_utf8"
                else pa.list_(kind, dim)
                if representation.startswith("fixed_")
                else pa.list_(kind)
            )
            vectors = [r["embedding"] for r in rows]
            if representation == "json_utf8":
                vectors = [
                    None if v is None else json.dumps(v, separators=(",", ":"))
                    for v in vectors
                ]
            table = pa.Table.from_arrays(
                [
                    pa.array([r["id"] for r in rows], type=pa.int32()),
                    pa.array(vectors, type=field_type),
                    pa.array([r["note"] for r in rows], type=pa.string()),
                ],
                names=["id", "embedding", "note"],
            )
            output = OUT / f"{representation}-{dim}.parquet"
            result = {
                "representation": representation,
                "dimension": dim,
                "path": output.name,
                "arrow_schema": str(table.schema),
                "expected_rows": len(rows),
            }
            try:
                pq.write_table(table, output, compression="zstd")
                result.update(
                    {
                        "write_pass": True,
                        "bytes": output.stat().st_size,
                        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                        "footer_rows": pq.read_metadata(output).num_rows,
                    }
                )
            except Exception as e:
                result.update({"write_pass": False, "error": str(e)})
            files.append(result)
    db.close()
    shutil.rmtree(path)
    save("expected.json", expected)
    save(
        "export.json",
        {
            "source": "alopex " + version("alopex"),
            "pyarrow": pa.__version__,
            "numpy": np.__version__,
            "persisted_reopened_source": True,
            "old_database_deleted_after_export": True,
            "files": files,
        },
    )
    print(
        json.dumps(
            {
                "phase": "export",
                "source": version("alopex"),
                "pyarrow": pa.__version__,
                "files_written": sum(x["write_pass"] for x in files),
                "files_total": len(files),
            }
        ),
        flush=True,
    )
    check("after-arrow-old-export")


def native_probe(path, dim):
    db = alopex.Database.new()
    db.execute_sql(ddl("native_input", dim))
    try:
        count = db.execute_sql(
            f"COPY native_input FROM {quoted(str(path))} WITH (FORMAT PARQUET)"
        )
        rows = db.execute_sql("SELECT * FROM native_input ORDER BY id")
        return {"supported": True, "count": count, "actual_rows": rows}
    except Exception as e:
        return {"supported": False, "error": str(e)}
    finally:
        db.close()


def migrate():
    assert version("alopex") == os.environ["ALOPEX_COMPAT_TARGET_VERSION"]
    check("before-arrow-new-import")
    export = json.loads((OUT / "export.json").read_text())
    expected = json.loads((OUT / "expected.json").read_text())
    results = []
    native_output = None
    for representation in FORMATS:
        check("before-arrow-case-" + representation)
        path = OUT / f"{representation}-new-db"
        case = {"representation": representation, "dimensions": [], "pass": False}
        db = None
        try:
            loaded = {}
            for dim in DIMS:
                info = next(
                    x
                    for x in export["files"]
                    if x["representation"] == representation and x["dimension"] == dim
                )
                assert info["write_pass"], info
                file = OUT / info["path"]
                assert hashlib.sha256(file.read_bytes()).hexdigest() == info["sha256"]
                native = native_probe(file, dim)
                dimension = {"dimension": dim, "native_copy_from": native}
                case["dimensions"].append(dimension)
                arrow_table = pq.read_table(file)
                rows = arrow_table.to_pylist()
                if representation == "json_utf8":
                    for row in rows:
                        if row["embedding"] is not None:
                            row["embedding"] = json.loads(row["embedding"])
                assert canonical(rows) == canonical(expected[str(dim)]["rows"]), (
                    "Parquet read differs from old DB"
                )
                dimension.update(
                    {
                        "pyarrow_read_pass": True,
                        "rows": len(rows),
                        "parquet_roundtrip_digest": digest(rows),
                    }
                )
                loaded[dim] = rows
            db = alopex.Database.open(str(path))
            for dim, rows in loaded.items():
                db.execute_sql(ddl(f"vec{dim}", dim))
                for row in rows:
                    insert(db, f"vec{dim}", row)
                db.execute_sql(
                    f"CREATE INDEX idx_vec{dim} ON vec{dim}(embedding) USING HNSW"
                )
            db.flush()
            db.close()
            db = alopex.Database.open(str(path))
            for dim in DIMS:
                actual = db.execute_sql(f"SELECT * FROM vec{dim} ORDER BY id")
                assert canonical(actual) == canonical(expected[str(dim)]["rows"]), (
                    "new reopened DB differs from old"
                )
                target = expected[str(dim)]["rows"][0]["embedding"]
                reference = min(
                    (
                        r
                        for r in expected[str(dim)]["rows"]
                        if r["embedding"] is not None
                    ),
                    key=lambda r: sum(
                        (a - b) ** 2 for a, b in zip(r["embedding"], target)
                    ),
                )["id"]
                query = f"SELECT id FROM vec{dim} ORDER BY vector_distance(embedding,{vector_sql(target)},'l2') ASC LIMIT 1"
                default_nearest = db.execute_sql(query)
                assert default_nearest[0]["id"] == reference
                default_plan = db.execute_sql("EXPLAIN " + query)
                indexed_query = query + " WITH (enable_hnsw = true)"
                nearest = db.execute_sql(indexed_query)
                plan = db.execute_sql("EXPLAIN " + indexed_query)
                assert nearest[0]["id"] == reference
                assert "HnswSearch" in json.dumps(plan), plan
                detail = next(x for x in case["dimensions"] if x["dimension"] == dim)
                detail.update(
                    {
                        "new_db_reopen_pass": True,
                        "actual_digest": digest(actual),
                        "nearest_id": nearest[0]["id"],
                        "reference_nearest_id": reference,
                        "hnsw_query_pass": True,
                        "explain": plan,
                        "default_knn_pass": True,
                        "default_explain": default_plan,
                        "null_vectors": sum(r["embedding"] is None for r in actual),
                    }
                )
            if native_output is None:
                try:
                    count = db.execute_sql(
                        f"COPY vec3 TO {quoted(str(OUT / 'native-vector-output.parquet'))} WITH (FORMAT PARQUET)"
                    )
                    native_output = {"supported": True, "count": count}
                except Exception as e:
                    native_output = {"supported": False, "error": str(e)}
            case["pass"] = True
        except Exception as e:
            case.update(
                {
                    "error": str(e),
                    "exception_type": type(e).__name__,
                    "traceback": traceback.format_exc(),
                }
            )
        finally:
            if db is not None:
                db.close()
            if path.exists():
                shutil.rmtree(path)
        results.append(case)
        save("import-progress.json", results)
        print(
            json.dumps(
                {
                    "representation": representation,
                    "pass": case["pass"],
                    "error": case.get("error"),
                }
            ),
            flush=True,
        )
    report = {
        "source": export["source"],
        "destination": "alopex " + version("alopex"),
        "pyarrow": pa.__version__,
        "numpy": np.__version__,
        "dimensions": DIMS,
        "rows_per_dimension": 5,
        "non_null_vectors_per_dimension": 4,
        "null_vectors_per_dimension": 1,
        "cases": results,
        "native_copy_to": native_output,
        "passed_representations": [x["representation"] for x in results if x["pass"]],
        "failed_representations": [
            x["representation"] for x in results if not x["pass"]
        ],
        "scope": "Published Python wheels; explicit DDL/index recreation; external PyArrow reader and SQL INSERT, distinct from native COPY. Small row counts, not a performance or full database migration test.",
    }
    save("report.json", report)
    check("after-arrow-new-import")
    assert len(results) == len(FORMATS) and all(x["pass"] for x in results), (
        "every Arrow representation must pass both dimensions"
    )


if __name__ == "__main__":
    if sys.argv[1] == "export":
        source_export()
    else:
        migrate()
