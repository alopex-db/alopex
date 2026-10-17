"""Public SQL Parquet conversion and row-diagnostic regressions for #477."""

import pytest
from alopex import AlopexError

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def test_parquet_copy_and_insert_preserve_large_types_and_integer_bounds(db, tmp_path):
    path = tmp_path / "large.parquet"
    expected = [
        {"id": -(2**31), "body": "日本語", "payload": b"\x00\xffA"},
        {"id": 2**31 - 1, "body": "", "payload": b""},
        {"id": None, "body": None, "payload": None},
    ]
    pq.write_table(
        pa.table(
            {
                "id": pa.array([row["id"] for row in expected], pa.int64()),
                "body": pa.array([row["body"] for row in expected], pa.large_string()),
                "payload": pa.array(
                    [row["payload"] for row in expected], pa.large_binary()
                ),
            }
        ),
        path,
    )
    assert db.execute_sql(f"SELECT * FROM read_parquet('{path}')") == expected
    for table, statement in [
        ("copied", f"COPY copied FROM '{path}' WITH (FORMAT PARQUET)"),
        ("selected", f"INSERT INTO selected SELECT * FROM read_parquet('{path}')"),
    ]:
        db.execute_sql(f"CREATE TABLE {table} (id INTEGER, body TEXT, payload BLOB)")
        assert db.execute_sql(statement) == 3
        assert (
            db.execute_sql(f"SELECT * FROM {table} WHERE id IS NOT NULL ORDER BY id")
            == expected[:2]
        )
        assert db.execute_sql(f"SELECT * FROM {table} WHERE id IS NULL") == expected[2:]


@pytest.mark.parametrize("overflow", [-(2**31) - 1, 2**31])
def test_parquet_overflow_reports_row_after_batch_boundary(db, tmp_path, overflow):
    path = tmp_path / "overflow.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1] * 1024 + [overflow], pa.int64())}), path
    )
    for table, statement, diagnostic in [
        (
            "copied",
            f"COPY copied FROM '{path}' WITH (FORMAT PARQUET)",
            f"row 1025: cannot convert INT64 value {overflow} to INTEGER",
        ),
        (
            "selected",
            f"INSERT INTO selected SELECT * FROM read_parquet('{path}')",
            "cannot cast BigInt to INTEGER: input row 1025: value is out of range",
        ),
    ]:
        db.execute_sql(f"CREATE TABLE {table} (id INTEGER)")
        with pytest.raises(AlopexError) as error:
            db.execute_sql(statement)
        assert diagnostic in str(error.value)


def test_parquet_type_mismatch_does_not_report_matching_column_counts(db, tmp_path):
    path = tmp_path / "wrong_type.parquet"
    pq.write_table(pa.table({"id": ["not an integer"]}), path)
    db.execute_sql("CREATE TABLE typed (id INTEGER)")
    with pytest.raises(AlopexError) as error:
        db.execute_sql(f"COPY typed FROM '{path}' WITH (FORMAT PARQUET)")
    message = str(error.value)
    assert "type mismatch for column 'id'" in message
    assert "expected 1 columns, got 1" not in message
