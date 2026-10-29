"""Tiny VECTOR/Parquet interoperability and atomic rejection cases for #693."""

import io
import struct

import alopex
import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def f32(value):
    return struct.unpack("f", struct.pack("f", value))[0]


@pytest.mark.parametrize("storage", ["row", "columnar"])
@pytest.mark.parametrize("dimension", [3, 384])
def test_vector_export_is_independently_readable_and_roundtrips(
    tmp_path, storage, dimension
):
    ddl = f"CREATE TABLE v(id INTEGER PRIMARY KEY,e VECTOR({dimension},L2)) WITH (storage='{storage}')"
    expected = [
        {"id": 1, "e": [i / 4 + 1 for i in range(dimension)]},
        {"id": 2, "e": [i / 4 + 10 for i in range(dimension)]},
        {"id": 3, "e": None},
    ]
    source = str(tmp_path / "source.db")
    destination = str(tmp_path / "destination.db")
    path = tmp_path / "vectors.parquet"
    db = alopex.Database.open(source)
    db.execute_sql(ddl)
    for row in expected:
        vector = "\\N" if row["e"] is None else f'"{row["e"]}"'
        db.copy_from_csv("v", io.BytesIO(f"{row['id']},{vector}\n".encode()), header=False)
    db.flush()
    db.close()
    db = alopex.Database.open(source)
    assert db.execute_sql("SELECT * FROM v ORDER BY id") == expected
    assert db.execute_sql(f"COPY v TO '{path}' WITH (FORMAT PARQUET)") == 3
    db.close()
    external = pq.read_table(path)
    assert external.to_pylist() == expected
    field = external.schema.field("e")
    assert pa.types.is_list(field.type)
    assert field.type.value_type == pa.float32()
    assert field.metadata[b"alopex.vector.dimension"] == str(dimension).encode()
    assert field.metadata[b"alopex.vector.metric"] == b"L2"
    db = alopex.Database.open(destination)
    db.execute_sql(ddl)
    assert db.execute_sql(f"COPY v FROM '{path}' WITH (FORMAT PARQUET)") == 3
    db.flush()
    db.close()
    db = alopex.Database.open(destination)
    assert db.execute_sql("SELECT * FROM v ORDER BY id") == expected
    search_table = "v"
    if storage == "columnar":
        db.execute_sql(f"CREATE TABLE searchable(id INTEGER PRIMARY KEY,e VECTOR({dimension},L2))")
        assert db.execute_sql(f"COPY searchable FROM '{path}' WITH (FORMAT PARQUET)") == 3
        search_table = "searchable"
    db.execute_sql(f"CREATE INDEX ih ON {search_table}(e) USING HNSW")
    query = expected[0]["e"]
    assert db.execute_sql(
        f"SELECT id FROM {search_table} ORDER BY vector_distance(e,{query},'l2') LIMIT 1 WITH (enable_hnsw=true)"
    ) == [{"id": 1}]
    assert db.hnsw_cache_stats()["entries"] == 1
    db.close()


@pytest.mark.parametrize("kind", ["list", "large", "fixed"])
@pytest.mark.parametrize("width", [32, 64])
@pytest.mark.parametrize("dimension", [3, 384])
def test_external_float_lists_preserve_rounded_elements_after_reopen(
    tmp_path, kind, width, dimension
):
    scalar = pa.float32() if width == 32 else pa.float64()
    dtype = {
        "list": pa.list_(scalar),
        "large": pa.large_list(scalar),
        "fixed": pa.list_(scalar, dimension),
    }[kind]
    values = [
        [i / 10 + 0.1 for i in range(dimension)],
        None,
        [i / 10 + 1 for i in range(dimension)],
    ]
    expected = [
        {"id": i + 1, "e": None if v is None else [f32(x) for x in v]}
        for i, v in enumerate(values)
    ]
    path = tmp_path / "external.parquet"
    pq.write_table(pa.table({"id": pa.array([1, 2, 3], pa.int32()), "e": pa.array(values, dtype)}), path)
    dbpath = str(tmp_path / "destination.db")
    db = alopex.Database.open(dbpath)
    db.execute_sql(f"CREATE TABLE v(id INTEGER PRIMARY KEY,e VECTOR({dimension},COSINE))")
    assert db.execute_sql(f"COPY v FROM '{path}' WITH (FORMAT PARQUET)") == 3
    db.flush()
    db.close()
    db = alopex.Database.open(dbpath)
    assert db.execute_sql("SELECT * FROM v ORDER BY id") == expected
    db.close()


@pytest.mark.parametrize("bad", [
    [], [1.0, 2.0], [1.0, 2.0, 3.0, 4.0], [1.0, None, 3.0],
    [1.0, float("nan"), 3.0], [1.0, float("inf"), 3.0],
    [1.0, -float("inf"), 3.0], [1.0, 1e300, 3.0], [1.0, -1e300, 3.0],
])
def test_invalid_vector_import_preserves_preexisting_rows(db, tmp_path, bad):
    path = tmp_path / "invalid.parquet"
    pq.write_table(pa.table({
        "id": pa.array([1, 2], pa.int32()),
        "e": pa.array([[1.0, 2.0, 3.0], bad], pa.list_(pa.float64())),
    }), path)
    db.execute_sql("CREATE TABLE v(id INTEGER PRIMARY KEY,e VECTOR(3,L2))")
    db.execute_sql("INSERT INTO v VALUES (9,[9,8,7])")
    before = db.execute_sql("SELECT * FROM v ORDER BY id")
    with pytest.raises(alopex.AlopexError):
        db.execute_sql(f"COPY v FROM '{path}' WITH (FORMAT PARQUET)")
    assert db.execute_sql("SELECT * FROM v ORDER BY id") == before


def test_integer_list_is_not_a_vector_input(db, tmp_path):
    path = tmp_path / "integer.parquet"
    pq.write_table(pa.table({"e": pa.array([[1, 2, 3]], pa.list_(pa.int32()))}), path)
    db.execute_sql("CREATE TABLE v(e VECTOR(3,L2))")
    with pytest.raises(alopex.AlopexError):
        db.execute_sql(f"COPY v FROM '{path}' WITH (FORMAT PARQUET)")
    assert db.execute_sql("SELECT * FROM v") == []


def test_empty_vector_export_keeps_declared_schema(db, tmp_path):
    path = tmp_path / "empty.parquet"
    db.execute_sql("CREATE TABLE v(e VECTOR(3,INNER))")
    assert db.execute_sql(f"COPY v TO '{path}' WITH (FORMAT PARQUET)") == 0
    table = pq.read_table(path)
    assert table.num_rows == 0
    assert table.schema.field("e").type.value_type == pa.float32()
    assert table.schema.field("e").metadata[b"alopex.vector.metric"] == b"INNER"


@pytest.mark.parametrize("declared_dimension", [3, 4])
def test_destination_ddl_controls_dimension_despite_metadata(
    db, tmp_path, declared_dimension
):
    path = tmp_path / "metadata.parquet"
    field = pa.field("e", pa.list_(pa.float32()), metadata={
        b"alopex.vector.dimension": b"999", b"alopex.vector.metric": b"INNER",
    })
    pq.write_table(pa.Table.from_arrays(
        [pa.array([[1, 2, 3]], field.type)], schema=pa.schema([field]),
    ), path)
    db.execute_sql(f"CREATE TABLE v(e VECTOR({declared_dimension},COSINE))")
    if declared_dimension == 3:
        assert db.execute_sql(f"COPY v FROM '{path}' WITH (FORMAT PARQUET)") == 1
        assert db.execute_sql("SELECT * FROM v") == [{"e": [1.0, 2.0, 3.0]}]
    else:
        with pytest.raises(alopex.AlopexError, match="dimension mismatch"):
            db.execute_sql(f"COPY v FROM '{path}' WITH (FORMAT PARQUET)")
        assert db.execute_sql("SELECT * FROM v") == []


@pytest.mark.parametrize("storage", ["row", "columnar"])
def test_outer_null_obeys_not_null_without_partial_import(db, tmp_path, storage):
    path = tmp_path / "not-null.parquet"
    pq.write_table(pa.table({
        "e": pa.array([[1, 2, 3], None], pa.list_(pa.float32())),
    }), path)
    db.execute_sql(
        f"CREATE TABLE v(e VECTOR(3,L2) NOT NULL) WITH (storage='{storage}')"
    )
    with pytest.raises(alopex.AlopexError):
        db.execute_sql(f"COPY v FROM '{path}' WITH (FORMAT PARQUET)")
    assert db.execute_sql("SELECT * FROM v") == []


@pytest.mark.parametrize("kind", ["list", "large", "fixed"])
def test_read_parquet_infers_plain_arrays(db, tmp_path, kind):
    dtype = {
        "list": pa.list_(pa.float32()),
        "large": pa.large_list(pa.float32()),
        "fixed": pa.list_(pa.float32(), 3),
    }[kind]
    path = tmp_path / "array.parquet"
    pq.write_table(pa.table({"e": pa.array([[1, 2, 3], None], dtype)}), path)
    assert db.execute_sql(f"SELECT * FROM read_parquet('{path}')") == [
        {"e": [1.0, 2.0, 3.0]}, {"e": None},
    ]
