"""Zero/two-row export contracts; no load or performance measurements."""

import io

import alopex
import pytest


@pytest.mark.parametrize("storage", ["columnar", "row"])
@pytest.mark.parametrize("route", ["PARQUET", "CSV", "stream"])
@pytest.mark.parametrize("empty", [False, True])
def test_export_preserves_rows_after_reopen(tmp_path, storage, route, empty):
    ddl = (
        "CREATE TABLE items (id INT PRIMARY KEY, note TEXT, score DOUBLE, "
        f"active BOOLEAN, n BIGINT) WITH (storage='{storage}')"
    )
    source = str(tmp_path / "source.db")
    destination = str(tmp_path / "destination.db")
    output = tmp_path / "export"
    select = "SELECT id, note, score, active, n FROM items ORDER BY id"
    expected = (
        []
        if empty
        else [
            {"id": 1, "note": None, "score": 1.5, "active": None, "n": 2**63 - 1},
            {"id": 2, "note": "", "score": None, "active": False, "n": -(2**63 - 1)},
        ]
    )
    db = alopex.Database.open(source)
    db.execute_sql(ddl)
    if not empty:
        # Separate one-row COPY calls exercise multiple segments.
        for data in [
            b"1,\\N,1.5,\\N,9223372036854775807\n",
            b'2,"",\\N,false,-9223372036854775807\n',
        ]:
            assert db.copy_from_csv("items", io.BytesIO(data), header=False) == 1
    db.flush()
    db.close()
    db = alopex.Database.open(source)
    assert db.execute_sql(select) == expected
    if route == "stream":
        stream = io.BytesIO()
        assert db.copy_to_csv("items", stream, header=True) == len(expected)
    else:
        output.write_bytes(b"previous output")
        assert db.execute_sql(
            f"COPY items TO '{output}' WITH (FORMAT {route}, HEADER TRUE)"
        ) == len(expected)
    db.close()
    assert not list(tmp_path.glob(".*.alopex-copy-*.tmp"))

    db = alopex.Database.open(destination)
    db.execute_sql(ddl)
    if route == "stream":
        assert db.copy_from_csv(
            "items", io.BytesIO(stream.getvalue()), header=True
        ) == len(expected)
    else:
        assert db.execute_sql(
            f"COPY items FROM '{output}' WITH (FORMAT {route}, HEADER TRUE)"
        ) == len(expected)
    db.flush()
    db.close()
    db = alopex.Database.open(destination)
    assert db.execute_sql(select) == expected
    db.close()
