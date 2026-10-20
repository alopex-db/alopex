"""Two-row, independent cases for CSV file/stream NULL preservation."""

import io

import alopex
import pytest


@pytest.mark.parametrize("route", ["file", "query", "stream"])
@pytest.mark.parametrize(
    "text",
    [
        "",
        r"\N",
        "NULL",
        " nUlL ",
        "one,two",
        "one\"two'三",
        "one\r\ntwo\n三",
        "日本語🙂",
        " \ttext \t",
        r"\\N\path",
    ],
)
def test_csv_null_and_text_survive_reopen(tmp_path, route, text):
    source = str(tmp_path / "source.alopex")
    destination = str(tmp_path / "destination.alopex")
    csv_path = tmp_path / "rows.csv"
    ddl = "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)"
    db = alopex.Database.open(source)
    db.execute_sql(ddl)
    escaped = text.replace("'", "''")
    db.execute_sql(f"INSERT INTO notes VALUES (1, NULL), (2, '{escaped}')")
    db.flush()
    db.close()

    db = alopex.Database.open(source)
    if route == "stream":
        output = io.BytesIO()
        assert db.copy_to_csv("notes", output, header=True) == 2
        contents = output.getvalue()
    else:
        table = (
            "(SELECT id, body FROM notes ORDER BY id)" if route == "query" else "notes"
        )
        db.execute_sql(f"COPY {table} TO '{csv_path}' WITH (FORMAT CSV, HEADER TRUE)")
    db.close()

    db = alopex.Database.open(destination)
    db.execute_sql(ddl)
    if route == "stream":
        assert db.copy_from_csv("notes", io.BytesIO(contents), header=True) == 2
    else:
        db.execute_sql(f"COPY notes FROM '{csv_path}' WITH (FORMAT CSV, HEADER TRUE)")
    db.flush()
    db.close()
    db = alopex.Database.open(destination)
    assert db.execute_sql("SELECT id, body FROM notes ORDER BY id") == [
        {"id": 1, "body": None},
        {"id": 2, "body": text},
    ]
    db.close()
