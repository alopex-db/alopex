import io

import alopex
import pytest


def test_copy_binary_stream_round_trip_and_atomic_failure():
    db = alopex.Database.new()
    db.execute_sql("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)")

    assert db.copy_from_csv("notes", io.BytesIO(b'id,body\n1,"comma, quoted"\n'), header=True) == 1
    output = io.BytesIO()
    assert db.copy_to_csv("notes", output, header=True) == 1
    assert output.getvalue() == b'id,body\n1,"comma, quoted"\n'

    with pytest.raises(alopex.AlopexError):
        db.copy_from_csv("notes", io.BytesIO(b"2,ok\n3,too,many\n"))
    assert db.execute_sql("SELECT id FROM notes ORDER BY id") == [{"id": 1}]
