"""One-record public API checks for Windows directory publication and pruning."""
import sys

import pytest
from alopex import Database, TxnMode

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows directory publication")


def test_nested_kv_database_prunes_and_reopens(tmp_path):
    path = tmp_path / "新規" / "child" / "kv.alopex"
    db = Database.open(str(path))
    tx = db.begin(TxnMode.READ_WRITE)
    tx.put(b"k", b"v")
    tx.commit()
    del tx  # Release the last transaction reference before testing prune.
    db.close()
    assert path.is_file()
    assert not path.with_name(path.name + ".d").exists()
    db = Database.open(str(path))
    tx = db.begin(TxnMode.READ_ONLY)
    assert tx.get(b"k") == b"v"
    tx.rollback()
    del tx
    db.close()


def test_nested_sql_database_prunes_and_reopens(tmp_path):
    path = tmp_path / "新規" / "child" / "sql.alopex"
    db = Database.open(str(path))
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY)")
    db.execute_sql("INSERT INTO items VALUES (1)")
    db.close()
    assert path.is_file()
    assert not path.with_name(path.name + ".d").exists()
    db = Database.open(str(path))
    assert db.execute_sql("SELECT id FROM items") == [{"id": 1}]
    db.close()
