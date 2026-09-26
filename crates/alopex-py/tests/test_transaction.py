import pytest

from alopex import AlopexError, Database, TxnMode


def test_put_get_delete_commit():
    db = Database.new()
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"key", b"value")
    assert txn.get(b"key") == b"value"
    txn.delete(b"key")
    txn.commit()

    txn2 = db.begin(TxnMode.READ_ONLY)
    assert txn2.get(b"key") is None


def test_read_only_put_is_error():
    db = Database.new()
    txn = db.begin(TxnMode.READ_ONLY)
    with pytest.raises(AlopexError):
        txn.put(b"key", b"value")


def test_commit_closes_transaction():
    db = Database.new()
    txn = db.begin(TxnMode.READ_WRITE)
    txn.commit()
    with pytest.raises(AlopexError):
        txn.get(b"key")


def test_context_manager_rolls_back():
    db = Database.new()
    with db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"key", b"value")

    txn2 = db.begin(TxnMode.READ_ONLY)
    assert txn2.get(b"key") is None


def test_savepoint_rolls_back_sql_work_and_auto_commit_guides_begin():
    db = Database.new()
    try:
        db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        txn = db.begin(TxnMode.READ_WRITE)
        txn.execute_sql("INSERT INTO items (id) VALUES (1)")
        txn.savepoint("optional_item")
        txn.execute_sql("INSERT INTO items (id) VALUES (2)")
        txn.rollback_to("optional_item")
        txn.release("optional_item")
        txn.commit()

        assert db.execute_sql("SELECT id FROM items ORDER BY id") == [{"id": 1}]
        with pytest.raises(AlopexError, match=r"db\.begin\(\)"):
            db.execute_sql("BEGIN")
    finally:
        db.close()
