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


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_completed_transaction_does_not_keep_database_locked(tmp_path, finish):
    path = tmp_path / "locked.alopex"
    db = Database.open(str(path))
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"key", b"value")
    getattr(txn, finish)()
    db.close()

    reopened = Database.open(str(path))
    try:
        with reopened.begin(TxnMode.READ_ONLY) as reader:
            assert reader.get(b"key") == (b"value" if finish == "commit" else None)
    finally:
        reopened.close()


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
        for sql in (
            "BEGIN",
            "-- retry after a transient failure\nBEGIN",
            "/* optional item */ SAVEPOINT retry",
            "SELECT 1; BEGIN",
            "SELECT 1; /* optional item */ SAVEPOINT retry",
        ):
            with pytest.raises(AlopexError, match=r"db\.begin\(\)"):
                db.execute_sql(sql)
    finally:
        db.close()


def test_failed_transaction_rejects_new_savepoints_but_can_roll_back_to_existing_one():
    db = Database.new()
    try:
        db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        txn = db.begin(TxnMode.READ_WRITE)
        txn.execute_sql("INSERT INTO items (id) VALUES (1)")
        txn.savepoint("recover")

        with pytest.raises(AlopexError):
            txn.execute_sql("INSERT INTO missing_items (id) VALUES (2)")
        with pytest.raises(AlopexError):
            txn.savepoint("after_failure")
        with pytest.raises(AlopexError):
            txn.release("recover")

        txn.rollback_to("recover")
        txn.commit()

        assert db.execute_sql("SELECT id FROM items") == [{"id": 1}]
    finally:
        db.close()
