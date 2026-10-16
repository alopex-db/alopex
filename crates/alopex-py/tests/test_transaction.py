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


@pytest.mark.parametrize("finish", ["commit", "rollback", "active"])
def test_retained_transaction_does_not_keep_closed_database_locked(tmp_path, finish):
    path = tmp_path / "locked.alopex"
    db = Database.open(str(path))
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"key", b"value")
    if finish != "active":
        getattr(txn, finish)()
    db.close()

    reopened = Database.open(str(path))
    try:
        with reopened.begin(TxnMode.READ_ONLY) as reader:
            assert reader.get(b"key") == (b"value" if finish == "commit" else None)
    finally:
        reopened.close()
    assert txn.status["state"] == ("committed" if finish == "commit" else "rolled_back")


def test_context_manager_rolls_back():
    db = Database.new()
    with db.begin(TxnMode.READ_WRITE) as txn:
        txn.put(b"key", b"value")

    txn2 = db.begin(TxnMode.READ_ONLY)
    assert txn2.get(b"key") is None
