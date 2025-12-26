import pytest

from alopex import AlopexError, Database, EmbeddedConfig, TxnMode


def test_open_in_memory():
    db = Database.open_in_memory()
    stats = db.memory_usage()
    assert stats.total_bytes >= 0
    db.close()


def test_new_is_in_memory(db):
    stats = db.memory_usage()
    assert stats.total_bytes >= 0


def test_open_with_config_in_memory():
    config = EmbeddedConfig()
    db = Database.open_with_config(config)
    db.close()


def test_begin_default_is_read_only(db):
    txn = db.begin()
    with pytest.raises(AlopexError):
        txn.put(b"key", b"value")


def test_begin_read_write_allows_put(db):
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"key", b"value")
    txn.commit()


def test_close_rolls_back_active_transaction(db):
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"key", b"value")
    db.close()
    with pytest.raises(AlopexError):
        txn.get(b"key")


def test_close_twice_is_error(db):
    db.close()
    with pytest.raises(AlopexError):
        db.close()
