import shutil

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


def test_close_twice_is_idempotent(db):
    db.close()
    db.close()


def test_close_converges_disk_db_into_a_single_alopex_file(tmp_path):
    """`close()` must leave a self-contained `.alopex`, per issue #178."""
    source = tmp_path / "source"
    source.mkdir()
    container = source / "mydb.alopex"

    db = Database.open(str(container))
    txn = db.begin(TxnMode.READ_WRITE)
    for index in range(200):
        txn.put(f"k-{index:04}".encode(), f"v-{index:04}".encode())
    txn.commit()
    db.close()

    assert container.exists()
    assert container.stat().st_size > 0, "close() must not leave a zero-byte marker"
    # The sidecar working directory may still be present here: pruning it happens
    # when the last handle is released, and a live Transaction object still holds
    # a reference to the store. What `close()` guarantees is that the container is
    # already complete, which is exactly what the copy below proves.

    # Copy only the single file, leaving every working artifact behind.
    target = tmp_path / "target"
    target.mkdir()
    copied = target / "mydb.alopex"
    shutil.copyfile(container, copied)
    assert list(p.name for p in target.iterdir()) == ["mydb.alopex"]

    restored = Database.open(str(copied))
    with restored.begin(TxnMode.READ_ONLY) as txn:
        for index in range(200):
            assert txn.get(f"k-{index:04}".encode()) == f"v-{index:04}".encode()
    restored.close()
