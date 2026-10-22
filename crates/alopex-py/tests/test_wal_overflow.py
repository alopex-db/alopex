"""Small explicit overflow contracts, with no load or performance assertions."""
import sys
from pathlib import Path

import pytest
from alopex import AlopexError, Database, WalOverflowOptions, TxnMode


def options(**overrides):
    values = dict(payload_bytes=16384, spool_bytes=32768, directory_bytes=131072,
                  replay_bytes=32768, key_bytes=1024, value_bytes=4096,
                  fragment_bytes=256, wal_segment_bytes=92, wal_segments=1,
                  inline_bytes=1024, activation_bytes=4096, container_fence_bytes=4096)
    values.update(overrides)
    return WalOverflowOptions(**values)


def test_invalid_geometry_rejected_before_storage_creation(tmp_path):
    with pytest.raises(AlopexError):
        Database.open(str(tmp_path / "absent.alopex"), wal_overflow=options(wal_segments=0))
    assert list(tmp_path.iterdir()) == []


def test_default_memory_database_has_no_overflow_statistics():
    db = Database.new()
    assert db.wal_overflow_stats() is None
    db.close()


def test_platform_capability_matches_open_contract(tmp_path):
    assert WalOverflowOptions.writes_supported() == (sys.platform != "win32")
    if sys.platform == "win32":
        with pytest.raises(AlopexError, match="Windows support is pending"):
            Database.open(str(tmp_path / "absent.alopex"), wal_overflow=options())
        assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(sys.platform != "win32", reason="Windows overflow refusal contract")
def test_windows_overflow_refusal_does_not_create_nested_ancestors(tmp_path):
    with pytest.raises(AlopexError, match="Windows support is pending"):
        Database.open(str(tmp_path / "missing" / "child" / "db.alopex"),
                      wal_overflow=options())
    assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(sys.platform != "win32", reason="Windows overflow refusal contract")
def test_windows_overflow_refusal_preserves_existing_file(tmp_path):
    path = tmp_path / "db.alopex"
    path.write_bytes(b"old")
    with pytest.raises(AlopexError, match="Windows support is pending"):
        Database.open(str(path), wal_overflow=options())
    assert path.read_bytes() == b"old"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.skipif(sys.platform == "win32", reason="durable Windows overflow publication pending")
def test_sql_transaction_larger_than_ring_reopens_atomically(tmp_path):
    path = str(tmp_path / "sql.alopex")
    db = Database.open(path, wal_overflow=options())
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE)")
    tx = db.begin(TxnMode.READ_WRITE)
    tx.execute_sql("INSERT INTO items VALUES (1, 'a'), (2, 'b')")
    tx.commit()
    stats = db.wal_overflow_stats()
    assert 64 < stats["directory_bytes"] <= stats["directory_limit_bytes"]
    db.close()
    with pytest.raises(AlopexError, match="overflow"):
        Database.open(path)
    db = Database.open(path, wal_overflow=options())
    assert db.execute_sql("SELECT id, code FROM items ORDER BY id") == [
        {"id": 1, "code": "a"}, {"id": 2, "code": "b"}]
    with pytest.raises(AlopexError):
        db.execute_sql("INSERT INTO items VALUES (3, 'a')")
    assert db.wal_overflow_stats()["gc_failures"] == 0
    db.close()


def test_capacity_error_code_is_registered():
    from alopex import ALOPEX_ERROR_CODES
    assert "ALOPEX-S004" in ALOPEX_ERROR_CODES


@pytest.mark.skipif(sys.platform == "win32", reason="durable Windows overflow publication pending")
def test_kv_capacity_error_preserves_previous_commit(tmp_path):
    path = str(tmp_path / "capacity.alopex")
    db = Database.open(path, wal_overflow=options(value_bytes=2))
    tx = db.begin(TxnMode.READ_WRITE)
    tx.put(b"k", b"ok")
    tx.commit()
    tx = db.begin(TxnMode.READ_WRITE)
    with pytest.raises(AlopexError) as failure:
        tx.put(b"k", b"bad")
    assert failure.value.code == "ALOPEX-S004"
    assert tx.get(b"k") == b"ok"
    tx.rollback()
    db.close()
    db = Database.open(path, wal_overflow=options(value_bytes=2))
    tx = db.begin(TxnMode.READ_ONLY)
    assert tx.get(b"k") == b"ok"
    tx.rollback()
    db.close()


@pytest.mark.skipif(sys.platform == "win32", reason="durable Windows overflow publication pending")
def test_sql_capacity_error_preserves_previous_row(tmp_path):
    path = str(tmp_path / "sql-capacity.alopex")
    db = Database.open(path, wal_overflow=options())
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, value TEXT)")
    db.execute_sql("INSERT INTO items VALUES (1, 'ok')")
    db.close()
    db = Database.open(path, wal_overflow=options(value_bytes=64))
    with pytest.raises(AlopexError) as failure:
        db.execute_sql("INSERT INTO items VALUES (2, '" + "x" * 65 + "')")
    assert failure.value.code == "ALOPEX-S004"
    db.close()
    db = Database.open(path, wal_overflow=options())
    assert db.execute_sql("SELECT id, value FROM items") == [{"id": 1, "value": "ok"}]
    db.close()


def seeded_overflow(path):
    db = Database.open(str(path), wal_overflow=options())
    tx = db.begin(TxnMode.READ_WRITE)
    tx.put(b"previous", b"ok")
    tx.commit()
    return db


def assert_previous_after_reopen(path, db):
    db.close()
    db = Database.open(str(path), wal_overflow=options())
    with db.begin(TxnMode.READ_ONLY) as tx:
        assert tx.get(b"previous") == b"ok"
        assert tx.get(b"pending") is None
    db.close()


@pytest.mark.skipif(sys.platform == "win32", reason="public Windows overflow qualification pending")
@pytest.mark.parametrize("terminal", ["rollback", "drop", "context_exception"])
def test_uncommitted_transaction_preserves_prior_commit(tmp_path, terminal):
    path = tmp_path / "cancel.alopex"
    db = seeded_overflow(path)
    before = db.wal_overflow_stats()
    tx = db.begin(TxnMode.READ_WRITE)
    tx.put(b"pending", b"x" * 65)  # One byte beyond ring payload, not a load fixture.
    if terminal == "rollback":
        tx.rollback()
    elif terminal == "drop":
        del tx
    else:
        with pytest.raises(RuntimeError, match="caller cancelled"):
            with tx:
                raise RuntimeError("caller cancelled")
    assert db.wal_overflow_stats() == before
    assert_previous_after_reopen(path, db)


@pytest.mark.skipif(sys.platform == "win32", reason="public Windows overflow qualification pending")
def test_sql_stream_cancel_rolls_back_rows_and_unique_index(tmp_path):
    path = tmp_path / "stream.alopex"
    db = Database.open(str(path), wal_overflow=options())
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE)")
    db.execute_sql("INSERT INTO items VALUES (1, 'old')")
    before = db.wal_overflow_stats()
    tx = db.begin(TxnMode.READ_WRITE)
    tx.execute_sql("INSERT INTO items VALUES (2, 'pending')")
    stream = tx.execute_sql_stream("SELECT id, code FROM items")
    stream.cancel()
    with pytest.raises(AlopexError) as failure:
        tx.commit()
    assert failure.value.code == "stream_abort_required"
    tx.rollback()
    assert db.wal_overflow_stats() == before
    db.close()
    db = Database.open(str(path), wal_overflow=options())
    assert db.execute_sql("SELECT id, code FROM items") == [{"id": 1, "code": "old"}]
    # Both primary and unique keys from the cancelled write can be reused.
    db.execute_sql("INSERT INTO items VALUES (2, 'pending')")
    assert db.execute_sql("SELECT id, code FROM items ORDER BY id") == [
        {"id": 1, "code": "old"}, {"id": 2, "code": "pending"}]
    db.close()


@pytest.mark.skipif(sys.platform == "win32", reason="public Windows overflow qualification pending")
@pytest.mark.parametrize("foreign_file", [False, True], ids=["missing-directory", "foreign-file"])
def test_commit_inventory_failure_preserves_prior_commit(tmp_path, foreign_file):
    path = tmp_path / "io.alopex"
    db = seeded_overflow(path)
    before = db.wal_overflow_stats()
    spool = Path(str(path) + ".d") / "overflow"
    saved = tmp_path / "saved-overflow"
    spool.rename(saved)
    if foreign_file:
        spool.write_bytes(b"x")
    try:
        tx = db.begin(TxnMode.READ_WRITE)
        tx.put(b"pending", b"x" * 65)
        with pytest.raises(AlopexError) as failure:
            tx.commit()
        assert failure.value.code == "ALOPEX-PY999"  # Neither is configured capacity.
        if foreign_file:
            assert spool.read_bytes() == b"x"
        else:
            assert not spool.exists()
    finally:
        if foreign_file:
            spool.unlink()
        saved.rename(spool)
    assert db.wal_overflow_stats() == before
    assert_previous_after_reopen(path, db)
