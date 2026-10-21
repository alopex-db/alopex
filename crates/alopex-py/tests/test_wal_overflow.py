"""Small explicit overflow contracts, with no load or performance assertions."""
import sys

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
