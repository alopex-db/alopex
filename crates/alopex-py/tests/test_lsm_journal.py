"""Three-row public LSM contracts; one owned temporary DB per case, no load."""
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from alopex import AlopexError, Database, TxnMode


@pytest.fixture
def disk_database():
    # The public legacy open uses its default ring geometry. Reclaim it after
    # each case instead of retaining pytest tmp_path DBs until the session ends.
    with TemporaryDirectory(prefix="alopex-684-python-") as temporary:
        path = str(Path(temporary) / "db.alopex")
        with closing(Database.open(path)) as db:
            db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER UNIQUE)")
            db.execute_sql("INSERT INTO items VALUES (1, 7), (2, 8)")
            yield db, path
    assert not Path(temporary).exists()


def rows(tx):
    return tx.execute_sql("SELECT id, value FROM items ORDER BY id")


def test_lsm_journal_rewrites_and_index_entries_survive_reopen(disk_database):
    db, path = disk_database
    with db.begin(TxnMode.READ_WRITE) as tx:
        tx.execute_sql("UPDATE items SET value = 9 WHERE id = 1")
        tx.execute_sql("UPDATE items SET value = 7 WHERE id = 1")
        tx.commit()
    assert rows(db) == [{"id": 1, "value": 7}, {"id": 2, "value": 8}]
    with db.begin(TxnMode.READ_WRITE) as tx:
        tx.execute_sql("UPDATE items SET value = 9 WHERE id = 1")
        tx.execute_sql("DELETE FROM items WHERE id = 2")
        tx.execute_sql("INSERT INTO items VALUES (3, 8)")
        tx.commit()
    db.close()
    with closing(Database.open(path)) as reopened:
        assert rows(reopened) == [{"id": 1, "value": 9}, {"id": 3, "value": 8}]
        assert reopened.execute_sql("SELECT id FROM items WHERE value = 7") == []
        assert reopened.execute_sql("SELECT id FROM items WHERE value = 8") == [{"id": 3}]


def test_lsm_journal_savepoint_and_rollback_preserve_prior_writes(disk_database):
    db, path = disk_database
    with db.begin(TxnMode.READ_WRITE) as tx:
        tx.execute_sql("UPDATE items SET value = 9 WHERE id = 1")
        tx.savepoint("keep")
        tx.execute_sql("DELETE FROM items WHERE id = 2")
        tx.execute_sql("INSERT INTO items VALUES (3, 10)")
        tx.rollback_to("keep")
        tx.release("keep")
        assert rows(tx) == [{"id": 1, "value": 9}, {"id": 2, "value": 8}]
        tx.commit()
    with db.begin(TxnMode.READ_WRITE) as tx:
        tx.execute_sql("UPDATE items SET value = 11 WHERE id = 1")
        assert tx.execute_sql("SELECT value FROM items WHERE id = 1") == [{"value": 11}]
    db.close()
    with closing(Database.open(path)) as reopened:
        assert rows(reopened) == [{"id": 1, "value": 9}, {"id": 2, "value": 8}]


def test_lsm_journal_snapshot_and_explicit_read_conflict(disk_database):
    db, _ = disk_database
    with db.begin(TxnMode.READ_WRITE) as older:
        assert older.execute_sql("SELECT value FROM items WHERE id = 1") == [{"value": 7}]
        with db.begin(TxnMode.READ_WRITE) as newer:
            newer.execute_sql("UPDATE items SET value = 9 WHERE id = 1")
            newer.commit()
        assert older.execute_sql("SELECT value FROM items WHERE id = 1") == [{"value": 7}]
        older.put(b"other", b"pending")
        with pytest.raises(AlopexError) as failure:
            older.commit()
        assert failure.value.code == "ALOPEX-PY104"
    with db.begin(TxnMode.READ_ONLY) as read:
        assert read.get(b"other") is None
        assert rows(read) == [{"id": 1, "value": 9}, {"id": 2, "value": 8}]
