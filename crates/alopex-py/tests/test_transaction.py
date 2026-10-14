import pytest

from alopex import AlopexError, Database, HnswConfig, Metric, TxnMode


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


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT retry",
        "ROLLBACK TO retry",
        "RELEASE retry",
        "-- leading comment\nSAVEPOINT retry",
        "/* leading comment */ COMMIT",
        "INSERT INTO items (id) VALUES (99); COMMIT",
    ],
)
def test_transaction_execute_sql_rejects_control_before_any_statement_runs(sql):
    db = Database.new()
    try:
        db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        txn = db.begin(TxnMode.READ_WRITE)
        with pytest.raises(AlopexError, match=r"Transaction\.savepoint\(\)"):
            txn.execute_sql(sql)
        assert txn.execute_sql("SELECT id FROM items") == []
        txn.commit()
        assert db.execute_sql("SELECT id FROM items") == []
    finally:
        db.close()


@pytest.mark.requires_numpy
def test_nested_hnsw_savepoint_rollback_restores_only_retained_graph():
    import numpy as np

    db = Database.new()
    try:
        db.create_hnsw_index("vec_idx", HnswConfig(2, metric=Metric.L2))
        txn = db.begin(TxnMode.READ_WRITE)
        txn.upsert_to_hnsw("vec_idx", b"keep", np.array([0.0, 0.0], dtype=np.float32))
        txn.savepoint("first")
        txn.upsert_to_hnsw("vec_idx", b"discard", np.array([1.0, 0.0], dtype=np.float32))
        txn.savepoint("second")
        txn.rollback_to("first")
        txn.upsert_to_hnsw("vec_idx", b"discard_again", np.array([2.0, 0.0], dtype=np.float32))
        txn.rollback_to("first")
        txn.release("first")
        txn.commit()

        results, _ = db.search_hnsw("vec_idx", np.array([0.0, 0.0], dtype=np.float32), 10)
        assert [result.key for result in results] == [b"keep"]
    finally:
        db.close()


@pytest.mark.requires_numpy
@pytest.mark.parametrize("recover", [False, True])
def test_failed_hnsw_restore_blocks_commit_or_recovers_from_clean_savepoint(recover):
    import numpy as np

    db = Database.new()
    try:
        db.create_hnsw_index("vec_idx", HnswConfig(2, metric=Metric.L2))
        txn = db.begin(TxnMode.READ_WRITE)
        txn.upsert_to_hnsw("vec_idx", b"keep", np.array([0.0, 0.0], dtype=np.float32))
        txn.savepoint("clean")
        txn.put(b"hnsw:node:vec_idx:0", b"invalid")
        txn.savepoint("unreadable")
        txn.upsert_to_hnsw("vec_idx", b"discard", np.array([1.0, 0.0], dtype=np.float32))

        with pytest.raises(AlopexError):
            txn.rollback_to("unreadable")
        with pytest.raises(AlopexError):
            txn.execute_sql("SELECT 1")
        with pytest.raises(AlopexError):
            txn.savepoint("new")
        with pytest.raises(AlopexError):
            txn.release("clean")

        if recover:
            txn.rollback_to("clean")
            txn.commit()
            expected = [b"keep"]
        else:
            with pytest.raises(AlopexError):
                txn.commit()
            expected = []
        results, _ = db.search_hnsw("vec_idx", np.array([0.0, 0.0], dtype=np.float32), 10)
        assert [result.key for result in results] == expected
    finally:
        db.close()


@pytest.mark.requires_numpy
def test_sql_after_direct_hnsw_change_can_drop_index():
    import numpy as np

    db = Database.new()
    try:
        db.execute_sql(
            "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2)); "
            "CREATE INDEX vec_idx ON items (embedding) USING HNSW; "
            "INSERT INTO items VALUES (1, [0.0, 0.0]);"
        )
        txn = db.begin(TxnMode.READ_WRITE)
        txn.upsert_to_hnsw("vec_idx", (2).to_bytes(8, "big"), np.array([1.0, 0.0], dtype=np.float32))
        txn.execute_sql("DROP INDEX vec_idx")
        txn.commit()
        with pytest.raises(AlopexError):
            db.search_hnsw("vec_idx", np.array([0.0, 0.0], dtype=np.float32), 10)
    finally:
        db.close()
