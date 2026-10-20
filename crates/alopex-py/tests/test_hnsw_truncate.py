"""Two-row functional witnesses for transactional HNSW TRUNCATE (#677)."""

import pytest
from alopex import Database, TxnMode

QUERY = (
    "SELECT id FROM items ORDER BY vector_distance(e, [0,0], 'l2') "
    "LIMIT 3 WITH (enable_hnsw = true)"
)


def seed(db):
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, e VECTOR(2,L2))")
    db.execute_sql("INSERT INTO items VALUES (1,[1,0]),(2,[5,0])")
    db.execute_sql("CREATE INDEX ih ON items (e) USING HNSW")
    assert db.warm_hnsw_index("ih") > 0


@pytest.mark.parametrize("transactional", [False, True])
def test_truncate_commit_and_reopen(tmp_path, transactional):
    path = str(tmp_path / "db")
    db = Database.open(path)
    seed(db)
    before = db.get_hnsw_stats("ih").memory_bytes
    tx = db.begin(TxnMode.READ_WRITE) if transactional else None
    run = tx.execute_sql if tx else db.execute_sql
    run("TRUNCATE items")
    run("INSERT INTO items VALUES (3,[0,0])")
    assert run(QUERY) == [{"id": 3}]
    if tx:
        tx.commit()
    assert db.get_hnsw_stats("ih").node_count == 1
    assert db.get_hnsw_stats("ih").memory_bytes < before
    db.warm_hnsw_index("ih")
    stats = db.hnsw_cache_stats()
    assert stats["entries"] == 1
    assert stats["used_bytes"] == db.get_hnsw_stats("ih").memory_bytes
    db.close()
    db = Database.open(path)
    assert db.get_hnsw_stats("ih").node_count == 1
    assert db.execute_sql(QUERY) == [{"id": 3}]
    db.execute_sql("INSERT INTO items VALUES (4,[2,0])")
    assert db.get_hnsw_stats("ih").node_count == 2
    db.close()


def test_truncate_rollback_preserves_graph():
    db = Database.new()
    seed(db)
    before = db.get_hnsw_stats("ih").memory_bytes
    tx = db.begin(TxnMode.READ_WRITE)
    tx.execute_sql("TRUNCATE items")
    tx.execute_sql("INSERT INTO items VALUES (3,[0,0])")
    assert tx.execute_sql(QUERY) == [{"id": 3}]
    tx.rollback()
    assert db.execute_sql(QUERY) == [{"id": 1}, {"id": 2}]
    assert db.get_hnsw_stats("ih").node_count == 2
    assert db.get_hnsw_stats("ih").memory_bytes == before
    db.close()


def test_truncate_mixed_delete_update():
    db = Database.new()
    seed(db)
    tx = db.begin(TxnMode.READ_WRITE)
    tx.execute_sql("DELETE FROM items WHERE id = 2")
    tx.execute_sql("TRUNCATE items")
    tx.execute_sql("INSERT INTO items VALUES (3,[3,0]),(4,[4,0])")
    tx.execute_sql("UPDATE items SET e = [0,0] WHERE id = 4")
    tx.execute_sql("DELETE FROM items WHERE id = 3")
    assert tx.execute_sql(QUERY) == [{"id": 4}]
    tx.commit()
    assert db.execute_sql(QUERY) == [{"id": 4}]
    stats = db.get_hnsw_stats("ih")
    assert stats.node_count == 1
    assert stats.deleted_count == 1
    db.close()
