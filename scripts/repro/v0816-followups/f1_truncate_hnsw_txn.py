"""Bounded #677 reproduction: two 2D rows -> TRUNCATE -> one row.

Run with the candidate Python package installed. Exit 0 if both transaction
and autocommit paths persist exactly one node. No NumPy or load test required.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

from alopex import Database, TxnMode


def check(transactional):
    with TemporaryDirectory(prefix="alopex-677-") as directory:
        path = str(Path(directory) / "db")
        db = Database.open(path)
        db.execute_sql("CREATE TABLE it (id INT PRIMARY KEY, e VECTOR(2,L2))")
        db.execute_sql("INSERT INTO it VALUES (1,[1,0]),(2,[5,0])")
        db.execute_sql("CREATE INDEX ih ON it (e) USING HNSW")
        db.warm_hnsw_index("ih")
        tx = db.begin(TxnMode.READ_WRITE) if transactional else None
        run = tx.execute_sql if tx else db.execute_sql
        run("TRUNCATE it")
        run("INSERT INTO it VALUES (3,[0,0])")
        if tx:
            tx.commit()
        assert db.get_hnsw_stats("ih").node_count == 1
        assert db.execute_sql("SELECT id FROM it") == [{"id": 3}]
        db.close()
        db = Database.open(path)
        assert db.get_hnsw_stats("ih").node_count == 1
        for enabled in ("true", "false"):
            assert db.execute_sql(
                "SELECT id FROM it ORDER BY vector_distance(e, [0,0], 'l2') "
                f"LIMIT 1 WITH (enable_hnsw = {enabled})"
            ) == [{"id": 3}]
        db.close()
        print(f"{'transaction' if transactional else 'autocommit'}: OK")


if __name__ == "__main__":
    check(True)
    check(False)
