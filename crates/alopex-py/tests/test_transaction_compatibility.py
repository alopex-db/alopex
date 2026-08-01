import pytest

from alopex import AlopexError, DataFrame, Database, TxnMode


def test_inherited_embedded_kv_sql_dataframe_and_lifecycle_remain_local_only():
    db = Database.new()
    try:
        with pytest.raises(TypeError):
            db.begin(TxnMode.READ_WRITE, require_distributed=True)

        write = db.begin(TxnMode.READ_WRITE)
        write.put(b"compat:\x00a", b"\x00value-a\xff")
        assert write.get(b"compat:\x00a") == b"\x00value-a\xff"
        write.commit()

        with db.begin(TxnMode.READ_ONLY) as read:
            assert read.get(b"compat:\x00a") == b"\x00value-a\xff"

        rolled_back = db.begin(TxnMode.READ_WRITE)
        rolled_back.delete(b"compat:\x00a")
        rolled_back.rollback()
        with db.begin(TxnMode.READ_ONLY) as read:
            assert read.get(b"compat:\x00a") == b"\x00value-a\xff"

        db.execute_sql("CREATE TABLE compat_rows (id INTEGER PRIMARY KEY, label TEXT)")
        db.execute_sql(
            "INSERT INTO compat_rows (id, label) VALUES (?, ?), (?, ?)",
            [2, None, 1, "first"],
        )
        assert db.execute_sql("SELECT id, label FROM compat_rows ORDER BY id") == [
            {"id": 1, "label": "first"},
            {"id": 2, "label": None},
        ]

        frame = DataFrame({"name": [" b ", None, "a"]})
        assert frame.str("name").strip_chars(output="trimmed").to_dict() == {
            "name": [" b ", None, "a"],
            "trimmed": ["b", None, "a"],
        }

        db.close()
        with pytest.raises(AlopexError):
            db.begin(TxnMode.READ_ONLY)
    finally:
        db.close()
