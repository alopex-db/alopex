"""Database.execute_sql / Transaction.execute_sql の契約テスト。

公開ガイド (docs/guides/python.md) が約束する API 形状を検証する:

- ``db.execute_sql(sql)`` / ``db.execute_sql(sql, params)`` (``?`` プレースホルダ)
- SELECT は行の list を返し、各行は列名でアクセスできる (``row["name"]``)
- DML は影響行数 (int)、DDL は None を返す
- ``Transaction.execute_sql`` も同一形状
"""

import pytest

from alopex import AlopexError, Database, TxnMode


@pytest.fixture()
def users_db(db):
    db.execute_sql(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )
        """
    )
    return db


def test_execute_sql_ddl_returns_none(db):
    result = db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    assert result is None


def test_execute_sql_insert_returns_rows_affected(users_db):
    result = users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
    )
    assert result == 1


def test_execute_sql_select_returns_list_of_name_accessible_rows(users_db):
    users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
    )
    results = users_db.execute_sql("SELECT * FROM users WHERE id = 1")
    assert isinstance(results, list)
    assert len(results) == 1
    row = results[0]
    assert row["id"] == 1
    assert row["name"] == "Alice"
    assert row["email"] == "alice@example.com"


def test_execute_sql_select_iteration_matches_guide_usage(users_db):
    users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES "
        "(1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')"
    )
    results = users_db.execute_sql("SELECT * FROM users ORDER BY id")
    rendered = [f"User: {row['name']} ({row['email']})" for row in results]
    assert rendered == [
        "User: Alice (alice@example.com)",
        "User: Bob (bob@example.com)",
    ]


def test_execute_sql_select_empty_returns_empty_list(users_db):
    assert users_db.execute_sql("SELECT * FROM users") == []


def test_execute_sql_params_binding(users_db):
    users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        [1, "Alice", "alice@example.com"],
    )
    results = users_db.execute_sql(
        "SELECT name, email FROM users WHERE id = ?", [1]
    )
    assert results == [{"name": "Alice", "email": "alice@example.com"}]


def test_execute_sql_params_accepts_tuple_and_none_value(users_db):
    users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        (1, "Alice", None),
    )
    rows = users_db.execute_sql("SELECT email FROM users WHERE id = ?", (1,))
    assert rows == [{"email": None}]


def test_execute_sql_params_escape_quotes(users_db):
    tricky = "O'Brien'; DROP TABLE users; --"
    users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)", [1, tricky, None]
    )
    rows = users_db.execute_sql(
        "SELECT name FROM users WHERE name = ?", [tricky]
    )
    assert rows == [{"name": tricky}]


def test_execute_sql_question_mark_inside_literal_is_not_placeholder(users_db):
    users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, 'wh?t', NULL)", [1]
    )
    rows = users_db.execute_sql("SELECT name FROM users WHERE id = ?", [1])
    assert rows == [{"name": "wh?t"}]


def test_execute_sql_value_types_roundtrip(db):
    db.execute_sql(
        "CREATE TABLE vals (id INTEGER PRIMARY KEY, flag BOOLEAN, ratio DOUBLE, big BIGINT)"
    )
    db.execute_sql(
        "INSERT INTO vals (id, flag, ratio, big) VALUES (?, ?, ?, ?)",
        [1, True, 0.5, 2**40],
    )
    rows = db.execute_sql("SELECT flag, ratio, big FROM vals WHERE id = ?", [1])
    assert rows == [{"flag": True, "ratio": 0.5, "big": 2**40}]


def test_execute_sql_vector_param_roundtrip(db):
    db.execute_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
    db.execute_sql(
        "INSERT INTO docs (id, embedding) VALUES (?, ?)",
        [1, [0.25, -1.5, 2.0]],
    )
    rows = db.execute_sql("SELECT embedding FROM docs WHERE id = ?", [1])
    assert rows == [{"embedding": [0.25, -1.5, 2.0]}]


def test_execute_sql_numpy_vector_param(db):
    np = pytest.importorskip("numpy")
    db.execute_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
    embedding = np.array([0.25, -1.5, 2.0], dtype=np.float32)
    db.execute_sql(
        "INSERT INTO docs (id, embedding) VALUES (?, ?)", [1, embedding]
    )
    rows = db.execute_sql("SELECT embedding FROM docs WHERE id = ?", [1])
    assert np.allclose(np.array(rows[0]["embedding"]), embedding)


def test_execute_sql_param_count_mismatch_raises_value_error(users_db):
    with pytest.raises(ValueError):
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [])
    with pytest.raises(ValueError):
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [1, 2])


def test_execute_sql_params_must_be_list_or_tuple(users_db):
    with pytest.raises(TypeError):
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", 1)
    with pytest.raises(TypeError):
        users_db.execute_sql("SELECT * FROM users WHERE name = ?", "Alice")


def test_execute_sql_unsupported_param_type_raises_type_error(users_db):
    with pytest.raises(TypeError):
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [{"a": 1}])


def test_execute_sql_bytes_param_raises_not_implemented(users_db):
    with pytest.raises(NotImplementedError):
        users_db.execute_sql("SELECT * FROM users WHERE name = ?", [b"raw"])


def test_execute_sql_non_finite_float_param_raises_value_error(users_db):
    with pytest.raises(ValueError):
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [float("nan")])
    with pytest.raises(ValueError):
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [float("inf")])


def test_execute_sql_invalid_sql_raises_alopex_error_with_code(db):
    with pytest.raises(AlopexError) as raised:
        db.execute_sql("SELEKT 1 FRUM nowhere")
    assert getattr(raised.value, "code", "").startswith("ALOPEX-")


def test_execute_sql_on_closed_database_raises(db):
    db.close()
    with pytest.raises(AlopexError):
        db.execute_sql("SELECT 1")


def test_transaction_execute_sql_commit_visible(users_db):
    txn = users_db.begin(TxnMode.READ_WRITE)
    txn.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)", [1, "Alice", None]
    )
    txn.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)", [2, "Bob", None]
    )
    txn.commit()
    rows = users_db.execute_sql("SELECT id FROM users ORDER BY id")
    assert [row["id"] for row in rows] == [1, 2]


def test_transaction_execute_sql_rollback_discards(users_db):
    txn = users_db.begin(TxnMode.READ_WRITE)
    txn.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)", [1, "Alice", None]
    )
    txn.rollback()
    assert users_db.execute_sql("SELECT id FROM users") == []


def test_transaction_execute_sql_select_within_transaction(users_db):
    txn = users_db.begin(TxnMode.READ_WRITE)
    txn.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)", [1, "Alice", None]
    )
    rows = txn.execute_sql("SELECT name FROM users WHERE id = ?", [1])
    assert rows == [{"name": "Alice"}]
    txn.rollback()


def test_transaction_execute_sql_after_close_raises(users_db):
    txn = users_db.begin(TxnMode.READ_WRITE)
    txn.rollback()
    with pytest.raises(AlopexError):
        txn.execute_sql("SELECT * FROM users")


def test_execute_sql_param_error_codes_are_stable(users_db):
    with pytest.raises(ValueError) as raised:
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [1, 2])
    assert raised.value.code == "ALOPEX-PY015"

    with pytest.raises(TypeError) as raised:
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [{"a": 1}])
    assert raised.value.code == "ALOPEX-PY016"

    with pytest.raises(ValueError) as raised:
        users_db.execute_sql("SELECT * FROM users WHERE id = ?", [float("nan")])
    assert raised.value.code == "ALOPEX-PY017"

    with pytest.raises(NotImplementedError) as raised:
        users_db.execute_sql("SELECT * FROM users WHERE name = ?", [b"raw"])
    assert raised.value.code == "ALOPEX-PY018"


def test_execute_sql_placeholder_inside_comment_is_ignored(users_db):
    users_db.execute_sql(
        "INSERT INTO users (id, name, email) VALUES (?, 'x', NULL) -- comment?",
        [1],
    )
    rows = users_db.execute_sql(
        "SELECT id FROM users /* which? */ WHERE id = ?", [1]
    )
    assert [row["id"] for row in rows] == [1]


def test_execute_sql_nul_character_param_raises_value_error(users_db):
    with pytest.raises(ValueError):
        users_db.execute_sql("SELECT * FROM users WHERE name = ?", ["a\x00b"])
