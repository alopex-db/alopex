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


def test_execute_sql_cte_column_name_list_renames_result_keys(db):
    rows = db.execute_sql(
        "WITH renamed(identifier, label) AS (SELECT 7, 'seven') "
        "SELECT label, identifier FROM renamed"
    )
    assert rows == [{"label": "seven", "identifier": 7}]


def test_execute_sql_recursive_cte_reaches_fixed_point(db):
    rows = db.execute_sql(
        "WITH RECURSIVE counter(n) AS ("
        "SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 4"
        ") SELECT n FROM counter ORDER BY n"
    )
    assert rows == [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]


def test_execute_sql_recursive_cte_resource_limit_has_stable_code(db):
    with pytest.raises(AlopexError) as raised:
        db.execute_sql(
            "WITH RECURSIVE cycle(n) AS ("
            "SELECT 1 UNION ALL SELECT n FROM cycle"
            ") SELECT n FROM cycle"
        )

    assert raised.value.code == "ALOPEX-E003"
    assert "recursive CTE 'cycle' reached iteration limit" in str(raised.value)


def test_execute_sql_lag_and_lead_preserve_exact_rows(db):
    db.execute_sql(
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, region TEXT, value INTEGER)"
    )
    db.execute_sql(
        "INSERT INTO samples VALUES "
        "(1, 'east', 10), (2, 'east', 20), "
        "(3, 'west', 30), (4, 'west', 40)"
    )

    rows = db.execute_sql(
        "SELECT id, "
        "LAG(value, 1, -1) OVER (PARTITION BY region ORDER BY id) AS previous, "
        "LEAD(value) OVER (PARTITION BY region ORDER BY id) AS following, "
        "value - LAG(value, 1, value) "
        "OVER (PARTITION BY region ORDER BY id) AS delta "
        "FROM samples ORDER BY id"
    )
    assert rows == [
        {"id": 1, "previous": -1, "following": 20, "delta": 0},
        {"id": 2, "previous": 10, "following": None, "delta": 10},
        {"id": 3, "previous": -1, "following": 40, "delta": 0},
        {"id": 4, "previous": 30, "following": None, "delta": 10},
    ]


def test_execute_sql_rows_and_range_frames_preserve_exact_rows(db):
    db.execute_sql(
        "CREATE TABLE frame_samples "
        "(id INTEGER PRIMARY KEY, amount INTEGER, qty INTEGER)"
    )
    db.execute_sql(
        "INSERT INTO frame_samples VALUES "
        "(1, 10, 3), (2, 20, 1), (3, 20, 5), (4, 30, 2)"
    )
    rows = db.execute_sql(
        "SELECT id, "
        "SUM(qty) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) "
        "AS physical, "
        "SUM(qty) OVER (ORDER BY amount RANGE CURRENT ROW) AS peers "
        "FROM frame_samples ORDER BY id"
    )
    assert rows == [
        {"id": 1, "physical": 4, "peers": 3},
        {"id": 2, "physical": 9, "peers": 6},
        {"id": 3, "physical": 8, "peers": 6},
        {"id": 4, "physical": 7, "peers": 2},
    ]


def test_execute_sql_value_and_distribution_windows_preserve_exact_rows(db):
    db.execute_sql(
        "CREATE TABLE window_samples "
        "(id INTEGER PRIMARY KEY, amount INTEGER)"
    )
    db.execute_sql(
        "INSERT INTO window_samples VALUES "
        "(1, 10), (2, 20), (3, 20), (4, 30)"
    )
    rows = db.execute_sql(
        "SELECT id, "
        "FIRST_VALUE(id) OVER (ORDER BY amount) AS first_id, "
        "LAST_VALUE(id) OVER (ORDER BY amount) AS last_id, "
        "NTH_VALUE(id, 2) OVER (ORDER BY amount) AS second_id, "
        "NTILE(3) OVER (ORDER BY amount) AS bucket, "
        "PERCENT_RANK() OVER (ORDER BY amount) AS percent_rank, "
        "CUME_DIST() OVER (ORDER BY amount) AS cume_dist "
        "FROM window_samples ORDER BY id"
    )
    assert rows == [
        {
            "id": 1,
            "first_id": 1,
            "last_id": 1,
            "second_id": None,
            "bucket": 1,
            "percent_rank": 0.0,
            "cume_dist": 0.25,
        },
        {
            "id": 2,
            "first_id": 1,
            "last_id": 3,
            "second_id": 2,
            "bucket": 1,
            "percent_rank": 1.0 / 3.0,
            "cume_dist": 0.75,
        },
        {
            "id": 3,
            "first_id": 1,
            "last_id": 3,
            "second_id": 2,
            "bucket": 2,
            "percent_rank": 1.0 / 3.0,
            "cume_dist": 0.75,
        },
        {
            "id": 4,
            "first_id": 1,
            "last_id": 4,
            "second_id": 2,
            "bucket": 3,
            "percent_rank": 1.0,
            "cume_dist": 1.0,
        },
    ]


def test_execute_sql_named_windows_and_qualify_preserve_exact_rows(db):
    db.execute_sql(
        "CREATE TABLE qualify_samples "
        "(id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)"
    )
    db.execute_sql(
        "INSERT INTO qualify_samples VALUES "
        "(1, 'east', 10), (2, 'east', 20), "
        "(3, 'west', 30), (4, 'west', 30)"
    )
    rows = db.execute_sql(
        "SELECT id, ROW_NUMBER() OVER ranked AS row_number "
        "FROM qualify_samples "
        "WINDOW ranked AS (base ORDER BY amount DESC, id), "
        "base AS (PARTITION BY region) "
        "QUALIFY row_number = 1 ORDER BY id"
    )
    assert rows == [
        {"id": 2, "row_number": 1},
        {"id": 3, "row_number": 1},
    ]


def test_execute_sql_values_query_preserves_exact_rows(db):
    rows = db.execute_sql("VALUES (2, 'b'), (1, 'a') ORDER BY column1")
    assert rows == [
        {"column1": 1, "column2": "a"},
        {"column1": 2, "column2": "b"},
    ]


def test_execute_sql_standard_predicates_preserve_exact_values(db):
    rows = db.execute_sql(
        "SELECT TRUE IS TRUE AS truth_value, "
        "NULL IS DISTINCT FROM 1 AS distinct_null, "
        "(1, 2) < (1, 3) AS row_less, "
        "(1, NULL) = (1, NULL) AS row_unknown"
    )
    assert rows == [
        {
            "truth_value": True,
            "distinct_null": True,
            "row_less": True,
            "row_unknown": None,
        }
    ]


def test_execute_sql_try_cast_preserves_values_and_cast_errors(db):
    rows = db.execute_sql(
        "SELECT TRY_CAST('42' AS INTEGER) AS parsed, "
        "TRY_CAST('bad' AS INTEGER) AS rejected, "
        "TRY_CAST([1.0, 2.0] AS VECTOR(3)) AS wrong_dimension"
    )
    assert rows == [
        {"parsed": 42, "rejected": None, "wrong_dimension": None}
    ]

    with pytest.raises(AlopexError) as captured:
        db.execute_sql("SELECT CAST('bad' AS INTEGER)")
    rendered = str(captured.value)
    assert "ALOPEX-E004" in rendered
    assert "cannot cast Text to INTEGER" in rendered
    assert "TypedExpr" not in rendered


def test_execute_sql_fetch_pagination_and_with_ties(db):
    db.execute_sql("CREATE TABLE pages (id INTEGER PRIMARY KEY, score INTEGER)")
    db.execute_sql(
        "INSERT INTO pages (id, score) VALUES "
        "(1, 10), (2, 20), (3, 20), (4, 20), (5, 30), (6, NULL)"
    )

    ties = db.execute_sql(
        "SELECT id FROM pages ORDER BY score FETCH FIRST 2 ROWS WITH TIES"
    )
    assert [row["id"] for row in ties] == [1, 2, 3, 4]

    fetched = db.execute_sql(
        "SELECT id FROM pages ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY"
    )
    assert [row["id"] for row in fetched] == [3, 4]

    # `?` placeholders keep working through the documented params substitution
    # (true bind parameters are tracked by issue #166).
    limited = db.execute_sql("SELECT id FROM pages ORDER BY id LIMIT ?", [2])
    assert [row["id"] for row in limited] == [1, 2]

    with pytest.raises(AlopexError) as captured:
        db.execute_sql("SELECT id FROM pages LIMIT -1")
    assert "LIMIT must not be negative" in str(captured.value)

    with pytest.raises(AlopexError) as captured:
        db.execute_sql("SELECT id FROM pages FETCH FIRST 2 ROWS WITH TIES")
    assert "WITH TIES requires ORDER BY" in str(captured.value)


def test_execute_sql_distinct_on_preserves_exact_rows(db):
    db.execute_sql(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)"
    )
    db.execute_sql(
        "INSERT INTO sales (id, region, amount) VALUES "
        "(1, 'east', 100), (2, 'east', 50), (3, 'west', 75), "
        "(4, 'west', 75), (5, NULL, 10), (6, NULL, 5)"
    )

    rows = db.execute_sql(
        "SELECT DISTINCT ON (region) region, amount FROM sales "
        "ORDER BY region, amount"
    )
    assert rows == [
        {"region": "east", "amount": 50},
        {"region": "west", "amount": 75},
        {"region": None, "amount": 5},
    ]

    # D4 tie contract: the two west rows tie on amount; the schema-order
    # tie-breaker deterministically elects id 3.
    tie = db.execute_sql(
        "SELECT DISTINCT ON (region) id FROM sales WHERE region = 'west' "
        "ORDER BY region, amount"
    )
    assert [row["id"] for row in tie] == [3]


def test_execute_sql_distinct_on_order_by_mismatch_raises(db):
    db.execute_sql(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)"
    )
    with pytest.raises(AlopexError) as captured:
        db.execute_sql("SELECT DISTINCT ON (region) region FROM sales ORDER BY amount")
    rendered = str(captured.value)
    assert (
        "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
        in rendered
    )
    assert "ALOPEX-T014" in rendered


def test_execute_sql_aggregate_filter_and_percentile_disc(db):
    db.execute_sql(
        "CREATE TABLE metrics (id INTEGER PRIMARY KEY, g TEXT, v INTEGER, name TEXT)"
    )
    db.execute_sql(
        "INSERT INTO metrics VALUES "
        "(1, 'a', 5, 'x'), (2, 'a', 20, 'y'), (3, 'a', NULL, 'z'), "
        "(4, 'b', 30, 'w'), (5, 'b', NULL, NULL), (6, 'b', 20, 'y')"
    )

    rows = db.execute_sql(
        "SELECT g, COUNT(*) FILTER (WHERE v > 10) AS big, "
        "STRING_AGG(name, ',' ORDER BY v DESC, name ASC) AS names, "
        "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) AS median "
        "FROM metrics GROUP BY g ORDER BY g"
    )
    assert rows == [
        {"g": "a", "big": 1, "names": "y,x,z", "median": 5},
        {"g": "b", "big": 2, "names": "w,y", "median": 20},
    ]

    empty = db.execute_sql(
        "SELECT SUM(v) FILTER (WHERE FALSE) AS none, "
        "COUNT(*) FILTER (WHERE FALSE) AS zero FROM metrics"
    )
    assert empty == [{"none": None, "zero": 0}]

    with pytest.raises(AlopexError) as captured:
        db.execute_sql("SELECT SUM(v) WITHIN GROUP (ORDER BY v) FROM metrics")
    rendered = str(captured.value)
    assert "ALOPEX-T007" in rendered
    assert "WITHIN GROUP is only valid for ordered-set aggregate functions" in rendered
    assert "TypedExpr" not in rendered

    with pytest.raises(AlopexError) as captured:
        db.execute_sql("SELECT PERCENTILE_DISC(2.0) WITHIN GROUP (ORDER BY v) FROM metrics")
    assert "PERCENTILE_DISC fraction must be between 0 and 1" in str(captured.value)


def test_execute_sql_grouped_window_composition_preserves_exact_rows(db):
    db.execute_sql(
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, region TEXT, value INTEGER)"
    )
    db.execute_sql(
        "INSERT INTO samples VALUES "
        "(1, 'east', 10), (2, 'east', 20), "
        "(3, 'west', 30), (4, 'west', 40)"
    )

    rows = db.execute_sql(
        "SELECT region, SUM(value) AS total, "
        "RANK() OVER (ORDER BY SUM(value) DESC) AS sales_rank, "
        "SUM(SUM(value)) OVER () AS retained_total "
        "FROM samples GROUP BY region HAVING SUM(value) >= 30 "
        "ORDER BY sales_rank, region"
    )
    assert rows == [
        {"region": "west", "total": 70, "sales_rank": 1, "retained_total": 100},
        {"region": "east", "total": 30, "sales_rank": 2, "retained_total": 100},
    ]


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
