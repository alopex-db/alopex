"""Query-local HNSW controls use native, typed prepared parameters (#671)."""

import pytest

from alopex import AlopexError, Database, TxnMode


@pytest.fixture(params=["single", "multi"])
def knn_db(request):
    db = Database.new(thread_mode=request.param)
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, e VECTOR(2, L2))")
    db.execute_sql("INSERT INTO items VALUES (1, [0.0, 0.0]), (2, [1.0, 1.0])")
    db.execute_sql("CREATE INDEX items_h ON items (e) USING HNSW")
    yield db
    db.close()


BASE = "SELECT id FROM items ORDER BY vector_distance(e, ?, 'l2') LIMIT ?"
OPTIONS = " WITH (ef_search = ?, enable_hnsw = ?)"


def test_bound_knn_options_match_literals_and_rebind(knn_db):
    db = knn_db
    for connection in [db, db.begin(TxnMode.READ_ONLY), db.begin(TxnMode.READ_WRITE)]:
        try:
            for ef, enabled in [(16, True), (64, False), (1_000_000, True)]:
                values = [[0.0, 0.0], 1, ef, enabled]
                expected = connection.execute_sql(BASE + f" WITH (ef_search = {ef}, enable_hnsw = {str(enabled).lower()})", values[:2])
                assert connection.execute_sql(BASE + OPTIONS, values) == expected == [{"id": 1}]
            statement = connection.prepare(BASE + OPTIONS)
            try:
                assert statement.parameter_count() == 4
                rows = [[[0.0, 0.0], 1, 16, True], [[1.0, 1.0], 1, 64, False]]
                assert statement.execute_many(rows) == [[{"id": 1}], [{"id": 2}]]
                for index, value in enumerate(rows[0], 1):
                    statement.bind(index, value)
                assert statement.execute() == [{"id": 1}]
                statement.bind(3, 128)
                assert statement.execute() == [{"id": 1}]
            finally:
                statement.finalize()
        finally:
            if connection is not db:
                connection.rollback()


@pytest.mark.parametrize("option, bad", [
    ("ef_search", 0), ("ef_search", -1), ("ef_search", 1_000_001),
    ("ef_search", "16"), ("ef_search", None), ("ef_search", 16.0),
    ("enable_hnsw", 1), ("enable_hnsw", "true"), ("enable_hnsw", None),
])
def test_bad_bound_knn_option_reports_key_and_parameter(knn_db, option, bad):
    sql = "SELECT id FROM items ORDER BY vector_distance(e, ?, 'l2') LIMIT 1"
    with pytest.raises(AlopexError) as caught:
        knn_db.execute_sql(sql + f" WITH ({option} = ?)", [[0.0, 0.0], bad])
    message = str(caught.value)
    assert option in message and "?2" in message


def test_bad_option_in_prepared_batch_rolls_back_earlier_insert(knn_db):
    knn_db.execute_sql("CREATE TABLE selected (id INTEGER PRIMARY KEY)")
    statement = knn_db.prepare("INSERT INTO selected " + BASE + OPTIONS)
    try:
        with pytest.raises(AlopexError, match="ef_search"):
            statement.execute_many([
                [[0.0, 0.0], 1, 16, True],
                [[1.0, 1.0], 1, 0, True],
            ])
        assert knn_db.execute_sql("SELECT * FROM selected") == []
    finally:
        statement.finalize()


def test_missing_knn_option_binding_reports_position(knn_db):
    statement = knn_db.prepare(BASE + OPTIONS)
    try:
        statement.bind(1, [0.0, 0.0])
        statement.bind(2, 1)
        with pytest.raises(AlopexError, match=r"parameter \?3 is unbound"):
            statement.execute()
    finally:
        statement.finalize()
