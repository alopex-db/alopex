import pytest

from alopex import AlopexError, Database, TxnMode


def test_issue582_transaction_prepared_typed_rebind(db):
    txn = db.begin(TxnMode.READ_WRITE)
    reference = db.prepare("SELECT ? AS value")
    try:
        statement = txn.prepare("SELECT ? AS value")
        assert type(statement) is type(reference)
        assert statement.parameter_count() == 1
        for value in (None, True, 7, 5.0, "SAVEPOINT ' ?", [1.0, 2.0]):
            statement.bind(1, value)
            result = statement.execute()[0]["value"]
            assert type(result) is type(value)
            assert result == value
        statement.reset()
        with pytest.raises(AlopexError, match="unbound"):
            statement.execute()
        with pytest.raises(AlopexError, match="outside"):
            statement.bind(0, 1)
        statement.finalize()
    finally:
        reference.finalize()
        txn.rollback()


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_issue582_transaction_prepared_owns_existing_transaction(db, finish):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    txn.execute_sql("INSERT INTO items VALUES (1)")
    statement = txn.prepare("INSERT INTO items VALUES (?)")
    statement.bind(1, 2)
    assert statement.execute() == 1
    assert txn.execute_sql("SELECT id FROM items ORDER BY id") == [{"id": 1}, {"id": 2}]
    getattr(txn, finish)()
    assert db.execute_sql("SELECT id FROM items ORDER BY id") == (
        [{"id": 1}, {"id": 2}] if finish == "commit" else []
    )
    statement.finalize()


def test_issue582_transaction_prepared_batch_success(db):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    txn.execute_sql("INSERT INTO items VALUES (1)")
    statement = txn.prepare("INSERT INTO items VALUES (?)")
    assert statement.execute_many([[2], [3]]) == [1, 1]
    assert txn.execute_many("INSERT INTO items VALUES (?)", ((4,), (5,))) == [1, 1]
    assert txn.execute_many("INSERT INTO items VALUES (?)", []) == []
    assert txn.execute_sql("SELECT id FROM items ORDER BY id") == [
        {"id": value} for value in range(1, 6)
    ]
    txn.rollback()
    assert db.execute_sql("SELECT id FROM items") == []
    statement.finalize()


@pytest.mark.parametrize("attempt_commit", [False, True])
def test_issue582_transaction_prepared_batch_failure_requires_rollback(db, attempt_commit):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    txn.execute_sql("INSERT INTO items VALUES (1)")
    statement = txn.prepare("INSERT INTO items VALUES (?)")
    with pytest.raises(AlopexError):
        statement.execute_many([[2], [2]])
    if attempt_commit:
        with pytest.raises(AlopexError):
            txn.commit()
        assert txn.status["state"] == "rolled_back"
        with pytest.raises(AlopexError, match="transaction is closed"):
            txn.rollback()
    else:
        txn.rollback()
    assert db.execute_sql("SELECT id FROM items") == []
    statement.finalize()


def test_issue582_transaction_prepared_preflight_preserves_transaction(db):
    from collections import UserDict
    from decimal import Decimal

    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    txn.execute_sql("INSERT INTO items VALUES (1)")
    statement = txn.prepare("INSERT INTO items VALUES (?)")
    with pytest.raises(ValueError) as error:
        statement.execute_many([[2], []])
    assert error.value.code == "ALOPEX-PY015"
    for value in (UserDict({2: "value"}), {2}, Decimal("2.0")):
        with pytest.raises(TypeError):
            statement.execute_many([[2], [value]])
    assert txn.execute_sql("SELECT id FROM items") == [{"id": 1}]
    txn.commit()
    assert db.execute_sql("SELECT id FROM items") == [{"id": 1}]
    statement.finalize()


@pytest.mark.parametrize("finish", ["commit", "rollback", "close"])
def test_issue582_transaction_prepared_lifecycle(tmp_path, finish):
    path = str(tmp_path / "prepared-transaction")
    db = Database.open(path)
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    statement = txn.prepare("INSERT INTO items VALUES (?)")
    statement.bind(1, 1)
    if finish == "close":
        db.close()
    else:
        getattr(txn, finish)()
    with pytest.raises(AlopexError):
        statement.execute()
    with pytest.raises(AlopexError):
        statement.execute_many([[2]])
    for operation in (lambda: statement.bind(1, 2), statement.reset, statement.parameter_count):
        with pytest.raises(AlopexError):
            operation()
    statement.finalize()
    with pytest.raises(AlopexError, match="prepared statement is finalized"):
        statement.finalize()
    db.close()
    reopened = Database.open(path)
    try:
        assert reopened.execute_sql("SELECT id FROM items") == []
    finally:
        reopened.close()


def test_issue582_transaction_prepared_original_owner_drop_rolls_back(db):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    txn.execute_sql("INSERT INTO items VALUES (1)")
    statement = txn.prepare("SELECT id FROM items")
    del txn
    with pytest.raises(AlopexError, match="transaction is closed"):
        statement.execute()
    statement.finalize()
    assert db.execute_sql("SELECT id FROM items") == []


@pytest.mark.parametrize("early_close", [False, True])
def test_issue582_transaction_prepared_stream_lease_outcome(db, early_close):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    db.execute_sql("INSERT INTO items VALUES (1), (2)")
    txn = db.begin(TxnMode.READ_WRITE)
    statement = txn.prepare("INSERT INTO items VALUES (?)")
    statement.bind(1, 3)
    stream = txn.execute_sql_stream("SELECT id FROM items")
    with pytest.raises(AlopexError):
        statement.execute()
    if early_close:
        stream.close()
        assert txn.status["stream_effect"] == "must_abort"
        with pytest.raises(AlopexError):
            statement.execute()
    else:
        assert sorted(row["id"] for row in stream) == [1, 2]
        assert statement.execute() == 1
    txn.rollback()
    statement.finalize()
    assert db.execute_sql("SELECT id FROM items ORDER BY id") == [{"id": 1}, {"id": 2}]


def test_issue582_transaction_prepared_rejects_control_sql(db):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    txn.execute_sql("INSERT INTO items VALUES (1)")
    for sql in ("", "/* comment */ COMMIT", "SAVEPOINT retry",
                "INSERT INTO items VALUES (?); ROLLBACK"):
        with pytest.raises(AlopexError):
            txn.prepare(sql)
        with pytest.raises(AlopexError):
            txn.execute_many(sql, [[99]])
    statement = txn.prepare("SELECT 'SAVEPOINT retry' AS value")
    assert statement.execute() == [{"value": "SAVEPOINT retry"}]
    statement.finalize()
    txn.commit()
    assert db.execute_sql("SELECT id FROM items") == [{"id": 1}]


def test_issue582_transaction_prepared_caller_savepoint_recovers(db):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    txn = db.begin(TxnMode.READ_WRITE)
    txn.execute_sql("INSERT INTO items VALUES (1)")
    txn.savepoint("before_batch")
    with pytest.raises(AlopexError):
        txn.execute_many("INSERT INTO items VALUES (?)", [[2], [2]])
    txn.rollback_to("before_batch")
    txn.release("before_batch")
    assert txn.execute_sql("SELECT id FROM items") == [{"id": 1}]
    assert txn.execute_many("INSERT INTO items VALUES (?)", [[3]]) == [1]
    txn.commit()
    assert db.execute_sql("SELECT id FROM items ORDER BY id") == [{"id": 1}, {"id": 3}]


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


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_completed_transaction_does_not_keep_database_locked(tmp_path, finish):
    path = tmp_path / "locked.alopex"
    db = Database.open(str(path))
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"key", b"value")
    getattr(txn, finish)()
    db.close()

    reopened = Database.open(str(path))
    try:
        with reopened.begin(TxnMode.READ_ONLY) as reader:
            assert reader.get(b"key") == (b"value" if finish == "commit" else None)
    finally:
        reopened.close()


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


@pytest.mark.parametrize("control", [
    "BEGIN", "START TRANSACTION", "SET TRANSACTION READ ONLY", "COMMIT",
    "ROLLBACK", "SAVEPOINT retry", "ROLLBACK TO SAVEPOINT retry",
    "RELEASE SAVEPOINT retry",
])
@pytest.mark.parametrize("prefix, params", [
    ("", None), ("-- transaction control\n/* retry */ ", None),
    ("INSERT INTO items (id) VALUES (99); /* retry */ ", None),
    ("INSERT INTO items (id) VALUES (?); /* retry */ ", [99]),
])
def test_transaction_sql_control_rejection_preserves_writes(control, prefix, params):
    db = Database.new()
    try:
        db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        txn = db.begin(TxnMode.READ_WRITE)
        txn.put(b"before", b"kept")
        txn.execute_sql("INSERT INTO items (id) VALUES (1)")
        txn.savepoint("retry")
        with pytest.raises(AlopexError) as rejected:
            txn.execute_sql(prefix + control, params)

        # Rejection must neither run preceding DML nor poison/finish the txn.
        assert txn.get(b"before") == b"kept"
        assert txn.execute_sql("SELECT id FROM items ORDER BY id") == [{"id": 1}]
        txn.put(b"after", b"also kept")
        txn.execute_sql("INSERT INTO items (id) VALUES (2)")
        txn.commit()
        with db.begin(TxnMode.READ_ONLY) as reader:
            assert reader.get(b"before") == b"kept"
            assert reader.get(b"after") == b"also kept"
        assert db.execute_sql("SELECT id FROM items ORDER BY id") == [
            {"id": 1}, {"id": 2},
        ]
        message = str(rejected.value)
        for method in ("savepoint", "rollback_to", "release", "commit", "rollback"):
            assert f"Transaction.{method}()" in message
    finally:
        db.close()


def test_transaction_sql_control_words_in_data_comments_and_explain_are_allowed():
    db = Database.new()
    try:
        db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
        txn = db.begin(TxnMode.READ_WRITE)
        txn.execute_sql("/* SAVEPOINT retry */ INSERT INTO items VALUES (1, 'SAVEPOINT retry; COMMIT')")
        txn.execute_sql("INSERT INTO items VALUES (?, ?)", [2, "ROLLBACK; BEGIN"])
        assert txn.execute_sql("EXPLAIN SELECT * FROM items")
        txn.commit()
        assert db.execute_sql("SELECT value FROM items ORDER BY id") == [
            {"value": "SAVEPOINT retry; COMMIT"}, {"value": "ROLLBACK; BEGIN"},
        ]
    finally:
        db.close()
