import pytest

from alopex import AlopexError, Database, TxnMode


def _assert_local_outcome(
    outcome: dict[str, object],
    request_id: str,
    state: str,
    operation: str,
    failure_class: str | None = None,
) -> None:
    assert outcome["outcome_version"] == "v0.9"
    assert outcome["transaction_id"].startswith("local-python-txn-")
    assert outcome["request_id"] == request_id
    assert outcome["participating_ranges"] == []
    assert outcome["read_point"] is None
    assert outcome["schema_version"] is None
    assert outcome["data_epoch"] is None
    assert outcome["isolation"] == "snapshot"
    assert outcome["state"] == state
    assert outcome["failure_class"] == failure_class
    assert outcome["routing"]["kind"] == "local_only"
    assert outcome["retryable"] is False
    if operation == "begin":
        assert outcome["idempotency"]["operation_id"] == outcome["transaction_id"]
    else:
        assert outcome["idempotency"]["operation_id"].startswith(
            f"{outcome['transaction_id']}:{operation}:"
        )
    assert outcome["idempotency"]["request_id"] == request_id
    assert outcome["idempotency"]["first_outcome"] == operation
    assert outcome["idempotency"]["state"] == state
    assert outcome["idempotency"]["duplicate_count"] == 0


def test_sync_transaction_status_adds_canonical_local_outcome_and_preserves_lifecycle() -> None:
    db = Database.new()
    try:
        db.execute_sql("CREATE TABLE transaction_status (id INTEGER PRIMARY KEY, value TEXT)")
        txn = db.begin(TxnMode.READ_WRITE, request_id="sync-transaction-1")

        before = txn.status
        assert before["state"] == "active"
        assert before["stream_effect"] == "committable"
        _assert_local_outcome(before["transaction"], "sync-transaction-1", "running", "begin")

        txn.put(b"key", b"value", request_id="sync-put-1")
        _assert_local_outcome(txn.status["transaction"], "sync-put-1", "running", "put")
        assert txn.get(b"key", request_id="sync-get-1") == b"value"
        _assert_local_outcome(txn.status["transaction"], "sync-get-1", "running", "get")
        txn.execute_sql(
            "INSERT INTO transaction_status (id, value) VALUES (1, 'visible')",
            request_id="sync-sql-write-1",
        )
        assert txn.execute_sql("SELECT value FROM transaction_status") == [{"value": "visible"}]
        txn.commit(request_id="sync-commit-1")

        after = txn.status
        assert after["state"] == "committed"
        _assert_local_outcome(after["transaction"], "sync-commit-1", "committed", "commit")

        with pytest.raises(AlopexError) as completed:
            txn.get(b"key", request_id="sync-after-commit-1")
        assert completed.value.code == "ALOPEX-PY999"
        _assert_local_outcome(
            completed.value.status,
            "sync-after-commit-1",
            "rejected",
            "get",
            "invalid_request",
        )
        assert completed.value.failure_class == "invalid_request"

        rollback = db.begin(TxnMode.READ_WRITE, request_id="sync-transaction-rollback")
        rollback.put(b"discard", b"value", request_id="sync-rollback-put-1")
        rollback.rollback(request_id="sync-rollback-1")
        _assert_local_outcome(
            rollback.status["transaction"],
            "sync-rollback-1",
            "cancelled",
            "rollback",
        )
        assert db.begin(TxnMode.READ_ONLY).get(b"discard") is None

        assert not hasattr(txn, "recover")
        assert not hasattr(txn, "cancel")
    finally:
        db.close()


def test_sync_transaction_rejects_empty_request_id_without_opening_a_transaction() -> None:
    db = Database.new()
    try:
        with pytest.raises(AlopexError) as rejected:
            db.begin(request_id="")
        assert rejected.value.code == "ALOPEX-PY999"
    finally:
        db.close()
