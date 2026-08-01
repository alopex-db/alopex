import asyncio
from typing import Dict, Optional

import pytest

from alopex import AlopexError, Database, TxnMode
from alopex.asyncio import AsyncDatabase, AsyncTransaction


def _assert_local_outcome(
    outcome: Dict[str, object],
    request_id: str,
    state: str,
    operation: str,
    failure_class: Optional[str] = None,
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
    assert outcome["idempotency"]["request_id"] == request_id
    assert outcome["idempotency"]["first_outcome"] == operation
    assert outcome["idempotency"]["state"] == state
    assert outcome["idempotency"]["duplicate_count"] == 0


def test_async_transaction_forwards_request_ids_and_preserves_local_outcomes() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            await db.execute_sql("CREATE TABLE async_transaction_status (id INTEGER PRIMARY KEY)")
            transaction = await db.begin(
                TxnMode.READ_WRITE, request_id="async-transaction-begin-1"
            )
            assert isinstance(transaction, AsyncTransaction)
            _assert_local_outcome(
                transaction.status["transaction"],
                "async-transaction-begin-1",
                "running",
                "begin",
            )

            await transaction.put(
                b"key", b"value", request_id="async-transaction-put-1"
            )
            assert (
                await transaction.get(b"key", request_id="async-transaction-get-1")
                == b"value"
            )
            _assert_local_outcome(
                transaction.status["transaction"],
                "async-transaction-get-1",
                "running",
                "get",
            )
            await transaction.execute_sql(
                "INSERT INTO async_transaction_status (id) VALUES (1)",
                request_id="async-transaction-sql-1",
            )
            assert await transaction.execute_sql("SELECT id FROM async_transaction_status") == [
                {"id": 1}
            ]
            await transaction.commit(request_id="async-transaction-commit-1")
            _assert_local_outcome(
                transaction.status["transaction"],
                "async-transaction-commit-1",
                "committed",
                "commit",
            )

            with pytest.raises(AlopexError) as completed:
                await transaction.get(
                    b"key", request_id="async-transaction-after-commit-1"
                )
            assert completed.value.code == "ALOPEX-PY999"
            _assert_local_outcome(
                completed.value.status,
                "async-transaction-after-commit-1",
                "rejected",
                "get",
                "invalid_request",
            )
            assert completed.value.failure_class == "invalid_request"

            rollback = await db.begin(
                TxnMode.READ_WRITE, request_id="async-transaction-rollback-begin-1"
            )
            await rollback.put(
                b"discard", b"value", request_id="async-transaction-rollback-put-1"
            )
            await rollback.rollback(request_id="async-transaction-rollback-1")
            _assert_local_outcome(
                rollback.status["transaction"],
                "async-transaction-rollback-1",
                "cancelled",
                "rollback",
            )
            reader = await db.begin(TxnMode.READ_ONLY)
            try:
                assert await reader.get(b"discard") is None
            finally:
                await reader.rollback()

            assert not hasattr(transaction, "recover")
            assert not hasattr(transaction, "cancel")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_transaction_stream_request_id_and_cancel_boundary() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            transaction = await db.begin(TxnMode.READ_WRITE)
            stream = await transaction.execute_sql_stream(
                "SELECT 1 AS value", request_id="async-transaction-stream-1"
            )
            assert await stream.__anext__() == {"value": 1}
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            _assert_local_outcome(
                transaction.status["transaction"],
                "async-transaction-stream-1",
                "running",
                "execute_sql_stream",
            )
            await transaction.commit(request_id="async-transaction-stream-commit-1")

            cancelled = await db.begin(TxnMode.READ_WRITE)
            stream = await cancelled.execute_sql_stream(
                "SELECT 2 AS value", request_id="async-transaction-stream-cancel-1"
            )
            await stream.cancel()
            with pytest.raises(AlopexError) as rejected:
                await cancelled.commit(request_id="async-transaction-cancelled-commit-1")
            _assert_local_outcome(
                rejected.value.status,
                "async-transaction-cancelled-commit-1",
                "rejected",
                "commit",
                "invalid_request",
            )
            await cancelled.rollback(request_id="async-transaction-cancelled-rollback-1")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_sync_and_async_transaction_status_share_the_local_outcome_contract() -> None:
    def comparable(outcome: Dict[str, object]) -> Dict[str, object]:
        return {
            "outcome_version": outcome["outcome_version"],
            "request_id": outcome["request_id"],
            "participating_ranges": outcome["participating_ranges"],
            "read_point": outcome["read_point"],
            "schema_version": outcome["schema_version"],
            "data_epoch": outcome["data_epoch"],
            "isolation": outcome["isolation"],
            "state": outcome["state"],
            "failure_class": outcome["failure_class"],
            "routing": outcome["routing"],
            "retryable": outcome["retryable"],
            "idempotency": {
                "request_id": outcome["idempotency"]["request_id"],
                "first_outcome": outcome["idempotency"]["first_outcome"],
                "state": outcome["idempotency"]["state"],
                "duplicate_count": outcome["idempotency"]["duplicate_count"],
            },
        }

    sync_db = Database.new()
    try:
        sync_transaction = sync_db.begin(TxnMode.READ_WRITE)
        sync_transaction.put(b"parity", b"value", request_id="parity-put-1")
        sync_outcome = sync_transaction.status["transaction"]
        sync_transaction.rollback()
    finally:
        sync_db.close()

    async def scenario() -> Dict[str, object]:
        async_db = await AsyncDatabase.new()
        try:
            async_transaction = await async_db.begin(TxnMode.READ_WRITE)
            await async_transaction.put(
                b"parity", b"value", request_id="parity-put-1"
            )
            outcome = async_transaction.status["transaction"]
            await async_transaction.rollback()
            return outcome
        finally:
            await async_db.close()

    assert comparable(asyncio.run(scenario())) == comparable(sync_outcome)
