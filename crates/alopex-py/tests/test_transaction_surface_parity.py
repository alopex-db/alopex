"""Phase 4 Python parity against the shared cross-surface outcome register."""

import asyncio
import re
from pathlib import Path

from alopex import Database, TxnMode
from alopex.asyncio import AsyncDatabase


def _common_outcome_fields() -> set[str]:
    source = (
        Path(__file__).resolve().parents[3] / "tests" / "f4_surface_matrix.rs"
    ).read_text(encoding="utf-8")
    block = re.search(
        r"COMMON_OUTCOME_FIELDS: &\[&str\] = &\[(?P<fields>.*?)\];",
        source,
        re.DOTALL,
    )
    assert block is not None
    return set(re.findall(r'"([^"]+)"', block.group("fields")))


def _normalize(outcome: dict) -> dict:
    fields = _common_outcome_fields()
    assert fields <= set(outcome), f"missing outcome fields: {fields - set(outcome)}"
    assert outcome["outcome_version"] == "v0.9"
    assert outcome["isolation"] == "snapshot"
    assert isinstance(outcome["routing"], dict)
    assert isinstance(outcome["idempotency"], dict)
    return {
        key: outcome[key]
        for key in (
            "outcome_version",
            "participating_ranges",
            "read_point",
            "schema_version",
            "data_epoch",
            "isolation",
            "state",
            "failure_class",
            "routing",
            "retryable",
        )
    }


def test_sync_and_async_registered_transaction_rows_share_outcome_shape() -> None:
    sync = Database.new()
    try:
        sync_txn = sync.begin(TxnMode.READ_WRITE, request_id="f4-parity-sync-begin")
        sync_txn.put(b"key", b"value", request_id="f4-parity-sync-put")
        assert sync_txn.get(b"key", request_id="f4-parity-sync-get") == b"value"
        sync_outcome = _normalize(sync_txn.status["transaction"])
        sync_txn.rollback(request_id="f4-parity-sync-rollback")
        assert not hasattr(sync_txn, "recover")
        assert not hasattr(sync_txn, "cancel")
    finally:
        sync.close()

    async def scenario() -> dict:
        database = await AsyncDatabase.new()
        try:
            transaction = await database.begin(
                TxnMode.READ_WRITE, request_id="f4-parity-async-begin"
            )
            await transaction.put(b"key", b"value", request_id="f4-parity-async-put")
            assert (
                await transaction.get(b"key", request_id="f4-parity-async-get")
                == b"value"
            )
            outcome = _normalize(transaction.status["transaction"])
            await transaction.rollback(request_id="f4-parity-async-rollback")
            assert not hasattr(transaction, "recover")
            assert not hasattr(transaction, "cancel")
            return outcome
        finally:
            await database.close()

    async_outcome = asyncio.run(scenario())
    assert async_outcome == sync_outcome
