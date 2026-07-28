import asyncio

import pytest

from alopex import AlopexError, Database
from alopex.asyncio import AsyncDatabase


def _create_counter(db: Database, operation_id: str = "operation-a") -> dict:
    return db.create_counter(
        "counter-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-a",
        operation_id=operation_id,
        update_version=12,
        initial_value=-4,
    )


def _read_counter(db: Database, operation_id: str = "operation-read") -> dict:
    return db.read_counter(
        "counter-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-read",
        operation_id=operation_id,
        update_version=12,
    )


def _increment_counter(db: Database, operation_id: str = "operation-increment") -> dict:
    return db.increment_counter(
        "counter-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-increment",
        operation_id=operation_id,
        update_version=13,
        delta=3,
    )


def test_i27_python_sync_counter_create_preserves_canonical_outcome_and_idempotency() -> None:
    db = Database.open_in_memory()

    first = _create_counter(db)
    duplicate = _create_counter(db)

    assert first["object_type"] == "counter"
    assert first["object_id"] == "counter-a"
    assert first["range"] == {
        "cluster_id": "cluster-a",
        "table_id": 7,
        "range_id": "range-a",
        "lower_bound": None,
        "upper_bound": None,
        "schema_version": 1,
        "data_epoch": 9,
    }
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"]["value_type"] == "counter"
    assert first["value"]["value"] == -4
    assert first["idempotency"]["duplicate_count"] == 0
    assert duplicate["value"] == first["value"]
    assert duplicate["idempotency"]["duplicate_count"] == 1


def test_i27_python_sync_counter_conflict_exposes_code_status_and_failure_class() -> None:
    db = Database.open_in_memory()
    _create_counter(db)

    with pytest.raises(AlopexError) as raised:
        _create_counter(db, operation_id="operation-b")

    assert raised.value.code == "crdt_conflict"
    assert raised.value.failure_class == "conflict"
    assert raised.value.status["object_id"] == "counter-a"
    assert raised.value.status["state"] == "rejected"


def test_i27_python_sync_counter_read_preserves_canonical_value_without_mutation() -> None:
    db = Database.open_in_memory()
    _create_counter(db)

    outcome = _read_counter(db)

    assert outcome["object_type"] == "counter"
    assert outcome["object_id"] == "counter-a"
    assert outcome["range"]["cluster_id"] == "cluster-a"
    assert outcome["range"]["table_id"] == 7
    assert outcome["range"]["range_id"] == "range-a"
    assert outcome["request_id"] == "request-read"
    assert outcome["operation_id"] == "operation-read"
    assert outcome["state"] == "committed"
    assert outcome["routing"]["kind"] == "local_only"
    assert outcome["value"] == {
        "value_type": "counter",
        "value": -4,
        "initial_value": -4,
        "accepted_delta_total": 0,
        "accepted_operation_versions": {"operation-a": 12},
    }
    assert outcome["idempotency"]["first_outcome"] == "counter_read"
    assert outcome["idempotency"]["duplicate_count"] == 0


def test_i27_python_sync_counter_increment_preserves_canonical_outcome_and_idempotency() -> None:
    db = Database.open_in_memory()
    _create_counter(db)

    first = _increment_counter(db)
    duplicate = _increment_counter(db)

    assert first["object_type"] == "counter"
    assert first["object_id"] == "counter-a"
    assert first["range"]["cluster_id"] == "cluster-a"
    assert first["range"]["table_id"] == 7
    assert first["range"]["range_id"] == "range-a"
    assert first["request_id"] == "request-increment"
    assert first["operation_id"] == "operation-increment"
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"] == {
        "value_type": "counter",
        "value": -1,
        "initial_value": -4,
        "accepted_delta_total": 3,
        "accepted_operation_versions": {
            "operation-a": 12,
            "operation-increment": 13,
        },
    }
    assert first["idempotency"]["first_outcome"] == "counter_committed"
    assert first["idempotency"]["duplicate_count"] == 0
    assert duplicate["value"] == first["value"]
    assert duplicate["idempotency"]["duplicate_count"] == 1


def test_i27_python_async_counter_create_preserves_canonical_outcome_and_error_mapping() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            first = await db.create_counter(
                "counter-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-a",
                operation_id="operation-a",
                update_version=12,
                initial_value=-4,
            )
            duplicate = await db.create_counter(
                "counter-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-a",
                operation_id="operation-a",
                update_version=12,
                initial_value=-4,
            )

            assert first["object_type"] == "counter"
            assert first["range"]["cluster_id"] == "cluster-a"
            assert first["value"] == {
                "value_type": "counter",
                "value": -4,
                "initial_value": -4,
                "accepted_delta_total": 0,
                "accepted_operation_versions": {"operation-a": 12},
            }
            assert first["state"] == "committed"
            assert first["routing"]["kind"] == "local_only"
            assert duplicate["idempotency"]["duplicate_count"] == 1

            read = await db.read_counter(
                "counter-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-read",
                operation_id="operation-read",
                update_version=12,
            )
            assert read["object_type"] == "counter"
            assert read["object_id"] == "counter-a"
            assert read["request_id"] == "request-read"
            assert read["operation_id"] == "operation-read"
            assert read["state"] == "committed"
            assert read["routing"]["kind"] == "local_only"
            assert read["value"] == first["value"]
            assert read["idempotency"]["first_outcome"] == "counter_read"
            assert read["idempotency"]["duplicate_count"] == 0

            with pytest.raises(AlopexError) as raised:
                await db.create_counter(
                    "counter-a",
                    cluster_id="cluster-a",
                    table_id=7,
                    range_id="range-a",
                    schema_version=1,
                    data_epoch=9,
                    request_id="request-a",
                    operation_id="operation-b",
                    update_version=12,
                    initial_value=-4,
                )
            assert raised.value.code == "crdt_conflict"
            assert raised.value.failure_class == "conflict"
            assert raised.value.status["state"] == "rejected"
        finally:
            await db.close()

    asyncio.run(scenario())


def test_i27_python_async_counter_increment_preserves_canonical_outcome_and_idempotency() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            await db.create_counter(
                "counter-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-create",
                operation_id="operation-create",
                update_version=12,
                initial_value=-4,
            )
            first = await db.increment_counter(
                "counter-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-increment",
                operation_id="operation-increment",
                update_version=13,
                delta=3,
            )
            duplicate = await db.increment_counter(
                "counter-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-increment",
                operation_id="operation-increment",
                update_version=13,
                delta=3,
            )

            assert first["object_type"] == "counter"
            assert first["object_id"] == "counter-a"
            assert first["state"] == "committed"
            assert first["routing"]["kind"] == "local_only"
            assert first["value"] == {
                "value_type": "counter",
                "value": -1,
                "initial_value": -4,
                "accepted_delta_total": 3,
                "accepted_operation_versions": {
                    "operation-create": 12,
                    "operation-increment": 13,
                },
            }
            assert first["idempotency"]["first_outcome"] == "counter_committed"
            assert first["idempotency"]["duplicate_count"] == 0
            assert duplicate["value"] == first["value"]
            assert duplicate["idempotency"]["duplicate_count"] == 1
        finally:
            await db.close()

    asyncio.run(scenario())
