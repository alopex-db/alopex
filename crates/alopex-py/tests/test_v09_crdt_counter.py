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


def _create_set(db: Database, operation_id: str = "operation-set-a") -> dict:
    return db.create_set(
        "set-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-set-a",
        operation_id=operation_id,
        update_version=12,
    )


def test_i27_python_sync_set_create_preserves_canonical_empty_membership_and_idempotency() -> None:
    db = Database.open_in_memory()
    first = _create_set(db)
    duplicate = _create_set(db)

    assert first["object_type"] == "set"
    assert first["object_id"] == "set-a"
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"] == {
        "value_type": "set",
        "members": [],
        "member_versions": {},
        "accepted_operation_versions": {"operation-set-a": 12},
    }
    assert first["idempotency"]["duplicate_count"] == 0
    assert duplicate["value"] == first["value"]
    assert duplicate["idempotency"]["duplicate_count"] == 1


def test_i27_python_async_set_create_preserves_canonical_empty_membership_and_idempotency() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            kwargs = dict(
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-a",
                operation_id="operation-set-async-a",
                update_version=12,
            )
            first = await db.create_set("set-async-a", **kwargs)
            duplicate = await db.create_set("set-async-a", **kwargs)
            assert first["object_type"] == "set"
            assert first["state"] == "committed"
            assert first["routing"]["kind"] == "local_only"
            assert first["value"] == {
                "value_type": "set",
                "members": [],
                "member_versions": {},
                "accepted_operation_versions": {"operation-set-async-a": 12},
            }
            assert first["idempotency"]["duplicate_count"] == 0
            assert duplicate["idempotency"]["duplicate_count"] == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_i27_python_async_set_read_preserves_canonical_membership_without_mutation() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            await db.create_set(
                "set-async-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-a",
                operation_id="operation-set-async-a",
                update_version=12,
            )
            outcome = await db.read_set(
                "set-async-a",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-read",
                operation_id="operation-set-async-read",
                update_version=12,
            )
            assert outcome["object_type"] == "set"
            assert outcome["object_id"] == "set-async-a"
            assert outcome["state"] == "committed"
            assert outcome["routing"]["kind"] == "local_only"
            assert outcome["value"] == {
                "value_type": "set",
                "members": [],
                "member_versions": {},
                "accepted_operation_versions": {"operation-set-async-a": 12},
            }
            assert outcome["idempotency"]["first_outcome"] == "set_read"
            assert outcome["idempotency"]["duplicate_count"] == 0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_i27_python_async_set_add_preserves_canonical_membership_and_idempotency() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            await db.create_set(
                "set-async-add",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-create",
                operation_id="operation-set-async-create",
                update_version=12,
            )
            kwargs = dict(
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-add",
                operation_id="00000000-0000-0000-0000-000000000161",
                update_version=13,
                member="alice",
            )
            first = await db.add_set("set-async-add", **kwargs)
            duplicate = await db.add_set("set-async-add", **kwargs)
            assert first["object_type"] == "set"
            assert first["object_id"] == "set-async-add"
            assert first["state"] == "committed"
            assert first["routing"]["kind"] == "local_only"
            assert first["value"] == {
                "value_type": "set",
                "members": ["alice"],
                "member_versions": {
                    "alice": {
                        "update_version": 13,
                        "operation_id": "00000000-0000-0000-0000-000000000161",
                        "present": True,
                    }
                },
                "accepted_operation_versions": {
                    "operation-set-async-create": 12,
                    "00000000-0000-0000-0000-000000000161": 13,
                },
            }
            assert first["idempotency"]["duplicate_count"] == 0
            assert duplicate["value"] == first["value"]
            assert duplicate["idempotency"]["duplicate_count"] == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_i27_python_async_set_remove_preserves_canonical_membership_and_idempotency() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            await db.create_set(
                "set-async-remove",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-remove-create",
                operation_id="operation-set-async-remove-create",
                update_version=12,
            )
            await db.add_set(
                "set-async-remove",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-remove-add",
                operation_id="00000000-0000-0000-0000-000000000161",
                update_version=13,
                member="alice",
            )
            kwargs = dict(
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-remove",
                operation_id="00000000-0000-0000-0000-000000000170",
                update_version=14,
                member="alice",
            )
            first = await db.remove_set("set-async-remove", **kwargs)
            duplicate = await db.remove_set("set-async-remove", **kwargs)
            assert first["object_type"] == "set"
            assert first["object_id"] == "set-async-remove"
            assert first["state"] == "committed"
            assert first["routing"]["kind"] == "local_only"
            assert first["value"] == {
                "value_type": "set",
                "members": [],
                "member_versions": {
                    "alice": {
                        "update_version": 14,
                        "operation_id": "00000000-0000-0000-0000-000000000170",
                        "present": False,
                    }
                },
                "accepted_operation_versions": {
                    "operation-set-async-remove-create": 12,
                    "00000000-0000-0000-0000-000000000161": 13,
                    "00000000-0000-0000-0000-000000000170": 14,
                },
            }
            assert first["idempotency"]["duplicate_count"] == 0
            assert duplicate["value"] == first["value"]
            assert duplicate["idempotency"]["duplicate_count"] == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_i27_python_async_set_contains_preserves_canonical_membership_without_a_ledger_mutation() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            await db.create_set(
                "set-async-contains",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-contains-create",
                operation_id="operation-set-async-contains-create",
                update_version=12,
            )
            await db.add_set(
                "set-async-contains",
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-contains-add",
                operation_id="00000000-0000-0000-0000-000000000162",
                update_version=13,
                member="alice",
            )
            kwargs = dict(
                cluster_id="cluster-a",
                table_id=7,
                range_id="range-a",
                schema_version=1,
                data_epoch=9,
                request_id="request-set-async-contains",
                operation_id="operation-set-async-contains",
                update_version=14,
                member="alice",
            )
            first = await db.contains_set("set-async-contains", **kwargs)
            repeated = await db.contains_set("set-async-contains", **kwargs)
            assert first["object_type"] == "set"
            assert first["object_id"] == "set-async-contains"
            assert first["range"] == {
                "cluster_id": "cluster-a",
                "table_id": 7,
                "range_id": "range-a",
                "lower_bound": None,
                "upper_bound": None,
                "schema_version": 1,
                "data_epoch": 9,
            }
            assert first["request_id"] == "request-set-async-contains"
            assert first["operation_id"] == "operation-set-async-contains"
            assert first["state"] == "committed"
            assert first["routing"]["kind"] == "local_only"
            assert first["value"] == {
                "value_type": "set",
                "members": ["alice"],
                "member_versions": {
                    "alice": {
                        "update_version": 13,
                        "operation_id": "00000000-0000-0000-0000-000000000162",
                        "present": True,
                    }
                },
                "accepted_operation_versions": {
                    "operation-set-async-contains-create": 12,
                    "00000000-0000-0000-0000-000000000162": 13,
                },
            }
            assert first["idempotency"] == {
                "first_outcome": "set_contains",
                "duplicate_count": 0,
                "request_id": "request-set-async-contains",
                "operation_id": "operation-set-async-contains",
                "state": "committed",
            }
            assert repeated == first
        finally:
            await db.close()

    asyncio.run(scenario())


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


def _read_set(db: Database, operation_id: str = "operation-set-read") -> dict:
    return db.read_set(
        "set-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-set-read",
        operation_id=operation_id,
        update_version=12,
    )


def _add_set(
    db: Database,
    operation_id: str = "00000000-0000-0000-0000-000000000160",
) -> dict:
    return db.add_set(
        "set-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-set-add",
        operation_id=operation_id,
        update_version=13,
        member="alice",
    )


def _remove_set(
    db: Database,
    operation_id: str = "00000000-0000-0000-0000-000000000169",
) -> dict:
    return db.remove_set(
        "set-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-set-remove",
        operation_id=operation_id,
        update_version=14,
        member="alice",
    )


def _contains_set(
    db: Database,
    operation_id: str = "operation-set-contains",
) -> dict:
    return db.contains_set(
        "set-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-set-contains",
        operation_id=operation_id,
        update_version=14,
        member="alice",
    )


def _list_set(db: Database, operation_id: str = "operation-set-list") -> dict:
    return db.list_set(
        "set-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-set-list",
        operation_id=operation_id,
        update_version=0,
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


def _decrement_counter(db: Database, operation_id: str = "operation-decrement") -> dict:
    return db.decrement_counter(
        "counter-a",
        cluster_id="cluster-a",
        table_id=7,
        range_id="range-a",
        schema_version=1,
        data_epoch=9,
        request_id="request-decrement",
        operation_id=operation_id,
        update_version=14,
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


def test_i27_python_sync_set_read_preserves_canonical_membership_without_mutation() -> None:
    db = Database.open_in_memory()
    _create_set(db)

    outcome = _read_set(db)

    assert outcome["object_type"] == "set"
    assert outcome["object_id"] == "set-a"
    assert outcome["range"]["cluster_id"] == "cluster-a"
    assert outcome["range"]["table_id"] == 7
    assert outcome["range"]["range_id"] == "range-a"
    assert outcome["request_id"] == "request-set-read"
    assert outcome["operation_id"] == "operation-set-read"
    assert outcome["state"] == "committed"
    assert outcome["routing"]["kind"] == "local_only"
    assert outcome["value"] == {
        "value_type": "set",
        "members": [],
        "member_versions": {},
        "accepted_operation_versions": {"operation-set-a": 12},
    }
    assert outcome["idempotency"]["first_outcome"] == "set_read"
    assert outcome["idempotency"]["duplicate_count"] == 0


def test_i27_python_sync_set_add_preserves_canonical_membership_and_idempotency() -> None:
    db = Database.open_in_memory()
    _create_set(db)

    first = _add_set(db)
    duplicate = _add_set(db)

    assert first["object_type"] == "set"
    assert first["object_id"] == "set-a"
    assert first["range"]["cluster_id"] == "cluster-a"
    assert first["range"]["table_id"] == 7
    assert first["range"]["range_id"] == "range-a"
    assert first["request_id"] == "request-set-add"
    assert first["operation_id"] == "00000000-0000-0000-0000-000000000160"
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"] == {
        "value_type": "set",
        "members": ["alice"],
        "member_versions": {
            "alice": {
                "update_version": 13,
                "operation_id": "00000000-0000-0000-0000-000000000160",
                "present": True,
            }
        },
        "accepted_operation_versions": {
            "operation-set-a": 12,
            "00000000-0000-0000-0000-000000000160": 13,
        },
    }
    assert first["idempotency"]["duplicate_count"] == 0
    assert duplicate["value"] == first["value"]
    assert duplicate["idempotency"]["duplicate_count"] == 1


def test_i27_python_sync_set_remove_preserves_canonical_membership_and_idempotency() -> None:
    db = Database.open_in_memory()
    _create_set(db)
    _add_set(db)

    first = _remove_set(db)
    duplicate = _remove_set(db)

    assert first["object_type"] == "set"
    assert first["object_id"] == "set-a"
    assert first["range"]["cluster_id"] == "cluster-a"
    assert first["range"]["table_id"] == 7
    assert first["range"]["range_id"] == "range-a"
    assert first["request_id"] == "request-set-remove"
    assert first["operation_id"] == "00000000-0000-0000-0000-000000000169"
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"] == {
        "value_type": "set",
        "members": [],
        "member_versions": {
            "alice": {
                "update_version": 14,
                "operation_id": "00000000-0000-0000-0000-000000000169",
                "present": False,
            }
        },
        "accepted_operation_versions": {
            "operation-set-a": 12,
            "00000000-0000-0000-0000-000000000160": 13,
            "00000000-0000-0000-0000-000000000169": 14,
        },
    }
    assert first["idempotency"]["duplicate_count"] == 0
    assert duplicate["value"] == first["value"]
    assert duplicate["idempotency"]["duplicate_count"] == 1


def test_i27_python_sync_set_contains_preserves_canonical_membership_without_a_ledger_mutation() -> None:
    db = Database.open_in_memory()
    _create_set(db)
    _add_set(db)

    first = _contains_set(db)
    repeated = _contains_set(db)

    assert first["object_type"] == "set"
    assert first["object_id"] == "set-a"
    assert first["range"] == {
        "cluster_id": "cluster-a",
        "table_id": 7,
        "range_id": "range-a",
        "lower_bound": None,
        "upper_bound": None,
        "schema_version": 1,
        "data_epoch": 9,
    }
    assert first["request_id"] == "request-set-contains"
    assert first["operation_id"] == "operation-set-contains"
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"] == {
        "value_type": "set",
        "members": ["alice"],
        "member_versions": {
            "alice": {
                "update_version": 13,
                "operation_id": "00000000-0000-0000-0000-000000000160",
                "present": True,
            }
        },
        "accepted_operation_versions": {
            "operation-set-a": 12,
            "00000000-0000-0000-0000-000000000160": 13,
        },
    }
    assert first["idempotency"] == {
        "first_outcome": "set_contains",
        "duplicate_count": 0,
        "request_id": "request-set-contains",
        "operation_id": "operation-set-contains",
        "state": "committed",
    }
    assert repeated == first


def test_i27_python_sync_set_list_preserves_canonical_membership_without_a_ledger_mutation() -> None:
    db = Database.open_in_memory()
    _create_set(db)
    _add_set(db)

    first = _list_set(db)
    repeated = _list_set(db)

    assert first["object_type"] == "set"
    assert first["object_id"] == "set-a"
    assert first["range"] == {
        "cluster_id": "cluster-a",
        "table_id": 7,
        "range_id": "range-a",
        "lower_bound": None,
        "upper_bound": None,
        "schema_version": 1,
        "data_epoch": 9,
    }
    assert first["request_id"] == "request-set-list"
    assert first["operation_id"] == "operation-set-list"
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"] == {
        "value_type": "set",
        "members": ["alice"],
        "member_versions": {
            "alice": {
                "update_version": 13,
                "operation_id": "00000000-0000-0000-0000-000000000160",
                "present": True,
            }
        },
        "accepted_operation_versions": {
            "operation-set-a": 12,
            "00000000-0000-0000-0000-000000000160": 13,
        },
    }
    assert first["idempotency"] == {
        "first_outcome": "set_list",
        "duplicate_count": 0,
        "request_id": "request-set-list",
        "operation_id": "operation-set-list",
        "state": "committed",
    }
    assert repeated == first


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


def test_i27_python_sync_counter_decrement_preserves_canonical_outcome_and_idempotency() -> None:
    db = Database.open_in_memory()
    _create_counter(db)

    first = _decrement_counter(db)
    duplicate = _decrement_counter(db)

    assert first["object_type"] == "counter"
    assert first["object_id"] == "counter-a"
    assert first["range"]["cluster_id"] == "cluster-a"
    assert first["range"]["table_id"] == 7
    assert first["range"]["range_id"] == "range-a"
    assert first["request_id"] == "request-decrement"
    assert first["operation_id"] == "operation-decrement"
    assert first["state"] == "committed"
    assert first["routing"]["kind"] == "local_only"
    assert first["value"] == {
        "value_type": "counter",
        "value": -7,
        "initial_value": -4,
        "accepted_delta_total": -3,
        "accepted_operation_versions": {
            "operation-a": 12,
            "operation-decrement": 14,
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


def test_i27_python_async_counter_decrement_preserves_canonical_outcome_and_idempotency() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            await db.create_counter(
                "counter-a", cluster_id="cluster-a", table_id=7, range_id="range-a",
                schema_version=1, data_epoch=9, request_id="request-create",
                operation_id="operation-create", update_version=12, initial_value=-4,
            )
            first = await db.decrement_counter(
                "counter-a", cluster_id="cluster-a", table_id=7, range_id="range-a",
                schema_version=1, data_epoch=9, request_id="request-decrement",
                operation_id="operation-decrement", update_version=14, delta=3,
            )
            duplicate = await db.decrement_counter(
                "counter-a", cluster_id="cluster-a", table_id=7, range_id="range-a",
                schema_version=1, data_epoch=9, request_id="request-decrement",
                operation_id="operation-decrement", update_version=14, delta=3,
            )
            assert first["state"] == "committed"
            assert first["routing"]["kind"] == "local_only"
            assert first["value"]["value"] == -7
            assert first["value"]["accepted_delta_total"] == -3
            assert first["idempotency"]["duplicate_count"] == 0
            assert duplicate["value"] == first["value"]
            assert duplicate["idempotency"]["duplicate_count"] == 1
        finally:
            await db.close()

    asyncio.run(scenario())
