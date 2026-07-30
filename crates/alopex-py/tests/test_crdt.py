import asyncio

import pytest

from alopex import AlopexError, Database
from alopex.asyncio import AsyncDatabase


COMMON = {
    "cluster_id": "cluster-f2-python",
    "table_id": 7,
    "range_id": "range-f2-python",
    "schema_version": 1,
    "data_epoch": 9,
}


def identity(name: str, update_version: int, **extra: object) -> dict[str, object]:
    return {
        **COMMON,
        "request_id": f"f2-{name}-request",
        "operation_id": f"f2-{name}-operation",
        "update_version": update_version,
        **extra,
    }


def assert_local(outcome: dict, object_type: str) -> None:
    assert outcome["object_type"] == object_type
    assert outcome["state"] == "committed"
    assert outcome["routing"]["kind"] == "local_only"
    assert outcome["idempotency"]["duplicate_count"] == 0


def test_f2_python_sync_register_covers_all_ten_crdt_operations_and_exception_boundary() -> None:
    db = Database.open_in_memory()
    try:
        counter_create = db.create_counter(
            "f2-counter", initial_value=-4, **identity("counter-create", 0)
        )
        counter_read = db.read_counter("f2-counter", **identity("counter-read", 0))
        counter_increment = db.increment_counter(
            "f2-counter", delta=3, **identity("counter-increment", 1)
        )
        counter_decrement = db.decrement_counter(
            "f2-counter", delta=3, **identity("counter-decrement", 2)
        )
        set_create = db.create_set("f2-set", **identity("set-create", 0))
        set_read = db.read_set("f2-set", **identity("set-read", 0))
        set_add = db.add_set(
            "f2-set",
            member="alice",
            **identity("set-add", 1, operation_id="00000000-0000-0000-0000-000000000905"),
        )
        set_contains = db.contains_set(
            "f2-set", member="alice", **identity("set-contains", 0)
        )
        set_list = db.list_set("f2-set", **identity("set-list", 0))
        set_remove = db.remove_set(
            "f2-set",
            member="alice",
            **identity("set-remove", 2, operation_id="00000000-0000-0000-0000-000000000906"),
        )

        for outcome in (counter_create, counter_read, counter_increment, counter_decrement):
            assert_local(outcome, "counter")
        for outcome in (set_create, set_read, set_add, set_contains, set_list, set_remove):
            assert_local(outcome, "set")
        assert counter_create["value"]["value"] == -4
        assert counter_increment["value"]["value"] == -1
        assert counter_decrement["value"]["value"] == -4
        assert set_add["value"]["members"] == ["alice"]
        assert set_contains["value"]["members"] == ["alice"]
        assert set_list["value"]["members"] == ["alice"]
        assert set_remove["value"]["members"] == []

        with pytest.raises(AlopexError) as raised:
            db.create_counter(
                "f2-counter", initial_value=-4, **identity("counter-conflict", 0)
            )
        assert raised.value.code == "crdt_conflict"
    finally:
        db.close()


def test_f2_python_async_register_covers_all_ten_crdt_operations_and_close_boundary() -> None:
    async def scenario() -> None:
        db = await AsyncDatabase.open_in_memory()
        try:
            counter_create = await db.create_counter(
                "f2-counter", initial_value=-4, **identity("async-counter-create", 0)
            )
            counter_read = await db.read_counter(
                "f2-counter", **identity("async-counter-read", 0)
            )
            counter_increment = await db.increment_counter(
                "f2-counter", delta=3, **identity("async-counter-increment", 1)
            )
            counter_decrement = await db.decrement_counter(
                "f2-counter", delta=3, **identity("async-counter-decrement", 2)
            )
            set_create = await db.create_set("f2-set", **identity("async-set-create", 0))
            set_read = await db.read_set("f2-set", **identity("async-set-read", 0))
            set_add = await db.add_set(
                "f2-set",
                member="alice",
                **identity(
                    "async-set-add",
                    1,
                    operation_id="00000000-0000-0000-0000-000000000907",
                ),
            )
            set_contains = await db.contains_set(
                "f2-set", member="alice", **identity("async-set-contains", 0)
            )
            set_list = await db.list_set("f2-set", **identity("async-set-list", 0))
            set_remove = await db.remove_set(
                "f2-set",
                member="alice",
                **identity(
                    "async-set-remove",
                    2,
                    operation_id="00000000-0000-0000-0000-000000000908",
                ),
            )

            for outcome in (
                counter_create,
                counter_read,
                counter_increment,
                counter_decrement,
            ):
                assert_local(outcome, "counter")
            for outcome in (set_create, set_read, set_add, set_contains, set_list, set_remove):
                assert_local(outcome, "set")
            assert counter_increment["value"]["value"] == -1
            assert counter_decrement["value"]["value"] == -4
            assert set_add["value"]["members"] == ["alice"]
            assert set_contains["value"]["members"] == ["alice"]
            assert set_list["value"]["members"] == ["alice"]
            assert set_remove["value"]["members"] == []
        finally:
            await db.close()

    asyncio.run(scenario())
