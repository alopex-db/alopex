import asyncio

import pytest

from alopex import AlopexError, LocalScan, TxnMode
from alopex.asyncio import AsyncDatabase, AsyncSqlResultStream, AsyncTransaction


def _error_code(error: BaseException) -> str:
    return getattr(error, "code", "")


@pytest.mark.parametrize("thread_mode", ["single", "multi"])
def test_async_kv_scan_bounds_and_internal_visibility(thread_mode):
    async def scenario():
        db = await AsyncDatabase.new(thread_mode=thread_mode)
        try:
            txn = await db.begin(TxnMode.READ_WRITE)
            await txn.put(b"__catalog__/hidden", b"raw")
            await txn.put(b"user:1", b"one")
            await txn.put(b"user:2", b"two")
            assert list(await txn.scan_prefix(b"", limit=1)) == [(b"user:1", b"one")]
            assert list(await txn.scan_range(b"user:", b"user;", limit=1, cursor=b"user:1")) == [(b"user:2", b"two")]
            assert (await txn.search_keys(b"*", limit=1))["entries"] == [(b"user:1", b"one")]
            assert dict(await txn.scan_prefix(b"__catalog__/", include_internal=True))[b"__catalog__/hidden"] == b"raw"
            assert dict((await txn.search_keys(b"__catalog__/*", include_internal=True))["entries"])[b"__catalog__/hidden"] == b"raw"
            for method, args in [(txn.scan_prefix, (b"user:",)), (txn.scan_range, (b"user:", b"user;"))]:
                with pytest.raises(AlopexError):
                    await method(*args, max_bytes=1)
                with pytest.raises(AlopexError):
                    await method(*args, scan_budget=1)
            await txn.rollback()
        finally:
            await db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("thread_mode", ["single", "multi"])
def test_async_kv_scans_preserve_transaction_visibility(thread_mode):
    async def scenario():
        db = await AsyncDatabase.new(thread_mode=thread_mode)
        try:
            seed = await db.begin(TxnMode.READ_WRITE)
            await seed.put(b"cart:1", b"old")
            await seed.put(b"cart:2", b"deleted")
            await seed.commit()
            txn = await db.begin(TxnMode.READ_WRITE)
            await txn.put(b"cart:1", b"changed")
            await txn.delete(b"cart:2")
            await txn.put(b"cart:3", b"new")
            expected = [(b"cart:1", b"changed"), (b"cart:3", b"new")]
            prefix = await txn.scan_prefix(b"cart:")
            bounded = await txn.scan_range(b"cart:", b"cart;")
            assert iter(prefix) is prefix
            assert iter(bounded) is bounded
            assert list(await txn.scan_range(b"cart:1", b"cart:3")) == expected[:1]
            assert list(await txn.scan_prefix(b"missing:")) == []
            await txn.put(b"cart:1", b"later")
            await txn.commit()
            assert list(prefix) == expected
            assert list(bounded) == expected
        finally:
            await db.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("thread_mode", ["single", "multi"])
def test_async_kv_search_preserves_pagination_and_limits(thread_mode):
    async def scenario():
        db = await AsyncDatabase.new(thread_mode=thread_mode)
        try:
            txn = await db.begin(TxnMode.READ_WRITE)
            for suffix in (b"1", b"2", b"3"):
                await txn.put(b"cart:" + suffix, suffix)
            first = await txn.search_keys(b"cart:*", limit=2)
            assert first == {
                "entries": [(b"cart:1", b"1"), (b"cart:2", b"2")],
                "next_cursor": b"cart:2",
                "scanned": 2,
            }
            second = await txn.search_keys(b"cart:*", limit=2, cursor=first["next_cursor"])
            assert second["entries"] == [(b"cart:3", b"3")]
            assert second["next_cursor"] is None
            assert (await txn.search_keys(r"^cart:[13]$", mode="regex"))["entries"] == [
                (b"cart:1", b"1"), (b"cart:3", b"3")
            ]
            assert await txn.search_keys(b"cart:*", cursor=b"cart:3") == {
                "entries": [], "next_cursor": None, "scanned": 0,
            }
            with pytest.raises(AlopexError, match="budget"):
                await txn.search_keys(b"cart:*", scan_budget=1)
            with pytest.raises(AlopexError, match="response size"):
                await txn.search_keys(b"cart:*", max_bytes=1)
            assert len((await txn.search_keys(b"cart:*", max_bytes=100))["entries"]) == 3
            for option in ("limit", "scan_budget", "max_bytes"):
                with pytest.raises(AlopexError, match=option):
                    await txn.search_keys(b"cart:*", **{option: 0})
            with pytest.raises(ValueError, match="mode"):
                await txn.search_keys(b"cart:*", mode="literal")
            await txn.rollback()
        finally:
            await db.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("method,args", [
    ("scan_prefix", (b"cart:",)),
    ("scan_range", (b"cart:", b"cart;")),
    ("search_keys", (b"cart:*",)),
])
@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_async_kv_methods_reject_completed_transaction(method, args, finish):
    async def scenario():
        db = await AsyncDatabase.new()
        try:
            txn = await db.begin(TxnMode.READ_WRITE)
            await getattr(txn, finish)()
            with pytest.raises(AlopexError, match="closed"):
                await getattr(txn, method)(*args)
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_sql_stream_consumes_owned_local_rows_and_single_thread_stays_local():
    async def scenario() -> None:
        db = await AsyncDatabase.new(thread_mode="single")
        try:
            await db.execute_sql("CREATE TABLE async_users (id INTEGER PRIMARY KEY, name TEXT)")
            await db.execute_sql(
                "INSERT INTO async_users (id, name) VALUES (1, 'one'), (2, 'two')"
            )
            stream = await db.execute_sql_stream(
                "SELECT id, name FROM async_users", prefetch_batches=1, max_buffered_batches=1
            )
            assert [row async for row in stream] == [
                {"id": 1, "name": "one"},
                {"id": 2, "name": "two"},
            ]
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_stream_cancel_and_repeated_terminal_are_classified():
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            stream = await db.execute_sql_stream("SELECT 1 AS value")
            assert isinstance(stream, AsyncSqlResultStream)
            await stream.cancel()
            with pytest.raises(AlopexError) as terminal:
                await stream.__anext__()
            assert _error_code(terminal.value) == "stream_cancelled"
            with pytest.raises(AlopexError) as repeated:
                await stream.__anext__()
            assert _error_code(repeated.value) == "stream_cancelled"
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_stream_validates_bounded_buffer_options_before_opening_native_work():
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            with pytest.raises(AlopexError) as invalid:
                await db.execute_sql_stream("SELECT 1 AS value", max_buffered_batches=0)
            assert _error_code(invalid.value) == "stream_resource_limit"
            assert await db.execute_sql("SELECT 1 AS value") == [{"value": 1}]
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_stream_native_prefetch_idle_timeout_discards_ready_rows():
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            stream = await db.execute_sql_stream(
                "SELECT 3 AS value",
                prefetch_batches=1,
                max_buffered_batches=1,
                consumer_idle_timeout=0.001,
            )
            await asyncio.sleep(0.02)
            with pytest.raises(AlopexError) as timed_out:
                await stream.__anext__()
            assert _error_code(timed_out.value) == "stream_timeout"
            assert stream.status["terminal"] == "timed_out"
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_query_stream_uses_native_bridge_for_phase_three_csv_batches(tmp_path):
    path = tmp_path / "async-stream.csv"
    path.write_text("value\n1\n2\n", encoding="utf-8")

    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            stream = await db.query_stream(
                LocalScan.csv(str(path)),
                prefetch_batches=1,
                max_buffered_batches=1,
            )
            batch = await stream.__anext__()
            assert batch.to_dict(as_series=False) == {"value": [1, 2]}
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_query_stream_uses_native_bridge_for_table_rows():
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            await db.execute_sql("CREATE TABLE async_scan_rows (id INTEGER PRIMARY KEY)")
            await db.execute_sql("INSERT INTO async_scan_rows (id) VALUES (7)")
            stream = await db.query_stream(LocalScan.table("async_scan_rows"))
            assert await stream.__anext__() == {"id": 7}
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_transaction_uses_native_sql_stream_and_preserves_commitability():
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            await db.execute_sql("CREATE TABLE async_txn (id INTEGER PRIMARY KEY)")
            transaction = await db.begin(TxnMode.READ_WRITE)
            assert isinstance(transaction, AsyncTransaction)
            await transaction.execute_sql("INSERT INTO async_txn (id) VALUES (4)")
            stream = await transaction.query_stream(LocalScan.table("async_txn"))
            assert await stream.__anext__() == {"id": 4}
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()
            assert transaction.status["stream_effect"] == "committable"
            await transaction.commit()
        finally:
            await db.close()

    asyncio.run(scenario())


def test_async_transaction_savepoint_rolls_back_sql_work():
    async def scenario() -> None:
        db = await AsyncDatabase.new()
        try:
            await db.execute_sql("CREATE TABLE async_savepoint (id INTEGER PRIMARY KEY)")
            transaction = await db.begin(TxnMode.READ_WRITE)
            await transaction.execute_sql("INSERT INTO async_savepoint (id) VALUES (1)")
            await transaction.savepoint("optional_item")
            await transaction.execute_sql("INSERT INTO async_savepoint (id) VALUES (2)")
            await transaction.rollback_to("optional_item")
            await transaction.release("optional_item")
            await transaction.commit()
            assert await db.execute_sql("SELECT id FROM async_savepoint") == [{"id": 1}]
        finally:
            await db.close()

    asyncio.run(scenario())
