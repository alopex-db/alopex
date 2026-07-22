import asyncio

import pytest

from alopex import AlopexError, LocalScan, TxnMode
from alopex.asyncio import AsyncDatabase, AsyncSqlResultStream, AsyncTransaction


def _error_code(error: BaseException) -> str:
    return getattr(error, "code", "")


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
            assert batch.to_dict() == {"value": [1, 2]}
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
