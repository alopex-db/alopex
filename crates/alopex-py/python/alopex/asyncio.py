"""Local asyncio facades for Alopex's owned synchronous Python API.

The module deliberately wraps only ``alopex.Database`` and its local stream
objects.  It does not create a Client, endpoint, remote session, or a caller
owned Rust/Tokio runtime.  Pyo3 local handles stay on the caller thread;
multi-thread stream source advancement is performed only by the native bridge
worker, while single-thread handles retain their documented owner-thread guard.
"""

from __future__ import annotations

import asyncio as _asyncio
from typing import Any, Optional, Sequence, Union

from . import AlopexError, Database, LocalScan, ThreadMode, Transaction, TxnMode


def _stream_error(code: str, message: str) -> AlopexError:
    error = AlopexError(message)
    error.code = code
    return error


def _validate_stream_options(
    *,
    prefetch_batches: int,
    max_buffered_batches: int,
    consumer_idle_timeout: Optional[float],
) -> None:
    if max_buffered_batches < 1:
        raise _stream_error(
            "stream_resource_limit", "max_buffered_batches must be at least one"
        )
    if not 0 <= prefetch_batches <= max_buffered_batches:
        raise _stream_error(
            "stream_resource_limit",
            "prefetch_batches must be between zero and max_buffered_batches",
        )
    if consumer_idle_timeout is not None and (
        not isinstance(consumer_idle_timeout, (int, float))
        or not float(consumer_idle_timeout) >= 0.0
        or not float(consumer_idle_timeout) < float("inf")
    ):
        raise _stream_error(
            "stream_timeout",
            "consumer_idle_timeout must be a finite non-negative number of seconds",
        )


class _AsyncLocalHandle:
    def __init__(self, handle: Any, single_thread: bool) -> None:
        self._handle = handle
        self._single_thread = single_thread


class AsyncSqlResultStream(_AsyncLocalHandle):
    """Async iteration over a bounded, native owned-stream bridge.

    The wrapped private native object owns the producer thread, bounded
    ``sync_channel``, cancellation token, and terminal transition.  This
    facade polls a native bounded handoff on the caller's event loop; it never
    creates a Python queue, calls a Pyo3 object from a Python worker thread, or
    stores prefetched Python rows.
    """

    def __init__(self, native_stream: Any, single_thread: bool) -> None:
        super().__init__(native_stream, single_thread)
        self._next_lock = _asyncio.Lock()

    def __aiter__(self) -> "AsyncSqlResultStream":
        return self

    def _terminal_before_next(self) -> None:
        """Return a terminal outcome without resubmitting a completed native bridge."""
        terminal = self.status.get("terminal")
        if terminal in (None, "open"):
            return
        if terminal == "exhausted":
            raise StopAsyncIteration
        code = {
            "closed": "stream_closed",
            "cancelled": "stream_cancelled",
            "timed_out": "stream_timeout",
            "resource_limit": "stream_resource_limit",
        }.get(terminal, "stream_failure")
        raise _stream_error(code, f"stream is terminal: {terminal}")

    async def __anext__(self) -> Any:
        if self._next_lock.locked():
            raise _stream_error("stream_busy", "only one __anext__ call may be in flight")
        await self._next_lock.acquire()
        try:
            try:
                while True:
                    self._terminal_before_next()
                    try:
                        row = self._handle.poll_next_raw()
                    except StopIteration:
                        raise StopAsyncIteration from None
                    except AlopexError as error:
                        if getattr(error, "code", "") != "stream_pending":
                            raise
                        await _asyncio.sleep(0.001)
                        continue
                    return row.into_python()
            except _asyncio.CancelledError:
                # This is a short native terminal transition, not a blocking row receive.  Keep
                # it on the event-loop thread so the bridge can terminally release its source.
                self._handle.cancel()
                raise
        finally:
            self._next_lock.release()

    async def aclose(self) -> None:
        self._handle.close()

    async def cancel(self) -> None:
        self._handle.cancel()

    @property
    def status(self) -> Any:
        return self._handle.status

    async def __aenter__(self) -> "AsyncSqlResultStream":
        return self

    async def __aexit__(
        self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]
    ) -> bool:
        await self.aclose()
        return False


class AsyncScanResultStream(AsyncSqlResultStream):
    """Async adapter for either a local SQL scan row stream or DataFrame batch stream."""


class AsyncTransaction(_AsyncLocalHandle):
    async def get(self, key: bytes) -> Optional[bytes]:
        return self._handle.get(key)

    async def put(self, key: bytes, value: bytes) -> None:
        self._handle.put(key, value)

    async def delete(self, key: bytes) -> None:
        self._handle.delete(key)

    async def execute_sql(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Union[list[dict[str, Any]], int, None]:
        return self._handle.execute_sql(sql, params)

    async def execute_sql_stream(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
        prefetch_batches: int = 1,
        max_buffered_batches: int = 1,
        consumer_idle_timeout: Optional[float] = None,
    ) -> AsyncSqlResultStream:
        _validate_stream_options(
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        stream = self._handle._open_native_async_sql_stream(
            sql,
            params,
            resource_limit_bytes=resource_limit_bytes,
            timeout=timeout,
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        return AsyncSqlResultStream(stream, self._single_thread)

    async def query_stream(
        self,
        scan: LocalScan,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
        prefetch_batches: int = 1,
        max_buffered_batches: int = 1,
        consumer_idle_timeout: Optional[float] = None,
    ) -> "AsyncScanResultStream":
        _validate_stream_options(
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        stream = self._handle._open_native_async_query_stream(
            scan,
            resource_limit_bytes=resource_limit_bytes,
            timeout=timeout,
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        return AsyncScanResultStream(stream, self._single_thread)

    async def commit(self) -> None:
        self._handle.commit()

    async def rollback(self) -> None:
        self._handle.rollback()

    @property
    def status(self) -> Any:
        return self._handle.status

    async def __aenter__(self) -> "AsyncTransaction":
        return self

    async def __aexit__(
        self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]
    ) -> bool:
        if self._handle.status["state"] == "active":
            await self.rollback()
        return False


class AsyncDatabase(_AsyncLocalHandle):
    """Awaitable embedded-local Database facade using the caller's asyncio loop."""

    @classmethod
    async def new(cls, *, thread_mode: Optional[str] = None) -> "AsyncDatabase":
        database = Database.new(thread_mode=thread_mode)
        return cls(database, database.thread_mode == ThreadMode.SINGLE)

    @classmethod
    async def open(
        cls, path: str, *, thread_mode: Optional[str] = None
    ) -> "AsyncDatabase":
        database = Database.open(path, thread_mode=thread_mode)
        return cls(database, database.thread_mode == ThreadMode.SINGLE)

    @classmethod
    async def open_in_memory(cls, *, thread_mode: Optional[str] = None) -> "AsyncDatabase":
        database = Database.open_in_memory(thread_mode=thread_mode)
        return cls(database, database.thread_mode == ThreadMode.SINGLE)

    async def execute_sql(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Union[list[dict[str, Any]], int, None]:
        return self._handle.execute_sql(sql, params)

    async def begin(self, mode: Optional[TxnMode] = None) -> AsyncTransaction:
        transaction = self._handle.begin(mode)
        return AsyncTransaction(transaction, self._single_thread)

    async def execute_sql_stream(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
        prefetch_batches: int = 1,
        max_buffered_batches: int = 1,
        consumer_idle_timeout: Optional[float] = None,
    ) -> AsyncSqlResultStream:
        _validate_stream_options(
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        stream = self._handle._open_native_async_sql_stream(
            sql,
            params,
            resource_limit_bytes=resource_limit_bytes,
            timeout=timeout,
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        return AsyncSqlResultStream(stream, self._single_thread)

    async def query_stream(
        self,
        scan: LocalScan,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
        prefetch_batches: int = 1,
        max_buffered_batches: int = 1,
        consumer_idle_timeout: Optional[float] = None,
    ) -> AsyncScanResultStream:
        _validate_stream_options(
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        stream = self._handle._open_native_async_query_stream(
            scan,
            resource_limit_bytes=resource_limit_bytes,
            timeout=timeout,
            prefetch_batches=prefetch_batches,
            max_buffered_batches=max_buffered_batches,
            consumer_idle_timeout=consumer_idle_timeout,
        )
        return AsyncScanResultStream(stream, self._single_thread)

    async def close(self) -> None:
        # `Database.close` owns the final native resource transition.  It must run on the caller
        # thread so a just-cancelled native stream and its database registry cannot wait on
        # separate executor completions while they release the same ownership graph.
        self._handle.close()

    @property
    def thread_mode(self) -> ThreadMode:
        return self._handle.thread_mode

    async def __aenter__(self) -> "AsyncDatabase":
        return self

    async def __aexit__(
        self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]
    ) -> bool:
        await self.close()
        return False
