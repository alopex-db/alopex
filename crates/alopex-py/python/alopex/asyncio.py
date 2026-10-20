"""Local asyncio facades for Alopex's owned synchronous Python API.

The module deliberately wraps only ``alopex.Database`` and its local stream
objects.  It does not create a Client, endpoint, remote session, or a caller
owned Rust/Tokio runtime.  Pyo3 local handles stay on the caller thread;
multi-thread stream source advancement is performed only by the native bridge
worker, while single-thread handles retain their documented owner-thread guard.
"""

from __future__ import annotations

import asyncio as _asyncio
from typing import Any, AsyncIterator, Iterator, Optional, Sequence, Tuple, Union

from . import AlopexError, Database, LocalScan, ThreadMode, TxnMode


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
                    return row.deliver_python()
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

    async def scan_prefix(
        self, prefix: bytes, *, limit: Optional[int] = None, page_size: int = 100,
        cursor: Optional[bytes] = None, scan_budget: int = 10_000,
        max_bytes: int = 16_777_216, max_total_bytes: Optional[int] = 104_857_600,
        include_internal: bool = False, strict_budget: bool = False,
    ) -> Iterator[Tuple[bytes, bytes]]:
        """Collect a snapshot, yielding between pages; fail atomically on total bytes."""
        if max_total_bytes is not None and max_total_bytes < 1:
            raise ValueError("max_total_bytes must be positive or None")
        entries = []
        total_bytes = 0
        async for key, value in self.iter_prefix(
            prefix, limit=limit, page_size=page_size, cursor=cursor,
            scan_budget=scan_budget, max_bytes=max_bytes,
            include_internal=include_internal, strict_budget=strict_budget,
        ):
            total_bytes += len(key) + len(value)
            if max_total_bytes is not None and total_bytes > max_total_bytes:
                raise _stream_error("ALOPEX-PY999", f"search response size exceeded: limit={max_total_bytes}, requested={total_bytes}")
            entries.append((key, value))
        return iter(entries)

    async def scan_prefix_page(
        self, prefix: bytes, *, limit: int = 100, cursor: Optional[bytes] = None,
        scan_budget: int = 10_000, max_bytes: int = 16_777_216,
        include_internal: bool = False, strict_budget: bool = False,
    ) -> dict[str, Any]:
        """Fetch one bounded page on the handle's owner thread."""
        await _asyncio.sleep(0)
        return self._handle.scan_prefix_page(
            prefix, limit=limit, cursor=cursor, scan_budget=scan_budget,
            max_bytes=max_bytes, include_internal=include_internal,
            strict_budget=strict_budget,
        )

    async def iter_prefix(
        self, prefix: bytes, *, page_size: int = 100, limit: Optional[int] = None,
        cursor: Optional[bytes] = None, scan_budget: int = 10_000,
        max_bytes: int = 16_777_216, include_internal: bool = False,
        strict_budget: bool = False,
    ) -> AsyncIterator[Tuple[bytes, bytes]]:
        """Read all pages, yielding event-loop control between bounded calls."""
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive or None")
        returned = 0
        while True:
            page_limit = page_size if limit is None else min(page_size, limit - returned)
            page = await self.scan_prefix_page(
                prefix, limit=page_limit, cursor=cursor, scan_budget=scan_budget,
                max_bytes=max_bytes, include_internal=include_internal,
                strict_budget=strict_budget,
            )
            for entry in page["entries"]:
                yield entry
                returned += 1
                if limit is not None and returned == limit:
                    return
            if not page["has_more"]:
                return
            cursor = page["next_cursor"]

    async def scan_range(
        self, start: bytes, end: bytes, *, limit: Optional[int] = None, page_size: int = 100,
        cursor: Optional[bytes] = None, scan_budget: int = 10_000,
        max_bytes: int = 16_777_216, max_total_bytes: Optional[int] = 104_857_600,
        include_internal: bool = False, strict_budget: bool = False,
    ) -> Iterator[Tuple[bytes, bytes]]:
        """Collect a snapshot, yielding between pages; fail atomically on total bytes."""
        if max_total_bytes is not None and max_total_bytes < 1:
            raise ValueError("max_total_bytes must be positive or None")
        entries = []
        total_bytes = 0
        async for key, value in self.iter_range(
            start, end, limit=limit, page_size=page_size, cursor=cursor,
            scan_budget=scan_budget, max_bytes=max_bytes,
            include_internal=include_internal, strict_budget=strict_budget,
        ):
            total_bytes += len(key) + len(value)
            if max_total_bytes is not None and total_bytes > max_total_bytes:
                raise _stream_error("ALOPEX-PY999", f"search response size exceeded: limit={max_total_bytes}, requested={total_bytes}")
            entries.append((key, value))
        return iter(entries)

    async def scan_range_page(
        self, start: bytes, end: bytes, *, limit: int = 100, cursor: Optional[bytes] = None,
        scan_budget: int = 10_000, max_bytes: int = 16_777_216,
        include_internal: bool = False, strict_budget: bool = False,
    ) -> dict[str, Any]:
        """Fetch one bounded page on the handle's owner thread."""
        await _asyncio.sleep(0)
        return self._handle.scan_range_page(
            start, end, limit=limit, cursor=cursor, scan_budget=scan_budget,
            max_bytes=max_bytes, include_internal=include_internal,
            strict_budget=strict_budget,
        )

    async def iter_range(
        self, start: bytes, end: bytes, *, page_size: int = 100, limit: Optional[int] = None,
        cursor: Optional[bytes] = None, scan_budget: int = 10_000,
        max_bytes: int = 16_777_216, include_internal: bool = False,
        strict_budget: bool = False,
    ) -> AsyncIterator[Tuple[bytes, bytes]]:
        """Read all pages, yielding event-loop control between bounded calls."""
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive or None")
        returned = 0
        while True:
            page_limit = page_size if limit is None else min(page_size, limit - returned)
            page = await self.scan_range_page(
                start, end, limit=page_limit, cursor=cursor, scan_budget=scan_budget,
                max_bytes=max_bytes, include_internal=include_internal,
                strict_budget=strict_budget,
            )
            for entry in page["entries"]:
                yield entry
                returned += 1
                if limit is not None and returned == limit:
                    return
            if not page["has_more"]:
                return
            cursor = page["next_cursor"]

    async def search_keys(
        self,
        pattern: Union[bytes, str],
        mode: str = "glob",
        limit: int = 100,
        cursor: Optional[bytes] = None,
        scan_budget: int = 10_000,
        max_bytes: int = 16_777_216,
        *,
        include_internal: bool = False,
        strict_budget: bool = False,
    ) -> dict[str, Any]:
        """Return a bounded native key-search page with its continuation cursor."""
        return self._handle.search_keys(
            pattern,
            mode=mode,
            limit=limit,
            cursor=cursor,
            scan_budget=scan_budget,
            max_bytes=max_bytes,
            include_internal=include_internal,
            strict_budget=strict_budget,
        )

    async def execute_sql(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Union[list[dict[str, Any]], int, None]:
        return self._handle.execute_sql(sql, params)

    async def savepoint(self, name: str) -> None:
        self._handle.savepoint(name)

    async def rollback_to(self, name: str) -> None:
        self._handle.rollback_to(name)

    async def release(self, name: str) -> None:
        self._handle.release(name)

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
    async def new(cls, *, thread_mode: Optional[str] = None, hnsw_cache_bytes: Optional[int] = None, hnsw_cache_entries: Optional[int] = None) -> "AsyncDatabase":
        database = Database.new(thread_mode=thread_mode, hnsw_cache_bytes=hnsw_cache_bytes, hnsw_cache_entries=hnsw_cache_entries)
        return cls(database, database.thread_mode == ThreadMode.SINGLE)

    @classmethod
    async def open(
        cls, path: str, *, thread_mode: Optional[str] = None, hnsw_cache_bytes: Optional[int] = None, hnsw_cache_entries: Optional[int] = None
    ) -> "AsyncDatabase":
        database = Database.open(path, thread_mode=thread_mode, hnsw_cache_bytes=hnsw_cache_bytes, hnsw_cache_entries=hnsw_cache_entries)
        return cls(database, database.thread_mode == ThreadMode.SINGLE)

    @classmethod
    async def open_in_memory(cls, *, thread_mode: Optional[str] = None, hnsw_cache_bytes: Optional[int] = None, hnsw_cache_entries: Optional[int] = None) -> "AsyncDatabase":
        database = Database.open_in_memory(thread_mode=thread_mode, hnsw_cache_bytes=hnsw_cache_bytes, hnsw_cache_entries=hnsw_cache_entries)
        return cls(database, database.thread_mode == ThreadMode.SINGLE)

    async def set_hnsw_cache_limit(self, bytes: int, *, entries: Optional[int] = None) -> None:
        self._handle.set_hnsw_cache_limit(bytes, entries=entries)

    async def hnsw_cache_stats(self) -> dict[str, Any]:
        return self._handle.hnsw_cache_stats()

    async def warm_hnsw_index(self, name: str) -> int:
        """Finite native load on the owner thread; may block the event loop."""
        return self._handle.warm_hnsw_index(name)

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
