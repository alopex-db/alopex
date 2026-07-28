from typing import Any, AsyncIterator, Dict, Literal, Optional, Sequence, Union

from . import DataFrame, LocalScan, ThreadMode, TxnMode


class AsyncSqlResultStream(AsyncIterator[Dict[str, Any]]):
    @property
    def status(self) -> Dict[str, Any]: ...
    async def __anext__(self) -> Dict[str, Any]: ...
    async def aclose(self) -> None: ...
    async def cancel(self) -> None: ...
    async def __aenter__(self) -> "AsyncSqlResultStream": ...
    async def __aexit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...


class AsyncScanResultStream(AsyncIterator[Union[Dict[str, Any], DataFrame]]):
    @property
    def status(self) -> Any: ...
    async def __anext__(self) -> Union[Dict[str, Any], DataFrame]: ...
    async def aclose(self) -> None: ...
    async def cancel(self) -> None: ...
    async def __aenter__(self) -> "AsyncScanResultStream": ...
    async def __aexit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...


class AsyncTransaction:
    @property
    def status(self) -> Dict[str, str]: ...
    async def get(self, key: bytes) -> Optional[bytes]: ...
    async def put(self, key: bytes, value: bytes) -> None: ...
    async def delete(self, key: bytes) -> None: ...
    async def execute_sql(self, sql: str, params: Optional[Sequence[Any]] = None) -> Union[list[dict[str, Any]], int, None]: ...
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
    ) -> AsyncSqlResultStream: ...
    async def query_stream(
        self,
        scan: LocalScan,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
        prefetch_batches: int = 1,
        max_buffered_batches: int = 1,
        consumer_idle_timeout: Optional[float] = None,
    ) -> AsyncScanResultStream: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def __aenter__(self) -> "AsyncTransaction": ...
    async def __aexit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...


class AsyncDatabase:
    @classmethod
    async def new(cls, *, thread_mode: Optional[Literal["multi", "single"]] = None) -> "AsyncDatabase": ...
    @classmethod
    async def open(cls, path: str, *, thread_mode: Optional[Literal["multi", "single"]] = None) -> "AsyncDatabase": ...
    @classmethod
    async def open_in_memory(cls, *, thread_mode: Optional[Literal["multi", "single"]] = None) -> "AsyncDatabase": ...
    @property
    def thread_mode(self) -> ThreadMode: ...
    async def execute_sql(self, sql: str, params: Optional[Sequence[Any]] = None) -> Union[list[dict[str, Any]], int, None]: ...
    async def create_counter(
        self,
        object_id: str,
        *,
        cluster_id: str,
        table_id: int,
        range_id: str,
        schema_version: int,
        data_epoch: int,
        request_id: str,
        operation_id: str,
        update_version: int,
        initial_value: int,
        actor: str = "alopex-python-local",
    ) -> Dict[str, Any]: ...
    async def read_counter(
        self,
        object_id: str,
        *,
        cluster_id: str,
        table_id: int,
        range_id: str,
        schema_version: int,
        data_epoch: int,
        request_id: str,
        operation_id: str,
        update_version: int,
        actor: str = "alopex-python-local",
    ) -> Dict[str, Any]: ...
    async def increment_counter(
        self,
        object_id: str,
        *,
        cluster_id: str,
        table_id: int,
        range_id: str,
        schema_version: int,
        data_epoch: int,
        request_id: str,
        operation_id: str,
        update_version: int,
        delta: int,
        actor: str = "alopex-python-local",
    ) -> Dict[str, Any]: ...
    async def decrement_counter(
        self,
        object_id: str,
        *,
        cluster_id: str,
        table_id: int,
        range_id: str,
        schema_version: int,
        data_epoch: int,
        request_id: str,
        operation_id: str,
        update_version: int,
        delta: int,
        actor: str = "alopex-python-local",
    ) -> Dict[str, Any]: ...
    async def begin(self, mode: Optional[TxnMode] = None) -> AsyncTransaction: ...
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
    ) -> AsyncSqlResultStream: ...
    async def query_stream(
        self,
        scan: LocalScan,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
        prefetch_batches: int = 1,
        max_buffered_batches: int = 1,
        consumer_idle_timeout: Optional[float] = None,
    ) -> AsyncScanResultStream: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> "AsyncDatabase": ...
    async def __aexit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...
