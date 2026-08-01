from builtins import str as _str
from typing import Any, Dict, Iterator, List, Literal, Optional, Sequence, Tuple, Union

ALOPEX_ERROR_CODES: Tuple[str, ...]


class polars:
    class DataFrame:
        ...

    class LazyFrame:
        ...


class AlopexError(Exception):
    code: str


class TxnMode:
    READ_ONLY: "TxnMode"
    READ_WRITE: "TxnMode"

    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...


class ThreadMode:
    MULTI: "ThreadMode"
    SINGLE: "ThreadMode"

    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...


class Metric:
    COSINE: "Metric"
    L2: "Metric"
    INNER_PRODUCT: "Metric"

    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...


class StorageMode:
    DISK: "StorageMode"
    IN_MEMORY: "StorageMode"

    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...


class HnswConfig:
    dim: int
    m: int
    ef_construction: int
    metric: Metric

    def __init__(
        self,
        dim: int,
        m: int = 16,
        ef_construction: int = 200,
        metric: Optional[Metric] = None,
    ) -> None: ...


class EmbeddedConfig:
    memory_limit_bytes: Optional[int]

    def __init__(self, memory_limit_bytes: Optional[int] = None) -> None: ...


class DatabaseOptions:
    path: Optional[str]
    storage_mode: StorageMode
    memory_limit_bytes: Optional[int]
    enable_metrics: bool

    def __init__(
        self,
        path: Optional[str] = None,
        storage_mode: Optional[StorageMode] = None,
        memory_limit_bytes: Optional[int] = None,
        enable_metrics: bool = False,
    ) -> None: ...


class StringNamespace:
    def to_lowercase(self, output: Optional[str] = None) -> "DataFrame": ...
    def to_uppercase(self, output: Optional[str] = None) -> "DataFrame": ...
    def contains(self, pattern: str, output: Optional[str] = None) -> "DataFrame": ...
    def replace(
        self,
        pattern: str,
        replacement: str,
        output: Optional[str] = None,
    ) -> "DataFrame": ...
    def strip_chars(
        self,
        chars: Optional[str] = None,
        output: Optional[str] = None,
    ) -> "DataFrame": ...
    def split(self, separator: str, output: Optional[str] = None) -> "DataFrame": ...
    def len_chars(self, output: Optional[str] = None) -> "DataFrame": ...
    def extract(
        self,
        pattern: str,
        capture_group: int = 1,
        output: Optional[str] = None,
    ) -> "DataFrame": ...


class DatetimeNamespace:
    def year(self, output: Optional[str] = None) -> "DataFrame": ...
    def month(self, output: Optional[str] = None) -> "DataFrame": ...
    def day(self, output: Optional[str] = None) -> "DataFrame": ...
    def weekday(self, output: Optional[str] = None) -> "DataFrame": ...
    def to_string(self, output: Optional[str] = None) -> "DataFrame": ...
    def convert_time_zone(
        self,
        from_offset: str,
        to_offset: str,
        output: Optional[str] = None,
    ) -> "DataFrame": ...


class ListNamespace:
    def join(
        self,
        separator: str,
        null_value: Optional[str] = None,
        output: Optional[str] = None,
    ) -> "DataFrame": ...
    def len(self, output: Optional[str] = None) -> "DataFrame": ...
    def contains(self, value: str, output: Optional[str] = None) -> "DataFrame": ...


class DataFrame:
    def __init__(
        self,
        columns: Dict[str, List[Any]],
        schema: Optional[Dict[str, str]] = None,
    ) -> None: ...

    @staticmethod
    def from_columns(
        columns: Dict[str, List[Any]],
        schema: Optional[Dict[str, str]] = None,
    ) -> "DataFrame": ...

    def height(self) -> int: ...
    def width(self) -> int: ...
    def to_dict(self) -> Dict[str, List[Any]]: ...
    def str(self, column: _str) -> StringNamespace: ...
    def dt(self, column: _str) -> DatetimeNamespace: ...
    def list(self, column: _str) -> ListNamespace: ...
    def explode(self, column: _str) -> "DataFrame": ...
    def implode(self) -> "DataFrame": ...
    def lazy(self) -> "LazyFrame": ...


class Expr:
    def alias(self, name: str) -> "Expr": ...
    def add(self, rhs: "Expr") -> "Expr": ...
    def sub(self, rhs: "Expr") -> "Expr": ...
    def mul(self, rhs: "Expr") -> "Expr": ...
    def div(self, rhs: "Expr") -> "Expr": ...
    def eq(self, rhs: "Expr") -> "Expr": ...
    def neq(self, rhs: "Expr") -> "Expr": ...
    def gt(self, rhs: "Expr") -> "Expr": ...
    def lt(self, rhs: "Expr") -> "Expr": ...
    def ge(self, rhs: "Expr") -> "Expr": ...
    def le(self, rhs: "Expr") -> "Expr": ...
    def and_(self, rhs: "Expr") -> "Expr": ...
    def or_(self, rhs: "Expr") -> "Expr": ...
    def not_(self) -> "Expr": ...


def col(name: str) -> Expr: ...
def lit(value: Union[None, bool, int, float, str]) -> Expr: ...
def concat(inputs: List[DataFrame]) -> DataFrame: ...
def concat_str(
    inputs: List[Expr],
    separator: str = "",
    *,
    null_behavior: Literal["propagate", "ignore", "replace"] = "propagate",
    null_value: Optional[str] = None,
) -> Expr: ...


class SearchResult:
    key: bytes
    score: float
    metadata: Optional[bytes]
    vector: Optional[Any]  # numpy.ndarray[float32] when return_vectors=True

    def __init__(
        self,
        key: bytes,
        score: float,
        metadata: Optional[bytes] = None,
        vector: Optional[Any] = None,
    ) -> None: ...


class HnswStats:
    node_count: int
    deleted_count: int
    level_distribution: List[int]
    memory_bytes: int
    avg_edges_per_node: float

    def __init__(
        self,
        node_count: int = 0,
        deleted_count: int = 0,
        level_distribution: List[int] = [],
        memory_bytes: int = 0,
        avg_edges_per_node: float = 0.0,
    ) -> None: ...


class MemoryStats:
    total_bytes: int
    used_bytes: int
    free_bytes: int

    def __init__(self, total_bytes: int, used_bytes: int, free_bytes: int) -> None: ...

TableType = Literal[
    "MANAGED",
    "EXTERNAL",
    "VIEW",
    "MATERIALIZED_VIEW",
    "STREAMING_TABLE",
    "MANAGED_SHALLOW_CLONE",
    "FOREIGN",
    "EXTERNAL_SHALLOW_CLONE",
]

DataSourceFormat = Literal[
    "DELTA",
    "CSV",
    "JSON",
    "AVRO",
    "PARQUET",
    "ORC",
    "TEXT",
    "UNITY_CATALOG",
    "DELTASHARING",
    "DATABRICKS_FORMAT",
    "REDSHIFT_FORMAT",
    "SNOWFLAKE_FORMAT",
    "SQLDW_FORMAT",
    "SALESFORCE_FORMAT",
    "BIGQUERY_FORMAT",
    "NETSUITE_FORMAT",
    "WORKDAY_RAAS_FORMAT",
    "HIVE_SERDE",
    "HIVE_CUSTOM",
    "VECTOR_INDEX_FORMAT",
]

DeltaMode = Literal["error", "ignore", "append", "overwrite", "merge"]
CredentialProvider = Union[Literal["auto"], Dict[str, str]]


class CatalogInfo:
    name: str
    comment: Optional[str]
    storage_root: Optional[str]

    def __init__(
        self,
        name: str,
        comment: Optional[str] = None,
        storage_root: Optional[str] = None,
    ) -> None: ...


class NamespaceInfo:
    name: str
    catalog_name: str
    comment: Optional[str]
    storage_root: Optional[str]

    def __init__(
        self,
        name: str,
        catalog_name: str,
        comment: Optional[str] = None,
        storage_root: Optional[str] = None,
    ) -> None: ...


class ColumnInfo:
    name: str
    type_name: str
    position: int
    nullable: bool
    comment: Optional[str]

    def __init__(
        self,
        name: str,
        type_name: str,
        position: int = 0,
        nullable: bool = True,
        comment: Optional[str] = None,
    ) -> None: ...


class TableInfo:
    name: str
    catalog_name: str
    namespace_name: str
    table_type: TableType
    storage_location: Optional[str]
    data_source_format: Optional[DataSourceFormat]
    columns: List[ColumnInfo]
    primary_key: Optional[List[str]]
    comment: Optional[str]

    def __init__(
        self,
        name: str,
        catalog_name: str,
        namespace_name: str,
        table_type: TableType = "MANAGED",
        storage_location: Optional[str] = None,
        data_source_format: Optional[DataSourceFormat] = None,
        columns: List[ColumnInfo] = [],
        primary_key: Optional[List[str]] = None,
        comment: Optional[str] = None,
    ) -> None: ...


class SqlResultStream:
    @property
    def status(self) -> Dict[str, Any]: ...

    def close(self) -> None: ...
    def cancel(self) -> None: ...
    def __iter__(self) -> "SqlResultStream": ...
    def __next__(self) -> Dict[str, Any]: ...
    def __enter__(self) -> "SqlResultStream": ...
    def __exit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...


class Changefeed(Iterator[Dict[str, Any]]):
    """Synchronous embedded-local changefeed handle.

    All lifecycle results retain the canonical `outcome`/`events` fields. A
    classified failure raises ``AlopexError`` with ``code``, ``status``, and
    ``failure_class`` attributes; this handle never opens a remote client or
    substitutes an in-memory feed for Durable.
    """

    @property
    def status(self) -> Dict[str, Any]: ...

    def subscribe(
        self, expected_generation: int, expected_epoch: int, request_id: str
    ) -> Dict[str, Any]: ...
    def poll(self, max_events: int, request_id: str) -> Dict[str, Any]: ...
    def stream(self, max_events: int, request_id: str) -> Dict[str, Any]: ...
    def ack(self, ack_id: str, checkpoint: str, request_id: str) -> Dict[str, Any]: ...
    def resume(self, checkpoint: str, request_id: str) -> Dict[str, Any]: ...
    def cancel(self, request_id: str) -> Dict[str, Any]: ...
    def close(self, request_id: str) -> Dict[str, Any]: ...
    def __iter__(self) -> "Changefeed": ...
    def __next__(self) -> Dict[str, Any]: ...
    def __enter__(self) -> "Changefeed": ...
    def __exit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...


class DataFrameStream(Iterator[DataFrame]):
    @property
    def status(self) -> Dict[str, Any]: ...

    def close(self) -> None: ...
    def cancel(self) -> None: ...
    def __iter__(self) -> "DataFrameStream": ...
    def __next__(self) -> DataFrame: ...
    def __enter__(self) -> "DataFrameStream": ...
    def __exit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...


class LazyFrame:
    @staticmethod
    def scan_csv(path: str) -> "LazyFrame": ...

    @staticmethod
    def scan_parquet(path: str) -> "LazyFrame": ...

    @staticmethod
    def from_dataframe(dataframe: DataFrame) -> "LazyFrame": ...

    @staticmethod
    def concat(inputs: List["LazyFrame"]) -> "LazyFrame": ...

    def select(self, exprs: List[Expr]) -> "LazyFrame": ...
    def filter(self, predicate: Expr) -> "LazyFrame": ...
    def with_columns(self, exprs: List[Expr]) -> "LazyFrame": ...

    def collect(
        self,
        *,
        streaming: bool = False,
        resource_limit_bytes: Optional[int] = None,
        batch_rows: Optional[int] = None,
    ) -> Union[DataFrame, DataFrameStream]: ...


class LocalScan:
    @staticmethod
    def table(
        name: str,
        projection: Optional[Sequence[str]] = None,
        predicate: None = None,
    ) -> "LocalScan": ...

    @staticmethod
    def csv(path: str, options: None = None) -> "LocalScan": ...

    @staticmethod
    def parquet(path: str, options: None = None) -> "LocalScan": ...

    @staticmethod
    def columnar_segment(segment_id: str) -> "LocalScan": ...

    @staticmethod
    def lazyframe(lazyframe: LazyFrame) -> "LocalScan": ...


class Database:
    @staticmethod
    def open(path: str, *, thread_mode: Optional[Literal["multi", "single"]] = None) -> "Database": ...

    @staticmethod
    def new(*, thread_mode: Optional[Literal["multi", "single"]] = None) -> "Database": ...

    @staticmethod
    def open_in_memory(*, thread_mode: Optional[Literal["multi", "single"]] = None) -> "Database": ...

    @staticmethod
    def open_with_config(
        config: EmbeddedConfig,
        *,
        thread_mode: Optional[Literal["multi", "single"]] = None,
    ) -> "Database": ...

    @property
    def thread_mode(self) -> ThreadMode: ...

    def execute_sql(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> Union[List[Dict[str, Any]], int, None]:
        """Execute SQL with optional ``?`` placeholder parameters (auto-commit).

        Returns:
            ``list[dict[str, Any]]`` for SELECT (column name -> value, in
            column order), ``int`` (rows affected) for DML, ``None`` for DDL.

        Raises:
            ValueError: Placeholder/parameter count mismatch or invalid value.
            TypeError: Unsupported parameter type.
            NotImplementedError: ``bytes`` parameters (BLOB literals are not
                supported by the SQL parser yet).
            AlopexError: SQL parse/execution errors (``code`` carries the
                stable ALOPEX-P/S/C/E### error code).
        """
        ...
    def execute_sql_stream(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> SqlResultStream: ...
    def query_stream(
        self,
        scan: LocalScan,
        *,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> Union[SqlResultStream, DataFrameStream]: ...
    def begin(
        self,
        mode: Optional[TxnMode] = None,
        *,
        request_id: Optional[str] = None,
    ) -> "Transaction": ...
    def flush(self) -> None: ...
    def memory_usage(self) -> MemoryStats: ...
    def cluster_status(self) -> Dict[str, Any]:
        """Return a cluster status snapshot as a dict.

        For the embedded engine this reflects a single local node.

        Raises:
            AlopexError: If the database is closed.
        """
        ...
    def routing_diagnostics(self) -> Dict[str, Any]:
        """Return the latest routing diagnostics produced by this database.

        The result is generated by the embedded engine's routing contract.
        Before a statement is evaluated, the reason is
        ``planning_input_unavailable``. After SQL execution it reflects the
        latest planner input, catalog epoch, and routing decision. Embedded
        execution is currently local-only, while the payload remains
        compatible with future placements and distributed targets.

        Raises:
            AlopexError: If the database is closed.
        """
        ...
    def create_counter(
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
    def create_changefeed(
        self,
        feed_id: str,
        *,
        cluster_id: str,
        table_id: int,
        range_id: str,
        generation: int,
        schema_version: int,
        data_epoch: int,
        request_id: str,
        tenant: str = "default",
        actor: str = "alopex-python-local",
        placement_node_id: str = "alopex-python-local",
        placement_version: int = 0,
        retention_deadline: Optional[int] = None,
        retained_through_position: Optional[int] = None,
    ) -> Changefeed:
        """Create the embedded-local changefeed facade after Durable preflight.

        The current compiled Durable integration is unavailable, so callers
        receive ``AlopexError(code="changefeed_prerequisite_missing")`` with
        the complete canonical status instead of a best-effort local feed.
        """
        ...
    def create_set(
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
    def add_set(
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
        member: str,
        actor: str = "alopex-python-local",
    ) -> Dict[str, Any]: ...
    def remove_set(
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
        member: str,
        actor: str = "alopex-python-local",
    ) -> Dict[str, Any]: ...
    def contains_set(
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
        member: str,
        actor: str = "alopex-python-local",
    ) -> Dict[str, Any]: ...
    def list_set(
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
    def read_set(
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
    def read_counter(
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
    def increment_counter(
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
    def decrement_counter(
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
    def close(self) -> None: ...
    def create_hnsw_index(self, name: str, config: HnswConfig) -> None: ...
    def search_hnsw(
        self,
        name: str,
        query: Any,
        k: int,
        ef_search: Optional[int] = None,
    ) -> Tuple[List[SearchResult], HnswStats]: ...
    def drop_hnsw_index(self, name: str) -> None: ...
    def get_hnsw_stats(self, name: str) -> HnswStats: ...


class Transaction:
    @property
    def status(self) -> Dict[str, Any]: ...

    def execute_sql(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        request_id: Optional[str] = None,
    ) -> Union[List[Dict[str, Any]], int, None]:
        """Execute SQL inside this transaction (no auto-commit).

        Returns:
            ``list[dict[str, Any]]`` for SELECT (column name -> value, in
            column order), ``int`` (rows affected) for DML, ``None`` for DDL.

        Raises:
            ValueError: Placeholder/parameter count mismatch or invalid value.
            TypeError: Unsupported parameter type.
            NotImplementedError: ``bytes`` parameters (BLOB literals are not
                supported by the SQL parser yet).
            AlopexError: SQL parse/execution errors, or the transaction is
                already completed.
        """
        ...
    def execute_sql_stream(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        request_id: Optional[str] = None,
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> SqlResultStream: ...
    def get(self, key: bytes, *, request_id: Optional[str] = None) -> Optional[bytes]: ...
    def put(self, key: bytes, value: bytes, *, request_id: Optional[str] = None) -> None: ...
    def delete(self, key: bytes, *, request_id: Optional[str] = None) -> None: ...
    def upsert_vector(
        self,
        key: bytes,
        metadata: Optional[bytes],
        vector: Any,
        metric: Metric,
    ) -> None: ...
    def search_similar(
        self,
        query: Any,
        metric: Metric,
        k: int,
        filter_keys: Optional[List[bytes]] = None,
        return_vectors: bool = False,
        zero_copy_return: bool = True,
    ) -> List[SearchResult]: ...
    def get_vector(
        self,
        key: bytes,
        metric: Metric,
        zero_copy_return: bool = True,
    ) -> Any:
        """Get vector by key.

        Args:
            key: Vector key.
            metric: Metric (must match the metric used when storing).
            zero_copy_return: If True, uses zero-copy ownership transfer.

        Returns:
            numpy.ndarray[float32]: The vector data.

        Raises:
            KeyError: If the key does not exist.
        """
        ...
    def upsert_to_hnsw(
        self,
        name: str,
        key: bytes,
        vector: Any,
        metadata: Optional[bytes] = None,
    ) -> None: ...
    def delete_from_hnsw(self, name: str, key: bytes) -> None: ...
    def commit(self, *, request_id: Optional[str] = None) -> None: ...
    def rollback(self, *, request_id: Optional[str] = None) -> None: ...
    def __enter__(self) -> "Transaction": ...
    def __exit__(self, exc_type: Optional[Any], exc: Optional[Any], traceback: Optional[Any]) -> bool: ...


class Catalog:
    @staticmethod
    def list_catalogs() -> List[CatalogInfo]: ...

    @staticmethod
    def list_namespaces(catalog_name: str) -> List[NamespaceInfo]: ...

    @staticmethod
    def list_tables(catalog_name: str, namespace: str) -> List[TableInfo]: ...

    @staticmethod
    def get_table_info(
        catalog_name: str,
        namespace: str,
        table_name: str,
    ) -> TableInfo: ...

    @staticmethod
    def create_catalog(name: str) -> None: ...

    @staticmethod
    def delete_catalog(name: str) -> None: ...

    @staticmethod
    def create_namespace(catalog_name: str, namespace: str) -> None: ...

    @staticmethod
    def delete_namespace(catalog_name: str, namespace: str) -> None: ...

    @staticmethod
    def create_table(
        catalog_name: str,
        namespace: str,
        table_name: str,
        columns: List[ColumnInfo],
        storage_location: str,
        data_source_format: DataSourceFormat = "PARQUET",
    ) -> None: ...

    @staticmethod
    def delete_table(catalog_name: str, namespace: str, table_name: str) -> None: ...

    @staticmethod
    def scan_table(
        catalog_name: str,
        namespace: str,
        table_name: str,
        credential_provider: CredentialProvider = "auto",
        storage_options: Optional[Dict[str, str]] = None,
    ) -> "polars.LazyFrame": ...

    @staticmethod
    def write_table(
        df: "polars.DataFrame | polars.LazyFrame",
        catalog_name: str,
        namespace: str,
        table_name: str,
        delta_mode: DeltaMode = "error",
        storage_location: Optional[str] = None,
        credential_provider: CredentialProvider = "auto",
        storage_options: Optional[Dict[str, str]] = None,
        primary_key: Optional[List[str]] = None,
    ) -> None: ...
