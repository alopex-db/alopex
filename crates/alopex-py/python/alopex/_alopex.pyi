from builtins import str as _str
from typing import Any, BinaryIO, Dict, Iterable, Iterator, List, Literal, Optional, Sequence, Tuple, Union

ALOPEX_ERROR_CODES: Tuple[str, ...]


def _bind_sql_params(sql: _str, params: Optional[Sequence[Any]] = None) -> _str:
    """Expand ``?`` placeholders into SQL literals (internal).

    Shared by the embedded bindings and ``alopex.remote`` so both surfaces have
    one implementation of the placeholder semantics. Not public API.
    """
    ...


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
        data: Optional[Dict[str, List[Any]]] = None,
        schema: Optional[Dict[str, str]] = None,
        *,
        schema_overrides: Optional[Dict[str, str]] = None,
        strict: bool = True,
        orient: Optional[str] = None,
        infer_schema_length: Optional[int] = 100,
        nan_to_null: bool = False,
        height: Optional[int] = None,
    ) -> None: ...

    @staticmethod
    def from_columns(
        columns: Dict[str, List[Any]],
        schema: Optional[Dict[str, str]] = None,
    ) -> "DataFrame": ...

    @property
    def height(self) -> int: ...
    @property
    def width(self) -> int: ...
    def to_dict(self, *, as_series: bool = True) -> Dict[str, Union["Series", List[Any]]]: ...
    def str(self, column: _str) -> StringNamespace: ...
    def dt(self, column: _str) -> DatetimeNamespace: ...
    def list(self, column: _str) -> ListNamespace: ...
    def explode(
        self,
        columns: Union[_str, Sequence[_str]],
        *more_columns: _str,
        empty_as_null: bool = ...,
        keep_nulls: bool = True,
    ) -> "DataFrame": ...
    def implode(self) -> "DataFrame": ...
    def lazy(self) -> "LazyFrame": ...


class Series:
    @property
    def name(self) -> str: ...
    def to_list(self) -> List[Any]: ...


class Expr:
    def alias(self, name: str) -> "Expr": ...
    def add(self, other: Any) -> "Expr": ...
    def sub(self, other: Any) -> "Expr": ...
    def mul(self, other: Any) -> "Expr": ...
    def div(self, other: Any) -> "Expr": ...
    def eq(self, other: Any) -> "Expr": ...
    def neq(self, other: Any) -> "Expr": ...
    def gt(self, other: Any) -> "Expr": ...
    def lt(self, other: Any) -> "Expr": ...
    def ge(self, other: Any) -> "Expr": ...
    def le(self, other: Any) -> "Expr": ...
    def and_(self, *others: Any) -> "Expr": ...
    def or_(self, *others: Any) -> "Expr": ...
    def not_(self) -> "Expr": ...


def col(name: str) -> Expr: ...
def lit(value: Any, dtype: Optional[Any] = None, *, allow_object: bool = False) -> Expr: ...
def concat(
    items: Iterable[DataFrame],
    *,
    how: str = "vertical",
    rechunk: bool = False,
    parallel: bool = True,
    strict: Optional[bool] = None,
) -> DataFrame: ...
def concat_str(
    exprs: Any,
    *more_exprs: Any,
    separator: str = "",
    ignore_nulls: bool = False,
) -> Expr: ...


SharedExecutionStepKind = Literal[
    "transaction_statement",
    "commit_barrier",
    "post_commit_read",
]
ExecutionStepErrorKind = Literal[
    "transaction",
    "commit",
    "post_commit_read",
    "invalid_order",
]


class SharedExecutionStep:
    step_id: str
    kind: SharedExecutionStepKind
    sql: Optional[str]

    def __init__(
        self,
        step_id: str,
        kind: SharedExecutionStepKind,
        sql: Optional[str] = None,
    ) -> None: ...

    @staticmethod
    def transaction_statement(step_id: str, sql: str) -> "SharedExecutionStep": ...

    @staticmethod
    def commit_barrier(step_id: str) -> "SharedExecutionStep": ...

    @staticmethod
    def post_commit_read(step_id: str, sql: str) -> "SharedExecutionStep": ...


class CommitMetadata:
    transaction_id: str


class ExecutionStepError:
    kind: ExecutionStepErrorKind
    message: str


class SharedExecutionStepResult:
    execution_id: str
    transaction_id: str
    step_id: str
    step_index: int
    outcome_kind: Literal["execution", "commit", "error"]
    result: Optional[Any]
    commit_metadata: Optional[CommitMetadata]
    error: Optional[ExecutionStepError]


class SharedExecutionReport:
    execution_id: str
    transaction_id: str
    steps: List[SharedExecutionStepResult]


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


class SearchStats:
    nodes_visited: int
    distance_computations: int
    search_time_us: int

    def __init__(self) -> None: ...


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

    def select(self, *exprs: object, **named_exprs: object) -> "LazyFrame": ...
    def filter(self, *predicates: object, **constraints: object) -> "LazyFrame": ...
    def with_columns(self, *exprs: object, **named_exprs: object) -> "LazyFrame": ...

    def collect(
        self,
        *,
        type_coercion: bool = True,
        predicate_pushdown: bool = True,
        projection_pushdown: bool = True,
        simplify_expression: bool = True,
        slice_pushdown: bool = True,
        comm_subplan_elim: bool = True,
        comm_subexpr_elim: bool = True,
        cluster_with_columns: bool = True,
        collapse_joins: bool = True,
        no_optimization: bool = False,
        engine: str = "auto",
        background: bool = False,
        optimizations: Any = ...,
        **_kwargs: Any,
    ) -> DataFrame: ...
    def collect_batches(
        self,
        *,
        chunk_size: Optional[int] = None,
        maintain_order: bool = True,
        lazy: bool = False,
        engine: str = "auto",
        optimizations: Any = ...,
    ) -> DataFrameStream: ...


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


class PreparedStatement:
    def parameter_count(self) -> int: ...
    def bind(self, index: int, value: Any) -> None: ...
    def reset(self) -> None: ...
    def finalize(self) -> None: ...
    def execute(self) -> Union[List[Dict[str, Any]], int, None]: ...


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
    def execute_shared(
        self,
        execution_id: str,
        transaction_id: str,
        steps: Sequence[SharedExecutionStep],
    ) -> SharedExecutionReport:
        """Execute ordered mutation, commit-barrier, and post-commit-read steps.

        The returned report retains successful earlier outcomes when a later
        step fails. This API does not expose freshness/version tokens.
        """
        ...
    def prepare(self, sql: str) -> PreparedStatement: ...
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
    def begin(self, mode: Optional[TxnMode] = None) -> "Transaction": ...
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
    def close(self) -> None: ...
    def create_hnsw_index(self, name: str, config: HnswConfig) -> None: ...
    def search_hnsw(
        self,
        name: str,
        query: Any,
        k: int,
        ef_search: Optional[int] = None,
    ) -> Tuple[List[SearchResult], SearchStats]: ...
    def drop_hnsw_index(self, name: str) -> None: ...
    def get_hnsw_stats(self, name: str) -> HnswStats: ...
    def copy_from_csv(self, table: str, source: BinaryIO, header: bool = False) -> int: ...
    def copy_to_csv(self, table: str, destination: BinaryIO, header: bool = False) -> int: ...
    def list_sequences(self) -> List[Dict[str, Any]]: ...


class Transaction:
    @property
    def status(self) -> Dict[str, str]: ...

    def execute_sql(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
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
        resource_limit_bytes: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> SqlResultStream: ...
    def get(self, key: bytes) -> Optional[bytes]: ...
    def put(self, key: bytes, value: bytes) -> None: ...
    def delete(self, key: bytes) -> None: ...
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
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
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
