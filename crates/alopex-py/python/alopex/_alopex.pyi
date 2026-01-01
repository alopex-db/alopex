from typing import Any, Dict, List, Literal, Optional, Tuple, Union


class AlopexError(Exception):
    ...


class TxnMode:
    READ_ONLY: "TxnMode"
    READ_WRITE: "TxnMode"

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


class Database:
    @staticmethod
    def open(path: str) -> "Database": ...

    @staticmethod
    def new() -> "Database": ...

    @staticmethod
    def open_in_memory() -> "Database": ...

    @staticmethod
    def open_with_config(config: EmbeddedConfig) -> "Database": ...

    def begin(self, mode: Optional[TxnMode] = None) -> "Transaction": ...
    def flush(self) -> None: ...
    def memory_usage(self) -> MemoryStats: ...
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
