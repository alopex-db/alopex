//! 永続化対応カタログ実装。
//!
//! 既存の `TableMetadata` / `IndexMetadata` は `Expr` を含むため、そのままシリアライズして
//! 永続化することができない。そこで、本モジュールでは永続化用 DTO を定義し、KV ストアへ
//! bincode で保存する。
//!
//! 注意: 現状は `ColumnMetadata.default`（DEFAULT 式）を永続化しない。復元時は `None` となる。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ast::ddl::{IndexMethod, VectorMetric};
use crate::catalog::{
    Catalog, ColumnMetadata, Compression, IndexMetadata, MemoryCatalog, RowIdMode,
};
use crate::catalog::{StorageOptions, StorageType, TableMetadata};
use crate::planner::PlannerError;
use crate::planner::types::ResolvedType;

/// カタログ用キープレフィックス。
pub const CATALOG_PREFIX: &[u8] = b"__catalog__/";
pub const CATALOGS_PREFIX: &[u8] = b"__catalog__/catalogs/";
pub const NAMESPACES_PREFIX: &[u8] = b"__catalog__/namespaces/";
pub const TABLES_PREFIX: &[u8] = b"__catalog__/tables/";
pub const INDEXES_PREFIX: &[u8] = b"__catalog__/indexes/";
pub const META_KEY: &[u8] = b"__catalog__/meta";

const CATALOG_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("kv error: {0}")]
    Kv(#[from] alopex_core::Error),

    #[error("serialize error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("invalid catalog key: {0}")]
    InvalidKey(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CatalogState {
    version: u32,
    table_id_counter: u32,
    index_id_counter: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedCatalogMeta {
    pub name: String,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedNamespaceMeta {
    pub name: String,
    pub catalog_name: String,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
}

pub type CatalogMeta = PersistedCatalogMeta;
pub type NamespaceMeta = PersistedNamespaceMeta;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableFqn {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexFqn {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub index: String,
}

impl TableFqn {
    pub fn new(catalog: &str, namespace: &str, table: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        }
    }
}

impl IndexFqn {
    pub fn new(catalog: &str, namespace: &str, table: &str, index: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            index: index.to_string(),
        }
    }
}

impl From<&TableMetadata> for TableFqn {
    fn from(value: &TableMetadata) -> Self {
        Self::new(&value.catalog_name, &value.namespace_name, &value.name)
    }
}

impl From<&IndexMetadata> for IndexFqn {
    fn from(value: &IndexMetadata) -> Self {
        Self::new(
            &value.catalog_name,
            &value.namespace_name,
            &value.table,
            &value.name,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableType {
    Managed,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataSourceFormat {
    Alopex,
    Parquet,
    Delta,
}

impl Default for DataSourceFormat {
    fn default() -> Self {
        Self::Alopex
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersistedVectorMetric {
    Cosine,
    L2,
    Inner,
}

impl From<VectorMetric> for PersistedVectorMetric {
    fn from(value: VectorMetric) -> Self {
        match value {
            VectorMetric::Cosine => Self::Cosine,
            VectorMetric::L2 => Self::L2,
            VectorMetric::Inner => Self::Inner,
        }
    }
}

impl From<PersistedVectorMetric> for VectorMetric {
    fn from(value: PersistedVectorMetric) -> Self {
        match value {
            PersistedVectorMetric::Cosine => Self::Cosine,
            PersistedVectorMetric::L2 => Self::L2,
            PersistedVectorMetric::Inner => Self::Inner,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersistedType {
    Integer,
    BigInt,
    Float,
    Double,
    Text,
    Blob,
    Boolean,
    Timestamp,
    Vector {
        dimension: u32,
        metric: PersistedVectorMetric,
    },
    Null,
}

impl From<ResolvedType> for PersistedType {
    fn from(value: ResolvedType) -> Self {
        match value {
            ResolvedType::Integer => Self::Integer,
            ResolvedType::BigInt => Self::BigInt,
            ResolvedType::Float => Self::Float,
            ResolvedType::Double => Self::Double,
            ResolvedType::Text => Self::Text,
            ResolvedType::Blob => Self::Blob,
            ResolvedType::Boolean => Self::Boolean,
            ResolvedType::Timestamp => Self::Timestamp,
            ResolvedType::Vector { dimension, metric } => Self::Vector {
                dimension,
                metric: metric.into(),
            },
            ResolvedType::Null => Self::Null,
        }
    }
}

impl From<PersistedType> for ResolvedType {
    fn from(value: PersistedType) -> Self {
        match value {
            PersistedType::Integer => Self::Integer,
            PersistedType::BigInt => Self::BigInt,
            PersistedType::Float => Self::Float,
            PersistedType::Double => Self::Double,
            PersistedType::Text => Self::Text,
            PersistedType::Blob => Self::Blob,
            PersistedType::Boolean => Self::Boolean,
            PersistedType::Timestamp => Self::Timestamp,
            PersistedType::Vector { dimension, metric } => Self::Vector {
                dimension,
                metric: metric.into(),
            },
            PersistedType::Null => Self::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersistedIndexType {
    BTree,
    Hnsw,
}

impl From<PersistedIndexType> for IndexMethod {
    fn from(value: PersistedIndexType) -> Self {
        match value {
            PersistedIndexType::BTree => IndexMethod::BTree,
            PersistedIndexType::Hnsw => IndexMethod::Hnsw,
        }
    }
}

impl TryFrom<IndexMethod> for PersistedIndexType {
    type Error = ();

    fn try_from(value: IndexMethod) -> Result<Self, Self::Error> {
        match value {
            IndexMethod::BTree => Ok(Self::BTree),
            IndexMethod::Hnsw => Ok(Self::Hnsw),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersistedStorageType {
    Row,
    Columnar,
}

impl From<PersistedStorageType> for StorageType {
    fn from(value: PersistedStorageType) -> Self {
        match value {
            PersistedStorageType::Row => Self::Row,
            PersistedStorageType::Columnar => Self::Columnar,
        }
    }
}

impl From<StorageType> for PersistedStorageType {
    fn from(value: StorageType) -> Self {
        match value {
            StorageType::Row => Self::Row,
            StorageType::Columnar => Self::Columnar,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersistedCompression {
    None,
    Lz4,
    Zstd,
}

impl From<PersistedCompression> for Compression {
    fn from(value: PersistedCompression) -> Self {
        match value {
            PersistedCompression::None => Self::None,
            PersistedCompression::Lz4 => Self::Lz4,
            PersistedCompression::Zstd => Self::Zstd,
        }
    }
}

impl From<Compression> for PersistedCompression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => Self::None,
            Compression::Lz4 => Self::Lz4,
            Compression::Zstd => Self::Zstd,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersistedRowIdMode {
    None,
    Direct,
}

impl From<PersistedRowIdMode> for RowIdMode {
    fn from(value: PersistedRowIdMode) -> Self {
        match value {
            PersistedRowIdMode::None => Self::None,
            PersistedRowIdMode::Direct => Self::Direct,
        }
    }
}

impl From<RowIdMode> for PersistedRowIdMode {
    fn from(value: RowIdMode) -> Self {
        match value {
            RowIdMode::None => Self::None,
            RowIdMode::Direct => Self::Direct,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedStorageOptions {
    pub storage_type: PersistedStorageType,
    pub compression: PersistedCompression,
    pub row_group_size: u32,
    pub row_id_mode: PersistedRowIdMode,
}

impl From<StorageOptions> for PersistedStorageOptions {
    fn from(value: StorageOptions) -> Self {
        Self {
            storage_type: value.storage_type.into(),
            compression: value.compression.into(),
            row_group_size: value.row_group_size,
            row_id_mode: value.row_id_mode.into(),
        }
    }
}

impl From<PersistedStorageOptions> for StorageOptions {
    fn from(value: PersistedStorageOptions) -> Self {
        Self {
            storage_type: value.storage_type.into(),
            compression: value.compression.into(),
            row_group_size: value.row_group_size,
            row_id_mode: value.row_id_mode.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedColumnMeta {
    pub name: String,
    pub data_type: PersistedType,
    pub not_null: bool,
    pub primary_key: bool,
    pub unique: bool,
}

impl From<&ColumnMetadata> for PersistedColumnMeta {
    fn from(value: &ColumnMetadata) -> Self {
        Self {
            name: value.name.clone(),
            data_type: value.data_type.clone().into(),
            not_null: value.not_null,
            primary_key: value.primary_key,
            unique: value.unique,
        }
    }
}

impl From<PersistedColumnMeta> for ColumnMetadata {
    fn from(value: PersistedColumnMeta) -> Self {
        ColumnMetadata::new(value.name, value.data_type.into())
            .with_not_null(value.not_null)
            .with_primary_key(value.primary_key)
            .with_unique(value.unique)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedTableMeta {
    pub table_id: u32,
    pub name: String,
    pub catalog_name: String,
    pub namespace_name: String,
    pub table_type: TableType,
    pub data_source_format: DataSourceFormat,
    pub columns: Vec<PersistedColumnMeta>,
    pub primary_key: Option<Vec<String>>,
    pub storage_options: PersistedStorageOptions,
    pub storage_location: Option<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

impl From<&TableMetadata> for PersistedTableMeta {
    fn from(value: &TableMetadata) -> Self {
        Self {
            table_id: value.table_id,
            name: value.name.clone(),
            catalog_name: value.catalog_name.clone(),
            namespace_name: value.namespace_name.clone(),
            table_type: TableType::Managed,
            data_source_format: DataSourceFormat::default(),
            columns: value
                .columns
                .iter()
                .map(PersistedColumnMeta::from)
                .collect(),
            primary_key: value.primary_key.clone(),
            storage_options: value.storage_options.clone().into(),
            storage_location: None,
            comment: None,
            properties: HashMap::new(),
        }
    }
}

impl From<PersistedTableMeta> for TableMetadata {
    fn from(value: PersistedTableMeta) -> Self {
        let mut table = TableMetadata::new(
            value.name,
            value
                .columns
                .into_iter()
                .map(ColumnMetadata::from)
                .collect(),
        )
        .with_table_id(value.table_id);
        table.primary_key = value.primary_key;
        table.storage_options = value.storage_options.into();
        table.catalog_name = value.catalog_name;
        table.namespace_name = value.namespace_name;
        table
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedIndexMeta {
    pub index_id: u32,
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub column_indices: Vec<usize>,
    pub unique: bool,
    pub method: Option<PersistedIndexType>,
    pub options: Vec<(String, String)>,
    pub catalog_name: String,
    pub namespace_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedIndexMetaV1 {
    index_id: u32,
    name: String,
    table: String,
    columns: Vec<String>,
    column_indices: Vec<usize>,
    unique: bool,
    method: Option<PersistedIndexType>,
    options: Vec<(String, String)>,
}

impl From<&IndexMetadata> for PersistedIndexMeta {
    fn from(value: &IndexMetadata) -> Self {
        Self {
            index_id: value.index_id,
            name: value.name.clone(),
            table: value.table.clone(),
            columns: value.columns.clone(),
            column_indices: value.column_indices.clone(),
            unique: value.unique,
            method: value
                .method
                .and_then(|m| PersistedIndexType::try_from(m).ok()),
            options: value.options.clone(),
            catalog_name: value.catalog_name.clone(),
            namespace_name: value.namespace_name.clone(),
        }
    }
}

impl From<PersistedIndexMeta> for IndexMetadata {
    fn from(value: PersistedIndexMeta) -> Self {
        let mut index = IndexMetadata::new(value.index_id, value.name, value.table, value.columns)
            .with_column_indices(value.column_indices)
            .with_unique(value.unique)
            .with_options(value.options);
        index.catalog_name = value.catalog_name;
        index.namespace_name = value.namespace_name;
        if let Some(method) = value.method {
            index = index.with_method(method.into());
        }
        index
    }
}

fn deserialize_index_meta(bytes: &[u8]) -> Result<PersistedIndexMeta, CatalogError> {
    match bincode::deserialize::<PersistedIndexMeta>(bytes) {
        Ok(meta) => Ok(meta),
        Err(err) => {
            let is_legacy = matches!(
                err.as_ref(),
                bincode::ErrorKind::Io(io)
                    if io.kind() == std::io::ErrorKind::UnexpectedEof
            );
            if !is_legacy {
                return Err(err.into());
            }
            let legacy: PersistedIndexMetaV1 = bincode::deserialize(bytes)?;
            Ok(PersistedIndexMeta {
                index_id: legacy.index_id,
                name: legacy.name,
                table: legacy.table,
                columns: legacy.columns,
                column_indices: legacy.column_indices,
                unique: legacy.unique,
                method: legacy.method,
                options: legacy.options,
                catalog_name: "default".to_string(),
                namespace_name: "default".to_string(),
            })
        }
    }
}

fn table_key(name: &str) -> Vec<u8> {
    let mut key = TABLES_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn catalog_key(name: &str) -> Vec<u8> {
    let mut key = CATALOGS_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn namespace_key(catalog_name: &str, namespace_name: &str) -> Vec<u8> {
    let mut key = NAMESPACES_PREFIX.to_vec();
    key.extend_from_slice(catalog_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(namespace_name.as_bytes());
    key
}

fn index_key(name: &str) -> Vec<u8> {
    let mut key = INDEXES_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn key_suffix(prefix: &[u8], key: &[u8]) -> Result<String, CatalogError> {
    let suffix = key
        .strip_prefix(prefix)
        .ok_or_else(|| CatalogError::InvalidKey(format!("{key:?}")))?;
    std::str::from_utf8(suffix)
        .map(|s| s.to_string())
        .map_err(|_| CatalogError::InvalidKey(format!("{key:?}")))
}

#[derive(Debug, Clone, Default)]
pub struct CatalogOverlay {
    added_catalogs: HashMap<String, CatalogMeta>,
    dropped_catalogs: HashSet<String>,
    added_namespaces: HashMap<(String, String), NamespaceMeta>,
    dropped_namespaces: HashSet<(String, String)>,
    added_tables: HashMap<TableFqn, TableMetadata>,
    dropped_tables: HashSet<TableFqn>,
    added_indexes: HashMap<IndexFqn, IndexMetadata>,
    dropped_indexes: HashSet<IndexFqn>,
}

impl CatalogOverlay {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_catalog(&mut self, meta: CatalogMeta) {
        self.dropped_catalogs.remove(&meta.name);
        self.added_catalogs.insert(meta.name.clone(), meta);
    }

    pub fn drop_catalog(&mut self, name: &str) {
        self.added_catalogs.remove(name);
        self.dropped_catalogs.insert(name.to_string());
    }

    pub fn add_namespace(&mut self, meta: NamespaceMeta) {
        let key = (meta.catalog_name.clone(), meta.name.clone());
        self.dropped_namespaces.remove(&key);
        self.added_namespaces.insert(key, meta);
    }

    pub fn drop_namespace(&mut self, catalog_name: &str, namespace_name: &str) {
        let key = (catalog_name.to_string(), namespace_name.to_string());
        self.added_namespaces.remove(&key);
        self.dropped_namespaces.insert(key);
    }

    pub fn add_table(&mut self, fqn: TableFqn, table: TableMetadata) {
        self.dropped_tables.remove(&fqn);
        self.added_tables.insert(fqn, table);
    }

    pub fn drop_table(&mut self, fqn: &TableFqn) {
        self.added_tables.remove(fqn);
        self.dropped_tables.insert(fqn.clone());
        self.added_indexes.retain(|key, _| {
            key.catalog != fqn.catalog || key.namespace != fqn.namespace || key.table != fqn.table
        });
    }

    pub fn add_index(&mut self, fqn: IndexFqn, index: IndexMetadata) {
        self.dropped_indexes.remove(&fqn);
        self.added_indexes.insert(fqn, index);
    }

    pub fn drop_index(&mut self, fqn: &IndexFqn) {
        self.added_indexes.remove(fqn);
        self.dropped_indexes.insert(fqn.clone());
    }

    pub fn drop_cascade_catalog(&mut self, catalog: &str) {
        self.drop_catalog(catalog);

        let namespace_keys: Vec<(String, String)> = self
            .added_namespaces
            .keys()
            .filter(|(cat, _)| cat == catalog)
            .cloned()
            .collect();
        for (catalog_name, namespace_name) in namespace_keys {
            self.drop_namespace(&catalog_name, &namespace_name);
        }

        let index_keys: Vec<IndexFqn> = self
            .added_indexes
            .keys()
            .filter(|fqn| fqn.catalog == catalog)
            .cloned()
            .collect();
        for fqn in index_keys {
            self.drop_index(&fqn);
        }

        let table_keys: Vec<TableFqn> = self
            .added_tables
            .keys()
            .filter(|fqn| fqn.catalog == catalog)
            .cloned()
            .collect();
        for fqn in table_keys {
            self.drop_table(&fqn);
        }
    }

    pub fn drop_cascade_namespace(&mut self, catalog: &str, namespace: &str) {
        self.drop_namespace(catalog, namespace);

        let index_keys: Vec<IndexFqn> = self
            .added_indexes
            .keys()
            .filter(|fqn| fqn.catalog == catalog && fqn.namespace == namespace)
            .cloned()
            .collect();
        for fqn in index_keys {
            self.drop_index(&fqn);
        }

        let table_keys: Vec<TableFqn> = self
            .added_tables
            .keys()
            .filter(|fqn| fqn.catalog == catalog && fqn.namespace == namespace)
            .cloned()
            .collect();
        for fqn in table_keys {
            self.drop_table(&fqn);
        }
    }
}

/// トランザクション内（オーバーレイ込み）で参照するための Catalog ビュー。
///
/// DML/SELECT の実行や Planner の参照用途に使う。書き込み系 API は利用しない前提のため、
/// `Catalog` trait の書き込みメソッドは `unreachable!()` とする。
pub struct TxnCatalogView<'a, S: KVStore> {
    catalog: &'a PersistentCatalog<S>,
    overlay: &'a CatalogOverlay,
}

impl<'a, S: KVStore> TxnCatalogView<'a, S> {
    pub fn new(catalog: &'a PersistentCatalog<S>, overlay: &'a CatalogOverlay) -> Self {
        Self { catalog, overlay }
    }
}

impl<'a, S: KVStore> Catalog for TxnCatalogView<'a, S> {
    fn create_table(&mut self, _table: TableMetadata) -> Result<(), PlannerError> {
        unreachable!("TxnCatalogView は参照専用です")
    }

    fn get_table(&self, name: &str) -> Option<&TableMetadata> {
        self.catalog.get_table_in_txn(name, self.overlay)
    }

    fn drop_table(&mut self, _name: &str) -> Result<(), PlannerError> {
        unreachable!("TxnCatalogView は参照専用です")
    }

    fn create_index(&mut self, _index: IndexMetadata) -> Result<(), PlannerError> {
        unreachable!("TxnCatalogView は参照専用です")
    }

    fn get_index(&self, name: &str) -> Option<&IndexMetadata> {
        self.catalog.get_index_in_txn(name, self.overlay)
    }

    fn get_indexes_for_table(&self, table: &str) -> Vec<&IndexMetadata> {
        let Some(table_meta) = self.catalog.get_table_in_txn(table, self.overlay) else {
            return Vec::new();
        };

        let mut indexes: Vec<&IndexMetadata> = self
            .catalog
            .inner
            .get_indexes_for_table(table)
            .into_iter()
            .filter(|idx| {
                idx.catalog_name == table_meta.catalog_name
                    && idx.namespace_name == table_meta.namespace_name
                    && !self.catalog.index_hidden_by_overlay(idx, self.overlay)
            })
            .collect();

        for idx in self.overlay.added_indexes.values() {
            if idx.table == table
                && idx.catalog_name == table_meta.catalog_name
                && idx.namespace_name == table_meta.namespace_name
                && !self.catalog.index_hidden_by_overlay(idx, self.overlay)
            {
                indexes.push(idx);
            }
        }

        indexes
    }

    fn drop_index(&mut self, _name: &str) -> Result<(), PlannerError> {
        unreachable!("TxnCatalogView は参照専用です")
    }

    fn table_exists(&self, name: &str) -> bool {
        self.catalog.table_exists_in_txn(name, self.overlay)
    }

    fn index_exists(&self, name: &str) -> bool {
        self.catalog.index_exists_in_txn(name, self.overlay)
    }

    fn next_table_id(&mut self) -> u32 {
        unreachable!("TxnCatalogView は参照専用です")
    }

    fn next_index_id(&mut self) -> u32 {
        unreachable!("TxnCatalogView は参照専用です")
    }
}

/// 永続カタログ実装。
#[derive(Debug)]
/// 永続化対応のカタログ実装。
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use alopex_core::kv::memory::MemoryKV;
/// use alopex_sql::Catalog;
/// use alopex_sql::catalog::PersistentCatalog;
///
/// let store = Arc::new(MemoryKV::new());
/// let catalog = PersistentCatalog::new(store);
/// assert!(catalog.table_exists("users") == false);
/// ```
pub struct PersistentCatalog<S: KVStore> {
    inner: MemoryCatalog,
    store: Arc<S>,
    catalogs: HashMap<String, CatalogMeta>,
    namespaces: HashMap<(String, String), NamespaceMeta>,
}

impl<S: KVStore> PersistentCatalog<S> {
    pub fn load(store: Arc<S>) -> Result<Self, CatalogError> {
        let mut txn = store.begin(TxnMode::ReadOnly)?;

        let mut inner = MemoryCatalog::new();
        let mut catalogs = HashMap::new();
        let mut namespaces = HashMap::new();

        let mut max_table_id = 0u32;
        let mut max_index_id = 0u32;

        for (key, value) in txn.scan_prefix(CATALOGS_PREFIX)? {
            let catalog_name = key_suffix(CATALOGS_PREFIX, &key)?;
            let mut meta: CatalogMeta = bincode::deserialize(&value)?;
            if meta.name != catalog_name {
                meta.name = catalog_name.clone();
            }
            catalogs.insert(catalog_name, meta);
        }

        for (key, value) in txn.scan_prefix(NAMESPACES_PREFIX)? {
            let suffix = key_suffix(NAMESPACES_PREFIX, &key)?;
            let mut parts = suffix.splitn(2, '/');
            let catalog_name = parts
                .next()
                .filter(|part| !part.is_empty())
                .ok_or_else(|| CatalogError::InvalidKey(suffix.clone()))?;
            let namespace_name = parts
                .next()
                .filter(|part| !part.is_empty())
                .ok_or_else(|| CatalogError::InvalidKey(suffix.clone()))?;
            let mut meta: NamespaceMeta = bincode::deserialize(&value)?;
            if meta.catalog_name != catalog_name {
                meta.catalog_name = catalog_name.to_string();
            }
            if meta.name != namespace_name {
                meta.name = namespace_name.to_string();
            }
            namespaces.insert((meta.catalog_name.clone(), meta.name.clone()), meta);
        }

        // テーブルをロード（まずテーブルを入れてからインデックスを入れる）
        for (key, value) in txn.scan_prefix(TABLES_PREFIX)? {
            let _table_name = key_suffix(TABLES_PREFIX, &key)?;
            let persisted: PersistedTableMeta = bincode::deserialize(&value)?;
            max_table_id = max_table_id.max(persisted.table_id);
            let table: TableMetadata = persisted.into();
            inner.insert_table_unchecked(table);
        }

        for (key, value) in txn.scan_prefix(INDEXES_PREFIX)? {
            let _index_name = key_suffix(INDEXES_PREFIX, &key)?;
            let persisted = deserialize_index_meta(&value)?;
            max_index_id = max_index_id.max(persisted.index_id);
            let mut index: IndexMetadata = persisted.into();
            // 参照先テーブルがない場合はスキップ（破損対策）
            if let Some(table) = inner.get_table(&index.table) {
                if index.catalog_name != table.catalog_name
                    || index.namespace_name != table.namespace_name
                {
                    index.catalog_name = table.catalog_name.clone();
                    index.namespace_name = table.namespace_name.clone();
                }
                inner.insert_index_unchecked(index);
            }
        }

        let (mut table_id_counter, mut index_id_counter) = (max_table_id, max_index_id);
        let meta_key = META_KEY.to_vec();
        if let Some(meta_bytes) = txn.get(&meta_key)? {
            let meta: CatalogState = bincode::deserialize(&meta_bytes)?;
            if meta.version == CATALOG_VERSION {
                table_id_counter = table_id_counter.max(meta.table_id_counter);
                index_id_counter = index_id_counter.max(meta.index_id_counter);
            }
        }
        inner.set_counters(table_id_counter, index_id_counter);

        txn.rollback_self()?;

        Ok(Self {
            inner,
            store,
            catalogs,
            namespaces,
        })
    }

    pub fn new(store: Arc<S>) -> Self {
        Self {
            inner: MemoryCatalog::new(),
            store,
            catalogs: HashMap::new(),
            namespaces: HashMap::new(),
        }
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub fn list_catalogs(&self) -> Vec<CatalogMeta> {
        let mut catalogs: Vec<CatalogMeta> = self.catalogs.values().cloned().collect();
        catalogs.sort_by(|a, b| a.name.cmp(&b.name));
        catalogs
    }

    pub fn get_catalog(&self, name: &str) -> Option<CatalogMeta> {
        self.catalogs.get(name).cloned()
    }

    pub fn create_catalog(&mut self, meta: CatalogMeta) -> Result<(), CatalogError> {
        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        let value = bincode::serialize(&meta)?;
        txn.put(catalog_key(&meta.name), value)?;
        txn.commit_self()?;
        self.catalogs.insert(meta.name.clone(), meta);
        Ok(())
    }

    pub fn delete_catalog(&mut self, name: &str) -> Result<(), CatalogError> {
        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        txn.delete(catalog_key(name))?;
        let mut namespace_prefix = NAMESPACES_PREFIX.to_vec();
        namespace_prefix.extend_from_slice(name.as_bytes());
        namespace_prefix.push(b'/');
        let mut namespace_keys = Vec::new();
        for (key, _) in txn.scan_prefix(&namespace_prefix)? {
            namespace_keys.push(key);
        }
        for key in namespace_keys {
            txn.delete(key)?;
        }
        let mut table_keys = Vec::new();
        let mut table_names = Vec::new();
        for (key, value) in txn.scan_prefix(TABLES_PREFIX)? {
            let persisted: PersistedTableMeta = bincode::deserialize(&value)?;
            if persisted.catalog_name == name {
                table_names.push(persisted.name.clone());
                table_keys.push(key);
            }
        }
        let table_set: HashSet<String> = table_names.iter().cloned().collect();
        for key in table_keys {
            txn.delete(key)?;
        }
        if !table_set.is_empty() {
            let mut index_keys = Vec::new();
            for (key, value) in txn.scan_prefix(INDEXES_PREFIX)? {
                let persisted = deserialize_index_meta(&value)?;
                if table_set.contains(&persisted.table) {
                    index_keys.push(key);
                }
            }
            for key in index_keys {
                txn.delete(key)?;
            }
        }
        txn.commit_self()?;
        self.catalogs.remove(name);
        self.namespaces.retain(|(catalog, _), _| catalog != name);
        for table_name in table_names {
            self.inner.remove_table_unchecked(&table_name);
        }
        Ok(())
    }

    pub fn list_namespaces(&self, catalog_name: &str) -> Vec<NamespaceMeta> {
        let mut namespaces: Vec<NamespaceMeta> = self
            .namespaces
            .values()
            .filter(|meta| meta.catalog_name == catalog_name)
            .cloned()
            .collect();
        namespaces.sort_by(|a, b| a.name.cmp(&b.name));
        namespaces
    }

    pub fn get_namespace(&self, catalog_name: &str, namespace_name: &str) -> Option<NamespaceMeta> {
        self.namespaces
            .get(&(catalog_name.to_string(), namespace_name.to_string()))
            .cloned()
    }

    pub fn create_namespace(&mut self, meta: NamespaceMeta) -> Result<(), CatalogError> {
        if !self.catalogs.contains_key(&meta.catalog_name) {
            return Err(CatalogError::InvalidKey(format!(
                "catalog not found: {}",
                meta.catalog_name
            )));
        }

        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        let value = bincode::serialize(&meta)?;
        txn.put(namespace_key(&meta.catalog_name, &meta.name), value)?;
        txn.commit_self()?;
        self.namespaces
            .insert((meta.catalog_name.clone(), meta.name.clone()), meta);
        Ok(())
    }

    pub fn delete_namespace(
        &mut self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<(), CatalogError> {
        if !self.catalogs.contains_key(catalog_name) {
            return Err(CatalogError::InvalidKey(format!(
                "catalog not found: {}",
                catalog_name
            )));
        }

        let mut txn = self.store.begin(TxnMode::ReadWrite)?;
        txn.delete(namespace_key(catalog_name, namespace_name))?;
        txn.commit_self()?;
        self.namespaces
            .remove(&(catalog_name.to_string(), namespace_name.to_string()));
        Ok(())
    }

    fn write_meta(&self, txn: &mut S::Transaction<'_>) -> Result<(), CatalogError> {
        let (table_id_counter, index_id_counter) = self.inner.counters();
        let meta = CatalogState {
            version: CATALOG_VERSION,
            table_id_counter,
            index_id_counter,
        };
        let meta_bytes = bincode::serialize(&meta)?;
        txn.put(META_KEY.to_vec(), meta_bytes)?;
        Ok(())
    }

    pub fn persist_create_table(
        &mut self,
        txn: &mut S::Transaction<'_>,
        table: &TableMetadata,
    ) -> Result<(), CatalogError> {
        let persisted = PersistedTableMeta::from(table);
        let value = bincode::serialize(&persisted)?;
        txn.put(table_key(&table.name), value)?;
        self.write_meta(txn)?;
        Ok(())
    }

    pub fn persist_drop_table(
        &mut self,
        txn: &mut S::Transaction<'_>,
        name: &str,
    ) -> Result<(), CatalogError> {
        txn.delete(table_key(name))?;

        // テーブルに紐づくインデックスも削除する。
        let mut to_delete: Vec<String> = Vec::new();
        for (key, value) in txn.scan_prefix(INDEXES_PREFIX)? {
            let persisted = deserialize_index_meta(&value)?;
            if persisted.table == name {
                let index_name = key_suffix(INDEXES_PREFIX, &key)?;
                to_delete.push(index_name);
            }
        }
        for index_name in to_delete {
            txn.delete(index_key(&index_name))?;
        }

        Ok(())
    }

    pub fn persist_create_index(
        &mut self,
        txn: &mut S::Transaction<'_>,
        index: &IndexMetadata,
    ) -> Result<(), CatalogError> {
        let persisted = PersistedIndexMeta::from(index);
        let value = bincode::serialize(&persisted)?;
        txn.put(index_key(&index.name), value)?;
        self.write_meta(txn)?;
        Ok(())
    }

    pub fn persist_drop_index(
        &mut self,
        txn: &mut S::Transaction<'_>,
        name: &str,
    ) -> Result<(), CatalogError> {
        txn.delete(index_key(name))?;
        Ok(())
    }

    fn namespace_dropped(overlay: &CatalogOverlay, catalog: &str, namespace: &str) -> bool {
        overlay.dropped_catalogs.contains(catalog)
            || overlay
                .dropped_namespaces
                .contains(&(catalog.to_string(), namespace.to_string()))
    }

    fn overlay_added_table_by_name<'a>(
        overlay: &'a CatalogOverlay,
        name: &str,
    ) -> Option<&'a TableMetadata> {
        let mut iter = overlay
            .added_tables
            .values()
            .filter(|table| table.name == name);
        let first = iter.next()?;
        if iter.next().is_some() {
            return None;
        }
        Some(first)
    }

    fn overlay_added_index_by_name<'a>(
        overlay: &'a CatalogOverlay,
        name: &str,
    ) -> Option<&'a IndexMetadata> {
        let mut iter = overlay
            .added_indexes
            .values()
            .filter(|index| index.name == name);
        let first = iter.next()?;
        if iter.next().is_some() {
            return None;
        }
        Some(first)
    }

    fn base_table_conflicts_with_overlay(
        &self,
        overlay: &CatalogOverlay,
        table: &TableMetadata,
    ) -> bool {
        let Some(base) = self.inner.get_table(&table.name) else {
            return false;
        };
        if self.table_hidden_by_overlay(base, overlay) {
            return false;
        }
        if overlay.dropped_tables.contains(&TableFqn::from(base)) {
            return false;
        }
        TableFqn::from(base) != TableFqn::from(table)
    }

    fn base_index_conflicts_with_overlay(
        &self,
        overlay: &CatalogOverlay,
        index: &IndexMetadata,
    ) -> bool {
        let Some(base) = self.inner.get_index(&index.name) else {
            return false;
        };
        if Self::namespace_dropped(overlay, &base.catalog_name, &base.namespace_name) {
            return false;
        }
        if overlay.dropped_indexes.contains(&IndexFqn::from(base)) {
            return false;
        }
        if self.dropped_table_matches_fqn(
            &base.table,
            &base.catalog_name,
            &base.namespace_name,
            overlay,
        ) {
            return false;
        }
        IndexFqn::from(base) != IndexFqn::from(index)
    }

    fn table_hidden_by_overlay(&self, table: &TableMetadata, overlay: &CatalogOverlay) -> bool {
        Self::namespace_dropped(overlay, &table.catalog_name, &table.namespace_name)
    }

    fn dropped_table_matches_fqn(
        &self,
        table_name: &str,
        catalog: &str,
        namespace: &str,
        overlay: &CatalogOverlay,
    ) -> bool {
        let fqn = TableFqn::new(catalog, namespace, table_name);
        overlay.dropped_tables.contains(&fqn)
    }

    fn index_hidden_by_overlay(&self, index: &IndexMetadata, overlay: &CatalogOverlay) -> bool {
        let index_fqn = IndexFqn::from(index);
        if overlay.dropped_indexes.contains(&index_fqn) {
            return true;
        }
        if Self::namespace_dropped(overlay, &index.catalog_name, &index.namespace_name) {
            return true;
        }
        if self.dropped_table_matches_fqn(
            &index.table,
            &index.catalog_name,
            &index.namespace_name,
            overlay,
        ) {
            return true;
        }
        match self.get_table_in_txn(&index.table, overlay) {
            Some(table) => {
                table.catalog_name != index.catalog_name
                    || table.namespace_name != index.namespace_name
            }
            None => true,
        }
    }

    pub fn get_catalog_in_txn<'a>(
        &'a self,
        name: &str,
        overlay: &'a CatalogOverlay,
    ) -> Option<&'a CatalogMeta> {
        if overlay.dropped_catalogs.contains(name) {
            return None;
        }
        if let Some(catalog) = overlay.added_catalogs.get(name) {
            return Some(catalog);
        }
        self.catalogs.get(name)
    }

    pub fn get_namespace_in_txn<'a>(
        &'a self,
        catalog_name: &str,
        namespace_name: &str,
        overlay: &'a CatalogOverlay,
    ) -> Option<&'a NamespaceMeta> {
        if overlay.dropped_catalogs.contains(catalog_name) {
            return None;
        }
        let key = (catalog_name.to_string(), namespace_name.to_string());
        if overlay.dropped_namespaces.contains(&key) {
            return None;
        }
        if let Some(namespace) = overlay.added_namespaces.get(&key) {
            return Some(namespace);
        }
        self.namespaces.get(&key)
    }

    pub fn list_catalogs_in_txn(&self, overlay: &CatalogOverlay) -> Vec<CatalogMeta> {
        let mut catalogs: HashMap<String, CatalogMeta> = HashMap::new();
        for (name, meta) in &self.catalogs {
            if !overlay.dropped_catalogs.contains(name) {
                catalogs.insert(name.clone(), meta.clone());
            }
        }
        for (name, meta) in &overlay.added_catalogs {
            if !overlay.dropped_catalogs.contains(name) {
                catalogs.insert(name.clone(), meta.clone());
            }
        }
        let mut values: Vec<CatalogMeta> = catalogs.into_values().collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    pub fn list_namespaces_in_txn(
        &self,
        catalog_name: &str,
        overlay: &CatalogOverlay,
    ) -> Vec<NamespaceMeta> {
        if overlay.dropped_catalogs.contains(catalog_name) {
            return Vec::new();
        }
        let mut namespaces: HashMap<(String, String), NamespaceMeta> = HashMap::new();
        for ((catalog, namespace), meta) in &self.namespaces {
            if catalog != catalog_name {
                continue;
            }
            let key = (catalog.clone(), namespace.clone());
            if overlay.dropped_namespaces.contains(&key) {
                continue;
            }
            namespaces.insert(key, meta.clone());
        }
        for ((catalog, namespace), meta) in &overlay.added_namespaces {
            if catalog != catalog_name {
                continue;
            }
            let key = (catalog.clone(), namespace.clone());
            if overlay.dropped_namespaces.contains(&key) {
                continue;
            }
            namespaces.insert(key, meta.clone());
        }
        let mut values: Vec<NamespaceMeta> = namespaces.into_values().collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    pub fn table_exists_in_txn(&self, name: &str, overlay: &CatalogOverlay) -> bool {
        if let Some(table) = Self::overlay_added_table_by_name(overlay, name) {
            if self.table_hidden_by_overlay(table, overlay) {
                return false;
            }
            if self.base_table_conflicts_with_overlay(overlay, table) {
                return false;
            }
            return true;
        }
        match self.inner.get_table(name) {
            Some(table) => {
                !self.table_hidden_by_overlay(table, overlay)
                    && !overlay.dropped_tables.contains(&TableFqn::from(table))
            }
            None => false,
        }
    }

    pub fn get_table_in_txn<'a>(
        &'a self,
        name: &str,
        overlay: &'a CatalogOverlay,
    ) -> Option<&'a TableMetadata> {
        if let Some(table) = Self::overlay_added_table_by_name(overlay, name) {
            if self.table_hidden_by_overlay(table, overlay) {
                return None;
            }
            if self.base_table_conflicts_with_overlay(overlay, table) {
                return None;
            }
            return Some(table);
        }
        self.inner.get_table(name).filter(|table| {
            !self.table_hidden_by_overlay(table, overlay)
                && !overlay.dropped_tables.contains(&TableFqn::from(*table))
        })
    }

    pub fn index_exists_in_txn(&self, name: &str, overlay: &CatalogOverlay) -> bool {
        if let Some(index) = Self::overlay_added_index_by_name(overlay, name) {
            if self.index_hidden_by_overlay(index, overlay) {
                return false;
            }
            if self.base_index_conflicts_with_overlay(overlay, index) {
                return false;
            }
            return true;
        }
        match self.inner.get_index(name) {
            Some(index) => !self.index_hidden_by_overlay(index, overlay),
            None => false,
        }
    }

    pub fn get_index_in_txn<'a>(
        &'a self,
        name: &str,
        overlay: &'a CatalogOverlay,
    ) -> Option<&'a IndexMetadata> {
        if let Some(index) = Self::overlay_added_index_by_name(overlay, name) {
            if self.index_hidden_by_overlay(index, overlay) {
                return None;
            }
            if self.base_index_conflicts_with_overlay(overlay, index) {
                return None;
            }
            return Some(index);
        }
        match self.inner.get_index(name) {
            Some(index) if self.index_hidden_by_overlay(index, overlay) => None,
            other => other,
        }
    }

    pub fn list_tables_in_txn(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        overlay: &CatalogOverlay,
    ) -> Vec<TableMetadata> {
        if overlay.dropped_catalogs.contains(catalog_name) {
            return Vec::new();
        }
        if overlay
            .dropped_namespaces
            .contains(&(catalog_name.to_string(), namespace_name.to_string()))
        {
            return Vec::new();
        }
        let mut tables: HashMap<TableFqn, TableMetadata> = HashMap::new();
        for name in self.inner.table_names() {
            if let Some(table) = self.inner.get_table(name)
                && table.catalog_name == catalog_name
                && table.namespace_name == namespace_name
            {
                let fqn = TableFqn::from(table);
                if !overlay.dropped_tables.contains(&fqn) {
                    tables.insert(fqn, table.clone());
                }
            }
        }
        for table in overlay.added_tables.values() {
            if table.catalog_name == catalog_name && table.namespace_name == namespace_name {
                tables.insert(TableFqn::from(table), table.clone());
            }
        }
        let mut values: Vec<TableMetadata> = tables.into_values().collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    pub fn list_indexes_in_txn(
        &self,
        fqn: &TableFqn,
        overlay: &CatalogOverlay,
    ) -> Vec<IndexMetadata> {
        if overlay.dropped_catalogs.contains(&fqn.catalog) {
            return Vec::new();
        }
        if overlay
            .dropped_namespaces
            .contains(&(fqn.catalog.clone(), fqn.namespace.clone()))
        {
            return Vec::new();
        }
        if overlay
            .dropped_tables
            .contains(&TableFqn::new(&fqn.catalog, &fqn.namespace, &fqn.table))
        {
            return Vec::new();
        }

        let mut indexes: HashMap<IndexFqn, IndexMetadata> = HashMap::new();
        for index in self.inner.get_indexes_for_table(&fqn.table) {
            if index.catalog_name == fqn.catalog && index.namespace_name == fqn.namespace {
                let index_fqn = IndexFqn::from(index);
                if !overlay.dropped_indexes.contains(&index_fqn) {
                    indexes.insert(index_fqn, index.clone());
                }
            }
        }
        for index in overlay.added_indexes.values() {
            if index.table == fqn.table
                && index.catalog_name == fqn.catalog
                && index.namespace_name == fqn.namespace
            {
                indexes.insert(IndexFqn::from(index), index.clone());
            }
        }
        let mut values: Vec<IndexMetadata> = indexes.into_values().collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    pub fn apply_overlay(&mut self, overlay: CatalogOverlay) {
        let CatalogOverlay {
            added_catalogs,
            dropped_catalogs,
            added_namespaces,
            dropped_namespaces,
            added_tables,
            dropped_tables,
            added_indexes,
            dropped_indexes,
        } = overlay;

        for (name, meta) in added_catalogs {
            self.catalogs.insert(name, meta);
        }
        for (catalog_name, namespace_name) in dropped_namespaces.iter() {
            self.namespaces
                .remove(&(catalog_name.clone(), namespace_name.clone()));
        }
        for ((catalog_name, namespace_name), meta) in added_namespaces {
            self.namespaces.insert((catalog_name, namespace_name), meta);
        }
        for name in dropped_catalogs {
            self.catalogs.remove(&name);
            self.namespaces.retain(|(catalog, _), _| catalog != &name);
        }
        for (_, table) in added_tables {
            self.inner.insert_table_unchecked(table);
        }
        for fqn in dropped_tables {
            self.inner.remove_table_unchecked(&fqn.table);
        }
        for (_, index) in added_indexes {
            self.inner.insert_index_unchecked(index);
        }
        for fqn in dropped_indexes {
            self.inner.remove_index_unchecked(&fqn.index);
        }
    }

    pub fn discard_overlay(_overlay: CatalogOverlay) {}
}

impl<S: KVStore> Catalog for PersistentCatalog<S> {
    fn create_table(&mut self, table: TableMetadata) -> Result<(), PlannerError> {
        self.inner.create_table(table)
    }

    fn get_table(&self, name: &str) -> Option<&TableMetadata> {
        self.inner.get_table(name)
    }

    fn drop_table(&mut self, name: &str) -> Result<(), PlannerError> {
        self.inner.drop_table(name)
    }

    fn create_index(&mut self, index: IndexMetadata) -> Result<(), PlannerError> {
        self.inner.create_index(index)
    }

    fn get_index(&self, name: &str) -> Option<&IndexMetadata> {
        self.inner.get_index(name)
    }

    fn get_indexes_for_table(&self, table: &str) -> Vec<&IndexMetadata> {
        self.inner.get_indexes_for_table(table)
    }

    fn drop_index(&mut self, name: &str) -> Result<(), PlannerError> {
        self.inner.drop_index(name)
    }

    fn table_exists(&self, name: &str) -> bool {
        self.inner.table_exists(name)
    }

    fn index_exists(&self, name: &str) -> bool {
        self.inner.index_exists(name)
    }

    fn next_table_id(&mut self) -> u32 {
        self.inner.next_table_id()
    }

    fn next_index_id(&mut self) -> u32 {
        self.inner.next_index_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::types::ResolvedType;
    use std::collections::HashSet;

    fn test_table(name: &str, id: u32) -> TableMetadata {
        TableMetadata::new(
            name,
            vec![ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true)],
        )
        .with_table_id(id)
        .with_primary_key(vec!["id".to_string()])
    }

    #[test]
    fn load_empty_store() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let catalog = PersistentCatalog::load(store).unwrap();
        assert_eq!(catalog.inner.table_count(), 0);
        assert_eq!(catalog.inner.index_count(), 0);
    }

    #[test]
    fn create_table_persists() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());

        // inner のカウンタを更新して meta が書き込まれることを担保する
        catalog.inner.set_counters(1, 0);

        let table = test_table("users", 1);
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog.persist_create_table(&mut txn, &table).unwrap();
        txn.commit_self().unwrap();

        let reloaded = PersistentCatalog::load(store).unwrap();
        assert!(reloaded.table_exists("users"));
        assert_eq!(reloaded.get_table("users").unwrap().table_id, 1);
    }

    #[test]
    fn drop_table_removes() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());
        catalog.inner.set_counters(1, 0);

        let table = test_table("users", 1);
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog.persist_create_table(&mut txn, &table).unwrap();
        txn.commit_self().unwrap();

        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog.persist_drop_table(&mut txn, "users").unwrap();
        txn.commit_self().unwrap();

        let reloaded = PersistentCatalog::load(store).unwrap();
        assert!(!reloaded.table_exists("users"));
    }

    #[test]
    fn reload_preserves_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("catalog.wal");
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::open(&wal_path).unwrap());
        let mut catalog = PersistentCatalog::new(store.clone());
        catalog.inner.set_counters(1, 0);

        let table = test_table("users", 1);
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog.persist_create_table(&mut txn, &table).unwrap();
        txn.commit_self().unwrap();
        store.flush().unwrap();

        drop(catalog);
        drop(store);

        let store = Arc::new(alopex_core::kv::memory::MemoryKV::open(&wal_path).unwrap());
        let reloaded = PersistentCatalog::load(store).unwrap();
        assert!(reloaded.table_exists("users"));
    }

    #[test]
    fn overlay_applied_on_commit() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);
        let users = test_table("users", 1);
        catalog.inner.insert_table_unchecked(users.clone());

        let mut overlay = CatalogOverlay::new();
        overlay.drop_table(&TableFqn::from(&users));
        let orders = test_table("orders", 2);
        overlay.add_table(TableFqn::from(&orders), orders);

        assert!(!catalog.table_exists_in_txn("users", &overlay));
        assert!(catalog.table_exists_in_txn("orders", &overlay));

        catalog.apply_overlay(overlay);

        assert!(!catalog.table_exists("users"));
        assert!(catalog.table_exists("orders"));
    }

    #[test]
    fn overlay_discarded_on_rollback() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);
        let users = test_table("users", 1);
        catalog.inner.insert_table_unchecked(users.clone());

        let mut overlay = CatalogOverlay::new();
        overlay.drop_table(&TableFqn::from(&users));

        PersistentCatalog::<alopex_core::kv::memory::MemoryKV>::discard_overlay(overlay);

        assert!(catalog.table_exists("users"));
    }

    #[test]
    fn catalog_crud_persists() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());

        let meta = CatalogMeta {
            name: "main".to_string(),
            comment: Some("primary".to_string()),
            storage_root: Some("/tmp/alopex".to_string()),
        };

        catalog.create_catalog(meta.clone()).unwrap();
        assert!(catalog.get_catalog("main").is_some());

        let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
        let stored = txn.get(&catalog_key("main")).unwrap().unwrap();
        let decoded: CatalogMeta = bincode::deserialize(&stored).unwrap();
        txn.rollback_self().unwrap();
        assert_eq!(decoded, meta);

        catalog.delete_catalog("main").unwrap();
        assert!(catalog.get_catalog("main").is_none());
    }

    #[test]
    fn namespace_crud_persists_and_validates_catalog() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());

        let missing_catalog = NamespaceMeta {
            name: "analytics".to_string(),
            catalog_name: "missing".to_string(),
            comment: None,
            storage_root: None,
        };
        let err = catalog.create_namespace(missing_catalog).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidKey(_)));

        catalog
            .create_catalog(CatalogMeta {
                name: "main".to_string(),
                comment: None,
                storage_root: None,
            })
            .unwrap();

        let namespace = NamespaceMeta {
            name: "analytics".to_string(),
            catalog_name: "main".to_string(),
            comment: Some("warehouse".to_string()),
            storage_root: None,
        };

        catalog.create_namespace(namespace.clone()).unwrap();
        assert!(catalog.get_namespace("main", "analytics").is_some());

        let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
        let stored = txn
            .get(&namespace_key("main", "analytics"))
            .unwrap()
            .unwrap();
        let decoded: NamespaceMeta = bincode::deserialize(&stored).unwrap();
        txn.rollback_self().unwrap();
        assert_eq!(decoded, namespace);

        catalog.delete_namespace("main", "analytics").unwrap();
        assert!(catalog.get_namespace("main", "analytics").is_none());
    }

    #[test]
    fn delete_catalog_removes_namespaces_from_store() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());

        catalog
            .create_catalog(CatalogMeta {
                name: "main".to_string(),
                comment: None,
                storage_root: None,
            })
            .unwrap();
        catalog
            .create_namespace(NamespaceMeta {
                name: "analytics".to_string(),
                catalog_name: "main".to_string(),
                comment: None,
                storage_root: None,
            })
            .unwrap();

        catalog.delete_catalog("main").unwrap();

        let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
        let mut prefix = NAMESPACES_PREFIX.to_vec();
        prefix.extend_from_slice(b"main");
        prefix.push(b'/');
        let remaining: Vec<_> = txn.scan_prefix(&prefix).unwrap().collect();
        txn.rollback_self().unwrap();

        assert!(remaining.is_empty());
        assert!(catalog.list_namespaces("main").is_empty());
    }

    #[test]
    fn delete_catalog_removes_tables_and_indexes_from_store() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());

        catalog
            .create_catalog(CatalogMeta {
                name: "main".to_string(),
                comment: None,
                storage_root: None,
            })
            .unwrap();

        let mut table = test_table("users", 1);
        table.catalog_name = "main".to_string();
        table.namespace_name = "default".to_string();

        let mut index = IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
            .with_column_indices(vec![0]);
        index.catalog_name = "main".to_string();
        index.namespace_name = "default".to_string();

        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog.persist_create_table(&mut txn, &table).unwrap();
        catalog.persist_create_index(&mut txn, &index).unwrap();
        txn.commit_self().unwrap();

        catalog.inner.insert_table_unchecked(table);
        catalog.inner.insert_index_unchecked(index);

        catalog.delete_catalog("main").unwrap();

        assert!(catalog.inner.get_table("users").is_none());
        assert!(catalog.inner.get_index("idx_users_id").is_none());

        let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
        assert!(txn.get(&table_key("users")).unwrap().is_none());
        assert!(txn.get(&index_key("idx_users_id")).unwrap().is_none());
        txn.rollback_self().unwrap();
    }

    #[test]
    fn index_meta_loads_catalog_and_namespace_from_table() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());
        catalog.inner.set_counters(1, 1);

        let mut table = test_table("users", 1);
        table.catalog_name = "main".to_string();
        table.namespace_name = "analytics".to_string();

        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog.persist_create_table(&mut txn, &table).unwrap();
        let mut index = IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
            .with_column_indices(vec![0])
            .with_method(IndexMethod::BTree);
        index.catalog_name = "main".to_string();
        index.namespace_name = "analytics".to_string();
        catalog.persist_create_index(&mut txn, &index).unwrap();
        txn.commit_self().unwrap();

        let reloaded = PersistentCatalog::load(store).unwrap();
        let index = reloaded.get_index("idx_users_id").unwrap();
        assert_eq!(index.catalog_name, "main");
        assert_eq!(index.namespace_name, "analytics");
    }

    #[test]
    fn legacy_index_meta_loads_catalog_and_namespace_from_table() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store.clone());
        catalog.inner.set_counters(1, 1);

        let mut table = test_table("users", 1);
        table.catalog_name = "main".to_string();
        table.namespace_name = "analytics".to_string();

        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        catalog.persist_create_table(&mut txn, &table).unwrap();
        let legacy = PersistedIndexMetaV1 {
            index_id: 1,
            name: "idx_users_id".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            column_indices: vec![0],
            unique: false,
            method: Some(PersistedIndexType::BTree),
            options: Vec::new(),
        };
        let bytes = bincode::serialize(&legacy).unwrap();
        txn.put(index_key("idx_users_id"), bytes).unwrap();
        txn.commit_self().unwrap();

        let reloaded = PersistentCatalog::load(store).unwrap();
        let index = reloaded.get_index("idx_users_id").unwrap();
        assert_eq!(index.catalog_name, "main");
        assert_eq!(index.namespace_name, "analytics");
    }

    #[test]
    fn overlay_catalog_get_and_list() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);
        catalog
            .create_catalog(CatalogMeta {
                name: "main".to_string(),
                comment: None,
                storage_root: None,
            })
            .unwrap();

        let mut overlay = CatalogOverlay::new();
        overlay.add_catalog(CatalogMeta {
            name: "temp".to_string(),
            comment: None,
            storage_root: None,
        });
        overlay.drop_catalog("main");

        assert!(catalog.get_catalog_in_txn("main", &overlay).is_none());
        assert!(catalog.get_catalog_in_txn("temp", &overlay).is_some());

        let names: Vec<String> = catalog
            .list_catalogs_in_txn(&overlay)
            .into_iter()
            .map(|meta| meta.name)
            .collect();
        assert_eq!(names, vec!["temp".to_string()]);
    }

    #[test]
    fn overlay_namespace_get_and_list() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);
        catalog
            .create_catalog(CatalogMeta {
                name: "main".to_string(),
                comment: None,
                storage_root: None,
            })
            .unwrap();
        catalog
            .create_namespace(NamespaceMeta {
                name: "default".to_string(),
                catalog_name: "main".to_string(),
                comment: None,
                storage_root: None,
            })
            .unwrap();

        let mut overlay = CatalogOverlay::new();
        overlay.add_namespace(NamespaceMeta {
            name: "analytics".to_string(),
            catalog_name: "main".to_string(),
            comment: None,
            storage_root: None,
        });
        overlay.drop_namespace("main", "default");

        assert!(
            catalog
                .get_namespace_in_txn("main", "default", &overlay)
                .is_none()
        );
        assert!(
            catalog
                .get_namespace_in_txn("main", "analytics", &overlay)
                .is_some()
        );

        let names: Vec<String> = catalog
            .list_namespaces_in_txn("main", &overlay)
            .into_iter()
            .map(|meta| meta.name)
            .collect();
        assert_eq!(names, vec!["analytics".to_string()]);
    }

    #[test]
    fn overlay_table_and_index_list() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);

        let mut users = test_table("users", 1);
        users.catalog_name = "main".to_string();
        users.namespace_name = "default".to_string();
        catalog.inner.insert_table_unchecked(users.clone());

        let mut users_index =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        users_index.catalog_name = "main".to_string();
        users_index.namespace_name = "default".to_string();
        catalog.inner.insert_index_unchecked(users_index);

        let mut overlay = CatalogOverlay::new();

        let mut orders = test_table("orders", 2);
        orders.catalog_name = "main".to_string();
        orders.namespace_name = "default".to_string();
        overlay.add_table(TableFqn::from(&orders), orders.clone());

        let mut orders_index =
            IndexMetadata::new(2, "idx_orders_id", "orders", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        orders_index.catalog_name = "main".to_string();
        orders_index.namespace_name = "default".to_string();
        overlay.add_index(IndexFqn::from(&orders_index), orders_index);

        overlay.drop_table(&TableFqn::from(&users));

        let table_names: Vec<String> = catalog
            .list_tables_in_txn("main", "default", &overlay)
            .into_iter()
            .map(|table| table.name)
            .collect();
        assert_eq!(table_names, vec!["orders".to_string()]);

        let users_fqn = TableFqn::new("main", "default", "users");
        assert!(catalog.list_indexes_in_txn(&users_fqn, &overlay).is_empty());

        let orders_fqn = TableFqn::new("main", "default", "orders");
        let index_names: Vec<String> = catalog
            .list_indexes_in_txn(&orders_fqn, &overlay)
            .into_iter()
            .map(|index| index.name)
            .collect();
        assert_eq!(index_names, vec!["idx_orders_id".to_string()]);
    }

    #[test]
    fn overlay_name_lookup_ambiguous_returns_none() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let catalog = PersistentCatalog::new(store);

        let mut overlay = CatalogOverlay::new();

        let mut users_default = test_table("users", 1);
        users_default.catalog_name = "main".to_string();
        users_default.namespace_name = "default".to_string();
        overlay.add_table(TableFqn::from(&users_default), users_default);

        let mut users_analytics = test_table("users", 2);
        users_analytics.catalog_name = "main".to_string();
        users_analytics.namespace_name = "analytics".to_string();
        overlay.add_table(TableFqn::from(&users_analytics), users_analytics);

        assert!(catalog.get_table_in_txn("users", &overlay).is_none());
        assert!(!catalog.table_exists_in_txn("users", &overlay));

        let mut idx_default =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        idx_default.catalog_name = "main".to_string();
        idx_default.namespace_name = "default".to_string();
        overlay.add_index(IndexFqn::from(&idx_default), idx_default);

        let mut idx_analytics =
            IndexMetadata::new(2, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        idx_analytics.catalog_name = "main".to_string();
        idx_analytics.namespace_name = "analytics".to_string();
        overlay.add_index(IndexFqn::from(&idx_analytics), idx_analytics);

        assert!(catalog.get_index_in_txn("idx_users_id", &overlay).is_none());
        assert!(!catalog.index_exists_in_txn("idx_users_id", &overlay));
    }

    #[test]
    fn overlay_name_lookup_ambiguous_with_base_returns_none() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);

        let mut overlay = CatalogOverlay::new();

        let mut base_users = test_table("users", 1);
        base_users.catalog_name = "main".to_string();
        base_users.namespace_name = "default".to_string();
        catalog.inner.insert_table_unchecked(base_users);

        let mut overlay_users = test_table("users", 2);
        overlay_users.catalog_name = "main".to_string();
        overlay_users.namespace_name = "analytics".to_string();
        overlay.add_table(TableFqn::from(&overlay_users), overlay_users);

        assert!(catalog.get_table_in_txn("users", &overlay).is_none());
        assert!(!catalog.table_exists_in_txn("users", &overlay));

        let mut base_index =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        base_index.catalog_name = "main".to_string();
        base_index.namespace_name = "default".to_string();
        catalog.inner.insert_index_unchecked(base_index);

        let mut overlay_index =
            IndexMetadata::new(2, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        overlay_index.catalog_name = "main".to_string();
        overlay_index.namespace_name = "analytics".to_string();
        overlay.add_index(IndexFqn::from(&overlay_index), overlay_index);

        assert!(catalog.get_index_in_txn("idx_users_id", &overlay).is_none());
        assert!(!catalog.index_exists_in_txn("idx_users_id", &overlay));
    }

    #[test]
    fn overlay_fqn_tables_separate_namespaces() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let catalog = PersistentCatalog::new(store);

        let mut overlay = CatalogOverlay::new();

        let mut users_default = test_table("users", 1);
        users_default.catalog_name = "main".to_string();
        users_default.namespace_name = "default".to_string();
        overlay.add_table(TableFqn::from(&users_default), users_default.clone());

        let mut users_analytics = test_table("users", 2);
        users_analytics.catalog_name = "main".to_string();
        users_analytics.namespace_name = "analytics".to_string();
        overlay.add_table(TableFqn::from(&users_analytics), users_analytics.clone());

        let default_tables: Vec<String> = catalog
            .list_tables_in_txn("main", "default", &overlay)
            .into_iter()
            .map(|table| table.name)
            .collect();
        assert_eq!(default_tables, vec!["users".to_string()]);

        let analytics_tables: Vec<String> = catalog
            .list_tables_in_txn("main", "analytics", &overlay)
            .into_iter()
            .map(|table| table.name)
            .collect();
        assert_eq!(analytics_tables, vec!["users".to_string()]);

        overlay.drop_table(&TableFqn::from(&users_default));

        let default_tables_after: Vec<String> = catalog
            .list_tables_in_txn("main", "default", &overlay)
            .into_iter()
            .map(|table| table.name)
            .collect();
        assert!(default_tables_after.is_empty());

        let analytics_tables_after: Vec<String> = catalog
            .list_tables_in_txn("main", "analytics", &overlay)
            .into_iter()
            .map(|table| table.name)
            .collect();
        assert_eq!(analytics_tables_after, vec!["users".to_string()]);
    }

    #[test]
    fn overlay_fqn_indexes_separate_namespaces() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let catalog = PersistentCatalog::new(store);

        let mut overlay = CatalogOverlay::new();

        let mut users_default = test_table("users", 1);
        users_default.catalog_name = "main".to_string();
        users_default.namespace_name = "default".to_string();
        overlay.add_table(TableFqn::from(&users_default), users_default.clone());

        let mut users_analytics = test_table("users", 2);
        users_analytics.catalog_name = "main".to_string();
        users_analytics.namespace_name = "analytics".to_string();
        overlay.add_table(TableFqn::from(&users_analytics), users_analytics.clone());

        let mut idx_default =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        idx_default.catalog_name = "main".to_string();
        idx_default.namespace_name = "default".to_string();
        overlay.add_index(IndexFqn::from(&idx_default), idx_default);

        let mut idx_analytics =
            IndexMetadata::new(2, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        idx_analytics.catalog_name = "main".to_string();
        idx_analytics.namespace_name = "analytics".to_string();
        overlay.add_index(IndexFqn::from(&idx_analytics), idx_analytics);

        let default_fqn = TableFqn::new("main", "default", "users");
        let analytics_fqn = TableFqn::new("main", "analytics", "users");

        let default_indexes: Vec<String> = catalog
            .list_indexes_in_txn(&default_fqn, &overlay)
            .into_iter()
            .map(|index| index.name)
            .collect();
        assert_eq!(default_indexes, vec!["idx_users_id".to_string()]);

        let analytics_indexes: Vec<String> = catalog
            .list_indexes_in_txn(&analytics_fqn, &overlay)
            .into_iter()
            .map(|index| index.name)
            .collect();
        assert_eq!(analytics_indexes, vec!["idx_users_id".to_string()]);
    }

    #[test]
    fn overlay_drop_cascade_namespace_removes_children() {
        let mut overlay = CatalogOverlay::new();

        overlay.add_namespace(NamespaceMeta {
            name: "default".to_string(),
            catalog_name: "main".to_string(),
            comment: None,
            storage_root: None,
        });

        let mut users = test_table("users", 1);
        users.catalog_name = "main".to_string();
        users.namespace_name = "default".to_string();
        overlay.add_table(TableFqn::from(&users), users.clone());

        let mut users_index =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        users_index.catalog_name = "main".to_string();
        users_index.namespace_name = "default".to_string();
        let users_index_fqn = IndexFqn::from(&users_index);
        overlay.add_index(users_index_fqn.clone(), users_index);

        overlay.drop_cascade_namespace("main", "default");

        assert!(
            overlay
                .dropped_namespaces
                .contains(&("main".to_string(), "default".to_string()))
        );
        assert!(overlay.dropped_tables.contains(&TableFqn::from(&users)));
        assert!(overlay.dropped_indexes.contains(&users_index_fqn));
        assert!(!overlay.added_tables.contains_key(&TableFqn::from(&users)));
        assert!(!overlay.added_indexes.contains_key(&users_index_fqn));
    }

    #[test]
    fn overlay_drop_cascade_catalog_removes_children() {
        let mut overlay = CatalogOverlay::new();

        overlay.add_catalog(CatalogMeta {
            name: "main".to_string(),
            comment: None,
            storage_root: None,
        });
        overlay.add_namespace(NamespaceMeta {
            name: "default".to_string(),
            catalog_name: "main".to_string(),
            comment: None,
            storage_root: None,
        });

        let mut users = test_table("users", 1);
        users.catalog_name = "main".to_string();
        users.namespace_name = "default".to_string();
        overlay.add_table(TableFqn::from(&users), users.clone());

        let mut users_index =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        users_index.catalog_name = "main".to_string();
        users_index.namespace_name = "default".to_string();
        let users_index_fqn = IndexFqn::from(&users_index);
        overlay.add_index(users_index_fqn.clone(), users_index);

        overlay.drop_cascade_catalog("main");

        assert!(overlay.dropped_catalogs.contains("main"));
        assert!(overlay.dropped_tables.contains(&TableFqn::from(&users)));
        assert!(overlay.dropped_indexes.contains(&users_index_fqn));
        assert!(!overlay.added_tables.contains_key(&TableFqn::from(&users)));
        assert!(!overlay.added_indexes.contains_key(&users_index_fqn));
    }

    #[test]
    fn dropped_namespace_hides_tables_and_indexes() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);

        let mut users = test_table("users", 1);
        users.catalog_name = "main".to_string();
        users.namespace_name = "default".to_string();
        catalog.inner.insert_table_unchecked(users);

        let mut users_index =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        users_index.catalog_name = "main".to_string();
        users_index.namespace_name = "default".to_string();
        catalog.inner.insert_index_unchecked(users_index);

        let mut overlay = CatalogOverlay::new();
        overlay.drop_namespace("main", "default");

        let table_names: Vec<String> = catalog
            .list_tables_in_txn("main", "default", &overlay)
            .into_iter()
            .map(|table| table.name)
            .collect();
        assert!(table_names.is_empty());

        let fqn = TableFqn {
            catalog: "main".to_string(),
            namespace: "default".to_string(),
            table: "users".to_string(),
        };
        let index_names: Vec<String> = catalog
            .list_indexes_in_txn(&fqn, &overlay)
            .into_iter()
            .map(|index| index.name)
            .collect();
        assert!(index_names.is_empty());
    }

    #[test]
    fn dropped_namespace_hides_get_and_exists() {
        let store = Arc::new(alopex_core::kv::memory::MemoryKV::new());
        let mut catalog = PersistentCatalog::new(store);

        let mut users = test_table("users", 1);
        users.catalog_name = "main".to_string();
        users.namespace_name = "default".to_string();
        catalog.inner.insert_table_unchecked(users);

        let mut users_index =
            IndexMetadata::new(1, "idx_users_id", "users", vec!["id".to_string()])
                .with_column_indices(vec![0]);
        users_index.catalog_name = "main".to_string();
        users_index.namespace_name = "default".to_string();
        catalog.inner.insert_index_unchecked(users_index);

        let mut overlay = CatalogOverlay::new();
        overlay.drop_namespace("main", "default");

        assert!(!catalog.table_exists_in_txn("users", &overlay));
        assert!(catalog.get_table_in_txn("users", &overlay).is_none());
        assert!(!catalog.index_exists_in_txn("idx_users_id", &overlay));
        assert!(catalog.get_index_in_txn("idx_users_id", &overlay).is_none());

        let view = TxnCatalogView::new(&catalog, &overlay);
        assert!(view.get_indexes_for_table("users").is_empty());
    }

    #[test]
    fn persisted_catalog_meta_roundtrip() {
        let meta = PersistedCatalogMeta {
            name: "main".to_string(),
            comment: Some("primary catalog".to_string()),
            storage_root: Some("/tmp/alopex".to_string()),
        };
        let bytes = bincode::serialize(&meta).unwrap();
        let decoded: PersistedCatalogMeta = bincode::deserialize(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn persisted_namespace_meta_roundtrip() {
        let meta = PersistedNamespaceMeta {
            name: "analytics".to_string(),
            catalog_name: "main".to_string(),
            comment: Some("warehouse".to_string()),
            storage_root: Some("s3://bucket/ns".to_string()),
        };
        let bytes = bincode::serialize(&meta).unwrap();
        let decoded: PersistedNamespaceMeta = bincode::deserialize(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn table_fqn_hash_and_eq() {
        let first = TableFqn {
            catalog: "main".to_string(),
            namespace: "default".to_string(),
            table: "users".to_string(),
        };
        let same = TableFqn {
            catalog: "main".to_string(),
            namespace: "default".to_string(),
            table: "users".to_string(),
        };
        let different = TableFqn {
            catalog: "main".to_string(),
            namespace: "default".to_string(),
            table: "orders".to_string(),
        };

        let mut set = HashSet::new();
        set.insert(first);
        assert!(set.contains(&same));
        assert!(!set.contains(&different));
    }

    #[test]
    fn index_fqn_hash_and_eq() {
        let first = IndexFqn {
            catalog: "main".to_string(),
            namespace: "default".to_string(),
            table: "users".to_string(),
            index: "idx_users_id".to_string(),
        };
        let same = IndexFqn {
            catalog: "main".to_string(),
            namespace: "default".to_string(),
            table: "users".to_string(),
            index: "idx_users_id".to_string(),
        };
        let different = IndexFqn {
            catalog: "main".to_string(),
            namespace: "default".to_string(),
            table: "users".to_string(),
            index: "idx_users_email".to_string(),
        };

        let mut set = HashSet::new();
        set.insert(first);
        assert!(set.contains(&same));
        assert!(!set.contains(&different));
    }

    #[test]
    fn table_type_and_data_source_format_serde() {
        let managed = serde_json::to_string(&TableType::Managed).unwrap();
        let external = serde_json::to_string(&TableType::External).unwrap();
        let alopex = serde_json::to_string(&DataSourceFormat::Alopex).unwrap();
        let parquet = serde_json::to_string(&DataSourceFormat::Parquet).unwrap();
        let delta = serde_json::to_string(&DataSourceFormat::Delta).unwrap();

        assert_eq!(managed, "\"MANAGED\"");
        assert_eq!(external, "\"EXTERNAL\"");
        assert_eq!(alopex, "\"ALOPEX\"");
        assert_eq!(parquet, "\"PARQUET\"");
        assert_eq!(delta, "\"DELTA\"");
    }
}
