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
struct CatalogMeta {
    version: u32,
    table_id_counter: u32,
    index_id_counter: u32,
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
    pub columns: Vec<PersistedColumnMeta>,
    pub primary_key: Option<Vec<String>>,
    pub storage_options: PersistedStorageOptions,
}

impl From<&TableMetadata> for PersistedTableMeta {
    fn from(value: &TableMetadata) -> Self {
        Self {
            table_id: value.table_id,
            name: value.name.clone(),
            columns: value
                .columns
                .iter()
                .map(PersistedColumnMeta::from)
                .collect(),
            primary_key: value.primary_key.clone(),
            storage_options: value.storage_options.clone().into(),
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
        }
    }
}

impl From<PersistedIndexMeta> for IndexMetadata {
    fn from(value: PersistedIndexMeta) -> Self {
        let mut index = IndexMetadata::new(value.index_id, value.name, value.table, value.columns)
            .with_column_indices(value.column_indices)
            .with_unique(value.unique)
            .with_options(value.options);
        if let Some(method) = value.method {
            index = index.with_method(method.into());
        }
        index
    }
}

fn table_key(name: &str) -> Vec<u8> {
    let mut key = TABLES_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
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
    added_tables: HashMap<String, TableMetadata>,
    dropped_tables: HashSet<String>,
    added_indexes: HashMap<String, IndexMetadata>,
    dropped_indexes: HashSet<String>,
}

impl CatalogOverlay {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, table: TableMetadata) {
        self.dropped_tables.remove(&table.name);
        self.added_tables.insert(table.name.clone(), table);
    }

    pub fn drop_table(&mut self, name: &str) {
        self.added_tables.remove(name);
        self.dropped_tables.insert(name.to_string());
        self.added_indexes.retain(|_, idx| idx.table != name);
    }

    pub fn add_index(&mut self, index: IndexMetadata) {
        self.dropped_indexes.remove(&index.name);
        self.added_indexes.insert(index.name.clone(), index);
    }

    pub fn drop_index(&mut self, name: &str) {
        self.added_indexes.remove(name);
        self.dropped_indexes.insert(name.to_string());
    }
}

/// 永続カタログ実装。
#[derive(Debug)]
pub struct PersistentCatalog<S: KVStore> {
    inner: MemoryCatalog,
    store: Arc<S>,
}

impl<S: KVStore> PersistentCatalog<S> {
    pub fn load(store: Arc<S>) -> Result<Self, CatalogError> {
        let mut txn = store.begin(TxnMode::ReadOnly)?;

        let mut inner = MemoryCatalog::new();

        let mut max_table_id = 0u32;
        let mut max_index_id = 0u32;

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
            let persisted: PersistedIndexMeta = bincode::deserialize(&value)?;
            max_index_id = max_index_id.max(persisted.index_id);
            let index: IndexMetadata = persisted.into();
            // 参照先テーブルがない場合はスキップ（破損対策）
            if inner.table_exists(&index.table) {
                inner.insert_index_unchecked(index);
            }
        }

        let (mut table_id_counter, mut index_id_counter) = (max_table_id, max_index_id);
        let meta_key = META_KEY.to_vec();
        if let Some(meta_bytes) = txn.get(&meta_key)? {
            let meta: CatalogMeta = bincode::deserialize(&meta_bytes)?;
            if meta.version == CATALOG_VERSION {
                table_id_counter = table_id_counter.max(meta.table_id_counter);
                index_id_counter = index_id_counter.max(meta.index_id_counter);
            }
        }
        inner.set_counters(table_id_counter, index_id_counter);

        txn.rollback_self()?;

        Ok(Self { inner, store })
    }

    pub fn new(store: Arc<S>) -> Self {
        Self {
            inner: MemoryCatalog::new(),
            store,
        }
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    fn write_meta(&self, txn: &mut S::Transaction<'_>) -> Result<(), CatalogError> {
        let (table_id_counter, index_id_counter) = self.inner.counters();
        let meta = CatalogMeta {
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
            let persisted: PersistedIndexMeta = bincode::deserialize(&value)?;
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

    pub fn table_exists_in_txn(&self, name: &str, overlay: &CatalogOverlay) -> bool {
        if overlay.dropped_tables.contains(name) {
            return false;
        }
        if overlay.added_tables.contains_key(name) {
            return true;
        }
        self.inner.table_exists(name)
    }

    pub fn get_table_in_txn<'a>(
        &'a self,
        name: &str,
        overlay: &'a CatalogOverlay,
    ) -> Option<&'a TableMetadata> {
        if overlay.dropped_tables.contains(name) {
            return None;
        }
        if let Some(table) = overlay.added_tables.get(name) {
            return Some(table);
        }
        self.inner.get_table(name)
    }

    pub fn index_exists_in_txn(&self, name: &str, overlay: &CatalogOverlay) -> bool {
        if overlay.dropped_indexes.contains(name) {
            return false;
        }
        if let Some(index) = overlay.added_indexes.get(name) {
            if overlay.dropped_tables.contains(&index.table) {
                return false;
            }
            return true;
        }
        match self.inner.get_index(name) {
            Some(index) if overlay.dropped_tables.contains(&index.table) => false,
            Some(_) => true,
            None => false,
        }
    }

    pub fn get_index_in_txn<'a>(
        &'a self,
        name: &str,
        overlay: &'a CatalogOverlay,
    ) -> Option<&'a IndexMetadata> {
        if overlay.dropped_indexes.contains(name) {
            return None;
        }
        if let Some(index) = overlay.added_indexes.get(name) {
            if overlay.dropped_tables.contains(&index.table) {
                return None;
            }
            return Some(index);
        }
        match self.inner.get_index(name) {
            Some(index) if overlay.dropped_tables.contains(&index.table) => None,
            other => other,
        }
    }

    pub fn apply_overlay(&mut self, overlay: CatalogOverlay) {
        for (_, table) in overlay.added_tables {
            self.inner.insert_table_unchecked(table);
        }
        for name in overlay.dropped_tables {
            self.inner.remove_table_unchecked(&name);
        }
        for (_, index) in overlay.added_indexes {
            self.inner.insert_index_unchecked(index);
        }
        for name in overlay.dropped_indexes {
            self.inner.remove_index_unchecked(&name);
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
        catalog.inner.insert_table_unchecked(test_table("users", 1));

        let mut overlay = CatalogOverlay::new();
        overlay.drop_table("users");
        overlay.add_table(test_table("orders", 2));

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
        catalog.inner.insert_table_unchecked(test_table("users", 1));

        let mut overlay = CatalogOverlay::new();
        overlay.drop_table("users");

        PersistentCatalog::<alopex_core::kv::memory::MemoryKV>::discard_overlay(overlay);

        assert!(catalog.table_exists("users"));
    }
}
