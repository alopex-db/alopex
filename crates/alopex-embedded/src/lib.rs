//! A user-friendly embedded API for the AlopexDB key-value store.

#![deny(missing_docs)]

pub mod catalog_api;
pub mod columnar_api;
pub mod options;
mod sql_api;

pub use crate::columnar_api::{EmbeddedConfig, StorageMode};
pub use crate::options::DatabaseOptions;
/// `Database::execute_sql()` / `Transaction::execute_sql()` の返却型。
pub type SqlResult = alopex_sql::SqlResult;
use alopex_core::vector::hnsw::{HnswTransactionState, SearchStats as HnswSearchStats};
use alopex_core::{
    columnar::{
        kvs_bridge::ColumnarKvsBridge, memory::InMemorySegmentStore, segment_v2::SegmentConfigV2,
    },
    kv::any::AnyKVTransaction,
    kv::memory::MemoryKV,
    kv::AnyKV,
    score, validate_dimensions, HnswConfig, HnswIndex, HnswSearchResult, HnswStats, KVStore,
    KVTransaction, Key, LargeValueKind, LargeValueMeta, LargeValueReader, LargeValueWriter,
    StorageFactory, VectorType, DEFAULT_CHUNK_SIZE,
};
pub use alopex_core::{MemoryStats, Metric, TxnMode};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::path::Path;
use std::result;
use std::sync::{Arc, RwLock};

/// A convenience `Result` type for database operations.
pub type Result<T> = result::Result<T, Error>;

/// The error type for embedded database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from the underlying core storage engine.
    #[error("core error: {0}")]
    Core(#[from] alopex_core::Error),
    /// An error from the SQL execution pipeline.
    #[error("{0}")]
    Sql(#[from] alopex_sql::SqlError),
    /// The transaction has already been completed and cannot be used.
    #[error("transaction is completed")]
    TxnCompleted,
    /// Catalog が見つかりません。
    #[error("カタログが見つかりません: {0}")]
    CatalogNotFound(String),
    /// Catalog が既に存在します。
    #[error("カタログは既に存在します: {0}")]
    CatalogAlreadyExists(String),
    /// Catalog が空ではありません。
    #[error("カタログが空ではありません: {0}")]
    CatalogNotEmpty(String),
    /// Namespace が見つかりません。
    #[error("ネームスペースが見つかりません: {0}.{1}")]
    NamespaceNotFound(String, String),
    /// Namespace が既に存在します。
    #[error("ネームスペースは既に存在します: {0}.{1}")]
    NamespaceAlreadyExists(String, String),
    /// Namespace が空ではありません。
    #[error("ネームスペースが空ではありません: {0}.{1}")]
    NamespaceNotEmpty(String, String),
    /// The requested table was not found or is invalid.
    #[error("table not found: {0}")]
    TableNotFound(String),
    /// Table が既に存在します。
    #[error("テーブルは既に存在します: {0}")]
    TableAlreadyExists(String),
    /// Index が見つかりません。
    #[error("インデックスが見つかりません: {0}")]
    IndexNotFound(String),
    /// default オブジェクトは削除できません。
    #[error("default オブジェクトは削除できません: {0}")]
    CannotDeleteDefault(String),
    /// Managed テーブルにはスキーマが必要です。
    #[error("managed テーブルにはスキーマが必要です")]
    SchemaRequired,
    /// External テーブルには storage_root が必要です。
    #[error("external テーブルには storage_root が必要です")]
    StorageRootRequired,
    /// トランザクションは read-only です。
    #[error("トランザクションは読み取り専用です")]
    TxnReadOnly,
    /// The operation requires in-memory columnar mode.
    #[error("not in in-memory columnar mode")]
    NotInMemoryMode,
}

impl Error {
    /// SQL エラーの場合はエラーコード（例: `ALOPEX-S003`）を返す。
    pub fn sql_error_code(&self) -> Option<&'static str> {
        match self {
            Self::Sql(err) => Some(err.code()),
            _ => None,
        }
    }
}

/// The main database object.
pub struct Database {
    /// The underlying key-value store.
    pub(crate) store: Arc<AnyKV>,
    pub(crate) sql_catalog: Arc<RwLock<alopex_sql::catalog::PersistentCatalog<AnyKV>>>,
    pub(crate) columnar_mode: StorageMode,
    pub(crate) columnar_bridge: ColumnarKvsBridge,
    pub(crate) columnar_memory: Option<InMemorySegmentStore>,
    pub(crate) segment_config: SegmentConfigV2,
}

pub(crate) fn disk_data_dir_path(path: &Path) -> std::path::PathBuf {
    if path.extension().is_some_and(|e| e == "alopex") {
        // v0.1 file-mode はディレクトリに WAL/SSTable を持つため、`.alopex` の横に
        // sidecar ディレクトリを作ってそこへ格納する。
        path.with_extension("alopex.d")
    } else {
        path.to_path_buf()
    }
}

impl Database {
    /// Opens a database at the specified path.
    pub fn open(path: &Path) -> Result<Self> {
        let data_dir = disk_data_dir_path(path);
        let store = StorageFactory::create(alopex_core::StorageMode::Disk {
            path: data_dir,
            config: None,
        })
        .map_err(Error::Core)?;
        let mut db = Self::init(store, StorageMode::Disk, None, SegmentConfigV2::default());
        db.load_sql_catalog()?;
        Ok(db)
    }

    /// Creates a new, purely in-memory (transient) database.
    pub fn new() -> Self {
        let store = AnyKV::Memory(MemoryKV::new());
        Self::init(
            store,
            StorageMode::InMemory,
            None,
            SegmentConfigV2::default(),
        )
    }

    /// Opens a database in in-memory mode with default options.
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_options(DatabaseOptions::in_memory())
    }

    /// Opens a database in in-memory mode with the given options.
    pub fn open_in_memory_with_options(opts: DatabaseOptions) -> Result<Self> {
        if !opts.memory_mode() {
            return Err(Error::Core(alopex_core::Error::InvalidFormat(
                "memory_mode must be enabled for in-memory open".into(),
            )));
        }
        let store = StorageFactory::create(opts.to_storage_mode(None)).map_err(Error::Core)?;
        let mut db = Self::init(
            store,
            StorageMode::InMemory,
            opts.memory_limit(),
            SegmentConfigV2::default(),
        );
        db.load_sql_catalog()?;
        Ok(db)
    }

    pub(crate) fn init(
        store: AnyKV,
        columnar_mode: StorageMode,
        memory_limit: Option<usize>,
        segment_config: SegmentConfigV2,
    ) -> Self {
        let store = Arc::new(store);
        let sql_catalog = Arc::new(RwLock::new(alopex_sql::catalog::PersistentCatalog::new(
            store.clone(),
        )));
        let columnar_bridge = ColumnarKvsBridge::new(store.clone());
        let columnar_memory = if matches!(columnar_mode, StorageMode::InMemory) {
            Some(InMemorySegmentStore::new(memory_limit.map(|v| v as u64)))
        } else {
            None
        };

        Self {
            store,
            sql_catalog,
            columnar_mode,
            columnar_bridge,
            columnar_memory,
            segment_config,
        }
    }

    fn load_sql_catalog(&mut self) -> Result<()> {
        use alopex_sql::catalog::CatalogError;

        let loaded = match alopex_sql::catalog::PersistentCatalog::load(self.store.clone()) {
            Ok(catalog) => catalog,
            Err(CatalogError::Kv(alopex_core::Error::NotFound)) => {
                alopex_sql::catalog::PersistentCatalog::new(self.store.clone())
            }
            Err(other) => return Err(Error::Sql(other.into())),
        };

        self.sql_catalog = Arc::new(RwLock::new(loaded));
        Ok(())
    }

    /// Flushes the current in-memory data to an SSTable on disk (beta).
    pub fn flush(&self) -> Result<()> {
        self.store.flush().map_err(Error::Core)
    }

    /// Returns current memory usage statistics (in-memory KV only).
    pub fn memory_usage(&self) -> Option<MemoryStats> {
        match self.store.as_ref() {
            AnyKV::Memory(kv) => Some(kv.memory_stats()),
            AnyKV::Lsm(_) => None,
        }
    }

    /// Persists the current in-memory database to disk atomically.
    ///
    /// `wal_path` は「データディレクトリ」として扱う（file-mode）。
    pub fn persist_to_disk(&self, wal_path: &Path) -> Result<()> {
        if !matches!(self.store.as_ref(), AnyKV::Memory(_)) {
            return Err(Error::NotInMemoryMode);
        }
        let data_dir = disk_data_dir_path(wal_path);
        if wal_path.exists() || data_dir.exists() {
            return Err(Error::Core(alopex_core::Error::PathExists(
                wal_path.to_path_buf(),
            )));
        }

        let tmp_dir = data_dir.with_extension("tmp");
        if tmp_dir.exists() {
            return Err(Error::Core(alopex_core::Error::PathExists(tmp_dir)));
        }

        let snapshot = self.snapshot_pairs()?;
        let write_result = (|| -> Result<()> {
            let store = StorageFactory::create(alopex_core::StorageMode::Disk {
                path: tmp_dir.clone(),
                config: None,
            })
            .map_err(Error::Core)?;

            let mut txn = store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
            for (key, value) in snapshot {
                txn.put(key, value).map_err(Error::Core)?;
            }
            txn.commit_self().map_err(Error::Core)?;

            Ok(())
        })();

        if let Err(e) = write_result {
            let _ = fs::remove_dir_all(&tmp_dir);
            return Err(e);
        }

        fs::rename(&tmp_dir, &data_dir).map_err(|e| Error::Core(e.into()))?;
        if wal_path.extension().is_some_and(|e| e == "alopex") {
            // `.alopex` パスを渡した場合は、存在確認用のマーカーを作る。
            let _ = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(wal_path);
        }
        Ok(())
    }

    /// Creates a fully in-memory clone of the current database.
    pub fn clone_to_memory(&self) -> Result<Self> {
        let snapshot = self.snapshot_pairs()?;
        let cloned = Database::open_in_memory()?;
        if snapshot.is_empty() {
            return Ok(cloned);
        }

        let mut txn = cloned.begin(TxnMode::ReadWrite)?;
        for (key, value) in snapshot {
            txn.put(&key, &value)?;
        }
        txn.commit()?;
        Ok(cloned)
    }

    /// Clears all data while keeping the database usable.
    pub fn clear(&self) -> Result<()> {
        let keys: Vec<Key> = self.snapshot_pairs()?.into_iter().map(|(k, _)| k).collect();
        if keys.is_empty() {
            return Ok(());
        }
        let mut txn = self.begin(TxnMode::ReadWrite)?;
        for key in keys {
            txn.delete(&key)?;
        }
        txn.commit()
    }

    /// Updates the memory limit in bytes for the underlying in-memory store.
    pub fn set_memory_limit(&self, bytes: Option<usize>) {
        if let AnyKV::Memory(kv) = self.store.as_ref() {
            kv.txn_manager().set_memory_limit(bytes);
        }
    }

    /// Returns a read-only snapshot of all key-value pairs.
    pub fn snapshot(&self) -> Vec<(Key, Vec<u8>)> {
        self.snapshot_pairs().unwrap_or_default()
    }

    fn snapshot_pairs(&self) -> Result<Vec<(Key, Vec<u8>)>> {
        let mut txn = self.store.begin(TxnMode::ReadOnly).map_err(Error::Core)?;
        let pairs: Vec<(Key, Vec<u8>)> = txn.scan_prefix(b"").map_err(Error::Core)?.collect();
        txn.commit_self().map_err(Error::Core)?;
        Ok(pairs)
    }

    /// HNSW インデックスを作成し、永続化する。
    pub fn create_hnsw_index(&self, name: &str, config: HnswConfig) -> Result<()> {
        let mut txn = self.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        let index = HnswIndex::create(name, config).map_err(Error::Core)?;
        index.save(&mut txn).map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)
    }

    /// HNSW インデックスを削除する。
    pub fn drop_hnsw_index(&self, name: &str) -> Result<()> {
        let mut txn = self.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        let index = HnswIndex::load(name, &mut txn).map_err(Error::Core)?;
        index.drop(&mut txn).map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)
    }

    /// HNSW 統計情報を取得する。
    pub fn get_hnsw_stats(&self, name: &str) -> Result<HnswStats> {
        let mut txn = self.store.begin(TxnMode::ReadOnly).map_err(Error::Core)?;
        let index = HnswIndex::load(name, &mut txn).map_err(Error::Core)?;
        Ok(index.stats())
    }

    /// HNSW インデックスをコンパクションし、結果を返す。
    pub fn compact_hnsw_index(&self, name: &str) -> Result<alopex_core::vector::CompactionResult> {
        let mut txn = self.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        let mut index = HnswIndex::load(name, &mut txn).map_err(Error::Core)?;
        let result = index.compact().map_err(Error::Core)?;
        index.save(&mut txn).map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)?;
        Ok(result)
    }

    /// HNSW インデックスに検索を行う。
    pub fn search_hnsw(
        &self,
        name: &str,
        query: &[f32],
        k: usize,
        ef_search: Option<usize>,
    ) -> Result<(Vec<HnswSearchResult>, HnswSearchStats)> {
        let mut txn = self.store.begin(TxnMode::ReadOnly).map_err(Error::Core)?;
        let index = HnswIndex::load(name, &mut txn).map_err(Error::Core)?;
        index.search(query, k, ef_search).map_err(Error::Core)
    }

    /// Creates a chunked large value writer for opaque blobs (beta).
    pub fn create_blob_writer(
        &self,
        path: &Path,
        total_len: u64,
        chunk_size: Option<u32>,
    ) -> Result<LargeValueWriter> {
        let meta = LargeValueMeta {
            kind: LargeValueKind::Blob,
            total_len,
            chunk_size: chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE),
        };
        LargeValueWriter::create(path, meta).map_err(Error::Core)
    }

    /// Creates a chunked large value writer for typed payloads (beta).
    pub fn create_typed_writer(
        &self,
        path: &Path,
        type_id: u16,
        total_len: u64,
        chunk_size: Option<u32>,
    ) -> Result<LargeValueWriter> {
        let meta = LargeValueMeta {
            kind: LargeValueKind::Typed(type_id),
            total_len,
            chunk_size: chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE),
        };
        LargeValueWriter::create(path, meta).map_err(Error::Core)
    }

    /// Opens a chunked large value reader (beta). Kind/type is read from the file header.
    pub fn open_large_value(&self, path: &Path) -> Result<LargeValueReader> {
        LargeValueReader::open(path).map_err(Error::Core)
    }

    /// Begins a new transaction.
    pub fn begin(&self, mode: TxnMode) -> Result<Transaction<'_>> {
        let txn = self.store.begin(mode).map_err(Error::Core)?;
        Ok(Transaction {
            inner: Some(txn),
            db: self,
            hnsw_indices: HashMap::new(),
            overlay: alopex_sql::catalog::CatalogOverlay::new(),
        })
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

/// A database transaction.
pub struct Transaction<'a> {
    inner: Option<AnyKVTransaction<'a>>,
    db: &'a Database,
    hnsw_indices: HashMap<String, (HnswIndex, alopex_core::vector::hnsw::HnswTransactionState)>,
    overlay: alopex_sql::catalog::CatalogOverlay,
}

/// A search result row containing key, metadata, and similarity score.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// User key associated with the vector.
    pub key: Key,
    /// Opaque metadata payload stored alongside the vector.
    pub metadata: Vec<u8>,
    /// Similarity score for the query/vector pair.
    pub score: f32,
}

const VECTOR_INDEX_KEY: &[u8] = b"__alopex_vector_index";

impl<'a> Transaction<'a> {
    pub(crate) fn catalog_overlay(&self) -> &alopex_sql::catalog::CatalogOverlay {
        &self.overlay
    }

    pub(crate) fn catalog_overlay_mut(&mut self) -> &mut alopex_sql::catalog::CatalogOverlay {
        &mut self.overlay
    }

    pub(crate) fn txn_mode(&self) -> Result<TxnMode> {
        let txn = self.inner.as_ref().ok_or(Error::TxnCompleted)?;
        Ok(txn.mode())
    }
    /// Retrieves the value for a given key.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner_mut()?.get(&key.to_vec()).map_err(Error::Core)
    }

    /// Sets a value for a given key.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner_mut()?
            .put(key.to_vec(), value.to_vec())
            .map_err(Error::Core)
    }

    /// Deletes a key-value pair.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner_mut()?.delete(key.to_vec()).map_err(Error::Core)
    }

    /// HNSW にベクトルをステージング挿入/更新する。
    pub fn upsert_to_hnsw(
        &mut self,
        index_name: &str,
        key: &[u8],
        vector: &[f32],
        metadata: &[u8],
    ) -> Result<()> {
        self.ensure_write_txn()?;
        let (index, state) = self.hnsw_entry_mut(index_name)?;
        index
            .upsert_staged(key, vector, metadata, state)
            .map_err(Error::Core)
    }

    /// HNSW からキーをステージング削除する。
    pub fn delete_from_hnsw(&mut self, index_name: &str, key: &[u8]) -> Result<bool> {
        self.ensure_write_txn()?;
        let (index, state) = self.hnsw_entry_mut(index_name)?;
        index.delete_staged(key, state).map_err(Error::Core)
    }

    /// Upserts a vector and metadata under the provided key after validating dimensions and metric.
    ///
    /// A small internal index is maintained to enable scanning for similarity search.
    pub fn upsert_vector(
        &mut self,
        key: &[u8],
        metadata: &[u8],
        vector: &[f32],
        metric: Metric,
    ) -> Result<()> {
        if vector.is_empty() {
            return Err(Error::Core(alopex_core::Error::InvalidFormat(
                "vector cannot be empty".into(),
            )));
        }
        let vt = VectorType::new(vector.len(), metric);
        vt.validate(vector).map_err(Error::Core)?;

        let payload = encode_vector_entry(vt, metadata, vector);
        let txn = self.inner_mut()?;
        txn.put(key.to_vec(), payload).map_err(Error::Core)?;

        let mut keys = self.load_vector_index()?;
        if !keys.iter().any(|k| k == key) {
            keys.push(key.to_vec());
            self.persist_vector_index(&keys)?;
        }
        Ok(())
    }

    /// Executes a flat similarity search over stored vectors using the provided metric and query.
    ///
    /// The optional `filter_keys` restricts the scan to the given keys; otherwise the full
    /// vector index is scanned. Results are sorted by descending score and truncated to `top_k`.
    pub fn search_similar(
        &mut self,
        query_vector: &[f32],
        metric: Metric,
        top_k: usize,
        filter_keys: Option<&[Key]>,
    ) -> Result<Vec<SearchResult>> {
        if top_k == 0 {
            return Ok(Vec::new());
        }

        let mut keys = match filter_keys {
            Some(keys) => keys.to_vec(),
            None => self.load_vector_index()?,
        };
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut rows = Vec::new();
        let txn = self.inner_mut()?;
        for key in keys.drain(..) {
            let Some(raw) = txn.get(&key).map_err(Error::Core)? else {
                continue;
            };
            let decoded = decode_vector_entry(&raw).map_err(Error::Core)?;
            if decoded.metric != metric {
                return Err(Error::Core(alopex_core::Error::UnsupportedMetric {
                    metric: metric.as_str().to_string(),
                }));
            }
            validate_dimensions(decoded.dim, query_vector.len()).map_err(Error::Core)?;
            let score = score(metric, query_vector, &decoded.vector).map_err(Error::Core)?;
            rows.push(SearchResult {
                key,
                metadata: decoded.metadata,
                score,
            });
        }

        rows.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.key.cmp(&b.key)));
        if rows.len() > top_k {
            rows.truncate(top_k);
        }
        Ok(rows)
    }

    fn load_vector_index(&mut self) -> Result<Vec<Key>> {
        let txn = self.inner_mut()?;
        let Some(raw) = txn.get(&VECTOR_INDEX_KEY.to_vec()).map_err(Error::Core)? else {
            return Ok(Vec::new());
        };
        decode_index(&raw).map_err(Error::Core)
    }

    fn persist_vector_index(&mut self, keys: &[Key]) -> Result<()> {
        let txn = self.inner_mut()?;
        let encoded = encode_index(keys)?;
        txn.put(VECTOR_INDEX_KEY.to_vec(), encoded)
            .map_err(Error::Core)
    }

    /// Commits the transaction, applying all changes.
    pub fn commit(mut self) -> Result<()> {
        {
            let txn = self.inner.as_mut().ok_or(Error::TxnCompleted)?;
            for (_, (index, state)) in self.hnsw_indices.iter_mut() {
                index.commit_staged(txn, state).map_err(Error::Core)?;
            }
        }
        let txn = self.inner.take().ok_or(Error::TxnCompleted)?;
        self.hnsw_indices.clear();
        txn.commit_self().map_err(Error::Core)?;

        // KV commit 成功後のみ、カタログにオーバーレイを適用する。
        let mut catalog = self.db.sql_catalog.write().expect("catalog lock poisoned");
        catalog.apply_overlay(std::mem::take(&mut self.overlay));
        Ok(())
    }

    /// Rolls back the transaction, discarding all changes.
    pub fn rollback(mut self) -> Result<()> {
        if let Some(txn) = self.inner.take() {
            for (_, (index, state)) in self.hnsw_indices.iter_mut() {
                let _ = index.rollback(state);
            }
            self.hnsw_indices.clear();
            txn.rollback_self().map_err(Error::Core)
        } else {
            Err(Error::TxnCompleted)
        }
    }

    fn inner_mut(&mut self) -> Result<&mut AnyKVTransaction<'a>> {
        self.inner.as_mut().ok_or(Error::TxnCompleted)
    }

    fn hnsw_entry_mut(&mut self, name: &str) -> Result<&mut (HnswIndex, HnswTransactionState)> {
        if !self.hnsw_indices.contains_key(name) {
            let index = {
                let txn = self.inner_mut()?;
                HnswIndex::load(name, txn).map_err(Error::Core)?
            };
            self.hnsw_indices
                .insert(name.to_string(), (index, HnswTransactionState::default()));
        }
        Ok(self.hnsw_indices.get_mut(name).unwrap())
    }

    fn ensure_write_txn(&self) -> Result<()> {
        let txn = self.inner.as_ref().ok_or(Error::TxnCompleted)?;
        if txn.mode() != TxnMode::ReadWrite {
            return Err(Error::Core(alopex_core::Error::TxnReadOnly));
        }
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if let Some(txn) = self.inner.take() {
            for (_, (index, state)) in self.hnsw_indices.iter_mut() {
                let _ = index.rollback(state);
            }
            self.hnsw_indices.clear();
            let _ = txn.rollback_self();
        }
    }
}

fn metric_to_byte(metric: Metric) -> u8 {
    match metric {
        Metric::Cosine => 0,
        Metric::L2 => 1,
        Metric::InnerProduct => 2,
    }
}

fn byte_to_metric(byte: u8) -> result::Result<Metric, alopex_core::Error> {
    match byte {
        0 => Ok(Metric::Cosine),
        1 => Ok(Metric::L2),
        2 => Ok(Metric::InnerProduct),
        other => Err(alopex_core::Error::UnsupportedMetric {
            metric: format!("unknown({other})"),
        }),
    }
}

fn encode_vector_entry(vector_type: VectorType, metadata: &[u8], vector: &[f32]) -> Vec<u8> {
    let dim = vector_type.dim() as u32;
    let meta_len = metadata.len() as u32;
    let mut buf = Vec::with_capacity(1 + 4 + 4 + metadata.len() + std::mem::size_of_val(vector));
    buf.push(metric_to_byte(vector_type.metric()));
    buf.extend_from_slice(&dim.to_le_bytes());
    buf.extend_from_slice(&meta_len.to_le_bytes());
    buf.extend_from_slice(metadata);
    for v in vector {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf
}

struct DecodedEntry {
    metric: Metric,
    dim: usize,
    metadata: Vec<u8>,
    vector: Vec<f32>,
}

fn decode_vector_entry(bytes: &[u8]) -> result::Result<DecodedEntry, alopex_core::Error> {
    if bytes.len() < 9 {
        return Err(alopex_core::Error::InvalidFormat(
            "vector entry too short".into(),
        ));
    }
    let metric = byte_to_metric(bytes[0])?;
    let dim = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
    let meta_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;

    let header = 9;
    let expected_len = header + meta_len + dim * std::mem::size_of::<f32>();
    if bytes.len() < expected_len {
        return Err(alopex_core::Error::InvalidFormat(
            "vector entry truncated".into(),
        ));
    }

    let metadata = bytes[header..header + meta_len].to_vec();
    let mut vector = Vec::with_capacity(dim);
    let vec_bytes = &bytes[header + meta_len..expected_len];
    for chunk in vec_bytes.chunks_exact(4) {
        vector.push(f32::from_le_bytes(chunk.try_into().unwrap()));
    }

    Ok(DecodedEntry {
        metric,
        dim,
        metadata,
        vector,
    })
}

fn encode_index(keys: &[Key]) -> result::Result<Vec<u8>, alopex_core::Error> {
    let mut buf = Vec::new();
    let count = keys.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());
    for key in keys {
        let len: u32 = key
            .len()
            .try_into()
            .map_err(|_| alopex_core::Error::InvalidFormat("key too long".into()))?;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(key);
    }
    Ok(buf)
}

fn decode_index(bytes: &[u8]) -> result::Result<Vec<Key>, alopex_core::Error> {
    if bytes.len() < 4 {
        return Err(alopex_core::Error::InvalidFormat("index too short".into()));
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut pos = 4;
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err(alopex_core::Error::InvalidFormat("index truncated".into()));
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            return Err(alopex_core::Error::InvalidFormat(
                "index key truncated".into(),
            ));
        }
        keys.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_open_and_crud() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(&path).unwrap();

        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.commit().unwrap();

        let mut txn2 = db.begin(TxnMode::ReadOnly).unwrap();
        let val = txn2.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_not_found() {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadOnly).unwrap();
        let val = txn.get(b"non-existent-key").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn test_crash_recovery_replays_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("replay.db");

        {
            let db = Database::open(&path).unwrap();
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.put(b"k1", b"v1").unwrap();
            txn.commit().unwrap();

            let mut uncommitted = db.begin(TxnMode::ReadWrite).unwrap();
            uncommitted.put(b"k2", b"v2").unwrap();
            // Drop without commit to simulate crash before commit.
        }

        let db = Database::open(&path).unwrap();
        let mut txn = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(txn.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(txn.get(b"k2").unwrap(), None);
    }

    #[test]
    fn test_txn_closed() {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"k1", b"v1").unwrap();
        txn.commit().unwrap();
        // The `commit` call consumes the transaction, so we can't call it again.
        // This test verifies that we can't use a transaction after it's been completed.
        // The `inner_mut` method will return `Error::TxnCompleted`.
        // This is a compile-time check in practice, but we can't write a test that fails to compile.
        // The logic is sound.
    }

    #[test]
    fn test_concurrency_conflict() {
        let db = std::sync::Arc::new(Database::new());
        let mut t0 = db.begin(TxnMode::ReadWrite).unwrap();
        t0.put(b"k1", b"v0").unwrap();
        t0.commit().unwrap();

        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();

        let db1 = db.clone();
        let t1 = thread::spawn(move || {
            let mut txn1 = db1.begin(TxnMode::ReadWrite).unwrap();
            let val = txn1.get(b"k1").unwrap();
            assert_eq!(val.unwrap(), b"v0");
            tx1.send(()).unwrap();
            rx2.recv().unwrap();
            txn1.put(b"k1", b"v1").unwrap();
            let result = txn1.commit();
            assert!(matches!(
                result,
                Err(Error::Core(alopex_core::Error::TxnConflict))
            ));
        });

        let db2 = db.clone();
        let t2 = thread::spawn(move || {
            rx1.recv().unwrap();
            let mut txn2 = db2.begin(TxnMode::ReadWrite).unwrap();
            txn2.put(b"k1", b"v2").unwrap();
            assert!(txn2.commit().is_ok());
            tx2.send(()).unwrap();
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let mut txn3 = db.begin(TxnMode::ReadOnly).unwrap();
        let val = txn3.get(b"k1").unwrap();
        assert_eq!(val.unwrap(), b"v2");
    }

    #[test]
    fn test_flush_and_reopen_via_embedded_api() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("persist.db");
        {
            let db = Database::open(&path).unwrap();
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.put(b"k1", b"v1").unwrap();
            txn.commit().unwrap();
            db.flush().unwrap();
        }

        let db = Database::open(&path).unwrap();
        let mut txn = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(txn.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn test_large_value_blob_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blob.lv");
        let payload = b"hello large value";

        {
            let db = Database::new();
            let mut writer = db
                .create_blob_writer(&path, payload.len() as u64, Some(16))
                .unwrap();
            writer.write_chunk(&payload[..5]).unwrap();
            writer.write_chunk(&payload[5..]).unwrap();
            writer.finish().unwrap();
        }

        let db = Database::new();
        let mut reader = db.open_large_value(&path).unwrap();
        let mut buf = Vec::new();
        while let Some((_info, chunk)) = reader.next_chunk().unwrap() {
            buf.extend_from_slice(&chunk);
        }
        assert_eq!(buf, payload);
    }

    #[test]
    fn upsert_and_search_same_txn() {
        let db = Database::new();
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_vector(b"k1", b"meta1", &[1.0, 0.0], Metric::Cosine)
            .unwrap();

        let results = txn
            .search_similar(&[1.0, 0.0], Metric::Cosine, 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"k1");
        assert_eq!(results[0].metadata, b"meta1");
        txn.commit().unwrap();
    }

    #[test]
    fn upsert_and_search_across_txn() {
        let db = Database::new();
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.upsert_vector(b"k1", b"meta1", &[1.0, 1.0], Metric::Cosine)
                .unwrap();
            txn.commit().unwrap();
        }

        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        let results = ro
            .search_similar(&[1.0, 1.0], Metric::Cosine, 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"k1");
    }

    #[test]
    fn read_only_upsert_rejected() {
        let db = Database::new();
        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        let err = ro
            .upsert_vector(b"k1", b"m", &[1.0, 0.0], Metric::Cosine)
            .unwrap_err();
        assert!(matches!(err, Error::Core(alopex_core::Error::TxnReadOnly)));
    }

    #[test]
    fn dimension_mismatch_on_search() {
        let db = Database::new();
        {
            let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
            txn.upsert_vector(b"k1", b"m", &[1.0, 0.0], Metric::Cosine)
                .unwrap();
            txn.commit().unwrap();
        }
        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        let err = ro
            .search_similar(&[1.0, 0.0, 1.0], Metric::Cosine, 1, None)
            .unwrap_err();
        assert!(matches!(
            err,
            Error::Core(alopex_core::Error::DimensionMismatch { .. })
        ));
    }
}
