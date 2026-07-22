//! カラムナーストレージの埋め込み API 拡張。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::encoding_v2::Bitmap;
use alopex_core::columnar::kvs_bridge::{ColumnarKvsBridge, StreamingSegmentLayoutPreflightV08};
use alopex_core::columnar::segment_v08::ChunkedSegmentAccessV08;
use alopex_core::columnar::segment_v2::{
    RecordBatch as CoreRecordBatch, Schema as CoreSchema, SegmentWriterV2,
};
use alopex_core::storage::format::AlopexFileWriter;
use alopex_core::{StorageFactory, StorageMode as CoreStorageMode};
use alopex_dataframe::io::ColumnarSegmentBatchSourceFactory;
use alopex_dataframe::physical::{
    ColumnarSegmentBatchPlan, ColumnarSegmentCursor, ColumnarSegmentProvider, DataFrameStream,
    PlanSubject, SourceLimit,
};
use alopex_dataframe::{DataFrameError, Result as DataFrameResult};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryBuilder, Float32Array, Float64Array,
    Int64Array,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;

use crate::{Database, Error, Result, SegmentConfigV2, Transaction, TxnMode};

#[cfg(test)]
thread_local! {
    static LAST_READ_COLUMN_INDICES: std::cell::RefCell<Option<Vec<usize>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn record_read_column_indices(indices: &[usize]) {
    LAST_READ_COLUMN_INDICES.with(|last| {
        *last.borrow_mut() = Some(indices.to_vec());
    });
}

/// セグメント統計情報。
#[derive(Debug, Clone)]
pub struct ColumnarSegmentStats {
    /// セグメント内の行数。
    pub row_count: usize,
    /// セグメント内のカラム数。
    pub column_count: usize,
    /// セグメントのサイズ（バイト）。
    pub size_bytes: usize,
}

/// カラムナーインデックス種別。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarIndexType {
    /// 最小値/最大値インデックス。
    Minmax,
    /// Bloom フィルタインデックス。
    Bloom,
}

impl ColumnarIndexType {
    /// 文字列表現を返す。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Minmax => "minmax",
            Self::Bloom => "bloom",
        }
    }
}

/// カラムナーインデックス情報。
#[derive(Debug, Clone)]
pub struct ColumnarIndexInfo {
    /// 対象カラム名。
    pub column: String,
    /// インデックス種別。
    pub index_type: ColumnarIndexType,
}

/// カラムナー関連設定。
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    /// データパス（Disk モード時に必須）。
    pub path: Option<PathBuf>,
    /// カラムナーストレージモード。
    pub storage_mode: StorageMode,
    /// InMemory モードのメモリ上限（バイト）。
    pub memory_limit: Option<usize>,
    /// セグメント設定。
    pub segment_config: SegmentConfigV2,
}

impl EmbeddedConfig {
    /// ディスクモードで初期化。
    pub fn disk(path: PathBuf) -> Self {
        Self {
            path: Some(path),
            storage_mode: StorageMode::Disk,
            memory_limit: None,
            segment_config: SegmentConfigV2::default(),
        }
    }

    /// インメモリモードで初期化（無制限）。
    pub fn in_memory() -> Self {
        Self {
            path: None,
            storage_mode: StorageMode::InMemory,
            memory_limit: None,
            segment_config: SegmentConfigV2::default(),
        }
    }

    /// インメモリモードでメモリ上限を設定。
    pub fn in_memory_with_limit(limit: usize) -> Self {
        Self {
            path: None,
            storage_mode: StorageMode::InMemory,
            memory_limit: Some(limit),
            segment_config: SegmentConfigV2::default(),
        }
    }

    /// セグメント設定を上書き。
    pub fn with_segment_config(mut self, cfg: SegmentConfigV2) -> Self {
        self.segment_config = cfg;
        self
    }
}

/// カラムナー用ストレージモード。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    /// KVS 経由でディスク永続化。
    Disk,
    /// 完全インメモリ保持。
    InMemory,
}

impl Database {
    /// 構成付きでデータベースを開く（カラムナー機能を初期化）。
    pub fn open_with_config(config: EmbeddedConfig) -> Result<Self> {
        let store = match config.storage_mode {
            StorageMode::Disk => {
                let path = config.path.clone().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "disk mode requires a path".into(),
                    ))
                })?;
                let path = crate::disk_data_dir_path(&path);
                StorageFactory::create(CoreStorageMode::Disk { path, config: None })
                    .map_err(Error::Core)?
            }
            StorageMode::InMemory => StorageFactory::create(CoreStorageMode::Memory {
                max_size: config.memory_limit,
            })
            .map_err(Error::Core)?,
        };

        Ok(Self::init(
            store,
            config.storage_mode,
            config.memory_limit,
            config.segment_config,
        ))
    }

    /// 現在のカラムナーストレージモードを返す。
    pub fn storage_mode(&self) -> StorageMode {
        self.columnar_mode
    }

    /// カラムナーセグメントを書き込む。
    pub fn write_columnar_segment(&self, table: &str, batch: CoreRecordBatch) -> Result<u64> {
        let mut writer = SegmentWriterV2::new(self.segment_config.clone());
        writer
            .write_batch(batch)
            .map_err(|e| Error::Core(e.into()))?;
        let segment = writer.finish().map_err(|e| Error::Core(e.into()))?;
        let table_id = table_id(table)?;

        match self.columnar_mode {
            StorageMode::Disk => self
                .columnar_bridge
                .write_segment(table_id, &segment)
                .map_err(|e| Error::Core(e.into())),
            StorageMode::InMemory => {
                let store = self.columnar_memory.as_ref().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?;
                store
                    .write_segment(table_id, segment)
                    .map_err(|e| Error::Core(e.into()))
            }
        }
    }

    /// カラムナーセグメントを書き込む（構成上書き）。
    pub fn write_columnar_segment_with_config(
        &self,
        table: &str,
        batch: CoreRecordBatch,
        config: SegmentConfigV2,
    ) -> Result<u64> {
        let mut writer = SegmentWriterV2::new(config);
        writer
            .write_batch(batch)
            .map_err(|e| Error::Core(e.into()))?;
        let segment = writer.finish().map_err(|e| Error::Core(e.into()))?;
        let table_id = table_id(table)?;

        match self.columnar_mode {
            StorageMode::Disk => self
                .columnar_bridge
                .write_segment(table_id, &segment)
                .map_err(|e| Error::Core(e.into())),
            StorageMode::InMemory => {
                let store = self.columnar_memory.as_ref().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?;
                store
                    .write_segment(table_id, segment)
                    .map_err(|e| Error::Core(e.into()))
            }
        }
    }

    /// カラムナーセグメントを読み取る（カラム名指定オプション付き）。
    pub fn read_columnar_segment(
        &self,
        table: &str,
        segment_id: u64,
        columns: Option<&[&str]>,
    ) -> Result<Vec<CoreRecordBatch>> {
        let table_id = table_id(table)?;
        match self.columnar_mode {
            StorageMode::Disk => {
                let read_indices: Vec<usize> = if let Some(names) = columns {
                    let segment = self
                        .columnar_bridge
                        .read_segment_raw(table_id, segment_id)
                        .map_err(|e| Error::Core(e.into()))?;
                    resolve_indices_from_schema(&segment.meta.schema, names)?
                } else {
                    let column_count = self
                        .columnar_bridge
                        .column_count(table_id, segment_id)
                        .map_err(|e| Error::Core(e.into()))?;
                    (0..column_count).collect()
                };

                #[cfg(test)]
                record_read_column_indices(&read_indices);

                self.columnar_bridge
                    .read_segment(table_id, segment_id, &read_indices)
                    .map_err(|e| Error::Core(e.into()))
            }
            StorageMode::InMemory => {
                let store = self.columnar_memory.as_ref().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?;
                let read_indices: Vec<usize> = if let Some(names) = columns {
                    let schema = store
                        .schema(table_id, segment_id)
                        .map_err(|e| Error::Core(e.into()))?;
                    resolve_indices_from_schema(&schema, names)?
                } else {
                    let column_count = store
                        .column_count(table_id, segment_id)
                        .map_err(|e| Error::Core(e.into()))?;
                    (0..column_count).collect()
                };

                #[cfg(test)]
                record_read_column_indices(&read_indices);

                store
                    .read_segment(table_id, segment_id, &read_indices)
                    .map_err(|e| Error::Core(e.into()))
            }
        }
    }

    /// InMemory モード時のメモリ使用量を返す。Disk モードでは None。
    pub fn in_memory_usage(&self) -> Option<u64> {
        if self.columnar_mode == StorageMode::InMemory {
            self.columnar_memory.as_ref().map(|m| m.memory_usage())
        } else {
            None
        }
    }

    /// メモリ上限付きでインメモリ DB を開く。
    pub fn open_in_memory_with_limit(limit: usize) -> Result<Self> {
        Self::open_with_config(EmbeddedConfig::in_memory_with_limit(limit))
    }

    /// テーブル名から内部 ID を解決する。
    pub fn resolve_table_id(&self, table: &str) -> Result<u32> {
        table_id(table)
    }

    /// Builds a bounded DataFrame factory for a provisioned V08 columnar segment.
    ///
    /// Legacy V2 segment blobs and in-memory columnar storage deliberately
    /// return `requires_v08_chunked_layout`; neither is converted into an
    /// artificial streaming source.
    pub fn columnar_segment_streaming_factory_v08(
        &self,
        table: &str,
        segment_id: u64,
    ) -> Result<ColumnarSegmentBatchSourceFactory> {
        let table_id = table_id(table)?;
        self.columnar_segment_streaming_factory_v08_by_ids(table_id, segment_id)
    }

    /// Builds a bounded DataFrame factory from the canonical `{table_id}:{segment_id}` handle.
    ///
    /// This is the same local V08-only path as [`Self::columnar_segment_streaming_factory_v08`],
    /// but preserves the public segment identity used by the catalog and Python `LocalScan`
    /// without requiring a lossy reverse lookup of a table name.
    pub fn columnar_segment_streaming_factory_v08_by_id(
        &self,
        segment_id: &str,
    ) -> Result<ColumnarSegmentBatchSourceFactory> {
        let (table_id, segment_id) = parse_segment_id(segment_id)?;
        self.columnar_segment_streaming_factory_v08_by_ids(table_id, segment_id)
    }

    fn columnar_segment_streaming_factory_v08_by_ids(
        &self,
        table_id: u32,
        segment_id: u64,
    ) -> Result<ColumnarSegmentBatchSourceFactory> {
        match self.columnar_mode {
            StorageMode::Disk => match self
                .columnar_bridge
                .streaming_layout_preflight_v08(table_id, segment_id)
                .map_err(|error| Error::DataFrame(map_v08_error(error)))?
            {
                StreamingSegmentLayoutPreflightV08::Available => Ok(
                    ColumnarSegmentBatchSourceFactory::new(Arc::new(EmbeddedV08SegmentProvider {
                        bridge: self.columnar_bridge.clone(),
                        table_id,
                        segment_id,
                    })),
                ),
                StreamingSegmentLayoutPreflightV08::RequiresV08ChunkedLayout => {
                    Err(Error::DataFrame(DataFrameError::streaming_unsupported(
                        "columnar_segment_scan",
                        "requires_v08_chunked_layout",
                    )))
                }
                StreamingSegmentLayoutPreflightV08::NotFound => {
                    Err(Error::DataFrame(DataFrameError::streaming_unsupported(
                        "columnar_segment_scan",
                        "v08_segment_not_found",
                    )))
                }
            },
            StorageMode::InMemory => Err(Error::DataFrame(DataFrameError::streaming_unsupported(
                "columnar_segment_scan",
                "requires_v08_chunked_layout",
            ))),
        }
    }

    /// Opens a bounded `DataFrameStream` for a provisioned V08 columnar segment.
    pub fn stream_columnar_segment_v08(
        &self,
        table: &str,
        segment_id: u64,
        options: alopex_dataframe::physical::budget::StreamOptions,
    ) -> Result<DataFrameStream> {
        let factory = self.columnar_segment_streaming_factory_v08(table, segment_id)?;
        DataFrameStream::from_factory(&factory, options).map_err(Error::DataFrame)
    }

    /// Opens a bounded V08 segment stream from the canonical `{table_id}:{segment_id}` handle.
    pub fn stream_columnar_segment_v08_by_id(
        &self,
        segment_id: &str,
        options: alopex_dataframe::physical::budget::StreamOptions,
    ) -> Result<DataFrameStream> {
        let factory = self.columnar_segment_streaming_factory_v08_by_id(segment_id)?;
        DataFrameStream::from_factory(&factory, options).map_err(Error::DataFrame)
    }

    /// Scan a columnar segment by string ID.
    ///
    /// The segment ID format is `{table_id}:{segment_id}` (e.g., "12345:1").
    /// Returns rows as a vector of SqlValue vectors.
    pub fn scan_columnar_segment(
        &self,
        segment_id: &str,
    ) -> Result<Vec<Vec<alopex_sql::SqlValue>>> {
        let (table_id, seg_id) = parse_segment_id(segment_id)?;
        let all_indices: Vec<usize> = match self.columnar_mode {
            StorageMode::Disk => {
                let count = self
                    .columnar_bridge
                    .column_count(table_id, seg_id)
                    .map_err(|e| Error::Core(e.into()))?;
                (0..count).collect()
            }
            StorageMode::InMemory => {
                let store = self.columnar_memory.as_ref().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?;
                let count = store
                    .column_count(table_id, seg_id)
                    .map_err(|e| Error::Core(e.into()))?;
                (0..count).collect()
            }
        };

        let batches = match self.columnar_mode {
            StorageMode::Disk => self
                .columnar_bridge
                .read_segment(table_id, seg_id, &all_indices)
                .map_err(|e| Error::Core(e.into()))?,
            StorageMode::InMemory => self
                .columnar_memory
                .as_ref()
                .ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?
                .read_segment(table_id, seg_id, &all_indices)
                .map_err(|e| Error::Core(e.into()))?,
        };

        // Convert RecordBatch to Vec<Vec<SqlValue>>
        let mut rows = Vec::new();
        for batch in batches {
            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(batch.columns.len());
                for col in &batch.columns {
                    let sql_val = column_value_to_sql_value(col, row_idx);
                    row.push(sql_val);
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Scan a columnar segment by string ID, returning RecordBatches for streaming (FR-7).
    ///
    /// This method returns raw `RecordBatch` objects, allowing the caller to iterate
    /// over rows without materializing all data upfront. Use this for large datasets
    /// where streaming is required.
    ///
    /// The segment ID format is `{table_id}:{segment_id}` (e.g., "12345:1").
    pub fn scan_columnar_segment_batches(&self, segment_id: &str) -> Result<Vec<CoreRecordBatch>> {
        let (table_id, seg_id) = parse_segment_id(segment_id)?;
        let all_indices: Vec<usize> = match self.columnar_mode {
            StorageMode::Disk => {
                let count = self
                    .columnar_bridge
                    .column_count(table_id, seg_id)
                    .map_err(|e| Error::Core(e.into()))?;
                (0..count).collect()
            }
            StorageMode::InMemory => {
                let store = self.columnar_memory.as_ref().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?;
                let count = store
                    .column_count(table_id, seg_id)
                    .map_err(|e| Error::Core(e.into()))?;
                (0..count).collect()
            }
        };

        match self.columnar_mode {
            StorageMode::Disk => self
                .columnar_bridge
                .read_segment(table_id, seg_id, &all_indices)
                .map_err(|e| Error::Core(e.into())),
            StorageMode::InMemory => self
                .columnar_memory
                .as_ref()
                .ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?
                .read_segment(table_id, seg_id, &all_indices)
                .map_err(|e| Error::Core(e.into())),
        }
    }

    /// Create a streaming row iterator over a columnar segment (FR-7).
    ///
    /// This returns a `ColumnarRowIterator` that yields rows one at a time from
    /// the underlying RecordBatches, without materializing all rows upfront.
    ///
    /// The segment ID format is `{table_id}:{segment_id}` (e.g., "12345:1").
    pub fn scan_columnar_segment_streaming(&self, segment_id: &str) -> Result<ColumnarRowIterator> {
        let batches = self.scan_columnar_segment_batches(segment_id)?;
        Ok(ColumnarRowIterator::new(batches))
    }

    /// Get statistics for a columnar segment by string ID.
    ///
    /// The segment ID format is `{table_id}:{segment_id}` (e.g., "12345:1").
    pub fn get_columnar_segment_stats(&self, segment_id: &str) -> Result<ColumnarSegmentStats> {
        let (table_id, seg_id) = parse_segment_id(segment_id)?;

        match self.columnar_mode {
            StorageMode::Disk => {
                let column_count = self
                    .columnar_bridge
                    .column_count(table_id, seg_id)
                    .map_err(|e| Error::Core(e.into()))?;
                let batches = self
                    .columnar_bridge
                    .read_segment(table_id, seg_id, &(0..column_count).collect::<Vec<_>>())
                    .map_err(|e| Error::Core(e.into()))?;
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

                Ok(ColumnarSegmentStats {
                    row_count,
                    column_count,
                    size_bytes: 0, // Size not available in current implementation
                })
            }
            StorageMode::InMemory => {
                let store = self.columnar_memory.as_ref().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?;
                let column_count = store
                    .column_count(table_id, seg_id)
                    .map_err(|e| Error::Core(e.into()))?;
                let batches = store
                    .read_segment(table_id, seg_id, &(0..column_count).collect::<Vec<_>>())
                    .map_err(|e| Error::Core(e.into()))?;
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

                Ok(ColumnarSegmentStats {
                    row_count,
                    column_count,
                    size_bytes: 0, // Size not available in current implementation
                })
            }
        }
    }

    /// List all columnar segments.
    ///
    /// Returns segment IDs in the format `{table_id}:{segment_id}`.
    pub fn list_columnar_segments(&self) -> Result<Vec<String>> {
        match self.columnar_mode {
            StorageMode::Disk => {
                let segments = self
                    .columnar_bridge
                    .list_segments()
                    .map_err(|e| Error::Core(e.into()))?;
                Ok(segments
                    .into_iter()
                    .map(|(table_id, seg_id)| format!("{}:{}", table_id, seg_id))
                    .collect())
            }
            StorageMode::InMemory => {
                let store = self.columnar_memory.as_ref().ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?;
                let segments = store.list_segments();
                Ok(segments
                    .into_iter()
                    .map(|(table_id, seg_id)| format!("{}:{}", table_id, seg_id))
                    .collect())
            }
        }
    }

    /// Create a columnar index for a segment/column.
    pub fn create_columnar_index(
        &self,
        segment_id: &str,
        column: &str,
        index_type: ColumnarIndexType,
    ) -> Result<()> {
        let _ = self.get_columnar_segment_stats(segment_id)?;
        let key = columnar_index_key(segment_id, column);
        let value = index_type.as_str().as_bytes().to_vec();
        let mut txn = self.begin(TxnMode::ReadWrite)?;
        txn.put(&key, &value)?;
        txn.commit()?;
        Ok(())
    }

    /// List columnar indexes for a segment.
    pub fn list_columnar_indexes(&self, segment_id: &str) -> Result<Vec<ColumnarIndexInfo>> {
        let _ = self.get_columnar_segment_stats(segment_id)?;
        let prefix = columnar_index_prefix(segment_id);
        let mut txn = self.begin(TxnMode::ReadOnly)?;
        let mut entries = Vec::new();
        for (key, value) in txn.scan_prefix(&prefix)? {
            let column = parse_index_column(segment_id, &key)?;
            let index_type = parse_index_type(&value)?;
            entries.push(ColumnarIndexInfo { column, index_type });
        }
        txn.commit()?;
        Ok(entries)
    }

    /// Drop a columnar index for a segment/column.
    pub fn drop_columnar_index(&self, segment_id: &str, column: &str) -> Result<()> {
        let _ = self.get_columnar_segment_stats(segment_id)?;
        let key = columnar_index_key(segment_id, column);
        let mut txn = self.begin(TxnMode::ReadWrite)?;
        let exists = txn.get(&key)?.is_some();
        if !exists {
            txn.rollback()?;
            return Err(Error::IndexNotFound(format!(
                "columnar index {}:{}",
                segment_id, column
            )));
        }
        txn.delete(&key)?;
        txn.commit()?;
        Ok(())
    }

    /// InMemory モードのセグメントをファイルへフラッシュする。
    pub fn flush_in_memory_segment_to_file(
        &self,
        table: &str,
        segment_id: u64,
        path: &Path,
    ) -> Result<()> {
        let store = self
            .columnar_memory
            .as_ref()
            .ok_or(Error::NotInMemoryMode)?;
        let table_id = table_id(table)?;
        store
            .flush_to_segment_file(table_id, segment_id, path)
            .map_err(|e| Error::Core(e.into()))
    }

    /// InMemory モードのセグメントを KVS へフラッシュする。
    pub fn flush_in_memory_segment_to_kvs(&self, table: &str, segment_id: u64) -> Result<u64> {
        let store = self
            .columnar_memory
            .as_ref()
            .ok_or(Error::NotInMemoryMode)?;
        let table_id = table_id(table)?;
        store
            .flush_to_kvs(table_id, segment_id, &self.columnar_bridge)
            .map_err(|e| Error::Core(e.into()))
    }

    /// InMemory モードのセグメントを `.alopex` ファイルへフラッシュする。
    pub fn flush_in_memory_segment_to_alopex(
        &self,
        table: &str,
        segment_id: u64,
        writer: &mut AlopexFileWriter,
    ) -> Result<u32> {
        let store = self
            .columnar_memory
            .as_ref()
            .ok_or(Error::NotInMemoryMode)?;
        let table_id = table_id(table)?;
        store
            .flush_to_alopex(table_id, segment_id, writer)
            .map_err(|e| Error::Core(e.into()))
    }
}

impl<'a> Transaction<'a> {
    /// 現在のカラムナーストレージモードを返す。
    pub fn storage_mode(&self) -> StorageMode {
        self.db.storage_mode()
    }

    /// カラムナーセグメントを書き込む（トランザクションコンテキスト利用）。
    pub fn write_columnar_segment(&self, table: &str, batch: CoreRecordBatch) -> Result<u64> {
        self.db.write_columnar_segment(table, batch)
    }

    /// カラムナーセグメントを読み取る（トランザクションコンテキスト利用）。
    pub fn read_columnar_segment(
        &self,
        table: &str,
        segment_id: u64,
        columns: Option<&[&str]>,
    ) -> Result<Vec<CoreRecordBatch>> {
        self.db.read_columnar_segment(table, segment_id, columns)
    }
}

fn table_id(table: &str) -> Result<u32> {
    if table.is_empty() {
        return Err(Error::TableNotFound("table name is empty".into()));
    }
    let mut hasher = DefaultHasher::new();
    table.hash(&mut hasher);
    Ok((hasher.finish() & 0xffff_ffff) as u32)
}

fn resolve_indices_from_schema(
    schema: &alopex_core::columnar::segment_v2::Schema,
    names: &[&str],
) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        let pos = schema
            .columns
            .iter()
            .position(|c| c.name == *name)
            .ok_or_else(|| {
                Error::Core(alopex_core::Error::InvalidFormat(format!(
                    "column not found: {name}"
                )))
            })?;
        indices.push(pos);
    }
    Ok(indices)
}

const COLUMNAR_INDEX_PREFIX: &str = "__alopex_columnar_index__:";

fn columnar_index_key(segment: &str, column: &str) -> Vec<u8> {
    let mut key =
        String::with_capacity(COLUMNAR_INDEX_PREFIX.len() + segment.len() + column.len() + 1);
    key.push_str(COLUMNAR_INDEX_PREFIX);
    key.push_str(segment);
    key.push(':');
    key.push_str(column);
    key.into_bytes()
}

fn columnar_index_prefix(segment: &str) -> Vec<u8> {
    let mut key = String::with_capacity(COLUMNAR_INDEX_PREFIX.len() + segment.len() + 1);
    key.push_str(COLUMNAR_INDEX_PREFIX);
    key.push_str(segment);
    key.push(':');
    key.into_bytes()
}

fn parse_index_column(segment: &str, key: &[u8]) -> Result<String> {
    let prefix = columnar_index_prefix(segment);
    if !key.starts_with(&prefix) {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(
            "columnar index key is invalid".into(),
        )));
    }
    let suffix = &key[prefix.len()..];
    String::from_utf8(suffix.to_vec()).map_err(|_| {
        Error::Core(alopex_core::Error::InvalidFormat(
            "columnar index column is not valid UTF-8".into(),
        ))
    })
}

fn parse_index_type(raw: &[u8]) -> Result<ColumnarIndexType> {
    let value = std::str::from_utf8(raw).map_err(|_| {
        Error::Core(alopex_core::Error::InvalidFormat(
            "columnar index type is not valid UTF-8".into(),
        ))
    })?;
    match value {
        "minmax" => Ok(ColumnarIndexType::Minmax),
        "bloom" => Ok(ColumnarIndexType::Bloom),
        other => Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "unknown columnar index type: {other}"
        )))),
    }
}

/// セグメントID文字列をパースする。
///
/// フォーマット: `{table_id}:{segment_id}` (例: "12345:1")
fn parse_segment_id(segment_id: &str) -> Result<(u32, u64)> {
    let parts: Vec<&str> = segment_id.split(':').collect();
    if parts.len() != 2 {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "invalid segment ID format: expected 'table_id:segment_id', got '{}'",
            segment_id
        ))));
    }

    let table_id: u32 = parts[0].parse().map_err(|_| {
        Error::Core(alopex_core::Error::InvalidFormat(format!(
            "invalid table_id in segment ID: '{}'",
            parts[0]
        )))
    })?;

    let seg_id: u64 = parts[1].parse().map_err(|_| {
        Error::Core(alopex_core::Error::InvalidFormat(format!(
            "invalid segment_id in segment ID: '{}'",
            parts[1]
        )))
    })?;

    Ok((table_id, seg_id))
}

/// カラム値を SqlValue に変換する。
fn column_value_to_sql_value(col: &Column, row_idx: usize) -> alopex_sql::SqlValue {
    match col {
        Column::Int64(vals) => vals
            .get(row_idx)
            .map(|&v| alopex_sql::SqlValue::BigInt(v))
            .unwrap_or(alopex_sql::SqlValue::Null),
        Column::Float32(vals) => vals
            .get(row_idx)
            .map(|&v| alopex_sql::SqlValue::Float(v))
            .unwrap_or(alopex_sql::SqlValue::Null),
        Column::Float64(vals) => vals
            .get(row_idx)
            .map(|&v| alopex_sql::SqlValue::Double(v))
            .unwrap_or(alopex_sql::SqlValue::Null),
        Column::Bool(vals) => vals
            .get(row_idx)
            .map(|&v| alopex_sql::SqlValue::Boolean(v))
            .unwrap_or(alopex_sql::SqlValue::Null),
        Column::Binary(vals) => vals
            .get(row_idx)
            .map(|v| alopex_sql::SqlValue::Blob(v.clone()))
            .unwrap_or(alopex_sql::SqlValue::Null),
        Column::Fixed { values, .. } => values
            .get(row_idx)
            .map(|v| alopex_sql::SqlValue::Blob(v.clone()))
            .unwrap_or(alopex_sql::SqlValue::Null),
    }
}

struct EmbeddedV08SegmentProvider {
    bridge: ColumnarKvsBridge,
    table_id: u32,
    segment_id: u64,
}

impl ColumnarSegmentProvider for EmbeddedV08SegmentProvider {
    fn source_limits(&self) -> Vec<SourceLimit> {
        vec![
            SourceLimit {
                source: PlanSubject::ColumnarSegmentScan,
                code: "stable_row_group_order",
                description: "V08 streaming preserves provisioned row-group and row order",
            },
            SourceLimit {
                source: PlanSubject::ColumnarSegmentScan,
                code: "requires_v08_chunked_layout",
                description: "legacy V2 blobs and in-memory columnar storage are rejected before streaming reads",
            },
            SourceLimit {
                source: PlanSubject::ColumnarSegmentScan,
                code: "row_group_must_fit_resource_bound",
                description: "each V08 row-group decoded and Arrow upper bound is reserved before chunk fetch",
            },
        ]
    }

    fn open(&self, metadata_limit_bytes: u64) -> DataFrameResult<Box<dyn ColumnarSegmentCursor>> {
        let access = self
            .bridge
            .open_v08_segment_with_metadata_limit(
                self.table_id,
                self.segment_id,
                metadata_limit_bytes,
            )
            .map_err(map_v08_error)?;
        let schema = core_schema_to_arrow(access.schema())?;
        let projection = (0..access.schema().column_count()).collect();
        Ok(Box::new(EmbeddedV08SegmentCursor {
            access,
            schema,
            projection,
            next_row_group: 0,
            closed: false,
        }))
    }
}

struct EmbeddedV08SegmentCursor {
    access: ChunkedSegmentAccessV08,
    schema: SchemaRef,
    projection: Vec<usize>,
    next_row_group: usize,
    closed: bool,
}

impl ColumnarSegmentCursor for EmbeddedV08SegmentCursor {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn metadata_footprint_upper_bound(&self) -> u64 {
        self.access.metadata_footprint_upper_bound()
    }

    fn next_batch_plan(&self) -> DataFrameResult<Option<ColumnarSegmentBatchPlan>> {
        if self.closed || self.next_row_group == self.access.row_group_count() {
            return Ok(None);
        }
        let row_group = self
            .access
            .row_group(self.next_row_group)
            .map_err(map_v08_error)?;
        Ok(Some(ColumnarSegmentBatchPlan {
            decoded_bytes: row_group.decoded_bytes,
            arrow_allocation_upper_bound: row_group.arrow_allocation_upper_bound,
        }))
    }

    fn next_batch(&mut self) -> DataFrameResult<Option<ArrowRecordBatch>> {
        if self.closed || self.next_row_group == self.access.row_group_count() {
            return Ok(None);
        }
        let core_batch = self
            .access
            .read_row_group(self.next_row_group, &self.projection)
            .map_err(map_v08_error)?;
        let batch = core_batch_to_arrow(core_batch, self.schema.clone())?;
        self.next_row_group = self.next_row_group.saturating_add(1);
        Ok(Some(batch))
    }

    fn close(&mut self) -> DataFrameResult<()> {
        self.closed = true;
        Ok(())
    }
}

fn core_schema_to_arrow(schema: &CoreSchema) -> DataFrameResult<SchemaRef> {
    let fields = schema
        .columns
        .iter()
        .map(|column| {
            Ok(Field::new(
                &column.name,
                arrow_type(column.logical_type)?,
                column.nullable,
            ))
        })
        .collect::<DataFrameResult<Vec<_>>>()?;
    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn arrow_type(logical_type: LogicalType) -> DataFrameResult<DataType> {
    match logical_type {
        LogicalType::Int64 => Ok(DataType::Int64),
        LogicalType::Float32 => Ok(DataType::Float32),
        LogicalType::Float64 => Ok(DataType::Float64),
        LogicalType::Bool => Ok(DataType::Boolean),
        LogicalType::Binary => Ok(DataType::Binary),
        LogicalType::Fixed(length) => Ok(DataType::FixedSizeBinary(i32::from(length))),
    }
}

fn core_batch_to_arrow(
    batch: CoreRecordBatch,
    schema: SchemaRef,
) -> DataFrameResult<ArrowRecordBatch> {
    if batch.schema.columns.len() != schema.fields().len()
        || batch.columns.len() != schema.fields().len()
        || batch.null_bitmaps.len() != schema.fields().len()
    {
        return Err(DataFrameError::schema_mismatch(
            "V08 decoded batch does not match its provisioned schema",
        ));
    }

    let arrays = batch
        .columns
        .iter()
        .zip(batch.null_bitmaps.iter())
        .map(|(column, bitmap)| core_column_to_arrow(column, bitmap.as_ref()))
        .collect::<DataFrameResult<Vec<_>>>()?;
    ArrowRecordBatch::try_new(schema, arrays).map_err(|source| DataFrameError::Arrow { source })
}

fn core_column_to_arrow(column: &Column, bitmap: Option<&Bitmap>) -> DataFrameResult<ArrayRef> {
    let valid = |index| bitmap.is_none_or(|bitmap| bitmap.is_valid(index));
    match column {
        Column::Int64(values) => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| valid(index).then_some(*value))
                .collect::<Vec<_>>(),
        )) as ArrayRef),
        Column::Float32(values) => Ok(Arc::new(Float32Array::from(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| valid(index).then_some(*value))
                .collect::<Vec<_>>(),
        )) as ArrayRef),
        Column::Float64(values) => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| valid(index).then_some(*value))
                .collect::<Vec<_>>(),
        )) as ArrayRef),
        Column::Bool(values) => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| valid(index).then_some(*value))
                .collect::<Vec<_>>(),
        )) as ArrayRef),
        Column::Binary(values) => Ok(Arc::new(BinaryArray::from(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| valid(index).then_some(value.as_slice()))
                .collect::<Vec<_>>(),
        )) as ArrayRef),
        Column::Fixed { len, values } => {
            let byte_width = i32::try_from(*len).map_err(|_| {
                DataFrameError::schema_mismatch("V08 fixed binary length does not fit Arrow")
            })?;
            let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), byte_width);
            for (index, value) in values.iter().enumerate() {
                if valid(index) {
                    builder
                        .append_value(value)
                        .map_err(|source| DataFrameError::Arrow { source })?;
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
    }
}

fn map_v08_error(error: alopex_core::columnar::ColumnarError) -> DataFrameError {
    match error {
        alopex_core::columnar::ColumnarError::RequiresV08ChunkedLayout => {
            DataFrameError::streaming_unsupported(
                "columnar_segment_scan",
                "requires_v08_chunked_layout",
            )
        }
        alopex_core::columnar::ColumnarError::NotFound => {
            DataFrameError::streaming_unsupported("columnar_segment_scan", "v08_segment_not_found")
        }
        error => DataFrameError::schema_mismatch(format!("V08 columnar segment error: {error}")),
    }
}

// ============================================================================
// ColumnarRowIterator - FR-7 Streaming Row Iterator
// ============================================================================

/// Streaming row iterator for columnar segments (FR-7 compliant).
///
/// This iterator yields rows one at a time from pre-loaded RecordBatches,
/// avoiding the need to materialize all rows into `Vec<Vec<SqlValue>>` upfront.
pub struct ColumnarRowIterator {
    /// Pre-loaded RecordBatches.
    batches: Vec<CoreRecordBatch>,
    /// Current batch index.
    batch_idx: usize,
    /// Current row index within the batch.
    row_idx: usize,
}

impl ColumnarRowIterator {
    /// Create a new row iterator from RecordBatches.
    pub fn new(batches: Vec<CoreRecordBatch>) -> Self {
        Self {
            batches,
            batch_idx: 0,
            row_idx: 0,
        }
    }

    /// Returns the total number of batches.
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Returns the current batch being iterated, if any.
    pub fn current_batch(&self) -> Option<&CoreRecordBatch> {
        self.batches.get(self.batch_idx)
    }
}

impl Iterator for ColumnarRowIterator {
    type Item = Vec<alopex_sql::SqlValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if we've exhausted all batches
            if self.batch_idx >= self.batches.len() {
                return None;
            }

            let batch = &self.batches[self.batch_idx];
            let row_count = batch.num_rows();

            // Check if we've exhausted the current batch
            if self.row_idx >= row_count {
                self.batch_idx += 1;
                self.row_idx = 0;
                continue;
            }

            // Convert current row
            let row_idx = self.row_idx;
            self.row_idx += 1;

            let mut row = Vec::with_capacity(batch.columns.len());
            for col in &batch.columns {
                let sql_val = column_value_to_sql_value(col, row_idx);
                row.push(sql_val);
            }
            return Some(row);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::columnar::encoding::{Column, LogicalType};
    use alopex_core::columnar::encoding_v2::{create_encoder, EncodingV2};
    use alopex_core::columnar::error::{ColumnarError, Result as ColumnarResult};
    use alopex_core::columnar::kvs_bridge::key_layout;
    use alopex_core::columnar::segment_v08::{
        ChecksummedMetadataV08, StreamingChunkMetaV08, StreamingRowGroupV08,
        StreamingSegmentDirectoryV08, StreamingSegmentHeaderV08,
        STREAMING_SEGMENT_LAYOUT_VERSION_V08, STREAMING_SEGMENT_MAGIC_V08,
    };
    use alopex_core::columnar::segment_v2::{ColumnSchema, Schema, SegmentReaderV2, SegmentSource};
    use alopex_core::kv::{KVStore, KVTransaction};
    use alopex_core::storage::compression::CompressionV2;
    use alopex_core::storage::format::bincode_config;
    use alopex_core::storage::format::{AlopexFileWriter, FileFlags, FileVersion};
    use alopex_core::TxnManager;
    use arrow::array::Array;
    use bincode::Options;
    use crc32fast::Hasher;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_batch() -> CoreRecordBatch {
        let schema = Schema {
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    logical_type: LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
                ColumnSchema {
                    name: "val".into(),
                    logical_type: LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
            ],
        };
        CoreRecordBatch::new(
            schema,
            vec![
                Column::Int64(vec![1, 2, 3]),
                Column::Int64(vec![10, 20, 30]),
            ],
            vec![None, None],
        )
    }

    fn make_wide_batch(column_count: usize, row_count: usize) -> CoreRecordBatch {
        let schema = Schema {
            columns: (0..column_count)
                .map(|idx| ColumnSchema {
                    name: format!("c{idx}"),
                    logical_type: LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                })
                .collect(),
        };
        let columns = (0..column_count)
            .map(|idx| {
                Column::Int64(
                    (0..row_count)
                        .map(|row| (idx as i64 * 1_000_000) + row as i64)
                        .collect(),
                )
            })
            .collect();
        CoreRecordBatch::new(schema, columns, vec![None; column_count])
    }

    fn decoded_payload_bytes(batches: &[CoreRecordBatch]) -> usize {
        batches
            .iter()
            .flat_map(|batch| batch.columns.iter())
            .map(|column| match column {
                Column::Int64(values) => values.len() * std::mem::size_of::<i64>(),
                Column::Float32(values) => values.len() * std::mem::size_of::<f32>(),
                Column::Float64(values) => values.len() * std::mem::size_of::<f64>(),
                Column::Bool(values) => values.len() * std::mem::size_of::<bool>(),
                Column::Binary(values) => values.iter().map(Vec::len).sum(),
                Column::Fixed { values, .. } => values.iter().map(Vec::len).sum(),
            })
            .sum()
    }

    fn checksum(bytes: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(bytes);
        hasher.finalize()
    }

    fn metadata_checksum<T: serde::Serialize>(value: &T) -> u32 {
        checksum(&bincode_config().serialize(value).unwrap())
    }

    fn checksummed_metadata<T: serde::Serialize + Clone>(value: T) -> Vec<u8> {
        let checksum = metadata_checksum(&value);
        bincode_config()
            .serialize(&ChecksummedMetadataV08 { value, checksum })
            .unwrap()
    }

    fn provision_v08_int64_fixture(db: &Database, table_id: u32, segment_id: u64) {
        let schema = Schema {
            columns: vec![ColumnSchema {
                name: "value".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            }],
        };
        let chunk_bytes = create_encoder(EncodingV2::Plain)
            .encode(&Column::Int64(vec![41, 42]), None)
            .unwrap();
        let chunk = StreamingChunkMetaV08 {
            column_index: 0,
            encoding: EncodingV2::Plain,
            compression: CompressionV2::None,
            encoded_bytes: chunk_bytes.len() as u64,
            decoded_bytes: chunk_bytes.len() as u64,
            checksum: checksum(&chunk_bytes),
        };
        let row_group = StreamingRowGroupV08 {
            row_start: 0,
            row_count: 2,
            encoded_bytes: chunk.encoded_bytes,
            decoded_bytes: chunk.decoded_bytes,
            arrow_allocation_upper_bound: 4096,
            chunks: vec![chunk],
        };
        let directory = StreamingSegmentDirectoryV08 {
            row_groups: vec![row_group],
        };
        let header = StreamingSegmentHeaderV08 {
            magic: STREAMING_SEGMENT_MAGIC_V08,
            format_version: STREAMING_SEGMENT_LAYOUT_VERSION_V08,
            row_count: 2,
            column_count: 1,
            row_group_count: 1,
            schema_checksum: metadata_checksum(&schema),
            directory_checksum: metadata_checksum(&directory),
        };

        let manager = db.store.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadWrite).unwrap();
        txn.put(
            key_layout::v08_header_key(table_id, segment_id),
            checksummed_metadata(header),
        )
        .unwrap();
        txn.put(
            key_layout::v08_schema_key(table_id, segment_id),
            checksummed_metadata(schema),
        )
        .unwrap();
        txn.put(
            key_layout::v08_directory_key(table_id, segment_id),
            checksummed_metadata(directory),
        )
        .unwrap();
        txn.put(
            key_layout::v08_chunk_key(table_id, segment_id, 0, 0),
            chunk_bytes,
        )
        .unwrap();
        manager.commit(txn).unwrap();
    }

    #[test]
    fn v08_factory_rejects_legacy_v2_before_stream_open() {
        let dir = tempfile::tempdir().unwrap();
        let db =
            Database::open_with_config(EmbeddedConfig::disk(dir.path().join("wal.log"))).unwrap();
        let segment_id = db.write_columnar_segment("legacy", make_batch()).unwrap();

        assert!(matches!(
            db.columnar_segment_streaming_factory_v08("legacy", segment_id),
            Err(Error::DataFrame(DataFrameError::StreamingUnsupported { reason, .. }))
                if reason == "requires_v08_chunked_layout"
        ));
    }

    #[test]
    fn v08_adapter_converts_nullable_core_columns_to_arrow() {
        let schema = CoreSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    logical_type: LogicalType::Int64,
                    nullable: true,
                    fixed_len: None,
                },
                ColumnSchema {
                    name: "payload".into(),
                    logical_type: LogicalType::Binary,
                    nullable: false,
                    fixed_len: None,
                },
            ],
        };
        let batch = CoreRecordBatch::new(
            schema.clone(),
            vec![
                Column::Int64(vec![10, 20]),
                Column::Binary(vec![b"a".to_vec(), b"b".to_vec()]),
            ],
            vec![Some(Bitmap::from_bools(&[true, false])), None],
        );

        let arrow_schema = core_schema_to_arrow(&schema).unwrap();
        let arrow_batch = core_batch_to_arrow(batch, arrow_schema).unwrap();
        let ids = arrow_batch.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payload = arrow_batch.columns()[1]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(ids.value(0), 10);
        assert!(ids.is_null(1));
        assert_eq!(payload.value(1), b"b");
    }

    #[test]
    fn v08_database_adapter_streams_a_provisioned_row_group() {
        let dir = tempfile::tempdir().unwrap();
        let db =
            Database::open_with_config(EmbeddedConfig::disk(dir.path().join("wal.log"))).unwrap();
        let table_id = db.resolve_table_id("v08").unwrap();
        provision_v08_int64_fixture(&db, table_id, 77);
        let options = alopex_dataframe::physical::budget::StreamOptions::new(
            64 * 1024,
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(2).unwrap(),
        );

        let mut stream = db.stream_columnar_segment_v08("v08", 77, options).unwrap();
        let batch = stream.next_batch().unwrap().unwrap();
        let values = batch.column("value").unwrap().to_arrow();
        let values = values[0].as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), 41);
        assert_eq!(values.value(1), 42);
        assert!(stream.next_batch().unwrap().is_none());

        let mut by_id = db
            .stream_columnar_segment_v08_by_id(&format!("{table_id}:77"), options)
            .unwrap();
        let batch = by_id.next_batch().unwrap().unwrap();
        let values = batch.column("value").unwrap().to_arrow();
        let values = values[0].as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), 41);
        assert_eq!(values.value(1), 42);
        assert!(by_id.next_batch().unwrap().is_none());
    }

    #[derive(Debug, Clone)]
    struct CountingSegmentSource {
        data: Arc<Vec<u8>>,
        bytes: Arc<AtomicU64>,
        calls: Arc<AtomicUsize>,
    }

    impl CountingSegmentSource {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: Arc::new(data),
                bytes: Arc::new(AtomicU64::new(0)),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn reset(&self) {
            self.bytes.store(0, Ordering::Relaxed);
            self.calls.store(0, Ordering::Relaxed);
        }

        fn bytes(&self) -> u64 {
            self.bytes.load(Ordering::Relaxed)
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl SegmentSource for CountingSegmentSource {
        fn read_range(&self, offset: u64, len: u64) -> ColumnarResult<Vec<u8>> {
            self.bytes.fetch_add(len, Ordering::Relaxed);
            self.calls.fetch_add(1, Ordering::Relaxed);
            let start = offset as usize;
            let end = start + len as usize;
            if end > self.data.len() {
                return Err(ColumnarError::InvalidFormat("range out of bounds".into()));
            }
            Ok(self.data[start..end].to_vec())
        }

        fn total_size(&self) -> u64 {
            self.data.len() as u64
        }
    }

    fn measured_segment_read(data: Vec<u8>, columns: &[usize]) -> ColumnarResult<(u64, usize)> {
        let source = CountingSegmentSource::new(data);
        let reader = SegmentReaderV2::open(Box::new(source.clone()))?;
        source.reset();
        let batches = reader.read_columns(columns)?;
        if batches.is_empty() {
            return Err(ColumnarError::InvalidFormat("segment is empty".into()));
        }
        Ok((source.bytes(), source.calls()))
    }

    fn reset_last_read_column_indices() {
        LAST_READ_COLUMN_INDICES.with(|last| {
            *last.borrow_mut() = None;
        });
    }

    fn last_read_column_indices() -> Vec<usize> {
        LAST_READ_COLUMN_INDICES.with(|last| {
            last.borrow()
                .clone()
                .expect("read_columnar_segment should record read indices")
        })
    }

    #[test]
    fn write_read_disk_mode() {
        let dir = tempdir().unwrap();
        let wal = dir.path().join("wal.log");
        let cfg = EmbeddedConfig::disk(wal);
        let db = Database::open_with_config(cfg).unwrap();
        let seg_id = db.write_columnar_segment("tbl", make_batch()).unwrap();
        let batches = db.read_columnar_segment("tbl", seg_id, None).unwrap();
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn read_with_column_names() {
        let dir = tempdir().unwrap();
        let wal = dir.path().join("wal.log");
        let cfg = EmbeddedConfig::disk(wal);
        let db = Database::open_with_config(cfg).unwrap();
        let seg_id = db.write_columnar_segment("tbl", make_batch()).unwrap();
        let batches = db
            .read_columnar_segment("tbl", seg_id, Some(&["val"]))
            .unwrap();
        assert_eq!(batches[0].columns.len(), 1);
        if let Column::Int64(vals) = &batches[0].columns[0] {
            assert_eq!(vals, &vec![10, 20, 30]);
        } else {
            panic!("expected int64");
        }
    }

    #[test]
    fn disk_projection_pushes_selected_columns_to_segment_reader() {
        let dir = tempdir().unwrap();
        let wal = dir.path().join("wal.log");
        let cfg = EmbeddedConfig::disk(wal);
        let db = Database::open_with_config(cfg).unwrap();
        let seg_id = db
            .write_columnar_segment("wide_tbl", make_wide_batch(12, 4096))
            .unwrap();
        let table_id = table_id("wide_tbl").unwrap();
        let raw = db
            .columnar_bridge
            .read_segment_raw(table_id, seg_id)
            .unwrap();

        let all_indices: Vec<usize> = (0..12).collect();
        let projection = [2, 7, 10];
        let (full_read_bytes, full_read_calls) =
            measured_segment_read(raw.data.clone(), &all_indices).unwrap();
        let (projected_read_bytes, projected_read_calls) =
            measured_segment_read(raw.data, &projection).unwrap();

        reset_last_read_column_indices();
        let full_batches = db.read_columnar_segment("wide_tbl", seg_id, None).unwrap();
        assert_eq!(last_read_column_indices(), all_indices);

        reset_last_read_column_indices();
        let projected_batches = db
            .read_columnar_segment("wide_tbl", seg_id, Some(&["c2", "c7", "c10"]))
            .unwrap();
        let pushed_indices = last_read_column_indices();
        let full_payload_bytes = decoded_payload_bytes(&full_batches);
        let projected_payload_bytes = decoded_payload_bytes(&projected_batches);

        eprintln!(
            "projection pushed_indices={pushed_indices:?} full_read_bytes={full_read_bytes} projected_read_bytes={projected_read_bytes} full_read_calls={full_read_calls} projected_read_calls={projected_read_calls} full_payload_bytes={full_payload_bytes} projected_payload_bytes={projected_payload_bytes}"
        );

        assert_eq!(pushed_indices, projection);
        assert!(projected_read_bytes < full_read_bytes);
        assert!(projected_payload_bytes < full_payload_bytes);
        assert_eq!(projected_batches[0].schema.columns.len(), projection.len());
    }

    #[test]
    fn in_memory_projection_pushes_selected_columns_to_segment_reader() {
        let db = Database::open_with_config(EmbeddedConfig::in_memory()).unwrap();
        let seg_id = db
            .write_columnar_segment("wide_mem_tbl", make_wide_batch(12, 4096))
            .unwrap();

        let all_indices: Vec<usize> = (0..12).collect();
        let projection = [2, 7, 10];

        reset_last_read_column_indices();
        let full_batches = db
            .read_columnar_segment("wide_mem_tbl", seg_id, None)
            .unwrap();
        assert_eq!(last_read_column_indices(), all_indices);

        reset_last_read_column_indices();
        let projected_batches = db
            .read_columnar_segment("wide_mem_tbl", seg_id, Some(&["c2", "c7", "c10"]))
            .unwrap();
        let pushed_indices = last_read_column_indices();
        let full_payload_bytes = decoded_payload_bytes(&full_batches);
        let projected_payload_bytes = decoded_payload_bytes(&projected_batches);

        eprintln!(
            "in_memory_projection pushed_indices={pushed_indices:?} full_payload_bytes={full_payload_bytes} projected_payload_bytes={projected_payload_bytes}"
        );

        assert_eq!(pushed_indices, projection);
        assert!(projected_payload_bytes < full_payload_bytes);
        assert_eq!(projected_batches[0].schema.columns.len(), projection.len());
    }

    #[test]
    fn in_memory_limit_rejects_large_segment() {
        let cfg = EmbeddedConfig::in_memory_with_limit(1);
        let db = Database::open_with_config(cfg).unwrap();
        let err = db
            .write_columnar_segment("tbl", make_batch())
            .expect_err("should exceed limit");
        assert!(format!("{err}").contains("memory limit exceeded"));
    }

    #[test]
    fn storage_mode_flags() {
        let dir = tempdir().unwrap();
        let wal = dir.path().join("wal.log");
        let disk = Database::open_with_config(EmbeddedConfig::disk(wal)).unwrap();
        assert!(matches!(disk.storage_mode(), StorageMode::Disk));

        let mem = Database::open_with_config(EmbeddedConfig::in_memory()).unwrap();
        assert!(matches!(mem.storage_mode(), StorageMode::InMemory));
    }

    #[test]
    fn transaction_write_and_read() {
        let dir = tempdir().unwrap();
        let wal = dir.path().join("wal.log");
        let db = Database::open_with_config(EmbeddedConfig::disk(wal)).unwrap();
        let txn = db.begin(crate::TxnMode::ReadWrite).unwrap();
        let seg_id = txn.write_columnar_segment("tbl_txn", make_batch()).unwrap();
        txn.commit().unwrap();

        let batches = db
            .read_columnar_segment("tbl_txn", seg_id, Some(&["id"]))
            .unwrap();
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn flush_in_memory_paths() {
        let dir = tempdir().unwrap();
        let db = Database::open_with_config(EmbeddedConfig::in_memory()).unwrap();
        let seg_id = db.write_columnar_segment("mem_tbl", make_batch()).unwrap();

        // flush to file
        let file_path = dir.path().join("seg.bin");
        db.flush_in_memory_segment_to_file("mem_tbl", seg_id, &file_path)
            .unwrap();
        let bytes = std::fs::read(&file_path).unwrap();
        assert!(!bytes.is_empty());

        // flush to kvs
        let kv_id = db
            .flush_in_memory_segment_to_kvs("mem_tbl", seg_id)
            .unwrap();
        assert_eq!(kv_id, 0);

        // flush to .alopex
        let alo_path = dir.path().join("out.alopex");
        let mut writer =
            AlopexFileWriter::new(alo_path.clone(), FileVersion::CURRENT, FileFlags(0)).unwrap();
        db.flush_in_memory_segment_to_alopex("mem_tbl", seg_id, &mut writer)
            .unwrap();
        writer.finalize().unwrap();
        assert!(alo_path.exists());
    }

    #[test]
    fn flush_not_in_memory_mode_errors() {
        let dir = tempdir().unwrap();
        let wal = dir.path().join("wal.log");
        let db = Database::open_with_config(EmbeddedConfig::disk(wal)).unwrap();
        let err = db
            .flush_in_memory_segment_to_kvs("tbl", 0)
            .expect_err("should error");
        assert!(matches!(err, Error::NotInMemoryMode));
    }
}
