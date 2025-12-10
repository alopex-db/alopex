//! カラムナーストレージの埋め込み API 拡張。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use alopex_core::columnar::segment_v2::{RecordBatch, SegmentWriterV2};
use alopex_core::storage::format::AlopexFileWriter;
use alopex_core::{StorageFactory, StorageMode as CoreStorageMode};

use crate::{Database, Error, Result, SegmentConfigV2, Transaction};

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
                StorageFactory::create(CoreStorageMode::Disk { path }).map_err(Error::Core)?
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
    pub fn write_columnar_segment(&self, table: &str, batch: RecordBatch) -> Result<u64> {
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

    /// カラムナーセグメントを読み取る（カラム名指定オプション付き）。
    pub fn read_columnar_segment(
        &self,
        table: &str,
        segment_id: u64,
        columns: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>> {
        let table_id = table_id(table)?;
        let column_count = match self.columnar_mode {
            StorageMode::Disk => self
                .columnar_bridge
                .column_count(table_id, segment_id)
                .map_err(|e| Error::Core(e.into()))?,
            StorageMode::InMemory => self
                .columnar_memory
                .as_ref()
                .ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?
                .column_count(table_id, segment_id)
                .map_err(|e| Error::Core(e.into()))?,
        };
        let all_indices: Vec<usize> = (0..column_count).collect();

        let batches_full = match self.columnar_mode {
            StorageMode::Disk => self
                .columnar_bridge
                .read_segment(table_id, segment_id, &all_indices)
                .map_err(|e| Error::Core(e.into()))?,
            StorageMode::InMemory => self
                .columnar_memory
                .as_ref()
                .ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "in-memory columnar store is not initialized".into(),
                    ))
                })?
                .read_segment(table_id, segment_id, &all_indices)
                .map_err(|e| Error::Core(e.into()))?,
        };

        if let Some(names) = columns {
            let indices = resolve_indices(&batches_full, names)?;
            project_batches(batches_full, &indices)
        } else {
            Ok(batches_full)
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
    pub fn write_columnar_segment(&self, table: &str, batch: RecordBatch) -> Result<u64> {
        self.db.write_columnar_segment(table, batch)
    }

    /// カラムナーセグメントを読み取る（トランザクションコンテキスト利用）。
    pub fn read_columnar_segment(
        &self,
        table: &str,
        segment_id: u64,
        columns: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>> {
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

fn resolve_indices(batches: &[RecordBatch], names: &[&str]) -> Result<Vec<usize>> {
    let Some(first) = batches.first() else {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(
            "segment is empty".into(),
        )));
    };
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        let pos = first
            .schema
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

fn project_batches(batches: Vec<RecordBatch>, indices: &[usize]) -> Result<Vec<RecordBatch>> {
    let mut projected = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut cols = Vec::with_capacity(indices.len());
        let mut bitmaps = Vec::with_capacity(indices.len());
        for &idx in indices {
            let col = batch
                .columns
                .get(idx)
                .ok_or_else(|| {
                    Error::Core(alopex_core::Error::InvalidFormat(
                        "column index out of bounds".into(),
                    ))
                })?
                .clone();
            let bitmap = batch.null_bitmaps.get(idx).cloned().unwrap_or(None);
            cols.push(col);
            bitmaps.push(bitmap);
        }
        let schema = alopex_core::columnar::segment_v2::Schema {
            columns: indices
                .iter()
                .map(|&idx| batch.schema.columns[idx].clone())
                .collect(),
        };
        projected.push(RecordBatch::new(schema, cols, bitmaps));
    }
    Ok(projected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_core::columnar::encoding::{Column, LogicalType};
    use alopex_core::columnar::segment_v2::{ColumnSchema, Schema};
    use alopex_core::storage::format::{AlopexFileWriter, FileFlags, FileVersion};
    use tempfile::tempdir;

    fn make_batch() -> RecordBatch {
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
        RecordBatch::new(
            schema,
            vec![
                Column::Int64(vec![1, 2, 3]),
                Column::Int64(vec![10, 20, 30]),
            ],
            vec![None, None],
        )
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
