//! インメモリカラムナーストア。

use std::collections::HashMap;
use std::fs::{rename, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::columnar::error::{ColumnarError, Result};
use crate::columnar::kvs_bridge::ColumnarKvsBridge;
use crate::columnar::segment_v2::{
    ColumnSegmentV2, InMemorySegmentSource, RecordBatch, SegmentReaderV2,
};
use crate::storage::format::{AlopexFileWriter, ColumnarSectionWriter};

/// インメモリにカラムナーセグメントを保持する。
pub struct InMemorySegmentStore {
    segments: RwLock<HashMap<(u32, u64), ColumnSegmentV2>>,
    memory_usage: AtomicU64,
    memory_limit: Option<u64>,
}

impl InMemorySegmentStore {
    /// `memory_limit` バイト上限付きで生成する。`None` は無制限。
    pub fn new(memory_limit: Option<u64>) -> Self {
        Self {
            segments: RwLock::new(HashMap::new()),
            memory_usage: AtomicU64::new(0),
            memory_limit,
        }
    }

    /// セグメントを書き込み、割り当てたセグメントIDを返す。
    pub fn write_segment(&self, table_id: u32, segment: ColumnSegmentV2) -> Result<u64> {
        let size = segment.data.len() as u64;
        let current = self.memory_usage.load(Ordering::Relaxed);
        let requested = current.saturating_add(size);
        if let Some(limit) = self.memory_limit {
            if requested > limit {
                return Err(ColumnarError::MemoryLimitExceeded {
                    limit: std::cmp::min(limit, usize::MAX as u64) as usize,
                    requested: std::cmp::min(requested, usize::MAX as u64) as usize,
                });
            }
        }

        let mut guard = self.segments.write().unwrap();
        let next_id = guard
            .keys()
            .filter(|(tid, _)| *tid == table_id)
            .map(|(_, sid)| *sid)
            .max()
            .map(|id| id.saturating_add(1))
            .unwrap_or(0);
        guard.insert((table_id, next_id), segment);
        drop(guard);
        self.memory_usage.store(requested, Ordering::Relaxed);
        Ok(next_id)
    }

    /// セグメントを読み取り、指定カラムを返す。
    pub fn read_segment(
        &self,
        table_id: u32,
        segment_id: u64,
        columns: &[usize],
    ) -> Result<Vec<RecordBatch>> {
        let guard = self.segments.read().unwrap();
        let segment = guard
            .get(&(table_id, segment_id))
            .ok_or(ColumnarError::NotFound)?
            .clone();
        drop(guard);
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))?;
        reader.read_columns(columns)
    }

    /// 現在のメモリ使用量（バイト）を返す。
    pub fn memory_usage(&self) -> u64 {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// セグメントをファイルへフラッシュする（fsync + rename）。
    pub fn flush_to_segment_file<P: AsRef<Path>>(
        &self,
        table_id: u32,
        segment_id: u64,
        path: P,
    ) -> Result<()> {
        let guard = self.segments.read().unwrap();
        let segment = guard
            .get(&(table_id, segment_id))
            .ok_or(ColumnarError::NotFound)?
            .clone();
        drop(guard);

        let tmp_path = path.as_ref().with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&segment.data)?;
        file.sync_all()?;
        rename(&tmp_path, path)?;
        Ok(())
    }

    /// KVS へフラッシュする（ColumnarKvsBridge 経由）。
    pub fn flush_to_kvs(
        &self,
        table_id: u32,
        segment_id: u64,
        bridge: &ColumnarKvsBridge,
    ) -> Result<u64> {
        let guard = self.segments.read().unwrap();
        let segment = guard
            .get(&(table_id, segment_id))
            .ok_or(ColumnarError::NotFound)?
            .clone();
        drop(guard);
        bridge.write_segment(table_id, &segment)
    }

    /// `.alopex` ファイルへフラッシュする。
    pub fn flush_to_alopex(
        &self,
        table_id: u32,
        segment_id: u64,
        writer: &mut AlopexFileWriter,
    ) -> Result<u32> {
        let guard = self.segments.read().unwrap();
        let segment = guard
            .get(&(table_id, segment_id))
            .ok_or(ColumnarError::NotFound)?
            .clone();
        drop(guard);

        ColumnarSectionWriter::write_section(writer, &segment)
            .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))
    }

    /// カラム数を返す（メタデータから取得）。
    pub fn column_count(&self, table_id: u32, segment_id: u64) -> Result<usize> {
        let guard = self.segments.read().unwrap();
        let segment = guard
            .get(&(table_id, segment_id))
            .ok_or(ColumnarError::NotFound)?;
        Ok(segment.meta.schema.column_count())
    }

    /// すべてのセグメント (table_id, segment_id) を返す。
    pub fn list_segments(&self) -> Vec<(u32, u64)> {
        let guard = self.segments.read().unwrap();
        guard.keys().cloned().collect()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::columnar::encoding::{Column, LogicalType};
    use crate::columnar::segment_v2::{ColumnSchema, Schema, SegmentWriterV2};
    use crate::kv::memory::MemoryKV;
    use crate::storage::format::{
        AlopexFileReader, FileFlags, FileReader, FileSource, FileVersion, SectionType,
    };
    use tempfile::tempdir;

    fn make_segment() -> ColumnSegmentV2 {
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
        let batch = RecordBatch::new(
            schema,
            vec![Column::Int64(vec![1, 2]), Column::Int64(vec![10, 20])],
            vec![None, None],
        );
        let mut writer = SegmentWriterV2::new(Default::default());
        writer.write_batch(batch).unwrap();
        writer.finish().unwrap()
    }

    #[test]
    fn test_memory_limit_enforcement() {
        let store = InMemorySegmentStore::new(Some(1));
        let segment = make_segment();
        let err = store.write_segment(1, segment).unwrap_err();
        assert!(matches!(err, ColumnarError::MemoryLimitExceeded { .. }));
    }

    #[test]
    fn test_write_and_read_segment() {
        let store = InMemorySegmentStore::new(None);
        let id = store.write_segment(2, make_segment()).unwrap();
        let batches = store.read_segment(2, id, &[0, 1]).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_flush_to_segment_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg.bin");
        let store = InMemorySegmentStore::new(None);
        let id = store.write_segment(1, make_segment()).unwrap();
        store
            .flush_to_segment_file(1, id, &path)
            .expect("flush succeeds");
        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_flush_to_kvs() {
        let store = InMemorySegmentStore::new(None);
        let seg = make_segment();
        let id = store.write_segment(4, seg.clone()).unwrap();
        let kv = MemoryKV::new();
        let bridge = ColumnarKvsBridge::new(std::sync::Arc::new(crate::kv::AnyKV::Memory(kv)));
        let new_id = store.flush_to_kvs(4, id, &bridge).unwrap();
        assert_eq!(new_id, 0);
        let batches = bridge.read_segment(4, new_id, &[0, 1]).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_flush_to_alopex() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.alopex");
        let mut writer =
            AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).unwrap();
        let store = InMemorySegmentStore::new(None);
        let id = store.write_segment(1, make_segment()).unwrap();
        let section_id = store.flush_to_alopex(1, id, &mut writer).unwrap();
        assert_eq!(section_id, 0);
        writer.finalize().unwrap();

        let reader =
            AlopexFileReader::open(FileSource::Path(path)).expect("alopex file should open");
        let entry = reader
            .section_index()
            .find_by_id(section_id)
            .expect("entry exists");
        assert_eq!(entry.section_type, SectionType::ColumnarSegment);
    }
}
