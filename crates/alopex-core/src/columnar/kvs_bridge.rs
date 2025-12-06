//! KVS ブリッジ: カラムナーセグメントをトランザクション経由で読み書きする。

use std::sync::Arc;
use std::time::Duration;

use bincode::Options;

use crate::columnar::error::{ColumnarError, Result};
use crate::columnar::segment_v2::{
    ColumnSegmentV2, InMemorySegmentSource, RecordBatch, SegmentMetaV2, SegmentReaderV2,
};
use crate::kv::{memory::MemoryTransaction, KVStore, KVTransaction};
use crate::storage::format::bincode_config;
use crate::txn::TxnManager;
use crate::types::TxnMode;
use crate::MemoryKV;

/// キーレイアウトおよびプレフィックス。
pub mod key_layout {
    /// テーブルメタデータ（予約）。
    pub const PREFIX_TABLE_META: u8 = 0x10;
    /// カラムナーセグメント本体。
    pub const PREFIX_COLUMN_SEGMENT: u8 = 0x11;
    /// セグメントインデックス。
    pub const PREFIX_SEGMENT_INDEX: u8 = 0x12;
    /// 統計情報。
    pub const PREFIX_STATISTICS: u8 = 0x13;
    /// RowGroup 単位の付加情報。
    pub const PREFIX_ROW_GROUP: u8 = 0x14;

    /// カラムナーセグメントのキーを生成する。
    pub fn column_segment_key(table_id: u32, segment_id: u64, column_idx: u16) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 4 + 8 + 2);
        key.push(PREFIX_COLUMN_SEGMENT);
        key.extend_from_slice(&table_id.to_le_bytes());
        key.extend_from_slice(&segment_id.to_le_bytes());
        key.extend_from_slice(&column_idx.to_le_bytes());
        key
    }

    /// テーブル単位のセグメントインデックスキーを生成する。
    pub fn segment_index_key(table_id: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 4);
        key.push(PREFIX_SEGMENT_INDEX);
        key.extend_from_slice(&table_id.to_le_bytes());
        key
    }

    /// 統計情報キーを生成する。
    pub fn statistics_key(table_id: u32, segment_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 4 + 8);
        key.push(PREFIX_STATISTICS);
        key.extend_from_slice(&table_id.to_le_bytes());
        key.extend_from_slice(&segment_id.to_le_bytes());
        key
    }
}

/// カラムナーセグメントを KVS に永続化するブリッジ。
#[derive(Clone)]
pub struct ColumnarKvsBridge {
    store: Arc<MemoryKV>,
    max_retries: usize,
}

impl ColumnarKvsBridge {
    /// 新規ブリッジを生成する。
    pub fn new(store: Arc<MemoryKV>) -> Self {
        Self {
            store,
            max_retries: 3,
        }
    }

    fn load_index<'a>(txn: &mut MemoryTransaction<'a>, table_id: u32) -> Result<Vec<u64>> {
        let key = key_layout::segment_index_key(table_id);
        if let Some(bytes) = txn.get(&key)? {
            bincode_config()
                .deserialize(&bytes)
                .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))
        } else {
            Ok(Vec::new())
        }
    }

    fn persist_index<'a>(
        txn: &mut MemoryTransaction<'a>,
        table_id: u32,
        index: &[u64],
    ) -> Result<()> {
        let key = key_layout::segment_index_key(table_id);
        let bytes = bincode_config()
            .serialize(index)
            .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
        txn.put(key, bytes)?;
        Ok(())
    }

    /// セグメントを書き込み、割り当てたセグメントIDを返す。
    ///
    /// 競合が起きた場合は簡易リトライを行う。
    pub fn write_segment(&self, table_id: u32, segment: &ColumnSegmentV2) -> Result<u64> {
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let store = self.store.clone();
            let manager = store.txn_manager();
            let mut txn = manager.begin(TxnMode::ReadWrite)?;
            let mut index = Self::load_index(&mut txn, table_id)?;
            let segment_id = index.last().copied().unwrap_or(0);
            let next_id = if index.is_empty() {
                0
            } else {
                segment_id.saturating_add(1)
            };

            let segment_key = key_layout::column_segment_key(table_id, next_id, 0);
            let bytes = bincode_config()
                .serialize(segment)
                .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
            txn.put(segment_key, bytes)?;

            // 統計は SegmentMetaV2 をそのまま保持。
            let stats_key = key_layout::statistics_key(table_id, next_id);
            let stats_bytes = bincode_config()
                .serialize(&segment.meta)
                .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
            txn.put(stats_key, stats_bytes)?;

            index.push(next_id);
            Self::persist_index(&mut txn, table_id, &index)?;

            let commit_result = manager.commit(txn).map_err(ColumnarError::from);

            match commit_result {
                Ok(()) => return Ok(next_id),
                Err(ColumnarError::TxnConflict) if attempts < self.max_retries => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// セグメントを読み取り、指定カラムのみを返す。
    pub fn read_segment(
        &self,
        table_id: u32,
        segment_id: u64,
        columns: &[usize],
    ) -> Result<Vec<RecordBatch>> {
        let key = key_layout::column_segment_key(table_id, segment_id, 0);
        let mut txn = self.store.begin(TxnMode::ReadOnly)?;
        let bytes = txn.get(&key)?.ok_or(ColumnarError::NotFound)?;

        let segment: ColumnSegmentV2 = bincode_config()
            .deserialize(&bytes)
            .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))?;
        reader.read_columns(columns)
    }

    /// セグメントメタデータのみ取得する（統計用）。
    pub fn read_statistics(&self, table_id: u32, segment_id: u64) -> Result<Vec<u8>> {
        let key = key_layout::statistics_key(table_id, segment_id);
        let mut txn = self.store.begin(TxnMode::ReadOnly)?;
        let bytes = txn.get(&key)?.ok_or(ColumnarError::NotFound)?;
        Ok(bytes)
    }

    /// インデックスを取得する（テスト用ユーティリティ）。
    pub fn segment_index(&self, table_id: u32) -> Result<Vec<u64>> {
        let mut txn = self.store.begin(TxnMode::ReadOnly)?;
        let index = Self::load_index(&mut txn, table_id)?;
        Ok(index)
    }

    /// カラム数を取得する（統計メタから取得）。
    pub fn column_count(&self, table_id: u32, segment_id: u64) -> Result<usize> {
        let stats = self.read_statistics(table_id, segment_id)?;
        let meta: SegmentMetaV2 = bincode_config()
            .deserialize(&stats)
            .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
        Ok(meta.schema.column_count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::encoding::{Column, LogicalType};
    use crate::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema};
    use crate::kv::memory::MemoryKV;
    use tempfile::tempdir;

    fn simple_schema() -> Schema {
        Schema {
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
        }
    }

    fn make_segment() -> ColumnSegmentV2 {
        let batch = RecordBatch::new(
            simple_schema(),
            vec![
                Column::Int64(vec![1, 2, 3]),
                Column::Int64(vec![10, 20, 30]),
            ],
            vec![None, None],
        );
        let mut writer = crate::columnar::segment_v2::SegmentWriterV2::new(Default::default());
        writer.write_batch(batch).unwrap();
        writer.finish().unwrap()
    }

    #[test]
    fn test_segment_atomic_write_transaction() {
        let store = MemoryKV::new_with_limit(Some(16));
        let bridge = ColumnarKvsBridge::new(Arc::new(store));
        let segment = make_segment();

        // セグメントバイトが大きすぎてメモリリミットに抵触し、コミットが拒否される。
        let err = bridge.write_segment(1, &segment).unwrap_err();
        assert!(matches!(err, ColumnarError::MemoryLimitExceeded { .. }));

        // インデックスとデータは存在しない。
        let store = bridge.store.clone();
        let manager = store.txn_manager();
        let mut txn = manager.begin(TxnMode::ReadOnly).unwrap();
        let index_key = key_layout::segment_index_key(1);
        assert!(txn.get(&index_key).unwrap().is_none());
        manager.commit(txn).unwrap();
    }

    #[test]
    fn test_segment_write_read_via_kvs() {
        let store = Arc::new(MemoryKV::new());
        let bridge = ColumnarKvsBridge::new(store.clone());
        let segment = make_segment();
        let id = bridge.write_segment(7, &segment).unwrap();
        assert_eq!(id, 0);

        let batches = bridge.read_segment(7, id, &[0, 1]).unwrap();
        assert_eq!(batches.len(), 1);
        if let Column::Int64(ids) = &batches[0].columns[0] {
            assert_eq!(ids, &vec![1, 2, 3]);
        } else {
            panic!("expected Column::Int64");
        }
    }

    #[test]
    fn test_segment_consistency_after_crash() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let segment = make_segment();
        {
            let store = Arc::new(MemoryKV::open(&wal_path).unwrap());
            let bridge = ColumnarKvsBridge::new(store);
            bridge.write_segment(9, &segment).unwrap();
        }
        // WAL から復旧
        let reopened = Arc::new(MemoryKV::open(&wal_path).unwrap());
        let bridge = ColumnarKvsBridge::new(reopened);
        let batches = bridge.read_segment(9, 0, &[1]).unwrap();
        if let Column::Int64(vals) = &batches[0].columns[0] {
            assert_eq!(vals, &vec![10, 20, 30]);
        } else {
            panic!("expected Column::Int64");
        }
    }

    #[test]
    fn test_multiple_segments_concurrent_access() {
        let store = Arc::new(MemoryKV::new());
        let bridge = ColumnarKvsBridge::new(store.clone());
        let segment = make_segment();
        let bridge_arc = Arc::new(bridge);

        let mut handles = Vec::new();
        for _ in 0..4 {
            let b = bridge_arc.clone();
            let seg = segment.clone();
            handles.push(std::thread::spawn(move || {
                b.write_segment(3, &seg).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let index = bridge_arc.segment_index(3).unwrap();
        assert_eq!(index.len(), 4);
        // すべて読めるか確認
        for id in index {
            let batches = bridge_arc.read_segment(3, id, &[0]).unwrap();
            assert_eq!(batches[0].num_rows(), 3);
        }
    }
}
