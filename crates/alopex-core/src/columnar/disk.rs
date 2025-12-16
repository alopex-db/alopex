//! Columnar API の Disk モード（LSM-Tree）向け最小テスト。

use crate::columnar::kvs_bridge::ColumnarKvsBridge;
use crate::columnar::segment_v2::{
    ColumnSchema, RecordBatch, Schema, SegmentConfigV2, SegmentWriterV2,
};
use crate::kv::storage::{StorageFactory, StorageMode};
use crate::kv::AnyKV;
use crate::lsm::wal::{SyncMode, WalConfig};
use crate::lsm::LsmKVConfig;

#[test]
fn disk_mode_can_write_and_read_segment_via_kvs_bridge() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::NoSync,
        },
        ..Default::default()
    };

    let schema = Schema {
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                logical_type: crate::columnar::encoding::LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
            ColumnSchema {
                name: "val".into(),
                logical_type: crate::columnar::encoding::LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
        ],
    };
    let batch = RecordBatch::new(
        schema.clone(),
        vec![
            crate::columnar::encoding::Column::Int64(vec![1, 2]),
            crate::columnar::encoding::Column::Int64(vec![10, 20]),
        ],
        vec![None, None],
    );

    let mut writer = SegmentWriterV2::new(SegmentConfigV2::default());
    writer.write_batch(batch).unwrap();
    let segment = writer.finish().unwrap();

    // 書く → drop → reopen → 読める（プロセス再起動相当）
    let segment_id = {
        let store = StorageFactory::create(StorageMode::Disk {
            path: dir.path().to_path_buf(),
            config: Some(cfg.clone()),
        })
        .unwrap();
        assert!(matches!(store, AnyKV::Lsm(_)));

        let bridge = ColumnarKvsBridge::new(std::sync::Arc::new(store));
        bridge.write_segment(42, &segment).unwrap()
    };

    {
        let store = StorageFactory::create(StorageMode::Disk {
            path: dir.path().to_path_buf(),
            config: Some(cfg),
        })
        .unwrap();
        let bridge = ColumnarKvsBridge::new(std::sync::Arc::new(store));

        let index = bridge.segment_index(42).unwrap();
        assert_eq!(index, vec![segment_id]);

        let out = bridge.read_segment(42, segment_id, &[0, 1]).unwrap();
        assert_eq!(out[0].num_rows(), 2);
    }
}
