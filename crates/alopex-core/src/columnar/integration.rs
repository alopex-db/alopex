//! Columnar API の統合テスト（Disk モード）。

use crate::columnar::kvs_bridge::ColumnarKvsBridge;
use crate::columnar::segment_v2::{
    ColumnSchema, RecordBatch, Schema, SegmentConfigV2, SegmentWriterV2,
};
use crate::kv::storage::{StorageFactory, StorageMode};
use crate::kv::AnyKV;
use crate::lsm::wal::{SyncMode, WalConfig};
use crate::lsm::LsmKVConfig;
use bincode::Options;

pub mod disk {
    use super::*;

    #[test]
    fn crud_and_scan_like_flow_works_in_disk_mode() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };

        // CREATE/INSERT 相当: 新しいセグメントを書き込み。
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
                    nullable: true,
                    fixed_len: None,
                },
            ],
        };
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                crate::columnar::encoding::Column::Int64(vec![1, 2, 3]),
                crate::columnar::encoding::Column::Int64(vec![10, 20, 30]),
            ],
            vec![None, None],
        );

        let mut writer = SegmentWriterV2::new(SegmentConfigV2::default());
        writer.write_batch(batch).unwrap();
        let segment = writer.finish().unwrap();

        // 書く → drop → reopen → 読める（プロセス再起動相当）
        let seg_id = {
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg.clone()),
            })
            .unwrap();
            assert!(matches!(store, AnyKV::Lsm(_)));

            let bridge = ColumnarKvsBridge::new(std::sync::Arc::new(store));
            bridge.write_segment(7, &segment).unwrap()
        };

        {
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg),
            })
            .unwrap();
            let bridge = ColumnarKvsBridge::new(std::sync::Arc::new(store));

            // READ: 指定カラムを取得。
            let out = bridge.read_segment(7, seg_id, &[0]).unwrap();
            assert_eq!(out[0].num_rows(), 3);

            // STATS: SegmentMetaV2 を取得して復号する。
            let bytes = bridge.read_statistics(7, seg_id).unwrap();
            let meta: crate::columnar::segment_v2::SegmentMetaV2 =
                crate::storage::format::bincode_config()
                    .deserialize(&bytes)
                    .unwrap();
            assert_eq!(meta.schema.column_count(), schema.column_count());

            // PREFIX SCAN 相当: segment_index を読む（scan_prefix を内部で使う）。
            let index = bridge.segment_index(7).unwrap();
            assert_eq!(index, vec![seg_id]);
        }
    }
}
