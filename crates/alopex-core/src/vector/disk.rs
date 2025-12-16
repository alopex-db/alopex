//! Vector API の Disk モード（LSM-Tree）向け最小テスト。

use crate::kv::storage::{StorageFactory, StorageMode};
use crate::kv::{KVStore, KVTransaction};
use crate::lsm::wal::{SyncMode, WalConfig};
use crate::lsm::LsmKVConfig;
use crate::types::TxnMode;
use crate::vector::hnsw::{HnswConfig, HnswIndex};
use crate::vector::vector_key_layout;

#[test]
fn disk_mode_can_persist_vector_segments_in_kvs() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::NoSync,
        },
        ..Default::default()
    };

    // VectorSegment を KVS に保存。
    let seg = crate::vector::VectorSegment {
        segment_id: 1,
        dimension: 2,
        metric: crate::vector::Metric::InnerProduct,
        num_vectors: 1,
        vectors: crate::vector::columnar::EncodedColumn {
            logical_type: crate::columnar::encoding::LogicalType::Float32,
            encoding: crate::columnar::encoding_v2::EncodingV2::Plain,
            num_values: 2,
            data: crate::columnar::encoding_v2::create_encoder(
                crate::columnar::encoding_v2::EncodingV2::Plain,
            )
            .encode(
                &crate::columnar::encoding::Column::Float32(vec![1.0, 0.0]),
                None,
            )
            .unwrap(),
            null_bitmap: None,
        },
        keys: crate::vector::columnar::EncodedColumn {
            logical_type: crate::columnar::encoding::LogicalType::Int64,
            encoding: crate::columnar::encoding_v2::EncodingV2::Plain,
            num_values: 1,
            data: crate::columnar::encoding_v2::create_encoder(
                crate::columnar::encoding_v2::EncodingV2::Plain,
            )
            .encode(&crate::columnar::encoding::Column::Int64(vec![10]), None)
            .unwrap(),
            null_bitmap: None,
        },
        deleted: crate::columnar::encoding_v2::Bitmap::new(1),
        metadata: None,
        statistics: crate::columnar::statistics::VectorSegmentStatistics {
            row_count: 1,
            null_count: 0,
            active_count: 1,
            deleted_count: 0,
            deletion_ratio: 0.0,
            norm_min: 0.0,
            norm_max: 0.0,
            min_values: Vec::new(),
            max_values: Vec::new(),
            created_at: 0,
        },
    };
    let bytes = seg.to_bytes().unwrap();
    {
        let store = StorageFactory::create(StorageMode::Disk {
            path: dir.path().to_path_buf(),
            config: Some(cfg.clone()),
        })
        .unwrap();
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        tx.put(vector_key_layout::vector_segment_key(seg.segment_id), bytes)
            .unwrap();
        tx.commit_self().unwrap();
    }

    // 書く → drop → reopen → 読める（プロセス再起動相当）
    let restored = {
        let store = StorageFactory::create(StorageMode::Disk {
            path: dir.path().to_path_buf(),
            config: Some(cfg),
        })
        .unwrap();
        let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
        let got = tx
            .get(&vector_key_layout::vector_segment_key(seg.segment_id))
            .unwrap()
            .unwrap();
        crate::vector::VectorSegment::from_bytes(&got).unwrap()
    };
    assert_eq!(restored.segment_id, seg.segment_id);
    assert_eq!(restored.dimension, seg.dimension);
}

#[test]
fn disk_mode_can_save_and_load_hnsw_index() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LsmKVConfig {
        wal: WalConfig {
            segment_size: 4096,
            max_segments: 2,
            sync_mode: SyncMode::NoSync,
        },
        ..Default::default()
    };

    // create + upsert + save
    {
        let store = StorageFactory::create(StorageMode::Disk {
            path: dir.path().to_path_buf(),
            config: Some(cfg.clone()),
        })
        .unwrap();
        let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
        let mut index = HnswIndex::create("idx", HnswConfig::default().with_dimension(2)).unwrap();
        index.upsert(b"k", &[1.0, 0.0], b"meta").expect("upsert");
        index.save(&mut tx).unwrap();
        tx.commit_self().unwrap();
    }

    // 書く → drop → reopen → 読める（プロセス再起動相当）
    {
        let store = StorageFactory::create(StorageMode::Disk {
            path: dir.path().to_path_buf(),
            config: Some(cfg),
        })
        .unwrap();
        let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
        let index = HnswIndex::load("idx", &mut tx).unwrap();
        let (results, _stats) = index.search(&[1.0, 0.0], 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"k".to_vec());
    }
}
