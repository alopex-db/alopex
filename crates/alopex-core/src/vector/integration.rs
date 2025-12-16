//! Vector API の統合テスト（Disk モード）。

use crate::kv::storage::{StorageFactory, StorageMode};
use crate::kv::{KVStore, KVTransaction};
use crate::lsm::wal::{SyncMode, WalConfig};
use crate::lsm::LsmKVConfig;
use crate::types::TxnMode;
use crate::vector::hnsw::{HnswConfig, HnswIndex};
use crate::vector::{Metric, VectorSearchParams, VectorStoreConfig, VectorStoreManager};

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

fn block_on<F: Future>(mut fut: F) -> F::Output {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker: Waker = Arc::new(Noop).into();
    let mut cx = Context::from_waker(&waker);
    // SAFETY: テスト内の単純 future のみを扱う。
    let mut fut = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

pub mod disk {
    use super::*;

    #[test]
    fn vector_store_delete_and_compaction_roundtrip_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };

        // VectorStoreManager は in-memory だが、セグメントは KVS に永続化できる（bytes として）。
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 2,
            ..Default::default()
        });
        let keys = vec![1, 2, 3];
        let vecs = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![0.6, 0.8]];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();

        block_on(mgr.delete_batch(&[2])).unwrap();
        let seg_id = mgr.segments()[0].segment_id;
        block_on(mgr.compact_segment(seg_id)).unwrap();

        // 永続化
        {
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg.clone()),
            })
            .unwrap();
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            for seg in mgr.segments() {
                let key = crate::vector::vector_key_layout::vector_segment_key(seg.segment_id);
                tx.put(key, seg.to_bytes().unwrap()).unwrap();
            }
            tx.commit_self().unwrap();
        }

        // 復元
        let mut segments = Vec::new();
        {
            // 書く → drop → reopen → 読める（プロセス再起動相当）
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg),
            })
            .unwrap();
            let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
            for seg in mgr.segments() {
                let key = crate::vector::vector_key_layout::vector_segment_key(seg.segment_id);
                let bytes = tx.get(&key).unwrap().unwrap();
                segments.push(crate::vector::VectorSegment::from_bytes(&bytes).unwrap());
            }
        }
        let restored = VectorStoreManager::from_segments(
            mgr.config().clone(),
            segments,
            mgr.next_segment_id(),
        );

        // 検索（削除済みキー 2 が出ないことを確認）
        let params = VectorSearchParams {
            query: vec![1.0, 0.0],
            metric: Metric::InnerProduct,
            top_k: 3,
            projection: None,
            filter_mask: Some(vec![true, true, true]),
        };
        let (results, _stats) = restored.search_with_stats(params).unwrap();
        assert!(results.iter().all(|r| r.row_id != 2));
    }

    #[test]
    fn hnsw_delete_and_compaction_roundtrip_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            ..Default::default()
        };

        // create + upsert + delete + save
        {
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg.clone()),
            })
            .unwrap();
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            let mut index =
                HnswIndex::create("idx", HnswConfig::default().with_dimension(2)).unwrap();
            index.upsert(b"a", &[1.0, 0.0], b"m1").unwrap();
            index.upsert(b"b", &[0.0, 1.0], b"m2").unwrap();
            assert!(index.delete(b"b").unwrap());
            index.save(&mut tx).unwrap();
            tx.commit_self().unwrap();
        }

        // reopen: load + compact + save
        {
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg.clone()),
            })
            .unwrap();
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            let mut index = HnswIndex::load("idx", &mut tx).unwrap();
            let _ = index.compact().unwrap();
            index.save(&mut tx).unwrap();
            tx.commit_self().unwrap();
        }

        // reload + search (deleted が返らない)
        {
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg),
            })
            .unwrap();
            let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
            let index = HnswIndex::load("idx", &mut tx).unwrap();
            let (results, _stats) = index.search(&[0.0, 1.0], 10, None).unwrap();
            assert!(results.iter().all(|r| r.key != b"b".to_vec()));
        }
    }
}
