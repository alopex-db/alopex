#![cfg(feature = "tokio")]

use alopex_core::kv::async_adapter::AsyncKVStoreAdapter;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{AsyncKVStore, AsyncKVTransaction};
use alopex_core::vector::{
    AsyncHnswIndex, AsyncVectorStore, AsyncVectorStoreAdapter, HnswConfig, HnswIndex, Metric,
    VectorStoreConfig, VectorStoreManager,
};

#[tokio::test]
async fn async_vector_store_and_hnsw_roundtrip() {
    let manager = VectorStoreManager::new(VectorStoreConfig {
        dimension: 2,
        metric: Metric::L2,
        ..Default::default()
    });
    let mut async_store = AsyncVectorStoreAdapter::new(manager);
    async_store
        .async_upsert(1, &[0.0, 0.0])
        .await
        .expect("upsert 1");
    async_store
        .async_upsert(2, &[1.0, 0.0])
        .await
        .expect("upsert 2");

    let results = async_store
        .async_search_similar(&[0.0, 0.0], 1)
        .await
        .expect("search id 1");
    assert_eq!(results.first().map(|(id, _)| *id), Some(1));

    let results = async_store
        .async_search_similar(&[1.0, 0.0], 1)
        .await
        .expect("search id 2");
    assert_eq!(results.first().map(|(id, _)| *id), Some(2));

    let store = MemoryKV::new();
    let async_kv = AsyncKVStoreAdapter::new(store);
    let mut txn = async_kv.begin_async().await.expect("begin");
    let mut index =
        HnswIndex::create("idx", HnswConfig::default().with_dimension(2)).expect("create");
    index.upsert(b"a", &[1.0, 0.0], b"m1").expect("upsert a");
    index.upsert(b"b", &[0.0, 1.0], b"m2").expect("upsert b");
    index.async_save(&mut txn).await.expect("save");
    txn.async_commit().await.expect("commit");

    let txn = async_kv.begin_async().await.expect("begin");
    let mut loaded =
        HnswIndex::create("idx", HnswConfig::default().with_dimension(2)).expect("create");
    loaded.async_load(&txn).await.expect("load");
    let (neighbors, _) = loaded.search(&[1.0, 0.0], 1, None).expect("search");
    assert_eq!(
        neighbors.first().map(|res| res.key.as_slice()),
        Some(b"a".as_ref())
    );
    txn.async_rollback().await.expect("rollback");
}
