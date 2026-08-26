#![cfg(feature = "tokio")]

use alopex_core::kv::async_adapter::AsyncKVStoreAdapter;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{
    AsyncKVStore, AsyncKVTransaction, KeyPattern, KeySearchCancellation, KeySearchRequest,
};
use futures::StreamExt;

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn async_kv_put_get_scan_commit() {
    let store = MemoryKV::new();
    let async_store = AsyncKVStoreAdapter::new(store);

    let mut txn = async_store.begin_async().await.expect("begin");
    txn.async_put(b"user:1", b"alice").await.expect("put");
    txn.async_put(b"user:2", b"bob").await.expect("put");
    txn.async_put(b"order:1", b"42").await.expect("put");

    let value = txn.async_get(b"user:1").await.expect("get");
    assert_eq!(value.as_deref(), Some(b"alice".as_ref()));

    let mut stream = txn.async_scan_prefix(b"user:");
    let mut keys = Vec::new();
    while let Some(entry) = stream.next().await {
        let (key, _) = entry.expect("scan");
        keys.push(String::from_utf8(key).expect("utf8"));
    }
    keys.sort();
    assert_eq!(keys, vec!["user:1".to_string(), "user:2".to_string()]);
    drop(stream);

    txn.async_commit().await.expect("commit");

    let txn = async_store.begin_async().await.expect("begin");
    let value = txn.async_get(b"user:2").await.expect("get");
    assert_eq!(value.as_deref(), Some(b"bob".as_ref()));
    txn.async_rollback().await.expect("rollback");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn async_kv_rollback_discards_changes() {
    let store = MemoryKV::new();
    let async_store = AsyncKVStoreAdapter::new(store);

    let mut txn = async_store.begin_async().await.expect("begin");
    txn.async_put(b"temp:1", b"shadow").await.expect("put");
    txn.async_rollback().await.expect("rollback");

    let txn = async_store.begin_async().await.expect("begin");
    let value = txn.async_get(b"temp:1").await.expect("get");
    assert!(value.is_none());
    txn.async_rollback().await.expect("rollback");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn async_kv_search_uses_the_common_contract() {
    let async_store = AsyncKVStoreAdapter::new(MemoryKV::new());
    let mut txn = async_store.begin_async().await.expect("begin");
    txn.async_put(b"app/one/config", b"one").await.unwrap();
    txn.async_put(b"app/two/config", b"two").await.unwrap();

    let page = txn
        .async_search_keys(KeySearchRequest::new(
            KeyPattern::glob(b"app/*/config"),
            10,
            10,
        ))
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);

    let cancellation = KeySearchCancellation::default();
    cancellation.cancel();
    let error = txn
        .async_search_keys_with_cancellation(
            KeySearchRequest::new(KeyPattern::glob(b"*"), 10, 10),
            cancellation,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    txn.async_rollback().await.unwrap();
}
