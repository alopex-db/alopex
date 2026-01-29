//! Async vector API adapters and traits.

use crate::async_runtime::{BoxFuture, MaybeSend};
use crate::error::{Error, Result};
use crate::kv::async_kv::AsyncKVTransaction;
use crate::vector::hnsw::{HnswIndex, HnswMetadata, HnswStorage};
use crate::vector::{VectorSearchParams, VectorStoreManager};
use core::future::poll_fn;
use std::sync::{Arc, Mutex};

/// Async version of vector search operations.
pub trait AsyncVectorStore: MaybeSend {
    /// Search for similar vectors and return (row_id, score) pairs.
    fn async_search_similar<'a>(
        &'a self,
        vector: &'a [f32],
        k: usize,
    ) -> BoxFuture<'a, Result<Vec<(u64, f32)>>>;

    /// Upsert a vector by row id.
    fn async_upsert<'a>(&'a mut self, id: u64, vector: &'a [f32]) -> BoxFuture<'a, Result<()>>;
}

/// Async HNSW load/save operations tied to async transactions.
pub trait AsyncHnswIndex: MaybeSend {
    /// Load index state from the transaction.
    fn async_load<'a>(
        &'a mut self,
        txn: &'a (impl AsyncKVTransaction<'a> + Sync),
    ) -> BoxFuture<'a, Result<()>>;

    /// Save index state into the transaction.
    fn async_save<'a>(
        &'a self,
        txn: &'a mut impl AsyncKVTransaction<'a>,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Async adapter for [`VectorStoreManager`] using blocking offload (tokio feature).
#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct AsyncVectorStoreAdapter {
    inner: Arc<Mutex<VectorStoreManager>>,
}

#[cfg(feature = "tokio")]
impl AsyncVectorStoreAdapter {
    /// Wrap a vector store manager for async usage.
    pub fn new(manager: VectorStoreManager) -> Self {
        Self {
            inner: Arc::new(Mutex::new(manager)),
        }
    }

    /// Wrap an existing shared manager.
    pub fn from_arc(inner: Arc<Mutex<VectorStoreManager>>) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "tokio")]
impl AsyncVectorStore for AsyncVectorStoreAdapter {
    fn async_search_similar<'a>(
        &'a self,
        vector: &'a [f32],
        k: usize,
    ) -> BoxFuture<'a, Result<Vec<(u64, f32)>>> {
        let inner = Arc::clone(&self.inner);
        let query = vector.to_vec();
        Box::pin(async move {
            let results = tokio::task::spawn_blocking(move || {
                let manager = inner
                    .lock()
                    .map_err(|_| Error::InvalidFormat("vector store lock poisoned".into()))?;
                let params = VectorSearchParams {
                    query,
                    metric: manager.config().metric,
                    top_k: k,
                    projection: None,
                    filter_mask: None,
                };
                let results = manager.search(params)?;
                results
                    .into_iter()
                    .map(|res| {
                        let row_id = u64::try_from(res.row_id).map_err(|_| {
                            Error::InvalidFormat("negative row_id in vector store".into())
                        })?;
                        Ok((row_id, res.score))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .await
            .map_err(|_| Error::TxnClosed)?;
            results
        })
    }

    fn async_upsert<'a>(&'a mut self, id: u64, vector: &'a [f32]) -> BoxFuture<'a, Result<()>> {
        let inner = Arc::clone(&self.inner);
        let vector = vector.to_vec();
        Box::pin(async move {
            let handle = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let mut manager = inner
                    .lock()
                    .map_err(|_| Error::InvalidFormat("vector store lock poisoned".into()))?;
                let keys = vec![id as i64];
                let vectors = vec![vector];
                handle.block_on(manager.delete_batch(&keys))?;
                handle.block_on(manager.append_batch(&keys, &vectors))?;
                Ok(())
            })
            .await
            .map_err(|_| Error::TxnClosed)?
        })
    }
}

#[cfg(feature = "tokio")]
impl AsyncHnswIndex for HnswIndex {
    fn async_load<'a>(
        &'a mut self,
        txn: &'a (impl AsyncKVTransaction<'a> + Sync),
    ) -> BoxFuture<'a, Result<()>> {
        let name = self.name().to_string();
        let storage = HnswStorage::new(&name);
        Box::pin(async move {
            let meta_key = storage.meta_key();
            let meta_bytes = txn
                .async_get(&meta_key)
                .await?
                .ok_or(Error::IndexNotFound { name: name.clone() })?;
            let metadata: HnswMetadata = bincode::deserialize(&meta_bytes)
                .map_err(|e| Error::InvalidFormat(e.to_string()))?;

            let mut node_bytes = Vec::with_capacity(metadata.next_node_id as usize);
            for node_id in 0..metadata.next_node_id {
                let node_key = storage.node_key(node_id);
                node_bytes.push(txn.async_get(&node_key).await?);
            }

            let storage_for_build = storage.clone();
            let graph = tokio::task::spawn_blocking(move || {
                storage_for_build.build_graph_from_bytes(metadata, node_bytes)
            })
            .await
            .map_err(|_| Error::TxnClosed)??;
            *self = HnswIndex::from_parts(&name, storage, graph);
            Ok(())
        })
    }

    fn async_save<'a>(
        &'a self,
        txn: &'a mut impl AsyncKVTransaction<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        let storage = HnswStorage::new(self.name());
        let graph = self.graph_handle();
        Box::pin(async move {
            let node_prefix = storage.node_prefix();
            let key_prefix = storage.key_index_prefix();
            purge_prefix_async(txn, node_prefix).await?;
            purge_prefix_async(txn, key_prefix).await?;

            let storage_for_build = storage.clone();
            let puts = tokio::task::spawn_blocking(move || {
                let graph = graph.read().unwrap_or_else(|e| e.into_inner());
                storage_for_build.build_save_plan(&graph)
            })
            .await
            .map_err(|_| Error::TxnClosed)??;

            for (key, value) in puts {
                txn.async_put(&key, &value).await?;
            }

            Ok(())
        })
    }
}

#[cfg(feature = "tokio")]
async fn purge_prefix_async(txn: &mut impl AsyncKVTransaction<'_>, prefix: Vec<u8>) -> Result<()> {
    let mut stream = txn.async_scan_prefix(&prefix);
    let mut keys = Vec::new();
    loop {
        let next = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        match next {
            Some(Ok((key, _))) => keys.push(key),
            Some(Err(err)) => return Err(err),
            None => break,
        }
    }
    drop(stream);

    for key in keys {
        txn.async_delete(&key).await?;
    }
    Ok(())
}
