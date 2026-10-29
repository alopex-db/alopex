//! HNSW インデックスの永続化レイヤー。

use std::collections::HashSet;

use super::cow::{KeyMap, PagedVec};

use crc32fast::hash;

use super::graph::HnswGraph;
use super::types::{HnswMetadata, HnswNode, HnswNodeData};
use crate::kv::KVTransaction;
use crate::vector::simd::select_kernel;
use crate::{Error, Result};

#[cfg(all(test, not(target_arch = "wasm32")))]
thread_local! {
    static SERIALIZED_NODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn take_serialized_node_count() -> usize {
    SERIALIZED_NODES.with(|count| count.replace(0))
}

/// HNSW フォーマットバージョン（v0.3 初版）。
#[allow(dead_code)]
pub(crate) const HNSW_FORMAT_VERSION: u32 = 1;

#[allow(dead_code)]
const META_PREFIX: &[u8] = b"hnsw:meta:";
#[allow(dead_code)]
const NODE_PREFIX: &[u8] = b"hnsw:node:";
#[allow(dead_code)]
const KEY_INDEX_PREFIX: &[u8] = b"hnsw:key:";

/// HNSW の KVS 保存を担当するハンドラ。
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct HnswStorage {
    /// インデックス名。
    pub(crate) index_name: String,
}

#[allow(dead_code)]
impl HnswStorage {
    /// ストレージハンドラを生成する。
    pub(crate) fn new(name: &str) -> Self {
        Self {
            index_name: name.to_string(),
        }
    }

    /// メタデータキーを生成する（`hnsw:meta:{name}`）。
    pub(crate) fn meta_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(META_PREFIX.len() + self.index_name.len());
        key.extend_from_slice(META_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key
    }

    /// ノードキーを生成する（`hnsw:node:{name}:{id}`）。
    pub(crate) fn node_key(&self, node_id: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(NODE_PREFIX.len() + self.index_name.len() + 12);
        key.extend_from_slice(NODE_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key.push(b':');
        key.extend_from_slice(node_id.to_string().as_bytes());
        key
    }

    /// キーインデックスキーを生成する（`hnsw:key:{name}:{key}`）。
    pub(crate) fn key_index_key(&self, key_bytes: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(
            KEY_INDEX_PREFIX.len() + self.index_name.len() + 1 + key_bytes.len(),
        );
        key.extend_from_slice(KEY_INDEX_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key.push(b':');
        key.extend_from_slice(key_bytes);
        key
    }

    /// ノードプレフィックス（削除スキャン用）を取得する。
    pub(crate) fn node_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(NODE_PREFIX.len() + self.index_name.len() + 1);
        key.extend_from_slice(NODE_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key.push(b':');
        key
    }

    /// キーインデックスプレフィックス（削除スキャン用）を取得する。
    pub(crate) fn key_index_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(KEY_INDEX_PREFIX.len() + self.index_name.len() + 1);
        key.extend_from_slice(KEY_INDEX_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key.push(b':');
        key
    }

    /// グラフ全体をフルシリアライズして保存する。
    pub(crate) fn save<'a, T: KVTransaction<'a>>(
        &self,
        txn: &mut T,
        graph: &HnswGraph,
    ) -> Result<Vec<u8>> {
        // 既存データを全削除してからフル書き込みする。
        self.purge_nodes(txn)?;
        self.purge_key_indices(txn)?;

        let mut metadata = HnswMetadata {
            version: HNSW_FORMAT_VERSION,
            config: graph.config.clone(),
            entry_point: graph.entry_point,
            max_level: graph.max_level,
            node_count: graph.active_count,
            deleted_count: graph.deleted_count,
            next_node_id: graph.next_node_id(),
            checksum: 0,
        };

        let mut all_node_checksums: u32 = 0;
        for (node_id, node_opt) in graph.nodes.iter().enumerate() {
            let Some(node) = node_opt else {
                continue;
            };

            let node_data = Self::node_to_data(node);
            let node_bytes =
                bincode::serialize(&node_data).map_err(|e| Error::InvalidFormat(e.to_string()))?;
            all_node_checksums = hash(&node_bytes).wrapping_add(all_node_checksums);

            txn.put(self.node_key(node_id as u32), node_bytes)?;
            txn.put(
                self.key_index_key(&node.key),
                (node_id as u32).to_le_bytes().to_vec(),
            )?;
        }

        let meta_bytes_for_hash =
            bincode::serialize(&metadata).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        metadata.checksum = hash(&meta_bytes_for_hash).wrapping_add(all_node_checksums);

        let final_meta_bytes =
            bincode::serialize(&metadata).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        txn.put(self.meta_key(), final_meta_bytes.clone())?;

        Ok(final_meta_bytes)
    }

    /// Save changed slots and adjust the format-v1 checksum from their before-images.
    pub(crate) fn save_incremental<'a, T: KVTransaction<'a>>(
        &self,
        txn: &mut T,
        graph: &HnswGraph,
        baseline: Option<&HnswGraph>,
        modified_nodes: &[u32],
        inserted_nodes: &[u32],
    ) -> Result<Vec<u8>> {
        // A flushed write may have been rolled back, or this index may be saved
        // into another store. Only use deltas against the exact known header.
        let stored_metadata = txn.get(&self.meta_key())?;
        if graph.persisted_metadata.is_none() || stored_metadata != graph.persisted_metadata {
            return self.save(txn, graph);
        }
        let changed: HashSet<u32> = modified_nodes
            .iter()
            .chain(inserted_nodes.iter())
            .chain(graph.dirty_nodes.iter())
            .copied()
            .collect();
        // A snapshot containing earlier unsaved changes is not a persisted
        // before-image. Direct (non-staged) mutations also use the full fallback.
        if !changed.is_empty()
            && !baseline.is_some_and(|before| {
                before.dirty_nodes.is_empty()
                    && before.persisted_metadata == graph.persisted_metadata
            })
        {
            return self.save(txn, graph);
        }
        let mut previous: HnswMetadata =
            bincode::deserialize(stored_metadata.as_deref().expect("matched known header"))
                .map_err(|e| Error::InvalidFormat(e.to_string()))?;
        let previous_checksum = previous.checksum;
        previous.checksum = 0;
        let previous_header =
            bincode::serialize(&previous).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        let mut all_node_checksums = previous_checksum.wrapping_sub(hash(&previous_header));

        for &node_id in &changed {
            let old_node = baseline
                .and_then(|before| before.nodes.get(node_id as usize).and_then(Option::as_ref));
            if let Some(old_node) = old_node {
                let old_bytes = bincode::serialize(&Self::node_to_data(old_node))
                    .map_err(|e| Error::InvalidFormat(e.to_string()))?;
                all_node_checksums = all_node_checksums.wrapping_sub(hash(&old_bytes));
                // A removed key may have been reinserted into another slot in
                // the same batch; that current mapping must not be deleted.
                if !graph.key_to_node.contains_key(&old_node.key) {
                    txn.delete(self.key_index_key(&old_node.key))?;
                }
            }
            if let Some(node) = graph.nodes.get(node_id as usize).and_then(Option::as_ref) {
                let node_bytes = bincode::serialize(&Self::node_to_data(node))
                    .map_err(|e| Error::InvalidFormat(e.to_string()))?;
                all_node_checksums = all_node_checksums.wrapping_add(hash(&node_bytes));
                txn.put(self.node_key(node_id), node_bytes)?;
                txn.put(
                    self.key_index_key(&node.key),
                    node_id.to_le_bytes().to_vec(),
                )?;
            } else {
                txn.delete(self.node_key(node_id))?;
            }
        }

        let mut metadata = HnswMetadata {
            version: HNSW_FORMAT_VERSION,
            config: graph.config.clone(),
            entry_point: graph.entry_point,
            max_level: graph.max_level,
            node_count: graph.active_count,
            deleted_count: graph.deleted_count,
            next_node_id: graph.next_node_id(),
            checksum: 0,
        };

        let meta_bytes_for_hash =
            bincode::serialize(&metadata).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        metadata.checksum = hash(&meta_bytes_for_hash).wrapping_add(all_node_checksums);

        let final_meta_bytes =
            bincode::serialize(&metadata).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        txn.put(self.meta_key(), final_meta_bytes.clone())?;

        Ok(final_meta_bytes)
    }

    /// Read only the header; graph checksum validation remains with full loading.
    pub(crate) fn load_metadata<'a, T: KVTransaction<'a>>(
        &self,
        txn: &mut T,
    ) -> Result<HnswMetadata> {
        let bytes = txn.get(&self.meta_key())?.ok_or(Error::IndexNotFound {
            name: self.index_name.clone(),
        })?;
        let metadata: HnswMetadata =
            bincode::deserialize(&bytes).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        if metadata.version > HNSW_FORMAT_VERSION {
            return Err(Error::UnsupportedIndexVersion {
                found: metadata.version,
                supported: HNSW_FORMAT_VERSION,
            });
        }
        Ok(metadata)
    }

    /// KVS からグラフを読み戻す。
    pub(crate) fn load<'a, T: KVTransaction<'a>>(&self, txn: &mut T) -> Result<HnswGraph> {
        let metadata = self.load_metadata(txn)?;

        let mut node_bytes = Vec::with_capacity(metadata.next_node_id as usize);
        for node_id in 0..metadata.next_node_id {
            let node_key = self.node_key(node_id);
            node_bytes.push(txn.get(&node_key)?);
        }

        self.build_graph_from_bytes(metadata, node_bytes)
    }

    /// 取得済みのメタデータとノードデータからグラフを構築する。
    pub(crate) fn build_graph_from_bytes(
        &self,
        metadata: HnswMetadata,
        node_bytes: Vec<Option<Vec<u8>>>,
    ) -> Result<HnswGraph> {
        if metadata.version > HNSW_FORMAT_VERSION {
            return Err(Error::UnsupportedIndexVersion {
                found: metadata.version,
                supported: HNSW_FORMAT_VERSION,
            });
        }

        let mut all_node_checksums: u32 = 0;
        let mut nodes = PagedVec::default();
        let mut key_to_node = KeyMap::default();
        let mut free_list = PagedVec::default();

        for (node_id, bytes) in node_bytes.into_iter().enumerate() {
            if let Some(node_bytes) = bytes {
                let node_data: HnswNodeData = bincode::deserialize(&node_bytes)
                    .map_err(|e| Error::InvalidFormat(e.to_string()))?;
                all_node_checksums = hash(&node_bytes).wrapping_add(all_node_checksums);

                key_to_node.insert(node_data.key.clone(), node_id as u32);
                let norm = super::vector_norm(&node_data.vector);
                nodes.push(Some(
                    HnswNode {
                        key: node_data.key,
                        vector: node_data.vector,
                        norm,
                        metadata: node_data.metadata,
                        neighbors: node_data.neighbors,
                        deleted: node_data.deleted,
                    }
                    .into(),
                ));
            } else {
                nodes.push(None);
                free_list.push(node_id as u32);
            }
        }

        let mut meta_for_hash = metadata.clone();
        meta_for_hash.checksum = 0;
        let meta_bytes_for_hash =
            bincode::serialize(&meta_for_hash).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        let expected_checksum = hash(&meta_bytes_for_hash).wrapping_add(all_node_checksums);

        if metadata.checksum != expected_checksum {
            return Err(Error::CorruptedIndex {
                name: self.index_name.clone(),
                reason: format!(
                    "checksum mismatch: expected {}, found {}",
                    expected_checksum, metadata.checksum
                ),
            });
        }

        let persisted_metadata =
            Some(bincode::serialize(&metadata).map_err(|e| Error::InvalidFormat(e.to_string()))?);
        Ok(HnswGraph {
            nodes,
            dirty_nodes: HashSet::new(),
            persisted_metadata,
            key_to_node,
            entry_point: metadata.entry_point,
            max_level: metadata.max_level,
            config: metadata.config,
            kernel: select_kernel(),
            free_list,
            active_count: metadata.node_count,
            deleted_count: metadata.deleted_count,
        })
    }

    /// フル保存のための書き込み計画を構築する。
    pub(crate) fn build_save_plan(&self, graph: &HnswGraph) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut puts = Vec::new();

        let mut metadata = HnswMetadata {
            version: HNSW_FORMAT_VERSION,
            config: graph.config.clone(),
            entry_point: graph.entry_point,
            max_level: graph.max_level,
            node_count: graph.active_count,
            deleted_count: graph.deleted_count,
            next_node_id: graph.next_node_id(),
            checksum: 0,
        };

        let mut all_node_checksums: u32 = 0;
        for (node_id, node_opt) in graph.nodes.iter().enumerate() {
            let Some(node) = node_opt else {
                continue;
            };

            let node_data = Self::node_to_data(node);
            let node_bytes =
                bincode::serialize(&node_data).map_err(|e| Error::InvalidFormat(e.to_string()))?;
            all_node_checksums = hash(&node_bytes).wrapping_add(all_node_checksums);

            puts.push((self.node_key(node_id as u32), node_bytes));
            puts.push((
                self.key_index_key(&node.key),
                (node_id as u32).to_le_bytes().to_vec(),
            ));
        }

        let meta_bytes_for_hash =
            bincode::serialize(&metadata).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        metadata.checksum = hash(&meta_bytes_for_hash).wrapping_add(all_node_checksums);

        let final_meta_bytes =
            bincode::serialize(&metadata).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        puts.push((self.meta_key(), final_meta_bytes));

        Ok(puts)
    }

    /// インデックスに紐づく全データを削除する。
    pub(crate) fn delete<'a, T: KVTransaction<'a>>(&self, txn: &mut T) -> Result<()> {
        self.purge_nodes(txn)?;
        self.purge_key_indices(txn)?;
        txn.delete(self.meta_key())?;
        Ok(())
    }

    fn node_to_data(node: &HnswNode) -> HnswNodeData {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        SERIALIZED_NODES.with(|count| count.set(count.get() + 1));
        HnswNodeData {
            key: node.key.clone(),
            vector: node.vector.clone(),
            metadata: node.metadata.clone(),
            neighbors: node.neighbors.clone(),
            deleted: node.deleted,
            level: node.neighbors.len().saturating_sub(1),
        }
    }

    fn purge_nodes<'a, T: KVTransaction<'a>>(&self, txn: &mut T) -> Result<()> {
        let prefix = self.node_prefix();
        let keys: Vec<_> = txn.scan_prefix(&prefix)?.map(|(k, _)| k).collect();
        for key in keys {
            txn.delete(key)?;
        }
        Ok(())
    }

    fn purge_key_indices<'a, T: KVTransaction<'a>>(&self, txn: &mut T) -> Result<()> {
        let prefix = self.key_index_prefix();
        let keys: Vec<_> = txn.scan_prefix(&prefix)?.map(|(k, _)| k).collect();
        for key in keys {
            txn.delete(key)?;
        }
        Ok(())
    }
}
