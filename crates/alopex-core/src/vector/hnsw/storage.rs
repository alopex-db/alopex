//! HNSW インデックスの永続化レイヤー。

use std::collections::{HashMap, HashSet};

use crc32fast::hash;

use super::graph::HnswGraph;
use super::types::{HnswMetadata, HnswNode, HnswNodeData};
use crate::kv::KVTransaction;
use crate::vector::simd::select_kernel;
use crate::{Error, Result};

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
    fn meta_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(META_PREFIX.len() + self.index_name.len());
        key.extend_from_slice(META_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key
    }

    /// ノードキーを生成する（`hnsw:node:{name}:{id}`）。
    fn node_key(&self, node_id: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(NODE_PREFIX.len() + self.index_name.len() + 12);
        key.extend_from_slice(NODE_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key.push(b':');
        key.extend_from_slice(node_id.to_string().as_bytes());
        key
    }

    /// キーインデックスキーを生成する（`hnsw:key:{name}:{key}`）。
    fn key_index_key(&self, key_bytes: &[u8]) -> Vec<u8> {
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
    fn node_prefix(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(NODE_PREFIX.len() + self.index_name.len() + 1);
        key.extend_from_slice(NODE_PREFIX);
        key.extend_from_slice(self.index_name.as_bytes());
        key.push(b':');
        key
    }

    /// キーインデックスプレフィックス（削除スキャン用）を取得する。
    fn key_index_prefix(&self) -> Vec<u8> {
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
    ) -> Result<()> {
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
        txn.put(self.meta_key(), final_meta_bytes)?;

        Ok(())
    }

    /// 変更分のみ保存しつつ、チェックサムは全ノードで再計算する。
    pub(crate) fn save_incremental<'a, T: KVTransaction<'a>>(
        &self,
        txn: &mut T,
        graph: &HnswGraph,
        modified_nodes: &[u32],
        inserted_nodes: &[u32],
        deleted_key_indices: &[Vec<u8>],
    ) -> Result<()> {
        // 更新/追加ノードのみ書き込み。
        for &node_id in modified_nodes.iter().chain(inserted_nodes.iter()) {
            if let Some(node) = graph.nodes.get(node_id as usize).and_then(|n| n.as_ref()) {
                let node_data = Self::node_to_data(node);
                let node_bytes = bincode::serialize(&node_data)
                    .map_err(|e| Error::InvalidFormat(e.to_string()))?;
                txn.put(self.node_key(node_id), node_bytes)?;
                txn.put(
                    self.key_index_key(&node.key),
                    node_id.to_le_bytes().to_vec(),
                )?;
            }
        }

        // 旧キーインデックスを削除。
        for old_key in deleted_key_indices {
            txn.delete(self.key_index_key(old_key))?;
        }

        // 存在しないノードIDのデータは削除して、空きリストと整合させる。
        for (node_id, node_opt) in graph.nodes.iter().enumerate() {
            if node_opt.is_none() {
                txn.delete(self.node_key(node_id as u32))?;
            }
        }

        // 現在のグラフ状態に基づきチェックサムを再計算。
        let mut all_node_checksums: u32 = 0;
        for node in graph.nodes.iter().flatten() {
            let node_data = Self::node_to_data(node);
            let node_bytes =
                bincode::serialize(&node_data).map_err(|e| Error::InvalidFormat(e.to_string()))?;
            all_node_checksums = hash(&node_bytes).wrapping_add(all_node_checksums);
        }

        // 期待されるキーインデックスだけを残し、余剰を削除する。
        self.sync_key_indices(txn, graph)?;

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
        txn.put(self.meta_key(), final_meta_bytes)?;

        Ok(())
    }

    /// KVS からグラフを読み戻す。
    pub(crate) fn load<'a, T: KVTransaction<'a>>(&self, txn: &mut T) -> Result<HnswGraph> {
        let meta_key = self.meta_key();
        let meta_bytes = txn.get(&meta_key)?.ok_or(Error::IndexNotFound {
            name: self.index_name.clone(),
        })?;
        let metadata: HnswMetadata =
            bincode::deserialize(&meta_bytes).map_err(|e| Error::InvalidFormat(e.to_string()))?;

        if metadata.version > HNSW_FORMAT_VERSION {
            return Err(Error::UnsupportedIndexVersion {
                found: metadata.version,
                supported: HNSW_FORMAT_VERSION,
            });
        }

        let mut all_node_checksums: u32 = 0;
        let mut nodes = Vec::with_capacity(metadata.next_node_id as usize);
        let mut key_to_node = HashMap::new();
        let mut free_list = Vec::new();

        for node_id in 0..metadata.next_node_id {
            let node_key = self.node_key(node_id);
            if let Some(node_bytes) = txn.get(&node_key)? {
                let node_data: HnswNodeData = bincode::deserialize(&node_bytes)
                    .map_err(|e| Error::InvalidFormat(e.to_string()))?;
                all_node_checksums = hash(&node_bytes).wrapping_add(all_node_checksums);

                key_to_node.insert(node_data.key.clone(), node_id);
                nodes.push(Some(HnswNode {
                    key: node_data.key,
                    vector: node_data.vector,
                    metadata: node_data.metadata,
                    neighbors: node_data.neighbors,
                    deleted: node_data.deleted,
                }));
            } else {
                nodes.push(None);
                free_list.push(node_id);
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

        Ok(HnswGraph {
            nodes,
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

    /// インデックスに紐づく全データを削除する。
    pub(crate) fn delete<'a, T: KVTransaction<'a>>(&self, txn: &mut T) -> Result<()> {
        self.purge_nodes(txn)?;
        self.purge_key_indices(txn)?;
        txn.delete(self.meta_key())?;
        Ok(())
    }

    fn node_to_data(node: &HnswNode) -> HnswNodeData {
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

    fn sync_key_indices<'a, T: KVTransaction<'a>>(
        &self,
        txn: &mut T,
        graph: &HnswGraph,
    ) -> Result<()> {
        let expected: HashSet<Vec<u8>> = graph.key_to_node.keys().cloned().collect();

        let prefix = self.key_index_prefix();
        let stored_keys: Vec<_> = txn.scan_prefix(&prefix)?.map(|(k, _)| k).collect();
        for key in stored_keys {
            // KVS に保存されているキー部分だけを取り出す。
            if let Some(actual_key) = key.strip_prefix(prefix.as_slice()) {
                if !expected.contains(actual_key) {
                    txn.delete(key)?;
                }
            } else {
                txn.delete(key)?;
            }
        }

        // 期待セットにないものを削除した上で、必要なキーを再度確実に書き込む。
        for (&node_id, key_bytes) in graph.key_to_node.iter().map(|(k, v)| (v, k)) {
            txn.put(
                self.key_index_key(key_bytes),
                node_id.to_le_bytes().to_vec(),
            )?;
        }

        Ok(())
    }
}
