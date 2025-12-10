//! HNSW の公開データ型と設定。

use serde::{Deserialize, Serialize};

use crate::vector::Metric;
use crate::{Error, Result};

/// HNSW インデックスの設定値。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HnswConfig {
    /// ベクトル次元（1-65535）。
    pub dimension: usize,
    /// 利用する距離メトリック。
    pub metric: Metric,
    /// 双方向接続の上限（2-100、既定16）。
    pub m: usize,
    /// 構築時の探索幅（m 以上、既定200）。
    pub ef_construction: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            dimension: 0,
            metric: Metric::Cosine,
            m: 16,
            ef_construction: 200,
        }
    }
}

impl HnswConfig {
    /// パラメータの範囲チェックを行う（dimension 1-65535、m 2-100、ef_construction>=m）。
    pub fn validate(&self) -> Result<()> {
        if !(1..=65535).contains(&self.dimension) {
            return Err(Error::InvalidParameter {
                param: "dimension".to_string(),
                reason: format!("must be between 1 and 65535 (got {})", self.dimension),
            });
        }

        if !(2..=100).contains(&self.m) {
            return Err(Error::InvalidParameter {
                param: "m".to_string(),
                reason: format!("must be between 2 and 100 (got {})", self.m),
            });
        }

        if self.ef_construction < self.m {
            return Err(Error::InvalidParameter {
                param: "ef_construction".to_string(),
                reason: format!(
                    "must be greater than or equal to m (m={}, ef_construction={})",
                    self.m, self.ef_construction
                ),
            });
        }

        Ok(())
    }

    /// 次元数を設定した新しい構成を返す。
    pub fn with_dimension(mut self, dim: usize) -> Self {
        self.dimension = dim;
        self
    }

    /// メトリックを設定した新しい構成を返す。
    pub fn with_metric(mut self, metric: Metric) -> Self {
        self.metric = metric;
        self
    }

    /// ノード接続数 `m` を設定した新しい構成を返す。
    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self
    }

    /// 構築時探索幅 `ef_construction` を設定した新しい構成を返す。
    pub fn with_ef_construction(mut self, ef_construction: usize) -> Self {
        self.ef_construction = ef_construction;
        self
    }
}

/// HNSW インデックスの集計統計。
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HnswStats {
    /// Number of active nodes.
    pub node_count: u64,
    /// Number of logically deleted nodes.
    pub deleted_count: u64,
    /// Distribution of nodes per level.
    pub level_distribution: Vec<u64>,
    /// Estimated memory usage in bytes.
    pub memory_bytes: u64,
    /// Average number of edges per node.
    pub avg_edges_per_node: f64,
}

/// 検索 1 回あたりの統計。
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchStats {
    /// Total nodes visited during search.
    pub nodes_visited: u64,
    /// Number of distance computations performed.
    pub distance_computations: u64,
    /// Elapsed search time in microseconds.
    pub search_time_us: u64,
}

/// HNSW 検索結果の 1 件。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HnswSearchResult {
    /// Key associated with the returned vector.
    pub key: Vec<u8>,
    /// Distance value for the result.
    pub distance: f32,
    /// Arbitrary metadata blob stored alongside the vector.
    pub metadata: Vec<u8>,
}

/// 挿入コールバックで通知する統計。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InsertStats {
    /// Assigned node identifier.
    pub node_id: u32,
    /// Level at which the node was inserted.
    pub level: usize,
    /// Number of neighbors connected during insertion.
    pub connected_neighbors: usize,
}

/// In-memory representation of an HNSW node.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct HnswNode {
    /// External key for the vector.
    pub key: Vec<u8>,
    /// Vector payload.
    pub vector: Vec<f32>,
    /// Metadata blob associated with the vector.
    pub metadata: Vec<u8>,
    /// Adjacency lists per level (level -> node IDs).
    pub neighbors: Vec<Vec<u32>>,
    /// Logical deletion marker.
    pub deleted: bool,
}

/// Serializable representation of an HNSW node.
#[allow(dead_code)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HnswNodeData {
    /// External key for the vector.
    pub key: Vec<u8>,
    /// Vector payload.
    pub vector: Vec<f32>,
    /// Metadata blob associated with the vector.
    pub metadata: Vec<u8>,
    /// Adjacency lists per level.
    pub neighbors: Vec<Vec<u32>>,
    /// Logical deletion marker.
    pub deleted: bool,
    /// Level assigned during insertion.
    pub level: usize,
}

/// Serializable metadata for an HNSW index.
#[allow(dead_code)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HnswMetadata {
    /// On-disk format version.
    pub version: u32,
    /// Index configuration.
    pub config: HnswConfig,
    /// Entry point node ID.
    pub entry_point: Option<u32>,
    /// Highest level currently present in the graph.
    pub max_level: usize,
    /// Total number of nodes (including deleted).
    pub node_count: u64,
    /// Count of logically deleted nodes.
    pub deleted_count: u64,
    /// Next node identifier to assign.
    pub next_node_id: u32,
    /// CRC32 checksum of persisted state.
    pub checksum: u32,
}
