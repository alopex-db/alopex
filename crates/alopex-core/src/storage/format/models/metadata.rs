//! メタデータセクションのシリアライズモデル。

use bincode::Options;
use serde::{Deserialize, Serialize};

/// メタデータセクションのルート。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// テーブルスキーマ一覧。
    pub schemas: Vec<TableSchema>,
    /// セカンダリインデックス定義。
    pub indexes: Vec<IndexDefinition>,
    /// ベクターインデックス定義。
    pub vector_indexes: Vec<VectorIndexMetadata>,
    /// Rangeメタデータ（分散モードのみ）。
    pub range_metadata: Option<RangeMetadata>,
}

/// テーブルスキーマ。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// テーブルID。
    pub table_id: u64,
    /// テーブル名。
    pub table_name: String,
    /// カラム定義。
    pub columns: Vec<ColumnDefinition>,
    /// プライマリキーのカラムインデックス。
    pub primary_key: Vec<u32>,
}

/// カラム定義。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// カラムID（0-based）。
    pub column_id: u32,
    /// カラム名。
    pub name: String,
    /// 型情報（例: "int64", "string"）。
    pub data_type: String,
    /// NULL許容フラグ。
    pub nullable: bool,
    /// デフォルト値（存在しない場合はNone）。
    pub default_value: Option<Vec<u8>>,
}

/// セカンダリインデックス定義。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// インデックスID。
    pub index_id: u64,
    /// 対象テーブルID。
    pub table_id: u64,
    /// インデックス名。
    pub index_name: String,
    /// 対象カラムインデックス（`TableSchema.columns` のindex）。
    pub columns: Vec<u32>,
    /// 一意制約。
    pub unique: bool,
}

/// ベクターインデックス定義。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexMetadata {
    /// インデックスID。
    pub index_id: u64,
    /// 対象テーブルID。
    pub table_id: u64,
    /// ベクタ次元数。
    pub dimension: u32,
    /// 類似度メトリクス。
    pub metric: VectorMetric,
}

/// 類似度メトリクス。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorMetric {
    /// ユークリッド距離。
    L2,
    /// コサイン類似度。
    Cosine,
    /// 内積。
    Dot,
}

/// Rangeメタデータ（分散配置情報）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeMetadata {
    /// Range ID。
    pub range_id: u64,
    /// Range開始キー（包含）。
    pub key_start: Vec<u8>,
    /// Range終了キー（排他）。
    pub key_end: Vec<u8>,
    /// 世代/epoch。
    pub epoch: u64,
    /// 副本のノードID一覧。
    pub replica_set: Vec<u64>,
}

/// 決定的なbincode設定。
pub fn bincode_config() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}
