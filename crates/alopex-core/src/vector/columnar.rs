//! VectorSegment とエンコード済みカラムのシリアライズ補助。
//!
//! Phase3 タスク: VectorSegment構造体定義・シリアライズ実装・KVSキー設計。

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::columnar::encoding::LogicalType;
use crate::columnar::encoding_v2::{Bitmap, EncodingV2};
use crate::columnar::statistics::VectorSegmentStatistics;
use crate::storage::compression::CompressionV2;
use crate::vector::Metric;
use crate::{Error, Result};

/// エンコード済みカラムのメタデータとペイロード。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncodedColumn {
    /// 論理型。
    pub logical_type: LogicalType,
    /// エンコーディング種別。
    pub encoding: crate::columnar::encoding_v2::EncodingV2,
    /// 圧縮種別。
    pub compression: CompressionV2,
    /// 値の個数。
    pub num_values: u64,
    /// エンコード済みペイロード。
    pub data: Vec<u8>,
    /// Null ビットマップ（任意）。
    pub null_bitmap: Option<Bitmap>,
}

/// ベクトル専用カラムナセグメント。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorSegment {
    /// セグメントID。
    pub segment_id: u64,
    /// ベクトル次元。
    pub dimension: usize,
    /// 採用メトリック。
    pub metric: Metric,
    /// ベクトル総数。
    pub num_vectors: u64,
    /// ベクトル本体（Float32連続配列をエンコード）。
    pub vectors: EncodedColumn,
    /// ベクトル識別子列。
    pub keys: EncodedColumn,
    /// 論理削除フラグ。
    pub deleted: Bitmap,
    /// メタデータ列（任意）。
    pub metadata: Option<Vec<EncodedColumn>>,
    /// 統計情報。
    pub statistics: VectorSegmentStatistics,
}

impl VectorSegment {
    /// チェックサム付きでシリアライズする（bincode + crc32）。
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut payload =
            bincode::serialize(self).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();
        payload.extend_from_slice(&checksum.to_le_bytes());
        Ok(payload)
    }

    /// チェックサム検証込みでデシリアライズする。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(Error::InvalidFormat("VectorSegment bytes too short".into()));
        }
        let (payload, checksum_bytes) = bytes.split_at(bytes.len() - 4);
        let expected =
            u32::from_le_bytes(checksum_bytes.try_into().expect("split gives 4-byte slice"));

        let mut hasher = Hasher::new();
        hasher.update(payload);
        let computed = hasher.finalize();
        if computed != expected {
            return Err(Error::ChecksumMismatch);
        }

        bincode::deserialize(payload).map_err(|e| Error::InvalidFormat(e.to_string()))
    }

    /// 内部整合性チェック。
    fn validate(&self) -> Result<()> {
        if self.dimension == 0 {
            return Err(Error::InvalidFormat("dimension must be > 0".into()));
        }
        let n = self.num_vectors as usize;

        // vectors: Float32 かつ行数一致
        if self.vectors.logical_type != LogicalType::Float32 {
            return Err(Error::InvalidFormat(
                "vectors.logical_type must be Float32".into(),
            ));
        }
        if self.vectors.num_values as usize != n {
            return Err(Error::InvalidFormat(
                "vectors.num_values mismatch num_vectors".into(),
            ));
        }

        // keys: Int64 かつ行数一致
        if self.keys.logical_type != LogicalType::Int64 {
            return Err(Error::InvalidFormat(
                "keys.logical_type must be Int64".into(),
            ));
        }
        if self.keys.num_values as usize != n {
            return Err(Error::InvalidFormat(
                "keys.num_values mismatch num_vectors".into(),
            ));
        }

        // deleted bitmap 長さ
        if self.deleted.len() != n {
            return Err(Error::InvalidFormat(
                "deleted bitmap length mismatch num_vectors".into(),
            ));
        }

        // metadata 各列の行数整合
        if let Some(meta_cols) = &self.metadata {
            for (idx, col) in meta_cols.iter().enumerate() {
                if col.num_values as usize != n {
                    return Err(Error::InvalidFormat(format!(
                        "metadata column {} num_values mismatch num_vectors",
                        idx
                    )));
                }
            }
        }

        // 既定エンコーディングの推奨チェック（警告的だが、InvalidFormatで戻す）
        if self.vectors.encoding != EncodingV2::ByteStreamSplit {
            return Err(Error::InvalidFormat(
                "vectors.encoding must be ByteStreamSplit".into(),
            ));
        }

        Ok(())
    }
}

/// KVS キーレイアウト。
pub mod key_layout {
    /// `vector_segment:{segment_id}` 形式のキーを生成する。
    pub fn vector_segment_key(segment_id: u64) -> Vec<u8> {
        format!("vector_segment:{segment_id}").into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::encoding_v2::EncodingV2;

    fn sample_encoded_column() -> EncodedColumn {
        EncodedColumn {
            logical_type: LogicalType::Float32,
            encoding: EncodingV2::ByteStreamSplit,
            compression: CompressionV2::None,
            num_values: 1,
            data: vec![0; 4], // 1 * f32 (little endian placeholder)
            null_bitmap: None,
        }
    }

    fn sample_segment() -> VectorSegment {
        VectorSegment {
            segment_id: 42,
            dimension: 4,
            metric: Metric::Cosine,
            num_vectors: 1,
            vectors: sample_encoded_column(),
            keys: EncodedColumn {
                logical_type: LogicalType::Int64,
                encoding: EncodingV2::Plain,
                compression: CompressionV2::None,
                num_values: 1,
                data: vec![0; 8], // 1 * i64
                null_bitmap: None,
            },
            deleted: Bitmap::all_valid(1),
            metadata: None,
            statistics: VectorSegmentStatistics {
                row_count: 1,
                null_count: 0,
                active_count: 1,
                deleted_count: 0,
                deletion_ratio: 0.0,
                norm_min: 0.0,
                norm_max: 0.0,
                min_values: Vec::new(),
                max_values: Vec::new(),
                created_at: 1_735_000_000,
            },
        }
    }

    #[test]
    fn roundtrip_with_checksum() {
        let seg = sample_segment();
        let bytes = seg.to_bytes().unwrap();
        let restored = VectorSegment::from_bytes(&bytes).unwrap();
        assert_eq!(restored.segment_id, seg.segment_id);
        assert_eq!(restored.dimension, seg.dimension);
        assert_eq!(restored.metric, seg.metric);
        assert_eq!(restored.num_vectors, seg.num_vectors);
        assert_eq!(restored.vectors.logical_type, seg.vectors.logical_type);
        assert_eq!(restored.keys.logical_type, seg.keys.logical_type);
        assert_eq!(restored.deleted, seg.deleted);
        assert_eq!(restored.statistics.row_count, seg.statistics.row_count);
    }

    #[test]
    fn checksum_mismatch_detected() {
        let seg = sample_segment();
        let mut bytes = seg.to_bytes().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xAA;
        let err = VectorSegment::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch));
    }

    #[test]
    fn vector_segment_key_layout() {
        let key = key_layout::vector_segment_key(123);
        assert_eq!(key, b"vector_segment:123");
    }

    #[test]
    fn validate_rejects_mismatched_lengths() {
        let mut seg = sample_segment();
        seg.num_vectors = 2; // mismatch
        let err = seg.to_bytes().unwrap_err();
        assert!(matches!(err, Error::InvalidFormat(_)));
    }
}
