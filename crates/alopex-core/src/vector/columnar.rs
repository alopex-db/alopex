//! VectorSegment とエンコード済みカラムのシリアライズ補助。
//!
//! Phase3 タスク: VectorSegment構造体定義・シリアライズ実装・KVSキー設計。

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::columnar::encoding::{Column, LogicalType};
use crate::columnar::encoding_v2::{create_decoder, create_encoder, Bitmap, EncodingV2};
use crate::columnar::segment_v2::{
    ColumnSchema, ColumnSegmentV2, InMemorySegmentSource, RecordBatch, Schema, SegmentReaderV2,
    SegmentWriterV2,
};
use crate::columnar::statistics::VectorSegmentStatistics;
use crate::storage::compression::CompressionV2;
use crate::vector::Metric;
use crate::{Error, Result};

const VECTOR_SEGMENT_VERSION: u8 = 1;

#[derive(Serialize, Deserialize)]
struct VectorSegmentEnvelope {
    version: u8,
    segment_id: u64,
    dimension: usize,
    metric: Metric,
    statistics: VectorSegmentStatistics,
    segment: ColumnSegmentV2,
}

/// エンコード済みカラムのメタデータとペイロード。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncodedColumn {
    /// 論理型。
    pub logical_type: LogicalType,
    /// エンコーディング種別。
    pub encoding: crate::columnar::encoding_v2::EncodingV2,
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
    /// ColumnSegmentV2 を埋め込んだエンベロープをチェックサム付きでシリアライズする。
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let envelope = VectorSegmentEnvelope {
            version: VECTOR_SEGMENT_VERSION,
            segment_id: self.segment_id,
            dimension: self.dimension,
            metric: self.metric,
            statistics: self.statistics.clone(),
            segment: self.build_column_segment()?,
        };

        let mut payload =
            bincode::serialize(&envelope).map_err(|e| Error::InvalidFormat(e.to_string()))?;
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

        let envelope: VectorSegmentEnvelope =
            bincode::deserialize(payload).map_err(|e| Error::InvalidFormat(e.to_string()))?;
        if envelope.version != VECTOR_SEGMENT_VERSION {
            return Err(Error::InvalidFormat(
                "unsupported VectorSegment version".into(),
            ));
        }

        let segment = Self::from_column_segment(envelope)?;
        segment.validate()?;
        Ok(segment)
    }

    /// 内部整合性チェック。
    fn validate(&self) -> Result<()> {
        if self.dimension == 0 {
            return Err(Error::InvalidFormat("dimension must be > 0".into()));
        }
        let n = self.num_vectors as usize;

        // vectors: Float32 かつ総要素数一致（num_vectors * dimension）
        if self.vectors.logical_type != LogicalType::Float32 {
            return Err(Error::InvalidFormat(
                "vectors.logical_type must be Float32".into(),
            ));
        }
        let expected_values = n
            .checked_mul(self.dimension)
            .ok_or_else(|| Error::InvalidFormat("num_vectors * dimension overflow".into()))?;
        if self.vectors.num_values as usize != expected_values {
            return Err(Error::InvalidFormat(
                "vectors.num_values mismatch num_vectors * dimension".into(),
            ));
        }
        if let Some(bm) = &self.vectors.null_bitmap {
            if bm.len() != expected_values {
                return Err(Error::InvalidFormat(
                    "vectors.null_bitmap length mismatch".into(),
                ));
            }
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
        if let Some(bm) = &self.keys.null_bitmap {
            if bm.len() != n {
                return Err(Error::InvalidFormat(
                    "keys.null_bitmap length mismatch".into(),
                ));
            }
        }
        // deleted bitmap 長さ
        if self.deleted.len() != n {
            return Err(Error::InvalidFormat(
                "deleted bitmap length mismatch num_vectors".into(),
            ));
        }
        let valid_count = (0..n).filter(|&i| self.deleted.get(i)).count() as u64;
        let deleted_count = self.num_vectors.saturating_sub(valid_count);
        let active_count = valid_count;

        // metadata 各列の行数整合
        if let Some(meta_cols) = &self.metadata {
            for (idx, col) in meta_cols.iter().enumerate() {
                if col.num_values as usize != n {
                    return Err(Error::InvalidFormat(format!(
                        "metadata column {} num_values mismatch num_vectors",
                        idx
                    )));
                }
                if let Some(bm) = &col.null_bitmap {
                    if bm.len() != n {
                        return Err(Error::InvalidFormat(format!(
                            "metadata column {} null_bitmap length mismatch",
                            idx
                        )));
                    }
                }
            }
        }

        // statistics 整合性
        if self.statistics.row_count != self.num_vectors {
            return Err(Error::InvalidFormat(
                "statistics.row_count mismatch num_vectors".into(),
            ));
        }
        let active_deleted = self
            .statistics
            .active_count
            .saturating_add(self.statistics.deleted_count);
        if active_deleted != self.num_vectors {
            return Err(Error::InvalidFormat(
                "statistics.active_count + deleted_count mismatch num_vectors".into(),
            ));
        }
        if self.statistics.deleted_count != deleted_count {
            return Err(Error::InvalidFormat(
                "statistics.deleted_count mismatch deleted bitmap".into(),
            ));
        }
        if self.statistics.active_count != active_count {
            return Err(Error::InvalidFormat(
                "statistics.active_count mismatch deleted bitmap".into(),
            ));
        }
        if self.statistics.row_count > 0 {
            let expected_ratio =
                (self.statistics.deleted_count as f32) / (self.statistics.row_count as f32);
            if (self.statistics.deletion_ratio - expected_ratio).abs() > 1e-6 {
                return Err(Error::InvalidFormat(
                    "statistics.deletion_ratio mismatch deleted_count/row_count".into(),
                ));
            }
        } else if self.statistics.deletion_ratio != 0.0 {
            return Err(Error::InvalidFormat(
                "statistics.deletion_ratio must be 0 when row_count is 0".into(),
            ));
        }

        Ok(())
    }

    /// EncodedColumn 群を ColumnSegmentV2 へ書き出す。
    fn build_column_segment(&self) -> Result<ColumnSegmentV2> {
        use crate::columnar::segment_v2::SegmentConfigV2;

        let n = self.num_vectors as usize;
        let dim = self.dimension;
        let compression = CompressionV2::None;

        // vectors -> Column::Fixed (byte packed per vector)
        let (vec_col_decoded, vec_bm) = self.decode_column(&self.vectors)?;
        let floats = match vec_col_decoded {
            Column::Float32(v) => v,
            other => {
                return Err(Error::InvalidFormat(format!(
                    "vectors column must decode to Float32, got {:?}",
                    other
                )))
            }
        };
        if floats.len() != n * dim {
            return Err(Error::InvalidFormat(
                "decoded vectors length mismatch dimension".into(),
            ));
        }
        let fixed_len = dim
            .checked_mul(4)
            .ok_or_else(|| Error::InvalidFormat("dimension overflow".into()))?;
        if fixed_len > u16::MAX as usize {
            return Err(Error::InvalidFormat("dimension too large for Fixed".into()));
        }
        let mut fixed_values = Vec::with_capacity(n);
        for chunk in floats.chunks(dim) {
            let mut buf = Vec::with_capacity(fixed_len);
            for v in chunk {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            fixed_values.push(buf);
        }
        let vectors_column = Column::Binary(fixed_values);

        // keys
        let (keys_col_decoded, keys_bm) = self.decode_column(&self.keys)?;
        let keys_column = match keys_col_decoded {
            Column::Int64(v) => {
                if v.len() != n {
                    return Err(Error::InvalidFormat(
                        "keys length mismatch num_vectors".into(),
                    ));
                }
                Column::Int64(v)
            }
            other => {
                return Err(Error::InvalidFormat(format!(
                    "keys column must decode to Int64, got {:?}",
                    other
                )))
            }
        };

        // deleted bitmap -> Column::Bool
        let mut deleted_vals = Vec::with_capacity(n);
        for i in 0..n {
            deleted_vals.push(self.deleted.get(i));
        }
        let deleted_column = Column::Bool(deleted_vals);

        // metadata
        let mut metadata_columns = Vec::new();
        let mut metadata_bitmaps = Vec::new();
        if let Some(meta_cols) = &self.metadata {
            for col in meta_cols {
                let (decoded, bm) = self.decode_column(col)?;
                if column_length(&decoded) != n {
                    return Err(Error::InvalidFormat(
                        "metadata length mismatch num_vectors".into(),
                    ));
                }
                let normalized = if let LogicalType::Fixed(len) = col.logical_type {
                    ensure_fixed_column(decoded, len as usize)?
                } else {
                    decoded
                };
                metadata_columns.push(normalized);
                metadata_bitmaps.push(bm);
            }
        }

        let mut schema_columns = Vec::new();
        let mut columns = Vec::new();
        let mut bitmaps = Vec::new();

        schema_columns.push(ColumnSchema {
            name: "vectors".into(),
            logical_type: LogicalType::Binary,
            nullable: vec_bm.is_some(),
            fixed_len: Some(fixed_len as u32),
        });
        columns.push(vectors_column);
        bitmaps.push(vec_bm);

        schema_columns.push(ColumnSchema {
            name: "keys".into(),
            logical_type: LogicalType::Int64,
            nullable: keys_bm.is_some(),
            fixed_len: None,
        });
        columns.push(keys_column);
        bitmaps.push(keys_bm);

        schema_columns.push(ColumnSchema {
            name: "deleted".into(),
            logical_type: LogicalType::Bool,
            nullable: false,
            fixed_len: None,
        });
        columns.push(deleted_column);
        bitmaps.push(None);

        for (idx, col) in metadata_columns.into_iter().enumerate() {
            let bm = metadata_bitmaps.get(idx).cloned().unwrap_or(None);
            schema_columns.push(ColumnSchema {
                name: format!("meta_{idx}"),
                logical_type: column_logical_type(&col)?,
                nullable: bm.is_some(),
                fixed_len: match &col {
                    Column::Fixed { len, .. } => Some(*len as u32),
                    _ => None,
                },
            });
            columns.push(col);
            bitmaps.push(bm);
        }

        let schema = Schema {
            columns: schema_columns,
        };
        let batch = RecordBatch::new(schema, columns, bitmaps);

        let mut writer = SegmentWriterV2::new(SegmentConfigV2 {
            compression,
            ..Default::default()
        });
        writer
            .write_batch(batch)
            .map_err(|e| Error::InvalidFormat(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| Error::InvalidFormat(e.to_string()))
    }

    /// ColumnSegmentV2 から VectorSegment を復元する。
    fn from_column_segment(envelope: VectorSegmentEnvelope) -> Result<Self> {
        let num_vectors = envelope.segment.meta.num_rows;

        // 読み出し
        let reader = SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(
            envelope.segment.data.clone(),
        )))
        .map_err(|e| Error::InvalidFormat(e.to_string()))?;

        let column_count = envelope.segment.meta.schema.column_count();
        let mut combined_columns: Vec<Option<Column>> = vec![None; column_count];
        let mut combined_bitmaps: Vec<Option<Bitmap>> = vec![None; column_count];

        for batch in reader
            .iter_row_groups()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::InvalidFormat(e.to_string()))?
        {
            for (idx, col) in batch.columns.iter().enumerate() {
                if idx >= combined_columns.len() {
                    return Err(Error::InvalidFormat("column index out of bounds".into()));
                }
                combined_columns[idx] =
                    Some(append_column(combined_columns[idx].take(), col.clone())?);
            }
            for (idx, bm) in batch.null_bitmaps.iter().enumerate() {
                if idx >= combined_bitmaps.len() {
                    return Err(Error::InvalidFormat("bitmap index out of bounds".into()));
                }
                combined_bitmaps[idx] = append_bitmap(combined_bitmaps[idx].take(), bm.clone());
            }
        }

        // vectors (index 0)
        let vectors_col = combined_columns
            .get(0)
            .and_then(|c| c.clone())
            .ok_or_else(|| Error::InvalidFormat("missing vectors column".into()))?;
        let vec_bitmap = combined_bitmaps.get(0).cloned().unwrap_or(None);
        let vectors =
            encode_vectors_from_fixed(vectors_col, vec_bitmap.clone(), envelope.dimension)?;

        // keys (index 1)
        let keys_col = combined_columns
            .get(1)
            .and_then(|c| c.clone())
            .ok_or_else(|| Error::InvalidFormat("missing keys column".into()))?;
        let keys_bitmap = combined_bitmaps.get(1).cloned().unwrap_or(None);
        let keys =
            encode_generic_column(keys_col, keys_bitmap, LogicalType::Int64, EncodingV2::Plain)?;

        // deleted (index 2)
        let deleted_col = combined_columns
            .get(2)
            .and_then(|c| c.clone())
            .ok_or_else(|| Error::InvalidFormat("missing deleted column".into()))?;
        let deleted = column_to_bitmap(deleted_col, num_vectors as usize)?;

        // metadata
        let mut metadata_cols = Vec::new();
        for idx in 3..combined_columns.len() {
            let col = combined_columns[idx]
                .clone()
                .ok_or_else(|| Error::InvalidFormat("missing metadata column".into()))?;
            let bm = combined_bitmaps.get(idx).cloned().unwrap_or(None);
            let logical_type = column_logical_type(&col)?;
            let encoded = encode_generic_column(col, bm, logical_type, EncodingV2::Plain)?;
            metadata_cols.push(encoded);
        }

        let segment = VectorSegment {
            segment_id: envelope.segment_id,
            dimension: envelope.dimension,
            metric: envelope.metric,
            num_vectors,
            vectors,
            keys,
            deleted,
            metadata: if metadata_cols.is_empty() {
                None
            } else {
                Some(metadata_cols)
            },
            statistics: envelope.statistics,
        };
        segment.validate()?;
        Ok(segment)
    }

    fn decode_column(&self, col: &EncodedColumn) -> Result<(Column, Option<Bitmap>)> {
        let decoder = create_decoder(col.encoding);
        let encoded_bytes = col.data.clone();

        decoder
            .decode(
                &encoded_bytes,
                col.num_values as usize,
                col.logical_type,
            )
            .map_err(|e| Error::InvalidFormat(e.to_string()))
    }

}

fn column_logical_type(col: &Column) -> Result<LogicalType> {
    match col {
        Column::Int64(_) => Ok(LogicalType::Int64),
        Column::Float32(_) => Ok(LogicalType::Float32),
        Column::Float64(_) => Ok(LogicalType::Float64),
        Column::Bool(_) => Ok(LogicalType::Bool),
        Column::Binary(_) => Ok(LogicalType::Binary),
        Column::Fixed { len, .. } => {
            Ok(LogicalType::Fixed((*len).try_into().map_err(|_| {
                Error::InvalidFormat("fixed length too large".into())
            })?))
        }
    }
}

fn column_length(col: &Column) -> usize {
    match col {
        Column::Int64(v) => v.len(),
        Column::Float32(v) => v.len(),
        Column::Float64(v) => v.len(),
        Column::Bool(v) => v.len(),
        Column::Binary(v) => v.len(),
        Column::Fixed { values, .. } => values.len(),
    }
}

fn append_column(current: Option<Column>, next: Column) -> Result<Column> {
    match (current, next) {
        (None, n) => Ok(n),
        (Some(Column::Int64(mut a)), Column::Int64(b)) => {
            a.extend_from_slice(&b);
            Ok(Column::Int64(a))
        }
        (Some(Column::Float32(mut a)), Column::Float32(b)) => {
            a.extend_from_slice(&b);
            Ok(Column::Float32(a))
        }
        (Some(Column::Float64(mut a)), Column::Float64(b)) => {
            a.extend_from_slice(&b);
            Ok(Column::Float64(a))
        }
        (Some(Column::Bool(mut a)), Column::Bool(b)) => {
            a.extend_from_slice(&b);
            Ok(Column::Bool(a))
        }
        (Some(Column::Binary(mut a)), Column::Binary(b)) => {
            a.extend_from_slice(&b);
            Ok(Column::Binary(a))
        }
        (
            Some(Column::Fixed { len, mut values }),
            Column::Fixed {
                len: len2,
                values: v,
            },
        ) => {
            if len != len2 {
                return Err(Error::InvalidFormat("fixed length mismatch".into()));
            }
            values.extend_from_slice(&v);
            Ok(Column::Fixed { len, values })
        }
        _ => Err(Error::InvalidFormat(
            "column type mismatch when merging row groups".into(),
        )),
    }
}

fn append_bitmap(current: Option<Bitmap>, next: Option<Bitmap>) -> Option<Bitmap> {
    match (current, next) {
        (None, None) => None,
        (Some(b), None) => Some(b),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => {
            let mut merged: Vec<bool> = Vec::with_capacity(a.len() + b.len());
            for i in 0..a.len() {
                merged.push(a.get(i));
            }
            for i in 0..b.len() {
                merged.push(b.get(i));
            }
            Some(Bitmap::from_bools(&merged))
        }
    }
}

fn encode_vectors_from_fixed(
    col: Column,
    bitmap: Option<Bitmap>,
    dimension: usize,
) -> Result<EncodedColumn> {
    let values = match col {
        Column::Binary(values) => values,
        Column::Fixed { values, len } => {
            if len != dimension * 4 {
                return Err(Error::InvalidFormat(
                    "vectors fixed length mismatch dimension".into(),
                ));
            }
            values
        }
        other => {
            return Err(Error::InvalidFormat(format!(
                "vectors column must be Binary/Fixed, got {:?}",
                other
            )))
        }
    };
    let expected_len = dimension
        .checked_mul(4)
        .ok_or_else(|| Error::InvalidFormat("dimension overflow".into()))?;

    let mut floats = Vec::with_capacity(values.len() * dimension);
    for chunk in values {
        if chunk.len() != expected_len {
            return Err(Error::InvalidFormat(
                "vector payload length mismatch".into(),
            ));
        }
        for bytes in chunk.chunks_exact(4) {
            floats.push(f32::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| Error::InvalidFormat("vector chunk".into()))?,
            ));
        }
    }

    let encoder = create_encoder(EncodingV2::ByteStreamSplit);
    let encoded = encoder
        .encode(&Column::Float32(floats.clone()), bitmap.as_ref())
        .map_err(|e| Error::InvalidFormat(e.to_string()))?;

    Ok(EncodedColumn {
        logical_type: LogicalType::Float32,
        encoding: EncodingV2::ByteStreamSplit,
        num_values: floats.len() as u64,
        data: encoded,
        null_bitmap: bitmap,
    })
}

fn encode_generic_column(
    col: Column,
    bitmap: Option<Bitmap>,
    logical_type: LogicalType,
    encoding: EncodingV2,
) -> Result<EncodedColumn> {
    let col = match logical_type {
        LogicalType::Fixed(len) => ensure_fixed_column(col, len as usize)?,
        _ => col,
    };
    let encoder = create_encoder(encoding);
    let encoded = encoder
        .encode(&col, bitmap.as_ref())
        .map_err(|e| Error::InvalidFormat(e.to_string()))?;

    Ok(EncodedColumn {
        logical_type,
        encoding,
        num_values: column_length(&col) as u64,
        data: encoded,
        null_bitmap: bitmap,
    })
}

fn column_to_bitmap(col: Column, expected_len: usize) -> Result<Bitmap> {
    match col {
        Column::Bool(values) => {
            if values.len() != expected_len {
                return Err(Error::InvalidFormat(
                    "deleted length mismatch num_vectors".into(),
                ));
            }
            if values.iter().all(|v| *v) {
                Ok(Bitmap::all_valid(expected_len))
            } else {
                Ok(Bitmap::from_bools(&values))
            }
        }
        other => Err(Error::InvalidFormat(format!(
            "deleted column must be Bool, got {:?}",
            other
        ))),
    }
}

fn ensure_fixed_column(col: Column, len: usize) -> Result<Column> {
    match col {
        Column::Fixed { len: l, values } => {
            if l != len {
                return Err(Error::InvalidFormat(
                    "fixed column length mismatch expected length".into(),
                ));
            }
            Ok(Column::Fixed { len, values })
        }
        Column::Binary(values) => {
            if values.iter().any(|v| v.len() != len) {
                return Err(Error::InvalidFormat(
                    "binary column has variable-length values for Fixed type".into(),
                ));
            }
            Ok(Column::Fixed { len, values })
        }
        other => Err(Error::InvalidFormat(format!(
            "column must be Fixed/Binary for Fixed logical type, got {:?}",
            other
        ))),
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

    fn encode_f32(values: &[f32]) -> EncodedColumn {
        let encoder = create_encoder(EncodingV2::ByteStreamSplit);
        let data = encoder
            .encode(&Column::Float32(values.to_vec()), None)
            .unwrap();
        EncodedColumn {
            logical_type: LogicalType::Float32,
            encoding: EncodingV2::ByteStreamSplit,
            num_values: values.len() as u64,
            data,
            null_bitmap: None,
        }
    }

    fn encode_i64(values: &[i64]) -> EncodedColumn {
        let encoder = create_encoder(EncodingV2::Plain);
        let data = encoder
            .encode(&Column::Int64(values.to_vec()), None)
            .unwrap();
        EncodedColumn {
            logical_type: LogicalType::Int64,
            encoding: EncodingV2::Plain,
            num_values: values.len() as u64,
            data,
            null_bitmap: None,
        }
    }

    fn sample_segment() -> VectorSegment {
        let vectors = vec![1.0f32, 2.0, 3.0, 4.0];
        VectorSegment {
            segment_id: 42,
            dimension: 4,
            metric: Metric::Cosine,
            num_vectors: 1,
            vectors: encode_f32(&vectors),
            keys: encode_i64(&[0]),
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
    fn roundtrip_with_checksum_and_segment_v2() {
        let seg = sample_segment();
        let bytes = seg.to_bytes().unwrap();
        let restored = VectorSegment::from_bytes(&bytes).unwrap();
        assert_eq!(restored.segment_id, seg.segment_id);
        assert_eq!(restored.dimension, seg.dimension);
        assert_eq!(restored.metric, seg.metric);
        assert_eq!(restored.num_vectors, seg.num_vectors);
        assert_eq!(restored.vectors.logical_type, LogicalType::Float32);
        assert_eq!(restored.keys.logical_type, LogicalType::Int64);
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
