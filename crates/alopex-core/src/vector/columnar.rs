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
use crate::columnar::statistics::{ScalarValue, VectorSegmentStatistics};
use crate::vector::simd::select_kernel;
use crate::storage::compression::CompressionV2;
use crate::vector::{CompactionResult, DeleteResult, Metric};
use crate::{Error, Result};
use std::collections::HashSet;

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
        let mut deleted_count = 0u64;
        let mut active_count = 0u64;
        for idx in 0..n {
            if self.deleted.get(idx) {
                deleted_count += 1;
            } else {
                active_count += 1;
            }
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

    /// ベクトルデータをデコード（FlattenされたFloat32配列）。
    pub fn decode_vectors(&self) -> Result<Vec<f32>> {
        let decoder = create_decoder(self.vectors.encoding);
        let (col, _) = decoder
            .decode(
                &self.vectors.data,
                self.vectors.num_values as usize,
                self.vectors.logical_type,
            )
            .map_err(|e| Error::InvalidFormat(e.to_string()))?;
        match col {
            Column::Float32(v) => Ok(v),
            other => Err(Error::InvalidFormat(format!(
                "vectors column decoded to unexpected type {:?}",
                other
            ))),
        }
    }

    /// キー列をデコード。
    pub fn decode_keys(&self) -> Result<Vec<i64>> {
        let decoder = create_decoder(self.keys.encoding);
        let (col, _) = decoder
            .decode(
                &self.keys.data,
                self.keys.num_values as usize,
                self.keys.logical_type,
            )
            .map_err(|e| Error::InvalidFormat(e.to_string()))?;
        match col {
            Column::Int64(v) => Ok(v),
            other => Err(Error::InvalidFormat(format!(
                "keys column decoded to unexpected type {:?}",
                other
            ))),
        }
    }

    /// 削除統計を deleted ビットマップから再計算する（norm は不変）。
    fn recompute_deletion_stats(&mut self) {
        let row_count = self.num_vectors;
        let deleted_count = (0..row_count as usize)
            .filter(|&i| self.deleted.get(i))
            .count() as u64;
        let active_count = row_count.saturating_sub(deleted_count);
        self.statistics.row_count = row_count;
        self.statistics.deleted_count = deleted_count;
        self.statistics.active_count = active_count;
        self.statistics.deletion_ratio = if row_count > 0 {
            deleted_count as f32 / row_count as f32
        } else {
            0.0
        };
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
        let deleted_column = Column::Bool((0..n).map(|i| self.deleted.get(i)).collect());

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

fn slice_column(col: &Column, indices: &[usize]) -> Result<Column> {
    Ok(match col {
        Column::Int64(v) => Column::Int64(take_indices(v, indices)?),
        Column::Float32(v) => Column::Float32(take_indices(v, indices)?),
        Column::Float64(v) => Column::Float64(take_indices(v, indices)?),
        Column::Bool(v) => Column::Bool(take_indices(v, indices)?),
        Column::Binary(v) => Column::Binary(take_indices(v, indices)?),
        Column::Fixed { len, values } => Column::Fixed {
            len: *len,
            values: take_indices(values, indices)?,
        },
    })
}

fn slice_bitmap(bm: Option<Bitmap>, indices: &[usize]) -> Option<Bitmap> {
    bm.map(|source| {
        let mut sliced = Bitmap::new_zeroed(indices.len());
        for (dst_idx, src_idx) in indices.iter().enumerate() {
            if source.get(*src_idx) {
                sliced.set(dst_idx, true);
            }
        }
        sliced
    })
}

fn take_indices<T: Clone>(values: &[T], indices: &[usize]) -> Result<Vec<T>> {
    let mut out = Vec::with_capacity(indices.len());
    for &idx in indices {
        out.push(
            values
                .get(idx)
                .cloned()
                .ok_or_else(|| Error::InvalidFormat("index out of bounds".into()))?,
        );
    }
    Ok(out)
}

fn column_to_bitmap(col: Column, expected_len: usize) -> Result<Bitmap> {
    match col {
        Column::Bool(values) => {
            if values.len() != expected_len {
                return Err(Error::InvalidFormat(
                    "deleted length mismatch num_vectors".into(),
                ));
            }
            Ok(if values.iter().all(|v| !*v) {
                Bitmap::new(expected_len)
            } else if values.iter().all(|v| *v) {
                Bitmap::all_valid(expected_len)
            } else {
                Bitmap::from_bools(&values)
            })
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

/// VectorStore の設定。
///
/// # Examples
/// ```
/// use alopex_core::vector::{VectorStoreConfig, Metric};
/// let cfg = VectorStoreConfig { dimension: 128, metric: Metric::Cosine, ..Default::default() };
/// assert_eq!(cfg.dimension, 128);
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorStoreConfig {
    /// ベクトル次元。
    pub dimension: usize,
    /// デフォルトメトリック。
    pub metric: Metric,
    /// 1 セグメントあたりの最大ベクトル数。
    pub segment_max_vectors: usize,
    /// 将来のコンパクション閾値（現状は設定のみ）。
    pub compaction_threshold: f32,
    /// ベクトルエンコーディング方式。
    pub encoding: EncodingV2,
}

impl Default for VectorStoreConfig {
    fn default() -> Self {
        Self {
            dimension: 128,
            metric: Metric::Cosine,
            segment_max_vectors: 65_536,
            compaction_threshold: 0.3,
            encoding: EncodingV2::ByteStreamSplit,
        }
    }
}

/// ベクトル追加結果。
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendResult {
    /// 追加されたベクトル数。
    pub vectors_added: usize,
    /// 新規作成されたセグメント数。
    pub segments_created: usize,
}

/// 検索パラメータ。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VectorSearchParams {
    /// クエリベクトル。
    pub query: Vec<f32>,
    /// 使用するメトリック。
    pub metric: Metric,
    /// 取得する Top-K 件数。
    pub top_k: usize,
    /// メタデータ列のプロジェクション（0-based、metadata 配列のインデックス）。
    pub projection: Option<Vec<usize>>,
    /// フィルタマスク（行単位、true=通過）。
    pub filter_mask: Option<Vec<bool>>,
}

/// 検索結果。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VectorSearchResult {
    /// ベクトル識別子。
    pub row_id: i64,
    /// スコア（DESCソート）。
    pub score: f32,
    /// 投影されたカラム（現状空配列）。
    pub columns: Vec<ScalarValue>,
}

/// 検索統計。
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchStats {
    /// 走査したセグメント数。
    pub segments_scanned: u64,
    /// プルーニングでスキップしたセグメント数。
    pub segments_pruned: u64,
    /// 走査した行数（削除済み含む）。
    pub rows_scanned: u64,
    /// スコア計算した行数。
    pub rows_matched: u64,
}

/// VectorStore のシンプルなインメモリ実装。
#[derive(Debug)]
pub struct VectorStoreManager {
    config: VectorStoreConfig,
    segments: Vec<VectorSegment>,
    next_segment_id: u64,
}

impl VectorStoreManager {
    /// 新しい VectorStoreManager を生成。
    pub fn new(config: VectorStoreConfig) -> Self {
        Self {
            config,
            segments: Vec::new(),
            next_segment_id: 0,
        }
    }

    /// ベクトルバッチを追加する。
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use alopex_core::vector::{VectorStoreManager, VectorStoreConfig};
    /// # use alopex_core::Result;
    /// # async fn demo() -> Result<()> {
    /// let mut mgr = VectorStoreManager::new(VectorStoreConfig { dimension: 2, ..Default::default() });
    /// let keys = vec![1, 2];
    /// let vecs = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
    /// mgr.append_batch(&keys, &vecs).await?;
    /// # Ok(()) }
    /// ```
    pub async fn append_batch(&mut self, keys: &[i64], vectors: &[Vec<f32>]) -> Result<AppendResult> {
        if keys.len() != vectors.len() {
            return Err(Error::InvalidFormat("keys/vectors length mismatch".into()));
        }
        if vectors.is_empty() {
            return Ok(AppendResult::default());
        }
        let dim = self.config.dimension;
        for (idx, v) in vectors.iter().enumerate() {
            if v.len() != dim {
                return Err(Error::DimensionMismatch {
                    expected: dim,
                    actual: v.len(),
                });
            }
            if contains_nan_or_inf(v) {
                return Err(Error::InvalidVector {
                    index: idx,
                    reason: "vector contains NaN or Inf".into(),
                });
            }
        }

        let mut vectors_added = 0usize;
        let mut segments_created = 0usize;
        let mut start = 0usize;
        while start < vectors.len() {
            let end = usize::min(start + self.config.segment_max_vectors, vectors.len());
            let slice = &vectors[start..end];
            let key_slice = &keys[start..end];

            let segment = self.build_segment(key_slice, slice)?;
            self.segments.push(segment);
            self.next_segment_id += 1;
            vectors_added += slice.len();
            segments_created += 1;
            start = end;
        }

        Ok(AppendResult {
            vectors_added,
            segments_created,
        })
    }

    /// コンパクション対象セグメントを取得する。
    pub fn segments_needing_compaction(&self) -> Vec<u64> {
        let threshold = self.config.compaction_threshold;
        if threshold >= 1.0 {
            return Vec::new();
        }
        let mut ids = Vec::new();
        for seg in &self.segments {
            if seg.statistics.deletion_ratio >= threshold && seg.statistics.deletion_ratio > 0.0 {
                ids.push(seg.segment_id);
            }
        }
        ids
    }

    /// 指定キーのベクトルを論理削除する（in-memory）。
    pub async fn delete_batch(&mut self, keys: &[i64]) -> Result<DeleteResult> {
        if keys.is_empty() {
            return Ok(DeleteResult::default());
        }
        let key_set: HashSet<i64> = keys.iter().copied().collect();
        let mut result = DeleteResult::default();

        for segment in &mut self.segments {
            let decoded_keys = segment.decode_keys()?;
            let mut modified = false;
            for (idx, key) in decoded_keys.iter().enumerate() {
                if !key_set.contains(key) {
                    continue;
                }
                if !segment.deleted.get(idx) {
                    segment.deleted.set(idx, true);
                    result.vectors_deleted = result.vectors_deleted.saturating_add(1);
                    modified = true;
                }
            }

            if modified {
                segment.recompute_deletion_stats();
                result.segments_modified.push(segment.segment_id);
            }
        }

        Ok(result)
    }

    /// セグメントをコンパクションして新セグメントに置換する。
    pub async fn compact_segment(&mut self, segment_id: u64) -> Result<CompactionResult> {
        let pos = self
            .segments
            .iter()
            .position(|s| s.segment_id == segment_id)
            .ok_or(Error::NotFound)?;

        let old = self
            .segments
            .get(pos)
            .cloned()
            .ok_or(Error::NotFound)?;
        let old_size = old.to_bytes().map(|b| b.len() as u64).unwrap_or(0);

        let active_indices: Vec<usize> = (0..old.num_vectors as usize)
            .filter(|&i| !old.deleted.get(i))
            .collect();

        if active_indices.is_empty() {
            self.segments.remove(pos);
            return Ok(CompactionResult {
                old_segment_id: segment_id,
                new_segment_id: None,
                vectors_removed: old.num_vectors,
                space_reclaimed: old_size,
            });
        }

        let decoded_vectors = old.decode_vectors()?;
        let decoded_keys = old.decode_keys()?;

        let mut new_vecs = Vec::with_capacity(active_indices.len());
        for &idx in &active_indices {
            let start = idx * self.config.dimension;
            let end = start + self.config.dimension;
            new_vecs.push(decoded_vectors[start..end].to_vec());
        }
        let new_keys: Vec<i64> = active_indices
            .iter()
            .map(|&i| {
                decoded_keys
                    .get(i)
                    .copied()
                    .ok_or_else(|| Error::InvalidFormat("missing key".into()))
            })
            .collect::<Result<_>>()?;

        let mut new_segment = self.build_segment(&new_keys, &new_vecs)?;

        // metadata を再構成（存在する場合）。
        if let Some(meta_cols) = &old.metadata {
            let mut new_meta = Vec::with_capacity(meta_cols.len());
            for col in meta_cols {
                let (decoded_col, bitmap) = old.decode_column(col)?;
                let sliced_col = slice_column(&decoded_col, &active_indices)?;
                let sliced_bitmap = slice_bitmap(bitmap, &active_indices);
                let encoded = encode_generic_column(
                    sliced_col,
                    sliced_bitmap,
                    col.logical_type,
                    col.encoding,
                )?;
                new_meta.push(encoded);
            }
            if !new_meta.is_empty() {
                new_segment.metadata = Some(new_meta);
            }
        }

        // 新セグメントIDを割当て、置換。
        let new_segment_id = self.next_segment_id;
        new_segment.segment_id = new_segment_id; // build_segment で設定した値と合わせるため明示
        self.next_segment_id += 1;
        let new_size = new_segment.to_bytes().map(|b| b.len() as u64).unwrap_or(0);
        let space_reclaimed = old_size.saturating_sub(new_size);
        let vectors_removed = old.num_vectors.saturating_sub(new_segment.num_vectors);

        self.segments[pos] = new_segment;

        Ok(CompactionResult {
            old_segment_id: segment_id,
            new_segment_id: Some(new_segment_id),
            vectors_removed,
            space_reclaimed,
        })
    }

    /// ベクトル検索。
    ///
    /// # Errors
    /// - `DimensionMismatch`: クエリ次元が設定と異なる場合。
    /// - `InvalidVector`: クエリに NaN/Inf が含まれる場合。
    pub fn search(&self, params: VectorSearchParams) -> Result<Vec<VectorSearchResult>> {
        let mut stats = SearchStats::default();
        let (results, _) = self.search_internal(params, &mut stats)?;
        Ok(results)
    }

    /// 統計付き検索。
    ///
    /// `search` と同じ結果に加え、走査/プルーニング件数を返す。
    pub fn search_with_stats(
        &self,
        params: VectorSearchParams,
    ) -> Result<(Vec<VectorSearchResult>, SearchStats)> {
        let mut stats = SearchStats::default();
        let (results, stats) = self.search_internal(params, &mut stats)?;
        Ok((results, stats))
    }

    fn search_internal(
        &self,
        params: VectorSearchParams,
        stats: &mut SearchStats,
    ) -> Result<(Vec<VectorSearchResult>, SearchStats)> {
        if params.top_k == 0 {
            return Ok((Vec::new(), stats.clone()));
        }
        if params.query.len() != self.config.dimension {
            return Err(Error::DimensionMismatch {
                expected: self.config.dimension,
                actual: params.query.len(),
            });
        }
        if contains_nan_or_inf(&params.query) {
            return Err(Error::InvalidVector {
                index: 0,
                reason: "query contains NaN or Inf".into(),
            });
        }

        let mut candidates: Vec<VectorSearchResult> = Vec::new();
        let query_norm = params
            .query
            .iter()
            .map(|v| v * v)
            .sum::<f32>()
            .sqrt();
        let mut row_offset = 0u64;
        for segment in &self.segments {
            if segment.statistics.deletion_ratio >= 1.0 {
                stats.segments_pruned += 1;
                row_offset += segment.num_vectors;
                continue;
            }
            // pruning by norm range
            if query_norm < segment.statistics.norm_min || query_norm > segment.statistics.norm_max {
                stats.segments_pruned += 1;
                row_offset += segment.num_vectors;
                continue;
            }
            stats.segments_scanned += 1;
            stats.rows_scanned = stats
                .rows_scanned
                .saturating_add(segment.num_vectors);
            let decoded = segment.decode_vectors()?;
            let decoded_keys = segment.decode_keys()?;
            let metadata = decode_metadata(&segment.metadata, segment.num_vectors as usize)?;
            let kernel = select_kernel();
            let mask = params.filter_mask.as_ref();
            for (idx, chunk) in decoded.chunks(self.config.dimension).enumerate() {
                // deleted bitmap uses `true` to mean logically deleted.
                if segment.deleted.get(idx) {
                    continue;
                }
                if let Some(mask_vec) = mask {
                    let global_idx = row_offset as usize + idx;
                    if global_idx >= mask_vec.len() || !mask_vec[global_idx] {
                        continue;
                    }
                }
                let score = match params.metric {
                    Metric::Cosine => kernel.cosine(&params.query, chunk),
                    Metric::L2 => kernel.l2(&params.query, chunk),
                    Metric::InnerProduct => kernel.inner_product(&params.query, chunk),
                };
                let row_id = *decoded_keys
                    .get(idx)
                    .ok_or_else(|| Error::InvalidFormat("missing key".into()))?;
                let columns = if let Some(proj) = &params.projection {
                    let mut cols = Vec::with_capacity(proj.len());
                    for &p in proj {
                        let col = metadata
                            .get(p)
                            .ok_or_else(|| Error::InvalidFormat("projection out of bounds".into()))?;
                        cols.push(
                            col.get(idx)
                                .cloned()
                                .ok_or_else(|| Error::InvalidFormat("projection row out of bounds".into()))?,
                        );
                    }
                    cols
                } else {
                    Vec::new()
                };
                candidates.push(VectorSearchResult {
                    row_id,
                    score,
                    columns,
                });
                stats.rows_matched += 1;
            }
            row_offset += segment.num_vectors;
        }

        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        candidates.truncate(params.top_k);
        Ok((candidates, stats.clone()))
    }

    fn build_segment(&mut self, keys: &[i64], vectors: &[Vec<f32>]) -> Result<VectorSegment> {
        let mut flattened = Vec::with_capacity(vectors.len() * self.config.dimension);
        for v in vectors {
            flattened.extend_from_slice(v);
        }
        let vec_enc = encode_generic_column(
            Column::Float32(flattened),
            None,
            LogicalType::Float32,
            self.config.encoding,
        )?;
        let key_enc = encode_generic_column(
            Column::Int64(keys.to_vec()),
            None,
            LogicalType::Int64,
            EncodingV2::Plain,
        )?;
        let deleted = Bitmap::new_zeroed(keys.len());
        let stats = compute_stats(vectors, Some(&deleted));
        let segment = VectorSegment {
            segment_id: self.next_segment_id,
            dimension: self.config.dimension,
            metric: self.config.metric,
            num_vectors: keys.len() as u64,
            vectors: vec_enc,
            keys: key_enc,
            deleted,
            metadata: None,
            statistics: stats,
        };
        Ok(segment)
    }
}

fn compute_stats(vectors: &[Vec<f32>], deleted: Option<&Bitmap>) -> VectorSegmentStatistics {
    let row_count = vectors.len() as u64;
    let null_count = 0;
    let deleted_count = (0..vectors.len())
        .filter(|&i| deleted.map_or(false, |bm| bm.get(i)))
        .count() as u64;
    let active_count = row_count.saturating_sub(deleted_count);
    let mut norm_min = f32::MAX;
    let mut norm_max = f32::MIN;
    for (idx, v) in vectors.iter().enumerate() {
        if deleted.map_or(false, |bm| bm.get(idx)) {
            continue;
        }
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        norm_min = norm_min.min(norm);
        norm_max = norm_max.max(norm);
    }
    if active_count == 0 {
        norm_min = 0.0;
        norm_max = 0.0;
    }
    let deletion_ratio = if row_count > 0 {
        deleted_count as f32 / row_count as f32
    } else {
        0.0
    };

    VectorSegmentStatistics {
        row_count,
        null_count,
        active_count,
        deleted_count,
        deletion_ratio,
        norm_min,
        norm_max,
        min_values: Vec::new(),
        max_values: Vec::new(),
        created_at: 0,
    }
}

fn contains_nan_or_inf(vec: &[f32]) -> bool {
    vec.iter().any(|v| !v.is_finite())
}

fn decode_metadata(
    metadata: &Option<Vec<EncodedColumn>>,
    rows: usize,
) -> Result<Vec<Vec<ScalarValue>>> {
    if let Some(cols) = metadata {
        let mut decoded_cols = Vec::with_capacity(cols.len());
        for col in cols {
            let decoder = create_decoder(col.encoding);
            let (column, _) = decoder
                .decode(&col.data, col.num_values as usize, col.logical_type)
                .map_err(|e| Error::InvalidFormat(e.to_string()))?;
            let values = column_to_scalar_values(column)?;
            if values.len() != rows {
                return Err(Error::InvalidFormat(
                    "metadata column length mismatch num_vectors".into(),
                ));
            }
            decoded_cols.push(values);
        }
        Ok(decoded_cols)
    } else {
        Ok(Vec::new())
    }
}

fn column_to_scalar_values(column: Column) -> Result<Vec<ScalarValue>> {
    Ok(match column {
        Column::Int64(v) => v.into_iter().map(ScalarValue::Int64).collect(),
        Column::Float32(v) => v.into_iter().map(ScalarValue::Float32).collect(),
        Column::Float64(v) => v.into_iter().map(ScalarValue::Float64).collect(),
        Column::Bool(v) => v.into_iter().map(ScalarValue::Bool).collect(),
        Column::Binary(v) => v.into_iter().map(ScalarValue::Binary).collect(),
        Column::Fixed { values, .. } => values.into_iter().map(ScalarValue::Binary).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::encoding_v2::EncodingV2;
    use crate::types::TxnMode;
    use crate::MemoryKV;
    use crate::kv::{KVStore, KVTransaction};
    use crate::txn::TxnManager;
    use crate::ScalarKernel;
    use crate::vector::simd::DistanceKernel;
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

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
            deleted: Bitmap::new_zeroed(1),
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

    #[test]
    fn compute_stats_updates_norms_and_counts() {
        let vectors = vec![vec![3.0f32, 4.0], vec![0.0f32, 0.0]];
        let stats = compute_stats(&vectors, None);
        assert_eq!(stats.row_count, 2);
        assert_eq!(stats.active_count, 2);
        assert_eq!(stats.deleted_count, 0);
        // norms: 5.0 and 0.0
        assert!((stats.norm_min - 0.0).abs() < 1e-6);
        assert!((stats.norm_max - 5.0).abs() < 1e-6);
    }

    #[test]
    fn compute_stats_respects_deleted_bitmap() {
        let vectors = vec![vec![1.0f32], vec![2.0f32]];
        let mut deleted = Bitmap::new_zeroed(2);
        deleted.set(1, true);

        let stats = compute_stats(&vectors, Some(&deleted));
        assert_eq!(stats.row_count, 2);
        assert_eq!(stats.active_count, 1);
        assert_eq!(stats.deleted_count, 1);
        assert!((stats.deletion_ratio - 0.5).abs() < 1e-6);
        assert!((stats.norm_min - 1.0).abs() < 1e-6);
        assert!((stats.norm_max - 1.0).abs() < 1e-6);
    }

    #[test]
    fn delete_batch_marks_keys_and_updates_stats() {
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 2,
            ..Default::default()
        });
        let keys = vec![10, 11, 12];
        let vecs = vec![vec![1.0, 0.0], vec![0.5, 0.5], vec![0.0, 1.0]];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();
        let seg0_id = mgr.segments[0].segment_id;
        let seg1_id = mgr.segments[1].segment_id;

        // 事前に1行を削除済みにしておく（再カウント対象外）。
        if let Some(seg) = mgr.segments.get_mut(0) {
            seg.deleted.set(1, true);
            seg.recompute_deletion_stats();
        }

        let res = block_on(mgr.delete_batch(&[11, 12, 999])).unwrap();
        assert_eq!(res.vectors_deleted, 1);
        assert_eq!(res.segments_modified, vec![seg1_id]);

        // セグメント0は変化なし、セグメント1は全削除でdeletion_ratio=1.0。
        assert_eq!(mgr.segments[0].segment_id, seg0_id);
        assert_eq!(mgr.segments[0].statistics.deleted_count, 1);
        assert_eq!(mgr.segments[0].statistics.active_count, 1);
        assert_eq!(mgr.segments[1].segment_id, seg1_id);
        assert_eq!(mgr.segments[1].statistics.deleted_count, 1);
        assert_eq!(mgr.segments[1].statistics.active_count, 0);
        assert!((mgr.segments[1].statistics.deletion_ratio - 1.0).abs() < 1e-6);
    }

    #[test]
    fn segments_needing_compaction_respects_thresholds() {
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 2,
            compaction_threshold: 0.5,
            ..Default::default()
        });
        let keys = vec![1, 2, 3, 4];
        let vecs = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![0.5, 0.5],
            vec![0.2, 0.8],
        ];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();
        let seg0 = mgr.segments[0].segment_id;

        // mark one row deleted in first segment -> deletion_ratio=0.5
        if let Some(seg) = mgr.segments.get_mut(0) {
            seg.deleted.set(0, true);
            seg.recompute_deletion_stats();
        }

        let mut ids = mgr.segments_needing_compaction();
        assert_eq!(ids, vec![seg0]);

        mgr.config.compaction_threshold = 1.0;
        assert!(mgr.segments_needing_compaction().is_empty());

        mgr.config.compaction_threshold = 0.0;
        ids = mgr.segments_needing_compaction();
        assert_eq!(ids, vec![seg0]);
    }

    #[test]
    fn compact_segment_removes_deleted_and_resets_stats() {
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 4,
            ..Default::default()
        });
        let keys = vec![1, 2, 3];
        let vecs = vec![vec![1.0, 0.0], vec![0.5, 0.5], vec![0.0, 1.0]];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();
        let old_id = mgr.segments[0].segment_id;
        mgr.segments[0].deleted.set(1, true);
        mgr.segments[0].recompute_deletion_stats();

        let res = block_on(mgr.compact_segment(old_id)).unwrap();
        let new_id = res.new_segment_id.expect("new segment");
        assert_eq!(res.old_segment_id, old_id);
        assert_eq!(res.vectors_removed, 1);

        let new_seg = mgr
            .segments
            .iter()
            .find(|s| s.segment_id == new_id)
            .expect("segment exists");
        assert_eq!(new_seg.num_vectors, 2);
        assert_eq!(new_seg.statistics.deleted_count, 0);
        assert_eq!(new_seg.statistics.active_count, 2);
        assert_eq!(new_seg.statistics.deletion_ratio, 0.0);
    }

    #[test]
    fn compact_segment_handles_all_deleted() {
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 4,
            ..Default::default()
        });
        let keys = vec![1, 2];
        let vecs = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();
        let old_id = mgr.segments[0].segment_id;
        mgr.segments[0].deleted.set(0, true);
        mgr.segments[0].deleted.set(1, true);
        mgr.segments[0].recompute_deletion_stats();

        let res = block_on(mgr.compact_segment(old_id)).unwrap();
        assert_eq!(res.old_segment_id, old_id);
        assert_eq!(res.new_segment_id, None);
        assert_eq!(res.vectors_removed, 2);
        assert!(mgr.segments.iter().all(|s| s.segment_id != old_id));
    }

    #[test]
    fn vector_store_append_and_search_with_filter_and_projection() {
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 2,
            ..Default::default()
        });
        let keys = vec![10, 11, 12];
        let vecs = vec![vec![1.0, 0.0], vec![0.5, 0.5], vec![0.0, 1.0]];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();

        // set metadata for projection (one column of ints)
        if let Some(seg) = mgr.segments.get_mut(0) {
            let meta_col = encode_generic_column(
                Column::Int64(vec![100, 200]),
                None,
                LogicalType::Int64,
                EncodingV2::Plain,
            )
            .unwrap();
            seg.metadata = Some(vec![meta_col]);
        }
        if let Some(seg) = mgr.segments.get_mut(1) {
            let meta_col = encode_generic_column(
                Column::Int64(vec![300]),
                None,
                LogicalType::Int64,
                EncodingV2::Plain,
            )
            .unwrap();
            seg.metadata = Some(vec![meta_col]);
        }

        // filter out the middle row, project metadata column 0
        let params = VectorSearchParams {
            query: vec![1.0, 0.0],
            metric: Metric::InnerProduct,
            top_k: 3,
            projection: Some(vec![0]),
            filter_mask: Some(vec![true, false, true]),
        };
        let (results, stats) = mgr.search_with_stats(params).unwrap();
        assert_eq!(stats.rows_scanned, 3);
        assert_eq!(stats.segments_scanned, 2);
        assert_eq!(stats.rows_matched, 2);
        assert_eq!(results.len(), 2);
        // first result should be key 10 with metadata 100
        assert_eq!(results[0].row_id, 10);
        assert_eq!(results[0].columns, vec![ScalarValue::Int64(100)]);
    }

    #[test]
    fn vector_store_topk_is_deterministic_on_ties() {
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 3,
            ..Default::default()
        });
        // All vectors have identical scores; ordering should fall back to row_id ascending.
        let keys = vec![20, 10, 30];
        let vecs = vec![vec![1.0, 0.0], vec![1.0, 0.0], vec![1.0, 0.0]];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();

        let params = VectorSearchParams {
            query: vec![1.0, 0.0],
            metric: Metric::InnerProduct,
            top_k: 3,
            projection: None,
            filter_mask: None,
        };
        let (results, stats) = mgr.search_with_stats(params).unwrap();
        assert_eq!(stats.rows_scanned, 3);
        assert_eq!(results.len(), 3);
        let row_ids: Vec<_> = results.iter().map(|r| r.row_id).collect();
        assert_eq!(row_ids, vec![10, 20, 30]);
    }

    #[test]
    fn vector_store_end_to_end_with_kvs_roundtrip() {
        let mut mgr = VectorStoreManager::new(VectorStoreConfig {
            dimension: 2,
            metric: Metric::InnerProduct,
            segment_max_vectors: 2,
            ..Default::default()
        });
        let keys = vec![1, 2, 3];
        let vecs = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![0.6, 0.8]];
        block_on(mgr.append_batch(&keys, &vecs)).unwrap();

        // 簡易メタデータを各セグメントに付与（整数1列）。
        if let Some(seg) = mgr.segments.get_mut(0) {
            let meta_col = encode_generic_column(
                Column::Int64(vec![100, 200]),
                None,
                LogicalType::Int64,
                EncodingV2::Plain,
            )
            .unwrap();
            seg.metadata = Some(vec![meta_col]);
        }
        if let Some(seg) = mgr.segments.get_mut(1) {
            let meta_col = encode_generic_column(
                Column::Int64(vec![300]),
                None,
                LogicalType::Int64,
                EncodingV2::Plain,
            )
            .unwrap();
            seg.metadata = Some(vec![meta_col]);
        }

        // 永続化: VectorSegment を KVS に保存。
        let store = MemoryKV::new();
        {
            let manager = store.txn_manager();
            let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
            for seg in &mgr.segments {
                let key = key_layout::vector_segment_key(seg.segment_id);
                let bytes = seg.to_bytes().unwrap();
                txn.put(key, bytes).unwrap();
            }
            manager.commit(txn).unwrap();
        }

        // 復元: KVS から VectorSegment を読み出して新しいマネージャに投入。
        let mut restored = VectorStoreManager::new(mgr.config.clone());
        restored.next_segment_id = mgr.next_segment_id;
        {
            let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
            for seg in &mgr.segments {
                let key = key_layout::vector_segment_key(seg.segment_id);
                let bytes = txn.get(&key).unwrap().unwrap();
                let decoded = VectorSegment::from_bytes(&bytes).unwrap();
                restored.segments.push(decoded);
            }
        }

        let params = VectorSearchParams {
            query: vec![1.0, 0.0],
            metric: Metric::InnerProduct,
            top_k: 3,
            projection: Some(vec![0]),
            filter_mask: Some(vec![true, true, true]),
        };
        let (results, _stats) = restored.search_with_stats(params.clone()).unwrap();
        assert_eq!(results.len(), 3);
        // 期待される並び（スコアDESC、同スコアはrow_id ASC）とプロジェクション値を計算。
        let scalar = ScalarKernel::default();
        let expected = vec![
            (keys[0], scalar.inner_product(&params.query, &vecs[0]), ScalarValue::Int64(100)),
            (keys[1], scalar.inner_product(&params.query, &vecs[1]), ScalarValue::Int64(200)),
            (keys[2], scalar.inner_product(&params.query, &vecs[2]), ScalarValue::Int64(300)),
        ];
        let mut expected_sorted = expected.clone();
        expected_sorted.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        for ((exp_id, _, exp_col), got) in expected_sorted.iter().zip(results.iter()) {
            assert_eq!(got.row_id, *exp_id);
            assert_same_scalar(exp_col, got.columns.first().unwrap());
        }
    }

    fn assert_same_scalar(expected: &ScalarValue, actual: &ScalarValue) {
        match (expected, actual) {
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => assert_eq!(a, b),
            (ScalarValue::Float32(a), ScalarValue::Float32(b)) => assert!((a - b).abs() < 1e-5),
            (ScalarValue::Float64(a), ScalarValue::Float64(b)) => assert!((a - b).abs() < 1e-8),
            (ScalarValue::Bool(a), ScalarValue::Bool(b)) => assert_eq!(a, b),
            (ScalarValue::Binary(a), ScalarValue::Binary(b)) => assert_eq!(a, b),
            other => panic!("scalar mismatch: {:?}", other),
        }
    }

    fn block_on<F: Future>(fut: F) -> F::Output {
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
            fn wake_by_ref(self: &Arc<Self>) {}
        }
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(fut);
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(val) => return val,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }
}
