//! Column statistics computation for segment metadata.
//!
//! Provides statistics (min, max, null_count, distinct_count) for columns
//! to enable predicate pushdown and query optimization.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::columnar::encoding::Column;
use crate::columnar::encoding_v2::Bitmap;
use crate::columnar::error::{ColumnarError, Result};

/// A scalar value that can represent min/max statistics for any column type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 32-bit floating point.
    Float32(f32),
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit floating point.
    Float64(f64),
    /// Variable-length binary data (also used for strings).
    Binary(Vec<u8>),
}

impl ScalarValue {
    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }
}

/// Statistics for a single column.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Minimum value in the column (None if all values are null).
    pub min: Option<ScalarValue>,
    /// Maximum value in the column (None if all values are null).
    pub max: Option<ScalarValue>,
    /// Number of null values.
    pub null_count: u64,
    /// Estimated number of distinct values (None if not computed).
    pub distinct_count: Option<u64>,
}

/// Statistics for an entire segment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentStatistics {
    /// Total number of rows in the segment.
    pub num_rows: u64,
    /// Statistics for each column.
    pub column_stats: Vec<ColumnStatistics>,
}

impl SegmentStatistics {
    /// Create new segment statistics with the given row count.
    pub fn new(num_rows: u64) -> Self {
        Self {
            num_rows,
            column_stats: Vec::new(),
        }
    }

    /// Add column statistics to the segment.
    pub fn add_column_stats(&mut self, stats: ColumnStatistics) {
        self.column_stats.push(stats);
    }
}

/// ベクトルセグメント向けの統計情報。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorSegmentStatistics {
    /// 全行数。
    pub row_count: u64,
    /// NULL 行数。
    pub null_count: u64,
    /// アクティブ（有効）行数。
    pub active_count: u64,
    /// 論理削除行数。
    pub deleted_count: u64,
    /// 削除率（0-1）。
    pub deletion_ratio: f32,
    /// ベクトルノルムの最小値。
    pub norm_min: f32,
    /// ベクトルノルムの最大値。
    pub norm_max: f32,
    /// フィルタ列の最小値。
    pub min_values: Vec<ScalarValue>,
    /// フィルタ列の最大値。
    pub max_values: Vec<ScalarValue>,
    /// 作成時刻（epoch millis）。
    pub created_at: u64,
}

impl VectorSegmentStatistics {
    /// シリアライズ（KVS 永続化用）。
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| ColumnarError::InvalidFormat(e.to_string()))
    }

    /// バイト列から復元する。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| ColumnarError::InvalidFormat(e.to_string()))
    }
}

/// Compute statistics for a column.
///
/// # Arguments
/// * `column` - The column data
/// * `null_bitmap` - Optional null bitmap (1 = valid, 0 = null)
/// * `compute_distinct` - Whether to compute distinct count (can be expensive)
pub fn compute_column_statistics(
    column: &Column,
    null_bitmap: Option<&Bitmap>,
    compute_distinct: bool,
) -> ColumnStatistics {
    match column {
        Column::Int64(values) => compute_int64_statistics(values, null_bitmap, compute_distinct),
        Column::Float32(values) => {
            compute_float32_statistics(values, null_bitmap, compute_distinct)
        }
        Column::Float64(values) => {
            compute_float64_statistics(values, null_bitmap, compute_distinct)
        }
        Column::Bool(values) => compute_bool_statistics(values, null_bitmap, compute_distinct),
        Column::Binary(values) => compute_binary_statistics(values, null_bitmap, compute_distinct),
        Column::Fixed { values, len: _ } => {
            compute_binary_statistics(values, null_bitmap, compute_distinct)
        }
    }
}

/// Compute statistics for Int64 column.
fn compute_int64_statistics(
    values: &[i64],
    null_bitmap: Option<&Bitmap>,
    compute_distinct: bool,
) -> ColumnStatistics {
    if values.is_empty() {
        return ColumnStatistics::default();
    }

    let mut min_val: Option<i64> = None;
    let mut max_val: Option<i64> = None;
    let mut null_count = 0u64;
    let mut distinct_set: Option<HashSet<i64>> = if compute_distinct {
        Some(HashSet::new())
    } else {
        None
    };

    for (i, &value) in values.iter().enumerate() {
        if let Some(bitmap) = null_bitmap {
            if !bitmap.get(i) {
                null_count += 1;
                continue;
            }
        }

        min_val = Some(min_val.map_or(value, |m| m.min(value)));
        max_val = Some(max_val.map_or(value, |m| m.max(value)));

        if let Some(ref mut set) = distinct_set {
            set.insert(value);
        }
    }

    ColumnStatistics {
        min: min_val.map(ScalarValue::Int64),
        max: max_val.map(ScalarValue::Int64),
        null_count,
        distinct_count: distinct_set.map(|s| s.len() as u64),
    }
}

/// Compute statistics for Float64 column.
fn compute_float64_statistics(
    values: &[f64],
    null_bitmap: Option<&Bitmap>,
    compute_distinct: bool,
) -> ColumnStatistics {
    if values.is_empty() {
        return ColumnStatistics::default();
    }

    let mut min_val: Option<f64> = None;
    let mut max_val: Option<f64> = None;
    let mut null_count = 0u64;
    let mut distinct_set: Option<HashSet<u64>> = if compute_distinct {
        Some(HashSet::new())
    } else {
        None
    };

    for (i, &value) in values.iter().enumerate() {
        if let Some(bitmap) = null_bitmap {
            if !bitmap.get(i) {
                null_count += 1;
                continue;
            }
        }

        // Skip NaN for min/max computation
        if value.is_nan() {
            if let Some(ref mut set) = distinct_set {
                // Use canonical NaN representation for distinct count
                set.insert(f64::NAN.to_bits());
            }
            continue;
        }

        min_val = Some(min_val.map_or(value, |m| m.min(value)));
        max_val = Some(max_val.map_or(value, |m| m.max(value)));

        if let Some(ref mut set) = distinct_set {
            set.insert(value.to_bits());
        }
    }

    ColumnStatistics {
        min: min_val.map(ScalarValue::Float64),
        max: max_val.map(ScalarValue::Float64),
        null_count,
        distinct_count: distinct_set.map(|s| s.len() as u64),
    }
}

/// Compute statistics for Float32 column.
fn compute_float32_statistics(
    values: &[f32],
    null_bitmap: Option<&Bitmap>,
    compute_distinct: bool,
) -> ColumnStatistics {
    if values.is_empty() {
        return ColumnStatistics::default();
    }

    let mut min_val: Option<f32> = None;
    let mut max_val: Option<f32> = None;
    let mut null_count = 0u64;
    let mut distinct_set: Option<HashSet<u32>> = if compute_distinct {
        Some(HashSet::new())
    } else {
        None
    };

    for (i, &value) in values.iter().enumerate() {
        if let Some(bitmap) = null_bitmap {
            if !bitmap.get(i) {
                null_count += 1;
                continue;
            }
        }

        if value.is_nan() {
            if let Some(ref mut set) = distinct_set {
                set.insert(f32::NAN.to_bits());
            }
            continue;
        }

        min_val = Some(min_val.map_or(value, |m| m.min(value)));
        max_val = Some(max_val.map_or(value, |m| m.max(value)));

        if let Some(ref mut set) = distinct_set {
            set.insert(value.to_bits());
        }
    }

    ColumnStatistics {
        min: min_val.map(ScalarValue::Float32),
        max: max_val.map(ScalarValue::Float32),
        null_count,
        distinct_count: distinct_set.map(|s| s.len() as u64),
    }
}

/// Compute statistics for Bool column.
fn compute_bool_statistics(
    values: &[bool],
    null_bitmap: Option<&Bitmap>,
    compute_distinct: bool,
) -> ColumnStatistics {
    if values.is_empty() {
        return ColumnStatistics::default();
    }

    let mut has_true = false;
    let mut has_false = false;
    let mut null_count = 0u64;

    for (i, &value) in values.iter().enumerate() {
        if let Some(bitmap) = null_bitmap {
            if !bitmap.get(i) {
                null_count += 1;
                continue;
            }
        }

        if value {
            has_true = true;
        } else {
            has_false = true;
        }
    }

    let (min, max) = match (has_false, has_true) {
        (false, false) => (None, None),
        (true, false) => (
            Some(ScalarValue::Bool(false)),
            Some(ScalarValue::Bool(false)),
        ),
        (false, true) => (Some(ScalarValue::Bool(true)), Some(ScalarValue::Bool(true))),
        (true, true) => (
            Some(ScalarValue::Bool(false)),
            Some(ScalarValue::Bool(true)),
        ),
    };

    let distinct_count = if compute_distinct {
        let count = match (has_false, has_true) {
            (false, false) => 0,
            (true, false) | (false, true) => 1,
            (true, true) => 2,
        };
        Some(count)
    } else {
        None
    };

    ColumnStatistics {
        min,
        max,
        null_count,
        distinct_count,
    }
}

/// Compute statistics for Binary column (lexicographic ordering).
fn compute_binary_statistics(
    values: &[Vec<u8>],
    null_bitmap: Option<&Bitmap>,
    compute_distinct: bool,
) -> ColumnStatistics {
    if values.is_empty() {
        return ColumnStatistics::default();
    }

    let mut min_val: Option<&[u8]> = None;
    let mut max_val: Option<&[u8]> = None;
    let mut null_count = 0u64;
    let mut distinct_set: Option<HashSet<&[u8]>> = if compute_distinct {
        Some(HashSet::new())
    } else {
        None
    };

    for (i, value) in values.iter().enumerate() {
        if let Some(bitmap) = null_bitmap {
            if !bitmap.get(i) {
                null_count += 1;
                continue;
            }
        }

        let slice: &[u8] = value.as_slice();
        min_val = Some(min_val.map_or(slice, |m| if slice < m { slice } else { m }));
        max_val = Some(max_val.map_or(slice, |m| if slice > m { slice } else { m }));

        if let Some(ref mut set) = distinct_set {
            set.insert(slice);
        }
    }

    ColumnStatistics {
        min: min_val.map(|v| ScalarValue::Binary(v.to_vec())),
        max: max_val.map(|v| ScalarValue::Binary(v.to_vec())),
        null_count,
        distinct_count: distinct_set.map(|s| s.len() as u64),
    }
}

/// Merge two column statistics.
///
/// This is useful when combining statistics from multiple row groups.
pub fn merge_column_statistics(a: &ColumnStatistics, b: &ColumnStatistics) -> ColumnStatistics {
    let min = merge_min(&a.min, &b.min);
    let max = merge_max(&a.max, &b.max);

    ColumnStatistics {
        min,
        max,
        null_count: a.null_count + b.null_count,
        // Cannot accurately merge distinct counts without full data
        distinct_count: None,
    }
}

fn merge_min(a: &Option<ScalarValue>, b: &Option<ScalarValue>) -> Option<ScalarValue> {
    match (a, b) {
        (None, None) => None,
        (Some(v), None) | (None, Some(v)) => Some(v.clone()),
        (Some(a_val), Some(b_val)) => Some(scalar_min(a_val, b_val)),
    }
}

fn merge_max(a: &Option<ScalarValue>, b: &Option<ScalarValue>) -> Option<ScalarValue> {
    match (a, b) {
        (None, None) => None,
        (Some(v), None) | (None, Some(v)) => Some(v.clone()),
        (Some(a_val), Some(b_val)) => Some(scalar_max(a_val, b_val)),
    }
}

fn scalar_min(a: &ScalarValue, b: &ScalarValue) -> ScalarValue {
    match (a, b) {
        (ScalarValue::Int64(a), ScalarValue::Int64(b)) => ScalarValue::Int64(*a.min(b)),
        (ScalarValue::Float32(a), ScalarValue::Float32(b)) => ScalarValue::Float32(a.min(*b)),
        (ScalarValue::Float64(a), ScalarValue::Float64(b)) => ScalarValue::Float64(a.min(*b)),
        (ScalarValue::Bool(a), ScalarValue::Bool(b)) => ScalarValue::Bool(*a && *b),
        (ScalarValue::Binary(a), ScalarValue::Binary(b)) => {
            ScalarValue::Binary(if a < b { a.clone() } else { b.clone() })
        }
        _ => a.clone(), // Type mismatch, return first
    }
}

fn scalar_max(a: &ScalarValue, b: &ScalarValue) -> ScalarValue {
    match (a, b) {
        (ScalarValue::Int64(a), ScalarValue::Int64(b)) => ScalarValue::Int64(*a.max(b)),
        (ScalarValue::Float32(a), ScalarValue::Float32(b)) => ScalarValue::Float32(a.max(*b)),
        (ScalarValue::Float64(a), ScalarValue::Float64(b)) => ScalarValue::Float64(a.max(*b)),
        (ScalarValue::Bool(a), ScalarValue::Bool(b)) => ScalarValue::Bool(*a || *b),
        (ScalarValue::Binary(a), ScalarValue::Binary(b)) => {
            ScalarValue::Binary(if a > b { a.clone() } else { b.clone() })
        }
        _ => a.clone(), // Type mismatch, return first
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn test_min_max_int64() {
        let values = vec![5i64, 2, 8, 1, 9, 3];
        let column = Column::Int64(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, Some(ScalarValue::Int64(1)));
        assert_eq!(stats.max, Some(ScalarValue::Int64(9)));
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.distinct_count, Some(6));
    }

    #[test]
    fn test_min_max_int64_with_duplicates() {
        let values = vec![5i64, 2, 5, 2, 5, 2];
        let column = Column::Int64(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, Some(ScalarValue::Int64(2)));
        assert_eq!(stats.max, Some(ScalarValue::Int64(5)));
        assert_eq!(stats.distinct_count, Some(2));
    }

    #[test]
    fn test_null_count() {
        let values = vec![1i64, 2, 3, 4, 5];
        let column = Column::Int64(values);

        // Create bitmap where indices 1 and 3 are null (0)
        let mut bitmap = Bitmap::new(5);
        bitmap.set(0, true);
        bitmap.set(1, false); // null
        bitmap.set(2, true);
        bitmap.set(3, false); // null
        bitmap.set(4, true);

        let stats = compute_column_statistics(&column, Some(&bitmap), true);

        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.min, Some(ScalarValue::Int64(1))); // indices 0, 2, 4
        assert_eq!(stats.max, Some(ScalarValue::Int64(5)));
        assert_eq!(stats.distinct_count, Some(3)); // 1, 3, 5
    }

    #[test]
    fn test_distinct_count_estimation() {
        // Test with many values
        let values: Vec<i64> = (0..1000).collect();
        let column = Column::Int64(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.distinct_count, Some(1000));
    }

    #[test]
    fn test_min_max_float64() {
        let values = vec![1.5f64, 2.5, 0.5, 3.5, -1.5];
        let column = Column::Float64(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, Some(ScalarValue::Float64(-1.5)));
        assert_eq!(stats.max, Some(ScalarValue::Float64(3.5)));
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.distinct_count, Some(5));
    }

    #[test]
    fn test_min_max_float32() {
        let values = vec![1.5f32, 2.5, 0.5, 3.5, -1.5];
        let column = Column::Float32(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, Some(ScalarValue::Float32(-1.5)));
        assert_eq!(stats.max, Some(ScalarValue::Float32(3.5)));
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.distinct_count, Some(5));
    }

    #[test]
    fn test_float64_with_nan() {
        let values = vec![1.0f64, f64::NAN, 2.0, f64::NAN, 3.0];
        let column = Column::Float64(values);
        let stats = compute_column_statistics(&column, None, true);

        // NaN should be excluded from min/max
        assert_eq!(stats.min, Some(ScalarValue::Float64(1.0)));
        assert_eq!(stats.max, Some(ScalarValue::Float64(3.0)));
        // NaN counts as one distinct value
        assert_eq!(stats.distinct_count, Some(4)); // 1.0, 2.0, 3.0, NaN
    }

    #[test]
    fn test_min_max_binary_lexicographic() {
        let values = vec![
            b"banana".to_vec(),
            b"apple".to_vec(),
            b"cherry".to_vec(),
            b"apricot".to_vec(),
        ];
        let column = Column::Binary(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, Some(ScalarValue::Binary(b"apple".to_vec())));
        assert_eq!(stats.max, Some(ScalarValue::Binary(b"cherry".to_vec())));
        assert_eq!(stats.distinct_count, Some(4));
    }

    #[test]
    fn test_bool_statistics() {
        let values = vec![true, false, true, true, false];
        let column = Column::Bool(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, Some(ScalarValue::Bool(false)));
        assert_eq!(stats.max, Some(ScalarValue::Bool(true)));
        assert_eq!(stats.distinct_count, Some(2));
    }

    #[test]
    fn test_bool_all_true() {
        let values = vec![true, true, true];
        let column = Column::Bool(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, Some(ScalarValue::Bool(true)));
        assert_eq!(stats.max, Some(ScalarValue::Bool(true)));
        assert_eq!(stats.distinct_count, Some(1));
    }

    #[test]
    fn test_empty_column() {
        let values: Vec<i64> = vec![];
        let column = Column::Int64(values);
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(stats.min, None);
        assert_eq!(stats.max, None);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.distinct_count, None);
    }

    #[test]
    fn test_all_nulls() {
        let values = vec![1i64, 2, 3];
        let column = Column::Int64(values);

        // All values are null
        let mut bitmap = Bitmap::new(3);
        bitmap.set(0, false);
        bitmap.set(1, false);
        bitmap.set(2, false);

        let stats = compute_column_statistics(&column, Some(&bitmap), true);

        assert_eq!(stats.min, None);
        assert_eq!(stats.max, None);
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.distinct_count, Some(0));
    }

    #[test]
    fn test_merge_statistics() {
        let stats1 = ColumnStatistics {
            min: Some(ScalarValue::Int64(5)),
            max: Some(ScalarValue::Int64(15)),
            null_count: 2,
            distinct_count: Some(10),
        };

        let stats2 = ColumnStatistics {
            min: Some(ScalarValue::Int64(3)),
            max: Some(ScalarValue::Int64(20)),
            null_count: 3,
            distinct_count: Some(8),
        };

        let merged = merge_column_statistics(&stats1, &stats2);

        assert_eq!(merged.min, Some(ScalarValue::Int64(3)));
        assert_eq!(merged.max, Some(ScalarValue::Int64(20)));
        assert_eq!(merged.null_count, 5);
        // Distinct count cannot be merged accurately
        assert_eq!(merged.distinct_count, None);
    }

    #[test]
    fn test_segment_statistics() {
        let mut seg_stats = SegmentStatistics::new(1000);

        seg_stats.add_column_stats(ColumnStatistics {
            min: Some(ScalarValue::Int64(1)),
            max: Some(ScalarValue::Int64(100)),
            null_count: 5,
            distinct_count: Some(95),
        });

        seg_stats.add_column_stats(ColumnStatistics {
            min: Some(ScalarValue::Binary(b"a".to_vec())),
            max: Some(ScalarValue::Binary(b"z".to_vec())),
            null_count: 10,
            distinct_count: Some(26),
        });

        assert_eq!(seg_stats.num_rows, 1000);
        assert_eq!(seg_stats.column_stats.len(), 2);
    }

    #[test]
    fn test_scalar_value_is_null() {
        assert!(ScalarValue::Null.is_null());
        assert!(!ScalarValue::Int64(42).is_null());
        assert!(!ScalarValue::Float64(std::f64::consts::PI).is_null());
        assert!(!ScalarValue::Bool(true).is_null());
        assert!(!ScalarValue::Binary(vec![1, 2, 3]).is_null());
    }

    #[test]
    fn test_fixed_column_statistics() {
        let values = vec![
            vec![0x01, 0x02, 0x03, 0x04],
            vec![0x00, 0x00, 0x00, 0x01],
            vec![0xFF, 0xFF, 0xFF, 0xFF],
        ];
        let column = Column::Fixed { len: 4, values };
        let stats = compute_column_statistics(&column, None, true);

        assert_eq!(
            stats.min,
            Some(ScalarValue::Binary(vec![0x00, 0x00, 0x00, 0x01]))
        );
        assert_eq!(
            stats.max,
            Some(ScalarValue::Binary(vec![0xFF, 0xFF, 0xFF, 0xFF]))
        );
        assert_eq!(stats.distinct_count, Some(3));
    }

    #[test]
    fn test_vector_segment_statistics_roundtrip() {
        let stats = VectorSegmentStatistics {
            row_count: 10_000,
            null_count: 5,
            active_count: 9_900,
            deleted_count: 95,
            deletion_ratio: 0.0095,
            norm_min: 0.1,
            norm_max: 3.2,
            min_values: vec![ScalarValue::Int64(1)],
            max_values: vec![ScalarValue::Int64(100)],
            created_at: 1_735_000_000,
        };

        let bytes = stats.to_bytes().unwrap();
        let decoded = VectorSegmentStatistics::from_bytes(&bytes).unwrap();

        assert_eq!(decoded, stats);
    }
}
