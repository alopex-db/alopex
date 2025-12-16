use std::cmp::Ordering;

use crate::storage::SqlValue;
use serde::{Deserialize, Serialize};

/// RowGroup 統計情報。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowGroupStatistics {
    pub row_count: u64,
    pub columns: Vec<ColumnStatistics>,
    #[serde(default)]
    pub row_id_min: Option<u64>,
    #[serde(default)]
    pub row_id_max: Option<u64>,
}

/// カラム統計情報。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub min: SqlValue,
    pub max: SqlValue,
    pub null_count: u64,
    pub total_count: u64,
    pub distinct_count: Option<u64>,
}

impl Default for ColumnStatistics {
    fn default() -> Self {
        Self {
            min: SqlValue::Null,
            max: SqlValue::Null,
            null_count: 0,
            total_count: 0,
            distinct_count: None,
        }
    }
}

impl ColumnStatistics {
    /// `SqlValue` スライスからカラム統計を計算する。
    pub fn compute(values: &[SqlValue]) -> Self {
        let total_count = values.len() as u64;
        let null_count = values.iter().filter(|v| v.is_null()).count() as u64;

        let mut non_nulls = values.iter().filter(|v| !v.is_null());
        let (min, max) = if let Some(first) = non_nulls.next() {
            let mut min = first.clone();
            let mut max = first.clone();
            for v in non_nulls {
                if let Some(Ordering::Less) = v.partial_cmp(&min) {
                    min = v.clone();
                }
                if let Some(Ordering::Greater) = v.partial_cmp(&max) {
                    max = v.clone();
                }
            }
            (min, max)
        } else {
            (SqlValue::Null, SqlValue::Null)
        };

        Self {
            min,
            max,
            null_count,
            total_count,
            distinct_count: None,
        }
    }
}

/// 複数行から RowGroup 統計を計算する。
pub fn compute_row_group_statistics(rows: &[Vec<SqlValue>]) -> RowGroupStatistics {
    let row_count = rows.len() as u64;
    let column_count = rows.first().map(|r| r.len()).unwrap_or(0);
    let mut columns = Vec::with_capacity(column_count);

    for idx in 0..column_count {
        let mut col_values = Vec::with_capacity(rows.len());
        for row in rows {
            col_values.push(row.get(idx).cloned().unwrap_or(SqlValue::Null));
        }
        columns.push(ColumnStatistics::compute(&col_values));
    }

    RowGroupStatistics {
        row_count,
        columns,
        row_id_min: None,
        row_id_max: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_statistics_basic() {
        let values = vec![
            SqlValue::Integer(3),
            SqlValue::Integer(1),
            SqlValue::Null,
            SqlValue::Integer(2),
        ];
        let stats = ColumnStatistics::compute(&values);
        assert_eq!(stats.min, SqlValue::Integer(1));
        assert_eq!(stats.max, SqlValue::Integer(3));
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.total_count, 4);
        assert_eq!(stats.distinct_count, None);
    }

    #[test]
    fn compute_row_group_statistics_handles_empty() {
        let stats = compute_row_group_statistics(&[]);
        assert_eq!(stats.row_count, 0);
        assert!(stats.columns.is_empty());
    }
}
