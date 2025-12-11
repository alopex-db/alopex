use alopex_core::kv::KVStore;

use crate::ast::expr::BinaryOp;
use crate::catalog::TableMetadata;
use crate::columnar::statistics::{RowGroupStatistics, compute_row_group_statistics};
use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{Result, Row};
use crate::planner::typed_expr::{Projection, TypedExpr, TypedExprKind};
use crate::storage::{SqlTransaction, SqlValue};

/// ColumnarScan オペレータ。
#[derive(Debug, Clone)]
pub struct ColumnarScan {
    pub table_id: u32,
    pub projected_columns: Vec<usize>,
    pub pushed_filter: Option<PushdownFilter>,
    pub residual_filter: Option<TypedExpr>,
}

/// プッシュダウン可能なフィルタ。
#[derive(Debug, Clone, PartialEq)]
pub enum PushdownFilter {
    Eq {
        column_idx: usize,
        value: SqlValue,
    },
    Range {
        column_idx: usize,
        min: Option<SqlValue>,
        max: Option<SqlValue>,
    },
    IsNull {
        column_idx: usize,
        is_null: bool,
    },
    And(Vec<PushdownFilter>),
    Or(Vec<PushdownFilter>),
}

impl ColumnarScan {
    pub fn new(
        table_id: u32,
        projected_columns: Vec<usize>,
        pushed_filter: Option<PushdownFilter>,
        residual_filter: Option<TypedExpr>,
    ) -> Self {
        Self {
            table_id,
            projected_columns,
            pushed_filter,
            residual_filter,
        }
    }

    /// RowGroup をプルーニングするか判定する。
    pub fn should_skip_row_group(&self, stats: &RowGroupStatistics) -> bool {
        match &self.pushed_filter {
            None => false,
            Some(filter) => Self::evaluate_pushdown(filter, stats),
        }
    }

    /// プッシュダウンフィルタを統計情報で評価する。
    pub fn evaluate_pushdown(filter: &PushdownFilter, stats: &RowGroupStatistics) -> bool {
        match filter {
            PushdownFilter::Eq { column_idx, value } => match stats.columns.get(*column_idx) {
                Some(col_stats) => {
                    if col_stats.total_count == 0 {
                        return true;
                    }
                    if matches!(
                        value.partial_cmp(&col_stats.min),
                        Some(std::cmp::Ordering::Less)
                    ) {
                        return true;
                    }
                    matches!(
                        value.partial_cmp(&col_stats.max),
                        Some(std::cmp::Ordering::Greater)
                    )
                }
                None => false,
            },

            PushdownFilter::Range {
                column_idx,
                min,
                max,
            } => match stats.columns.get(*column_idx) {
                Some(col_stats) => {
                    if col_stats.total_count == 0 {
                        return true;
                    }
                    if let Some(filter_min) = min {
                        if matches!(
                            col_stats.max.partial_cmp(filter_min),
                            Some(std::cmp::Ordering::Less)
                        ) {
                            return true;
                        }
                    }
                    if let Some(filter_max) = max {
                        if matches!(
                            col_stats.min.partial_cmp(filter_max),
                            Some(std::cmp::Ordering::Greater)
                        ) {
                            return true;
                        }
                    }
                    false
                }
                None => false,
            },

            PushdownFilter::IsNull {
                column_idx,
                is_null,
            } => match stats.columns.get(*column_idx) {
                Some(col_stats) => {
                    if *is_null {
                        col_stats.null_count == 0
                    } else {
                        col_stats.null_count == col_stats.total_count
                    }
                }
                None => false,
            },

            PushdownFilter::And(filters) => {
                if filters.is_empty() {
                    return false;
                }
                filters.iter().any(|f| Self::evaluate_pushdown(f, stats))
            }

            PushdownFilter::Or(filters) => {
                if filters.is_empty() {
                    return false;
                }
                filters.iter().all(|f| Self::evaluate_pushdown(f, stats))
            }
        }
    }
}

/// ColumnarScan を実行する。
pub fn execute_columnar_scan<S: KVStore>(
    txn: &mut SqlTransaction<'_, S>,
    table_meta: &TableMetadata,
    scan: &ColumnarScan,
) -> Result<Vec<Row>> {
    debug_assert_eq!(scan.table_id, table_meta.table_id);
    let _ = &scan.projected_columns;

    // Columnar ストレージが未実装のため、現状は RowStorage から行を取得して統計を計算する。
    let rows = txn.with_table(table_meta, |storage| {
        let iter = storage.range_scan(0, u64::MAX)?;
        let mut rows = Vec::new();
        for entry in iter {
            let (row_id, values) = entry?;
            rows.push(Row::new(row_id, values));
        }
        Ok(rows)
    })?;

    if scan.pushed_filter.is_some() {
        let values: Vec<Vec<SqlValue>> = rows.iter().map(|r| r.values.clone()).collect();
        let stats = compute_row_group_statistics(&values);
        if scan.should_skip_row_group(&stats) {
            return Ok(Vec::new());
        }
    }

    let mut filtered_rows = Vec::new();
    for row in rows {
        if let Some(predicate) = &scan.residual_filter {
            let ctx = EvalContext::new(&row.values);
            let keep = matches!(evaluate(predicate, &ctx)?, SqlValue::Boolean(true));
            if !keep {
                continue;
            }
        }
        filtered_rows.push(row);
    }

    Ok(filtered_rows)
}

/// TypedExpr から PushdownFilter へ変換する（変換不可なら None）。
pub fn expr_to_pushdown(expr: &TypedExpr) -> Option<PushdownFilter> {
    match &expr.kind {
        TypedExprKind::BinaryOp { left, op, right } => match op {
            BinaryOp::And => {
                let l = expr_to_pushdown(left)?;
                let r = expr_to_pushdown(right)?;
                Some(PushdownFilter::And(vec![l, r]))
            }
            BinaryOp::Or => {
                let l = expr_to_pushdown(left)?;
                let r = expr_to_pushdown(right)?;
                Some(PushdownFilter::Or(vec![l, r]))
            }
            BinaryOp::Eq => extract_eq(left, right),
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                extract_range(op, left, right)
            }
            _ => None,
        },
        TypedExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            if *negated {
                return None;
            }
            let (column_idx, value_min, value_max) = match expr.kind {
                TypedExprKind::ColumnRef { column_index, .. } => {
                    let low_v = literal_value(low)?;
                    let high_v = literal_value(high)?;
                    (column_index, low_v, high_v)
                }
                _ => return None,
            };
            Some(PushdownFilter::Range {
                column_idx,
                min: Some(value_min),
                max: Some(value_max),
            })
        }
        TypedExprKind::IsNull { expr, negated } => match expr.kind {
            TypedExprKind::ColumnRef { column_index, .. } => Some(PushdownFilter::IsNull {
                column_idx: column_index,
                is_null: !negated,
            }),
            _ => None,
        },
        _ => None,
    }
}

fn extract_eq(left: &TypedExpr, right: &TypedExpr) -> Option<PushdownFilter> {
    if let Some((col_idx, value)) = extract_column_literal(left, right) {
        return Some(PushdownFilter::Eq {
            column_idx: col_idx,
            value,
        });
    }
    if let Some((col_idx, value)) = extract_column_literal(right, left) {
        return Some(PushdownFilter::Eq {
            column_idx: col_idx,
            value,
        });
    }
    None
}

fn extract_range(op: &BinaryOp, left: &TypedExpr, right: &TypedExpr) -> Option<PushdownFilter> {
    match (
        extract_column_literal(left, right),
        extract_column_literal(right, left),
    ) {
        (Some((col_idx, value)), _) => match op {
            BinaryOp::Lt | BinaryOp::LtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: None,
                max: Some(value),
            }),
            BinaryOp::Gt | BinaryOp::GtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: Some(value),
                max: None,
            }),
            _ => None,
        },
        (_, Some((col_idx, value))) => match op {
            BinaryOp::Lt | BinaryOp::LtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: Some(value),
                max: None,
            }),
            BinaryOp::Gt | BinaryOp::GtEq => Some(PushdownFilter::Range {
                column_idx: col_idx,
                min: None,
                max: Some(value),
            }),
            _ => None,
        },
        _ => None,
    }
}

fn extract_column_literal(
    column_expr: &TypedExpr,
    literal_expr: &TypedExpr,
) -> Option<(usize, SqlValue)> {
    match column_expr.kind {
        TypedExprKind::ColumnRef { column_index, .. } => {
            let value = literal_value(literal_expr)?;
            Some((column_index, value))
        }
        _ => None,
    }
}

fn literal_value(expr: &TypedExpr) -> Option<SqlValue> {
    match &expr.kind {
        TypedExprKind::Literal(_) | TypedExprKind::VectorLiteral(_) => {
            evaluate(expr, &EvalContext::new(&[])).ok()
        }
        _ => None,
    }
}

/// projection 情報からカラムインデックスを推定する（現状は全カラム）。
pub fn projection_to_columns(projection: &Projection, table_meta: &TableMetadata) -> Vec<usize> {
    match projection {
        Projection::All(names) => names
            .iter()
            .filter_map(|name| table_meta.columns.iter().position(|c| &c.name == name))
            .collect(),
        Projection::Columns(_) => (0..table_meta.columns.len()).collect(),
    }
}

/// フィルタと Projection を ColumnarScan にまとめるユーティリティ。
pub fn build_columnar_scan_for_filter(
    table_meta: &TableMetadata,
    projection: Projection,
    predicate: &TypedExpr,
) -> ColumnarScan {
    let projected_columns = projection_to_columns(&projection, table_meta);
    let pushed_filter = expr_to_pushdown(predicate);
    ColumnarScan::new(
        table_meta.table_id,
        projected_columns,
        pushed_filter,
        Some(predicate.clone()),
    )
}

/// Projection だけを指定して ColumnarScan を構築する。
pub fn build_columnar_scan(table_meta: &TableMetadata, projection: &Projection) -> ColumnarScan {
    let projected_columns = projection_to_columns(projection, table_meta);
    ColumnarScan::new(table_meta.table_id, projected_columns, None, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::Literal;
    use crate::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
    use crate::columnar::statistics::ColumnStatistics;
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::typed_expr::TypedExpr;
    use crate::planner::typed_expr::TypedExprKind;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    #[test]
    fn evaluate_pushdown_eq_prunes_out_of_range() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(3),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
        };
        let filter = PushdownFilter::Eq {
            column_idx: 0,
            value: SqlValue::Integer(10),
        };
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_range_allows_overlap() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(5),
                max: SqlValue::Integer(10),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
        };
        let filter = PushdownFilter::Range {
            column_idx: 0,
            min: Some(SqlValue::Integer(8)),
            max: Some(SqlValue::Integer(12)),
        };
        assert!(!ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_is_null_skips_when_no_nulls() {
        let stats = RowGroupStatistics {
            row_count: 2,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(2),
                null_count: 0,
                total_count: 2,
                distinct_count: None,
            }],
        };
        let filter = PushdownFilter::IsNull {
            column_idx: 0,
            is_null: true,
        };
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_is_not_null_skips_when_all_null() {
        let stats = RowGroupStatistics {
            row_count: 2,
            columns: vec![ColumnStatistics {
                min: SqlValue::Null,
                max: SqlValue::Null,
                null_count: 2,
                total_count: 2,
                distinct_count: None,
            }],
        };
        let filter = PushdownFilter::IsNull {
            column_idx: 0,
            is_null: false,
        };
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_and_prunes_if_any_branch_skips() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(3),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
        };
        let filter = PushdownFilter::And(vec![
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(10),
            },
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(2),
            },
        ]);
        assert!(ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn evaluate_pushdown_or_keeps_if_any_branch_may_match() {
        let stats = RowGroupStatistics {
            row_count: 3,
            columns: vec![ColumnStatistics {
                min: SqlValue::Integer(1),
                max: SqlValue::Integer(3),
                null_count: 0,
                total_count: 3,
                distinct_count: None,
            }],
        };
        let filter = PushdownFilter::Or(vec![
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(10),
            },
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(2),
            },
        ]);
        assert!(!ColumnarScan::evaluate_pushdown(&filter, &stats));
    }

    #[test]
    fn expr_to_pushdown_converts_eq() {
        let expr = TypedExpr {
            kind: TypedExprKind::BinaryOp {
                left: Box::new(TypedExpr::column_ref(
                    "t".into(),
                    "c".into(),
                    0,
                    ResolvedType::Integer,
                    crate::Span::default(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(TypedExpr::literal(
                    Literal::Number("1".into()),
                    ResolvedType::Integer,
                    crate::Span::default(),
                )),
            },
            resolved_type: ResolvedType::Boolean,
            span: crate::Span::default(),
        };
        let filter = expr_to_pushdown(&expr).unwrap();
        assert_eq!(
            filter,
            PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(1)
            }
        );
    }

    #[test]
    fn execute_columnar_scan_applies_residual_filter() {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut catalog = MemoryCatalog::new();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_table_id(1);
        let mut ddl_txn = bridge.begin_write().unwrap();
        execute_create_table(&mut ddl_txn, &mut catalog, table.clone(), vec![], false).unwrap();
        ddl_txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        crate::executor::dml::execute_insert(
            &mut txn,
            &catalog,
            "users",
            vec!["id".into(), "name".into()],
            vec![vec![
                TypedExpr::literal(
                    Literal::Number("1".into()),
                    ResolvedType::Integer,
                    crate::Span::default(),
                ),
                TypedExpr::literal(
                    Literal::String("alice".into()),
                    ResolvedType::Text,
                    crate::Span::default(),
                ),
            ]],
        )
        .unwrap();
        txn.commit().unwrap();

        let scan = ColumnarScan::new(
            table.table_id,
            vec![0, 1],
            Some(PushdownFilter::Eq {
                column_idx: 0,
                value: SqlValue::Integer(1),
            }),
            Some(TypedExpr {
                kind: TypedExprKind::BinaryOp {
                    left: Box::new(TypedExpr::column_ref(
                        "users".into(),
                        "id".into(),
                        0,
                        ResolvedType::Integer,
                        crate::Span::default(),
                    )),
                    op: BinaryOp::Eq,
                    right: Box::new(TypedExpr::literal(
                        Literal::Number("1".into()),
                        ResolvedType::Integer,
                        crate::Span::default(),
                    )),
                },
                resolved_type: ResolvedType::Boolean,
                span: crate::Span::default(),
            }),
        );

        let mut read_txn = bridge.begin_read().unwrap();
        let rows = execute_columnar_scan(&mut read_txn, &table, &scan).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[1], SqlValue::Text("alice".into()));
    }
}
