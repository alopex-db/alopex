//! DML executor for INSERT/UPDATE/DELETE operations.
//!
//! This module provides helpers to execute DML plans against the storage layer
//! while enforcing constraints and maintaining secondary indexes.

mod constraints;
mod delete;
mod insert;
mod merge;
mod update;

use alopex_core::kv::KVStore;

use crate::ast::expr::BinaryOp;
use crate::catalog::{Catalog, StorageType, TableMetadata};
use crate::executor::evaluator::{EvalContext, coerce_value, evaluate};
use crate::executor::{Result, Row};
use crate::planner::typed_expr::{TypedExpr, TypedExprKind};
use crate::storage::{SqlTxn, SqlValue};

#[allow(unused_imports)]
pub use delete::{execute_delete, execute_delete_with_returning};
pub(crate) use insert::{evaluate_default, normalize_assignment_value};
#[allow(unused_imports)]
pub use insert::{
    execute_insert, execute_insert_rows, execute_insert_rows_with_plan, execute_insert_with_plan,
};
pub use merge::execute_merge;
#[allow(unused_imports)]
pub use update::{execute_update, execute_update_with_returning};

/// Resolve a single-column PRIMARY KEY equality against its implicit B-tree.
///
/// `None` means that the predicate shape cannot use the primary-key index;
/// `Some` contains every matching row (zero or one for a valid primary key).
pub(crate) fn lookup_primary_key_equality<'txn, S, C, T>(
    txn: &mut T,
    catalog: &C,
    table: &TableMetadata,
    predicate: &TypedExpr,
) -> Result<Option<Vec<Row>>>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
{
    let Some(value) = primary_key_equality_value(table, predicate)? else {
        return Ok(None);
    };
    let Some(index) = catalog
        .get_index(&crate::executor::ddl::create_pk_index_name(&table.name))
        .filter(|index| index.unique && index.column_indices.len() == 1)
    else {
        return Ok(None);
    };

    let row_ids = txn
        .index_storage(index.index_id, index.unique, index.column_indices.clone())
        .lookup(&value)?;
    let mut storage = txn.table_storage(table);
    let mut rows = Vec::with_capacity(row_ids.len());
    for row_id in row_ids {
        if let Some(values) = storage.get(row_id)? {
            rows.push(Row::new(row_id, values));
        }
    }
    Ok(Some(rows))
}

/// Resolve a row-storage `TO_TSVECTOR(column) @@ tsquery` predicate through
/// a matching FTS index. Callers retain the predicate for exact evaluation.
pub(crate) fn lookup_fts_match<'txn, S, C, T>(
    txn: &mut T,
    catalog: &C,
    table: &TableMetadata,
    predicate: &TypedExpr,
) -> Result<Option<Vec<Row>>>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
{
    if table.storage_options.storage_type != StorageType::Row {
        return Ok(None);
    }
    let Some((column, config, query_expr)) = fts_match_parts(table, predicate) else {
        return Ok(None);
    };
    let Some(index) = catalog
        .get_indexes_for_table(&table.name)
        .into_iter()
        .find(|index| {
            matches!(index.method, Some(crate::ast::ddl::IndexMethod::Fts))
                && index.column_indices == [column]
                && crate::executor::fts_bridge::config(index).eq_ignore_ascii_case(config)
                && index.get_option("fts_format_version") == Some(crate::fts::INDEX_FORMAT_VERSION)
        })
    else {
        return Ok(None);
    };
    let Some(query_value) = fts_query_value(query_expr)? else {
        return Ok(None);
    };
    let SqlValue::Text(query_value) = query_value else {
        return Ok(Some(Vec::new()));
    };
    let query = match crate::fts::parse_tsquery("simple", &query_value) {
        Ok(query) => query,
        Err(_) => return Ok(None),
    };
    let Some(row_ids) = crate::executor::fts_bridge::lookup_query(txn, index, &query)? else {
        return Ok(None);
    };
    let mut storage = txn.table_storage(table);
    let mut rows = Vec::with_capacity(row_ids.len());
    for row_id in row_ids {
        if let Some(values) = storage.get(row_id)? {
            rows.push(Row::new(row_id, values));
        }
    }
    Ok(Some(rows))
}

/// Read the first `max_rows` entries at or above a single-column PRIMARY KEY.
///
/// Callers must retain and re-evaluate the source predicate. This helper
/// provides ordered candidates only; it does not change SQL comparison
/// semantics.
pub(crate) fn scan_primary_key_from<'txn, S, C, T>(
    txn: &mut T,
    catalog: &C,
    table: &TableMetadata,
    start: &SqlValue,
    start_inclusive: bool,
    max_rows: usize,
) -> Result<Option<Vec<Row>>>
where
    S: KVStore + 'txn,
    C: Catalog + ?Sized,
    T: SqlTxn<'txn, S>,
{
    let Some(index) = catalog
        .get_index(&crate::executor::ddl::create_pk_index_name(&table.name))
        .filter(|index| index.unique && index.column_indices.len() == 1)
    else {
        return Ok(None);
    };

    let row_ids = {
        let mut storage =
            txn.index_storage(index.index_id, index.unique, index.column_indices.clone());
        storage
            .range_scan(Some(start), None, start_inclusive, true)?
            .take(max_rows)
            .collect::<std::result::Result<Vec<_>, crate::storage::StorageError>>()?
    };
    let mut storage = txn.table_storage(table);
    let mut rows = Vec::with_capacity(row_ids.len());
    for row_id in row_ids {
        if let Some(values) = storage.get(row_id)? {
            rows.push(Row::new(row_id, values));
        }
    }
    Ok(Some(rows))
}

fn primary_key_equality_value(
    table: &TableMetadata,
    predicate: &TypedExpr,
) -> Result<Option<SqlValue>> {
    let Some(primary_key_index) = single_primary_key_column_index(table) else {
        return Ok(None);
    };
    let TypedExprKind::BinaryOp { left, op, right } = &predicate.kind else {
        return Ok(None);
    };
    if *op != BinaryOp::Eq {
        return Ok(None);
    }

    let value = match (&left.kind, &right.kind) {
        (
            TypedExprKind::ColumnRef {
                table: column_table,
                column_index,
                ..
            },
            _,
        ) if column_table == &table.name && *column_index == primary_key_index => {
            literal_value(right)?
        }
        (
            _,
            TypedExprKind::ColumnRef {
                table: column_table,
                column_index,
                ..
            },
        ) if column_table == &table.name && *column_index == primary_key_index => {
            literal_value(left)?
        }
        _ => None,
    };
    Ok(value.and_then(|value| normalize_primary_key_value(table, primary_key_index, value)))
}

fn fts_match_parts<'a>(
    table: &TableMetadata,
    predicate: &'a TypedExpr,
) -> Option<(usize, &'a str, &'a TypedExpr)> {
    let TypedExprKind::BinaryOp {
        left,
        op: BinaryOp::TsMatch,
        right,
    } = &predicate.kind
    else {
        return None;
    };
    let TypedExprKind::FunctionCall { name, args, .. } = &left.kind else {
        return None;
    };
    if !name.eq_ignore_ascii_case("to_tsvector") {
        return None;
    }
    let (config, column) = match args.as_slice() {
        [column] => ("simple", column),
        [
            TypedExpr {
                kind: TypedExprKind::Literal(crate::ast::expr::Literal::String(config)),
                ..
            },
            column,
        ] => (config.as_str(), column),
        _ => return None,
    };
    let TypedExprKind::ColumnRef {
        table: column_table,
        column_index,
        ..
    } = &column.kind
    else {
        return None;
    };
    (column_table == &table.name).then_some((*column_index, config, right))
}

/// Evaluate only an FTS query expression whose value is independent of a row.
/// Other expressions retain the normal filter so their per-row semantics hold.
fn fts_query_value(expr: &TypedExpr) -> Result<Option<SqlValue>> {
    let is_query_function = matches!(
        &expr.kind,
        TypedExprKind::FunctionCall { name, args, .. }
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "to_tsquery" | "plainto_tsquery" | "websearch_to_tsquery"
            ) && args.iter().all(is_literal_expression)
    );
    if is_literal_expression(expr) || is_query_function {
        Ok(Some(evaluate(expr, &EvalContext::new(&[]))?))
    } else {
        Ok(None)
    }
}

/// Return the lower bound of a direct single-column primary-key predicate.
///
/// The current SQL path uses this only when the result is also ordered by the
/// primary key and has a finite plain LIMIT.
pub(crate) fn primary_key_lower_bound(
    table: &TableMetadata,
    predicate: &TypedExpr,
) -> Result<Option<(SqlValue, bool)>> {
    let Some(primary_key_index) = single_primary_key_column_index(table) else {
        return Ok(None);
    };
    let TypedExprKind::BinaryOp { left, op, right } = &predicate.kind else {
        return Ok(None);
    };
    let is_primary_key = |expr: &TypedExpr| {
        matches!(
            &expr.kind,
            TypedExprKind::ColumnRef {
                table: column_table,
                column_index,
                ..
            } if column_table == &table.name && *column_index == primary_key_index
        )
    };

    let (value, inclusive) = match (*op, is_primary_key(left), is_primary_key(right)) {
        (BinaryOp::Gt, true, false) => (literal_value(right)?, false),
        (BinaryOp::GtEq, true, false) => (literal_value(right)?, true),
        (BinaryOp::Lt, false, true) => (literal_value(left)?, false),
        (BinaryOp::LtEq, false, true) => (literal_value(left)?, true),
        _ => return Ok(None),
    };
    Ok(value.filter(|value| !value.is_null()).and_then(|value| {
        normalize_primary_key_value(table, primary_key_index, value).map(|value| (value, inclusive))
    }))
}

fn single_primary_key_column_index(table: &TableMetadata) -> Option<usize> {
    table
        .primary_key
        .as_ref()
        .filter(|keys| keys.len() == 1)
        .and_then(|keys| table.get_column_index(&keys[0]))
}

fn normalize_primary_key_value(
    table: &TableMetadata,
    primary_key_index: usize,
    value: SqlValue,
) -> Option<SqlValue> {
    if value.is_null() || value.resolved_type() == table.columns[primary_key_index].data_type {
        Some(value)
    } else {
        // A mixed-width numeric comparison remains valid even if the literal
        // does not fit the indexed column. In that case retain the normal
        // scan, which evaluates the comparison without narrowing its domain.
        coerce_value(value, &table.columns[primary_key_index].data_type).ok()
    }
}

fn literal_value(expr: &TypedExpr) -> Result<Option<SqlValue>> {
    if is_literal_expression(expr) {
        Ok(Some(evaluate(expr, &EvalContext::new(&[]))?))
    } else {
        Ok(None)
    }
}

/// Only fold values whose evaluation is independent of the row being tested.
///
/// A cast may wrap another cast or a literal, but it must not wrap a function
/// or another expression: predicates such as `id = random()` are evaluated for
/// each row by the normal filter and cannot safely become a one-time lookup.
fn is_literal_expression(expr: &TypedExpr) -> bool {
    match &expr.kind {
        TypedExprKind::Literal(_) => true,
        TypedExprKind::Cast { expr, .. } | TypedExprKind::TryCast { expr, .. } => {
            is_literal_expression(expr)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::Span;
    use crate::ast::expr::Literal;
    use crate::catalog::ColumnMetadata;
    use crate::planner::types::ResolvedType;

    use super::*;

    fn predicate(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        predicate_with_op(left, BinaryOp::Eq, right)
    }

    fn predicate_with_op(left: TypedExpr, op: BinaryOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: TypedExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            resolved_type: ResolvedType::Boolean,
            span: Span::default(),
        }
    }

    fn column(index: usize) -> TypedExpr {
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "items".into(),
                column: if index == 0 { "id" } else { "value" }.into(),
                column_index: index,
            },
            resolved_type: ResolvedType::Integer,
            span: Span::default(),
        }
    }

    fn foreign_column(index: usize) -> TypedExpr {
        TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "other_items".into(),
                column: "id".into(),
                column_index: index,
            },
            resolved_type: ResolvedType::Integer,
            span: Span::default(),
        }
    }

    fn integer(value: i32) -> TypedExpr {
        TypedExpr {
            kind: TypedExprKind::Literal(Literal::Number(value.to_string())),
            resolved_type: ResolvedType::Integer,
            span: Span::default(),
        }
    }

    fn bigint(value: i64) -> TypedExpr {
        TypedExpr {
            kind: TypedExprKind::Literal(Literal::Number(value.to_string())),
            resolved_type: ResolvedType::BigInt,
            span: Span::default(),
        }
    }

    #[test]
    fn primary_key_equality_accepts_only_the_single_primary_key_column() {
        let table = TableMetadata::new(
            "items",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("value", ResolvedType::Integer),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        assert_eq!(
            primary_key_equality_value(&table, &predicate(column(0), integer(7))).unwrap(),
            Some(SqlValue::Integer(7))
        );
        assert_eq!(
            primary_key_equality_value(&table, &predicate(integer(7), column(0))).unwrap(),
            Some(SqlValue::Integer(7))
        );
        assert_eq!(
            primary_key_equality_value(&table, &predicate(column(1), integer(7))).unwrap(),
            None
        );
        assert_eq!(
            primary_key_equality_value(&table, &predicate(foreign_column(0), integer(7))).unwrap(),
            None
        );
        assert_eq!(
            primary_key_equality_value(
                &table,
                &predicate(column(0), bigint(i64::from(i32::MAX) + 1)),
            )
            .unwrap(),
            None
        );

        let bigint_table = TableMetadata::new(
            "items",
            vec![ColumnMetadata::new("id", ResolvedType::BigInt).with_primary_key(true)],
        )
        .with_primary_key(vec!["id".into()]);
        assert_eq!(
            primary_key_equality_value(&bigint_table, &predicate(column(0), integer(7))).unwrap(),
            Some(SqlValue::BigInt(7))
        );
    }

    #[test]
    fn primary_key_lower_bound_requires_an_ordered_primary_key_predicate() {
        let table = TableMetadata::new(
            "items",
            vec![ColumnMetadata::new("id", ResolvedType::BigInt).with_primary_key(true)],
        )
        .with_primary_key(vec!["id".into()]);

        assert_eq!(
            primary_key_lower_bound(
                &table,
                &predicate_with_op(column(0), BinaryOp::GtEq, integer(7)),
            )
            .unwrap(),
            Some((SqlValue::BigInt(7), true))
        );
        assert_eq!(
            primary_key_lower_bound(
                &table,
                &predicate_with_op(integer(7), BinaryOp::Lt, column(0)),
            )
            .unwrap(),
            Some((SqlValue::BigInt(7), false))
        );
        assert_eq!(
            primary_key_lower_bound(
                &table,
                &predicate_with_op(column(0), BinaryOp::LtEq, integer(7)),
            )
            .unwrap(),
            None
        );
    }
}
