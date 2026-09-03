use alopex_core::kv::{KVStore, RuntimeStats};

use crate::ast::expr::Literal;
use crate::executor::ddl::sequence;
use crate::executor::{ColumnInfo, ExecutionResult, ExecutorError, QueryResult, Result};
use crate::planner::ResolvedType;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::typed_expr::{Projection, TypedExpr, TypedExprKind};
use crate::storage::{SqlValue, TxnBridge};

/// PRAGMA cache sizes are expressed in 4096-byte pages.
const CACHE_PAGE_SIZE: usize = 4096;

pub(crate) fn try_execute<S: KVStore>(
    bridge: &TxnBridge<S>,
    plan: &LogicalPlan,
) -> Result<Option<ExecutionResult>> {
    if let Some((function, name, alias)) = direct_sequence_call(plan) {
        let mut txn = bridge.begin_write().map_err(ExecutorError::from)?;
        let value = if function == "nextval" {
            sequence::next_value(&mut txn, name)?
        } else {
            sequence::current_value(&mut txn, name)?
        };
        txn.commit().map_err(ExecutorError::from)?;
        return Ok(Some(ExecutionResult::Query(QueryResult::new(
            vec![ColumnInfo::new(
                alias.unwrap_or(function),
                ResolvedType::BigInt,
            )],
            vec![vec![SqlValue::BigInt(value)]],
        ))));
    }
    let Some((name, return_type)) = direct_system_call(plan) else {
        return Ok(None);
    };
    let value = match name {
        "memory_stats" => memory_stats(bridge),
        "io_stats" => io_stats(bridge),
        "clear_cache" => Ok(SqlValue::BigInt(bridge.clear_cache()? as i64)),
        _ => return Ok(None),
    }?;
    Ok(Some(ExecutionResult::Query(QueryResult::new(
        vec![ColumnInfo::new(name, return_type)],
        vec![vec![value]],
    ))))
}

pub(crate) fn direct_sequence_call(plan: &LogicalPlan) -> Option<(&str, &str, Option<&str>)> {
    let (expression, alias) = match plan {
        LogicalPlan::Explain { input, .. } => return direct_sequence_call(input),
        LogicalPlan::Scan { projection, .. } | LogicalPlan::Project { projection, .. } => {
            let Projection::Columns(columns) = projection else {
                return None;
            };
            if columns.len() != 1 {
                return None;
            }
            (&columns[0].expr, columns[0].alias.as_deref())
        }
        LogicalPlan::Values { rows, .. } => (rows.first()?.first()?, None),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => return direct_sequence_call(input),
        _ => return None,
    };
    let Some(TypedExpr {
        kind: TypedExprKind::FunctionCall { name, args, .. },
        ..
    }) = Some(expression)
    else {
        return None;
    };
    if !matches!(name.to_ascii_lowercase().as_str(), "nextval" | "currval") || args.len() != 1 {
        return None;
    }
    match &args[0].kind {
        TypedExprKind::Literal(Literal::String(sequence)) => {
            Some((name.as_str(), sequence.as_str(), alias))
        }
        _ => None,
    }
}

/// Returns whether a plan is a standalone system function that needs the
/// executor's store-backed path rather than an external transaction bridge.
pub fn is_store_direct_plan(plan: &LogicalPlan) -> bool {
    direct_system_call(plan).is_some() || direct_sequence_call(plan).is_some()
}

fn direct_system_call(plan: &LogicalPlan) -> Option<(&str, ResolvedType)> {
    let projection = match plan {
        LogicalPlan::Explain { input, .. } => return direct_system_call(input),
        LogicalPlan::Scan { projection, .. } => projection,
        LogicalPlan::Project { projection, .. } => projection,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => return direct_system_call(input),
        _ => return None,
    };
    let Projection::Columns(columns) = projection else {
        return None;
    };
    if columns.len() != 1 {
        return None;
    }
    let TypedExpr {
        kind: TypedExprKind::FunctionCall { name, args, .. },
        resolved_type,
        ..
    } = &columns[0].expr
    else {
        return None;
    };
    if !args.is_empty() || !matches!(name.as_str(), "memory_stats" | "io_stats" | "clear_cache") {
        return None;
    }
    Some((name.as_str(), resolved_type.clone()))
}

fn memory_stats<S: KVStore>(bridge: &TxnBridge<S>) -> Result<SqlValue> {
    let Some(RuntimeStats::Memory(stats)) = bridge.runtime_stats() else {
        return Ok(SqlValue::Null);
    };
    Ok(SqlValue::Text(format!(
        "{{\"total_bytes\":{},\"kv_bytes\":{},\"index_bytes\":{}}}",
        stats.total_bytes, stats.kv_bytes, stats.index_bytes
    )))
}

fn io_stats<S: KVStore>(bridge: &TxnBridge<S>) -> Result<SqlValue> {
    let Some(RuntimeStats::Lsm(stats)) = bridge.runtime_stats() else {
        return Ok(SqlValue::Null);
    };
    Ok(SqlValue::Text(format!(
        "{{\"wal_write_bytes\":{},\"sstable_read_bytes\":{},\"buffer_pool_hit_rate\":{},\"buffer_pool_size_bytes\":{},\"memtable_size_bytes\":{},\"compaction_bytes_written\":{}}}",
        stats.wal_write_bytes,
        stats.sstable_read_bytes,
        stats.buffer_pool_hit_rate,
        stats.buffer_pool_size_bytes,
        stats.memtable_size_bytes,
        stats.compaction_bytes_written
    )))
}

pub(crate) fn execute_pragma<S: KVStore>(
    bridge: &TxnBridge<S>,
    name: &str,
    value: Option<&crate::ast::PragmaValue>,
) -> Result<ExecutionResult> {
    match name {
        "cache_size" => {
            let Some(crate::ast::PragmaValue::Int(pages)) = value else {
                return Err(ExecutorError::UnsupportedOperation(
                    "cache_size requires an integer page count".to_string(),
                ));
            };
            let pages = usize::try_from(*pages).map_err(|_| {
                ExecutorError::UnsupportedOperation("cache_size is out of range".to_string())
            })?;
            let capacity = pages.checked_mul(CACHE_PAGE_SIZE).ok_or_else(|| {
                ExecutorError::UnsupportedOperation("cache_size is out of range".to_string())
            })?;
            bridge.set_cache_capacity_bytes(capacity)?;
            Ok(ExecutionResult::Success)
        }
        "memory_limit" => {
            let limit = value.map(parse_memory_limit).transpose()?;
            bridge.set_memory_limit_bytes(limit)?;
            Ok(ExecutionResult::Success)
        }
        "io_stats" => {
            let value = io_stats(bridge)?;
            Ok(ExecutionResult::Query(QueryResult::new(
                vec![ColumnInfo::new("io_stats", ResolvedType::Text)],
                vec![vec![value]],
            )))
        }
        _ => Err(ExecutorError::UnsupportedOperation(format!(
            "PRAGMA {name}"
        ))),
    }
}

fn parse_memory_limit(value: &crate::ast::PragmaValue) -> Result<usize> {
    let text = match value {
        crate::ast::PragmaValue::Int(value) => value.to_string(),
        crate::ast::PragmaValue::Text(value) => value.trim().to_ascii_lowercase(),
    };
    let (number, multiplier) = if let Some(value) = text.strip_suffix("kib") {
        (value, 1024usize)
    } else if let Some(value) = text.strip_suffix("mib") {
        (value, 1024usize.pow(2))
    } else if let Some(value) = text.strip_suffix("gib") {
        (value, 1024usize.pow(3))
    } else if let Some(value) = text.strip_suffix("kb") {
        (value, 1000usize)
    } else if let Some(value) = text.strip_suffix("mb") {
        (value, 1000usize.pow(2))
    } else if let Some(value) = text.strip_suffix("gb") {
        (value, 1000usize.pow(3))
    } else {
        (text.as_str(), 1)
    };
    let number = number.trim().parse::<usize>().map_err(|_| {
        ExecutorError::UnsupportedOperation("invalid memory_limit value".to_string())
    })?;
    number.checked_mul(multiplier).ok_or_else(|| {
        ExecutorError::UnsupportedOperation("memory_limit is out of range".to_string())
    })
}
