//! Owned, bounded SQL stream planning for embedded-local consumers.
//!
//! This module deliberately does not reuse `execute_sql_streaming`: that legacy helper creates
//! a borrowed iterator and commits its source transaction before returning it. The public Python
//! stream surface instead obtains one [`OwnedSqlStreamPlan`] here, opens its cursor through an
//! owned core session, and advances the cursor until a terminal lease transition.

use alopex_core::kv::{OwnedKVScan, OwnedKVTransaction};
use alopex_core::{Key, Result as CoreResult, Value};
use alopex_sql::catalog::{
    Catalog, CatalogOverlay, ColumnMetadata, StorageType, TableMetadata, TxnCatalogView,
};
use alopex_sql::executor::evaluator::{evaluate, EvalContext};
use alopex_sql::executor::ColumnInfo;
use alopex_sql::planner::typed_expr::{Projection, TypedExpr};
use alopex_sql::{AlopexDialect, LogicalPlan, Parser, Planner, RowCodec, SqlError, SqlValue};

use crate::{Database, Error, Result};

const UNSUPPORTED_STREAMING_SQL: &str = "unsupported_streaming_sql";

/// A preflight-validated local SQL stream plan.
///
/// Only a single table scan (or literal-only `SELECT`) followed by a row-local filter and a
/// slice is representable. The plan owns every catalog-derived value it needs after opening, so
/// no catalog lock or borrowed SQL transaction is retained by a live stream.
#[derive(Clone, Debug)]
pub struct OwnedSqlStreamPlan {
    source: OwnedSqlSource,
    filter: Option<TypedExpr>,
    projection: Projection,
    columns: Vec<ColumnInfo>,
    offset_remaining: u64,
    limit_remaining: Option<u64>,
}

#[derive(Clone, Debug)]
enum OwnedSqlSource {
    Table(Box<TableMetadata>),
    Literal,
}

/// One row-processing outcome after consuming a physical cursor entry.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedSqlRowOutcome {
    /// The physical entry was filtered or skipped by OFFSET.
    Skip,
    /// One projected SQL row is ready for the caller.
    Row(Vec<SqlValue>),
    /// A `LIMIT` was reached and the cursor must not be advanced again.
    Exhausted,
}

impl OwnedSqlStreamPlan {
    /// Parse and validate the documented v0.8 local SELECT subset without opening a session.
    pub fn preflight(database: &Database, sql: &str) -> Result<Self> {
        let dialect = AlopexDialect;
        let statements = Parser::parse_sql(&dialect, sql).map_err(SqlError::from)?;
        if statements.len() != 1 {
            return Err(unsupported("exactly one SELECT statement is required"));
        }

        let plan = {
            let catalog = database
                .sql_catalog
                .read()
                .map_err(|_| Error::CatalogLockPoisoned)?;
            Planner::new(&*catalog)
                .plan(&statements[0])
                .map_err(SqlError::from)?
        };
        let catalog = database
            .sql_catalog
            .read()
            .map_err(|_| Error::CatalogLockPoisoned)?;
        Self::from_plan(&*catalog, plan)
    }

    /// Parse and validate a local SELECT against an uncommitted catalog overlay.
    ///
    /// The returned plan copies the table metadata it needs, so neither the catalog guard nor the
    /// overlay borrow survives into the cursor.  This preserves transaction-local DDL visibility
    /// without coupling a live Python stream to a borrowed SQL transaction.
    pub fn preflight_in_transaction(
        database: &Database,
        overlay: &CatalogOverlay,
        sql: &str,
    ) -> Result<Self> {
        let dialect = AlopexDialect;
        let statements = Parser::parse_sql(&dialect, sql).map_err(SqlError::from)?;
        if statements.len() != 1 {
            return Err(unsupported("exactly one SELECT statement is required"));
        }

        let catalog = database
            .sql_catalog
            .read()
            .map_err(|_| Error::CatalogLockPoisoned)?;
        let view = TxnCatalogView::new(&*catalog, overlay);
        let plan = Planner::new(&view)
            .plan(&statements[0])
            .map_err(SqlError::from)?;
        Self::from_plan(&view, plan)
    }

    /// Return the output schema before a cursor is opened.
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Open the physical cursor through the owned core transaction supplied by the session lease.
    pub fn open_cursor(
        &self,
        transaction: &mut dyn OwnedKVTransaction,
    ) -> CoreResult<Box<dyn OwnedKVScan>> {
        match &self.source {
            OwnedSqlSource::Table(table) => {
                transaction.scan_prefix(&alopex_sql::KeyEncoder::table_prefix(table.table_id))
            }
            OwnedSqlSource::Literal => Ok(Box::new(OneRowCursor { emitted: false })),
        }
    }

    /// Whether the logical slice is already exhausted without another storage read.
    pub fn is_exhausted(&self) -> bool {
        self.limit_remaining == Some(0)
    }

    /// Decode, filter, slice, and project a single owned cursor entry.
    pub fn process_entry(&mut self, key: Key, value: Value) -> Result<OwnedSqlRowOutcome> {
        if self.is_exhausted() {
            return Ok(OwnedSqlRowOutcome::Exhausted);
        }

        let row = match &self.source {
            OwnedSqlSource::Table(table) => {
                let (table_id, _) = alopex_sql::KeyEncoder::decode_row_key(&key)
                    .map_err(|error| Error::Sql(SqlError::from(error)))?;
                if table_id != table.table_id {
                    return Err(Error::Sql(SqlError::Execution {
                        message: "owned table cursor yielded a row from another table".to_string(),
                        code: "ALOPEX-E020",
                    }));
                }
                RowCodec::decode(&value).map_err(|error| Error::Sql(SqlError::from(error)))?
            }
            OwnedSqlSource::Literal => Vec::new(),
        };

        if let Some(predicate) = &self.filter {
            let context = EvalContext::new(&row);
            if !matches!(
                evaluate(predicate, &context).map_err(sql_execution_error)?,
                SqlValue::Boolean(true)
            ) {
                return Ok(OwnedSqlRowOutcome::Skip);
            }
        }

        if self.offset_remaining > 0 {
            self.offset_remaining -= 1;
            return Ok(OwnedSqlRowOutcome::Skip);
        }

        let projected = project_row(&self.projection, &row)?;
        if let Some(remaining) = &mut self.limit_remaining {
            *remaining = remaining.saturating_sub(1);
        }
        Ok(OwnedSqlRowOutcome::Row(projected))
    }

    fn from_plan(catalog: &impl Catalog, plan: LogicalPlan) -> Result<Self> {
        let mut filter = None;
        let mut limit = None;
        let mut offset = 0;
        let scan = unwrap_stream_nodes(plan, &mut filter, &mut limit, &mut offset)?;
        let LogicalPlan::Scan { table, projection } = scan else {
            return Err(unsupported(
                "only a table scan or literal SELECT is streamable",
            ));
        };

        let source = if table == alopex_sql::ast::dml::LITERAL_TABLE {
            OwnedSqlSource::Literal
        } else {
            let table_meta = catalog
                .get_table(&table)
                .cloned()
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            if table_meta.storage_options.storage_type != StorageType::Row {
                return Err(unsupported(
                    "columnar tables require the LocalScan.columnar_segment streaming path",
                ));
            }
            OwnedSqlSource::Table(Box::new(table_meta))
        };
        let schema = match &source {
            OwnedSqlSource::Table(table) => table.columns.clone(),
            OwnedSqlSource::Literal => Vec::new(),
        };
        let columns = columns_for(&projection, &schema)?;

        Ok(Self {
            source,
            filter,
            projection,
            columns,
            offset_remaining: offset,
            limit_remaining: limit,
        })
    }
}

/// An owned one-entry cursor used only for literal-only SELECT queries. The plan recognizes the
/// empty key/value pair as its synthetic row; no caller can observe this physical representation.
struct OneRowCursor {
    emitted: bool,
}

impl OwnedKVScan for OneRowCursor {
    fn next_entry(&mut self) -> CoreResult<Option<(Key, Value)>> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some((Vec::new(), Vec::new())))
        }
    }
}

fn unwrap_stream_nodes(
    plan: LogicalPlan,
    filter: &mut Option<TypedExpr>,
    limit: &mut Option<u64>,
    offset: &mut u64,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Limit {
            input,
            limit: next_limit,
            offset: next_offset,
            ties,
        } => {
            if ties.is_some() {
                return Err(unsupported("FETCH ... WITH TIES is not streamable"));
            }
            if limit.is_some() || *offset != 0 {
                return Err(unsupported("multiple slice nodes are not streamable"));
            }
            *limit = next_limit;
            *offset = next_offset.unwrap_or(0);
            unwrap_stream_nodes(*input, filter, limit, offset)
        }
        LogicalPlan::Filter { input, predicate } => {
            if filter.replace(predicate).is_some() {
                return Err(unsupported("multiple filter nodes are not streamable"));
            }
            unwrap_stream_nodes(*input, filter, limit, offset)
        }
        LogicalPlan::Scan { .. } => Ok(plan),
        _ => Err(unsupported(
            "streaming supports only SELECT with one table, row-local WHERE, projection, LIMIT, and OFFSET",
        )),
    }
}

fn columns_for(projection: &Projection, schema: &[ColumnMetadata]) -> Result<Vec<ColumnInfo>> {
    match projection {
        Projection::All(names) => names
            .iter()
            .map(|name| {
                let column = schema
                    .iter()
                    .find(|column| column.name == *name)
                    .ok_or_else(|| {
                        Error::Sql(SqlError::Execution {
                            message: format!("column not found: {name}"),
                            code: "ALOPEX-E020",
                        })
                    })?;
                Ok(ColumnInfo::new(&column.name, column.data_type.clone()))
            })
            .collect(),
        Projection::Columns(columns) => Ok(columns
            .iter()
            .map(|column| {
                let name = column
                    .alias
                    .clone()
                    .unwrap_or_else(|| match &column.expr.kind {
                        alopex_sql::TypedExprKind::ColumnRef { column, .. } => column.clone(),
                        _ => "?column?".to_string(),
                    });
                ColumnInfo::new(name, column.expr.resolved_type.clone())
            })
            .collect()),
    }
}

fn project_row(projection: &Projection, row: &[SqlValue]) -> Result<Vec<SqlValue>> {
    match projection {
        Projection::All(names) => {
            if names.len() != row.len() {
                return Err(Error::Sql(SqlError::Execution {
                    message: "owned stream projection no longer matches the table schema"
                        .to_string(),
                    code: "ALOPEX-E020",
                }));
            }
            Ok(row.to_vec())
        }
        Projection::Columns(columns) => {
            let context = EvalContext::new(row);
            columns
                .iter()
                .map(|column| evaluate(&column.expr, &context).map_err(sql_execution_error))
                .collect()
        }
    }
}

fn sql_execution_error(error: alopex_sql::ExecutorError) -> Error {
    Error::Sql(SqlError::from(error))
}

fn unsupported(message: impl Into<String>) -> Error {
    Error::Sql(SqlError::Execution {
        message: message.into(),
        code: UNSUPPORTED_STREAMING_SQL,
    })
}

#[cfg(test)]
mod tests {
    use alopex_core::txn::OwnedLeaseOutcome;
    use alopex_core::TxnMode;

    use super::{OwnedSqlRowOutcome, OwnedSqlStreamPlan};
    use crate::Database;

    #[test]
    fn owned_sql_plan_streams_filter_projection_and_slice_without_borrowed_transaction() {
        let database = Database::new();
        database
            .execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, enabled BOOLEAN)")
            .unwrap();
        database
            .execute_sql(
                "INSERT INTO users (id, name, enabled) VALUES (1, 'one', true), (2, 'two', false), (3, 'three', true)",
            )
            .unwrap();

        let mut plan = OwnedSqlStreamPlan::preflight(
            &database,
            "SELECT name, id + 10 AS next_id FROM users WHERE enabled = true LIMIT 1 OFFSET 1",
        )
        .unwrap();
        assert_eq!(
            plan.columns()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["name", "next_id"]
        );

        let session = database.begin_owned_read(Default::default()).unwrap();
        let lease = session.acquire_lease().unwrap();
        let mut cursor = lease
            .with_transaction(|transaction| plan.open_cursor(transaction))
            .unwrap();
        let mut rows = Vec::new();
        while let Some((key, value)) = cursor.next_entry().unwrap() {
            if let OwnedSqlRowOutcome::Row(row) = plan.process_entry(key, value).unwrap() {
                rows.push(row);
            }
        }
        cursor.close().unwrap();
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                alopex_sql::SqlValue::Text("three".to_string()),
                alopex_sql::SqlValue::Integer(13),
            ]]
        );
    }

    #[test]
    fn unsupported_streaming_sql_is_rejected_before_an_owned_session_is_opened() {
        let database = Database::new();
        database
            .execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
            .unwrap();
        let error = OwnedSqlStreamPlan::preflight(&database, "SELECT id FROM users ORDER BY id")
            .unwrap_err();
        assert_eq!(error.sql_error_code(), Some("unsupported_streaming_sql"));

        let session = database
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();
        let lease = session.acquire_lease().unwrap();
        lease
            .with_transaction(|transaction| {
                transaction.put(b"still-open".to_vec(), b"yes".to_vec())
            })
            .unwrap();
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
        session.rollback().unwrap();
    }

    #[test]
    fn literal_only_select_uses_the_owned_values_cursor() {
        let database = Database::new();
        let mut plan = OwnedSqlStreamPlan::preflight(&database, "SELECT 1 + 2 AS value").unwrap();
        let session = database.begin_owned_read(Default::default()).unwrap();
        let lease = session.acquire_lease().unwrap();
        let mut cursor = lease
            .with_transaction(|transaction| plan.open_cursor(transaction))
            .unwrap();
        let (key, value) = cursor.next_entry().unwrap().unwrap();
        assert_eq!(
            plan.process_entry(key, value).unwrap(),
            OwnedSqlRowOutcome::Row(vec![alopex_sql::SqlValue::Integer(3)])
        );
        assert!(cursor.next_entry().unwrap().is_none());
        cursor.close().unwrap();
        lease.finish(OwnedLeaseOutcome::Exhausted).unwrap();
    }
}
