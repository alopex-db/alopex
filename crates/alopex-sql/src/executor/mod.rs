//! SQL Executor module for Alopex SQL.
//!
//! This module provides the execution engine for SQL statements.
//!
//! # Overview
//!
//! The Executor takes a [`LogicalPlan`] from the Planner and executes it
//! against the storage layer. It supports DDL, DML, and Query operations.
//!
//! Query execution currently materializes intermediate results per stage;
//! future versions may add streaming pipelines as requirements grow.

//! # Components
//!
//! - [`Executor`]: Main executor struct
//! - [`ExecutorError`]: Error types for execution
//! - [`ExecutionResult`]: Execution result types
//!
//! # Example
//!
//! ```ignore
//! use std::sync::{Arc, RwLock};
//! use alopex_core::kv::memory::MemoryKV;
//! use alopex_sql::executor::Executor;
//! use alopex_sql::catalog::MemoryCatalog;
//! use alopex_sql::planner::LogicalPlan;
//!
//! // Create storage and catalog
//! let store = Arc::new(MemoryKV::new());
//! let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
//!
//! // Create executor
//! let mut executor = Executor::new(store, catalog);
//!
//! // Execute a plan
//! let result = executor.execute(plan)?;
//! ```

#[cfg(feature = "tokio")]
pub mod async_executor;
pub mod bulk;
pub(crate) mod ddl;
pub(crate) mod dml;
mod error;
pub mod evaluator;
mod fts_bridge;
mod hnsw_bridge;
pub mod memory;
pub mod query;
mod result;
mod system;

#[cfg(feature = "tokio")]
pub use async_executor::AsyncExecutor;
pub use error::{ConstraintViolation, EvaluationError, ExecutorError, Result};
pub use memory::{MemoryPolicy, SpillPolicy};
pub use query::{RowIterator, ScanIterator, build_streaming_pipeline};
pub use result::{ColumnInfo, ExecutionResult, QueryResult, QueryRowIterator, Row};

/// Returns whether a plan requires direct access to the backing KV store.
pub fn is_store_direct_plan(plan: &LogicalPlan) -> bool {
    system::is_store_direct_plan(plan)
}

use std::sync::{Arc, RwLock};

use alopex_core::kv::KVStore;
use alopex_core::types::TxnMode;

use crate::catalog::Catalog;
use crate::catalog::persistent::{IndexFqn, TableFqn};
use crate::catalog::{CatalogError, CatalogOverlay, PersistentCatalog, TxnCatalogView};
use crate::planner::LogicalPlan;
use crate::storage::{
    BorrowedSqlTransaction, KeyEncoder, SqlTransaction, SqlTxn as _, SqlValue, TxnBridge,
};
use crate::{ExplainFormat, ResolvedType};
use std::time::Instant;

fn explain_result(
    plan: &LogicalPlan,
    analyze: bool,
    format: ExplainFormat,
    elapsed_ns: Option<u64>,
    rows: Option<u64>,
) -> ExecutionResult {
    let (column, value) = match format {
        ExplainFormat::Text => ("QUERY PLAN", plan.explain_text(elapsed_ns, rows)),
        ExplainFormat::Json => ("query_plan", plan.explain_json(analyze, elapsed_ns, rows)),
    };
    ExecutionResult::Query(QueryResult::new(
        vec![ColumnInfo::new(column, ResolvedType::Text)],
        vec![vec![SqlValue::Text(value)]],
    ))
}

fn result_rows(result: &ExecutionResult) -> u64 {
    match result {
        ExecutionResult::Success => 0,
        ExecutionResult::RowsAffected(rows) => *rows,
        ExecutionResult::Query(result) => result.rows.len() as u64,
    }
}

/// SQL statement executor.
///
/// The Executor takes a [`LogicalPlan`] and executes it against the storage layer.
/// It manages transactions and coordinates between DDL, DML, and Query operations.
///
/// # Type Parameters
///
/// - `S`: The underlying KV store type (must implement [`KVStore`])
/// - `C`: The catalog type (must implement [`Catalog`])
pub struct Executor<S: KVStore, C: Catalog> {
    /// Transaction bridge for storage operations.
    bridge: TxnBridge<S>,

    /// Catalog for metadata operations.
    catalog: Arc<RwLock<C>>,
}

impl<S: KVStore, C: Catalog> Executor<S, C> {
    fn run_in_write_txn<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut SqlTransaction<'_, S>) -> Result<R>,
    {
        let mut txn = self.bridge.begin_write().map_err(ExecutorError::from)?;
        match f(&mut txn) {
            Ok(result) => {
                txn.commit().map_err(ExecutorError::from)?;
                Ok(result)
            }
            Err(err) => {
                txn.rollback().map_err(ExecutorError::from)?;
                Err(err)
            }
        }
    }

    /// Create a new Executor with the given store and catalog.
    ///
    /// # Arguments
    ///
    /// - `store`: The underlying KV store
    /// - `catalog`: The catalog for metadata operations
    pub fn new(store: Arc<S>, catalog: Arc<RwLock<C>>) -> Self {
        Self {
            bridge: TxnBridge::new(store),
            catalog,
        }
    }

    /// Execute a logical plan and return the result.
    ///
    /// # Arguments
    ///
    /// - `plan`: The logical plan to execute
    ///
    /// # Returns
    ///
    /// Returns an [`ExecutionResult`] on success, or an [`ExecutorError`] on failure.
    ///
    /// # DDL Operations
    ///
    /// - `CreateTable`: Creates a new table with optional PK index
    /// - `DropTable`: Drops a table and its associated indexes
    /// - `CreateIndex`: Creates a new index
    /// - `DropIndex`: Drops an index
    ///
    /// # DML Operations
    ///
    /// - `Insert`: Inserts rows into a table
    /// - `Update`: Updates rows in a table
    /// - `Delete`: Deletes rows from a table
    ///
    /// # Query Operations
    ///
    /// - `Scan`, `Filter`, `Sort`, `Limit`: SELECT query execution
    pub fn execute(&mut self, plan: LogicalPlan) -> Result<ExecutionResult> {
        let _statement_timestamp = evaluator::begin_statement();
        let plan = match plan {
            LogicalPlan::Explain {
                analyze,
                format,
                input,
            } => {
                if !analyze {
                    return Ok(explain_result(&input, false, format, None, None));
                }
                let started = Instant::now();
                let result = self.execute((*input).clone())?;
                let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                return Ok(explain_result(
                    &input,
                    true,
                    format,
                    Some(elapsed_ns),
                    Some(result_rows(&result)),
                ));
            }
            plan => plan,
        };
        match plan {
            LogicalPlan::Explain { .. } => unreachable!("EXPLAIN handled before dispatch"),
            LogicalPlan::Pragma { name, value } => {
                system::execute_pragma(&self.bridge, &name, value.as_ref())
            }
            // DDL Operations
            LogicalPlan::CreateTable {
                table,
                if_not_exists,
                with_options,
            } => self.execute_create_table(table, with_options, if_not_exists),
            LogicalPlan::DropTable { name, if_exists } => self.execute_drop_table(&name, if_exists),
            LogicalPlan::CreateView {
                table,
                if_not_exists,
            } => self.execute_create_table(table, Vec::new(), if_not_exists),
            LogicalPlan::DropView { name, if_exists } => self.execute_drop_view(&name, if_exists),
            LogicalPlan::AlterTable {
                name,
                if_exists,
                action,
            } => self.execute_alter_table(&name, if_exists, action),
            LogicalPlan::Truncate { name } => self.execute_truncate(&name),
            LogicalPlan::CreateIndex {
                index,
                if_not_exists,
            } => self.execute_create_index(index, if_not_exists),
            LogicalPlan::DropIndex { name, if_exists } => self.execute_drop_index(&name, if_exists),
            LogicalPlan::Copy {
                query: Some(_),
                direction: crate::ast::CopyDirection::From,
                ..
            } => Err(ExecutorError::InvalidOperation {
                operation: "COPY FROM".into(),
                reason: "COPY FROM requires a table source and file input".into(),
            }),
            LogicalPlan::Copy {
                query: Some(query),
                path,
                options,
                direction: crate::ast::CopyDirection::To,
                ..
            } => {
                let header = options.iter().any(|option| {
                    option.name.eq_ignore_ascii_case("header")
                        && option.value.eq_ignore_ascii_case("true")
                });
                let format = copy_format(&path, &options);
                let ExecutionResult::Query(result) = self.execute_query(*query)? else {
                    return Err(ExecutorError::InvalidOperation {
                        operation: "COPY TO".into(),
                        reason: "query source did not return rows".into(),
                    });
                };
                bulk::execute_copy_query_to(
                    &result,
                    &path,
                    format,
                    bulk::CopyOptions { header },
                    &bulk::CopySecurityConfig::default(),
                )
            }
            LogicalPlan::Copy {
                table,
                path,
                options,
                direction: crate::ast::CopyDirection::To,
                query: None,
            } => {
                let catalog = self.catalog.read().expect("catalog lock poisoned");
                let header = options.iter().any(|option| {
                    option.name.eq_ignore_ascii_case("header")
                        && option.value.eq_ignore_ascii_case("true")
                });
                let format = copy_format(&path, &options);
                self.run_in_write_txn(|txn| {
                    bulk::execute_copy_to(
                        txn,
                        &*catalog,
                        &table,
                        &path,
                        format,
                        bulk::CopyOptions { header },
                        &bulk::CopySecurityConfig::default(),
                    )
                })
            }
            LogicalPlan::Copy {
                table,
                path,
                options,
                direction: crate::ast::CopyDirection::From,
                query: None,
                ..
            } => {
                let catalog = self.catalog.read().expect("catalog lock poisoned");
                self.run_in_write_txn(|txn| {
                    let format = if path.ends_with(".parquet") {
                        bulk::FileFormat::Parquet
                    } else {
                        bulk::FileFormat::Csv
                    };
                    let header = options.iter().any(|option| {
                        option.name.eq_ignore_ascii_case("header")
                            && option.value.eq_ignore_ascii_case("true")
                    });
                    bulk::execute_copy(
                        txn,
                        &*catalog,
                        &table,
                        &path,
                        format,
                        bulk::CopyOptions { header },
                        &bulk::CopySecurityConfig::default(),
                    )
                })
            }

            LogicalPlan::CreateSequence(statement) => {
                self.run_in_write_txn(|txn| ddl::sequence::create(txn, statement))
            }
            LogicalPlan::AlterSequence(statement) => {
                self.run_in_write_txn(|txn| ddl::sequence::alter(txn, statement))
            }
            LogicalPlan::DropSequence(statement) => {
                self.run_in_write_txn(|txn| ddl::sequence::drop(txn, statement))
            }

            // DML Operations
            LogicalPlan::Insert {
                table,
                columns,
                values,
                conflict,
                returning,
            } => self.execute_insert(&table, columns, values, conflict, returning),
            LogicalPlan::InsertSelect {
                table,
                columns,
                source,
                conflict,
                returning,
            } => self.execute_insert_select(&table, columns, *source, conflict, returning),
            LogicalPlan::Update {
                table,
                assignments,
                filter,
                join_source,
                returning,
            } => self.execute_update(&table, assignments, filter, join_source, returning),
            LogicalPlan::Delete {
                table,
                filter,
                join_source,
                returning,
            } => self.execute_delete(&table, filter, join_source, returning),

            // Query Operations
            LogicalPlan::Scan { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::LateralJoin { .. }
            | LogicalPlan::TableFunction { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::SetOperation { .. }
            | LogicalPlan::RecursiveCte { .. }
            | LogicalPlan::RecursiveReference { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::DistinctOn { .. }
            | LogicalPlan::Limit { .. } => self.execute_query(plan),
        }
    }

    // ========================================================================
    // DDL Operations (to be implemented in Phase 2)
    // ========================================================================

    fn execute_create_table(
        &mut self,
        table: crate::catalog::TableMetadata,
        with_options: Vec<(String, String)>,
        if_not_exists: bool,
    ) -> Result<ExecutionResult> {
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            ddl::create_table::execute_create_table(
                txn,
                &mut *catalog,
                table,
                with_options,
                if_not_exists,
            )
        })
    }

    fn execute_drop_table(&mut self, name: &str, if_exists: bool) -> Result<ExecutionResult> {
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            ddl::drop_table::execute_drop_table(txn, &mut *catalog, name, if_exists)
        })
    }

    fn execute_drop_view(&mut self, name: &str, if_exists: bool) -> Result<ExecutionResult> {
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            let Some(view) =
                ddl::schema_evolution::prepare_drop_view(txn, &*catalog, name, if_exists)?
            else {
                return Ok(ExecutionResult::Success);
            };
            catalog.drop_table(&view.name)?;
            Ok(ExecutionResult::Success)
        })
    }

    fn execute_alter_table(
        &mut self,
        name: &str,
        if_exists: bool,
        action: crate::ast::AlterTableAction,
    ) -> Result<ExecutionResult> {
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        if !catalog.table_exists(name) && if_exists {
            return Ok(ExecutionResult::Success);
        }
        self.run_in_write_txn(|txn| {
            let outcome = ddl::schema_evolution::prepare_alter(txn, &*catalog, name, action)?;
            catalog.drop_table(&outcome.old_table.name)?;
            catalog.create_table(outcome.new_table)?;
            for (_, index) in outcome.updated_indexes {
                catalog.create_index(index)?;
            }
            Ok(ExecutionResult::Success)
        })
    }

    fn execute_truncate(&mut self, name: &str) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| ddl::schema_evolution::execute_truncate(txn, &*catalog, name))
    }

    fn execute_create_index(
        &mut self,
        index: crate::catalog::IndexMetadata,
        if_not_exists: bool,
    ) -> Result<ExecutionResult> {
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            ddl::create_index::execute_create_index(txn, &mut *catalog, index, if_not_exists)
        })
    }

    fn execute_drop_index(&mut self, name: &str, if_exists: bool) -> Result<ExecutionResult> {
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            ddl::drop_index::execute_drop_index(txn, &mut *catalog, name, if_exists)
        })
    }

    // ========================================================================
    // DML Operations (implemented in Phase 4)
    // ========================================================================

    fn execute_insert(
        &mut self,
        table: &str,
        columns: Vec<String>,
        values: Vec<Vec<crate::planner::TypedExpr>>,
        conflict: Option<crate::planner::OnConflictPlan>,
        returning: Option<crate::planner::typed_expr::Projection>,
    ) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            dml::execute_insert_with_plan(
                txn, &*catalog, table, columns, values, conflict, returning,
            )
        })
    }

    fn execute_insert_select(
        &mut self,
        table: &str,
        columns: Vec<String>,
        source: LogicalPlan,
        conflict: Option<crate::planner::OnConflictPlan>,
        returning: Option<crate::planner::typed_expr::Projection>,
    ) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            let ExecutionResult::Query(result) = query::execute_query(txn, &*catalog, source)?
            else {
                return Err(ExecutorError::InvalidOperation {
                    operation: "INSERT ... SELECT".into(),
                    reason: "SELECT source did not return query rows".into(),
                });
            };
            dml::execute_insert_rows_with_plan(
                txn,
                &*catalog,
                table,
                columns,
                result.rows,
                conflict,
                returning,
            )
        })
    }

    fn execute_update(
        &mut self,
        table: &str,
        assignments: Vec<crate::planner::TypedAssignment>,
        filter: Option<crate::planner::TypedExpr>,
        join_source: Option<crate::planner::JoinedDmlSource>,
        returning: Option<crate::planner::typed_expr::Projection>,
    ) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            dml::execute_update_with_returning(
                txn,
                &*catalog,
                table,
                assignments,
                filter,
                join_source,
                returning,
            )
        })
    }

    fn execute_delete(
        &mut self,
        table: &str,
        filter: Option<crate::planner::TypedExpr>,
        join_source: Option<crate::planner::JoinedDmlSource>,
        returning: Option<crate::planner::typed_expr::Projection>,
    ) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| {
            dml::execute_delete_with_returning(
                txn,
                &*catalog,
                table,
                filter,
                join_source,
                returning,
            )
        })
    }

    // ========================================================================
    // Query Operations (to be implemented in Phase 5)
    // ========================================================================

    fn execute_query(&mut self, plan: LogicalPlan) -> Result<ExecutionResult> {
        if let Some(result) = system::try_execute(&self.bridge, &plan)? {
            return Ok(result);
        }
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| query::execute_query(txn, &*catalog, plan))
    }
}

impl<S: KVStore> Executor<S, PersistentCatalog<S>> {
    pub fn execute_in_txn<'a, 'b, 'c>(
        &mut self,
        plan: LogicalPlan,
        txn: &mut BorrowedSqlTransaction<'a, 'b, 'c, S>,
    ) -> Result<ExecutionResult> {
        let plan = match plan {
            LogicalPlan::Explain {
                analyze,
                format,
                input,
            } => {
                if !analyze {
                    return Ok(explain_result(&input, false, format, None, None));
                }
                let started = Instant::now();
                let result = self.execute_in_txn((*input).clone(), txn)?;
                let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                return Ok(explain_result(
                    &input,
                    true,
                    format,
                    Some(elapsed_ns),
                    Some(result_rows(&result)),
                ));
            }
            plan => plan,
        };
        if txn.mode() == TxnMode::ReadOnly
            && !matches!(
                plan,
                LogicalPlan::Scan { .. }
                    | LogicalPlan::Values { .. }
                    | LogicalPlan::Filter { .. }
                    | LogicalPlan::Project { .. }
                    | LogicalPlan::Join { .. }
                    | LogicalPlan::LateralJoin { .. }
                    | LogicalPlan::TableFunction { .. }
                    | LogicalPlan::Aggregate { .. }
                    | LogicalPlan::Window { .. }
                    | LogicalPlan::SetOperation { .. }
                    | LogicalPlan::RecursiveCte { .. }
                    | LogicalPlan::RecursiveReference { .. }
                    | LogicalPlan::Sort { .. }
                    | LogicalPlan::DistinctOn { .. }
                    | LogicalPlan::Limit { .. }
            )
        {
            return Err(ExecutorError::ReadOnlyTransaction {
                operation: plan.operation_name().to_string(),
            });
        }

        let _statement_timestamp = evaluator::begin_statement();
        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        let (mut sql_txn, overlay) = txn.split_parts();

        let result = match plan {
            LogicalPlan::Explain { .. } => unreachable!("EXPLAIN handled before dispatch"),
            LogicalPlan::CreateTable {
                table,
                if_not_exists,
                with_options,
            } => self.execute_create_table_in_txn(
                &mut *catalog,
                &mut sql_txn,
                overlay,
                table,
                with_options,
                if_not_exists,
            ),
            LogicalPlan::DropTable { name, if_exists } => self.execute_drop_table_in_txn(
                &mut *catalog,
                &mut sql_txn,
                overlay,
                &name,
                if_exists,
            ),
            LogicalPlan::CreateView {
                table,
                if_not_exists,
            } => self.execute_create_table_in_txn(
                &mut *catalog,
                &mut sql_txn,
                overlay,
                table,
                Vec::new(),
                if_not_exists,
            ),
            LogicalPlan::DropView { name, if_exists } => self.execute_drop_view_in_txn(
                &mut *catalog,
                &mut sql_txn,
                overlay,
                &name,
                if_exists,
            ),
            LogicalPlan::AlterTable {
                name,
                if_exists,
                action,
            } => self.execute_alter_table_in_txn(
                &mut *catalog,
                &mut sql_txn,
                overlay,
                &name,
                if_exists,
                action,
            ),
            LogicalPlan::Truncate { name } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                ddl::schema_evolution::execute_truncate(&mut sql_txn, &view, &name)
            }
            LogicalPlan::CreateIndex {
                index,
                if_not_exists,
            } => self.execute_create_index_in_txn(
                &mut *catalog,
                &mut sql_txn,
                overlay,
                index,
                if_not_exists,
            ),
            LogicalPlan::DropIndex { name, if_exists } => self.execute_drop_index_in_txn(
                &mut *catalog,
                &mut sql_txn,
                overlay,
                &name,
                if_exists,
            ),
            LogicalPlan::Pragma { .. } => Err(ExecutorError::UnsupportedOperation(
                "PRAGMA is not available inside an external transaction".to_string(),
            )),
            LogicalPlan::Insert {
                table,
                columns,
                values,
                conflict,
                returning,
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                dml::execute_insert_with_plan(
                    &mut sql_txn,
                    &view,
                    &table,
                    columns,
                    values,
                    conflict,
                    returning,
                )
            }
            LogicalPlan::Copy {
                query: Some(_),
                direction: crate::ast::CopyDirection::From,
                ..
            } => Err(ExecutorError::InvalidOperation {
                operation: "COPY FROM".into(),
                reason: "COPY FROM requires a table source and file input".into(),
            }),
            LogicalPlan::Copy {
                query: Some(query),
                path,
                options,
                direction: crate::ast::CopyDirection::To,
                ..
            } => {
                let header = options.iter().any(|option| {
                    option.name.eq_ignore_ascii_case("header")
                        && option.value.eq_ignore_ascii_case("true")
                });
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                let ExecutionResult::Query(result) =
                    query::execute_query(&mut sql_txn, &view, *query)?
                else {
                    return Err(ExecutorError::InvalidOperation {
                        operation: "COPY TO".into(),
                        reason: "query source did not return rows".into(),
                    });
                };
                let format = copy_format(&path, &options);
                bulk::execute_copy_query_to(
                    &result,
                    &path,
                    format,
                    bulk::CopyOptions { header },
                    &bulk::CopySecurityConfig::default(),
                )
            }
            LogicalPlan::Copy {
                table,
                path,
                options,
                direction: crate::ast::CopyDirection::To,
                query: None,
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                let header = options.iter().any(|option| {
                    option.name.eq_ignore_ascii_case("header")
                        && option.value.eq_ignore_ascii_case("true")
                });
                let format = copy_format(&path, &options);
                bulk::execute_copy_to(
                    &mut sql_txn,
                    &view,
                    &table,
                    &path,
                    format,
                    bulk::CopyOptions { header },
                    &bulk::CopySecurityConfig::default(),
                )
            }
            LogicalPlan::Copy {
                table,
                path,
                options,
                direction: crate::ast::CopyDirection::From,
                query: None,
                ..
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                let format = if path.ends_with(".parquet") {
                    bulk::FileFormat::Parquet
                } else {
                    bulk::FileFormat::Csv
                };
                let header = options.iter().any(|option| {
                    option.name.eq_ignore_ascii_case("header")
                        && option.value.eq_ignore_ascii_case("true")
                });
                bulk::execute_copy(
                    &mut sql_txn,
                    &view,
                    &table,
                    &path,
                    format,
                    bulk::CopyOptions { header },
                    &bulk::CopySecurityConfig::default(),
                )
            }
            LogicalPlan::CreateSequence(statement) => {
                ddl::sequence::create(&mut sql_txn, statement)
            }
            LogicalPlan::AlterSequence(statement) => ddl::sequence::alter(&mut sql_txn, statement),
            LogicalPlan::DropSequence(statement) => ddl::sequence::drop(&mut sql_txn, statement),
            LogicalPlan::InsertSelect {
                table,
                columns,
                source,
                conflict,
                returning,
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                let ExecutionResult::Query(result) =
                    query::execute_query(&mut sql_txn, &view, *source)?
                else {
                    return Err(ExecutorError::InvalidOperation {
                        operation: "INSERT ... SELECT".into(),
                        reason: "SELECT source did not return query rows".into(),
                    });
                };
                dml::execute_insert_rows_with_plan(
                    &mut sql_txn,
                    &view,
                    &table,
                    columns,
                    result.rows,
                    conflict,
                    returning,
                )
            }
            LogicalPlan::Update {
                table,
                assignments,
                filter,
                join_source,
                returning,
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                dml::execute_update_with_returning(
                    &mut sql_txn,
                    &view,
                    &table,
                    assignments,
                    filter,
                    join_source,
                    returning,
                )
            }
            LogicalPlan::Delete {
                table,
                filter,
                join_source,
                returning,
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                dml::execute_delete_with_returning(
                    &mut sql_txn,
                    &view,
                    &table,
                    filter,
                    join_source,
                    returning,
                )
            }
            LogicalPlan::Scan { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::LateralJoin { .. }
            | LogicalPlan::TableFunction { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::SetOperation { .. }
            | LogicalPlan::RecursiveCte { .. }
            | LogicalPlan::RecursiveReference { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::DistinctOn { .. }
            | LogicalPlan::Limit { .. } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                if let Some(name) = system::direct_nextval_name(&plan) {
                    let value = ddl::sequence::next_value(&mut sql_txn, name)?;
                    Ok(ExecutionResult::Query(QueryResult::new(
                        vec![ColumnInfo::new("nextval", ResolvedType::BigInt)],
                        vec![vec![SqlValue::BigInt(value)]],
                    )))
                } else {
                    query::execute_query(&mut sql_txn, &view, plan)
                }
            }
        };

        match result {
            Ok(value) => {
                sql_txn.flush_hnsw()?;
                Ok(value)
            }
            Err(err) => {
                let _ = sql_txn.abandon_hnsw();
                Err(err)
            }
        }
    }

    fn map_catalog_error(err: CatalogError) -> ExecutorError {
        match err {
            CatalogError::Kv(e) => ExecutorError::Core(e),
            CatalogError::Serialize(e) => ExecutorError::InvalidOperation {
                operation: "CatalogPersistence".into(),
                reason: e.to_string(),
            },
            CatalogError::InvalidKey(reason) => ExecutorError::InvalidOperation {
                operation: "CatalogPersistence".into(),
                reason,
            },
        }
    }

    fn execute_create_table_in_txn<'txn>(
        &self,
        catalog: &mut PersistentCatalog<S>,
        txn: &mut impl crate::storage::SqlTxn<'txn, S>,
        overlay: &mut CatalogOverlay,
        mut table: crate::catalog::TableMetadata,
        with_options: Vec<(String, String)>,
        if_not_exists: bool,
    ) -> Result<ExecutionResult>
    where
        S: 'txn,
    {
        if catalog.table_exists_in_txn(&table.name, overlay) {
            return if if_not_exists {
                Ok(ExecutionResult::Success)
            } else {
                Err(ExecutorError::TableAlreadyExists(table.name))
            };
        }

        table.storage_options = ddl::create_table::parse_storage_options(&with_options)?;

        let pk_index = if let Some(pk_columns) = table.primary_key.clone() {
            let column_indices = pk_columns
                .iter()
                .map(|name| {
                    table
                        .get_column_index(name)
                        .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
                })
                .collect::<Result<Vec<_>>>()?;
            let index_id = catalog.next_index_id();
            let index_name = ddl::create_pk_index_name(&table.name);
            let mut index = crate::catalog::IndexMetadata::new(
                index_id,
                index_name,
                table.name.clone(),
                pk_columns,
            )
            .with_column_indices(column_indices)
            .with_unique(true);
            index.catalog_name = table.catalog_name.clone();
            index.namespace_name = table.namespace_name.clone();
            Some(index)
        } else {
            None
        };

        let table_id = catalog.next_table_id();
        table = table.with_table_id(table_id);

        // storage keyspace の初期化
        txn.delete_prefix(&KeyEncoder::table_prefix(table_id))?;
        txn.delete_prefix(&KeyEncoder::sequence_key(table_id))?;

        // 永続化（同一 KV トランザクション内）
        catalog
            .persist_create_table(txn.inner_mut(), &table)
            .map_err(Self::map_catalog_error)?;
        for column in &table.columns {
            if let Some(sequence) = &column.generated_sequence {
                ddl::sequence::create_generated(
                    txn,
                    sequence.clone(),
                    column
                        .generated_sequence_options
                        .clone()
                        .unwrap_or_default(),
                )?;
            }
        }
        if let Some(index) = &pk_index {
            catalog
                .persist_create_index(txn.inner_mut(), index)
                .map_err(Self::map_catalog_error)?;
        }

        // オーバーレイに反映（ベースカタログはコミットまで不変）
        overlay.add_table(TableFqn::from(&table), table);
        if let Some(index) = pk_index {
            overlay.add_index(IndexFqn::from(&index), index);
        }

        Ok(ExecutionResult::Success)
    }

    fn execute_drop_table_in_txn<'txn>(
        &self,
        catalog: &mut PersistentCatalog<S>,
        txn: &mut impl crate::storage::SqlTxn<'txn, S>,
        overlay: &mut CatalogOverlay,
        table_name: &str,
        if_exists: bool,
    ) -> Result<ExecutionResult>
    where
        S: 'txn,
    {
        let table_meta = match catalog.get_table_in_txn(table_name, overlay) {
            Some(table) => table.clone(),
            None => {
                return if if_exists {
                    Ok(ExecutionResult::Success)
                } else {
                    Err(ExecutorError::TableNotFound(table_name.to_string()))
                };
            }
        };
        if table_meta.catalog_name != "default" || table_meta.namespace_name != "default" {
            return if if_exists {
                Ok(ExecutionResult::Success)
            } else {
                Err(ExecutorError::TableNotFound(table_name.to_string()))
            };
        }

        if table_meta.table_type == crate::TableType::View {
            return Err(ExecutorError::InvalidOperation {
                operation: "DROP TABLE".into(),
                reason: format!("'{}' is a view; use DROP VIEW", table_meta.name),
            });
        }
        ddl::schema_evolution::ensure_no_dependent_views(
            &TxnCatalogView::new(catalog, overlay),
            table_name,
        )?;

        let indexes = TxnCatalogView::new(catalog, overlay)
            .get_indexes_for_table(table_name)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        for index in &indexes {
            if matches!(index.method, Some(crate::ast::ddl::IndexMethod::Hnsw)) {
                crate::executor::hnsw_bridge::HnswBridge::drop_index(txn, index, false)?;
            } else {
                txn.delete_prefix(&KeyEncoder::index_prefix(index.index_id))?;
            }
        }

        txn.delete_prefix(&KeyEncoder::table_prefix(table_meta.table_id))?;
        txn.delete_prefix(&KeyEncoder::sequence_key(table_meta.table_id))?;

        catalog
            .persist_drop_table(txn.inner_mut(), &TableFqn::from(&table_meta))
            .map_err(Self::map_catalog_error)?;

        overlay.drop_table(&TableFqn::from(&table_meta));

        Ok(ExecutionResult::Success)
    }

    fn execute_drop_view_in_txn<'txn>(
        &self,
        catalog: &mut PersistentCatalog<S>,
        txn: &mut impl crate::storage::SqlTxn<'txn, S>,
        overlay: &mut CatalogOverlay,
        name: &str,
        if_exists: bool,
    ) -> Result<ExecutionResult>
    where
        S: 'txn,
    {
        let view = TxnCatalogView::new(catalog, overlay);
        let Some(metadata) = ddl::schema_evolution::prepare_drop_view(txn, &view, name, if_exists)?
        else {
            return Ok(ExecutionResult::Success);
        };
        overlay.drop_table(&TableFqn::from(&metadata));
        Ok(ExecutionResult::Success)
    }

    fn execute_alter_table_in_txn<'txn>(
        &self,
        catalog: &mut PersistentCatalog<S>,
        txn: &mut impl crate::storage::SqlTxn<'txn, S>,
        overlay: &mut CatalogOverlay,
        name: &str,
        if_exists: bool,
        action: crate::ast::AlterTableAction,
    ) -> Result<ExecutionResult>
    where
        S: 'txn,
    {
        if catalog.get_table_in_txn(name, overlay).is_none() && if_exists {
            return Ok(ExecutionResult::Success);
        }
        let view = TxnCatalogView::new(catalog, overlay);
        let outcome = ddl::schema_evolution::prepare_alter(txn, &view, name, action)?;
        if outcome.old_table.name != outcome.new_table.name {
            overlay.drop_table(&TableFqn::from(&outcome.old_table));
        }
        overlay.add_table(TableFqn::from(&outcome.new_table), outcome.new_table);
        for (_, index) in outcome.updated_indexes {
            overlay.add_index(IndexFqn::from(&index), index);
        }
        Ok(ExecutionResult::Success)
    }

    fn execute_create_index_in_txn<'txn>(
        &self,
        catalog: &mut PersistentCatalog<S>,
        txn: &mut impl crate::storage::SqlTxn<'txn, S>,
        overlay: &mut CatalogOverlay,
        mut index: crate::catalog::IndexMetadata,
        if_not_exists: bool,
    ) -> Result<ExecutionResult>
    where
        S: 'txn,
    {
        if ddl::is_implicit_pk_index(&index.name) {
            return Err(ExecutorError::InvalidIndexName {
                name: index.name.clone(),
                reason: "Index names starting with '__pk_' are reserved for PRIMARY KEY".into(),
            });
        }

        if catalog.index_exists_in_txn(&index.name, overlay) {
            return if if_not_exists {
                Ok(ExecutionResult::Success)
            } else {
                Err(ExecutorError::IndexAlreadyExists(index.name))
            };
        }

        let table = catalog
            .get_table_in_txn(&index.table, overlay)
            .ok_or_else(|| ExecutorError::TableNotFound(index.table.clone()))?
            .clone();
        index.catalog_name = table.catalog_name.clone();
        index.namespace_name = table.namespace_name.clone();

        let column_indices = index
            .columns
            .iter()
            .map(|name| {
                table
                    .get_column_index(name)
                    .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let index_id = catalog.next_index_id();
        index.index_id = index_id;
        index.column_indices = column_indices.clone();
        if matches!(index.method, Some(crate::ast::ddl::IndexMethod::Fts)) {
            crate::executor::fts_bridge::FtsBridge::prepare(&mut index)?;
        }

        if matches!(index.method, Some(crate::ast::ddl::IndexMethod::Hnsw)) {
            crate::executor::hnsw_bridge::HnswBridge::create_index(txn, &table, &index)?;
        } else if matches!(index.method, Some(crate::ast::ddl::IndexMethod::Fts)) {
            crate::executor::fts_bridge::FtsBridge::validate(
                &index,
                &table.columns[column_indices[0]].data_type,
            )?;
            ddl::create_index::build_fts_index_for_existing_rows(txn, &table, &index)?;
        } else {
            ddl::create_index::build_index_for_existing_rows(txn, &table, &index, column_indices)?;
        }

        catalog
            .persist_create_index(txn.inner_mut(), &index)
            .map_err(Self::map_catalog_error)?;

        overlay.add_index(IndexFqn::from(&index), index);

        Ok(ExecutionResult::Success)
    }

    fn execute_drop_index_in_txn<'txn>(
        &self,
        catalog: &mut PersistentCatalog<S>,
        txn: &mut impl crate::storage::SqlTxn<'txn, S>,
        overlay: &mut CatalogOverlay,
        index_name: &str,
        if_exists: bool,
    ) -> Result<ExecutionResult>
    where
        S: 'txn,
    {
        if ddl::is_implicit_pk_index(index_name) {
            return Err(ExecutorError::InvalidOperation {
                operation: "DROP INDEX".into(),
                reason: "Cannot drop implicit PRIMARY KEY index directly; use DROP TABLE".into(),
            });
        }

        let index = match catalog.get_index_in_txn(index_name, overlay) {
            Some(index) => index.clone(),
            None => {
                return if if_exists {
                    Ok(ExecutionResult::Success)
                } else {
                    Err(ExecutorError::IndexNotFound(index_name.to_string()))
                };
            }
        };
        if index.catalog_name != "default" || index.namespace_name != "default" {
            return if if_exists {
                Ok(ExecutionResult::Success)
            } else {
                Err(ExecutorError::IndexNotFound(index_name.to_string()))
            };
        }

        if matches!(index.method, Some(crate::ast::ddl::IndexMethod::Hnsw)) {
            crate::executor::hnsw_bridge::HnswBridge::drop_index(txn, &index, if_exists)?;
        } else {
            txn.delete_prefix(&KeyEncoder::index_prefix(index.index_id))?;
        }

        catalog
            .persist_drop_index(txn.inner_mut(), &IndexFqn::from(&index))
            .map_err(Self::map_catalog_error)?;

        overlay.drop_index(&IndexFqn::from(&index));

        Ok(ExecutionResult::Success)
    }
}

fn copy_format(path: &str, options: &[crate::ast::dml::CopyOption]) -> bulk::FileFormat {
    options
        .iter()
        .find(|option| option.name.eq_ignore_ascii_case("format"))
        .map(|option| {
            if option.value.eq_ignore_ascii_case("parquet") {
                bulk::FileFormat::Parquet
            } else {
                bulk::FileFormat::Csv
            }
        })
        .unwrap_or_else(|| {
            if path.ends_with(".parquet") {
                bulk::FileFormat::Parquet
            } else {
                bulk::FileFormat::Csv
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MemoryCatalog;
    use alopex_core::kv::memory::MemoryKV;

    fn create_executor() -> Executor<MemoryKV, MemoryCatalog> {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        Executor::new(store, catalog)
    }

    #[test]
    fn test_executor_creation() {
        let _executor = create_executor();
        // Executor should be created without panic
    }

    #[test]
    fn create_table_is_supported() {
        let mut executor = create_executor();

        use crate::catalog::{ColumnMetadata, TableMetadata};
        use crate::planner::ResolvedType;

        let table = TableMetadata::new(
            "test",
            vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        );

        let result = executor.execute(LogicalPlan::CreateTable {
            table,
            if_not_exists: false,
            with_options: vec![],
        });
        assert!(matches!(result, Ok(ExecutionResult::Success)));

        let catalog = executor.catalog.read().unwrap();
        assert!(catalog.table_exists("test"));
    }

    #[test]
    fn insert_is_supported() {
        use crate::Span;
        use crate::catalog::{ColumnMetadata, TableMetadata};
        use crate::planner::typed_expr::TypedExprKind;
        use crate::planner::types::ResolvedType;

        let mut executor = create_executor();

        let table = TableMetadata::new("t", vec![ColumnMetadata::new("id", ResolvedType::Integer)])
            .with_primary_key(vec!["id".into()]);

        executor
            .execute(LogicalPlan::CreateTable {
                table,
                if_not_exists: false,
                with_options: vec![],
            })
            .unwrap();

        let result = executor.execute(LogicalPlan::Insert {
            table: "t".into(),
            columns: vec!["id".into()],
            values: vec![vec![crate::planner::typed_expr::TypedExpr {
                kind: TypedExprKind::Literal(crate::ast::expr::Literal::Number("1".into())),
                resolved_type: ResolvedType::Integer,
                span: Span::default(),
            }]],
            conflict: None,
            returning: None,
        });
        assert!(matches!(result, Ok(ExecutionResult::RowsAffected(1))));
    }

    #[test]
    fn system_pragma_and_stats_function_use_the_store() {
        let mut executor = create_executor();
        let catalog = MemoryCatalog::new();

        let pragma = crate::Parser::parse_sql(&crate::AlopexDialect, "PRAGMA cache_size = 8")
            .unwrap()
            .pop()
            .unwrap();
        let plan = crate::Planner::new(&catalog).plan(&pragma).unwrap();
        assert!(matches!(
            executor.execute(plan),
            Ok(ExecutionResult::Success)
        ));

        let select = crate::Parser::parse_sql(&crate::AlopexDialect, "SELECT memory_stats()")
            .unwrap()
            .pop()
            .unwrap();
        let plan = crate::Planner::new(&catalog).plan(&select).unwrap();
        let result = executor.execute(plan).unwrap();
        let ExecutionResult::Query(result) = result else {
            panic!("expected query result");
        };
        assert_eq!(result.columns[0].name, "memory_stats");
        assert!(
            matches!(&result.rows[0][0], crate::SqlValue::Text(text) if text.contains("total_bytes"))
        );
    }
}
