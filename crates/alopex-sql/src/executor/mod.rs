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

pub mod bulk;
mod ddl;
mod dml;
mod error;
pub mod evaluator;
mod hnsw_bridge;
pub mod query;
mod result;

pub use error::{ConstraintViolation, EvaluationError, ExecutorError, Result};
pub use result::{ColumnInfo, ExecutionResult, QueryResult, Row};

use std::sync::{Arc, RwLock};

use alopex_core::kv::KVStore;

use crate::catalog::Catalog;
use crate::planner::LogicalPlan;
use crate::storage::{SqlTransaction, TxnBridge};

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
        match plan {
            // DDL Operations
            LogicalPlan::CreateTable {
                table,
                if_not_exists,
                with_options,
            } => self.execute_create_table(table, with_options, if_not_exists),
            LogicalPlan::DropTable { name, if_exists } => self.execute_drop_table(&name, if_exists),
            LogicalPlan::CreateIndex {
                index,
                if_not_exists,
            } => self.execute_create_index(index, if_not_exists),
            LogicalPlan::DropIndex { name, if_exists } => self.execute_drop_index(&name, if_exists),

            // DML Operations
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => self.execute_insert(&table, columns, values),
            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => self.execute_update(&table, assignments, filter),
            LogicalPlan::Delete { table, filter } => self.execute_delete(&table, filter),

            // Query Operations
            LogicalPlan::Scan { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Sort { .. }
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
    ) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| dml::execute_insert(txn, &*catalog, table, columns, values))
    }

    fn execute_update(
        &mut self,
        table: &str,
        assignments: Vec<crate::planner::TypedAssignment>,
        filter: Option<crate::planner::TypedExpr>,
    ) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| dml::execute_update(txn, &*catalog, table, assignments, filter))
    }

    fn execute_delete(
        &mut self,
        table: &str,
        filter: Option<crate::planner::TypedExpr>,
    ) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| dml::execute_delete(txn, &*catalog, table, filter))
    }

    // ========================================================================
    // Query Operations (to be implemented in Phase 5)
    // ========================================================================

    fn execute_query(&mut self, plan: LogicalPlan) -> Result<ExecutionResult> {
        let catalog = self.catalog.read().expect("catalog lock poisoned");
        self.run_in_write_txn(|txn| query::execute_query(txn, &*catalog, plan))
    }
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
        });
        assert!(matches!(result, Ok(ExecutionResult::RowsAffected(1))));
    }
}
