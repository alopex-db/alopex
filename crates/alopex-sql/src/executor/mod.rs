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
mod config;
mod ddl;
mod dml;
mod error;
pub mod evaluator;
mod hnsw_bridge;
pub mod query;
mod result;

pub use config::ExecutionConfig;
pub use error::{ConstraintViolation, EvaluationError, ExecutorError, Result};
pub use query::{RowIterator, ScanIterator, build_streaming_pipeline};
pub use result::{ColumnInfo, ExecutionResult, QueryResult, QueryRowIterator, Row};

use std::sync::{Arc, RwLock};

use alopex_core::kv::KVStore;
use alopex_core::types::TxnMode;

use crate::catalog::Catalog;
use crate::catalog::persistent::{IndexFqn, TableFqn};
use crate::catalog::{CatalogError, CatalogOverlay, PersistentCatalog, TxnCatalogView};
use crate::planner::LogicalPlan;
use crate::storage::{BorrowedSqlTransaction, KeyEncoder, SqlTransaction, SqlTxn as _, TxnBridge};

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
            | LogicalPlan::Aggregate { .. }
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

impl<S: KVStore> Executor<S, PersistentCatalog<S>> {
    pub fn execute_in_txn<'a, 'b, 'c>(
        &mut self,
        plan: LogicalPlan,
        txn: &mut BorrowedSqlTransaction<'a, 'b, 'c, S>,
    ) -> Result<ExecutionResult> {
        if txn.mode() == TxnMode::ReadOnly
            && !matches!(
                plan,
                LogicalPlan::Scan { .. }
                    | LogicalPlan::Filter { .. }
                    | LogicalPlan::Aggregate { .. }
                    | LogicalPlan::Sort { .. }
                    | LogicalPlan::Limit { .. }
            )
        {
            return Err(ExecutorError::ReadOnlyTransaction {
                operation: plan.operation_name().to_string(),
            });
        }

        let mut catalog = self.catalog.write().expect("catalog lock poisoned");
        let (mut sql_txn, overlay) = txn.split_parts();

        let result = match plan {
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
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                dml::execute_insert(&mut sql_txn, &view, &table, columns, values)
            }
            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                dml::execute_update(&mut sql_txn, &view, &table, assignments, filter)
            }
            LogicalPlan::Delete { table, filter } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                dml::execute_delete(&mut sql_txn, &view, &table, filter)
            }
            LogicalPlan::Scan { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. } => {
                let view = TxnCatalogView::new(&*catalog, &*overlay);
                query::execute_query(&mut sql_txn, &view, plan)
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

        if matches!(index.method, Some(crate::ast::ddl::IndexMethod::Hnsw)) {
            crate::executor::hnsw_bridge::HnswBridge::create_index(txn, &table, &index)?;
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
