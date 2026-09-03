//! In-memory catalog implementation for the Alopex SQL dialect.
//!
//! This module provides [`MemoryCatalog`], a HashMap-based implementation
//! of the [`Catalog`] trait for use in v0.1.1.

use std::collections::HashMap;

use super::{Catalog, IndexMetadata, TableMetadata, ViewMetadata};
use crate::ast::AlterTableAction;
use crate::planner::PlannerError;

/// In-memory catalog implementation using HashMaps.
///
/// This is the default catalog implementation for v0.1.2.
/// It stores all metadata in memory and does not persist across restarts.
///
/// # Thread Safety
///
/// `MemoryCatalog` is not thread-safe. For concurrent access, wrap it in
/// appropriate synchronization primitives (e.g., `Arc<RwLock<MemoryCatalog>>`).
///
/// # Example
///
/// ```
/// use alopex_sql::catalog::{Catalog, MemoryCatalog, TableMetadata, ColumnMetadata};
/// use alopex_sql::planner::types::ResolvedType;
///
/// let mut catalog = MemoryCatalog::new();
///
/// // Create a table
/// let table = TableMetadata::new("users", vec![
///     ColumnMetadata::new("id", ResolvedType::Integer),
///     ColumnMetadata::new("name", ResolvedType::Text),
/// ]);
/// catalog.create_table(table).unwrap();
///
/// // Access the table
/// assert!(catalog.table_exists("users"));
/// let table = catalog.get_table("users").unwrap();
/// assert_eq!(table.column_count(), 2);
/// ```
#[derive(Debug, Default)]
pub struct MemoryCatalog {
    /// Tables stored by name.
    tables: HashMap<String, TableMetadata>,
    /// Indexes stored by name.
    indexes: HashMap<String, IndexMetadata>,
    /// Views stored by name.
    views: HashMap<String, ViewMetadata>,
    /// Counter for generating unique table IDs (starts at 0, first ID is 1).
    table_id_counter: u32,
    /// Counter for generating unique index IDs (starts at 0, first ID is 1).
    index_id_counter: u32,
}

impl MemoryCatalog {
    /// Create a new empty in-memory catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of tables in the catalog.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Get the number of indexes in the catalog.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }

    /// Get all table names in the catalog.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Get all index names in the catalog.
    pub fn index_names(&self) -> Vec<&str> {
        self.indexes.keys().map(|s| s.as_str()).collect()
    }

    /// Clear all tables and indexes from the catalog.
    pub fn clear(&mut self) {
        self.tables.clear();
        self.indexes.clear();
        self.views.clear();
    }

    pub(crate) fn counters(&self) -> (u32, u32) {
        (self.table_id_counter, self.index_id_counter)
    }

    pub(crate) fn set_counters(&mut self, table_id_counter: u32, index_id_counter: u32) {
        self.table_id_counter = table_id_counter;
        self.index_id_counter = index_id_counter;
    }

    pub(crate) fn insert_table_unchecked(&mut self, table: TableMetadata) {
        self.tables.insert(table.name.clone(), table);
    }

    pub(crate) fn remove_table_unchecked(&mut self, name: &str) {
        self.tables.remove(name);
        self.indexes.retain(|_, idx| idx.table != name);
    }

    pub(crate) fn insert_index_unchecked(&mut self, index: IndexMetadata) {
        self.indexes.insert(index.name.clone(), index);
    }

    pub(crate) fn remove_index_unchecked(&mut self, name: &str) {
        self.indexes.remove(name);
    }
}

impl Catalog for MemoryCatalog {
    fn create_table(&mut self, table: TableMetadata) -> Result<(), PlannerError> {
        if self.tables.contains_key(&table.name) {
            return Err(PlannerError::table_already_exists(&table.name));
        }
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    fn get_table(&self, name: &str) -> Option<&TableMetadata> {
        self.tables.get(name)
    }

    fn drop_table(&mut self, name: &str) -> Result<(), PlannerError> {
        if self.tables.remove(name).is_none() {
            return Err(PlannerError::TableNotFound {
                name: name.to_string(),
                line: 0,
                column: 0,
            });
        }
        // Also drop all indexes for this table
        self.indexes.retain(|_, idx| idx.table != name);
        Ok(())
    }

    fn create_index(&mut self, index: IndexMetadata) -> Result<(), PlannerError> {
        // Check for duplicate index name
        if self.indexes.contains_key(&index.name) {
            return Err(PlannerError::index_already_exists(&index.name));
        }

        // Verify target table exists
        let table = self
            .tables
            .get(&index.table)
            .ok_or_else(|| PlannerError::TableNotFound {
                name: index.table.clone(),
                line: 0,
                column: 0,
            })?;

        // Verify all target columns exist in table
        for column in &index.columns {
            if table.get_column(column).is_none() {
                return Err(PlannerError::ColumnNotFound {
                    column: column.clone(),
                    table: index.table.clone(),
                    line: 0,
                    col: 0,
                });
            }
        }

        self.indexes.insert(index.name.clone(), index);
        Ok(())
    }

    fn get_index(&self, name: &str) -> Option<&IndexMetadata> {
        self.indexes.get(name)
    }

    fn get_indexes_for_table(&self, table: &str) -> Vec<&IndexMetadata> {
        self.indexes
            .values()
            .filter(|idx| idx.table == table)
            .collect()
    }

    fn drop_index(&mut self, name: &str) -> Result<(), PlannerError> {
        if self.indexes.remove(name).is_none() {
            return Err(PlannerError::index_not_found(name));
        }
        Ok(())
    }

    fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    fn index_exists(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }

    fn create_view(&mut self, view: ViewMetadata) -> Result<(), PlannerError> {
        if self.views.contains_key(&view.name) {
            return Err(PlannerError::unsupported_feature(
                format!("view '{}' already exists", view.name),
                "CREATE VIEW IF NOT EXISTS",
                crate::ast::Span::default(),
            ));
        }
        self.views.insert(view.name.clone(), view);
        Ok(())
    }

    fn get_view(&self, name: &str) -> Option<&ViewMetadata> {
        self.views.get(name)
    }

    fn drop_view(&mut self, name: &str) -> Result<(), PlannerError> {
        if self.views.remove(name).is_none() {
            return Err(PlannerError::unsupported_feature(
                format!("view '{name}' not found"),
                "DROP VIEW IF EXISTS",
                crate::ast::Span::default(),
            ));
        }
        Ok(())
    }

    fn alter_table(&mut self, name: &str, action: &AlterTableAction) -> Result<(), PlannerError> {
        match action {
            AlterTableAction::AddColumn { column, .. } => {
                let table = self.tables.get_mut(name).ok_or_else(|| {
                    PlannerError::table_not_found(name, crate::ast::Span::default())
                })?;
                if table
                    .columns
                    .iter()
                    .any(|existing| existing.name == column.name)
                {
                    return Err(PlannerError::unsupported_feature(
                        format!("column '{}' already exists", column.name),
                        "ALTER TABLE ADD COLUMN with a unique name",
                        crate::ast::Span::default(),
                    ));
                }
                table.columns.push(crate::catalog::ColumnMetadata::new(
                    column.name.clone(),
                    crate::planner::ResolvedType::from_ast(&column.data_type),
                ));
                Ok(())
            }
            AlterTableAction::DropColumn {
                name: column_name, ..
            } => {
                let table = self.tables.get_mut(name).ok_or_else(|| {
                    PlannerError::table_not_found(name, crate::ast::Span::default())
                })?;
                let Some(index) = table
                    .columns
                    .iter()
                    .position(|col| col.name == *column_name)
                else {
                    return Err(PlannerError::ColumnNotFound {
                        column: column_name.clone(),
                        table: name.to_string(),
                        line: 0,
                        col: 0,
                    });
                };
                table.columns.remove(index);
                self.indexes.retain(|_, idx| {
                    idx.table != name || !idx.columns.iter().any(|col| col == column_name)
                });
                Ok(())
            }
            AlterTableAction::RenameColumn { from, to, .. } => {
                let table = self.tables.get_mut(name).ok_or_else(|| {
                    PlannerError::table_not_found(name, crate::ast::Span::default())
                })?;
                let Some(index) = table.columns.iter().position(|col| col.name == *from) else {
                    return Err(PlannerError::ColumnNotFound {
                        column: from.clone(),
                        table: name.to_string(),
                        line: 0,
                        col: 0,
                    });
                };
                if table.columns.iter().any(|col| col.name == *to) {
                    return Err(PlannerError::unsupported_feature(
                        format!("column '{}' already exists", to),
                        "ALTER TABLE RENAME COLUMN to a fresh name",
                        crate::ast::Span::default(),
                    ));
                }
                table.columns[index].name = to.clone();
                for index_meta in self.indexes.values_mut().filter(|idx| idx.table == name) {
                    for column in &mut index_meta.columns {
                        if column == from {
                            *column = to.clone();
                        }
                    }
                }
                Ok(())
            }
            AlterTableAction::RenameTable { to, .. } => {
                if self.tables.contains_key(to) {
                    return Err(PlannerError::table_already_exists(to));
                }
                let mut table = self.tables.remove(name).ok_or_else(|| {
                    PlannerError::table_not_found(name, crate::ast::Span::default())
                })?;
                table.name = to.clone();
                self.tables.insert(to.clone(), table);
                for index in self.indexes.values_mut().filter(|idx| idx.table == name) {
                    index.table = to.clone();
                }
                Ok(())
            }
        }
    }

    fn next_table_id(&mut self) -> u32 {
        self.table_id_counter += 1;
        self.table_id_counter
    }

    fn next_index_id(&mut self) -> u32 {
        self.index_id_counter += 1;
        self.index_id_counter
    }

    fn list_tables(&self) -> Vec<TableMetadata> {
        self.tables.values().cloned().collect()
    }
}
