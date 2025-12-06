//! In-memory catalog implementation for the Alopex SQL dialect.
//!
//! This module provides [`MemoryCatalog`], a HashMap-based implementation
//! of the [`Catalog`] trait for use in v0.1.1.

use std::collections::HashMap;

use super::{Catalog, IndexMetadata, TableMetadata};
use crate::planner::PlannerError;

/// In-memory catalog implementation using HashMaps.
///
/// This is the default catalog implementation for v0.1.1.
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

        // Verify target column exists in table
        if table.get_column(&index.column).is_none() {
            return Err(PlannerError::ColumnNotFound {
                column: index.column.clone(),
                table: index.table.clone(),
                line: 0,
                col: 0,
            });
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
}
