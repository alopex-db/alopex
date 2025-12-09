//! Catalog module for the Alopex SQL dialect.
//!
//! This module provides metadata management for tables and indexes.
//!
//! # Components
//!
//! - [`TableMetadata`]: Table schema information
//! - [`ColumnMetadata`]: Column schema information
//! - [`IndexMetadata`]: Index schema information
//! - [`Catalog`]: Trait for catalog implementations
//! - [`MemoryCatalog`]: In-memory catalog implementation
//!
//! # Example
//!
//! ```
//! use alopex_sql::catalog::{Catalog, MemoryCatalog, TableMetadata, ColumnMetadata, IndexMetadata};
//! use alopex_sql::planner::types::ResolvedType;
//! use alopex_sql::ast::ddl::IndexMethod;
//!
//! // Create an in-memory catalog
//! let mut catalog = MemoryCatalog::new();
//!
//! // Create a table
//! let columns = vec![
//!     ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
//!     ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
//! ];
//! let table = TableMetadata::new("users", columns);
//! catalog.create_table(table).unwrap();
//!
//! // Check table existence
//! assert!(catalog.table_exists("users"));
//! assert!(catalog.get_table("users").is_some());
//!
//! // Create an index (index_id is assigned by catalog in production)
//! let index = IndexMetadata::new(1, "idx_users_name", "users", vec!["name".into()])
//!     .with_method(IndexMethod::BTree);
//! catalog.create_index(index).unwrap();
//!
//! // Query indexes
//! assert!(catalog.index_exists("idx_users_name"));
//! assert_eq!(catalog.get_indexes_for_table("users").len(), 1);
//! ```

mod index;
mod memory;
mod table;

#[cfg(test)]
mod tests;

pub use index::IndexMetadata;
pub use memory::MemoryCatalog;
pub use table::{ColumnMetadata, TableMetadata};

use crate::planner::PlannerError;

/// Trait for catalog implementations.
///
/// A catalog manages metadata for tables and indexes. This trait abstracts
/// the storage mechanism, allowing both in-memory and persistent implementations.
///
/// # Design Notes
///
/// - Read methods take `&self` and return references or copies
/// - Write methods take `&mut self` and return `Result<(), PlannerError>`
/// - The `Planner` only uses read methods; `Executor` performs writes
/// - ID generation is done via `next_table_id()` / `next_index_id()` at execute time
///
/// # Error Handling
///
/// - `create_table`: Returns `TableAlreadyExists` if table exists
/// - `drop_table`: Returns `TableNotFound` if table doesn't exist
/// - `create_index`: Returns `IndexAlreadyExists` if index exists
/// - `drop_index`: Returns `IndexNotFound` if index doesn't exist
pub trait Catalog {
    /// Create a new table in the catalog.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::TableAlreadyExists` if a table with the same name exists.
    fn create_table(&mut self, table: TableMetadata) -> Result<(), PlannerError>;

    /// Get a table by name.
    ///
    /// Returns `None` if the table doesn't exist.
    fn get_table(&self, name: &str) -> Option<&TableMetadata>;

    /// Drop a table from the catalog.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::TableNotFound` if the table doesn't exist.
    fn drop_table(&mut self, name: &str) -> Result<(), PlannerError>;

    /// Create a new index in the catalog.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::IndexAlreadyExists` if an index with the same name exists.
    fn create_index(&mut self, index: IndexMetadata) -> Result<(), PlannerError>;

    /// Get an index by name.
    ///
    /// Returns `None` if the index doesn't exist.
    fn get_index(&self, name: &str) -> Option<&IndexMetadata>;

    /// Get all indexes for a table.
    ///
    /// Returns an empty vector if the table has no indexes.
    fn get_indexes_for_table(&self, table: &str) -> Vec<&IndexMetadata>;

    /// Drop an index from the catalog.
    ///
    /// # Errors
    ///
    /// Returns `PlannerError::IndexNotFound` if the index doesn't exist.
    fn drop_index(&mut self, name: &str) -> Result<(), PlannerError>;

    /// Check if a table exists.
    fn table_exists(&self, name: &str) -> bool;

    /// Check if an index exists.
    fn index_exists(&self, name: &str) -> bool;

    /// Generate the next unique table ID.
    ///
    /// Called by the Executor when creating a new table.
    /// IDs start from 1 and increment monotonically.
    fn next_table_id(&mut self) -> u32;

    /// Generate the next unique index ID.
    ///
    /// Called by the Executor when creating a new index.
    /// IDs start from 1 and increment monotonically.
    fn next_index_id(&mut self) -> u32;
}
