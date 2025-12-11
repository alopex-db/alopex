//! Table and column metadata definitions for the Alopex SQL catalog.
//!
//! This module defines [`TableMetadata`] and [`ColumnMetadata`] which store
//! schema information for tables and their columns.

use crate::ast::expr::Expr;
use crate::planner::types::ResolvedType;

/// Storage layout for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageType {
    /// Row-oriented storage (existing engine).
    Row,
    /// Columnar storage (v0.1.3+).
    Columnar,
}

/// Compression codec used for stored data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

/// Configurable storage options for a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageOptions {
    pub storage_type: StorageType,
    pub compression: Compression,
    pub row_group_size: u32,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Row,
            compression: Compression::Lz4,
            row_group_size: 100_000,
        }
    }
}

/// Metadata for a table in the catalog.
///
/// Contains the table ID, name, column definitions, and optional primary key constraint.
///
/// # Examples
///
/// ```
/// use alopex_sql::catalog::{TableMetadata, ColumnMetadata};
/// use alopex_sql::planner::types::ResolvedType;
///
/// let columns = vec![
///     ColumnMetadata::new("id", ResolvedType::Integer)
///         .with_primary_key(true)
///         .with_not_null(true),
///     ColumnMetadata::new("name", ResolvedType::Text)
///         .with_not_null(true),
/// ];
///
/// let table = TableMetadata::new("users", columns)
///     .with_primary_key(vec!["id".to_string()]);
///
/// assert_eq!(table.name, "users");
/// assert!(table.get_column("id").is_some());
/// assert_eq!(table.column_names(), vec!["id", "name"]);
/// ```
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Unique table ID assigned by the catalog.
    pub table_id: u32,
    /// Table name.
    pub name: String,
    /// Column definitions (order is preserved).
    pub columns: Vec<ColumnMetadata>,
    /// Primary key columns (supports composite keys).
    pub primary_key: Option<Vec<String>>,
    /// Storage configuration (row/columnar, compression, row group sizing).
    pub storage_options: StorageOptions,
}

impl TableMetadata {
    /// Create a new table metadata with the given name and columns.
    ///
    /// The table_id defaults to 0; use `with_table_id()` to set it,
    /// or it will be assigned by the Catalog when the table is created.
    pub fn new(name: impl Into<String>, columns: Vec<ColumnMetadata>) -> Self {
        Self {
            table_id: 0,
            name: name.into(),
            columns,
            primary_key: None,
            storage_options: StorageOptions::default(),
        }
    }

    /// Set the table ID.
    pub fn with_table_id(mut self, table_id: u32) -> Self {
        self.table_id = table_id;
        self
    }

    /// Set the primary key columns.
    pub fn with_primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = Some(columns);
        self
    }

    /// Get a column by name.
    ///
    /// Returns `None` if the column doesn't exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_sql::catalog::{TableMetadata, ColumnMetadata};
    /// use alopex_sql::planner::types::ResolvedType;
    ///
    /// let table = TableMetadata::new("users", vec![
    ///     ColumnMetadata::new("id", ResolvedType::Integer),
    ///     ColumnMetadata::new("name", ResolvedType::Text),
    /// ]);
    ///
    /// assert!(table.get_column("id").is_some());
    /// assert!(table.get_column("unknown").is_none());
    /// ```
    pub fn get_column(&self, name: &str) -> Option<&ColumnMetadata> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get the index of a column by name.
    ///
    /// Returns `None` if the column doesn't exist.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get a list of all column names in definition order.
    ///
    /// # Examples
    ///
    /// ```
    /// use alopex_sql::catalog::{TableMetadata, ColumnMetadata};
    /// use alopex_sql::planner::types::ResolvedType;
    ///
    /// let table = TableMetadata::new("users", vec![
    ///     ColumnMetadata::new("id", ResolvedType::Integer),
    ///     ColumnMetadata::new("name", ResolvedType::Text),
    ///     ColumnMetadata::new("age", ResolvedType::Integer),
    /// ]);
    ///
    /// assert_eq!(table.column_names(), vec!["id", "name", "age"]);
    /// ```
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get the number of columns in the table.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Metadata for a column in a table.
///
/// Contains the column name, data type, and constraint information.
///
/// # Examples
///
/// ```
/// use alopex_sql::catalog::ColumnMetadata;
/// use alopex_sql::planner::types::ResolvedType;
///
/// let column = ColumnMetadata::new("id", ResolvedType::Integer)
///     .with_not_null(true)
///     .with_primary_key(true);
///
/// assert_eq!(column.name, "id");
/// assert_eq!(column.data_type, ResolvedType::Integer);
/// assert!(column.not_null);
/// assert!(column.primary_key);
/// ```
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name.
    pub name: String,
    /// Column data type (normalized).
    pub data_type: ResolvedType,
    /// NOT NULL constraint.
    pub not_null: bool,
    /// PRIMARY KEY constraint.
    pub primary_key: bool,
    /// UNIQUE constraint.
    pub unique: bool,
    /// DEFAULT value expression.
    pub default: Option<Expr>,
}

impl ColumnMetadata {
    /// Create a new column metadata with the given name and data type.
    ///
    /// All constraints default to `false`, and `default` is `None`.
    pub fn new(name: impl Into<String>, data_type: ResolvedType) -> Self {
        Self {
            name: name.into(),
            data_type,
            not_null: false,
            primary_key: false,
            unique: false,
            default: None,
        }
    }

    /// Set the NOT NULL constraint.
    pub fn with_not_null(mut self, not_null: bool) -> Self {
        self.not_null = not_null;
        self
    }

    /// Set the PRIMARY KEY constraint.
    pub fn with_primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
        self
    }

    /// Set the UNIQUE constraint.
    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Set the DEFAULT value.
    pub fn with_default(mut self, default: Expr) -> Self {
        self.default = Some(default);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_metadata_new() {
        let table = TableMetadata::new("users", vec![]);
        assert_eq!(table.table_id, 0);
        assert_eq!(table.name, "users");
        assert!(table.columns.is_empty());
        assert!(table.primary_key.is_none());
    }

    #[test]
    fn test_table_metadata_with_table_id() {
        let table = TableMetadata::new("users", vec![]).with_table_id(42);
        assert_eq!(table.table_id, 42);
        assert_eq!(table.name, "users");
    }

    #[test]
    fn test_table_metadata_with_columns() {
        let columns = vec![
            ColumnMetadata::new("id", ResolvedType::Integer),
            ColumnMetadata::new("name", ResolvedType::Text),
        ];
        let table = TableMetadata::new("users", columns);

        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[1].name, "name");
    }

    #[test]
    fn test_table_metadata_with_primary_key() {
        let table = TableMetadata::new("users", vec![])
            .with_primary_key(vec!["id".to_string(), "tenant_id".to_string()]);

        assert_eq!(
            table.primary_key,
            Some(vec!["id".to_string(), "tenant_id".to_string()])
        );
    }

    #[test]
    fn test_get_column() {
        let columns = vec![
            ColumnMetadata::new("id", ResolvedType::Integer),
            ColumnMetadata::new("name", ResolvedType::Text),
        ];
        let table = TableMetadata::new("users", columns);

        let id_col = table.get_column("id");
        assert!(id_col.is_some());
        assert_eq!(id_col.unwrap().name, "id");
        assert_eq!(id_col.unwrap().data_type, ResolvedType::Integer);

        let name_col = table.get_column("name");
        assert!(name_col.is_some());
        assert_eq!(name_col.unwrap().data_type, ResolvedType::Text);

        assert!(table.get_column("unknown").is_none());
    }

    #[test]
    fn test_get_column_index() {
        let columns = vec![
            ColumnMetadata::new("id", ResolvedType::Integer),
            ColumnMetadata::new("name", ResolvedType::Text),
            ColumnMetadata::new("age", ResolvedType::Integer),
        ];
        let table = TableMetadata::new("users", columns);

        assert_eq!(table.get_column_index("id"), Some(0));
        assert_eq!(table.get_column_index("name"), Some(1));
        assert_eq!(table.get_column_index("age"), Some(2));
        assert_eq!(table.get_column_index("unknown"), None);
    }

    #[test]
    fn test_column_names() {
        let columns = vec![
            ColumnMetadata::new("id", ResolvedType::Integer),
            ColumnMetadata::new("name", ResolvedType::Text),
            ColumnMetadata::new("age", ResolvedType::Integer),
        ];
        let table = TableMetadata::new("users", columns);

        assert_eq!(table.column_names(), vec!["id", "name", "age"]);
    }

    #[test]
    fn test_column_count() {
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        );
        assert_eq!(table.column_count(), 2);
    }

    #[test]
    fn test_column_metadata_new() {
        let column = ColumnMetadata::new("id", ResolvedType::Integer);

        assert_eq!(column.name, "id");
        assert_eq!(column.data_type, ResolvedType::Integer);
        assert!(!column.not_null);
        assert!(!column.primary_key);
        assert!(!column.unique);
        assert!(column.default.is_none());
    }

    #[test]
    fn test_column_metadata_constraints() {
        let column = ColumnMetadata::new("id", ResolvedType::Integer)
            .with_not_null(true)
            .with_primary_key(true)
            .with_unique(true);

        assert!(column.not_null);
        assert!(column.primary_key);
        assert!(column.unique);
    }

    #[test]
    fn test_storage_options_default() {
        let options = StorageOptions::default();
        assert_eq!(options.storage_type, StorageType::Row);
        assert_eq!(options.compression, Compression::Lz4);
        assert_eq!(options.row_group_size, 100_000);
    }

    #[test]
    fn test_table_metadata_sets_default_storage_options() {
        let table = TableMetadata::new("users", vec![]);
        assert_eq!(table.storage_options, StorageOptions::default());
    }
}
