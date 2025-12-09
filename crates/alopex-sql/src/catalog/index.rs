//! Index metadata definitions for the Alopex SQL catalog.
//!
//! This module defines [`IndexMetadata`] which stores schema information
//! for indexes on tables.

use crate::ast::ddl::IndexMethod;

/// Metadata for an index in the catalog.
///
/// Contains the index ID, name, target table, columns, uniqueness flag,
/// index method, and optional parameters.
///
/// # Examples
///
/// ```
/// use alopex_sql::catalog::IndexMetadata;
/// use alopex_sql::ast::ddl::IndexMethod;
///
/// // Create a B-tree index on a single column
/// let btree_idx = IndexMetadata::new(1, "idx_users_name", "users", vec!["name".into()])
///     .with_column_indices(vec![1])
///     .with_method(IndexMethod::BTree);
///
/// assert_eq!(btree_idx.index_id, 1);
/// assert_eq!(btree_idx.name, "idx_users_name");
/// assert_eq!(btree_idx.table, "users");
/// assert_eq!(btree_idx.columns, vec!["name"]);
/// assert!(!btree_idx.unique);
///
/// // Create a unique composite index
/// let unique_idx = IndexMetadata::new(2, "idx_orders_composite", "orders", vec!["user_id".into(), "order_date".into()])
///     .with_column_indices(vec![0, 2])
///     .with_unique(true);
///
/// assert!(unique_idx.unique);
/// assert_eq!(unique_idx.columns.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// Unique index ID assigned by the catalog.
    pub index_id: u32,
    /// Index name.
    pub name: String,
    /// Target table name.
    pub table: String,
    /// Target column names (supports composite indexes).
    pub columns: Vec<String>,
    /// Column indices within the table (for IndexStorage).
    pub column_indices: Vec<usize>,
    /// Whether this is a UNIQUE index.
    pub unique: bool,
    /// Index method (BTree, Hnsw, etc.).
    pub method: Option<IndexMethod>,
    /// Index options (e.g., HNSW parameters: m, ef_construction).
    pub options: Vec<(String, String)>,
}

impl IndexMetadata {
    /// Create a new index metadata with the given ID, name, table, and columns.
    ///
    /// The index defaults to non-unique, with no method specified,
    /// empty column_indices, and no options.
    ///
    /// # Note
    ///
    /// `column_indices` should be set via `with_column_indices()` after creation,
    /// typically resolved by the Executor when the table schema is available.
    pub fn new(
        index_id: u32,
        name: impl Into<String>,
        table: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            index_id,
            name: name.into(),
            table: table.into(),
            columns,
            column_indices: Vec::new(),
            unique: false,
            method: None,
            options: Vec::new(),
        }
    }

    /// Set the column indices (positions within the table).
    pub fn with_column_indices(mut self, indices: Vec<usize>) -> Self {
        self.column_indices = indices;
        self
    }

    /// Set whether this is a UNIQUE index.
    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Set the index method.
    pub fn with_method(mut self, method: IndexMethod) -> Self {
        self.method = Some(method);
        self
    }

    /// Add an index option.
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.push((key.into(), value.into()));
        self
    }

    /// Set multiple options at once.
    pub fn with_options(mut self, options: Vec<(String, String)>) -> Self {
        self.options = options;
        self
    }

    /// Get an option value by key.
    ///
    /// Returns `None` if the option doesn't exist.
    pub fn get_option(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Check if this index covers the given column name.
    pub fn covers_column(&self, column: &str) -> bool {
        self.columns.iter().any(|c| c == column)
    }

    /// Check if this is a single-column index.
    pub fn is_single_column(&self) -> bool {
        self.columns.len() == 1
    }

    /// Get the first (or only) column name.
    ///
    /// Returns `None` if no columns are defined.
    pub fn first_column(&self) -> Option<&str> {
        self.columns.first().map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_metadata_new() {
        let index = IndexMetadata::new(1, "idx_users_id", "users", vec!["id".into()]);

        assert_eq!(index.index_id, 1);
        assert_eq!(index.name, "idx_users_id");
        assert_eq!(index.table, "users");
        assert_eq!(index.columns, vec!["id"]);
        assert!(index.column_indices.is_empty());
        assert!(!index.unique);
        assert!(index.method.is_none());
        assert!(index.options.is_empty());
    }

    #[test]
    fn test_index_metadata_with_column_indices() {
        let index = IndexMetadata::new(1, "idx", "table", vec!["col".into()])
            .with_column_indices(vec![2]);

        assert_eq!(index.column_indices, vec![2]);
    }

    #[test]
    fn test_index_metadata_with_unique() {
        let index = IndexMetadata::new(1, "idx", "table", vec!["col".into()])
            .with_unique(true);

        assert!(index.unique);
    }

    #[test]
    fn test_index_metadata_composite() {
        let index = IndexMetadata::new(
            5,
            "idx_composite",
            "orders",
            vec!["user_id".into(), "order_date".into()],
        )
        .with_column_indices(vec![0, 3])
        .with_unique(true);

        assert_eq!(index.index_id, 5);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.column_indices, vec![0, 3]);
        assert!(index.unique);
        assert!(!index.is_single_column());
    }

    #[test]
    fn test_index_metadata_with_method() {
        let index = IndexMetadata::new(1, "idx_users_name", "users", vec!["name".into()])
            .with_method(IndexMethod::BTree);

        assert_eq!(index.method, Some(IndexMethod::BTree));
    }

    #[test]
    fn test_index_metadata_hnsw_with_options() {
        let index = IndexMetadata::new(1, "idx_items_embedding", "items", vec!["embedding".into()])
            .with_method(IndexMethod::Hnsw)
            .with_option("m", "16")
            .with_option("ef_construction", "200");

        assert_eq!(index.method, Some(IndexMethod::Hnsw));
        assert_eq!(index.options.len(), 2);
        assert_eq!(index.get_option("m"), Some("16"));
        assert_eq!(index.get_option("ef_construction"), Some("200"));
        assert_eq!(index.get_option("nonexistent"), None);
    }

    #[test]
    fn test_index_metadata_with_options_bulk() {
        let options = vec![
            ("m".to_string(), "32".to_string()),
            ("ef_construction".to_string(), "400".to_string()),
        ];
        let index = IndexMetadata::new(1, "idx", "table", vec!["col".into()])
            .with_options(options);

        assert_eq!(index.options.len(), 2);
        assert_eq!(index.get_option("m"), Some("32"));
    }

    #[test]
    fn test_covers_column() {
        let index = IndexMetadata::new(
            1,
            "idx",
            "table",
            vec!["a".into(), "b".into()],
        );

        assert!(index.covers_column("a"));
        assert!(index.covers_column("b"));
        assert!(!index.covers_column("c"));
    }

    #[test]
    fn test_is_single_column() {
        let single = IndexMetadata::new(1, "idx", "table", vec!["a".into()]);
        let composite = IndexMetadata::new(2, "idx", "table", vec!["a".into(), "b".into()]);

        assert!(single.is_single_column());
        assert!(!composite.is_single_column());
    }

    #[test]
    fn test_first_column() {
        let index = IndexMetadata::new(1, "idx", "table", vec!["first".into(), "second".into()]);
        let empty = IndexMetadata::new(2, "idx", "table", vec![]);

        assert_eq!(index.first_column(), Some("first"));
        assert_eq!(empty.first_column(), None);
    }
}
