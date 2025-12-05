//! Index metadata definitions for the Alopex SQL catalog.
//!
//! This module defines [`IndexMetadata`] which stores schema information
//! for indexes on tables.

use crate::ast::ddl::IndexMethod;

/// Metadata for an index in the catalog.
///
/// Contains the index name, target table and column, index method,
/// and optional parameters.
///
/// # Examples
///
/// ```
/// use alopex_sql::catalog::IndexMetadata;
/// use alopex_sql::ast::ddl::IndexMethod;
///
/// // Create a B-tree index
/// let btree_idx = IndexMetadata::new("idx_users_name", "users", "name")
///     .with_method(IndexMethod::BTree);
///
/// assert_eq!(btree_idx.name, "idx_users_name");
/// assert_eq!(btree_idx.table, "users");
/// assert_eq!(btree_idx.column, "name");
///
/// // Create an HNSW index with options
/// let hnsw_idx = IndexMetadata::new("idx_items_embedding", "items", "embedding")
///     .with_method(IndexMethod::Hnsw)
///     .with_option("m", "16")
///     .with_option("ef_construction", "200");
///
/// assert_eq!(hnsw_idx.options.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// Index name.
    pub name: String,
    /// Target table name.
    pub table: String,
    /// Target column name.
    pub column: String,
    /// Index method (BTree, Hnsw, etc.).
    pub method: Option<IndexMethod>,
    /// Index options (e.g., HNSW parameters: m, ef_construction).
    pub options: Vec<(String, String)>,
}

impl IndexMetadata {
    /// Create a new index metadata with the given name, table, and column.
    ///
    /// The index method defaults to `None` (implementation default),
    /// and options are empty.
    pub fn new(
        name: impl Into<String>,
        table: impl Into<String>,
        column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            column: column.into(),
            method: None,
            options: Vec::new(),
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_metadata_new() {
        let index = IndexMetadata::new("idx_users_id", "users", "id");

        assert_eq!(index.name, "idx_users_id");
        assert_eq!(index.table, "users");
        assert_eq!(index.column, "id");
        assert!(index.method.is_none());
        assert!(index.options.is_empty());
    }

    #[test]
    fn test_index_metadata_with_method() {
        let index = IndexMetadata::new("idx_users_name", "users", "name")
            .with_method(IndexMethod::BTree);

        assert_eq!(index.method, Some(IndexMethod::BTree));
    }

    #[test]
    fn test_index_metadata_hnsw_with_options() {
        let index = IndexMetadata::new("idx_items_embedding", "items", "embedding")
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
        let index = IndexMetadata::new("idx", "table", "col").with_options(options);

        assert_eq!(index.options.len(), 2);
        assert_eq!(index.get_option("m"), Some("32"));
    }
}
