//! DDL executor modules.
//!
//! This module groups CREATE/DROP TABLE/INDEX executors and shared helpers
//! such as implicit primary key index naming.

pub mod create_index;
pub mod create_table;
pub mod drop_index;
pub mod drop_table;

/// Prefix reserved for implicit primary key indexes.
pub const PK_INDEX_PREFIX: &str = "__pk_";

/// Build the implicit primary key index name for a table.
pub fn create_pk_index_name(table_name: &str) -> String {
    format!("{PK_INDEX_PREFIX}{table_name}")
}

/// Returns true if the index name is reserved for implicit PK indexes.
pub fn is_implicit_pk_index(index_name: &str) -> bool {
    index_name.starts_with(PK_INDEX_PREFIX)
}
