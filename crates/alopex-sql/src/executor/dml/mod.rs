//! DML executor for INSERT/UPDATE/DELETE operations.
//!
//! This module provides helpers to execute DML plans against the storage layer
//! while enforcing constraints and maintaining secondary indexes.

mod delete;
mod insert;
mod update;

pub use delete::execute_delete;
pub(crate) use insert::{evaluate_default, normalize_assignment_value};
pub use insert::{execute_insert, execute_insert_rows};
pub use update::execute_update;
