//! DML executor for INSERT/UPDATE/DELETE operations.
//!
//! This module provides helpers to execute DML plans against the storage layer
//! while enforcing constraints and maintaining secondary indexes.

mod delete;
mod insert;
mod update;

pub use delete::execute_delete;
pub use insert::execute_insert;
pub use update::execute_update;
