//! DML executor for INSERT/UPDATE/DELETE operations.
//!
//! This module provides helpers to execute DML plans against the storage layer
//! while enforcing constraints and maintaining secondary indexes.

mod constraints;
mod delete;
mod insert;
mod update;

#[allow(unused_imports)]
pub use delete::{execute_delete, execute_delete_with_returning};
pub(crate) use insert::{evaluate_default, normalize_assignment_value};
#[allow(unused_imports)]
pub use insert::{
    execute_insert, execute_insert_rows, execute_insert_rows_with_plan, execute_insert_with_plan,
};
#[allow(unused_imports)]
pub use update::{execute_update, execute_update_with_returning};
