use alopex_core::kv::KVStore;

use crate::ast::Select;
use crate::catalog::{Catalog, ViewMetadata};
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::SqlTxn;

pub fn execute_create_view<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    _txn: &mut impl SqlTxn<'txn, S>,
    catalog: &mut C,
    name: String,
    query: Select,
    if_not_exists: bool,
) -> Result<ExecutionResult> {
    if catalog.view_exists(&name) {
        return if if_not_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::UnsupportedOperation(format!(
                "view '{name}' already exists"
            )))
        };
    }
    catalog.create_view(ViewMetadata { name, query })?;
    Ok(ExecutionResult::Success)
}

pub fn execute_drop_view<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    _txn: &mut impl SqlTxn<'txn, S>,
    catalog: &mut C,
    name: &str,
    if_exists: bool,
) -> Result<ExecutionResult> {
    if !catalog.view_exists(name) {
        return if if_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::UnsupportedOperation(format!(
                "view '{name}' not found"
            )))
        };
    }
    catalog.drop_view(name)?;
    Ok(ExecutionResult::Success)
}
