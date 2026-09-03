use alopex_core::kv::KVStore;

use crate::ast::AlterTableAction;
use crate::catalog::Catalog;
use crate::executor::{ExecutionResult, Result};
use crate::storage::SqlTxn;

pub fn execute_alter_table<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    _txn: &mut impl SqlTxn<'txn, S>,
    catalog: &mut C,
    table_name: &str,
    action: AlterTableAction,
) -> Result<ExecutionResult> {
    catalog.alter_table(table_name, &action)?;
    Ok(ExecutionResult::Success)
}
