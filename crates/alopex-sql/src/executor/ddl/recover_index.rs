//! Rebuild derived index data from canonical rows inside the caller's transaction.

use alopex_core::kv::KVStore;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{IndexMetadata, TableMetadata};
use crate::executor::fts_bridge::FtsBridge;
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::{ExecutorError, Result};
use crate::storage::{KeyEncoder, SqlTxn};

use super::create_index::{
    build_fts_index_for_existing_rows, build_index_for_existing_rows, ensure_indexable_columns,
};
use super::persistence::persist_index;

pub(crate) fn rebuild_index<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table: &TableMetadata,
    index: &IndexMetadata,
) -> Result<()> {
    if index.column_indices.is_empty()
        || index
            .column_indices
            .iter()
            .any(|&column| column >= table.columns.len())
    {
        return Err(ExecutorError::InvalidOperation {
            operation: "index recovery".into(),
            reason: "index must reference existing columns".into(),
        });
    }
    if matches!(index.method, Some(IndexMethod::Fts)) {
        FtsBridge::validate(index, &table.columns[index.column_indices[0]].data_type)?;
    } else if !matches!(index.method, Some(IndexMethod::Hnsw)) {
        ensure_indexable_columns(table, &index.column_indices, "index recovery")?;
    }
    match index.method {
        Some(IndexMethod::Hnsw) => {
            HnswBridge::drop_index(txn, index, true)?;
            // Reuse CREATE's builder; recovery adds no full-row buffer. The existing
            // builder, graph and transaction write set still have O(n) memory cost.
            HnswBridge::create_index(txn, table, index)?;
        }
        Some(IndexMethod::Fts) => {
            txn.delete_prefix(&KeyEncoder::index_prefix(index.index_id))?;
            build_fts_index_for_existing_rows(txn, table, index)?;
        }
        _ => {
            txn.delete_prefix(&KeyEncoder::index_prefix(index.index_id))?;
            build_index_for_existing_rows(txn, table, index, index.column_indices.clone())?;
        }
    }
    persist_index(txn.inner_mut(), index)
}
