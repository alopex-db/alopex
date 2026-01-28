use alopex_core::kv::KVTransaction;
use alopex_core::storage::format::bincode_config;
use bincode::Options;

use crate::catalog::persistent::{
    INDEXES_PREFIX, PersistedIndexMeta, PersistedTableMeta, TABLES_PREFIX,
};
use crate::catalog::{IndexMetadata, TableMetadata};
use crate::executor::ExecutorError;

pub fn persist_table<'txn, T: KVTransaction<'txn>>(
    txn: &mut T,
    table: &TableMetadata,
) -> Result<(), ExecutorError> {
    let persisted = PersistedTableMeta::from(table);
    let value =
        bincode_config()
            .serialize(&persisted)
            .map_err(|err| ExecutorError::InvalidOperation {
                operation: "CatalogPersistence".into(),
                reason: err.to_string(),
            })?;
    txn.put(
        table_key(&table.catalog_name, &table.namespace_name, &table.name),
        value,
    )?;
    Ok(())
}

pub fn persist_index<'txn, T: KVTransaction<'txn>>(
    txn: &mut T,
    index: &IndexMetadata,
) -> Result<(), ExecutorError> {
    let persisted = PersistedIndexMeta::from(index);
    let value =
        bincode_config()
            .serialize(&persisted)
            .map_err(|err| ExecutorError::InvalidOperation {
                operation: "CatalogPersistence".into(),
                reason: err.to_string(),
            })?;
    txn.put(
        index_key(
            &index.catalog_name,
            &index.namespace_name,
            &index.table,
            &index.name,
        ),
        value,
    )?;
    Ok(())
}

pub fn delete_table<'txn, T: KVTransaction<'txn>>(
    txn: &mut T,
    table: &TableMetadata,
) -> Result<(), ExecutorError> {
    txn.delete(table_key(
        &table.catalog_name,
        &table.namespace_name,
        &table.name,
    ))?;
    Ok(())
}

pub fn delete_index<'txn, T: KVTransaction<'txn>>(
    txn: &mut T,
    index: &IndexMetadata,
) -> Result<(), ExecutorError> {
    txn.delete(index_key(
        &index.catalog_name,
        &index.namespace_name,
        &index.table,
        &index.name,
    ))?;
    Ok(())
}

fn table_key(catalog_name: &str, namespace_name: &str, table_name: &str) -> Vec<u8> {
    let mut key = TABLES_PREFIX.to_vec();
    key.extend_from_slice(catalog_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(namespace_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(table_name.as_bytes());
    key
}

fn index_key(
    catalog_name: &str,
    namespace_name: &str,
    table_name: &str,
    index_name: &str,
) -> Vec<u8> {
    let mut key = INDEXES_PREFIX.to_vec();
    key.extend_from_slice(catalog_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(namespace_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(table_name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(index_name.as_bytes());
    key
}
