use alopex_core::kv::KVStore;
use std::collections::HashSet;

use crate::catalog::{
    Catalog, Compression, IndexMetadata, RowIdMode, StorageOptions, StorageType, TableMetadata,
};
use crate::executor::{ExecutionResult, ExecutorError, Result};
use crate::storage::{KeyEncoder, SqlTransaction};

use super::create_pk_index_name;

/// Execute CREATE TABLE.
pub fn execute_create_table<S: KVStore, C: Catalog>(
    txn: &mut SqlTransaction<'_, S>,
    catalog: &mut C,
    mut table: TableMetadata,
    with_options: Vec<(String, String)>,
    if_not_exists: bool,
) -> Result<ExecutionResult> {
    if catalog.table_exists(&table.name) {
        return if if_not_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::TableAlreadyExists(table.name))
        };
    }

    // WITH オプションを解析してストレージ設定へ反映
    table.storage_options = parse_storage_options(&with_options)?;

    // Resolve PK column indices before mutating the catalog to avoid partial writes.
    let pk_index = if let Some(pk_columns) = table.primary_key.clone() {
        let column_indices = resolve_column_indices(&table, &pk_columns)?;
        let index_id = catalog.next_index_id();
        let index_name = create_pk_index_name(&table.name);

        Some(
            IndexMetadata::new(index_id, index_name, table.name.clone(), pk_columns)
                .with_column_indices(column_indices)
                .with_unique(true),
        )
    } else {
        None
    };

    let table_id = catalog.next_table_id();
    table = table.with_table_id(table_id);

    // Ensure storage keyspace is clean for this table_id (defensive for future persistence).
    let table_prefix = KeyEncoder::table_prefix(table_id);
    txn.delete_prefix(&table_prefix)?;
    let seq_key = KeyEncoder::sequence_key(table_id);
    txn.delete_prefix(&seq_key)?;

    catalog.create_table(table.clone())?;

    if let Some(index) = pk_index
        && let Err(err) = catalog.create_index(index)
    {
        // Best-effort rollback to keep catalog consistent.
        let _ = catalog.drop_table(&table.name);
        return Err(err.into());
    }

    Ok(ExecutionResult::Success)
}

fn resolve_column_indices(table: &TableMetadata, columns: &[String]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|name| {
            table
                .get_column_index(name)
                .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
        })
        .collect()
}

/// WITH 句オプションを StorageOptions に変換する。
///
/// - storage: row/columnar (case-insensitive)
/// - compression: none/lz4/zstd (case-insensitive)
/// - row_group_size: 1000〜1_000_000
/// - rowid_mode: none/direct (case-insensitive)
/// - 未知キーは UnknownTableOption
/// - 重複キーは DuplicateOption
pub fn parse_storage_options(with_options: &[(String, String)]) -> Result<StorageOptions> {
    let mut options = StorageOptions::default();
    let mut seen = HashSet::new();

    for (key, value) in with_options {
        let key_lower = key.to_lowercase();
        if !seen.insert(key_lower.clone()) {
            return Err(ExecutorError::DuplicateOption(key.clone()));
        }

        match key_lower.as_str() {
            "storage" => {
                let normalized = value.trim().to_lowercase();
                options.storage_type = match normalized.as_str() {
                    "row" => StorageType::Row,
                    "columnar" => StorageType::Columnar,
                    _ => return Err(ExecutorError::InvalidStorageType(value.clone())),
                };
            }
            "compression" => {
                let normalized = value.trim().to_lowercase();
                options.compression = match normalized.as_str() {
                    "none" => Compression::None,
                    "lz4" => Compression::Lz4,
                    "zstd" => Compression::Zstd,
                    _ => return Err(ExecutorError::InvalidCompression(value.clone())),
                };
            }
            "row_group_size" => {
                let trimmed = value.trim();
                let size: u32 = trimmed
                    .parse()
                    .map_err(|_| ExecutorError::InvalidRowGroupSize(value.clone()))?;
                if size < 1_000 || size > 1_000_000 {
                    return Err(ExecutorError::InvalidRowGroupSize(value.clone()));
                }
                options.row_group_size = size;
            }
            "rowid_mode" => {
                let normalized = value.trim().to_lowercase();
                options.row_id_mode = match normalized.as_str() {
                    "none" => RowIdMode::None,
                    "direct" => RowIdMode::Direct,
                    _ => return Err(ExecutorError::InvalidRowIdMode(value.clone())),
                };
            }
            _ => return Err(ExecutorError::UnknownTableOption(key.clone())),
        }
    }

    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RowIdMode;
    use crate::catalog::{ColumnMetadata, MemoryCatalog};
    use crate::executor::ExecutorError;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    fn bridge() -> (TxnBridge<MemoryKV>, MemoryCatalog) {
        (
            TxnBridge::new(Arc::new(MemoryKV::new())),
            MemoryCatalog::new(),
        )
    }

    #[test]
    fn create_table_assigns_ids_and_pk_index() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        let mut txn = bridge.begin_write().unwrap();
        let result = execute_create_table(&mut txn, &mut catalog, table.clone(), vec![], false);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();
        let stored = catalog.get_table("users").expect("table stored");
        assert_eq!(stored.table_id, 1);

        let pk_index = catalog.get_index("__pk_users").expect("pk index stored");
        assert_eq!(pk_index.index_id, 1);
        assert!(pk_index.unique);
        assert_eq!(pk_index.column_indices, vec![0]);
    }

    #[test]
    fn create_table_if_not_exists_is_noop() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "items",
            vec![ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true)],
        )
        .with_primary_key(vec!["id".into()]);

        let mut txn = bridge.begin_write().unwrap();
        let first = execute_create_table(&mut txn, &mut catalog, table.clone(), vec![], false);
        assert!(first.is_ok());
        txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let second = execute_create_table(&mut txn, &mut catalog, table.clone(), vec![], true);
        assert!(matches!(second, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        assert_eq!(catalog.table_count(), 1);
        assert_eq!(catalog.index_count(), 1);
    }

    #[test]
    fn create_table_validates_pk_columns() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "bad",
            vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        )
        .with_primary_key(vec!["missing".into()]);

        let mut txn = bridge.begin_write().unwrap();
        let err = execute_create_table(&mut txn, &mut catalog, table, vec![], false).unwrap_err();
        txn.rollback().unwrap();

        assert!(matches!(
            err,
            ExecutorError::ColumnNotFound(name) if name == "missing"
        ));
        assert!(!catalog.table_exists("bad"));
    }

    #[test]
    fn parse_storage_options_defaults_and_overrides() {
        // empty -> defaults
        let opts = parse_storage_options(&[]).unwrap();
        assert_eq!(opts.storage_type, StorageType::Row);
        assert_eq!(opts.compression, Compression::Lz4);
        assert_eq!(opts.row_group_size, 100_000);
        assert_eq!(opts.row_id_mode, RowIdMode::Direct);

        // overrides
        let opts = parse_storage_options(&[
            ("storage".into(), " columnar ".into()),
            ("compression".into(), " zstd ".into()),
            ("row_group_size".into(), " 5000 ".into()),
            ("rowid_mode".into(), " direct ".into()),
        ])
        .unwrap();
        assert_eq!(opts.storage_type, StorageType::Columnar);
        assert_eq!(opts.compression, Compression::Zstd);
        assert_eq!(opts.row_group_size, 5_000);
        assert_eq!(opts.row_id_mode, RowIdMode::Direct);
    }

    #[test]
    fn parse_storage_options_validates() {
        // duplicate
        let err = parse_storage_options(&[
            ("storage".into(), "row".into()),
            ("storage".into(), "columnar".into()),
        ])
        .unwrap_err();
        assert!(matches!(err, ExecutorError::DuplicateOption(_)));

        // unknown key
        let err = parse_storage_options(&[("foo".into(), "bar".into())]).unwrap_err();
        assert!(matches!(err, ExecutorError::UnknownTableOption(_)));

        // invalid storage
        let err = parse_storage_options(&[("storage".into(), "heap".into())]).unwrap_err();
        assert!(matches!(err, ExecutorError::InvalidStorageType(_)));

        // invalid compression
        let err = parse_storage_options(&[("compression".into(), "gzip".into())]).unwrap_err();
        assert!(matches!(err, ExecutorError::InvalidCompression(_)));

        // invalid row_group_size (parse)
        let err = parse_storage_options(&[("row_group_size".into(), "abc".into())]).unwrap_err();
        assert!(matches!(err, ExecutorError::InvalidRowGroupSize(_)));

        // invalid row_group_size (range)
        let err = parse_storage_options(&[("row_group_size".into(), "10".into())]).unwrap_err();
        assert!(matches!(err, ExecutorError::InvalidRowGroupSize(_)));

        // invalid rowid_mode
        let err = parse_storage_options(&[("rowid_mode".into(), "invalid".into())]).unwrap_err();
        assert!(matches!(err, ExecutorError::InvalidRowIdMode(_)));
    }

    #[test]
    fn execute_create_table_applies_storage_options() {
        let (bridge, mut catalog) = bridge();
        let table = TableMetadata::new(
            "col_tbl",
            vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        );

        let mut txn = bridge.begin_write().unwrap();
        execute_create_table(
            &mut txn,
            &mut catalog,
            table,
            vec![
                ("storage".into(), "columnar".into()),
                ("compression".into(), "none".into()),
                ("row_group_size".into(), "2000".into()),
            ],
            false,
        )
        .unwrap();
        txn.commit().unwrap();

        let stored = catalog.get_table("col_tbl").unwrap();
        assert_eq!(stored.storage_options.storage_type, StorageType::Columnar);
        assert_eq!(stored.storage_options.compression, Compression::None);
        assert_eq!(stored.storage_options.row_group_size, 2_000);
    }
}
