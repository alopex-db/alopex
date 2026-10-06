use alopex_core::kv::KVStore;

use crate::ast::ddl::IndexMethod;
use crate::catalog::{Catalog, IndexMetadata, StorageType, TableMetadata};
use crate::executor::Row;
use crate::executor::columnar_constraints;
use crate::executor::fts_bridge::FtsBridge;
use crate::executor::hnsw_bridge::HnswBridge;
use crate::executor::query::columnar_scan::{ColumnarScan, create_columnar_scan_iterator};
use crate::executor::query::iterator::RowIterator;
use crate::executor::{ConstraintViolation, ExecutionResult, ExecutorError, Result};
use crate::storage::{SqlTxn, SqlValue, StorageError};

use super::is_implicit_pk_index;
use super::persistence::persist_index;

/// Execute CREATE INDEX.
pub fn execute_create_index<'txn, S: KVStore + 'txn, C: Catalog + ?Sized>(
    txn: &mut impl SqlTxn<'txn, S>,
    catalog: &mut C,
    mut index: IndexMetadata,
    if_not_exists: bool,
) -> Result<ExecutionResult> {
    if is_implicit_pk_index(&index.name) {
        return Err(ExecutorError::InvalidIndexName {
            name: index.name.clone(),
            reason: "Index names starting with '__pk_' are reserved for PRIMARY KEY".into(),
        });
    }

    if catalog.index_exists(&index.name) {
        return if if_not_exists {
            Ok(ExecutionResult::Success)
        } else {
            Err(ExecutorError::IndexAlreadyExists(index.name.clone()))
        };
    }

    let table = catalog
        .get_table(&index.table)
        .ok_or_else(|| ExecutorError::TableNotFound(index.table.clone()))?
        .clone();

    let column_indices = resolve_column_indices(&table, &index)?;
    ensure_indexable_columns(&table, &column_indices, "CREATE INDEX")?;
    let index_id = catalog.next_index_id();
    index.index_id = index_id;
    index.column_indices = column_indices.clone();
    if matches!(index.method, Some(IndexMethod::Fts)) {
        FtsBridge::prepare(&mut index)?;
    }

    if matches!(index.method, Some(IndexMethod::Hnsw)) {
        HnswBridge::create_index(txn, &table, &index)?;
    } else if matches!(index.method, Some(IndexMethod::Fts)) {
        FtsBridge::validate(&index, &table.columns[column_indices[0]].data_type)?;
        build_fts_index_for_existing_rows(txn, &table, &index)?;
    } else {
        // Populate index entries for existing rows before publishing metadata.
        build_index_for_existing_rows(txn, &table, &index, column_indices)?;
    }

    catalog.create_index(index.clone())?;
    persist_index(txn.inner_mut(), &index)?;

    Ok(ExecutionResult::Success)
}

pub(crate) fn build_fts_index_for_existing_rows<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table: &TableMetadata,
    index: &IndexMetadata,
) -> Result<()> {
    let mut start_row_id = 0;
    loop {
        let rows = txn.with_table(table, |storage| {
            storage
                .range_scan(start_row_id, u64::MAX)?
                .take(2048)
                .collect::<std::result::Result<Vec<_>, _>>()
        })?;
        if rows.is_empty() {
            return Ok(());
        }
        for (row_id, row) in rows {
            FtsBridge::on_insert(txn, index, row_id, &row)?;
            start_row_id = row_id.saturating_add(1);
        }
    }
}

pub(crate) fn ensure_indexable_columns(
    table: &TableMetadata,
    column_indices: &[usize],
    operation: &str,
) -> Result<()> {
    if let Some(data_type) = column_indices.iter().find_map(|column| {
        let data_type = &table.columns[*column].data_type;
        matches!(
            data_type,
            crate::planner::ResolvedType::Json
                | crate::planner::ResolvedType::Array(_)
                | crate::planner::ResolvedType::Map { .. }
                | crate::planner::ResolvedType::Struct(_)
        )
        .then_some(data_type)
    }) {
        return Err(ExecutorError::InvalidOperation {
            operation: operation.into(),
            reason: if matches!(data_type, crate::planner::ResolvedType::Json) {
                "Alopex does not define a JSON sort order"
            } else {
                "Alopex does not define a nested-value sort order"
            }
            .into(),
        });
    }
    Ok(())
}

fn resolve_column_indices(
    table: &crate::catalog::TableMetadata,
    index: &IndexMetadata,
) -> Result<Vec<usize>> {
    index
        .columns
        .iter()
        .map(|name| {
            table
                .get_column_index(name)
                .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))
        })
        .collect()
}

fn should_skip_unique_index_for_null(index: &IndexMetadata, row: &[SqlValue]) -> bool {
    index.unique
        && index
            .column_indices
            .iter()
            .any(|&idx| row.get(idx).is_none_or(SqlValue::is_null))
}

pub(crate) fn build_index_for_existing_rows<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table: &TableMetadata,
    index: &IndexMetadata,
    column_indices: Vec<usize>,
) -> Result<()> {
    const CHUNK_SIZE: u64 = 2048;
    if table.storage_options.storage_type == StorageType::Columnar
        && index.unique
        && matches!(index.method, None | Some(IndexMethod::BTree))
    {
        columnar_constraints::validate_index_columns(table, index)?;
        // Both direct DDL and the PersistentCatalog overlay use this owner.
        // Validate types before NULL skipping, including an empty columnar table.
        ensure_indexable_columns(table, &index.column_indices, "CREATE INDEX")?;
        for &position in &index.column_indices {
            let unsupported = match table.columns[position].data_type {
                crate::planner::ResolvedType::Vector { .. } => Some("Vector"),
                crate::planner::ResolvedType::Interval => Some("Interval"),
                crate::planner::ResolvedType::Decimal { .. } => Some("Decimal"),
                _ => None,
            };
            if let Some(actual) = unsupported {
                return Err(StorageError::TypeMismatch {
                    expected: "indexable scalar type".into(),
                    actual: actual.into(),
                }
                .into());
            }
        }
        let policy = columnar_constraints::memory_policy(txn.memory_policy());
        let mut scan = create_columnar_scan_iterator(
            txn,
            table,
            &ColumnarScan::new(table.table_id, Vec::new(), None, None),
        )?;
        let next = || -> Result<Option<Row>> {
            while let Some(row) = scan.next_row() {
                let row = row?;
                if row.values.len() != table.column_count() {
                    return Err(ExecutorError::Columnar(format!(
                        "row has {} columns, expected {}",
                        row.values.len(),
                        table.column_count()
                    )));
                }
                if let Some(key) = columnar_constraints::key(index, &row.values)? {
                    return Ok(Some(Row::new(0, vec![SqlValue::Blob(key)])));
                }
            }
            Ok(None)
        };
        if columnar_constraints::duplicate_constraint(next, policy)?.is_some() {
            return Err(ExecutorError::ConstraintViolation(
                ConstraintViolation::Unique {
                    index_name: index.name.clone(),
                    columns: index.columns.clone(),
                    value: None,
                },
            ));
        }
        // Columnar reads/COPY use segment scans and UNIQUE metadata, not a
        // maintained physical B-tree. Never publish metadata before validation.
        return Ok(());
    }
    let mut start_row_id = 0u64;

    loop {
        if start_row_id == u64::MAX {
            break;
        }
        let rows = fetch_rows_chunk(txn, table, start_row_id + 1, CHUNK_SIZE)?;
        if rows.is_empty() {
            break;
        }

        let mut duplicate_value = None;
        let insert_result = txn.with_index(
            index.index_id,
            index.unique,
            column_indices.clone(),
            |storage| {
                for (row_id, row) in rows {
                    if should_skip_unique_index_for_null(index, &row) {
                        start_row_id = row_id;
                        continue;
                    }
                    if let Err(error) = storage.insert(&row, row_id) {
                        duplicate_value = Some(format!(
                            "{:?}",
                            column_indices
                                .iter()
                                .map(|&column| &row[column])
                                .collect::<Vec<_>>()
                        ));
                        return Err(error);
                    }
                    start_row_id = row_id;
                }
                Ok(())
            },
        );

        match insert_result {
            Ok(()) => {}
            Err(StorageError::UniqueViolation { .. }) => {
                return Err(ExecutorError::ConstraintViolation(
                    ConstraintViolation::Unique {
                        index_name: index.name.clone(),
                        columns: index.columns.clone(),
                        value: duplicate_value,
                    },
                ));
            }
            Err(other) => return Err(other.into()),
        }
    }

    Ok(())
}

fn fetch_rows_chunk<'txn, S: KVStore + 'txn>(
    txn: &mut impl SqlTxn<'txn, S>,
    table: &TableMetadata,
    start_row_id: u64,
    chunk_size: u64,
) -> Result<Vec<(u64, Vec<SqlValue>)>> {
    Ok(txn.with_table(table, |table_storage| {
        // Bound the number of actual rows, not the row-id interval: deletions
        // can leave arbitrarily large gaps before the next live row.
        let scan = table_storage.range_scan(start_row_id, u64::MAX)?;
        let mut rows = Vec::new();
        for entry in scan.take(chunk_size as usize) {
            let (row_id, row) = entry?;
            rows.push((row_id, row));
        }
        Ok(rows)
    })?)
}

#[cfg(test)]
mod tests {
    // The public borrowed transaction has no MemoryPolicy setter. Resource
    // tests therefore exercise the existing private DDL owner directly.
    mod columnar_resources {
        use super::*;
        use crate::dialect::AlopexDialect;
        use crate::executor::Executor;
        use crate::parser::Parser;
        use crate::planner::Planner;
        use alopex_core::kv::{KVStore, KVTransaction};
        use alopex_core::types::TxnMode;
        use std::io::Write;
        use std::sync::RwLock;

        fn constraint_execute(
            executor: &mut Executor<MemoryKV, MemoryCatalog>,
            catalog: &Arc<RwLock<MemoryCatalog>>,
            sql: &str,
        ) -> Result<ExecutionResult> {
            let statement = Parser::parse_sql(&AlopexDialect, sql)
                .unwrap()
                .pop()
                .unwrap();
            let plan = Planner::new(&*catalog.read().unwrap())
                .plan(&statement)
                .unwrap();
            executor.execute(plan)
        }

        fn constraint_fixture(
            columns: &str,
        ) -> (
            Arc<MemoryKV>,
            Arc<RwLock<MemoryCatalog>>,
            Executor<MemoryKV, MemoryCatalog>,
        ) {
            let store = Arc::new(MemoryKV::new());
            let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
            let mut executor = Executor::new(store.clone(), catalog.clone());
            constraint_execute(
                &mut executor,
                &catalog,
                &format!("CREATE TABLE users ({columns}) WITH (storage='columnar')"),
            )
            .unwrap();
            assert_eq!(
                catalog
                    .read()
                    .unwrap()
                    .get_table("users")
                    .unwrap()
                    .storage_options
                    .storage_type,
                crate::catalog::StorageType::Columnar
            );
            (store, catalog, executor)
        }

        fn constraint_csv(contents: &str) -> tempfile::NamedTempFile {
            let mut file = tempfile::NamedTempFile::new().unwrap();
            file.write_all(contents.as_bytes()).unwrap();
            file.flush().unwrap();
            file
        }

        fn constraint_copy_sql(file: &tempfile::NamedTempFile) -> String {
            format!(
                "COPY users FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
                file.path().to_str().unwrap().replace('\'', "''")
            )
        }

        fn constraint_snapshot(store: &MemoryKV) -> Vec<(Vec<u8>, Vec<u8>)> {
            let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
            let entries = txn.scan_prefix(&[]).unwrap().collect();
            txn.rollback_self().unwrap();
            entries
        }

        struct CopyPolicyTxn<'txn> {
            inner: crate::storage::SqlTransaction<'txn, MemoryKV>,
            policy: crate::executor::MemoryPolicy,
        }

        impl<'txn> SqlTxn<'txn, MemoryKV> for CopyPolicyTxn<'txn> {
            fn memory_policy(&self) -> Option<&crate::executor::MemoryPolicy> {
                Some(&self.policy)
            }
            fn mode(&self) -> TxnMode {
                SqlTxn::mode(&self.inner)
            }
            fn ensure_write_txn(&self) -> alopex_core::Result<()> {
                SqlTxn::ensure_write_txn(&self.inner)
            }
            fn inner_mut(&mut self) -> &mut alopex_core::kv::memory::MemoryTransaction<'txn> {
                SqlTxn::inner_mut(&mut self.inner)
            }
            fn hnsw_entry(
                &mut self,
                name: &str,
            ) -> alopex_core::Result<&alopex_core::vector::hnsw::HnswIndex> {
                SqlTxn::hnsw_entry(&mut self.inner, name)
            }
            fn hnsw_entry_mut(
                &mut self,
                name: &str,
            ) -> alopex_core::Result<&mut crate::storage::bridge::HnswTxnEntry> {
                SqlTxn::hnsw_entry_mut(&mut self.inner, name)
            }
            fn flush_hnsw(&mut self) -> crate::storage::error::Result<()> {
                SqlTxn::flush_hnsw(&mut self.inner)
            }
            fn abandon_hnsw(&mut self) -> crate::storage::error::Result<()> {
                SqlTxn::abandon_hnsw(&mut self.inner)
            }
            fn delete_prefix(&mut self, prefix: &[u8]) -> crate::storage::error::Result<()> {
                SqlTxn::delete_prefix(&mut self.inner, prefix)
            }
        }

        struct CopySpillObservation {
            directory: std::path::PathBuf,
            files: std::sync::atomic::AtomicU64,
            bytes: std::sync::atomic::AtomicU64,
            visible_files: std::sync::atomic::AtomicU64,
        }

        impl crate::executor::memory::SpillMetricsSink for CopySpillObservation {
            fn record_spill(&self, bytes: u64, files: u64) {
                use std::sync::atomic::Ordering::Relaxed;
                self.files.fetch_add(files, Relaxed);
                self.bytes.fetch_add(bytes, Relaxed);
                let visible = std::fs::read_dir(&self.directory).unwrap().count() as u64;
                self.visible_files.fetch_max(visible, Relaxed);
            }
        }

        fn assert_postload_unique_resource_case(case: &str) {
            use crate::executor::{MemoryPolicy, SpillPolicy};
            use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
            let (store, catalog, mut executor) = constraint_fixture("id INT");
            let csv = format!(
                "id\n{}{}",
                (1..=8).map(|id| format!("{id}\n")).collect::<String>(),
                if case == "duplicate" { "8\n" } else { "" }
            );
            let file = constraint_csv(&csv);
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap();
            let directory = tempfile::tempdir().unwrap();
            let observation = Arc::new(CopySpillObservation {
                directory: directory.path().to_path_buf(),
                files: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                visible_files: AtomicU64::new(0),
            });
            let policy = MemoryPolicy::new(
                Some(if case == "failfast" { 1 } else { 64 }),
                if case == "failfast" {
                    SpillPolicy::FailFast
                } else {
                    SpillPolicy::SpillToDisk {
                        directory: directory.path().into(),
                    }
                },
            )
            .with_metrics(observation.clone());
            let bridge = TxnBridge::new(store.clone());
            let mut txn = CopyPolicyTxn {
                inner: bridge.begin_write().unwrap(),
                policy,
            };
            let before = txn
                .inner_mut()
                .scan_prefix(&[])
                .unwrap()
                .collect::<Vec<_>>();
            let result = execute_create_index(
                &mut txn,
                &mut *catalog.write().unwrap(),
                crate::catalog::IndexMetadata::new(0, "uq_users", "users", vec!["id".into()])
                    .with_unique(true),
                false,
            );
            let after = txn
                .inner_mut()
                .scan_prefix(&[])
                .unwrap()
                .collect::<Vec<_>>();
            let commit = txn.inner.commit();
            let files = observation.files.load(Relaxed);
            let bytes = observation.bytes.load(Relaxed);
            let visible = observation.visible_files.load(Relaxed);
            let remaining = std::fs::read_dir(directory.path()).unwrap().count();
            eprintln!(
                "postload {case}: {result:?}; unchanged={}; files={files}; bytes={bytes}; visible={visible}; remaining={remaining}",
                before == after
            );
            match case {
                "success" => assert_eq!(result.unwrap(), ExecutionResult::Success),
                "duplicate" => assert!(
                    matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, .. })) if index_name == "uq_users"),
                    "{result:?}"
                ),
                "failfast" => assert!(
                    matches!(&result, Err(ExecutorError::ResourceExhausted { .. })),
                    "{result:?}"
                ),
                _ => unreachable!(),
            }
            commit.unwrap();
            assert_eq!(remaining, 0);
            if case == "failfast" {
                assert_eq!(files, 0);
            } else {
                assert!(files > 0 && bytes > 0 && visible > 0);
            }
            if case != "success" {
                assert_eq!(after, before);
                assert_eq!(constraint_snapshot(&store), before);
                assert!(catalog.read().unwrap().get_index("uq_users").is_none());
            } else {
                assert!(
                    catalog
                        .read()
                        .unwrap()
                        .get_index("uq_users")
                        .unwrap()
                        .unique
                );
            }
            let ExecutionResult::Query(query) =
                constraint_execute(&mut executor, &catalog, "SELECT id FROM users ORDER BY id")
                    .unwrap()
            else {
                panic!("expected query");
            };
            let mut expected = (1..=8)
                .map(|id| vec![SqlValue::Integer(id)])
                .collect::<Vec<_>>();
            if case == "duplicate" {
                expected.push(vec![SqlValue::Integer(8)]);
            }
            assert_eq!(query.rows, expected);
        }

        #[test]
        #[cfg_attr(not(feature = "lane_ci"), ignore)]
        fn columnar_postload_unique_failfast_preserves_all_bytes() {
            assert_postload_unique_resource_case("failfast");
        }

        #[test]
        #[cfg_attr(not(feature = "lane_ci"), ignore)]
        fn columnar_postload_unique_spill_success_cleans_directory() {
            assert_postload_unique_resource_case("success");
        }

        #[test]
        #[cfg_attr(not(feature = "lane_ci"), ignore)]
        fn columnar_postload_unique_spill_duplicate_cleans_directory() {
            assert_postload_unique_resource_case("duplicate");
        }
    }

    use super::*;
    use crate::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
    use crate::executor::ddl::create_table::execute_create_table;
    use crate::planner::types::ResolvedType;
    use crate::storage::TxnBridge;
    use alopex_core::kv::memory::MemoryKV;
    use std::sync::Arc;

    fn setup_table() -> (TxnBridge<MemoryKV>, MemoryCatalog, TableMetadata) {
        let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
        let mut catalog = MemoryCatalog::new();
        let table = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
                ColumnMetadata::new("age", ResolvedType::Integer),
            ],
        )
        .with_primary_key(vec!["id".into()]);

        let mut txn = bridge.begin_write().unwrap();
        execute_create_table(&mut txn, &mut catalog, table.clone(), vec![], false).unwrap();
        txn.commit().unwrap();

        let stored = catalog.get_table("users").unwrap().clone();
        (bridge, catalog, stored)
    }

    #[test]
    fn create_index_resolves_columns_and_assigns_id() {
        let (bridge, mut catalog, _table_meta) = setup_table();
        let mut txn = bridge.begin_write().unwrap();

        let index = IndexMetadata::new(0, "idx_users_name", "users", vec!["name".into()]);
        let result = execute_create_index(&mut txn, &mut catalog, index, false);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        let stored = catalog
            .get_index("idx_users_name")
            .expect("index stored")
            .clone();
        assert_eq!(stored.index_id, 2); // pk index consumes 1
        assert_eq!(stored.column_indices, vec![1]);
    }

    #[test]
    fn create_index_populates_existing_rows() {
        let (bridge, mut catalog, table_meta) = setup_table();

        // Insert a row before creating the index.
        {
            let mut txn = bridge.begin_write().unwrap();
            let mut table = txn.table_storage(&table_meta);
            table
                .insert(
                    1,
                    &[
                        SqlValue::Integer(1),
                        SqlValue::Text("alice".into()),
                        SqlValue::Integer(30),
                    ],
                )
                .unwrap();
            txn.commit().unwrap();
        }

        let mut txn = bridge.begin_write().unwrap();
        let index = IndexMetadata::new(0, "idx_users_name", "users", vec!["name".into()]);
        execute_create_index(&mut txn, &mut catalog, index, false).unwrap();
        txn.commit().unwrap();

        let stored = catalog.get_index("idx_users_name").unwrap().clone();
        let mut txn = bridge.begin_write().unwrap();
        let mut index_storage = txn.index_storage(
            stored.index_id,
            stored.unique,
            stored.column_indices.clone(),
        );
        let rows = index_storage
            .lookup(&SqlValue::Text("alice".into()))
            .unwrap();
        assert_eq!(rows, vec![1]);
        txn.commit().unwrap();
    }

    #[test]
    fn create_index_rejects_reserved_prefix() {
        let (bridge, mut catalog, _table_meta) = setup_table();
        let mut txn = bridge.begin_write().unwrap();
        let index = IndexMetadata::new(0, "__pk_users", "users", vec!["name".into()]);

        let err = execute_create_index(&mut txn, &mut catalog, index, false).unwrap_err();
        txn.rollback().unwrap();
        assert!(matches!(err, ExecutorError::InvalidIndexName { .. }));
    }

    #[test]
    fn create_index_if_not_exists_is_noop() {
        let (bridge, mut catalog, _table_meta) = setup_table();
        let mut txn = bridge.begin_write().unwrap();
        let index = IndexMetadata::new(0, "idx_users_age", "users", vec!["age".into()]);
        execute_create_index(&mut txn, &mut catalog, index.clone(), false).unwrap();
        txn.commit().unwrap();

        let mut txn = bridge.begin_write().unwrap();
        let result = execute_create_index(&mut txn, &mut catalog, index, true);
        assert!(matches!(result, Ok(ExecutionResult::Success)));
        txn.commit().unwrap();

        assert!(catalog.index_exists("idx_users_age"));
    }
}
