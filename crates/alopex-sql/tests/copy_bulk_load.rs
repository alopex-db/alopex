use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_sql::Catalog;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::bulk::{CopyOptions, CopySecurityConfig, FileFormat, execute_copy};
use alopex_sql::executor::{ConstraintViolation, ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{SqlTxn, SqlValue, TxnBridge};

fn create_table(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
) {
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH (storage='columnar');",
    )
    .unwrap()
    .pop()
    .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    executor.execute(plan).unwrap();
}

fn write_csv(path: &Path) {
    let mut f = File::create(path).unwrap();
    writeln!(f, "id,name").unwrap();
    writeln!(f, "1,alice").unwrap();
    writeln!(f, "2,bob").unwrap();
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_csv_success_and_query() {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    create_table(&mut executor, &catalog);

    let file = tempfile::NamedTempFile::new().unwrap();
    write_csv(file.path());

    {
        let guard = catalog.read().unwrap();
        let mut txn = bridge.begin_write().unwrap();
        let res = execute_copy(
            &mut txn,
            &*guard,
            "users",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        txn.commit().unwrap();
        assert_eq!(res, ExecutionResult::RowsAffected(2));
    }

    let stmt = Parser::parse_sql(&AlopexDialect, "SELECT name FROM users ORDER BY id")
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    match executor.execute(plan).unwrap() {
        ExecutionResult::Query(q) => {
            assert_eq!(
                q.rows,
                vec![
                    vec![alopex_sql::storage::SqlValue::Text("alice".into())],
                    vec![alopex_sql::storage::SqlValue::Text("bob".into())],
                ]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_schema_mismatch_rolls_back() {
    let store = Arc::new(MemoryKV::new());
    let bridge = TxnBridge::new(store.clone());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    create_table(&mut executor, &catalog);
    let before = constraint_snapshot(&store);

    // bad CSV: missing column
    let bad_file = tempfile::NamedTempFile::new().unwrap();
    {
        let mut f = File::create(bad_file.path()).unwrap();
        writeln!(f, "id").unwrap();
        writeln!(f, "1").unwrap();
    }

    let (err, pending_after) = {
        let guard = catalog.read().unwrap();
        let mut txn = bridge.begin_write().unwrap();
        let res = execute_copy(
            &mut txn,
            &*guard,
            "users",
            bad_file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        );
        let err = res.unwrap_err();
        let pending_after = txn
            .inner_mut()
            .scan_prefix(&[])
            .unwrap()
            .collect::<Vec<_>>();
        txn.rollback().unwrap();
        (err, pending_after)
    };
    assert!(matches!(
        err,
        ExecutorError::SchemaMismatch { .. } | ExecutorError::BulkLoad(_)
    ));

    assert_eq!(pending_after, before, "rejected COPY changed pending KV");
    assert_eq!(constraint_snapshot(&store), before);
    let ExecutionResult::Query(query) = constraint_execute(
        &mut executor,
        &catalog,
        "SELECT id, name FROM users ORDER BY id",
    )
    .unwrap() else {
        panic!("expected query result");
    };
    assert!(query.rows.is_empty());
    eprintln!("schema mismatch: pending/committed full KV and public SELECT reached");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_rejects_existing_primary_key() {
    // The same public SQL/auto-transaction boundary owns the row control and columnar case.
    for (storage_name, storage_type) in [
        ("row", alopex_sql::catalog::StorageType::Row),
        ("columnar", alopex_sql::catalog::StorageType::Columnar),
    ] {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(store.clone(), catalog.clone());
        let plan_sql = |sql: &str| {
            let statement = Parser::parse_sql(&AlopexDialect, sql)
                .unwrap()
                .pop()
                .unwrap();
            Planner::new(&*catalog.read().unwrap())
                .plan(&statement)
                .unwrap()
        };
        executor
            .execute(plan_sql(&format!(
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH (storage='{storage_name}')"
            )))
            .unwrap();
        let table_id = {
            let guard = catalog.read().unwrap();
            let table = guard.get_table("users").unwrap();
            assert_eq!(table.storage_options.storage_type, storage_type);
            assert_eq!(
                table.primary_key.as_deref(),
                Some(["id".to_owned()].as_slice())
            );
            assert!(guard.get_indexes_for_table("users").iter().any(|index| {
                index.unique && index.name.starts_with("__pk_") && index.columns == ["id"]
            }));
            table.table_id
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        write_csv(file.path());
        let copy_sql = format!(
            "COPY users FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
            file.path().to_str().unwrap().replace('\'', "''")
        );
        let snapshot = || {
            let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
            let entries = txn.scan_prefix(&[]).unwrap().collect::<Vec<_>>();
            txn.rollback_self().unwrap();
            entries
        };
        let query_rows = |result| {
            let ExecutionResult::Query(query) = result else {
                panic!("expected query result");
            };
            query.rows
        };

        assert_eq!(
            executor.execute(plan_sql(&copy_sql)).unwrap(),
            ExecutionResult::RowsAffected(2),
            "{storage_name}: the first COPY must succeed"
        );
        let expected = vec![
            vec![
                alopex_sql::storage::SqlValue::Integer(1),
                alopex_sql::storage::SqlValue::Text("alice".into()),
            ],
            vec![
                alopex_sql::storage::SqlValue::Integer(2),
                alopex_sql::storage::SqlValue::Text("bob".into()),
            ],
        ];
        assert_eq!(
            query_rows(
                executor
                    .execute(plan_sql("SELECT id, name FROM users ORDER BY id"))
                    .unwrap()
            ),
            expected
        );
        let before = snapshot();
        if storage_type == alopex_sql::catalog::StorageType::Columnar {
            let segment_index =
                alopex_core::columnar::kvs_bridge::key_layout::segment_index_key(table_id);
            assert!(before.iter().any(|(key, _)| key == &segment_index));
        }

        let duplicate = executor.execute(plan_sql(&copy_sql));
        // Observe the state even on RED. Auto-transaction rollback is this public entry's duty.
        let after = query_rows(
            executor
                .execute(plan_sql("SELECT id, name FROM users ORDER BY id"))
                .unwrap(),
        );
        let after_kv = snapshot();
        eprintln!(
            "{storage_name}: duplicate={duplicate:?}, rows_after={after:?}, all_kv_preserved={}",
            after_kv == before
        );
        assert!(
            matches!(duplicate, Err(ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { columns, .. })) if columns == ["id"]),
            "{storage_name}: COPY must reject the existing PK with the PRIMARY KEY error"
        );
        assert_eq!(after, expected, "{storage_name}: existing rows changed");
        assert_eq!(
            after_kv, before,
            "{storage_name}: failed COPY changed KV state"
        );
        eprintln!("{storage_name}: existing-PK contract PASS");
    }
}

fn constraint_execute(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Result<ExecutionResult, ExecutorError> {
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
        alopex_sql::catalog::StorageType::Columnar
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

fn assert_columnar_null_rejected(columns: &str, csv: &str, expected_column: &str) {
    let (store, catalog, mut executor) = constraint_fixture(columns);
    let before = constraint_snapshot(&store);
    let file = constraint_csv(csv);
    let result = constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file));
    let after = constraint_snapshot(&store);
    eprintln!(
        "NULL({expected_column}): runtime={result:?}, all_kv_unchanged={}",
        before == after
    );
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::NotNull { column })) if column == expected_column),
        "expected NotNull({expected_column}), got {result:?}"
    );
    assert_eq!(after, before, "rejected NULL changed the complete KV state");
    eprintln!("NULL({expected_column}): strict error and complete KV oracle reached");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_inline_pk_null_is_rejected() {
    assert_columnar_null_rejected("id INT PRIMARY KEY, name TEXT", "id,name\nNULL,bad\n", "id");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_not_null_only_is_rejected() {
    assert_columnar_null_rejected("id INT, name TEXT NOT NULL", "id,name\n1,NULL\n", "name");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_table_pk_null_is_rejected() {
    assert_columnar_null_rejected(
        "id INT, name TEXT, PRIMARY KEY (id)",
        "id,name\nNULL,bad\n",
        "id",
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_pk_first_null_is_rejected() {
    assert_columnar_null_rejected("a INT, b INT, PRIMARY KEY (a, b)", "a,b\nNULL,1\n", "a");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_pk_second_null_is_rejected() {
    assert_columnar_null_rejected("a INT, b INT, PRIMARY KEY (a, b)", "a,b\n1,NULL\n", "b");
}

fn assert_nullable_composite_unique(csv: &str, expected_row: Vec<SqlValue>) {
    let (_, catalog, mut executor) =
        constraint_fixture("a INT, b INT, CONSTRAINT uq_pair UNIQUE (a, b)");
    let file = constraint_csv(csv);
    // Each COPY covers an input duplicate; the second also covers existing segments.
    for copy in 1..=2 {
        assert_eq!(
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
            ExecutionResult::RowsAffected(2),
            "nullable UNIQUE COPY {copy} must accept both rows"
        );
        let ExecutionResult::Query(query) =
            constraint_execute(&mut executor, &catalog, "SELECT a, b FROM users").unwrap()
        else {
            panic!("expected query result");
        };
        assert_eq!(query.rows, vec![expected_row.clone(); 2 * copy]);
        eprintln!("nullable UNIQUE COPY {copy}: complete row values reached");
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_unique_first_null_is_allowed() {
    assert_nullable_composite_unique(
        "a,b\nNULL,7\nNULL,7\n",
        vec![SqlValue::Null, SqlValue::Integer(7)],
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_unique_second_null_is_allowed() {
    assert_nullable_composite_unique(
        "a,b\n7,NULL\n7,NULL\n",
        vec![SqlValue::Integer(7), SqlValue::Null],
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_composite_unique_non_null_duplicate_is_rejected() {
    let (store, catalog, mut executor) =
        constraint_fixture("a INT, b INT, CONSTRAINT uq_pair UNIQUE (a, b)");
    let first = constraint_csv("a,b\n1,7\n");
    assert_eq!(
        constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&first)).unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    let before = constraint_snapshot(&store);
    let conflicting = constraint_csv("a,b\n2,8\n1,7\n");
    let result = constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&conflicting));
    let after = constraint_snapshot(&store);
    eprintln!(
        "non-NULL UNIQUE: runtime={result:?}, all_kv_unchanged={}",
        before == after
    );
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. }))
            if index_name == "uq_pair" && columns == &["a", "b"]),
        "expected the named composite UNIQUE error, got {result:?}"
    );
    assert_eq!(after, before);
    let ExecutionResult::Query(query) =
        constraint_execute(&mut executor, &catalog, "SELECT a, b FROM users").unwrap()
    else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(7)]]
    );
}

fn assert_borrowed_copy_failure_preserves_prior_write(csv: &str, null_error: bool) {
    let (store, catalog, mut executor) = constraint_fixture("id INT, name TEXT, PRIMARY KEY (id)");
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_write().unwrap();
    let first = constraint_csv("id,name\n1,prior\n");
    let failing = constraint_csv(csv);
    let guard = catalog.read().unwrap();
    let committed_before = constraint_snapshot(&store);
    assert_eq!(
        execute_copy(
            &mut txn,
            &*guard,
            "users",
            first.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default()
        )
        .unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    let before = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    assert_ne!(
        before, committed_before,
        "the first COPY must be pending in this transaction"
    );
    let result = execute_copy(
        &mut txn,
        &*guard,
        "users",
        failing.path().to_str().unwrap(),
        FileFormat::Csv,
        CopyOptions { header: true },
        &CopySecurityConfig::default(),
    );
    let after = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    drop(guard);
    // Observe all outcomes even on RED; never rollback to make the equality oracle pass.
    let commit_result = txn.commit();
    let committed_after = constraint_snapshot(&store);
    let query_result = constraint_execute(
        &mut executor,
        &catalog,
        "SELECT id, name FROM users ORDER BY id",
    );
    eprintln!(
        "borrowed COPY: runtime={result:?}, all_kv_unchanged={}, commit={commit_result:?}, query={query_result:?}",
        before == after
    );
    let correct_error = if null_error {
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::NotNull { column })) if column == "id")
    } else {
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { columns, .. })) if columns == &["id"])
    };
    assert!(correct_error, "unexpected borrowed COPY result: {result:?}");
    assert_eq!(
        after, before,
        "failed COPY changed the pending complete KV state"
    );
    commit_result.unwrap();
    assert_eq!(
        committed_after, before,
        "commit did not preserve exactly the prior write"
    );
    let ExecutionResult::Query(query) = query_result.unwrap() else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("prior".into())]]
    );
    eprintln!("borrowed COPY: strict error, full KV equality, commit and prior values reached");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_borrowed_late_null_preserves_prior_write() {
    assert_borrowed_copy_failure_preserves_prior_write("id,name\n2,new\nNULL,bad\n", true);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_borrowed_late_pk_duplicate_preserves_prior_write() {
    assert_borrowed_copy_failure_preserves_prior_write("id,name\n2,new\n1,duplicate\n", false);
}

// Only MemoryPolicy is supplied here. All eight storage methods delegate to the
// real SqlTransaction; this wrapper neither emulates storage nor rolls it back.
struct CopyPolicyTxn<'txn> {
    inner: alopex_sql::storage::SqlTransaction<'txn, MemoryKV>,
    policy: alopex_sql::executor::MemoryPolicy,
}

impl<'txn> SqlTxn<'txn, MemoryKV> for CopyPolicyTxn<'txn> {
    fn memory_policy(&self) -> Option<&alopex_sql::executor::MemoryPolicy> {
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
    ) -> alopex_core::Result<&mut alopex_sql::storage::bridge::HnswTxnEntry> {
        SqlTxn::hnsw_entry_mut(&mut self.inner, name)
    }
    fn flush_hnsw(&mut self) -> alopex_sql::storage::error::Result<()> {
        SqlTxn::flush_hnsw(&mut self.inner)
    }
    fn abandon_hnsw(&mut self) -> alopex_sql::storage::error::Result<()> {
        SqlTxn::abandon_hnsw(&mut self.inner)
    }
    fn delete_prefix(&mut self, prefix: &[u8]) -> alopex_sql::storage::error::Result<()> {
        SqlTxn::delete_prefix(&mut self.inner, prefix)
    }
}

fn boundary_fixture() -> (
    Arc<MemoryKV>,
    Arc<RwLock<MemoryCatalog>>,
    Executor<MemoryKV, MemoryCatalog>,
) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    constraint_execute(&mut executor, &catalog,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH (storage='columnar', row_group_size=1000)").unwrap();
    assert_eq!(
        catalog
            .read()
            .unwrap()
            .get_table("users")
            .unwrap()
            .storage_options
            .row_group_size,
        1000
    );
    (store, catalog, executor)
}

fn boundary_csv(count: i32) -> String {
    use std::fmt::Write as _;
    let mut csv = String::from("id,name\n");
    for id in 1..=count {
        writeln!(csv, "{id},value-{id}").unwrap();
    }
    csv
}

fn boundary_copy<'txn>(
    txn: &mut impl SqlTxn<'txn, MemoryKV>,
    catalog: &MemoryCatalog,
    file: &tempfile::NamedTempFile,
    header: bool,
) -> Result<ExecutionResult, ExecutorError> {
    execute_copy(
        txn,
        catalog,
        "users",
        file.path().to_str().unwrap(),
        FileFormat::Csv,
        CopyOptions { header },
        &CopySecurityConfig::default(),
    )
}

fn assert_boundary_pk(result: &Result<ExecutionResult, ExecutorError>) {
    assert!(
        matches!(result,
        Err(ExecutorError::ConstraintViolation(ConstraintViolation::PrimaryKey { columns, .. }))
        if columns == &["id"]),
        "expected PrimaryKey(id), got {result:?}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_cross_batch_duplicate_preserves_all_kv() {
    let (store, catalog, _) = boundary_fixture();
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_write().unwrap();
    let mut csv = boundary_csv(1000);
    csv.push_str("1,duplicate-after-batch\n");
    let file = constraint_csv(&csv);
    let before = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let result = boundary_copy(&mut txn, &catalog.read().unwrap(), &file, true);
    let after = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let commit = txn.commit();
    eprintln!(
        "1001 rows/1000 batch: {result:?}; pending_equal={}; commit={commit:?}",
        before == after
    );
    assert_boundary_pk(&result);
    assert_eq!(after, before);
    commit.unwrap();
    assert_eq!(constraint_snapshot(&store), before);
}

struct CopySpillObservation {
    directory: std::path::PathBuf,
    files: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    visible_files: std::sync::atomic::AtomicU64,
}

impl alopex_sql::executor::memory::SpillMetricsSink for CopySpillObservation {
    fn record_spill(&self, bytes: u64, files: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.files.fetch_add(files, Relaxed);
        self.bytes.fetch_add(bytes, Relaxed);
        let visible = std::fs::read_dir(&self.directory).unwrap().count() as u64;
        self.visible_files.fetch_max(visible, Relaxed);
    }
}

// Each test owns a real configured directory and observes flushed files through
// the existing metrics callback; an empty directory alone cannot prove spilling.
fn assert_copy_resource_case(case: &str) {
    use alopex_sql::executor::{MemoryPolicy, SpillPolicy};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    let (store, catalog, mut executor) = boundary_fixture();
    let bridge = TxnBridge::new(store.clone());
    let directory = tempfile::tempdir().unwrap();
    let observation = Arc::new(CopySpillObservation {
        directory: directory.path().to_path_buf(),
        files: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        visible_files: AtomicU64::new(0),
    });
    // The success and duplicate cases use exactly the same eight-row input.
    // Only the duplicate case has a pre-existing conflicting segment.
    if case == "duplicate" {
        let prior = constraint_csv("id,name\n8,prior\n");
        assert_eq!(
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&prior)).unwrap(),
            ExecutionResult::RowsAffected(1)
        );
    }
    let (limit, spill) = if case == "failfast" {
        (1, SpillPolicy::FailFast)
    } else {
        (
            256,
            SpillPolicy::SpillToDisk {
                directory: directory.path().to_path_buf(),
            },
        )
    };
    // Eight INT keys are only ~160 tracked bytes. A 64-byte spill limit makes
    // both success and duplicate tests spill; late-parse uses fewer larger runs.
    let limit = if case == "success" || case == "duplicate" {
        64
    } else {
        limit
    };
    let policy = MemoryPolicy::new(Some(limit), spill).with_metrics(observation.clone());
    let mut txn = CopyPolicyTxn {
        inner: bridge.begin_write().unwrap(),
        policy,
    };
    let mut csv = boundary_csv(if case == "late_parse" { 1000 } else { 8 });
    if case == "late_parse" {
        csv.push_str("not_an_integer,bad-after-first-batch\n");
    }
    let file = constraint_csv(&csv);
    let before = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let result = boundary_copy(&mut txn, &catalog.read().unwrap(), &file, true);
    let after = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let files = observation.files.load(Relaxed);
    let bytes = observation.bytes.load(Relaxed);
    let visible = observation.visible_files.load(Relaxed);
    let remaining = std::fs::read_dir(directory.path()).unwrap().count();
    // Commit even after a reported error: no rollback may manufacture equality.
    let commit = txn.inner.commit();
    eprintln!(
        "resource {case}: {result:?}; pending_equal={}; files={files}; bytes={bytes}; visible={visible}; remaining={remaining}; commit={commit:?}",
        before == after
    );
    match case {
        "success" => assert_eq!(result.as_ref().unwrap(), &ExecutionResult::RowsAffected(8)),
        "duplicate" => assert_boundary_pk(&result),
        "failfast" => assert!(
            matches!(result, Err(ExecutorError::ResourceExhausted { .. })),
            "{result:?}"
        ),
        "late_parse" => assert!(
            matches!(&result, Err(ExecutorError::BulkLoad(reason)) if reason.contains("not_an_integer")),
            "{result:?}"
        ),
        _ => unreachable!(),
    }
    assert_eq!(
        remaining, 0,
        "COPY leaked files in its specified spill directory"
    );
    if case == "failfast" {
        assert_eq!(files, 0);
    } else {
        assert!(
            files > 0 && bytes > 0 && visible > 0,
            "real spill was not observed"
        );
    }
    commit.unwrap();
    if case != "success" {
        assert_eq!(after, before);
        assert_eq!(constraint_snapshot(&store), before);
    }
    let ExecutionResult::Query(query) = constraint_execute(
        &mut executor,
        &catalog,
        "SELECT id, name FROM users ORDER BY id",
    )
    .unwrap() else {
        panic!("expected rows");
    };
    let expected = if case == "success" {
        (1..=8)
            .map(|id| vec![SqlValue::Integer(id), SqlValue::Text(format!("value-{id}"))])
            .collect::<Vec<_>>()
    } else if case == "duplicate" {
        vec![vec![SqlValue::Integer(8), SqlValue::Text("prior".into())]]
    } else {
        Vec::new()
    };
    assert_eq!(query.rows, expected);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_constraint_failfast_preserves_all_kv() {
    assert_copy_resource_case("failfast");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_constraint_spill_success_cleans_directory() {
    assert_copy_resource_case("success");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_constraint_spill_duplicate_cleans_directory() {
    assert_copy_resource_case("duplicate");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_spill_before_late_parse_error_preserves_all_kv() {
    assert_copy_resource_case("late_parse");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_same_snapshot_duplicate_conflicts_at_commit() {
    let (store, catalog, mut executor) = boundary_fixture();
    let bridge = TxnBridge::new(store.clone());
    // Both begin before either COPY or commit: this is a shared snapshot, not
    // sequential COPY rejection against an already committed primary key.
    let mut first = bridge.begin_write().unwrap();
    let mut second = bridge.begin_write().unwrap();
    let a = constraint_csv("id,name\n1,first\n");
    let b = constraint_csv("id,name\n1,second\n");
    for (txn, file) in [(&mut first, &a), (&mut second, &b)] {
        assert_eq!(
            boundary_copy(txn, &catalog.read().unwrap(), file, true).unwrap(),
            ExecutionResult::RowsAffected(1)
        );
    }
    first.commit().unwrap();
    let committed = constraint_snapshot(&store);
    let second_commit = second.commit();
    eprintln!("shared-snapshot second commit: {second_commit:?}");
    assert!(
        matches!(
            second_commit,
            Err(alopex_sql::storage::StorageError::TransactionConflict)
        ),
        "{second_commit:?}"
    );
    assert_eq!(constraint_snapshot(&store), committed);
    let ExecutionResult::Query(query) =
        constraint_execute(&mut executor, &catalog, "SELECT id, name FROM users").unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("first".into())]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_empty_csv_preserves_pending_and_committed_kv() {
    let (store, catalog, mut executor) = boundary_fixture();
    let prior = constraint_csv("id,name\n1,prior\n");
    constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&prior)).unwrap();
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_write().unwrap();
    let before = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let empty = constraint_csv("");
    let result = boundary_copy(&mut txn, &catalog.read().unwrap(), &empty, false);
    let after = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let commit = txn.commit();
    assert_eq!(result.unwrap(), ExecutionResult::RowsAffected(0));
    assert_eq!(after, before);
    commit.unwrap();
    assert_eq!(constraint_snapshot(&store), before);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn copy_columnar_named_unique_supporting_pk_reports_primary_key() {
    let (store, catalog, mut executor) = constraint_fixture("id INT, name TEXT, PRIMARY KEY (id)");
    // Exercise public Catalog operations, not a fake lookup or reimplementation
    // of PersistentCatalog recovery. Keep the real table/column/namespace/ID.
    {
        let mut guard = catalog.write().unwrap();
        let mut supporting = guard.get_index("__pk_users").unwrap().clone();
        guard.drop_index("__pk_users").unwrap();
        supporting.name = "customer_identity_unique".into();
        guard.create_index(supporting).unwrap();
        assert!(guard.get_index("__pk_users").is_none());
        assert_eq!(guard.get_indexes_for_table("users").len(), 1);
    }
    let prior = constraint_csv("id,name\n1,prior\n");
    assert_eq!(
        constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&prior)).unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    let bridge = TxnBridge::new(store.clone());
    let mut txn = bridge.begin_write().unwrap();
    let before = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let duplicate = constraint_csv("id,name\n2,new\n1,duplicate\n");
    let result = boundary_copy(&mut txn, &catalog.read().unwrap(), &duplicate, true);
    let after = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    let commit = txn.commit();
    eprintln!(
        "named supporting UNIQUE: {result:?}; pending_equal={}; commit={commit:?}",
        before == after
    );
    assert_boundary_pk(&result);
    assert_eq!(after, before);
    commit.unwrap();
    assert_eq!(constraint_snapshot(&store), before);
    let ExecutionResult::Query(query) = constraint_execute(
        &mut executor,
        &catalog,
        "SELECT id, name FROM users ORDER BY id",
    )
    .unwrap() else {
        panic!("expected rows");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("prior".into())]]
    );
}

fn assert_postload_columnar_unique_rejects_duplicates(public_sql: bool) {
    use alopex_sql::catalog::IndexMetadata;
    use alopex_sql::planner::LogicalPlan;

    let (store, catalog, mut executor) = constraint_fixture("id INT");
    let file = constraint_csv("id\n1\n1\n");
    assert_eq!(
        constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
        ExecutionResult::RowsAffected(2)
    );
    let expected = vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(1)]];
    let ExecutionResult::Query(before_rows) =
        constraint_execute(&mut executor, &catalog, "SELECT id FROM users ORDER BY id").unwrap()
    else {
        panic!("expected existing columnar rows");
    };
    assert_eq!(before_rows.rows, expected);
    let before = constraint_snapshot(&store);
    let table_before = catalog.read().unwrap().get_table("users").unwrap().clone();
    let plan = if public_sql {
        let statement =
            Parser::parse_sql(&AlopexDialect, "CREATE UNIQUE INDEX uq_users ON users(id)")
                .unwrap()
                .pop()
                .unwrap();
        assert!(matches!(
            &statement.kind,
            alopex_sql::ast::StatementKind::CreateIndex(index) if index.unique
        ));
        Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap()
    } else {
        LogicalPlan::CreateIndex {
            index: IndexMetadata::new(0, "uq_users", "users", vec!["id".into()]).with_unique(true),
            if_not_exists: false,
        }
    };
    assert!(matches!(&plan, LogicalPlan::CreateIndex { index, .. } if index.unique));
    let result = executor.execute(plan);
    let after = constraint_snapshot(&store);
    let index_absent = catalog.read().unwrap().get_index("uq_users").is_none();
    let table_after = catalog.read().unwrap().get_table("users").unwrap().clone();
    let ExecutionResult::Query(after_rows) =
        constraint_execute(&mut executor, &catalog, "SELECT id FROM users ORDER BY id").unwrap()
    else {
        panic!("expected preserved columnar rows");
    };
    eprintln!(
        "public_sql={public_sql} result={result:?} all_kv_unchanged={} index_absent={index_absent} rows={:?}",
        before == after,
        after_rows.rows
    );
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. }))
            if index_name == "uq_users" && columns == &["id"]),
        "existing duplicate columnar rows must reject UNIQUE creation: {result:?}"
    );
    assert_eq!(after, before);
    assert!(index_absent);
    assert_eq!(table_after.table_id, table_before.table_id);
    assert_eq!(table_after.name, table_before.name);
    assert_eq!(table_after.primary_key, table_before.primary_key);
    assert_eq!(table_after.properties, table_before.properties);
    assert_eq!(table_after.storage_options, table_before.storage_options);
    assert_eq!(after_rows.rows, expected);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn columnar_postload_unique_sql_rejects_existing_duplicates() {
    assert_postload_columnar_unique_rejects_duplicates(true);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn columnar_postload_unique_typed_plan_rejects_existing_duplicates() {
    assert_postload_columnar_unique_rejects_duplicates(false);
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn columnar_postload_unique_preserves_nulls_and_checks_future_copy() {
    let (store, catalog, mut executor) = constraint_fixture("id INT");
    for csv in ["id\n1\nNULL\n", "id\n2\nNULL\n"] {
        let file = constraint_csv(csv);
        constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap();
    }
    constraint_execute(
        &mut executor,
        &catalog,
        "CREATE UNIQUE INDEX uq_users ON users(id)",
    )
    .unwrap();
    let index = catalog
        .read()
        .unwrap()
        .get_index("uq_users")
        .unwrap()
        .clone();
    assert!(index.unique);
    assert_eq!(index.column_indices, vec![0]);
    let valid = constraint_csv("id\n3\nNULL\n");
    constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&valid)).unwrap();
    let before = constraint_snapshot(&store);
    let duplicate = constraint_csv("id\n4\n1\n");
    let result = constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&duplicate));
    assert!(
        matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, .. })) if index_name == "uq_users"),
        "{result:?}"
    );
    assert_eq!(constraint_snapshot(&store), before);
    let ExecutionResult::Query(query) = constraint_execute(
        &mut executor,
        &catalog,
        "SELECT id FROM users ORDER BY id NULLS LAST",
    )
    .unwrap() else {
        panic!("expected query");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
            vec![SqlValue::Null],
            vec![SqlValue::Null],
            vec![SqlValue::Null]
        ]
    );
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn columnar_postload_composite_unique_checks_complete_keys() {
    for duplicate in [false, true] {
        let (store, catalog, mut executor) = constraint_fixture("a INT, b INT");
        for csv in [
            "a,b\n1,10\nNULL,10\n1,NULL\n",
            "a,b\n1,11\nNULL,10\n1,NULL\n",
        ] {
            let file = constraint_csv(csv);
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap();
        }
        if duplicate {
            let file = constraint_csv("a,b\n1,10\n");
            constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap();
        }
        let before = constraint_snapshot(&store);
        let ExecutionResult::Query(rows_before) = constraint_execute(
            &mut executor,
            &catalog,
            "SELECT a, b FROM users ORDER BY a NULLS LAST, b NULLS LAST",
        )
        .unwrap() else {
            panic!("expected composite rows");
        };
        let result = executor.execute(alopex_sql::planner::LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "uq_pair",
                "users",
                vec!["a".into(), "b".into()],
            )
            .with_unique(true),
            if_not_exists: false,
        });
        let ExecutionResult::Query(rows_after) = constraint_execute(
            &mut executor,
            &catalog,
            "SELECT a, b FROM users ORDER BY a NULLS LAST, b NULLS LAST",
        )
        .unwrap() else {
            panic!("expected preserved composite rows");
        };
        assert_eq!(rows_after.rows, rows_before.rows);
        if duplicate {
            assert!(
                matches!(&result, Err(ExecutorError::ConstraintViolation(ConstraintViolation::Unique { index_name, columns, .. })) if index_name == "uq_pair" && columns == &["a", "b"]),
                "{result:?}"
            );
            assert_eq!(constraint_snapshot(&store), before);
            assert!(catalog.read().unwrap().get_index("uq_pair").is_none());
        } else {
            result.unwrap();
            assert_eq!(
                catalog
                    .read()
                    .unwrap()
                    .get_index("uq_pair")
                    .unwrap()
                    .column_indices,
                vec![0, 1]
            );
            let file = constraint_csv("a,b\n2,10\nNULL,10\n1,NULL\n");
            assert_eq!(
                constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
                ExecutionResult::RowsAffected(3)
            );
            let before = constraint_snapshot(&store);
            let file = constraint_csv("a,b\n1,11\n");
            assert!(matches!(
                constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)),
                Err(ExecutorError::ConstraintViolation(
                    ConstraintViolation::Unique { .. }
                ))
            ));
            assert_eq!(constraint_snapshot(&store), before);
        }
    }
}

#[test]
#[cfg_attr(not(feature = "lane_ci"), ignore)]
fn columnar_postload_unique_rejects_unindexable_type_before_null_skip() {
    for (declaration, kind) in [
        ("JSON", "Json"),
        ("VECTOR(2)", "Vector"),
        ("INTERVAL", "Interval"),
        ("DECIMAL(10,2)", "Decimal"),
    ] {
        let (store, catalog, mut executor) = constraint_fixture(&format!("payload {declaration}"));
        // Fixed-encoding COPY has a separate known selector boundary on this
        // base. Keep those cases at the empty-table DDL boundary, not a codec test.
        if kind == "Json" {
            let file = constraint_csv("payload\nNULL\n");
            assert_eq!(
                constraint_execute(&mut executor, &catalog, &constraint_copy_sql(&file)).unwrap(),
                ExecutionResult::RowsAffected(1)
            );
        }
        let before = constraint_snapshot(&store);
        let result = executor.execute(alopex_sql::planner::LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "uq_json",
                "users",
                vec!["payload".into()],
            )
            .with_unique(true),
            if_not_exists: false,
        });
        if kind == "Json" {
            assert!(
                matches!(&result, Err(ExecutorError::InvalidOperation { operation, reason })
            if operation == "CREATE INDEX" && reason == "Alopex does not define a JSON sort order"),
                "{result:?}"
            );
        } else {
            assert!(
                matches!(&result, Err(ExecutorError::Storage(alopex_sql::storage::StorageError::TypeMismatch { expected, actual })) if expected == "indexable scalar type" && actual == kind),
                "{result:?}"
            );
        }
        assert_eq!(constraint_snapshot(&store), before);
        assert!(catalog.read().unwrap().get_index("uq_json").is_none());
    }
}
