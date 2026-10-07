use std::fs::File;
use std::sync::{Arc, RwLock};

use alopex_core::Result as CoreResult;
use alopex_core::kv::memory::{MemoryKV, MemoryTransaction};
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::HnswIndex;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::bulk::CopySecurityConfig;
use alopex_sql::executor::query::execute_query_with_policy;
use alopex_sql::executor::{
    ExecutionResult, Executor, ExecutorError, MemoryPolicy, QueryResult, SpillPolicy,
};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::bridge::HnswTxnEntry;
use alopex_sql::storage::error::Result as StorageResult;
use alopex_sql::storage::{SqlTransaction, SqlTxn, SqlValue, TxnBridge};
use arrow_array::{Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

fn write_parquet(file: &tempfile::NamedTempFile, schema: Arc<Schema>, batch: RecordBatch) {
    let mut writer =
        ArrowWriter::try_new(File::create(file.path()).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn parquet_columnar_bigint_boundaries_preserve_exact_rows() {
    // Deliberately unsorted; every expected value/comparison stays in native i64.
    let values = [
        9_007_199_254_740_993_i64,
        -9_007_199_254_740_992,
        9_007_199_254_740_992,
        -9_007_199_254_740_993,
    ];
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    write_parquet(&file, schema, batch);

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let literals = values
        .iter()
        .map(|value| format!("(CAST('{value}' AS BIGINT))"))
        .collect::<Vec<_>>()
        .join(", ");
    run(
        &mut executor,
        &catalog,
        &format!(
            "CREATE TABLE row_values (id BIGINT);
             INSERT INTO row_values VALUES {literals};
             CREATE TABLE column_values (id BIGINT) WITH (storage='columnar');
             COPY column_values FROM '{}' WITH (FORMAT PARQUET)",
            file.path().display()
        ),
    )
    .unwrap();

    let mut failures = Vec::new();
    let mut checked = 0;
    let mut check = |sql: String, expected: Vec<i64>| {
        checked += 1;
        let expected = expected
            .into_iter()
            .map(|value| vec![SqlValue::BigInt(value)])
            .collect::<Vec<_>>();
        match run(&mut executor, &catalog, &sql) {
            Ok(Some(result)) if result.rows == expected => {}
            actual => failures.push(format!("{sql}: expected {expected:?}, got {actual:?}")),
        }
    };
    for source in [
        format!("read_parquet('{}')", file.path().display()),
        "row_values".to_string(),
        "column_values".to_string(),
    ] {
        let mut ascending = values.to_vec();
        ascending.sort_unstable();
        check(
            format!("SELECT id FROM {source} ORDER BY id ASC"),
            ascending,
        );
        let mut descending = values.to_vec();
        descending.sort_unstable_by(|a, b| b.cmp(a));
        descending.truncate(2);
        check(
            format!("SELECT id FROM {source} ORDER BY id DESC LIMIT 2"),
            descending,
        );
        for threshold in values {
            for operator in ["=", "!=", "<", ">="] {
                let mut expected = values
                    .iter()
                    .copied()
                    .filter(|value| match operator {
                        "=" => *value == threshold,
                        "!=" => *value != threshold,
                        "<" => *value < threshold,
                        ">=" => *value >= threshold,
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();
                expected.sort_unstable();
                expected.truncate(3);
                check(
                    format!(
                        "SELECT id FROM {source} WHERE id {operator} CAST('{threshold}' AS BIGINT) \
                         ORDER BY id ASC LIMIT 3"
                    ),
                    expected,
                );
            }
        }
    }
    assert_eq!(checked, 54);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn restricted_parquet_copy_preserves_columnar_unique_and_pending_rows() {
    use alopex_core::kv::KVTransaction;
    use alopex_sql::executor::ConstraintViolation;
    use alopex_sql::executor::bulk::{CopyOptions, FileFormat, execute_copy};

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let allowed = root.join("allowed");
    std::fs::create_dir(&allowed).unwrap();
    let first = tempfile::NamedTempFile::new_in(&allowed).unwrap();
    let duplicate = tempfile::NamedTempFile::new_in(&allowed).unwrap();
    let outside = tempfile::NamedTempFile::new_in(&root).unwrap();
    let missing = root.join("missing.parquet");
    for (file, id) in [(&first, 1_i64), (&duplicate, 2), (&outside, 3)] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec!["shared"])),
            ],
        )
        .unwrap();
        write_parquet(file, schema, batch);
    }

    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));
    run(
        &mut executor,
        &catalog,
        "CREATE TABLE copied (id INTEGER PRIMARY KEY, label TEXT) WITH (storage='columnar');
         CREATE UNIQUE INDEX uq_copied_label ON copied(label)",
    )
    .unwrap();
    let bridge = TxnBridge::new(store);
    let mut txn = bridge.begin_write().unwrap();
    let guard = catalog.read().unwrap();
    let config = CopySecurityConfig {
        allowed_base_dirs: Some(vec![allowed.clone()]),
        allow_symlinks: false,
    };
    let before_load = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(
        execute_copy(
            &mut txn,
            &*guard,
            "copied",
            first.path().to_str().unwrap(),
            FileFormat::Parquet,
            CopyOptions { header: false },
            &config,
        )
        .unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    let pending = txn
        .inner_mut()
        .scan_prefix(&[])
        .unwrap()
        .collect::<Vec<_>>();
    assert_ne!(
        pending, before_load,
        "the first COPY must have pending writes"
    );

    for path in [outside.path(), missing.as_path()] {
        let error = execute_copy(
            &mut txn,
            &*guard,
            "copied",
            path.to_str().unwrap(),
            FileFormat::Parquet,
            CopyOptions { header: false },
            &config,
        )
        .unwrap_err();
        assert!(!error.to_string().contains(allowed.to_str().unwrap()));
        assert!(
            matches!(&error, ExecutorError::PathValidationFailed { path: actual, reason }
                if actual == path.to_str().unwrap() && reason == "path not in allowed directories"),
            "expected the same denial for existing and missing paths: {error:?}"
        );
        assert_eq!(
            txn.inner_mut()
                .scan_prefix(&[])
                .unwrap()
                .collect::<Vec<_>>(),
            pending,
            "denied COPY must preserve all pending KV bytes"
        );
    }

    let error = execute_copy(
        &mut txn,
        &*guard,
        "copied",
        duplicate.path().to_str().unwrap(),
        FileFormat::Parquet,
        CopyOptions { header: false },
        &config,
    )
    .unwrap_err();
    assert!(
        matches!(&error, ExecutorError::ConstraintViolation(ConstraintViolation::Unique {
            index_name, columns, ..
        }) if index_name == "uq_copied_label" && columns == &["label"]),
        "a different PK must fail specifically on the named UNIQUE: {error:?}"
    );
    assert_eq!(
        txn.inner_mut()
            .scan_prefix(&[])
            .unwrap()
            .collect::<Vec<_>>(),
        pending,
        "duplicate COPY must preserve all pending KV bytes"
    );
    drop(guard);
    txn.commit().unwrap();
    let result = run(
        &mut executor,
        &catalog,
        "SELECT id, label FROM copied ORDER BY id",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("shared".into())]]
    );
}

// Only the inherited policy differs; every storage operation uses the real txn.
struct RestrictedTxn<'txn> {
    inner: SqlTransaction<'txn, MemoryKV>,
    security: CopySecurityConfig,
}

impl<'txn> SqlTxn<'txn, MemoryKV> for RestrictedTxn<'txn> {
    fn read_security(&self) -> Option<&CopySecurityConfig> {
        Some(&self.security)
    }

    fn mode(&self) -> TxnMode {
        SqlTxn::mode(&self.inner)
    }

    fn ensure_write_txn(&self) -> CoreResult<()> {
        SqlTxn::ensure_write_txn(&self.inner)
    }

    fn inner_mut(&mut self) -> &mut MemoryTransaction<'txn> {
        SqlTxn::inner_mut(&mut self.inner)
    }

    fn hnsw_entry(&mut self, name: &str) -> CoreResult<&HnswIndex> {
        SqlTxn::hnsw_entry(&mut self.inner, name)
    }

    fn hnsw_entry_mut(&mut self, name: &str) -> CoreResult<&mut HnswTxnEntry> {
        SqlTxn::hnsw_entry_mut(&mut self.inner, name)
    }

    fn flush_hnsw(&mut self) -> StorageResult<()> {
        SqlTxn::flush_hnsw(&mut self.inner)
    }

    fn abandon_hnsw(&mut self) -> StorageResult<()> {
        SqlTxn::abandon_hnsw(&mut self.inner)
    }

    fn delete_prefix(&mut self, prefix: &[u8]) -> StorageResult<()> {
        SqlTxn::delete_prefix(&mut self.inner, prefix)
    }
}

#[test]
fn issue577_read_parquet_inherits_restricted_transaction_policy() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let allowed = root.join("allowed");
    std::fs::create_dir(&allowed).unwrap();
    let inside = tempfile::NamedTempFile::new_in(&allowed).unwrap();
    let outside = tempfile::NamedTempFile::new_in(&root).unwrap();
    for (file, values) in [(&inside, vec![11, 12]), (&outside, vec![99])] {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        write_parquet(file, schema, batch);
    }
    let catalog = MemoryCatalog::new();
    let plan = |file: &tempfile::NamedTempFile| {
        let sql = format!("SELECT * FROM read_parquet('{}')", file.path().display());
        let statement = Parser::parse_sql(&AlopexDialect, &sql).unwrap().remove(0);
        // Deliberately allow planning: this test owns runtime policy inheritance.
        Planner::new(&catalog).plan(&statement).unwrap()
    };
    let inside_plan = plan(&inside);
    let outside_plan = plan(&outside);
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let mut txn = RestrictedTxn {
        inner: bridge.begin_write().unwrap(),
        security: CopySecurityConfig {
            allowed_base_dirs: Some(vec![allowed]),
            allow_symlinks: false,
        },
    };

    let result = execute_query_with_policy(&mut txn, &catalog, inside_plan.clone(), None).unwrap();
    let ExecutionResult::Query(result) = result else {
        panic!("expected the allowed input to produce rows");
    };
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::BigInt(11)], vec![SqlValue::BigInt(12)]]
    );

    let error = execute_query_with_policy(&mut txn, &catalog, outside_plan, None)
        .expect_err("runtime must inherit the transaction's restricted policy");
    assert!(
        matches!(&error, ExecutorError::PathValidationFailed { reason, .. }
            if reason == "path not in allowed directories"),
        "expected the generic runtime path denial, got {error:?}"
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("extra", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(Int64Array::from(vec![14])),
        ],
    )
    .unwrap();
    write_parquet(&inside, schema, batch);
    let error = execute_query_with_policy(&mut txn, &catalog, inside_plan, None)
        .expect_err("the restricted reader must validate the actual file against the plan");
    assert!(
        matches!(
            error,
            ExecutorError::SchemaMismatch {
                expected: 1,
                actual: 2,
                ..
            }
        ),
        "expected a precise runtime schema mismatch, got {error:?}"
    );
    txn.inner.rollback().unwrap();
}

fn run(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Result<Option<QueryResult>, String> {
    let mut result = None;
    for statement in Parser::parse_sql(&AlopexDialect, sql).map_err(|error| error.to_string())? {
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .map_err(|error| error.to_string())?;
        if let ExecutionResult::Query(query) =
            executor.execute(plan).map_err(|error| error.to_string())?
        {
            result = Some(query);
        }
    }
    Ok(result)
}

#[test]
fn issue577_memory_policy_reaches_read_parquet() {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let values = ["a".repeat(128), "b".repeat(128)];
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            values[0].as_str(),
            values[1].as_str(),
        ]))],
    )
    .unwrap();
    write_parquet(&file, schema, batch);

    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let catalog = MemoryCatalog::new();
    let sql = format!("SELECT * FROM read_parquet('{}')", file.path().display());
    let statement = Parser::parse_sql(&AlopexDialect, &sql).unwrap().remove(0);
    let plan = Planner::new(&catalog).plan(&statement).unwrap();
    let mut txn = bridge.begin_write().unwrap();
    let sufficient = MemoryPolicy::new(Some(4096), SpillPolicy::FailFast);
    let result =
        execute_query_with_policy(&mut txn, &catalog, plan.clone(), Some(&sufficient)).unwrap();
    let ExecutionResult::Query(result) = result else {
        panic!("expected a query result with sufficient memory");
    };
    assert_eq!(
        result.rows,
        values
            .iter()
            .map(|value| vec![SqlValue::Text(value.clone())])
            .collect::<Vec<_>>()
    );

    let insufficient = MemoryPolicy::new(Some(64), SpillPolicy::FailFast);
    let error = execute_query_with_policy(&mut txn, &catalog, plan, Some(&insufficient))
        .expect_err("READ_PARQUET must apply the supplied FailFast memory policy");
    assert!(
        matches!(error, ExecutorError::ResourceExhausted { .. }),
        "expected the memory-policy error, got {error:?}"
    );
}

#[test]
fn issue577_runtime_schema_change_rejected() {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    write_parquet(&file, schema, batch);

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let sql = format!("SELECT * FROM read_parquet('{}')", file.path().display());
    let statement = Parser::parse_sql(&AlopexDialect, &sql).unwrap().remove(0);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let ExecutionResult::Query(before) = executor.execute(plan.clone()).unwrap() else {
        panic!("expected the unchanged file to produce a query result");
    };
    assert_eq!(
        before.rows,
        vec![vec![SqlValue::BigInt(1)], vec![SqlValue::BigInt(2)]]
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("extra", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(Int64Array::from(vec![30, 40])),
        ],
    )
    .unwrap();
    write_parquet(&file, schema, batch);

    let error = executor
        .execute(plan)
        .expect_err("execution must reject a file with more columns than the planned schema");
    assert!(
        matches!(
            error,
            ExecutorError::SchemaMismatch {
                expected: 1,
                actual: 2,
                ..
            }
        ),
        "expected a precise schema mismatch, got {error:?}"
    );
}

#[test]
fn issue577_runtime_type_change_rejected() {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    write_parquet(&file, schema, batch);
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let sql = format!("SELECT * FROM read_parquet('{}')", file.path().display());
    let statement = Parser::parse_sql(&AlopexDialect, &sql).unwrap().remove(0);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let ExecutionResult::Query(before) = executor.execute(plan.clone()).unwrap() else {
        panic!("expected a query result before changing the file type");
    };
    assert_eq!(
        before.rows,
        vec![vec![SqlValue::BigInt(1)], vec![SqlValue::BigInt(2)]]
    );

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["changed", "type"]))],
    )
    .unwrap();
    write_parquet(&file, schema, batch);
    let error = executor
        .execute(plan)
        .expect_err("execution must reject a same-width file whose planned type changed");
    assert!(
        matches!(
            error,
            ExecutorError::SchemaMismatch {
                expected: 1,
                actual: 1,
                ..
            }
        ),
        "expected a schema type mismatch, got {error:?}"
    );
}

#[test]
fn issue577_column_alias_preserves_read_parquet_values() {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7, 8]))],
    )
    .unwrap();
    write_parquet(&file, schema, batch);
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let result = run(
        &mut executor,
        &catalog,
        &format!(
            "SELECT renamed FROM read_parquet('{}') AS p(renamed)",
            file.path().display()
        ),
    )
    .unwrap()
    .unwrap();
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].name, "renamed");
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::BigInt(7)], vec![SqlValue::BigInt(8)]]
    );
}

#[test]
fn read_parquet_and_copy_accept_pandas_shaped_columns() {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::LargeUtf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(LargeStringArray::from(vec!["one", "two"])),
        ],
    )
    .unwrap();
    write_parquet(&file, schema, batch);

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let path = file.path().display();
    let selected = run(
        &mut executor,
        &catalog,
        &format!(
            "CREATE TABLE copied (id INTEGER PRIMARY KEY, body TEXT);
             COPY copied FROM '{path}' WITH (FORMAT PARQUET);
             CREATE TABLE selected (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO selected SELECT * FROM read_parquet('{path}');
             SELECT id, body FROM selected ORDER BY id"
        ),
    )
    .unwrap()
    .unwrap();

    let copied = run(
        &mut executor,
        &catalog,
        "SELECT id, body FROM copied ORDER BY id",
    )
    .unwrap()
    .unwrap();
    let expected = vec![
        vec![SqlValue::Integer(1), SqlValue::Text("one".into())],
        vec![SqlValue::Integer(2), SqlValue::Text("two".into())],
    ];
    assert_eq!(selected.rows, expected);
    assert_eq!(copied.rows, expected);
}

#[test]
fn read_parquet_maps_large_binary_to_blob() {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let payload = [0x00, 0xff, b'A'];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::LargeBinary,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(LargeBinaryArray::from(vec![payload.as_slice()]))],
    )
    .unwrap();
    write_parquet(&file, schema, batch);

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let result = run(
        &mut executor,
        &catalog,
        &format!(
            "SELECT payload FROM read_parquet('{}')",
            file.path().display()
        ),
    )
    .unwrap()
    .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Blob(payload.to_vec())]]);
}

#[test]
fn copy_reports_the_source_row_for_out_of_range_int64() {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut ids = vec![1_i64; 1024];
    ids.push(i64::from(i32::MAX) + 1);
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(ids))]).unwrap();
    write_parquet(&file, schema, batch);

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let path = file.path().display();
    let error = run(
        &mut executor,
        &catalog,
        &format!(
            "CREATE TABLE copied (id INTEGER); COPY copied FROM '{path}' WITH (FORMAT PARQUET)"
        ),
    )
    .unwrap_err();

    assert!(
        error.contains("row 1025: cannot convert INT64 value 2147483648 to INTEGER"),
        "{error}"
    );
}
