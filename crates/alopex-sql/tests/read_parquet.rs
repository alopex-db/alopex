use std::fs::File;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::execute_query_with_policy;
use alopex_sql::executor::{
    ExecutionResult, Executor, ExecutorError, MemoryPolicy, QueryResult, SpillPolicy,
};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{SqlValue, TxnBridge};
use arrow_array::{Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

fn write_parquet(file: &tempfile::NamedTempFile, schema: Arc<Schema>, batch: RecordBatch) {
    let mut writer =
        ArrowWriter::try_new(File::create(file.path()).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
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
