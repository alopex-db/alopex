use std::fs::File;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;
use arrow_array::{Int64Array, LargeStringArray, RecordBatch};
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
