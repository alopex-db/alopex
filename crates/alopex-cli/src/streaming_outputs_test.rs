use crate::batch::{BatchMode, BatchModeSource};
use crate::cli::{ColumnarCommand, SqlCommand, VectorCommand};
use crate::commands::{columnar, sql, vector};
use crate::output::csv::CsvFormatter;
use crate::output::formatter::Formatter;
use crate::output::jsonl::JsonlFormatter;
use crate::output::tsv::TsvFormatter;
use crate::streaming::StreamingWriter;
use alopex_core::columnar::encoding::{Column as ColumnData, LogicalType};
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema};
use alopex_core::HnswConfig;
use alopex_embedded::{Database, TxnMode};

fn default_batch_mode() -> BatchMode {
    BatchMode {
        is_batch: false,
        is_tty: true,
        source: BatchModeSource::Default,
    }
}

fn non_empty_lines(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect()
}

fn assert_jsonl_rows(output: &str, expected_rows: usize, expected_keys: &[&str]) {
    let lines = non_empty_lines(output);
    assert_eq!(lines.len(), expected_rows);
    for line in lines {
        let value: serde_json::Value = serde_json::from_str(line).expect("jsonl line");
        for key in expected_keys {
            assert!(value.get(*key).is_some());
        }
    }
}

fn assert_delimited_rows(
    output: &str,
    expected_rows: usize,
    expected_headers: &[&str],
    delimiter: char,
) {
    let lines = non_empty_lines(output);
    assert_eq!(lines.len(), expected_rows + 1);
    let header = lines.first().expect("header line");
    for expected in expected_headers {
        assert!(header.contains(expected));
    }
    assert!(header.contains(delimiter));
}

fn setup_sql_data(db: &Database) {
    db.execute_sql("CREATE TABLE stream_test (id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    db.execute_sql("INSERT INTO stream_test (id, name) VALUES (1, 'Alice');")
        .unwrap();
    db.execute_sql("INSERT INTO stream_test (id, name) VALUES (2, 'Bob');")
        .unwrap();
}

fn run_sql_with_formatter(db: &Database, formatter: Box<dyn Formatter>) -> String {
    let cmd = SqlCommand {
        query: Some("SELECT * FROM stream_test;".to_string()),
        file: None,
    };
    let mut output = Vec::new();
    sql::execute_with_formatter(
        db,
        cmd,
        &default_batch_mode(),
        &mut output,
        formatter,
        None,
        true,
    )
    .unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn sql_streaming_outputs() {
    let db = Database::open_in_memory().unwrap();
    setup_sql_data(&db);

    let output = run_sql_with_formatter(&db, Box::new(JsonlFormatter::new()));
    assert_jsonl_rows(&output, 2, &["id", "name"]);

    let output = run_sql_with_formatter(&db, Box::new(CsvFormatter::new()));
    assert_delimited_rows(&output, 2, &["id", "name"], ',');

    let output = run_sql_with_formatter(&db, Box::new(TsvFormatter::new()));
    assert_delimited_rows(&output, 2, &["id", "name"], '\t');
}

fn setup_columnar_segment(db: &Database) -> String {
    let schema = Schema {
        columns: vec![
            ColumnSchema {
                name: "id".to_string(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
            ColumnSchema {
                name: "score".to_string(),
                logical_type: LogicalType::Float64,
                nullable: false,
                fixed_len: None,
            },
        ],
    };
    let columns = vec![
        ColumnData::Int64(vec![1, 2]),
        ColumnData::Float64(vec![1.25, 2.5]),
    ];
    let batch = RecordBatch::new(schema, columns, vec![None, None]);
    let seg_id = db.write_columnar_segment("stream_table", batch).unwrap();
    let table_id = db.resolve_table_id("stream_table").unwrap();
    format!("{}:{}", table_id, seg_id)
}

fn run_columnar_scan(db: &Database, formatter: Box<dyn Formatter>, segment: &str) -> String {
    let cmd = ColumnarCommand::Scan {
        segment: segment.to_string(),
        progress: false,
    };
    let mut output = Vec::new();
    columnar::execute_with_formatter(
        db,
        cmd,
        &default_batch_mode(),
        &mut output,
        formatter,
        None,
        true,
    )
    .unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn columnar_scan_streaming_outputs() {
    let db = Database::open_in_memory().unwrap();
    let segment = setup_columnar_segment(&db);

    let output = run_columnar_scan(&db, Box::new(JsonlFormatter::new()), &segment);
    assert_jsonl_rows(&output, 2, &["column1", "column2"]);

    let output = run_columnar_scan(&db, Box::new(CsvFormatter::new()), &segment);
    assert_delimited_rows(&output, 2, &["column1", "column2"], ',');

    let output = run_columnar_scan(&db, Box::new(TsvFormatter::new()), &segment);
    assert_delimited_rows(&output, 2, &["column1", "column2"], '\t');
}

fn setup_vector_index(db: &Database) {
    let config = HnswConfig::default().with_dimension(3);
    db.create_hnsw_index("vec_stream", config).unwrap();

    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    txn.upsert_to_hnsw("vec_stream", b"vec1", &[1.0, 2.0, 3.0], b"")
        .unwrap();
    txn.upsert_to_hnsw("vec_stream", b"vec2", &[4.0, 5.0, 6.0], b"")
        .unwrap();
    txn.commit().unwrap();
}

fn run_vector_search(db: &Database, formatter: Box<dyn Formatter>) -> String {
    let cmd = VectorCommand::Search {
        index: "vec_stream".to_string(),
        query: "[1.0,2.0,3.0]".to_string(),
        k: 2,
        progress: false,
    };
    let mut output = Vec::new();
    let columns = vector::vector_search_columns();
    let mut writer = StreamingWriter::new(&mut output, formatter, columns, None);
    vector::execute(db, cmd, &default_batch_mode(), &mut writer).unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn vector_search_streaming_outputs() {
    let db = Database::open_in_memory().unwrap();
    setup_vector_index(&db);

    let output = run_vector_search(&db, Box::new(JsonlFormatter::new()));
    assert_jsonl_rows(&output, 2, &["id", "distance"]);

    let output = run_vector_search(&db, Box::new(CsvFormatter::new()));
    assert_delimited_rows(&output, 2, &["id", "distance"], ',');

    let output = run_vector_search(&db, Box::new(TsvFormatter::new()));
    assert_delimited_rows(&output, 2, &["id", "distance"], '\t');
}
