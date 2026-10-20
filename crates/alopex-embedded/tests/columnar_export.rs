use alopex_core::columnar::kvs_bridge::key_layout::PREFIX_COLUMN_SEGMENT;
use alopex_embedded::{Database, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};
use std::io::{self, Cursor, Write};

fn ddl(storage: &str) -> String {
    format!("CREATE TABLE items (id INT PRIMARY KEY, note TEXT, score DOUBLE, active BOOLEAN, n BIGINT) WITH (storage='{storage}')")
}

fn seed(db: &Database) {
    // Two one-row COPY calls create two independent columnar segments.
    for csv in [
        "1,\\N,1.5,\\N,9223372036854775807\n",
        "2,\"\",\\N,false,-9223372036854775807\n",
    ] {
        assert_eq!(
            db.copy_from_csv_reader("items", Cursor::new(csv.as_bytes().to_vec()), false)
                .unwrap(),
            ExecutionResult::RowsAffected(1)
        );
    }
}

fn rows(db: &Database) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = db
        .execute_sql("SELECT id, note, score, active, n FROM items ORDER BY id")
        .unwrap()
    else {
        panic!("expected query")
    };
    result.rows
}

fn expected() -> Vec<Vec<SqlValue>> {
    vec![
        vec![
            SqlValue::Integer(1),
            SqlValue::Null,
            SqlValue::Double(1.5),
            SqlValue::Null,
            SqlValue::BigInt(i64::MAX),
        ],
        vec![
            SqlValue::Integer(2),
            SqlValue::Text(String::new()),
            SqlValue::Null,
            SqlValue::Boolean(false),
            SqlValue::BigInt(-i64::MAX),
        ],
    ]
}

fn roundtrip(storage: &str, route: &str, empty: bool) {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.db");
    let destination = dir.path().join("destination.db");
    let output = dir.path().join("export");
    let expected = if empty { vec![] } else { expected() };
    let count = expected.len() as u64;
    {
        let db = Database::open(&source).unwrap();
        db.execute_sql(&ddl(storage)).unwrap();
        if !empty {
            seed(&db);
            if storage == "columnar" {
                let mut txn = db.begin(TxnMode::ReadOnly).unwrap();
                assert_eq!(
                    txn.scan_prefix(&[PREFIX_COLUMN_SEGMENT]).unwrap().count(),
                    2
                );
            }
        }
    }
    let mut bytes = Vec::new();
    {
        let db = Database::open(&source).unwrap();
        assert_eq!(rows(&db), expected);
        let result = if route == "stream" {
            db.copy_to_csv_writer("items", &mut bytes, true).unwrap()
        } else {
            // Successful exports must replace existing output, including empty exports.
            std::fs::write(&output, b"previous output").unwrap();
            db.execute_sql(&format!(
                "COPY items TO '{}' WITH (FORMAT {route}, HEADER TRUE)",
                output.display()
            ))
            .unwrap()
        };
        assert_eq!(result, ExecutionResult::RowsAffected(count));
        assert_no_temps(dir.path());
    }
    {
        let db = Database::open(&destination).unwrap();
        db.execute_sql(&ddl(storage)).unwrap();
        let result = if route == "stream" {
            db.copy_from_csv_reader("items", Cursor::new(bytes), true)
                .unwrap()
        } else {
            db.execute_sql(&format!(
                "COPY items FROM '{}' WITH (FORMAT {route}, HEADER TRUE)",
                output.display()
            ))
            .unwrap()
        };
        assert_eq!(result, ExecutionResult::RowsAffected(count));
    }
    let db = Database::open(&destination).unwrap();
    assert_eq!(rows(&db), expected);
}

fn assert_no_temps(dir: &std::path::Path) {
    assert!(!std::fs::read_dir(dir).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains(".alopex-copy-")));
}

macro_rules! export_case {
    ($name:ident, $storage:literal, $route:literal, $empty:literal) => {
        #[test]
        fn $name() {
            roundtrip($storage, $route, $empty);
        }
    };
}
export_case!(
    columnar_parquet_preserves_two_segments,
    "columnar",
    "PARQUET",
    false
);
export_case!(
    columnar_csv_preserves_two_segments,
    "columnar",
    "CSV",
    false
);
export_case!(
    columnar_writer_preserves_two_segments,
    "columnar",
    "stream",
    false
);
export_case!(row_parquet_preserves_values, "row", "PARQUET", false);
export_case!(empty_columnar_parquet, "columnar", "PARQUET", true);
export_case!(empty_columnar_csv, "columnar", "CSV", true);
export_case!(empty_columnar_writer, "columnar", "stream", true);

fn failed_rename(format: &str) {
    let db = Database::new();
    db.execute_sql(&ddl("columnar")).unwrap();
    seed(&db);
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("output");
    std::fs::create_dir(&output).unwrap();
    std::fs::write(output.join("sentinel"), b"keep").unwrap();
    assert!(db
        .execute_sql(&format!(
            "COPY items TO '{}' WITH (FORMAT {format})",
            output.display()
        ))
        .is_err());
    assert_eq!(std::fs::read(output.join("sentinel")).unwrap(), b"keep");
    assert_no_temps(dir.path());
}
#[test]
fn parquet_failed_rename_cleans_temporary_output() {
    failed_rename("PARQUET");
}
#[test]
fn csv_failed_rename_cleans_temporary_output() {
    failed_rename("CSV");
}

fn missing_segment(format: &str) {
    let db = Database::new();
    db.execute_sql(&ddl("columnar")).unwrap();
    seed(&db);
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
    let key = txn
        .scan_prefix(&[PREFIX_COLUMN_SEGMENT])
        .unwrap()
        .next()
        .unwrap()
        .0;
    txn.delete(&key).unwrap();
    txn.commit().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("output");
    std::fs::write(&output, b"keep").unwrap();
    assert!(db
        .execute_sql(&format!(
            "COPY items TO '{}' WITH (FORMAT {format})",
            output.display()
        ))
        .is_err());
    assert_eq!(std::fs::read(&output).unwrap(), b"keep");
    assert_no_temps(dir.path());
}
#[test]
fn parquet_missing_segment_preserves_destination() {
    missing_segment("PARQUET");
}
#[test]
fn csv_missing_segment_preserves_destination() {
    missing_segment("CSV");
}

struct FailingWriter;
impl Write for FailingWriter {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("injected writer failure"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[test]
fn columnar_writer_failure_is_not_success() {
    let db = Database::new();
    db.execute_sql(&ddl("columnar")).unwrap();
    seed(&db);
    let error = db
        .copy_to_csv_writer("items", &mut FailingWriter, false)
        .unwrap_err();
    assert!(error.to_string().contains("injected writer failure"));
    assert_eq!(rows(&db), expected());
}
