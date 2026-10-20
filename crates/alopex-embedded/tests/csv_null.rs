use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};
use std::io::Cursor;

// Every scenario uses one NULL and one TEXT row, independently for each public path.
fn roundtrip(value: &str) {
    for route in ["file", "query", "stream"] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.alopex");
        let destination = dir.path().join("destination.alopex");
        let csv = dir.path().join("rows.csv");
        let ddl = "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)";
        {
            let db = Database::open(&source).unwrap();
            db.execute_sql(ddl).unwrap();
            db.execute_sql(&format!(
                "INSERT INTO notes VALUES (1, NULL), (2, '{}')",
                value.replace('\'', "''")
            ))
            .unwrap();
        }
        let bytes = {
            let db = Database::open(&source).unwrap();
            if route == "stream" {
                let mut output = Vec::new();
                assert_eq!(
                    db.copy_to_csv_writer("notes", &mut output, true).unwrap(),
                    ExecutionResult::RowsAffected(2)
                );
                output
            } else {
                let table = if route == "query" {
                    "(SELECT id, body FROM notes ORDER BY id)"
                } else {
                    "notes"
                };
                assert_eq!(
                    db.execute_sql(&format!(
                        "COPY {table} TO '{}' WITH (FORMAT CSV, HEADER TRUE)",
                        csv.display()
                    ))
                    .unwrap(),
                    ExecutionResult::RowsAffected(2)
                );
                std::fs::read(&csv).unwrap()
            }
        };
        {
            let db = Database::open(&destination).unwrap();
            db.execute_sql(ddl).unwrap();
            if route == "stream" {
                assert_eq!(
                    db.copy_from_csv_reader("notes", Cursor::new(bytes), true)
                        .unwrap(),
                    ExecutionResult::RowsAffected(2)
                );
            } else {
                assert_eq!(
                    db.execute_sql(&format!(
                        "COPY notes FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
                        csv.display()
                    ))
                    .unwrap(),
                    ExecutionResult::RowsAffected(2)
                );
            }
        }
        let db = Database::open(&destination).unwrap();
        let ExecutionResult::Query(result) = db
            .execute_sql("SELECT id, body FROM notes ORDER BY id")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Integer(1), SqlValue::Null],
                vec![SqlValue::Integer(2), SqlValue::Text(value.into())],
            ],
            "route={route}, text={value:?}"
        );
    }
}

macro_rules! text_case {
    ($name:ident, $value:expr) => {
        #[test]
        fn $name() {
            roundtrip($value);
        }
    };
}

text_case!(empty_text, "");
text_case!(null_marker_text, r"\N");
text_case!(null_word_text, "NULL");
text_case!(mixed_case_null_word_text, " nUlL ");
text_case!(comma_text, "one,two");
text_case!(quotes_text, "one\"two'三");
text_case!(multiline_text, "one\r\ntwo\n三");
text_case!(unicode_text, "日本語🙂");
text_case!(whitespace_text, " \ttext \t");
text_case!(backslashes_text, r"\\N\path");

#[test]
fn legacy_csv_empty_text_and_null_word_remain_distinct() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, n INT)")
        .unwrap();
    db.copy_from_csv_reader("notes", Cursor::new(b"1,,NULL\n2,NULL,7\n".to_vec()), false)
        .unwrap();
    let ExecutionResult::Query(result) = db
        .execute_sql("SELECT body, n FROM notes ORDER BY id")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Text("".into()), SqlValue::Null],
            vec![SqlValue::Null, SqlValue::Integer(7)],
        ]
    );
}
