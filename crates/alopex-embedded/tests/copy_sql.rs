use std::fs;
use std::fs::File;
use std::io::Write;

use alopex_embedded::Database;
use alopex_sql::ExecutionResult;

#[test]
fn copy_from_csv_is_available_through_sql() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut stream = File::create(file.path()).unwrap();
    writeln!(stream, "id,name").unwrap();
    writeln!(stream, "1,alice").unwrap();
    let sql = format!(
        "COPY users FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
        file.path().display()
    );
    assert_eq!(
        db.execute_sql(&sql).unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    assert!(matches!(
        db.execute_sql("SELECT * FROM users").unwrap(),
        ExecutionResult::Query(_)
    ));
}

#[test]
fn copy_to_csv_is_available_through_sql() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users VALUES (1, 'alice')")
        .unwrap();
    let output = tempfile::NamedTempFile::new().unwrap();
    let sql = format!(
        "COPY users TO '{}' WITH (FORMAT CSV, HEADER TRUE)",
        output.path().display()
    );
    assert_eq!(
        db.execute_sql(&sql).unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    let content = fs::read_to_string(output.path()).unwrap();
    assert!(content.contains("id,name"));
    assert!(content.contains("1,alice"));
}

#[test]
fn copy_query_to_csv_is_available_through_sql() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    let output = tempfile::NamedTempFile::new().unwrap();
    let sql = format!(
        "COPY (SELECT name, id FROM users ORDER BY id) TO '{}' WITH (FORMAT CSV, HEADER TRUE)",
        output.path().display()
    );
    assert_eq!(
        db.execute_sql(&sql).unwrap(),
        ExecutionResult::RowsAffected(2)
    );
    let content = fs::read_to_string(output.path()).unwrap();
    assert!(content.contains("name,id"));
    assert!(content.contains("alice,1"));
    assert!(content.contains("bob,2"));
}

#[test]
fn copy_to_parquet_is_available_through_sql() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, NULL)")
        .unwrap();
    let output = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let sql = format!(
        "COPY users TO '{}' WITH (FORMAT PARQUET)",
        output.path().display()
    );
    assert_eq!(
        db.execute_sql(&sql).unwrap(),
        ExecutionResult::RowsAffected(3)
    );
    let db2 = Database::new();
    db2.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    let import = format!(
        "COPY users FROM '{}' WITH (FORMAT PARQUET)",
        output.path().display()
    );
    assert_eq!(
        db2.execute_sql(&import).unwrap(),
        ExecutionResult::RowsAffected(3)
    );
    let ExecutionResult::Query(result) = db2.execute_sql("SELECT * FROM users").unwrap() else {
        panic!("expected restored query result")
    };
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn copy_query_to_parquet_is_available_through_sql() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    let output = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let sql = format!(
        "COPY (SELECT name, id FROM users ORDER BY id) TO '{}' WITH (FORMAT PARQUET)",
        output.path().display()
    );
    assert_eq!(
        db.execute_sql(&sql).unwrap(),
        ExecutionResult::RowsAffected(2)
    );
    let db2 = Database::new();
    db2.execute_sql("CREATE TABLE users (name TEXT, id INT PRIMARY KEY)")
        .unwrap();
    let import = format!(
        "COPY users FROM '{}' WITH (FORMAT PARQUET)",
        output.path().display()
    );
    assert_eq!(
        db2.execute_sql(&import).unwrap(),
        ExecutionResult::RowsAffected(2)
    );
}

#[test]
fn copy_rejects_unknown_format_without_overwriting_output() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY)")
        .unwrap();
    db.execute_sql("INSERT INTO users VALUES (1)").unwrap();
    let mut output = tempfile::NamedTempFile::new().unwrap();
    writeln!(output, "sentinel").unwrap();
    let sql = format!(
        "COPY users TO '{}' WITH (FORMAT JSON)",
        output.path().display()
    );

    let error = db.execute_sql(&sql).unwrap_err().to_string();

    assert!(error.contains("COPY FORMAT JSON"), "{error}");
    assert_eq!(fs::read_to_string(output.path()).unwrap(), "sentinel\n");
}

#[test]
fn copy_reader_writer_streams_share_csv_quoting_and_atomicity() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.copy_from_csv_reader(
        "notes",
        std::io::Cursor::new(b"id,body\n1,\"comma, quoted\"\n".to_vec()),
        true,
    )
    .unwrap();
    let mut output = Vec::new();
    db.copy_to_csv_writer("notes", &mut output, true).unwrap();
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "id,body\n1,\"comma, quoted\"\n"
    );

    assert!(db
        .copy_from_csv_reader(
            "notes",
            std::io::Cursor::new(b"2,ok\n3,too,many\n".to_vec()),
            false,
        )
        .is_err());
    let ExecutionResult::Query(rows) = db.execute_sql("SELECT id FROM notes ORDER BY id").unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows.len(), 1, "failed stream import must roll back");
}
