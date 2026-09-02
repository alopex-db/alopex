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
