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
