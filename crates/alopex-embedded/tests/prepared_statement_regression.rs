use alopex_embedded::Database;
use alopex_sql::storage::SqlValue;
use alopex_sql::ExecutionResult;

#[test]
fn prepared_statement_bind_execute_and_rebind() {
    let db = Database::open_in_memory().expect("db");
    db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        .expect("create");
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');")
        .expect("seed");

    let mut stmt = db
        .prepare("SELECT name FROM users WHERE id = ? LIMIT ? OFFSET ?")
        .expect("prepare");
    stmt.bind(1, 1_i64).expect("bind id");
    stmt.bind(2, 1_i64).expect("bind limit");
    stmt.bind(3, 0_i64).expect("bind offset");
    let result = stmt.execute().expect("execute");
    let ExecutionResult::Query(query) = result else {
        panic!("expected query");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Text("alice".to_string())]]);

    stmt.bind(1, 2_i64).expect("rebind id");
    let result = stmt.execute().expect("execute after rebind");
    let ExecutionResult::Query(query) = result else {
        panic!("expected query");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Text("bob".to_string())]]);
}

#[test]
fn prepared_statement_ignores_placeholders_inside_quotes_and_comments() {
    let db = Database::open_in_memory().expect("db");
    let mut stmt = db
        .prepare("SELECT '?' AS literal, ? AS value -- ? in comment\n")
        .expect("prepare");
    assert_eq!(stmt.placeholder_count(), 1);
    stmt.bind(1, 7_i64).expect("bind");
    let result = stmt.execute().expect("execute");
    let ExecutionResult::Query(query) = result else {
        panic!("expected query");
    };
    assert_eq!(query.rows.len(), 1);
    assert_eq!(query.rows[0][0], SqlValue::Text("?".to_string()));
    assert!(matches!(
        query.rows[0][1],
        SqlValue::Integer(7) | SqlValue::BigInt(7)
    ));
}

#[test]
fn prepared_statement_requires_all_placeholders_to_be_bound() {
    let db = Database::open_in_memory().expect("db");
    let mut stmt = db.prepare("SELECT ?, ?").expect("prepare");
    stmt.bind(1, 1_i64).expect("bind one");
    let err = stmt.execute().expect_err("missing bind must fail");
    assert!(err
        .to_string()
        .contains("missing bind value for parameter 2"));
}

#[test]
fn prepared_statement_reset_requires_rebind() {
    let db = Database::open_in_memory().expect("db");
    let mut stmt = db.prepare("SELECT ?").expect("prepare");
    stmt.bind(1, "alice").expect("bind");
    stmt.reset().expect("reset");
    let err = stmt.execute().expect_err("execute after reset");
    assert!(err
        .to_string()
        .contains("missing bind value for parameter 1"));
}

#[test]
fn prepared_statement_finalize_prevents_reuse() {
    let db = Database::open_in_memory().expect("db");
    let mut stmt = db.prepare("SELECT ?").expect("prepare");
    stmt.bind_null(1).expect("bind null");
    stmt.finalize();
    let err = stmt.execute().expect_err("finalized statement");
    assert!(err.to_string().contains("prepared statement is finalized"));
}

#[test]
fn prepared_statement_rejects_dollar_and_named_parameters() {
    let db = Database::open_in_memory().expect("db");
    let err = db.prepare("SELECT $1").expect_err("must reject $1");
    assert!(err.to_string().contains("positional '$n' parameters"));

    let err = db.prepare("SELECT :id").expect_err("must reject named");
    assert!(err
        .to_string()
        .contains("named parameters are not supported"));
}
