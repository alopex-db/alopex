use std::sync::Arc;

use alopex_embedded::{Database, Error};
use alopex_sql::{ExecutionResult, SqlValue};

#[test]
fn prepared_statement_supports_null_rebind_reset_and_finalize() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id, note) VALUES (?, ?)")
        .unwrap();
    assert_eq!(statement.parameter_count(), 2);

    statement.bind(1, SqlValue::Integer(1)).unwrap();
    statement.bind(2, SqlValue::Text("first".into())).unwrap();
    statement.bind(2, SqlValue::Text("rebound".into())).unwrap();
    statement.execute().unwrap();

    statement.reset().unwrap();
    statement.bind(1, SqlValue::Integer(2)).unwrap();
    statement.bind(2, SqlValue::Null).unwrap();
    statement.execute().unwrap();
    statement.finalize().unwrap();
    assert!(matches!(
        statement.bind(1, SqlValue::Integer(3)),
        Err(Error::PreparedStatementFinalized)
    ));
    assert!(matches!(
        statement.execute(),
        Err(Error::PreparedStatementFinalized)
    ));

    let ExecutionResult::Query(rows) = database
        .execute_sql("SELECT id, note FROM items ORDER BY id")
        .unwrap()
    else {
        panic!("SELECT must return rows");
    };
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0][1], SqlValue::Text("rebound".into()));
    assert_eq!(rows.rows[1][1], SqlValue::Null);
}

#[test]
fn session_prepared_statement_uses_the_active_transaction() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut session = database.sql_session();
    session.execute_sql("BEGIN").unwrap();
    {
        let mut statement = session
            .prepare("INSERT INTO items (id) VALUES (?)")
            .unwrap();
        statement.bind(1, SqlValue::Integer(1)).unwrap();
        statement.execute().unwrap();
    }
    session.execute_sql("ROLLBACK").unwrap();

    let ExecutionResult::Query(rows) = database.execute_sql("SELECT id FROM items").unwrap() else {
        panic!("SELECT must return rows");
    };
    assert!(rows.rows.is_empty());
}

#[test]
fn prepared_parameters_work_in_limit_offset_and_fetch_expressions() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    database
        .execute_sql("INSERT INTO items VALUES (1), (2), (3)")
        .unwrap();
    let mut limit = database
        .prepare("SELECT id FROM items ORDER BY id LIMIT ? OFFSET ?")
        .unwrap();
    limit.bind(1, SqlValue::Integer(1)).unwrap();
    limit.bind(2, SqlValue::Integer(1)).unwrap();
    let ExecutionResult::Query(rows) = limit.execute().unwrap() else {
        panic!("SELECT must return rows");
    };
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(2)]]);

    let mut fetch = database
        .prepare("SELECT id FROM items ORDER BY id FETCH FIRST ? ROWS ONLY")
        .unwrap();
    fetch.bind(1, SqlValue::Integer(2)).unwrap();
    let ExecutionResult::Query(rows) = fetch.execute().unwrap() else {
        panic!("SELECT must return rows");
    };
    assert_eq!(rows.rows.len(), 2);
}

#[test]
fn prepared_statement_rejects_missing_and_non_value_parameters() {
    let database = Arc::new(Database::new());
    assert!(database.execute_sql("SELECT ?").is_err());
    assert_eq!(
        database
            .prepare("SELECT '?' AS literal /* ? */")
            .unwrap()
            .parameter_count(),
        0
    );
    assert_eq!(
        database
            .prepare("SELECT ? -- ?\n")
            .unwrap()
            .parameter_count(),
        1
    );
    let mut statement = database.prepare("SELECT ?").unwrap();
    assert!(matches!(
        statement.execute(),
        Err(Error::PreparedParameterUnbound(1))
    ));
    assert!(matches!(
        statement.bind(0, SqlValue::Integer(1)),
        Err(Error::PreparedParameterOutOfRange { index: 0, count: 1 })
    ));
    assert!(matches!(
        statement.bind(2, SqlValue::Integer(1)),
        Err(Error::PreparedParameterOutOfRange { index: 2, count: 1 })
    ));
    assert!(database.prepare("SELECT $1").is_err());
    assert!(database.prepare("SELECT :named").is_err());
    assert!(database.prepare("SELECT * FROM ?").is_err());
}

#[test]
fn prepared_statement_reparses_after_schema_change_and_can_retry() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id) VALUES (?)")
        .unwrap();
    database.execute_sql("DROP TABLE items").unwrap();
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
        .unwrap();
    statement.bind(1, SqlValue::Integer(1)).unwrap();
    assert!(statement.execute().is_err());

    database.execute_sql("DROP TABLE items").unwrap();
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    statement.execute().unwrap();
}

#[test]
fn prepared_statement_is_send_and_text_binding_cannot_change_sql_structure() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    let mut statement = database
        .prepare("INSERT INTO items (id, note) VALUES (?, ?)")
        .unwrap();
    statement.bind(1, SqlValue::Integer(1)).unwrap();
    statement
        .bind(2, SqlValue::Text("x'); DROP TABLE items; --".into()))
        .unwrap();
    std::thread::spawn(move || statement.execute())
        .join()
        .expect("prepared statement thread")
        .unwrap();
    assert!(database.execute_sql("SELECT id FROM items").is_ok());
}
