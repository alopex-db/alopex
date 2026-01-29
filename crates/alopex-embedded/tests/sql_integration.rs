use alopex_embedded::{Database, Error, TxnMode};
use alopex_sql::ExecutionResult;
use alopex_sql::SqlValue;

#[test]
fn sql_integration_database_execute_sql_ddl() {
    let db = Database::new();
    let result = db
        .execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    assert!(matches!(result, ExecutionResult::Success));

    let result = db.execute_sql("SELECT id, name FROM users;").unwrap();
    match result {
        ExecutionResult::Query(q) => assert!(q.rows.is_empty()),
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn sql_integration_database_execute_sql_dml() {
    let db = Database::new();
    let result = db
        .execute_sql(
            r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'alice');
            "#,
        )
        .unwrap();
    assert!(matches!(result, ExecutionResult::RowsAffected(1)));

    let result = db
        .execute_sql("SELECT id, name FROM users ORDER BY id;")
        .unwrap();
    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 1);
            assert_eq!(q.rows[0][0], SqlValue::Integer(1));
            assert_eq!(q.rows[0][1], SqlValue::Text("alice".into()));
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn sql_integration_database_execute_sql_query() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
        "#,
    )
    .unwrap();

    let result = db
        .execute_sql("SELECT name FROM users WHERE id = 2;")
        .unwrap();
    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 1);
            assert_eq!(q.rows[0][0], SqlValue::Text("bob".into()));
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn sql_integration_database_execute_sql_error() {
    let db = Database::new();
    let err = db
        .execute_sql("INSERT INTO missing (id) VALUES (1);")
        .unwrap_err();
    assert!(matches!(err, Error::Sql(_)));
    assert_eq!(err.sql_error_code(), Some("ALOPEX-C001"));
}

#[test]
fn sql_integration_transaction_execute_sql_shares_kv_changes() {
    let db = Database::new();
    {
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"custom", b"v1").unwrap();
        txn.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();
        txn.commit().unwrap();
    }

    {
        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(b"custom").unwrap(), Some(b"v1".to_vec()));
    }

    db.execute_sql("SELECT id FROM t;").unwrap();
}

#[test]
fn sql_integration_transaction_rollback_discards_sql_changes() {
    let db = Database::new();
    {
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();
        txn.rollback().unwrap();
    }

    let err = db.execute_sql("SELECT id FROM t;").unwrap_err();
    assert_eq!(err.sql_error_code(), Some("ALOPEX-C001"));
}

#[test]
fn sql_integration_readonly_txn_rejects_ddl() {
    let db = Database::new();
    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let err = ro
        .execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
        .unwrap_err();
    assert_eq!(err.sql_error_code(), Some("ALOPEX-E002"));
}

#[test]
fn sql_integration_readonly_txn_rejects_dml() {
    let db = Database::new();
    db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
        .unwrap();

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let err = ro
        .execute_sql("INSERT INTO t (id) VALUES (1);")
        .unwrap_err();
    assert_eq!(err.sql_error_code(), Some("ALOPEX-E002"));
}

#[test]
fn sql_integration_readonly_txn_allows_select() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE t (id INTEGER PRIMARY KEY);
        INSERT INTO t (id) VALUES (1);
        "#,
    )
    .unwrap();

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let result = ro.execute_sql("SELECT id FROM t;").unwrap();
    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 1);
            assert_eq!(q.rows[0][0], SqlValue::Integer(1));
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn sql_integration_multiple_execute_sql_in_same_txn() {
    let db = Database::new();
    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();

    txn.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
        .unwrap();
    txn.execute_sql("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b');")
        .unwrap();
    let result = txn.execute_sql("SELECT name FROM t ORDER BY id;").unwrap();

    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 2);
            assert_eq!(q.rows[0][0], SqlValue::Text("a".into()));
            assert_eq!(q.rows[1][0], SqlValue::Text("b".into()));
        }
        other => panic!("expected query result, got {other:?}"),
    }

    txn.commit().unwrap();
}

#[test]
fn sql_integration_create_then_insert_in_same_txn() {
    let db = Database::new();
    {
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();
        txn.execute_sql("INSERT INTO t (id) VALUES (1);").unwrap();
        txn.commit().unwrap();
    }

    let result = db.execute_sql("SELECT id FROM t;").unwrap();
    match result {
        ExecutionResult::Query(q) => assert_eq!(q.rows.len(), 1),
        other => panic!("expected query result, got {other:?}"),
    }
}
