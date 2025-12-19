use alopex_embedded::{Database, TxnMode};
use alopex_sql::ExecutionResult;
use alopex_sql::SqlValue;

#[test]
fn full_sql_workflow() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e2e.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute_sql(
            r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'alice');
            "#,
        )
        .unwrap();
        db.flush().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
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
}

#[test]
fn mixed_kv_sql_transaction() {
    let db = Database::new();

    {
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(b"kv", b"v1").unwrap();
        txn.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();
        txn.execute_sql("INSERT INTO t (id) VALUES (1);").unwrap();
        txn.commit().unwrap();
    }

    {
        let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(b"kv").unwrap(), Some(b"v1".to_vec()));
    }

    let result = db.execute_sql("SELECT id FROM t;").unwrap();
    match result {
        ExecutionResult::Query(q) => assert_eq!(q.rows.len(), 1),
        other => panic!("expected query result, got {other:?}"),
    }
}
