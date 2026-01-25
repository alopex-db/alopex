use alopex_embedded::{Database, Error, TxnMode};
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

#[test]
fn sql_group_by_having_aggregates() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE products (
            category TEXT,
            price DOUBLE,
            label TEXT
        );
        INSERT INTO products (category, price, label) VALUES
            ('book', 10.0, 'a'),
            ('book', 15.0, 'b'),
            ('book', NULL, 'a'),
            ('game', 20.0, 'c'),
            ('game', NULL, NULL),
            ('toy', 3.0, 'c');
        "#,
    )
    .unwrap();

    let result = db
        .execute_sql(
            "SELECT category, COUNT(*), COUNT(DISTINCT label), SUM(price), AVG(price) \
             FROM products GROUP BY category HAVING COUNT(*) > 1 ORDER BY category;",
        )
        .unwrap();
    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 2);
            assert_eq!(q.rows[0][0], SqlValue::Text("book".into()));
            assert_eq!(q.rows[0][1], SqlValue::BigInt(3));
            assert_eq!(q.rows[0][2], SqlValue::BigInt(2));
            assert_eq!(q.rows[0][3], SqlValue::Double(25.0));
            match &q.rows[0][4] {
                SqlValue::Double(value) => assert!((*value - 12.5).abs() < 1e-6),
                other => panic!("unexpected avg {other:?}"),
            }

            assert_eq!(q.rows[1][0], SqlValue::Text("game".into()));
            assert_eq!(q.rows[1][1], SqlValue::BigInt(2));
            assert_eq!(q.rows[1][2], SqlValue::BigInt(1));
            assert_eq!(q.rows[1][3], SqlValue::Double(20.0));
            assert_eq!(q.rows[1][4], SqlValue::Double(20.0));
        }
        other => panic!("expected query result, got {other:?}"),
    }

    let result = db
        .execute_sql(
            "SELECT category, GROUP_CONCAT(label, '|') FROM products \
             GROUP BY category HAVING COUNT(*) > 1 ORDER BY category;",
        )
        .unwrap();
    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 2);
            assert_eq!(q.rows[0][0], SqlValue::Text("book".into()));
            assert_eq!(q.rows[0][1], SqlValue::Text("a|b|a".into()));
            assert_eq!(q.rows[1][0], SqlValue::Text("game".into()));
            assert_eq!(q.rows[1][1], SqlValue::Text("c".into()));
        }
        other => panic!("expected query result, got {other:?}"),
    }

    let result = db
        .execute_sql(
            "SELECT category, STRING_AGG(label, ';') FROM products \
             GROUP BY category HAVING COUNT(*) > 1 ORDER BY category;",
        )
        .unwrap();
    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 2);
            assert_eq!(q.rows[0][0], SqlValue::Text("book".into()));
            assert_eq!(q.rows[0][1], SqlValue::Text("a;b;a".into()));
            assert_eq!(q.rows[1][0], SqlValue::Text("game".into()));
            assert_eq!(q.rows[1][1], SqlValue::Text("c".into()));
        }
        other => panic!("expected query result, got {other:?}"),
    }

    let result = db
        .execute_sql(
            "SELECT category, MIN(price), MAX(price), TOTAL(price) FROM products \
             GROUP BY category HAVING COUNT(*) > 1 ORDER BY category;",
        )
        .unwrap();
    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 2);
            assert_eq!(q.rows[0][0], SqlValue::Text("book".into()));
            assert_eq!(q.rows[0][1], SqlValue::Double(10.0));
            assert_eq!(q.rows[0][2], SqlValue::Double(15.0));
            assert_eq!(q.rows[0][3], SqlValue::Double(25.0));

            assert_eq!(q.rows[1][0], SqlValue::Text("game".into()));
            assert_eq!(q.rows[1][1], SqlValue::Double(20.0));
            assert_eq!(q.rows[1][2], SqlValue::Double(20.0));
            assert_eq!(q.rows[1][3], SqlValue::Double(20.0));
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn sql_invalid_having_surface_sql_error() {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, group_id INTEGER);
        INSERT INTO users (id, name, group_id) VALUES (1, 'alice', 10);
        "#,
    )
    .unwrap();

    let err = db
        .execute_sql("SELECT u.group_id FROM users u GROUP BY u.group_id HAVING missing > 1;")
        .unwrap_err();
    assert!(matches!(err, Error::Sql(_)));
}
