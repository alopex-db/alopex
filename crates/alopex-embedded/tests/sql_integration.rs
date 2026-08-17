use alopex_embedded::{Database, Error, TxnMode};
use alopex_sql::ExecutionResult;
use alopex_sql::SqlValue;

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_cte_column_name_list_is_public_schema() {
    let db = Database::new();
    let result = db
        .execute_sql(
            "WITH renamed(identifier, label) AS (SELECT 7, 'seven') \
             SELECT label, identifier FROM renamed;",
        )
        .unwrap();

    match result {
        ExecutionResult::Query(query) => {
            assert_eq!(query.columns[0].name, "label");
            assert_eq!(query.columns[1].name, "identifier");
            assert_eq!(
                query.rows,
                vec![vec![SqlValue::Text("seven".into()), SqlValue::Integer(7),]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_lag_and_lead_preserve_exact_rows() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, region TEXT, value INTEGER); \
         INSERT INTO samples VALUES \
             (1, 'east', 10), (2, 'east', 20), \
             (3, 'west', 30), (4, 'west', 40);",
    )
    .unwrap();

    let result = db
        .execute_sql(
            "SELECT id, \
                    LAG(value, 1, -1) OVER (PARTITION BY region ORDER BY id) AS previous, \
                    LEAD(value) OVER (PARTITION BY region ORDER BY id) AS following, \
                    value - LAG(value, 1, value) \
                        OVER (PARTITION BY region ORDER BY id) AS delta \
             FROM samples ORDER BY id;",
        )
        .unwrap();

    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(-1),
                SqlValue::Integer(20),
                SqlValue::Integer(0),
            ],
            vec![
                SqlValue::Integer(2),
                SqlValue::Integer(10),
                SqlValue::Null,
                SqlValue::Integer(10),
            ],
            vec![
                SqlValue::Integer(3),
                SqlValue::Integer(-1),
                SqlValue::Integer(40),
                SqlValue::Integer(0),
            ],
            vec![
                SqlValue::Integer(4),
                SqlValue::Integer(30),
                SqlValue::Null,
                SqlValue::Integer(10),
            ],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_database_execute_sql_pragma_uses_store_path() {
    let db = Database::new();

    assert!(matches!(
        db.execute_sql("PRAGMA cache_size = 8;").unwrap(),
        ExecutionResult::Success
    ));
    assert!(matches!(
        db.execute_sql("PRAGMA memory_limit = '64MiB';").unwrap(),
        ExecutionResult::Success
    ));

    match db.execute_sql("PRAGMA io_stats;").unwrap() {
        ExecutionResult::Query(result) => assert_eq!(result.columns[0].name, "io_stats"),
        other => panic!("expected PRAGMA query result, got {other:?}"),
    }

    match db.execute_sql("SELECT clear_cache();").unwrap() {
        ExecutionResult::Query(result) => assert!(matches!(
            result.rows[0][0],
            SqlValue::BigInt(_) | SqlValue::Integer(_)
        )),
        other => panic!("expected system function query result, got {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_database_execute_sql_error() {
    let db = Database::new();
    let err = db
        .execute_sql("INSERT INTO missing (id) VALUES (1);")
        .unwrap_err();
    assert!(matches!(err, Error::Sql(_)));
    assert_eq!(err.sql_error_code(), Some("ALOPEX-C001"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_readonly_txn_rejects_ddl() {
    let db = Database::new();
    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let err = ro
        .execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY);")
        .unwrap_err();
    assert_eq!(err.sql_error_code(), Some("ALOPEX-E002"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_execute_sql_multi_returns_result_per_statement() {
    let db = Database::new();
    let results = db
        .execute_sql_multi(
            r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');
            SELECT id FROM users ORDER BY id;
            "#,
        )
        .unwrap();

    assert_eq!(results.len(), 3, "one result per statement");
    assert!(matches!(results[0], ExecutionResult::Success));
    assert!(matches!(results[1], ExecutionResult::RowsAffected(2)));
    match &results[2] {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 2);
            assert_eq!(q.rows[0][0], SqlValue::Integer(1));
            assert_eq!(q.rows[1][0], SqlValue::Integer(2));
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_execute_sql_multi_empty_input_returns_no_results() {
    let db = Database::new();
    let results = db.execute_sql_multi("  ").unwrap();
    assert!(results.is_empty());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_execute_sql_multi_error_rolls_back_whole_batch() {
    let db = Database::new();
    let err = db.execute_sql_multi(
        "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO missing (id) VALUES (1);",
    );
    assert!(err.is_err(), "failing statement must abort the batch");

    // The CREATE TABLE from the failed batch must have been rolled back.
    assert!(
        db.execute_sql("SELECT id FROM t;").is_err(),
        "table t should not exist after rollback"
    );
}
