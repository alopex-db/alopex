use alopex_embedded::{Database, Error, StreamingQueryResult, TxnMode};
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
fn sql_integration_recursive_cte_reaches_fixed_point() {
    let db = Database::new();
    let result = db
        .execute_sql(
            "WITH RECURSIVE counter(n) AS (\
                 SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 4\
             ) SELECT n FROM counter ORDER BY n;",
        )
        .unwrap();

    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
            vec![SqlValue::Integer(4)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_recursive_cte_resource_limit_has_stable_error_contract() {
    let db = Database::new();
    let error = db
        .execute_sql(
            "WITH RECURSIVE cycle(n) AS (\
                 SELECT 1 UNION ALL SELECT n FROM cycle\
             ) SELECT n FROM cycle;",
        )
        .expect_err("an unbounded recursive CTE must fail");

    assert_eq!(error.sql_error_code(), Some("ALOPEX-E003"));
    assert!(
        error
            .to_string()
            .contains("recursive CTE 'cycle' reached iteration limit"),
        "unexpected public error: {error}"
    );
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
fn sql_integration_rows_and_range_frames_preserve_exact_rows() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, amount INTEGER, qty INTEGER); \
         INSERT INTO samples VALUES (1, 10, 3), (2, 20, 1), (3, 20, 5), (4, 30, 2);",
    )
    .unwrap();

    let result = db
        .execute_sql(
            "SELECT id, \
                    SUM(qty) OVER (ORDER BY id \
                      ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS physical, \
                    SUM(qty) OVER (ORDER BY amount RANGE CURRENT ROW) AS peers \
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
                SqlValue::BigInt(4),
                SqlValue::BigInt(3)
            ],
            vec![
                SqlValue::Integer(2),
                SqlValue::BigInt(9),
                SqlValue::BigInt(6)
            ],
            vec![
                SqlValue::Integer(3),
                SqlValue::BigInt(8),
                SqlValue::BigInt(6)
            ],
            vec![
                SqlValue::Integer(4),
                SqlValue::BigInt(7),
                SqlValue::BigInt(2)
            ],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_value_and_distribution_windows_preserve_exact_rows() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE window_samples (id INTEGER PRIMARY KEY, amount INTEGER); \
         INSERT INTO window_samples VALUES (1, 10), (2, 20), (3, 20), (4, 30);",
    )
    .unwrap();

    let result = db
        .execute_sql(
            "SELECT id, \
                    FIRST_VALUE(id) OVER (ORDER BY amount) AS first_id, \
                    LAST_VALUE(id) OVER (ORDER BY amount) AS last_id, \
                    NTH_VALUE(id, 2) OVER (ORDER BY amount) AS second_id, \
                    NTILE(3) OVER (ORDER BY amount) AS bucket, \
                    PERCENT_RANK() OVER (ORDER BY amount) AS percent_rank, \
                    CUME_DIST() OVER (ORDER BY amount) AS cume_dist \
             FROM window_samples ORDER BY id;",
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
                SqlValue::Integer(1),
                SqlValue::Integer(1),
                SqlValue::Null,
                SqlValue::BigInt(1),
                SqlValue::Double(0.0),
                SqlValue::Double(0.25),
            ],
            vec![
                SqlValue::Integer(2),
                SqlValue::Integer(1),
                SqlValue::Integer(3),
                SqlValue::Integer(2),
                SqlValue::BigInt(1),
                SqlValue::Double(1.0 / 3.0),
                SqlValue::Double(0.75),
            ],
            vec![
                SqlValue::Integer(3),
                SqlValue::Integer(1),
                SqlValue::Integer(3),
                SqlValue::Integer(2),
                SqlValue::BigInt(2),
                SqlValue::Double(1.0 / 3.0),
                SqlValue::Double(0.75),
            ],
            vec![
                SqlValue::Integer(4),
                SqlValue::Integer(1),
                SqlValue::Integer(4),
                SqlValue::Integer(2),
                SqlValue::BigInt(3),
                SqlValue::Double(1.0),
                SqlValue::Double(1.0),
            ],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_named_windows_and_qualify_preserve_exact_rows() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE qualify_samples \
             (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER); \
         INSERT INTO qualify_samples VALUES \
             (1, 'east', 10), (2, 'east', 20), \
             (3, 'west', 30), (4, 'west', 30);",
    )
    .unwrap();

    let result = db
        .execute_sql(
            "SELECT id, ROW_NUMBER() OVER ranked AS row_number \
             FROM qualify_samples \
             WINDOW ranked AS (base ORDER BY amount DESC, id), \
                    base AS (PARTITION BY region) \
             QUALIFY row_number = 1 ORDER BY id;",
        )
        .unwrap();
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(2), SqlValue::BigInt(1)],
            vec![SqlValue::Integer(3), SqlValue::BigInt(1)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_values_query_preserves_exact_rows() {
    let db = Database::new();
    let result = db
        .execute_sql("VALUES (2, 'b'), (1, 'a') ORDER BY column1")
        .unwrap();
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
        ]
    );

    let streamed = db
        .execute_sql_with_rows("VALUES (2, 'b'), (1, 'a') ORDER BY column1", |mut rows| {
            let mut actual = Vec::new();
            while let Some(row) = rows.next_row()? {
                actual.push(row);
            }
            Ok(actual)
        })
        .unwrap();
    let StreamingQueryResult::QueryProcessed(streamed_rows) = streamed else {
        panic!("expected streaming query result");
    };
    assert_eq!(
        streamed_rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_standard_predicates_preserve_exact_values() {
    let db = Database::new();
    let result = db
        .execute_sql(
            "SELECT TRUE IS TRUE AS truth_value, \
             NULL IS DISTINCT FROM 1 AS distinct_null, \
             (1, 2) < (1, 3) AS row_less, \
             (1, NULL) = (1, NULL) AS row_unknown",
        )
        .unwrap();
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
            SqlValue::Boolean(true),
            SqlValue::Null,
        ]]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_try_cast_preserves_values_and_cast_errors() {
    let db = Database::new();
    let result = db
        .execute_sql(
            "SELECT TRY_CAST('42' AS INTEGER), \
             TRY_CAST('bad' AS INTEGER), \
             TRY_CAST([1.0, 2.0] AS VECTOR(3))",
        )
        .unwrap();
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(42), SqlValue::Null, SqlValue::Null,]]
    );

    let error = db
        .execute_sql("SELECT CAST('bad' AS INTEGER)")
        .expect_err("CAST conversion failure must remain an error");
    let rendered = error.to_string();
    assert!(rendered.contains("ALOPEX-E004"), "{rendered}");
    assert!(
        rendered.contains("cannot cast Text to INTEGER"),
        "{rendered}"
    );
    assert!(!rendered.contains("TypedExpr"), "{rendered}");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_fetch_pagination_and_with_ties_preserve_exact_rows() {
    let db = Database::new();
    db.execute_sql(
        "CREATE TABLE pages (id INTEGER PRIMARY KEY, score INTEGER); \
         INSERT INTO pages (id, score) VALUES \
         (1, 10), (2, 20), (3, 20), (4, 20), (5, 30), (6, NULL);",
    )
    .unwrap();

    let fetched = db
        .execute_sql("SELECT id FROM pages ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY")
        .unwrap();
    let ExecutionResult::Query(fetched) = fetched else {
        panic!("expected query result");
    };
    assert_eq!(
        fetched.rows,
        vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]]
    );

    let ties = db
        .execute_sql("SELECT id FROM pages ORDER BY score FETCH FIRST 2 ROWS WITH TIES")
        .unwrap();
    let ExecutionResult::Query(ties) = ties else {
        panic!("expected query result");
    };
    assert_eq!(
        ties.rows,
        vec![
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
            vec![SqlValue::Integer(4)],
        ]
    );

    let expression = db
        .execute_sql("SELECT id FROM pages ORDER BY id LIMIT 1 + 1")
        .unwrap();
    let ExecutionResult::Query(expression) = expression else {
        panic!("expected query result");
    };
    assert_eq!(expression.rows.len(), 2);

    let missing_order = db
        .execute_sql("SELECT id FROM pages FETCH FIRST 2 ROWS WITH TIES")
        .expect_err("WITH TIES requires ORDER BY");
    assert!(
        missing_order
            .to_string()
            .contains("FETCH ... WITH TIES requires ORDER BY"),
        "{missing_order}"
    );

    let negative = db
        .execute_sql("SELECT id FROM pages LIMIT -1")
        .expect_err("negative LIMIT must fail");
    assert!(
        negative.to_string().contains("LIMIT must not be negative"),
        "{negative}"
    );

    // Streaming SQL rejects WITH TIES (ordered pagination is not streamable).
    let stream_error = alopex_embedded::OwnedSqlStreamPlan::preflight(
        &db,
        "SELECT id FROM pages ORDER BY id FETCH FIRST 1 ROW WITH TIES",
    )
    .expect_err("WITH TIES streaming must be rejected");
    assert_eq!(
        stream_error.sql_error_code(),
        Some("unsupported_streaming_sql")
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_integration_grouped_window_composition_preserves_exact_rows() {
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
            "SELECT region, SUM(value) AS total, \
                    RANK() OVER (ORDER BY SUM(value) DESC) AS sales_rank, \
                    SUM(SUM(value)) OVER () AS retained_total \
             FROM samples GROUP BY region HAVING SUM(value) >= 30 \
             ORDER BY sales_rank, region;",
        )
        .unwrap();
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![
                SqlValue::Text("west".into()),
                SqlValue::BigInt(70),
                SqlValue::BigInt(1),
                SqlValue::BigInt(100),
            ],
            vec![
                SqlValue::Text("east".into()),
                SqlValue::BigInt(30),
                SqlValue::BigInt(2),
                SqlValue::BigInt(100),
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
