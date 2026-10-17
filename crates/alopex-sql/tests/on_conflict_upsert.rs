use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn execute_sql(sql: &str) -> Vec<ExecutionResult> {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(&mut executor, &catalog, sql).expect("execute SQL")
}

fn execute_sql_on(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Result<Vec<ExecutionResult>, alopex_sql::executor::ExecutorError> {
    Parser::parse_sql(&AlopexDialect, sql)
        .expect("parse SQL")
        .into_iter()
        .map(|statement| {
            let plan = Planner::new(&*catalog.read().expect("catalog read"))
                .plan(&statement)
                .map_err(alopex_sql::executor::ExecutorError::from)?;
            executor.execute(plan)
        })
        .collect()
}

fn assert_targetless_unique_conflict(setup: &str, conflicting_insert: &str) {
    let results = execute_sql(&format!(
        "{setup} {conflicting_insert} ON CONFLICT DO NOTHING; SELECT id FROM items ORDER BY id;"
    ));
    assert_eq!(results[results.len() - 2], ExecutionResult::RowsAffected(0));
    let ExecutionResult::Query(query) = results.last().unwrap() else {
        panic!("expected unchanged rows");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Integer(1)]]);
}

#[test]
fn targetless_do_nothing_handles_unique_constraint() {
    assert_targetless_unique_conflict(
        "CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE); INSERT INTO items VALUES (1, 'a');",
        "INSERT INTO items VALUES (2, 'a')",
    );
}

#[test]
fn targetless_do_nothing_handles_unique_index() {
    assert_targetless_unique_conflict(
        "CREATE TABLE items (id INT PRIMARY KEY, code TEXT); CREATE UNIQUE INDEX items_code ON items(code); INSERT INTO items VALUES (1, 'a');",
        "INSERT INTO items VALUES (2, 'a')",
    );
}

#[test]
fn targetless_do_nothing_handles_composite_unique() {
    assert_targetless_unique_conflict(
        "CREATE TABLE items (id INT PRIMARY KEY, a TEXT, b INT, UNIQUE(a,b)); INSERT INTO items VALUES (1, 'a', 7);",
        "INSERT INTO items VALUES (2, 'a', 7)",
    );
}

#[test]
fn targetless_do_nothing_skips_only_conflicting_batch_rows() {
    let results = execute_sql(
        "CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE);
         INSERT INTO items VALUES (1,'a'),(1,'b'),(2,'a'),(3,'c'),(4,NULL),(5,NULL),(5,'d')
             ON CONFLICT DO NOTHING RETURNING id;
         SELECT id,code FROM items ORDER BY id;",
    );
    let ExecutionResult::Query(returned) = &results[1] else {
        panic!("expected inserted rows only");
    };
    assert_eq!(
        returned.rows,
        [1, 3, 4, 5]
            .into_iter()
            .map(|id| vec![SqlValue::Integer(id)])
            .collect::<Vec<_>>()
    );
    let ExecutionResult::Query(query) = &results[2] else {
        panic!("expected retained rows");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(3), SqlValue::Text("c".into())],
            vec![SqlValue::Integer(4), SqlValue::Null],
            vec![SqlValue::Integer(5), SqlValue::Null],
        ]
    );
}

#[test]
fn targetless_do_nothing_preserves_other_constraint_errors() {
    use alopex_sql::executor::{ConstraintViolation, ExecutorError};
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE NOT NULL, qty INT, CHECK(qty > 0));
         INSERT INTO items VALUES (1,'a',1);",
    )
    .unwrap();
    let error = execute_sql_on(
        &mut executor,
        &catalog,
        "INSERT INTO items VALUES (2,'a',2) ON CONFLICT(id) DO NOTHING;",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ExecutorError::ConstraintViolation(ConstraintViolation::Unique { .. })
    ));
    for (sql, expected) in [
        (
            "INSERT INTO items VALUES (2,CAST(NULL AS TEXT),2) ON CONFLICT DO NOTHING;",
            ConstraintViolation::NotNull {
                column: "code".into(),
            },
        ),
        (
            "INSERT INTO items VALUES (2,'b',0) ON CONFLICT DO NOTHING;",
            ConstraintViolation::Check {
                constraint: "CHECK".into(),
            },
        ),
        (
            "INSERT INTO items VALUES (1,CAST(NULL AS TEXT),2) ON CONFLICT DO NOTHING;",
            ConstraintViolation::NotNull {
                column: "code".into(),
            },
        ),
        (
            "INSERT INTO items VALUES (1,'a',0) ON CONFLICT DO NOTHING;",
            ConstraintViolation::Check {
                constraint: "CHECK".into(),
            },
        ),
    ] {
        let error = execute_sql_on(&mut executor, &catalog, sql).unwrap_err();
        let ExecutorError::ConstraintViolation(actual) = error else {
            panic!("expected {expected:?}, got {error:?}: {sql}");
        };
        assert_eq!(actual, expected, "{sql}");
    }
    let results = execute_sql_on(&mut executor, &catalog, "SELECT id FROM items;").unwrap();
    let ExecutionResult::Query(query) = &results[0] else {
        panic!("expected unchanged rows");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Integer(1)]]);
}

#[test]
fn targetless_do_update_keeps_primary_key_only_contract() {
    use alopex_sql::executor::{ConstraintViolation, ExecutorError};
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let results = execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE, qty INT);
         INSERT INTO items VALUES (1,'a',10);
         INSERT INTO items VALUES (1,'b',20) ON CONFLICT DO UPDATE SET qty = EXCLUDED.qty;
         SELECT id,code,qty FROM items;",
    )
    .unwrap();
    assert_eq!(results[2], ExecutionResult::RowsAffected(1));
    let expected = vec![vec![
        SqlValue::Integer(1),
        SqlValue::Text("a".into()),
        SqlValue::Integer(20),
    ]];
    let ExecutionResult::Query(query) = &results[3] else {
        panic!("expected PK update");
    };
    assert_eq!(query.rows, expected);
    let error = execute_sql_on(
        &mut executor,
        &catalog,
        "INSERT INTO items VALUES (2,'a',30) ON CONFLICT DO UPDATE SET qty = EXCLUDED.qty;",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ExecutorError::ConstraintViolation(ConstraintViolation::Unique { .. })
    ));
    let result = execute_sql_on(&mut executor, &catalog, "SELECT id,code,qty FROM items;").unwrap();
    let ExecutionResult::Query(query) = &result[0] else {
        panic!("expected unchanged rows");
    };
    assert_eq!(query.rows, expected);

    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE no_pk (code TEXT UNIQUE, qty INT); INSERT INTO no_pk VALUES ('a',10);",
    )
    .unwrap();
    let error = execute_sql_on(
        &mut executor,
        &catalog,
        "INSERT INTO no_pk VALUES ('a',30) ON CONFLICT DO UPDATE SET qty = EXCLUDED.qty;",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ExecutorError::ConstraintViolation(ConstraintViolation::Unique { .. })
    ));
    let result = execute_sql_on(&mut executor, &catalog, "SELECT code,qty FROM no_pk;").unwrap();
    let ExecutionResult::Query(query) = &result[0] else {
        panic!("expected unchanged no-PK rows");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Text("a".into()), SqlValue::Integer(10)]]
    );
}

#[test]
fn targetless_do_nothing_checks_foreign_keys_only_for_accepted_rows() {
    use alopex_sql::executor::{ConstraintViolation, ExecutorError};
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(&mut executor, &catalog,
        "CREATE TABLE parents (id INT PRIMARY KEY);
         CREATE TABLE children (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY(parent_id) REFERENCES parents(id));
         INSERT INTO parents VALUES (10); INSERT INTO children VALUES (1,10);").unwrap();
    let skipped = execute_sql_on(
        &mut executor,
        &catalog,
        "INSERT INTO children VALUES (1,999) ON CONFLICT DO NOTHING;",
    )
    .unwrap();
    assert_eq!(skipped[0], ExecutionResult::RowsAffected(0));
    let error = execute_sql_on(
        &mut executor,
        &catalog,
        "INSERT INTO children VALUES (2,999) ON CONFLICT DO NOTHING;",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ExecutorError::ConstraintViolation(ConstraintViolation::ForeignKey { .. })
    ));
    let result = execute_sql_on(
        &mut executor,
        &catalog,
        "SELECT id,parent_id FROM children;",
    )
    .unwrap();
    let ExecutionResult::Query(query) = &result[0] else {
        panic!("expected unchanged child rows");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
    );
}

#[test]
fn multi_row_upsert_uses_excluded_values() {
    let results = execute_sql(
        "
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]);
        INSERT INTO items (id, embedding) VALUES (1, [1.0, 0.0]), (2, [0.0, 1.0])
          ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;
        SELECT id, embedding FROM items ORDER BY id;
        ",
    );

    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Vector(vec![1.0, 0.0])],
            vec![SqlValue::Integer(2), SqlValue::Vector(vec![0.0, 1.0])],
        ]
    );
}

#[test]
fn duplicate_ids_in_one_upsert_are_rejected_atomically() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));\
         INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]);",
    )
    .expect("seed SQL");

    assert!(
        execute_sql_on(
            &mut executor,
            &catalog,
            "INSERT INTO items (id, embedding) VALUES (1, [1.0, 0.0]), (1, [0.0, 1.0])\
         ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;",
        )
        .is_err()
    );

    let results = execute_sql_on(&mut executor, &catalog, "SELECT id, embedding FROM items;")
        .expect("verify SQL");
    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Vector(vec![0.0, 0.0]),]]
    );
}

#[test]
fn unique_index_enforces_conflicts_and_allows_multiple_nulls() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "
        CREATE TABLE users (id INT PRIMARY KEY, code TEXT);
        INSERT INTO users (id, code) VALUES (1, 'a'), (2, NULL), (3, NULL);
        CREATE UNIQUE INDEX users_code_key ON users (code);
        INSERT INTO users (id, code) VALUES (4, NULL);
        ",
    )
    .expect("create unique index");

    assert!(
        execute_sql_on(
            &mut executor,
            &catalog,
            "INSERT INTO users (id, code) VALUES (5, 'a');",
        )
        .is_err()
    );
    assert!(
        execute_sql_on(
            &mut executor,
            &catalog,
            "UPDATE users SET code = 'a' WHERE id = 2;",
        )
        .is_err()
    );

    let results = execute_sql_on(
        &mut executor,
        &catalog,
        "
        INSERT INTO users (id, code) VALUES (6, 'a') ON CONFLICT (code) DO NOTHING;
        SELECT id, code FROM users ORDER BY id;
        ",
    )
    .expect("execute ON CONFLICT");
    assert_eq!(results[0], ExecutionResult::RowsAffected(0));
    let ExecutionResult::Query(query) = &results[1] else {
        panic!("expected users query");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Null],
            vec![SqlValue::Integer(3), SqlValue::Null],
            vec![SqlValue::Integer(4), SqlValue::Null],
        ]
    );
}

#[test]
fn unique_index_build_rejects_existing_duplicates_without_publishing_index() {
    use alopex_sql::Catalog;
    use alopex_sql::executor::{ConstraintViolation, ExecutorError};

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE users (id INT PRIMARY KEY, code TEXT); \
         INSERT INTO users VALUES (1, 'a'), (2, 'a');",
    )
    .unwrap();

    let error = execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE UNIQUE INDEX users_code_key ON users (code);",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ExecutorError::ConstraintViolation(ConstraintViolation::Unique { .. })
    ));
    assert!(
        catalog
            .read()
            .unwrap()
            .get_index("users_code_key")
            .is_none()
    );

    let result = execute_sql_on(
        &mut executor,
        &catalog,
        "SELECT id, code FROM users ORDER BY id;",
    )
    .unwrap();
    let ExecutionResult::Query(query) = &result[0] else {
        panic!("expected unchanged rows");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("a".into())],
        ]
    );

    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE INDEX users_code_key ON users (code);",
    )
    .expect("failed UNIQUE build must leave the index name available");
    assert!(
        !catalog
            .read()
            .unwrap()
            .get_index("users_code_key")
            .unwrap()
            .unique
    );
}

#[test]
fn composite_unique_rejects_insert_and_update_but_allows_multiple_nulls() {
    use alopex_sql::executor::{ConstraintViolation, ExecutorError};

    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE users (id INT PRIMARY KEY, code TEXT, region INT, UNIQUE(code, region)); \
         INSERT INTO users VALUES (1, 'a', 1), (2, 'a', 2), (3, NULL, 1), (4, NULL, 1);",
    )
    .unwrap();
    for sql in [
        "INSERT INTO users VALUES (5, 'a', 1);",
        "UPDATE users SET region = 1 WHERE id = 2;",
    ] {
        let error = execute_sql_on(&mut executor, &catalog, sql).unwrap_err();
        assert!(matches!(
            error,
            ExecutorError::ConstraintViolation(ConstraintViolation::Unique { .. })
        ));
    }
    let result = execute_sql_on(
        &mut executor,
        &catalog,
        "SELECT id, code, region FROM users ORDER BY id;",
    )
    .unwrap();
    let ExecutionResult::Query(query) = &result[0] else {
        panic!("expected unchanged rows");
    };
    assert_eq!(query.rows.len(), 4);
    assert_eq!(
        query.rows[1],
        vec![
            SqlValue::Integer(2),
            SqlValue::Text("a".into()),
            SqlValue::Integer(2),
        ]
    );
}

#[test]
fn explicit_conflict_target_requires_a_unique_key_even_without_conflicting_rows() {
    use alopex_sql::executor::ExecutorError;
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(&mut executor, &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE, qty INT, UNIQUE(id, code), CONSTRAINT positive CHECK(qty > 0)); CREATE INDEX qty_idx ON items(qty);").unwrap();
    for target in [
        "(qty)",
        "(code, qty)",
        "(code, code)",
        "ON CONSTRAINT missing",
        "ON CONSTRAINT positive",
        "ON CONSTRAINT qty_idx",
    ] {
        for action in ["NOTHING", "UPDATE SET qty=EXCLUDED.qty"] {
            let error = execute_sql_on(
                &mut executor,
                &catalog,
                &format!("INSERT INTO items VALUES (1,'a',10) ON CONFLICT {target} DO {action}"),
            )
            .expect_err("invalid target must fail before accepting any row");
            assert!(
                matches!(error, ExecutorError::InvalidOperation { .. }),
                "{target}: {error}"
            );
        }
    }
    let result = execute_sql_on(&mut executor, &catalog, "SELECT id FROM items").unwrap();
    let ExecutionResult::Query(result) = &result[0] else {
        panic!("expected query");
    };
    assert!(result.rows.is_empty());
}

#[test]
fn named_unique_conflict_uses_its_columns_and_rejects_repeated_updates() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(&mut executor, &catalog,
        "CREATE TABLE items (id INT, code TEXT, region INT, qty INT, CONSTRAINT item_pk PRIMARY KEY(id), CONSTRAINT item_key UNIQUE(code, region)); INSERT INTO items VALUES (1,'a',7,10)").unwrap();
    let result = execute_sql_on(
        &mut executor,
        &catalog,
        "INSERT INTO items VALUES (2,'a',7,20) ON CONFLICT ON CONSTRAINT item_key DO NOTHING",
    )
    .unwrap();
    assert_eq!(result[0], ExecutionResult::RowsAffected(0));
    for target in ["ON CONSTRAINT item_key", "(region, code)"] {
        let result = execute_sql_on(&mut executor, &catalog,
            &format!("INSERT INTO items VALUES (2,'a',7,20) ON CONFLICT {target} DO UPDATE SET qty=EXCLUDED.qty")).unwrap();
        assert_eq!(result[0], ExecutionResult::RowsAffected(1));
        let error = execute_sql_on(&mut executor, &catalog,
            &format!("INSERT INTO items VALUES (3,'a',7,30),(4,'a',7,40) ON CONFLICT {target} DO UPDATE SET qty=EXCLUDED.qty")).unwrap_err();
        assert!(
            matches!(
                error,
                alopex_sql::executor::ExecutorError::ConstraintViolation(
                    alopex_sql::executor::ConstraintViolation::Unique { .. }
                )
            ),
            "{target}: {error}"
        );
    }
    let result = execute_sql_on(&mut executor, &catalog,
        "INSERT INTO items VALUES (1,'unused',9,20) ON CONFLICT ON CONSTRAINT item_pk DO UPDATE SET qty=EXCLUDED.qty").unwrap();
    assert_eq!(result[0], ExecutionResult::RowsAffected(1));
    let result = execute_sql_on(
        &mut executor,
        &catalog,
        "SELECT id,code,region,qty FROM items",
    )
    .unwrap();
    let ExecutionResult::Query(result) = &result[0] else {
        panic!("expected query");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("a".into()),
            SqlValue::Integer(7),
            SqlValue::Integer(20)
        ]]
    );
}
