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
                .expect("plan SQL");
            executor.execute(plan)
        })
        .collect()
}

#[test]
fn unique_creation_checks_sparse_rows_methods_and_repeated_names() {
    use alopex_sql::catalog::Catalog;
    use alopex_sql::executor::ExecutorError;
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let mut run = |sql: &str| execute_sql_on(&mut executor, &catalog, sql);

    run("CREATE TABLE sparse (id INT, code INT)").unwrap();
    // Preserve the 2,048-row gap without exceeding the parser's per-statement
    // MessagePack collection budget while constructing the fixture.
    for first in (1..=2050).step_by(128) {
        let values = (first..=(first + 127).min(2050))
            .map(|id| format!("({id}, 7)"))
            .collect::<Vec<_>>()
            .join(",");
        run(&format!("INSERT INTO sparse VALUES {values}")).unwrap();
    }
    run("DELETE FROM sparse WHERE id <= 2048").unwrap();
    let error = run("CREATE UNIQUE INDEX sparse_unique ON sparse (code)").unwrap_err();
    assert!(
        matches!(
            error,
            ExecutorError::ConstraintViolation(
                alopex_sql::executor::ConstraintViolation::Unique { .. }
            )
        ),
        "{error}"
    );
    assert!(!catalog.read().unwrap().index_exists("sparse_unique"));
    let rows = run("SELECT id, code FROM sparse ORDER BY id").unwrap();
    let ExecutionResult::Query(query) = &rows[0] else {
        panic!("expected sparse rows");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(2049), SqlValue::Integer(7)],
            vec![SqlValue::Integer(2050), SqlValue::Integer(7)],
        ]
    );
    run("CREATE INDEX sparse_unique ON sparse (code)").unwrap();

    run("CREATE TABLE vectors (id INT, embedding VECTOR(2, L2));
         INSERT INTO vectors VALUES (1, [1.0, 0.0]), (2, [1.0, 0.0]);")
    .unwrap();
    let error =
        run("CREATE UNIQUE INDEX vector_unique ON vectors (embedding) USING HNSW").unwrap_err();
    assert!(
        matches!(
            error,
            ExecutorError::InvalidOperation { ref operation, ref reason }
                if operation == "CREATE UNIQUE INDEX" && reason.contains("HNSW")
        ),
        "{error}"
    );
    assert!(!catalog.read().unwrap().index_exists("vector_unique"));
    let rows = run("SELECT id FROM vectors ORDER BY id").unwrap();
    let ExecutionResult::Query(query) = &rows[0] else {
        panic!("expected unchanged vector rows");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)],]
    );

    let error = run(
        "CREATE TABLE repeated (a INT, CONSTRAINT duplicate_name UNIQUE (a),
         CONSTRAINT duplicate_name UNIQUE (a))",
    )
    .unwrap_err();
    assert!(
        matches!(
            error, ExecutorError::IndexAlreadyExists(ref name) if name == "duplicate_name"
        ),
        "{error}"
    );
    let guard = catalog.read().unwrap();
    assert!(!guard.table_exists("repeated"));
    assert!(!guard.index_exists("duplicate_name"));
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

    assert_eq!(results[2], ExecutionResult::RowsAffected(2));
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
        INSERT INTO users (id, code) VALUES (1, 'a'), (2, NULL);
        CREATE UNIQUE INDEX users_code_key ON users (code);
        INSERT INTO users (id, code) VALUES (3, NULL);
        ",
    )
    .expect("create unique index");

    for sql in [
        "INSERT INTO users (id, code) VALUES (4, 'a');",
        "UPDATE users SET code = 'a' WHERE id = 2;",
    ] {
        let error = execute_sql_on(&mut executor, &catalog, sql).unwrap_err();
        let alopex_sql::executor::ExecutorError::ConstraintViolation(
            alopex_sql::executor::ConstraintViolation::Unique {
                value: Some(value), ..
            },
        ) = error
        else {
            panic!("expected UNIQUE violation with duplicate value: {error}")
        };
        assert_eq!(value, "[Text(\"a\")]");
    }

    let results = execute_sql_on(
        &mut executor,
        &catalog,
        "
        INSERT INTO users (id, code) VALUES (5, 'a') ON CONFLICT (code) DO NOTHING;
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
        ]
    );
}

#[test]
fn unique_constraints_share_composite_conflict_and_null_semantics() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(&mut executor, &catalog, "
        CREATE TABLE pairs (id INT PRIMARY KEY, a TEXT, b INT, note TEXT, UNIQUE (a, b));
        INSERT INTO pairs VALUES (1, 'key', 2, 'first'), (2, 'key', NULL, 'null'), (3, 'key', NULL, 'null');
    ").unwrap();
    for sql in [
        "INSERT INTO pairs VALUES (4, 'key', 2, 'duplicate')",
        "UPDATE pairs SET b = 2 WHERE id = 2",
    ] {
        let error = execute_sql_on(&mut executor, &catalog, sql).unwrap_err();
        let alopex_sql::executor::ExecutorError::ConstraintViolation(
            alopex_sql::executor::ConstraintViolation::Unique {
                value: Some(value), ..
            },
        ) = error
        else {
            panic!("expected unique violation: {error}")
        };
        assert_eq!(value, "[Text(\"key\"), Integer(2)]");
    }
    let results = execute_sql_on(&mut executor, &catalog, "
        INSERT INTO pairs VALUES (4, 'key', 2, 'updated') ON CONFLICT (a, b) DO UPDATE SET note = EXCLUDED.note;
        SELECT id, note FROM pairs ORDER BY id;
    ").unwrap();
    assert_eq!(results[0], ExecutionResult::RowsAffected(1));
    let ExecutionResult::Query(query) = &results[1] else {
        panic!("expected rows")
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("updated".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("null".into())],
            vec![SqlValue::Integer(3), SqlValue::Text("null".into())],
        ]
    );
}

#[test]
fn unique_index_creation_reports_existing_duplicate_and_rolls_back() {
    use alopex_sql::Catalog;
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE TABLE duplicates (code TEXT); INSERT INTO duplicates VALUES ('same'), ('same');",
    )
    .unwrap();
    let error = execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE UNIQUE INDEX ux ON duplicates (code)",
    )
    .unwrap_err();
    let alopex_sql::executor::ExecutorError::ConstraintViolation(
        alopex_sql::executor::ConstraintViolation::Unique {
            value: Some(value), ..
        },
    ) = error
    else {
        panic!("expected unique violation: {error}")
    };
    assert_eq!(value, "[Text(\"same\")]");
    assert!(!catalog.read().unwrap().index_exists("ux"));
    execute_sql_on(
        &mut executor,
        &catalog,
        "CREATE INDEX ux ON duplicates (code)",
    )
    .unwrap();
    let results = execute_sql_on(&mut executor, &catalog, "SELECT code FROM duplicates").unwrap();
    let ExecutionResult::Query(query) = &results[0] else {
        panic!("expected duplicate rows");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Text("same".into())],
            vec![SqlValue::Text("same".into())],
        ]
    );
}
