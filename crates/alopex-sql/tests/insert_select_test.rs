use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::PlannerError;
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;

fn execute_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse sql");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());

    statements
        .iter()
        .map(|statement| {
            let catalog = catalog.read().expect("catalog lock");
            let plan = Planner::new(&*catalog).plan(statement)?;
            drop(catalog);
            executor.execute(plan)
        })
        .collect()
}

#[test]
fn insert_select_inserts_rows_with_and_without_explicit_columns() {
    let results = execute_sql(
        "
        CREATE TABLE t (id INT, name TEXT);
        INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob');
        INSERT INTO t SELECT id, name FROM t;
        SELECT id, name FROM t ORDER BY id;
        ",
    )
    .expect("INSERT INTO ... SELECT executes");

    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
        ]
    );

    let explicit_columns = execute_sql(
        "
        CREATE TABLE t (id INT, name TEXT);
        INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob');
        INSERT INTO t (name, id) SELECT name, id FROM t;
        SELECT id, name FROM t ORDER BY id;
        ",
    )
    .expect("INSERT INTO columns ... SELECT executes");

    let ExecutionResult::Query(query) = explicit_columns.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(1), SqlValue::Text("alice".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn create_table_as_select_materializes_source_rows() {
    let results = execute_sql(
        "
        CREATE TABLE p (id INT, cat TEXT);
        INSERT INTO p VALUES (1, 'a'), (2, 'b');
        CREATE TABLE p2 AS SELECT * FROM p;
        SELECT id, cat FROM p2 ORDER BY id;
        ",
    )
    .expect("CREATE TABLE AS SELECT executes");

    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
        ]
    );
}

#[test]
fn create_table_as_select_derives_names_and_types_without_source_constraints() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog));
    let statements = Parser::parse_sql(
        &AlopexDialect,
        "CREATE TABLE p (id INT PRIMARY KEY, cat TEXT NOT NULL DEFAULT 'a'); \
         INSERT INTO p VALUES (1, 'x'); \
         CREATE TABLE p2 AS SELECT id AS copied_id, cat AS copied_cat, id + 1 AS next_id FROM p; \
         INSERT INTO p2 (copied_id, copied_cat, next_id) VALUES (1, NULL, 3); \
         SELECT copied_id, copied_cat, next_id FROM p2 ORDER BY next_id;",
    )
    .expect("parse CTAS");
    let mut results = Vec::new();
    for statement in statements {
        let plan = Planner::new(&*catalog.read().expect("catalog lock"))
            .plan(&statement)
            .expect("plan CTAS");
        results.push(
            executor
                .execute(plan)
                .expect("execute CTAS and unconstrained insert"),
        );
    }

    let table = catalog
        .read()
        .expect("catalog lock")
        .get_table("p2")
        .expect("created CTAS table")
        .clone();
    assert_eq!(
        table.column_names(),
        vec!["copied_id", "copied_cat", "next_id"]
    );
    assert_eq!(
        table
            .columns
            .iter()
            .map(|column| &column.data_type)
            .collect::<Vec<_>>(),
        vec![
            &ResolvedType::Integer,
            &ResolvedType::Text,
            &ResolvedType::Integer
        ]
    );
    assert!(table.primary_key.is_none());
    assert!(table.constraints.is_empty());
    assert!(table.columns.iter().all(|column| {
        !column.not_null && !column.primary_key && !column.unique && column.default.is_none()
    }));

    let ExecutionResult::Query(query) = results.last().expect("select result") else {
        panic!("expected CTAS rows");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![
                SqlValue::Integer(1),
                SqlValue::Text("x".into()),
                SqlValue::Integer(2),
            ],
            vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Integer(3)],
        ]
    );
}

#[test]
fn ctas_rejects_duplicate_output_names_before_creation() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let statement = Parser::parse_sql(
        &AlopexDialect,
        "CREATE TABLE copied AS SELECT 1 AS id, 2 AS id",
    )
    .expect("parse CTAS")
    .pop()
    .expect("one statement");
    let error = Planner::new(&*catalog.read().expect("catalog lock"))
        .plan(&statement)
        .expect_err("duplicate output names must be rejected");
    assert!(matches!(error, PlannerError::InvalidExpression { .. }));
    assert!(error.to_string().contains("duplicate"), "{error}");
    assert!(!catalog.read().expect("catalog lock").table_exists("copied"));
}

#[test]
fn ctas_resolves_untyped_null_output_to_text() {
    let results = execute_sql(
        "CREATE TABLE bare AS SELECT NULL AS x;
         CREATE TABLE typed AS SELECT CAST(NULL AS INTEGER) AS x;
         SELECT x FROM bare;
         SELECT x FROM typed;",
    )
    .expect("CTAS with NULL output executes");
    let ExecutionResult::Query(bare) = &results[2] else {
        panic!("expected bare NULL query result");
    };
    let ExecutionResult::Query(typed) = &results[3] else {
        panic!("expected typed NULL query result");
    };
    assert_eq!(bare.columns[0].data_type, ResolvedType::Text);
    assert_eq!(typed.columns[0].data_type, ResolvedType::Integer);
    assert_eq!(bare.rows, vec![vec![SqlValue::Null]]);
    assert_eq!(typed.rows, vec![vec![SqlValue::Null]]);
}
