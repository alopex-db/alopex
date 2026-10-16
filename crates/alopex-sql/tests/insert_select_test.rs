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
fn columnar_ctas_plan_rejects_before_catalog_mutation_and_preserves_noop() {
    use alopex_sql::catalog::Catalog;
    use alopex_sql::planner::LogicalPlan;
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), catalog.clone());
    let statement = Parser::parse_sql(&AlopexDialect, "CREATE TABLE copied AS SELECT 1 AS id")
        .unwrap()
        .remove(0);
    let row_plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let mut columnar_plan = row_plan.clone();
    let LogicalPlan::CreateTableAs {
        with_options,
        if_not_exists,
        ..
    } = &mut columnar_plan
    else {
        panic!("expected CTAS plan");
    };
    with_options.push(("storage".into(), "columnar".into()));
    *if_not_exists = true;
    assert!(matches!(executor.execute(columnar_plan.clone()),
        Err(ExecutorError::UnsupportedOperation(message)) if message.contains("columnar")));
    assert!(!catalog.read().unwrap().table_exists("copied"));
    executor.execute(row_plan).unwrap();
    assert_eq!(
        executor.execute(columnar_plan).unwrap(),
        ExecutionResult::Success
    );
    let statement = Parser::parse_sql(&AlopexDialect, "SELECT id FROM copied")
        .unwrap()
        .remove(0);
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let ExecutionResult::Query(query) = executor.execute(plan).unwrap() else {
        panic!("expected preserved rows");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Integer(1)]]);
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
fn ctas_with_options_reaches_existing_storage_guard() {
    use alopex_sql::planner::LogicalPlan;
    for sql in [
        "CREATE TABLE copied WITH (storage='columnar') AS SELECT 1 AS id",
        "CREATE TABLE copied AS SELECT 1 AS id WITH (storage='columnar')",
    ] {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(Arc::new(MemoryKV::new()), catalog.clone());
        let statement = Parser::parse_sql(&AlopexDialect, sql)
            .expect("parse CTAS WITH")
            .remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .expect("plan CTAS WITH");
        let LogicalPlan::CreateTableAs { with_options, .. } = &plan else {
            panic!("expected CTAS plan");
        };
        assert_eq!(with_options, &vec![("storage".into(), "columnar".into())]);
        assert!(matches!(executor.execute(plan),
            Err(ExecutorError::UnsupportedOperation(message)) if message.contains("columnar")));
        assert!(!catalog.read().unwrap().table_exists("copied"));
    }
}

#[test]
fn create_table_as_select_keeps_output_schema_without_source_constraints() {
    let results = execute_sql(
        "CREATE TABLE source (id INT PRIMARY KEY, cat TEXT NOT NULL);
         INSERT INTO source VALUES (1, 'a');
         CREATE TABLE copied AS SELECT id AS copied_id, cat FROM source;
         INSERT INTO copied VALUES (1, NULL);
         CREATE TABLE IF NOT EXISTS copied AS SELECT id, cat FROM source;
         SELECT copied_id, cat FROM copied ORDER BY cat NULLS LAST;",
    )
    .expect("CTAS copies output names and types without source constraints");
    let ExecutionResult::Query(query) = results.last().unwrap() else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(1), SqlValue::Null],
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
fn ctas_rejects_untyped_null_and_accepts_explicit_cast() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let statement = Parser::parse_sql(&AlopexDialect, "CREATE TABLE bare AS SELECT NULL AS x")
        .expect("parse CTAS")
        .pop()
        .expect("one statement");
    let error = Planner::new(&*catalog.read().expect("catalog lock"))
        .plan(&statement)
        .expect_err("untyped NULL must be rejected");
    assert!(matches!(error, PlannerError::InvalidExpression { .. }));
    assert!(
        error.to_string().contains("cast NULL explicitly"),
        "{error}"
    );
    assert!(!catalog.read().expect("catalog lock").table_exists("bare"));

    let results = execute_sql(
        "CREATE TABLE typed AS SELECT CAST(NULL AS INTEGER) AS x;
         SELECT x FROM typed;",
    )
    .expect("explicitly typed NULL output executes");
    let ExecutionResult::Query(typed) = &results[1] else {
        panic!("expected typed NULL query result");
    };
    assert_eq!(typed.columns[0].data_type, ResolvedType::Integer);
    assert_eq!(typed.rows, vec![vec![SqlValue::Null]]);
}
