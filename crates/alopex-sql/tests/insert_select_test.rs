use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
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
fn explain_ctas_includes_indexed_source_without_creating_target() {
    let results = execute_sql(
        "CREATE TABLE source (id INT);
         INSERT INTO source VALUES (7), (9);
         CREATE INDEX source_id ON source (id);
         EXPLAIN CREATE TABLE copied AS SELECT id FROM source WHERE id = 7;
         CREATE TABLE copied AS SELECT id FROM source WHERE id = 7;
         SELECT id FROM copied;",
    )
    .unwrap();
    let ExecutionResult::Query(explain) = &results[3] else {
        panic!("expected EXPLAIN rows");
    };
    let SqlValue::Text(plan) = &explain.rows[0][0] else {
        panic!("expected EXPLAIN text");
    };
    assert!(plan.contains("CreateTableAs"), "{plan}");
    assert!(
        plan.contains("IndexScan index=source_id table=source"),
        "{plan}"
    );
    let ExecutionResult::Query(query) = results.last().unwrap() else {
        panic!("expected copied rows");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Integer(7)]]);
}

#[test]
fn ctas_materializes_nested_natural_join_and_outer_using_join() {
    let results = execute_sql(
        "CREATE TABLE l (id INT, lv INT);
         CREATE TABLE r (id INT, rv INT);
         CREATE TABLE s (id INT);
         INSERT INTO l VALUES (1,10), (2,20);
         INSERT INTO r VALUES (2,200), (3,300);
         INSERT INTO s VALUES (2);
         CREATE TABLE copied AS
         SELECT * FROM (SELECT * FROM l NATURAL JOIN r) q JOIN s USING (id);
         SELECT id, lv, rv FROM copied;",
    )
    .unwrap();
    let ExecutionResult::Query(query) = results.last().unwrap() else {
        panic!("expected materialized rows");
    };
    assert_eq!(
        query
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "lv", "rv"]
    );
    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Integer(2),
            SqlValue::Integer(20),
            SqlValue::Integer(200)
        ]]
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
