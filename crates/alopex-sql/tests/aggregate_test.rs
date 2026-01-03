use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionConfig, ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::{LogicalPlan, Planner, PlannerError};
use alopex_sql::storage::SqlValue;

fn setup_executor() -> (
    Executor<MemoryKV, MemoryCatalog>,
    Arc<RwLock<MemoryCatalog>>,
    ExecutionConfig,
) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let executor = Executor::new(store, catalog.clone());
    let config = ExecutionConfig::default();
    (executor, catalog, config)
}

fn plan_single(
    sql: &str,
    catalog: &Arc<RwLock<MemoryCatalog>>,
) -> Result<LogicalPlan, PlannerError> {
    let stmts = Parser::parse_sql(&AlopexDialect, sql).expect("parse sql");
    assert_eq!(stmts.len(), 1, "expected a single statement");
    let guard = catalog.read().expect("catalog lock poisoned");
    let planner = Planner::new(&*guard);
    planner.plan(&stmts[0])
}

fn execute_sql(
    sql: &str,
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    config: &ExecutionConfig,
) -> Vec<ExecutionResult> {
    let stmts = Parser::parse_sql(&AlopexDialect, sql).expect("parse sql");
    let mut results = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        let plan = {
            let guard = catalog.read().expect("catalog lock poisoned");
            let planner = Planner::new(&*guard);
            planner.plan(&stmt).expect("plan")
        };
        results.push(executor.execute(plan, config).expect("execute"));
    }
    results
}

fn execute_query(
    sql: &str,
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    config: &ExecutionConfig,
) -> Result<ExecutionResult, ExecutorError> {
    let plan = plan_single(sql, catalog).expect("plan");
    executor.execute(plan, config)
}

#[test]
fn e2e_group_by_count() {
    let (mut executor, catalog, config) = setup_executor();
    execute_sql(
        r#"
        CREATE TABLE emp (dept TEXT, name TEXT, salary INT);
        INSERT INTO emp (dept, name, salary) VALUES
            ('engineering', 'alice', 100),
            ('engineering', 'bob', 200),
            ('sales', 'carol', 150);
        "#,
        &mut executor,
        &catalog,
        &config,
    );

    let result = execute_query(
        "SELECT dept, COUNT(*) FROM emp GROUP BY dept",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };

    let mut rows: Vec<(String, i64)> = q
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (SqlValue::Text(dept), SqlValue::BigInt(count)) => (dept.clone(), *count),
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        rows,
        vec![("engineering".to_string(), 2), ("sales".to_string(), 1),]
    );
}

#[test]
fn e2e_count_distinct() {
    let (mut executor, catalog, config) = setup_executor();
    execute_sql(
        r#"
        CREATE TABLE orders (id INT, status TEXT, amount INT);
        INSERT INTO orders (id, status, amount) VALUES
            (1, 'new', 100),
            (2, 'new', 200),
            (3, 'shipped', 150),
            (4, NULL, 50);
        "#,
        &mut executor,
        &catalog,
        &config,
    );

    let result = execute_query(
        "SELECT COUNT(DISTINCT status) FROM orders",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    assert_eq!(q.rows.len(), 1);
    assert_eq!(q.rows[0][0], SqlValue::BigInt(2));
}

#[test]
fn e2e_global_aggregate_and_having() {
    let (mut executor, catalog, config) = setup_executor();
    execute_sql(
        r#"
        CREATE TABLE orders (id INT, amount INT);
        INSERT INTO orders (id, amount) VALUES (1, 100), (2, 200), (3, 50);
        "#,
        &mut executor,
        &catalog,
        &config,
    );

    let result = execute_query(
        "SELECT COUNT(*), SUM(amount) FROM orders",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    assert_eq!(q.rows.len(), 1);
    assert_eq!(q.rows[0], vec![SqlValue::BigInt(3), SqlValue::Integer(350)]);

    // Use a BigInt literal to match COUNT(*) result type.
    let result = execute_query(
        "SELECT COUNT(*) FROM orders HAVING COUNT(*) > -2147483649",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    assert_eq!(q.rows.len(), 1);
    assert_eq!(q.rows[0][0], SqlValue::BigInt(3));
}

#[test]
fn e2e_having_order_by_and_limit() {
    let (mut executor, catalog, config) = setup_executor();
    execute_sql(
        r#"
        CREATE TABLE products (category TEXT, price INT);
        INSERT INTO products (category, price) VALUES
            ('A', 600),
            ('A', 500),
            ('B', 700),
            ('B', 200),
            ('C', 1200);
        "#,
        &mut executor,
        &catalog,
        &config,
    );

    let result = execute_query(
        "SELECT category, SUM(price) FROM products GROUP BY category HAVING SUM(price) > 1000",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };

    let mut rows: Vec<(String, i32)> = q
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (SqlValue::Text(category), SqlValue::Integer(total)) => (category.clone(), *total),
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(rows, vec![("A".to_string(), 1100), ("C".to_string(), 1200)]);

    let result = execute_query(
        "SELECT category, SUM(price) AS total FROM products GROUP BY category ORDER BY total DESC",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    let ordered: Vec<String> = q
        .rows
        .iter()
        .map(|row| match &row[0] {
            SqlValue::Text(category) => category.clone(),
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    assert_eq!(
        ordered,
        vec!["C".to_string(), "A".to_string(), "B".to_string()]
    );

    let result = execute_query(
        "SELECT category, SUM(price) AS total FROM products GROUP BY category ORDER BY total DESC LIMIT 1",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    assert_eq!(q.rows.len(), 1);
    assert_eq!(
        q.rows[0],
        vec![SqlValue::Text("C".to_string()), SqlValue::Integer(1200)]
    );
}

#[test]
fn e2e_empty_table_aggregate_behavior() {
    let (mut executor, catalog, config) = setup_executor();
    execute_sql(
        "CREATE TABLE empty_orders (category TEXT, amount INT);",
        &mut executor,
        &catalog,
        &config,
    );

    let result = execute_query(
        "SELECT COUNT(*), SUM(amount) FROM empty_orders",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    assert_eq!(q.rows.len(), 1);
    assert_eq!(q.rows[0], vec![SqlValue::BigInt(0), SqlValue::Null]);

    let result = execute_query(
        "SELECT category, COUNT(*) FROM empty_orders GROUP BY category",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    assert!(q.rows.is_empty());

    // Use a BigInt literal to match COUNT(*) result type.
    let result = execute_query(
        "SELECT COUNT(*) FROM empty_orders HAVING COUNT(*) > 2147483648",
        &mut executor,
        &catalog,
        &config,
    )
    .expect("execute");
    let ExecutionResult::Query(q) = result else {
        panic!("expected query result");
    };
    assert!(q.rows.is_empty());
}

#[test]
fn e2e_aggregate_error_cases() {
    let (mut executor, catalog, config) = setup_executor();
    execute_sql(
        "CREATE TABLE emp (dept TEXT, name TEXT);",
        &mut executor,
        &catalog,
        &config,
    );

    let err = plan_single(
        "SELECT dept, name, COUNT(*) FROM emp GROUP BY dept",
        &catalog,
    )
    .unwrap_err();
    assert!(matches!(err, PlannerError::InvalidGroupBy { .. }));

    let err = plan_single("SELECT COUNT(*) FROM emp WHERE COUNT(*) > 0", &catalog).unwrap_err();
    assert!(matches!(err, PlannerError::AggregateInWhere { .. }));

    let err = plan_single("SELECT name FROM emp HAVING COUNT(*) > 0", &catalog).unwrap_err();
    assert!(matches!(err, PlannerError::InvalidHaving { .. }));

    let err = plan_single("SELECT SUM(name) FROM emp", &catalog).unwrap_err();
    assert!(matches!(err, PlannerError::TypeMismatch { .. }));

    execute_sql(
        r#"
        INSERT INTO emp (dept, name) VALUES
            ('engineering', 'alice'),
            ('sales', 'bob');
        "#,
        &mut executor,
        &catalog,
        &config,
    );

    let limited = ExecutionConfig { max_groups: 1 };
    let err = execute_query(
        "SELECT dept, COUNT(*) FROM emp GROUP BY dept",
        &mut executor,
        &catalog,
        &limited,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecutorError::TooManyGroups {
            limit: 1,
            actual: 2
        }
    ));
}
