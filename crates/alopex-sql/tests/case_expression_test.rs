use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::{Planner, PlannerError};
use alopex_sql::storage::SqlValue;

struct SqlHarness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl SqlHarness {
    fn new() -> Self {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        Self {
            executor: Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog)),
            catalog,
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Vec<ExecutionResult> {
        let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse CASE SQL");
        statements
            .into_iter()
            .map(|statement| {
                let plan = {
                    let catalog = self.catalog.read().expect("catalog read");
                    Planner::new(&*catalog)
                        .plan(&statement)
                        .expect("plan CASE SQL")
                };
                self.executor.execute(plan).expect("execute CASE SQL")
            })
            .collect()
    }

    fn query_sql(&mut self, sql: &str) -> QueryResult {
        self.execute_sql(sql)
            .into_iter()
            .rev()
            .find_map(|result| match result {
                ExecutionResult::Query(query) => Some(query),
                _ => None,
            })
            .expect("query result")
    }
}

fn populated_harness() -> SqlHarness {
    let mut harness = SqlHarness::new();
    harness.execute_sql(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, score INTEGER, label TEXT, active BOOLEAN); \
         INSERT INTO items VALUES \
           (1, 5, 'a', TRUE), \
           (2, 15, 'b', FALSE), \
           (3, NULL, 'c', NULL);",
    );
    harness
}

#[test]
fn searched_case_supports_multiple_when_else_and_null_conditions() {
    let mut harness = populated_harness();

    assert_eq!(
        harness
            .query_sql(
                "SELECT CASE \
                   WHEN score >= 10 THEN 'high' \
                   WHEN score IS NULL THEN 'missing' \
                   ELSE 'low' \
                 END FROM items ORDER BY id",
            )
            .rows,
        vec![
            vec![SqlValue::Text("low".into())],
            vec![SqlValue::Text("high".into())],
            vec![SqlValue::Text("missing".into())],
        ]
    );

    assert_eq!(
        harness
            .query_sql("SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END FROM items WHERE id = 1")
            .rows,
        vec![vec![SqlValue::Text("no".into())]]
    );
}

#[test]
fn simple_case_supports_multiple_when_and_implicit_null_else() {
    let mut harness = populated_harness();

    assert_eq!(
        harness
            .query_sql(
                "SELECT CASE label WHEN 'a' THEN 10 WHEN 'b' THEN 20 ELSE 30 END, \
                        CASE WHEN active THEN label END \
                 FROM items ORDER BY id",
            )
            .rows,
        vec![
            vec![SqlValue::Integer(10), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(20), SqlValue::Null],
            vec![SqlValue::Integer(30), SqlValue::Null],
        ]
    );

    assert_eq!(
        harness
            .query_sql("SELECT CASE NULL WHEN NULL THEN 'matched' ELSE 'not matched' END")
            .rows,
        vec![vec![SqlValue::Text("not matched".into())]]
    );
}

#[test]
fn case_can_be_nested_and_used_in_where_and_order_by() {
    let mut harness = populated_harness();

    assert_eq!(
        harness
            .query_sql(
                "SELECT id, \
                        CASE WHEN id = 1 \
                          THEN CASE label WHEN 'a' THEN 'nested' ELSE 'wrong' END \
                          ELSE 'other' \
                        END \
                 FROM items \
                 WHERE CASE WHEN active THEN TRUE END \
                 ORDER BY CASE label WHEN 'a' THEN 0 ELSE 1 END, id",
            )
            .rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("nested".into()),]]
    );
}

#[test]
fn case_promotes_numeric_result_branches_to_a_common_type() {
    let mut harness = populated_harness();

    assert_eq!(
        harness
            .query_sql("SELECT CASE WHEN id = 1 THEN 1 ELSE 2.5 END FROM items ORDER BY id")
            .rows,
        vec![
            vec![SqlValue::Double(1.0)],
            vec![SqlValue::Double(2.5)],
            vec![SqlValue::Double(2.5)],
        ]
    );

    assert_eq!(
        harness
            .query_sql("SELECT CASE WHEN TRUE THEN 7 ELSE 1 / 0 END")
            .rows,
        vec![vec![SqlValue::Integer(7)]]
    );
}

#[test]
fn case_rejects_incompatible_then_and_else_result_types() {
    let statement = Parser::parse_sql(
        &AlopexDialect,
        "SELECT CASE WHEN TRUE THEN 1 ELSE 'text' END",
    )
    .expect("CASE syntax should parse")
    .remove(0);
    let catalog = MemoryCatalog::new();

    assert!(matches!(
        Planner::new(&catalog).plan(&statement),
        Err(PlannerError::TypeMismatch { .. })
    ));
}
