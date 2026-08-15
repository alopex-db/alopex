use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::build_streaming_pipeline;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::{Planner, PlannerError};
use alopex_sql::storage::{SqlValue, TxnBridge};

fn execute_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse set-operation SQL");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, Arc::clone(&catalog));
    let mut results = Vec::with_capacity(statements.len());

    for statement in statements {
        let plan = {
            let guard = catalog.read().expect("catalog read");
            Planner::new(&*guard).plan(&statement)?
        };
        results.push(executor.execute(plan)?);
    }

    Ok(results)
}

fn query(sql: &str) -> QueryResult {
    execute_sql(sql)
        .expect("execute set-operation SQL")
        .into_iter()
        .rev()
        .find_map(|result| match result {
            ExecutionResult::Query(query) => Some(query),
            _ => None,
        })
        .expect("query result")
}

fn integer_rows(query: QueryResult) -> Vec<i32> {
    let mut values = query
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [SqlValue::Integer(value)] => *value,
            other => panic!("expected one integer column, got {other:?}"),
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn setup(query: &str) -> String {
    format!(
        "CREATE TABLE t1 (a INT); \
         CREATE TABLE t2 (a INT); \
         INSERT INTO t1 (a) VALUES (1), (2), (2); \
         INSERT INTO t2 (a) VALUES (2), (3); \
         {query};"
    )
}

fn streaming_pipeline_rows(sql: &str) -> Vec<Vec<SqlValue>> {
    let statements = Parser::parse_sql(&AlopexDialect, sql).expect("parse streaming set operation");
    assert_eq!(statements.len(), 1, "expected one streaming statement");

    let catalog = MemoryCatalog::new();
    let plan = Planner::new(&catalog)
        .plan(&statements[0])
        .expect("plan streaming set operation");
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let mut txn = bridge.begin_read().expect("begin streaming read");
    let (mut iter, _projection, _schema) =
        build_streaming_pipeline(&mut txn, &catalog, plan).expect("build streaming set operation");

    let mut rows = Vec::new();
    while let Some(row) = iter.next_row() {
        rows.push(row.expect("read streaming set-operation row").values);
    }
    rows
}

#[test]
fn union_returns_distinct_rows_from_both_inputs() {
    let result = query(&setup("SELECT a FROM t1 UNION SELECT a FROM t2"));
    assert_eq!(integer_rows(result), vec![1, 2, 3]);
}

#[test]
fn union_all_preserves_duplicates_from_both_inputs() {
    let result = query(&setup("SELECT a FROM t1 UNION ALL SELECT a FROM t2"));
    assert_eq!(integer_rows(result), vec![1, 2, 2, 2, 3]);
}

#[test]
fn intersect_returns_distinct_rows_present_in_both_inputs() {
    let result = query(&setup("SELECT a FROM t1 INTERSECT SELECT a FROM t2"));
    assert_eq!(integer_rows(result), vec![2]);
}

#[test]
fn except_returns_distinct_rows_absent_from_right_input() {
    let result = query(&setup("SELECT a FROM t1 EXCEPT SELECT a FROM t2"));
    assert_eq!(integer_rows(result), vec![1]);
}

#[test]
fn set_operation_rejects_different_column_counts() {
    let error = execute_sql(
        "CREATE TABLE t1 (a INT); \
         CREATE TABLE t2 (a INT, b INT); \
         SELECT a FROM t1 UNION SELECT a, b FROM t2;",
    )
    .expect_err("different set-operation column counts must be rejected");

    assert!(
        error
            .to_string()
            .contains("set operation column count mismatch: left 1, right 2"),
        "unexpected error: {error}"
    );
}

#[test]
fn set_operation_rejects_incompatible_column_types() {
    let error = execute_sql(
        "CREATE TABLE t1 (a INT); \
         CREATE TABLE t2 (a TEXT); \
         SELECT a FROM t1 UNION SELECT a FROM t2;",
    )
    .expect_err("incompatible set-operation column types must be rejected");

    assert!(
        matches!(
            error,
            ExecutorError::Planner(PlannerError::TypeMismatch { .. })
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn order_by_applies_to_the_combined_union_all_result() {
    let result = query(&setup(
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a DESC",
    ));
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(3)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(1)],
        ]
    );
}

#[test]
fn intersect_binds_more_tightly_than_union() {
    let result = query("SELECT 1 UNION SELECT 2 INTERSECT SELECT 2;");
    assert_eq!(integer_rows(result), vec![1, 2]);
}

#[test]
fn set_operation_words_inside_literals_and_comments_are_not_operators() {
    let literal = query("SELECT 'UNION ALL';");
    assert_eq!(
        literal.rows,
        vec![vec![SqlValue::Text("UNION ALL".to_string())]]
    );

    let comment = query("SELECT 1 /* UNION SELECT 2 */;");
    assert_eq!(comment.rows, vec![vec![SqlValue::Integer(1)]]);
}

#[test]
fn union_all_runs_through_the_streaming_pipeline() {
    assert_eq!(
        streaming_pipeline_rows("SELECT 1 UNION ALL SELECT 2"),
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)],]
    );
}
