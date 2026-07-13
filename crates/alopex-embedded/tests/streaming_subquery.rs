//! Streaming SQL API subquery tests (GitHub issues #23 / #24).
//!
//! The CLI executes local SELECT statements through
//! `Database::execute_sql_with_rows` (see `alopex-cli/src/commands/sql.rs`),
//! which uses the streaming pipeline. Historically this path either failed
//! with `UnsupportedExpression` (WHERE-clause subqueries, issue #24) or
//! silently produced an empty result set (SELECT-list scalar subqueries,
//! issue #23), while the non-streaming `Database::execute_sql` path executed
//! the same statements correctly.
//!
//! These tests pin the streaming API to the same results as
//! `alopex-sql/tests/subquery_exec_test.rs` (same data set).

use alopex_embedded::{Database, SqlStreamingResult, StreamingQueryResult};
use alopex_sql::storage::SqlValue;

fn setup_db() -> Database {
    let db = Database::new();
    db.execute_sql(
        r#"
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
        INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 20);
        "#,
    )
    .expect("setup schema and data");
    db
}

/// Collect columns and rows through the CLI-equivalent streaming API.
/// Errors from `next_row` are propagated, never swallowed.
fn collect_streaming_rows(
    db: &Database,
    sql: &str,
) -> alopex_embedded::Result<(Vec<String>, Vec<Vec<SqlValue>>)> {
    let result = db.execute_sql_with_rows(sql, |mut rows| {
        let columns: Vec<String> = rows.columns().iter().map(|c| c.name.clone()).collect();
        let mut collected = Vec::new();
        while let Some(row) = rows.next_row()? {
            collected.push(row);
        }
        Ok((columns, collected))
    })?;
    match result {
        StreamingQueryResult::QueryProcessed(data) => Ok(data),
        _ => panic!("expected query result for SELECT"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn streaming_rows_where_in_subquery_executes() {
    let db = setup_db();
    let (columns, rows) = collect_streaming_rows(
        &db,
        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders) ORDER BY users.id;",
    )
    .expect("streaming IN subquery must execute (issue #24)");
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn streaming_rows_scalar_subquery_in_select_list_executes() {
    let db = setup_db();
    let (columns, rows) = collect_streaming_rows(
        &db,
        "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id;",
    )
    .expect("streaming scalar subquery must execute (issue #23)");
    // Issue #23: this used to yield columns/rows silently empty.
    assert_eq!(columns, vec!["name".to_string(), "order_count".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into()), SqlValue::BigInt(2)],
            vec![SqlValue::Text("bob".into()), SqlValue::BigInt(1)],
            vec![SqlValue::Text("carol".into()), SqlValue::BigInt(0)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn streaming_rows_correlated_exists_subquery_executes() {
    let db = setup_db();
    let (_, rows) = collect_streaming_rows(
        &db,
        "SELECT users.name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY users.id;",
    )
    .expect("streaming EXISTS subquery must execute");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn streaming_iterator_scalar_subquery_executes() {
    let db = setup_db();
    let result = db
        .execute_sql_streaming(
            "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id;",
        )
        .expect("execute_sql_streaming must succeed");
    let SqlStreamingResult::Query(mut iter) = result else {
        panic!("expected query result for SELECT");
    };
    let columns: Vec<String> = iter.columns().iter().map(|c| c.name.clone()).collect();
    assert_eq!(columns, vec!["name".to_string(), "order_count".to_string()]);
    let mut rows = Vec::new();
    while let Some(row) = iter.next_row().expect("next_row must not error") {
        rows.push(row);
    }
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Text("alice".into()), SqlValue::BigInt(2)],
            vec![SqlValue::Text("bob".into()), SqlValue::BigInt(1)],
            vec![SqlValue::Text("carol".into()), SqlValue::BigInt(0)],
        ]
    );
}

/// Guard against the issue #23 failure mode: a SELECT that reaches the
/// streaming path must never yield an empty result while the non-streaming
/// path yields rows. Whatever happens, both paths must agree.
#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn streaming_and_non_streaming_paths_agree_on_subqueries() {
    let db = setup_db();
    let sql = "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id;";

    let non_streaming = match db.execute_sql(sql).expect("non-streaming path") {
        alopex_sql::ExecutionResult::Query(q) => q.rows,
        other => panic!("expected query result, got {other:?}"),
    };
    let (_, streaming) = collect_streaming_rows(&db, sql).expect("streaming path");
    assert_eq!(streaming, non_streaming);
}
