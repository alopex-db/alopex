use std::sync::Arc;

use alopex_embedded::Database;
use alopex_sql::{ExecutionResult, SqlValue};

fn one_text(result: ExecutionResult) -> String {
    let ExecutionResult::Query(query) = result else {
        panic!("EXPLAIN must return a query result");
    };
    let SqlValue::Text(value) = &query.rows[0][0] else {
        panic!("EXPLAIN payload must be text");
    };
    value.clone()
}

fn row_count(database: &Database) -> i64 {
    let ExecutionResult::Query(query) = database.execute_sql("SELECT COUNT(*) FROM items").unwrap()
    else {
        panic!("COUNT must return rows");
    };
    let SqlValue::BigInt(value) = query.rows[0][0] else {
        panic!("COUNT must be BIGINT");
    };
    value
}

#[test]
fn explain_does_not_execute_but_analyze_does_and_reports_metrics() {
    let database = Database::new();
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();

    let plain = one_text(
        database
            .execute_sql("EXPLAIN INSERT INTO items VALUES (1)")
            .unwrap(),
    );
    assert!(plain.contains("Insert table=items"));
    assert!(!plain.contains("elapsed_ns="));
    assert_eq!(row_count(&database), 0);

    let analyzed = one_text(
        database
            .execute_sql("EXPLAIN ANALYZE INSERT INTO items VALUES (1)")
            .unwrap(),
    );
    assert!(analyzed.contains("elapsed_ns="));
    assert!(analyzed.contains("rows=1"));
    assert_eq!(row_count(&database), 1);
}

#[test]
fn json_contract_is_versioned_complete_and_redacts_literals_and_binds() {
    let database = Arc::new(Database::new());
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, secret TEXT)")
        .unwrap();
    let mut statement = database
        .prepare("EXPLAIN (FORMAT JSON) SELECT id FROM items WHERE secret = ?")
        .unwrap();
    statement
        .bind(1, SqlValue::Text("never-show-this".into()))
        .unwrap();

    let payload = one_text(statement.execute().unwrap());
    assert!(!payload.contains("never-show-this"));
    let document: serde_json::Value = serde_json::from_str(&payload).unwrap();
    let plan = serde_json::json!({
        "node": "Filter",
        "table": "items",
        "children": [{"node": "Scan", "table": "items", "children": []}],
    });
    assert_eq!(
        document,
        serde_json::json!({
            "schema": "alopex.explain",
            "version": 1,
            "analyze": false,
            "logical_plan": plan.clone(),
            "physical_plan": {"engine": "logical-direct", "root": plan},
            "distributed_plan": {
                "mode": "single-node",
                "fragments": [{"id": 0, "placement": "local", "root": "Filter"}],
            },
            "optimizer_rules": [
                {"name": "knn_pattern_detection", "status": "integrated", "applied": false}
            ],
            "metrics": {"elapsed_ns": null, "rows": null},
        })
    );
}

#[test]
fn analyze_failure_rolls_back_partial_writes() {
    let database = Database::new();
    database
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert!(database
        .execute_sql("EXPLAIN ANALYZE INSERT INTO items VALUES (1), (1)")
        .is_err());
    assert_eq!(row_count(&database), 0);
}

#[test]
fn explain_rejects_control_and_nested_explain_statements() {
    let database = Database::new();
    assert!(database.execute_sql("EXPLAIN BEGIN").is_err());
    assert!(database.execute_sql("EXPLAIN EXPLAIN SELECT 1").is_err());
}
