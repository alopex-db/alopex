#![cfg(feature = "tokio")]

use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::{AsyncKVStore, memory::MemoryKV};
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::{ExecutionResult, Executor, MemoryPolicy, SpillPolicy};
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::async_storage::{AsyncSqlTransaction, AsyncTxnBridge};
use alopex_sql::{AlopexDialect, Parser, Planner};
use std::sync::{Arc, RwLock};

fn text(result: ExecutionResult) -> String {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query");
    };
    let SqlValue::Text(value) = &result.rows[0][0] else {
        panic!("expected text");
    };
    value.clone()
}
const SETUP: [&str; 3] = [
    "CREATE TABLE k (id INTEGER PRIMARY KEY, g INTEGER, e VECTOR(4,L2))",
    "CREATE INDEX kg ON k (g)",
    "INSERT INTO k VALUES (1,2,[1,0,0,1])",
];
const QUERY: &str =
    "SELECT id FROM k WHERE g = 2 ORDER BY vector_distance(e, [1,0,0,1], 'l2') LIMIT 1";
fn access_nodes<'a>(node: &'a serde_json::Value, out: &mut Vec<&'a serde_json::Value>) {
    if node.get("actual").is_some() {
        out.push(node);
    }
    if let Some(children) = node["children"].as_array() {
        for child in children {
            access_nodes(child, out);
        }
    }
}

#[test]
fn explain_btree_identical_cte_and_view_probes_have_independent_loops() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), catalog.clone());
    let mut run = |sql: &str| {
        let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        executor.execute(plan).unwrap()
    };
    for sql in SETUP {
        run(sql);
    }
    run("CREATE VIEW visible AS SELECT id FROM k WHERE g = 2");
    for sql in [
        "WITH visible AS (SELECT id FROM k WHERE g = 2) SELECT id FROM visible UNION ALL SELECT id FROM visible",
        "SELECT id FROM visible UNION ALL SELECT id FROM visible",
    ] {
        let output = text(run(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}")));
        let document: serde_json::Value = serde_json::from_str(&output).unwrap();
        let mut nodes = Vec::new();
        access_nodes(&document["physical_plan"]["root"], &mut nodes);
        assert_eq!(nodes.len(), 2, "{output}");
        for node in nodes {
            assert_eq!(node["actual"]["loops"], 1, "{output}");
            assert_eq!(node["actual"]["candidates_examined"], 1, "{output}");
        }
    }
}

fn assert_not_executed(text: &str, json: &str) {
    assert!(text.contains("ExactKnnScan"), "{text}");
    assert!(
        !text
            .lines()
            .any(|line| line.trim_start().starts_with("IndexScan")),
        "{text}"
    );
    assert!(text.contains("planned=IndexScan index=kg"), "{text}");
    assert!(text.contains("actual_path=NotExecuted"), "{text}");
    let json: serde_json::Value = serde_json::from_str(json).unwrap();
    let access = &json["physical_plan"]["access_path"];
    assert_eq!(access["node"], "NotExecuted");
    assert_eq!(access["planned"], "IndexScan");
    assert_eq!(access["actual"]["loops"], 0);
    assert_eq!(access["actual"]["candidates_examined"], 0);
}

#[test]
fn explain_btree_sync_does_not_name_unexecuted_index_as_actual() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), catalog.clone());
    let mut run = |sql: &str| {
        let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        executor.execute(plan).unwrap()
    };
    for sql in SETUP {
        run(sql);
    }
    let actual = text(run(&format!("EXPLAIN ANALYZE {QUERY}")));
    let json = text(run(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {QUERY}")));
    assert_not_executed(&actual, &json);
}

async fn bridge() -> AsyncTxnBridge<'static, AsyncKVTransactionAdapter> {
    let store = AsyncKVStoreAdapter::from_arc(Arc::new(MemoryKV::new()), TxnMode::ReadWrite);
    let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> =
        Arc::new(RwLock::new(MemoryCatalog::new()));
    AsyncTxnBridge::with_catalog(
        store.begin_async().await.unwrap(),
        TxnMode::ReadWrite,
        catalog,
    )
}

#[tokio::test]
async fn explain_btree_async_cte_probes_have_independent_loops() {
    let mut bridge = bridge().await;
    for sql in SETUP {
        bridge.async_execute(sql).await.unwrap();
    }
    let output = text(bridge.async_execute("EXPLAIN (ANALYZE, FORMAT JSON) WITH visible AS (SELECT id FROM k WHERE g = 2) SELECT id FROM visible UNION ALL SELECT id FROM visible").await.unwrap());
    let document: serde_json::Value = serde_json::from_str(&output).unwrap();
    let mut nodes = Vec::new();
    access_nodes(&document["physical_plan"]["root"], &mut nodes);
    assert_eq!(nodes.len(), 2, "{output}");
    for node in nodes {
        assert_eq!(node["actual"]["loops"], 1, "{output}");
        assert_eq!(node["actual"]["candidates_examined"], 1, "{output}");
    }
}

#[tokio::test]
async fn explain_btree_async_does_not_name_unexecuted_index_as_actual() {
    let mut bridge = bridge().await;
    for sql in SETUP {
        bridge.async_execute(sql).await.unwrap();
    }
    let actual = text(
        bridge
            .async_execute(&format!("EXPLAIN ANALYZE {QUERY}"))
            .await
            .unwrap(),
    );
    let json = text(
        bridge
            .async_execute(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {QUERY}"))
            .await
            .unwrap(),
    );
    assert_not_executed(&actual, &json);
}

#[tokio::test]
async fn explain_btree_async_insert_select_applies_reported_memory_policy() {
    let mut bridge = bridge().await;
    for sql in [
        "CREATE TABLE source (id INTEGER, g INTEGER, wide TEXT)".to_owned(),
        "CREATE TABLE destination (id INTEGER)".to_owned(),
        "CREATE INDEX source_g ON source(g)".to_owned(),
        format!("INSERT INTO source VALUES (1,2,'{}')", "x".repeat(257)),
    ] {
        bridge.async_execute(&sql).await.unwrap();
    }
    bridge.set_memory_policy(Some(MemoryPolicy::new(Some(256), SpillPolicy::FailFast)));
    let json = text(bridge.async_execute("EXPLAIN (ANALYZE, FORMAT JSON) INSERT INTO destination SELECT id FROM source WHERE g = 2").await.unwrap());
    let json: serde_json::Value = serde_json::from_str(&json).unwrap();
    let access = &json["physical_plan"]["access_path"];
    assert_eq!(access["adaptive"]["max_bytes"], 256);
    assert_eq!(access["actual"]["path"], "Scan");
    assert_eq!(access["actual"]["fallback"], "ByteBudget");
    let ExecutionResult::Query(rows) = bridge
        .async_execute("SELECT id FROM destination")
        .await
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(1)]]);
}
