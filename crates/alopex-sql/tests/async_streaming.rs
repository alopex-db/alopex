#![cfg(all(feature = "tokio", not(target_arch = "wasm32")))]

use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::ast::expr::Literal;
use alopex_sql::catalog::{ColumnMetadata, PersistentCatalog, TableMetadata};
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::planner::typed_expr::TypedExpr;
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{
    AsyncSqlTransaction, AsyncTxnBridge, LogicalPlan, Projection, Span, TokioAsyncTxnBridge,
};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn dropping_stream_allows_commit_to_finish() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    let mut executor = Executor::new(store.clone(), catalog.clone());

    let table = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
        ],
    )
    .with_primary_key(vec!["id".into()]);

    let create = LogicalPlan::CreateTable {
        table,
        if_not_exists: false,
        with_options: vec![],
    };
    let result = executor.execute(create).unwrap();
    assert!(matches!(result, ExecutionResult::Success));

    let mut values = Vec::new();
    for id in 0..256 {
        let row = vec![
            TypedExpr::literal(
                Literal::Number(id.to_string()),
                ResolvedType::Integer,
                Span::default(),
            ),
            TypedExpr::literal(
                Literal::String(format!("name_{id}")),
                ResolvedType::Text,
                Span::default(),
            ),
        ];
        values.push(row);
    }

    let insert = LogicalPlan::Insert {
        table: "users".into(),
        columns: vec!["id".into(), "name".into()],
        values,
    };
    let result = executor.execute(insert).unwrap();
    assert!(matches!(result, ExecutionResult::RowsAffected(256)));

    let bridge = TokioAsyncTxnBridge::new(store, catalog);
    let mut txn = bridge.begin_read().await.unwrap();

    let stream = txn
        .query_stream(LogicalPlan::Scan {
            table: "users".into(),
            projection: Projection::All(vec!["id".into(), "name".into()]),
        })
        .await
        .unwrap();

    drop(stream);

    let commit = timeout(Duration::from_millis(200), txn.commit()).await;
    assert!(commit.is_ok());
    assert!(commit.unwrap().is_ok());
}
