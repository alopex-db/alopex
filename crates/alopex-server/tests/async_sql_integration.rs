#![cfg(not(target_arch = "wasm32"))]

use std::future::poll_fn;
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::ast::expr::Literal;
use alopex_sql::catalog::{ColumnMetadata, PersistentCatalog, TableMetadata};
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::planner::typed_expr::{ProjectedColumn, TypedExpr};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{
    AsyncResult, AsyncRowStream, AsyncSqlTransaction, AsyncTxnBridge, LogicalPlan, Projection,
    Span, SqlValue, TokioAsyncTxnBridge,
};

async fn next_stream_item(
    stream: &mut AsyncRowStream<'_>,
) -> Option<AsyncResult<alopex_sql::executor::Row>> {
    poll_fn(|cx| futures_core::Stream::poll_next(stream.as_mut(), cx)).await
}

async fn collect_ok_rows(stream: &mut AsyncRowStream<'_>) -> AsyncResult<Vec<Vec<SqlValue>>> {
    let mut rows = Vec::new();
    while let Some(item) = next_stream_item(stream).await {
        let row = item?;
        rows.push(row.values);
    }
    Ok(rows)
}

fn sample_table(name: &str) -> TableMetadata {
    TableMetadata::new(
        name,
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
        ],
    )
    .with_primary_key(vec!["id".into()])
}

fn insert_row(table: &str, id: i64, name: &str) -> LogicalPlan {
    LogicalPlan::Insert {
        table: table.into(),
        columns: vec!["id".into(), "name".into()],
        values: vec![vec![
            TypedExpr::literal(
                Literal::Number(id.to_string()),
                ResolvedType::Integer,
                Span::default(),
            ),
            TypedExpr::literal(
                Literal::String(name.to_string()),
                ResolvedType::Text,
                Span::default(),
            ),
        ]],
    }
}

#[tokio::test]
async fn async_txn_commit_and_rollback_integrate() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    let bridge = TokioAsyncTxnBridge::new(store.clone(), catalog.clone());

    let mut txn = bridge.begin_write().await.unwrap();
    let create = LogicalPlan::CreateTable {
        table: sample_table("sessions"),
        if_not_exists: false,
        with_options: vec![],
    };
    txn.execute_plan(create).await.unwrap();
    txn.execute_plan(insert_row("sessions", 1, "alpha"))
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let mut executor = Executor::new(store.clone(), catalog.clone());
    let result = executor
        .execute(LogicalPlan::Scan {
            table: "sessions".into(),
            projection: Projection::All(vec!["id".into(), "name".into()]),
        })
        .unwrap();
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Text("alpha".into())]]
    );

    let mut txn = bridge.begin_write().await.unwrap();
    let create = LogicalPlan::CreateTable {
        table: sample_table("rollbacked"),
        if_not_exists: false,
        with_options: vec![],
    };
    txn.execute_plan(create).await.unwrap();
    txn.rollback().await.unwrap();

    let err = executor.execute(LogicalPlan::Scan {
        table: "rollbacked".into(),
        projection: Projection::All(vec!["id".into(), "name".into()]),
    });
    assert!(matches!(err, Err(ExecutorError::TableNotFound(_))));
}

#[tokio::test]
async fn async_streaming_matches_sync_results() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));

    let mut executor = Executor::new(store.clone(), catalog.clone());
    executor
        .execute(LogicalPlan::CreateTable {
            table: sample_table("users"),
            if_not_exists: false,
            with_options: vec![],
        })
        .unwrap();
    executor.execute(insert_row("users", 1, "alice")).unwrap();
    executor.execute(insert_row("users", 2, "bob")).unwrap();

    let sync_result = executor
        .execute(LogicalPlan::Scan {
            table: "users".into(),
            projection: Projection::All(vec!["id".into(), "name".into()]),
        })
        .unwrap();
    let ExecutionResult::Query(sync_query) = sync_result else {
        panic!("expected query result");
    };

    let bridge = TokioAsyncTxnBridge::new(store, catalog);
    let mut txn = bridge.begin_read().await.unwrap();
    let mut stream = txn
        .query_stream(LogicalPlan::Scan {
            table: "users".into(),
            projection: Projection::All(vec!["id".into(), "name".into()]),
        })
        .await
        .unwrap();
    let async_rows = collect_ok_rows(&mut stream).await.unwrap();
    assert_eq!(async_rows, sync_query.rows);
    drop(stream);
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn async_streaming_propagates_errors() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));

    let mut executor = Executor::new(store.clone(), catalog.clone());
    executor
        .execute(LogicalPlan::CreateTable {
            table: sample_table("items"),
            if_not_exists: false,
            with_options: vec![],
        })
        .unwrap();
    executor.execute(insert_row("items", 1, "first")).unwrap();

    let invalid_projection = Projection::Columns(vec![ProjectedColumn {
        expr: TypedExpr::column_ref(
            "items".to_string(),
            "missing".to_string(),
            9,
            ResolvedType::Integer,
            Span::default(),
        ),
        alias: None,
    }]);

    let bridge = TokioAsyncTxnBridge::new(store, catalog);
    let mut txn = bridge.begin_read().await.unwrap();
    let mut stream = txn
        .query_stream(LogicalPlan::Scan {
            table: "items".into(),
            projection: invalid_projection,
        })
        .await
        .unwrap();
    let first = next_stream_item(&mut stream).await.unwrap();
    assert!(first.is_err());
    drop(stream);
    txn.commit().await.unwrap();
}
