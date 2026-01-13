#![cfg(all(feature = "tokio", not(target_arch = "wasm32")))]

use std::sync::{Arc, RwLock};

use alopex_core::KVStore;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::HnswIndex;
use alopex_sql::ast::ddl::IndexMethod;
use alopex_sql::catalog::{ColumnMetadata, IndexMetadata, PersistentCatalog, TableMetadata};
use alopex_sql::executor::ExecutionResult;
use alopex_sql::planner::typed_expr::{TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{AsyncSqlTransaction, AsyncTxnBridge, LogicalPlan, Span, TokioAsyncTxnBridge};

#[tokio::test]
async fn async_txn_preserves_hnsw_index_consistency() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));

    let bridge = TokioAsyncTxnBridge::new(store.clone(), catalog.clone());
    let mut txn = bridge.begin_write().await.unwrap();

    let table = TableMetadata::new(
        "items",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new(
                "embedding",
                ResolvedType::Vector {
                    dimension: 2,
                    metric: alopex_sql::ast::ddl::VectorMetric::Cosine,
                },
            )
            .with_not_null(true),
        ],
    )
    .with_primary_key(vec!["id".into()]);

    let create_table = LogicalPlan::CreateTable {
        table,
        if_not_exists: false,
        with_options: vec![],
    };
    let result = txn.execute_plan(create_table).await.unwrap();
    assert!(matches!(result, ExecutionResult::Success));

    let index = IndexMetadata::new(0, "idx_items_embedding", "items", vec!["embedding".into()])
        .with_method(IndexMethod::Hnsw);
    let create_index = LogicalPlan::CreateIndex {
        index,
        if_not_exists: false,
    };
    let result = txn.execute_plan(create_index).await.unwrap();
    assert!(matches!(result, ExecutionResult::Success));
    txn.commit().await.unwrap();

    let mut txn = bridge.begin_write().await.unwrap();
    let insert = LogicalPlan::Insert {
        table: "items".into(),
        columns: vec!["id".into(), "embedding".into()],
        values: vec![vec![
            TypedExpr::literal(
                alopex_sql::ast::expr::Literal::Number("1".into()),
                ResolvedType::Integer,
                Span::default(),
            ),
            TypedExpr {
                kind: TypedExprKind::VectorLiteral(vec![1.0, 0.0]),
                resolved_type: ResolvedType::Vector {
                    dimension: 2,
                    metric: alopex_sql::ast::ddl::VectorMetric::Cosine,
                },
                span: Span::default(),
            },
        ]],
    };
    let result = txn.execute_plan(insert).await.unwrap();
    assert!(matches!(result, ExecutionResult::RowsAffected(1)));
    txn.commit().await.unwrap();

    let mut txn = bridge.begin_write().await.unwrap();
    let insert = LogicalPlan::Insert {
        table: "items".into(),
        columns: vec!["id".into(), "embedding".into()],
        values: vec![vec![
            TypedExpr::literal(
                alopex_sql::ast::expr::Literal::Number("2".into()),
                ResolvedType::Integer,
                Span::default(),
            ),
            TypedExpr {
                kind: TypedExprKind::VectorLiteral(vec![0.0, 1.0]),
                resolved_type: ResolvedType::Vector {
                    dimension: 2,
                    metric: alopex_sql::ast::ddl::VectorMetric::Cosine,
                },
                span: Span::default(),
            },
        ]],
    };
    let result = txn.execute_plan(insert).await.unwrap();
    assert!(matches!(result, ExecutionResult::RowsAffected(1)));
    txn.rollback().await.unwrap();

    let mut read_txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut read_txn).unwrap();
    let (results, _) = index.search(&[0.0, 1.0], 1, None).unwrap();
    assert_eq!(results.len(), 1);
    let key: [u8; 8] = results[0].key.as_slice().try_into().unwrap();
    assert_eq!(u64::from_be_bytes(key), 1);
}
