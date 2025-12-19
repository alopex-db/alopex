use std::sync::{Arc, RwLock};

use alopex_core::kv::KVStore;
use alopex_core::kv::memory::MemoryKV;
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::HnswIndex;

use alopex_sql::Span;
use alopex_sql::ast::ddl::{IndexMethod, VectorMetric};
use alopex_sql::ast::expr::Literal;
use alopex_sql::catalog::{
    Catalog, CatalogOverlay, PersistentCatalog, TableMetadata, TxnCatalogView,
};
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::typed_expr::{Projection, TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{SqlTxn as _, TxnBridge};

fn executor_with_persistent_catalog(
    store: Arc<MemoryKV>,
) -> (
    Executor<MemoryKV, PersistentCatalog<MemoryKV>>,
    Arc<RwLock<PersistentCatalog<MemoryKV>>>,
) {
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    (Executor::new(store, catalog.clone()), catalog)
}

fn wrap_external<'a, 'b>(
    txn: &'a mut <MemoryKV as KVStore>::Transaction<'b>,
    mode: TxnMode,
    overlay: &'a mut CatalogOverlay,
) -> alopex_sql::storage::BorrowedSqlTransaction<'a, 'b, 'a, MemoryKV> {
    TxnBridge::<MemoryKV>::wrap_external(txn, mode, overlay)
}

fn vector_table() -> TableMetadata {
    TableMetadata::new(
        "items",
        vec![
            alopex_sql::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            alopex_sql::catalog::ColumnMetadata::new(
                "embedding",
                ResolvedType::Vector {
                    dimension: 3,
                    metric: VectorMetric::Cosine,
                },
            )
            .with_not_null(true),
        ],
    )
    .with_primary_key(vec!["id".to_string()])
}

#[test]
fn wrap_external_preserves_mode() {
    let store = MemoryKV::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();

    let borrowed = wrap_external(&mut txn, TxnMode::ReadOnly, &mut overlay);
    assert_eq!(borrowed.mode(), TxnMode::ReadOnly);
}

#[test]
fn execute_in_txn_readonly_rejects_dml() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, _catalog) = executor_with_persistent_catalog(store.clone());

    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadOnly, &mut overlay);

    let plan = LogicalPlan::DropTable {
        name: "users".to_string(),
        if_exists: true,
    };

    let err = executor.execute_in_txn(plan, &mut borrowed).unwrap_err();
    assert!(matches!(err, ExecutorError::ReadOnlyTransaction { .. }));
}

#[test]
fn execute_in_txn_readonly_allows_select() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());

    // ベースカタログにテーブルを投入（ReadOnly で SELECT を通す目的）。
    {
        let mut catalog = catalog.write().expect("catalog lock poisoned");
        let table = TableMetadata::new(
            "users",
            vec![alopex_sql::catalog::ColumnMetadata::new(
                "id",
                ResolvedType::Integer,
            )],
        )
        .with_table_id(1);
        catalog.create_table(table).unwrap();
    }

    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadOnly, &mut overlay);

    let plan = LogicalPlan::Scan {
        table: "users".to_string(),
        projection: Projection::All(vec!["id".to_string()]),
    };

    let result = executor.execute_in_txn(plan, &mut borrowed).unwrap();
    assert!(matches!(result, ExecutionResult::Query(_)));
}

#[test]
fn overlay_visible_in_same_txn() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    let mut overlay = CatalogOverlay::new();

    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();

    // CREATE TABLE（コミット前なのでベースには反映されない）
    {
        let plan = LogicalPlan::CreateTable {
            table: TableMetadata::new(
                "users",
                vec![
                    alopex_sql::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                        .with_primary_key(true),
                ]
                .into_iter()
                .collect(),
            )
            .with_primary_key(vec!["id".to_string()]),
            if_not_exists: false,
            with_options: vec![],
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    {
        let catalog = catalog.read().expect("catalog lock poisoned");
        assert!(!catalog.table_exists("users"));
        let view = TxnCatalogView::new(&*catalog, &overlay);
        assert!(view.table_exists("users"));
    }

    // 同一トランザクション内で DROP TABLE が通る（オーバーレイで可視）
    {
        let plan = LogicalPlan::DropTable {
            name: "users".to_string(),
            if_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }
}

#[test]
fn hnsw_flush_on_success() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, _catalog) = executor_with_persistent_catalog(store.clone());
    let mut overlay = CatalogOverlay::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();

    // CREATE TABLE
    {
        let plan = LogicalPlan::CreateTable {
            table: vector_table(),
            if_not_exists: false,
            with_options: vec![],
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // CREATE INDEX (HNSW)
    {
        let plan = LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "idx_items_embedding",
                "items",
                vec!["embedding".to_string()],
            )
            .with_method(IndexMethod::Hnsw),
            if_not_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // INSERT（HNSW は staged -> execute_in_txn が flush する）
    {
        let id = TypedExpr::literal(
            Literal::Number("1".to_string()),
            ResolvedType::Integer,
            Span::default(),
        );
        let embedding = TypedExpr {
            kind: TypedExprKind::VectorLiteral(vec![0.1, 0.2, 0.3]),
            resolved_type: ResolvedType::Vector {
                dimension: 3,
                metric: VectorMetric::Cosine,
            },
            span: Span::default(),
        };
        let plan = LogicalPlan::Insert {
            table: "items".to_string(),
            columns: vec!["id".to_string(), "embedding".to_string()],
            values: vec![vec![id, embedding]],
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // flush が効いていれば、同一トランザクション内でも HNSW インデックスがロードできる。
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (results, _) = index.search(&[0.1f32, 0.2, 0.3], 1, None).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn hnsw_abandon_on_drop() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    let mut overlay = CatalogOverlay::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();

    // CREATE TABLE + HNSW INDEX（ベースとなるインデックスを作成）
    {
        let plan = LogicalPlan::CreateTable {
            table: vector_table(),
            if_not_exists: false,
            with_options: vec![],
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }
    {
        let plan = LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "idx_items_embedding",
                "items",
                vec!["embedding".to_string()],
            )
            .with_method(IndexMethod::Hnsw),
            if_not_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // dirty 状態だけ作って Drop させる（flush しない）。
    {
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let (mut sql_txn, overlay) = borrowed.split_parts();
        let catalog_guard = catalog.read().expect("catalog lock poisoned");
        let table_view = TxnCatalogView::new(&*catalog_guard, &*overlay);
        let index = table_view.get_index("idx_items_embedding").unwrap().clone();

        // staged だけ作る（Drop により rollback される/少なくとも残らないことを期待）
        let entry = sql_txn.hnsw_entry_mut(&index.name).unwrap();
        entry
            .index
            .upsert_staged(
                &1u64.to_be_bytes(),
                &[0.1f32, 0.2, 0.3],
                &[],
                &mut entry.state,
            )
            .unwrap();
        entry.dirty = true;
    }

    // Drop 後もインデックスが壊れずロードできる（rollback が安全に働くことの確認）。
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (results, _) = index.search(&[0.1f32, 0.2, 0.3], 1, None).unwrap();
    assert!(results.is_empty());
}
