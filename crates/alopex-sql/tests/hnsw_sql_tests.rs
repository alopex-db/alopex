use std::sync::{Arc, RwLock};

use alopex_core::HnswIndex;
use alopex_core::TxnMode;
use alopex_core::kv::KVStore;
use alopex_core::kv::KVTransaction;
use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;

fn run_sql(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Vec<ExecutionResult> {
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, sql).expect("SQL のパースに失敗");
    let mut results = Vec::new();
    for stmt in stmts {
        let plan = {
            let guard = catalog.read().unwrap();
            let planner = Planner::new(&*guard);
            planner.plan(&stmt).expect("プラン作成に失敗")
        };
        let res = executor.execute(plan).expect("実行に失敗");
        results.push(res);
    }
    results
}

#[test]
fn create_insert_and_search_hnsw_index() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());

    run_sql(
        &mut executor,
        &catalog,
        "
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (m = 8, ef_construction = 32);
        INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]), (2, [1.0, 0.0]), (3, [0.5, 0.0]);
    ",
    );

    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (results, _) = index.search(&[0.8, 0.0], 2, Some(8)).unwrap();
    txn.commit_self().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].key, 2u64.to_be_bytes().to_vec());
    assert_eq!(results[1].key, 3u64.to_be_bytes().to_vec());
}

#[test]
fn invalid_with_option_returns_error() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());

    run_sql(
        &mut executor,
        &catalog,
        "CREATE TABLE docs (id INT PRIMARY KEY, embedding VECTOR(2, COSINE));",
    );

    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(
        &dialect,
        "CREATE INDEX bad_idx ON docs (embedding) USING HNSW WITH (unknown = 1)",
    )
    .unwrap();
    let stmt = &stmts[0];
    let plan = {
        let guard = catalog.read().unwrap();
        let planner = Planner::new(&*guard);
        planner.plan(stmt).unwrap()
    };
    let err = executor.execute(plan).unwrap_err();
    match err {
        ExecutorError::Core(alopex_core::Error::UnknownOption { key }) => {
            assert_eq!(key, "unknown");
        }
        other => panic!("想定外のエラー: {:?}", other),
    }
}

#[test]
fn dml_changes_are_reflected_in_hnsw_index() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());

    run_sql(
        &mut executor,
        &catalog,
        "
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;
        INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]), (2, [2.0, 0.0]);
    ",
    );

    // UPDATE で距離順位が入れ替わることを確認（行1を遠ざける）
    run_sql(
        &mut executor,
        &catalog,
        "UPDATE items SET embedding = [5.0, 0.0] WHERE id = 1;",
    );

    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (results, _) = index.search(&[0.0, 0.0], 2, Some(10)).unwrap();
    txn.commit_self().unwrap();
    assert_eq!(results[0].key, 2u64.to_be_bytes().to_vec());

    // DELETE で結果から消える
    run_sql(&mut executor, &catalog, "DELETE FROM items WHERE id = 2;");

    let mut txn2 = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn2).unwrap();
    let (results, _) = index.search(&[1.0, 0.0], 5, Some(10)).unwrap();
    txn2.commit_self().unwrap();
    assert!(
        results
            .iter()
            .all(|res| res.key != 2u64.to_be_bytes().to_vec())
    );
}

#[test]
fn dimension_mismatch_on_insert_returns_error_and_no_index_write() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());

    run_sql(
        &mut executor,
        &catalog,
        "
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, COSINE));
        CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;
    ",
    );

    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(
        &dialect,
        "INSERT INTO items (id, embedding) VALUES (1, [1.0])",
    )
    .unwrap();
    let plan_err = {
        let guard = catalog.read().unwrap();
        let planner = Planner::new(&*guard);
        planner.plan(&stmts[0]).unwrap_err()
    };
    assert!(matches!(
        plan_err,
        alopex_sql::planner::PlannerError::TypeMismatch { .. }
    ));

    // インデックスには何も入っていないことを確認
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (results, _) = index.search(&[1.0, 0.0], 1, Some(5)).unwrap();
    txn.commit_self().unwrap();
    assert!(results.is_empty());
}
