use std::fmt::Write as _;
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

fn assert_query_ids(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
    expected: &[i32],
) {
    let results = run_sql(executor, catalog, sql);
    let ExecutionResult::Query(result) = results.last().expect("query result") else {
        panic!("expected query result");
    };
    assert_eq!(
        result.rows,
        expected
            .iter()
            .map(|id| vec![alopex_sql::storage::SqlValue::Integer(*id)])
            .collect::<Vec<_>>(),
        "{sql}",
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn truncate_recreates_an_empty_hnsw_index_for_future_inserts() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    run_sql(
        &mut executor,
        &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
         CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;
         INSERT INTO items VALUES (1, [0.0, 0.0]);
         TRUNCATE items;",
    );
    {
        let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
        let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
        assert!(index.search(&[0.0, 0.0], 1, None).unwrap().0.is_empty());
        txn.commit_self().unwrap();
    }
    run_sql(
        &mut executor,
        &catalog,
        "INSERT INTO items VALUES (2, [1.0, 0.0]);",
    );
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    assert_eq!(index.search(&[1.0, 0.0], 1, None).unwrap().0.len(), 1);
    txn.commit_self().unwrap();
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_nulls_last_exact_matches_sort_prefix() {
    assert_knn_nulls_last_matches_sort_prefix(false, KnnInput::Insert);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_nulls_last_columnar_matches_sort_prefix() {
    assert_knn_nulls_last_matches_sort_prefix(
        false,
        KnnInput::Copy("id,embedding,s\n3,\"[2,0]\",1\n1,\"[0,0]\",1\n2,NULL,1\n4,\"[3,0]\",0\n"),
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_nulls_last_sorted_columnar_matches_sort_prefix() {
    assert_knn_nulls_last_matches_sort_prefix(
        false,
        KnnInput::Copy("id,embedding,s\n1,\"[0,0]\",1\n2,NULL,1\n3,\"[2,0]\",1\n4,\"[3,0]\",0\n"),
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_nulls_last_hnsw_matches_sort_prefix() {
    assert_knn_nulls_last_matches_sort_prefix(true, KnnInput::Insert);
}

enum KnnInput {
    Insert,
    Copy(&'static str),
}

fn assert_knn_nulls_last_matches_sort_prefix(indexed: bool, input: KnnInput) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    let storage = if matches!(input, KnnInput::Copy(_)) {
        " WITH (storage='columnar')"
    } else {
        ""
    };
    run_sql(
        &mut executor,
        &catalog,
        &format!(
            "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2), s INT){storage};"
        ),
    );
    if let KnnInput::Copy(csv) = input {
        use alopex_sql::executor::bulk::{
            CopyOptions, CopySecurityConfig, FileFormat, execute_copy,
        };
        use alopex_sql::storage::TxnBridge;
        use std::io::Write as _;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(csv.as_bytes()).unwrap();
        let bridge = TxnBridge::new(store);
        let mut txn = bridge.begin_write().unwrap();
        execute_copy(
            &mut txn,
            &*catalog.read().unwrap(),
            "items",
            file.path().to_str().unwrap(),
            FileFormat::Csv,
            CopyOptions { header: true },
            &CopySecurityConfig::default(),
        )
        .unwrap();
        txn.commit().unwrap();
    } else {
        run_sql(
            &mut executor,
            &catalog,
            "INSERT INTO items VALUES (1, [0.0, 0.0], 1), (2, NULL, 1), (3, [2.0, 0.0], 1), (4, [3.0, 0.0], 0);",
        );
    }
    let stored = run_sql(
        &mut executor,
        &catalog,
        "SELECT id, embedding, s FROM items ORDER BY id",
    );
    let ExecutionResult::Query(stored) = stored.last().unwrap() else {
        panic!("expected stored rows before kNN");
    };
    use alopex_sql::storage::SqlValue::{Integer, Null, Vector};
    assert_eq!(
        stored.rows,
        vec![
            vec![Integer(1), Vector(vec![0.0, 0.0]), Integer(1)],
            vec![Integer(2), Null, Integer(1)],
            vec![Integer(3), Vector(vec![2.0, 0.0]), Integer(1)],
            vec![Integer(4), Vector(vec![3.0, 0.0]), Integer(0)],
        ],
        "all input rows must survive before kNN validation"
    );
    if indexed {
        run_sql(
            &mut executor,
            &catalog,
            "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;",
        );
    }
    for (function, direction) in [("vector_distance", "ASC"), ("vector_similarity", "DESC")] {
        for nulls in ["", " NULLS LAST"] {
            for predicate in [
                "",
                " WHERE s = 1",
                " WHERE s = 0",
                " WHERE embedding IS NULL",
                " WHERE id < 0",
            ] {
                let sql = format!(
                    "SELECT id, {function}(embedding, [0.0, 0.0], 'l2') FROM items{predicate} \
                     ORDER BY {function}(embedding, [0.0, 0.0], 'l2') {direction}{nulls}"
                );
                let ordinary = run_sql(&mut executor, &catalog, &sql);
                let ExecutionResult::Query(ordinary) = ordinary.last().unwrap() else {
                    panic!("expected ordinary query result");
                };
                // Distinct non-NULL scores and one NULL avoid unspecified peer ordering.
                for k in [0, 1, 2, 3, 4, 8] {
                    let limited = format!("{sql} LIMIT {k} WITH (enable_hnsw = {indexed})");
                    if k == 4 {
                        let explained =
                            run_sql(&mut executor, &catalog, &format!("EXPLAIN {limited}"));
                        let ExecutionResult::Query(explained) = explained.last().unwrap() else {
                            panic!("expected explain result");
                        };
                        let alopex_sql::storage::SqlValue::Text(plan) = &explained.rows[0][0]
                        else {
                            panic!("expected explain text");
                        };
                        let expected_path = if indexed {
                            "HnswSearch"
                        } else {
                            "ExactKnnScan"
                        };
                        assert!(plan.contains(expected_path), "{limited}: {plan}");
                    }
                    let actual = run_sql(&mut executor, &catalog, &limited);
                    let ExecutionResult::Query(actual) = actual.last().unwrap() else {
                        panic!("expected limited query result");
                    };
                    assert_eq!(
                        actual.rows,
                        ordinary.rows[..ordinary.rows.len().min(k)],
                        "{limited}"
                    );
                }
            }
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn null_vectors_are_skipped_by_distance_and_hnsw() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());

    run_sql(
        &mut executor,
        &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
         INSERT INTO items VALUES (1, [0.0, 0.0]), (2, NULL), (3, [2.0, 0.0]);",
    );

    let rows = run_sql(
        &mut executor,
        &catalog,
        "SELECT vector_distance(embedding, [0.0, 0.0], 'l2'),
                vector_similarity(embedding, [0.0, 0.0], 'l2'),
                vector_distance([0.0, 0.0], embedding, 'l2')
         FROM items WHERE id = 2;",
    );
    let ExecutionResult::Query(result) = rows.last().expect("query result") else {
        panic!("expected query result");
    };
    assert_eq!(
        result.rows,
        vec![vec![alopex_sql::storage::SqlValue::Null; 3]]
    );

    let rows = run_sql(
        &mut executor,
        &catalog,
        "SELECT id FROM items
         ORDER BY vector_distance(embedding, [0.0, 0.0], 'l2') ASC;",
    );
    let ExecutionResult::Query(result) = rows.last().expect("query result") else {
        panic!("expected query result");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![alopex_sql::storage::SqlValue::Integer(1)],
            vec![alopex_sql::storage::SqlValue::Integer(3)],
            vec![alopex_sql::storage::SqlValue::Integer(2)],
        ]
    );

    let rows = run_sql(
        &mut executor,
        &catalog,
        "SELECT id FROM items
         ORDER BY vector_distance(embedding, [0.0, 0.0], 'l2') ASC LIMIT 2;",
    );
    let ExecutionResult::Query(result) = rows.last().expect("query result") else {
        panic!("expected query result");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![alopex_sql::storage::SqlValue::Integer(1)],
            vec![alopex_sql::storage::SqlValue::Integer(3)],
        ]
    );

    for indexed in [false, true] {
        if indexed {
            run_sql(
                &mut executor,
                &catalog,
                "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;",
            );
        }
        for function in ["vector_distance", "vector_similarity"] {
            let direction = if function == "vector_distance" {
                "ASC"
            } else {
                "DESC"
            };
            for (nulls, expected) in [("FIRST", [2, 1]), ("LAST", [1, 3])] {
                let sql = format!(
                    "SELECT id FROM items ORDER BY {function}(embedding, [0.0, 0.0], 'l2') {direction} NULLS {nulls} LIMIT 2"
                );
                assert_query_ids(&mut executor, &catalog, &sql, &expected);
            }
        }
    }
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (hits, _) = index.search(&[0.0, 0.0], 2, Some(10)).unwrap();
    assert_eq!(
        hits.into_iter().map(|hit| hit.key).collect::<Vec<_>>(),
        vec![1u64.to_be_bytes().to_vec(), 3u64.to_be_bytes().to_vec()]
    );
    txn.commit_self().unwrap();

    run_sql(
        &mut executor,
        &catalog,
        "INSERT INTO items VALUES (4, NULL);
         UPDATE items SET embedding = [1.0, 0.0] WHERE id = 2;",
    );
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    assert_eq!(
        index
            .search(&[0.0, 0.0], 4, Some(10))
            .unwrap()
            .0
            .into_iter()
            .map(|hit| hit.key)
            .collect::<Vec<_>>(),
        vec![
            1u64.to_be_bytes().to_vec(),
            2u64.to_be_bytes().to_vec(),
            3u64.to_be_bytes().to_vec()
        ],
    );
    txn.commit_self().unwrap();
    let knn_sql =
        "SELECT id FROM items ORDER BY vector_distance(embedding, [0.0, 0.0], 'l2') ASC LIMIT 4";
    assert_query_ids(&mut executor, &catalog, knn_sql, &[1, 2, 3, 4]);

    run_sql(
        &mut executor,
        &catalog,
        "UPDATE items SET embedding = NULL WHERE id = 2;",
    );
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (hits, _) = index.search(&[0.0, 0.0], 3, Some(10)).unwrap();
    assert_eq!(
        hits.into_iter().map(|hit| hit.key).collect::<Vec<_>>(),
        vec![1u64.to_be_bytes().to_vec(), 3u64.to_be_bytes().to_vec()]
    );
    txn.commit_self().unwrap();
    let results = run_sql(&mut executor, &catalog, knn_sql);
    let ExecutionResult::Query(result) = results.last().unwrap() else {
        panic!("expected query result");
    };
    assert_eq!(
        &result.rows[..2],
        &[
            vec![alopex_sql::storage::SqlValue::Integer(1)],
            vec![alopex_sql::storage::SqlValue::Integer(3)]
        ]
    );
    let mut null_ids = result.rows[2..].to_vec();
    null_ids.sort_by_key(|row| match row[0] {
        alopex_sql::storage::SqlValue::Integer(id) => id,
        _ => panic!("expected id"),
    });
    assert_eq!(
        null_ids,
        vec![
            vec![alopex_sql::storage::SqlValue::Integer(2)],
            vec![alopex_sql::storage::SqlValue::Integer(4)]
        ]
    );

    // Reinsertion after deletion must restore the same row's persisted entry.
    run_sql(
        &mut executor,
        &catalog,
        "UPDATE items SET embedding = [1.0, 0.0] WHERE id = 2;",
    );
    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    assert_eq!(
        index
            .search(&[0.0, 0.0], 4, Some(10))
            .unwrap()
            .0
            .into_iter()
            .map(|hit| hit.key)
            .collect::<Vec<_>>(),
        vec![
            1u64.to_be_bytes().to_vec(),
            2u64.to_be_bytes().to_vec(),
            3u64.to_be_bytes().to_vec()
        ],
    );
    txn.commit_self().unwrap();
    assert_query_ids(&mut executor, &catalog, knn_sql, &[1, 2, 3, 4]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_knn_hnsw_path_skips_null_vectors() {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, catalog.clone());
    let tail = ", 0.0".repeat(1_023);
    let query_vector = format!("[0{tail}]");
    let knn_sql = format!(
        "SELECT id FROM items ORDER BY vector_distance(embedding, {query_vector}, 'l2') ASC LIMIT 1;"
    );
    let nulls_first_sql = format!(
        "SELECT id FROM items ORDER BY vector_distance(embedding, {query_vector}, 'l2') ASC NULLS FIRST LIMIT 1;"
    );

    run_sql(
        &mut executor,
        &catalog,
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(1024, L2));
         INSERT INTO items VALUES (0, NULL);",
    );
    // The cost gate requires strictly more than 1,024 indexed vectors here;
    // the NULL row does not count toward the HNSW index's node count.
    for id in 1..=1_025 {
        let mut sql = String::from("INSERT INTO items VALUES (");
        write!(&mut sql, "{id}, [{}{tail}]);", id - 1).unwrap();
        run_sql(&mut executor, &catalog, &sql);
    }

    let exact = run_sql(&mut executor, &catalog, &knn_sql);
    let ExecutionResult::Query(result) = exact.last().expect("query result") else {
        panic!("expected query result");
    };
    assert_eq!(
        result.rows,
        vec![vec![alopex_sql::storage::SqlValue::Integer(1)]]
    );
    assert_query_ids(&mut executor, &catalog, &nulls_first_sql, &[0]);

    run_sql(
        &mut executor,
        &catalog,
        "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (ef_search = 1024);",
    );
    let explained = run_sql(&mut executor, &catalog, &format!("EXPLAIN {knn_sql}"));
    let ExecutionResult::Query(result) = explained.last().expect("explain result") else {
        panic!("expected explain result");
    };
    let alopex_sql::storage::SqlValue::Text(plan) = &result.rows[0][0] else {
        panic!("expected explain text");
    };
    assert!(
        plan.lines()
            .any(|line| line.trim() == "HnswSearch index=idx_items_embedding k=1"),
        "{plan}"
    );

    let indexed = run_sql(&mut executor, &catalog, &knn_sql);
    let ExecutionResult::Query(result) = indexed.last().expect("query result") else {
        panic!("expected query result");
    };
    assert_eq!(
        result.rows,
        vec![vec![alopex_sql::storage::SqlValue::Integer(1)]]
    );
    assert_query_ids(&mut executor, &catalog, &nulls_first_sql, &[0]);
    let explained = run_sql(
        &mut executor,
        &catalog,
        &format!("EXPLAIN {nulls_first_sql}"),
    );
    let ExecutionResult::Query(result) = explained.last().unwrap() else {
        panic!("expected explain result");
    };
    let alopex_sql::storage::SqlValue::Text(plan) = &result.rows[0][0] else {
        panic!("expected explain text");
    };
    assert!(!plan.contains("HnswSearch"), "{plan}");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
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
