use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;

fn run_sql(
    sql: &str,
) -> (
    Executor<MemoryKV, MemoryCatalog>,
    Arc<RwLock<MemoryCatalog>>,
) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store, catalog.clone());
    execute_sql(&mut executor, &catalog, sql);
    (executor, catalog)
}

fn execute_sql(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) {
    let dialect = AlopexDialect;
    let stmts = Parser::parse_sql(&dialect, sql).expect("parse sql");
    for stmt in stmts {
        let plan = {
            let guard = catalog.read().unwrap();
            Planner::new(&*guard).plan(&stmt).expect("plan")
        };
        let _ = executor.execute(plan).expect("execute");
    }
}

fn explain_text(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> String {
    let stmt = Parser::parse_sql(&AlopexDialect, sql)
        .expect("parse explain")
        .pop()
        .expect("one explain statement");
    let plan = {
        let guard = catalog.read().expect("catalog lock");
        Planner::new(&*guard).plan(&stmt).expect("plan explain")
    };
    let ExecutionResult::Query(result) = executor.execute(plan).expect("execute explain") else {
        panic!("EXPLAIN must return a query result");
    };
    let alopex_sql::storage::SqlValue::Text(text) = &result.rows[0][0] else {
        panic!("EXPLAIN must return text");
    };
    text.clone()
}

fn query_ids(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Vec<i32> {
    let stmt = Parser::parse_sql(&AlopexDialect, sql)
        .expect("parse query")
        .pop()
        .expect("one query statement");
    let plan = {
        let guard = catalog.read().expect("catalog lock");
        Planner::new(&*guard).plan(&stmt).expect("plan query")
    };
    let ExecutionResult::Query(result) = executor.execute(plan).expect("execute query") else {
        panic!("kNN query must return a query result");
    };
    result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(alopex_sql::storage::SqlValue::Integer(id)) => *id,
            value => panic!("expected integer id, got {value:?}"),
        })
        .collect()
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_optimization_without_index() {
    let sql = r#"
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        INSERT INTO items (id, embedding) VALUES
            (1, [0.0, 0.0]),
            (2, [1.0, 0.0]),
            (3, [2.0, 0.0]);
    "#;
    let (mut executor, catalog) = run_sql(sql);

    let query =
        "SELECT id FROM items ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2";
    let stmt = Parser::parse_sql(&AlopexDialect, query)
        .unwrap()
        .pop()
        .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    match executor.execute(plan).unwrap() {
        ExecutionResult::Query(q) => {
            let mut ids: Vec<i32> = q
                .rows
                .iter()
                .map(|r| match &r[0] {
                    alopex_sql::storage::SqlValue::Integer(v) => *v,
                    other => panic!("unexpected {other:?}"),
                })
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, vec![1, 2]);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explain_knn_reports_exact_scan_when_small_table_skips_hnsw() {
    let sql = r#"
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;
        INSERT INTO items (id, embedding) VALUES
            (1, [0.0, 0.0]),
            (2, [1.0, 0.0]),
            (3, [2.0, 0.0]);
    "#;
    let (mut executor, catalog) = run_sql(sql);
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "EXPLAIN SELECT id FROM items ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2",
    )
    .unwrap()
    .pop()
    .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };

    let ExecutionResult::Query(result) = executor.execute(plan).unwrap() else {
        panic!("EXPLAIN must return a query result");
    };
    let alopex_sql::storage::SqlValue::Text(plan) = &result.rows[0][0] else {
        panic!("EXPLAIN must return text");
    };
    assert!(plan.starts_with("ExactKnnScan"), "{plan}");
    assert!(!plan.contains("HnswSearch"), "{plan}");

    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "EXPLAIN (FORMAT JSON) SELECT id FROM items ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2",
    )
    .unwrap()
    .pop()
    .unwrap();
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    let ExecutionResult::Query(result) = executor.execute(plan).unwrap() else {
        panic!("EXPLAIN must return a query result");
    };
    let alopex_sql::storage::SqlValue::Text(plan) = &result.rows[0][0] else {
        panic!("EXPLAIN must return text");
    };
    let document: serde_json::Value = serde_json::from_str(plan).unwrap();
    assert_eq!(document["physical_plan"]["selected_path"], "ExactKnnScan");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_query_options_reject_non_knn_query() {
    let catalog = MemoryCatalog::new();
    let stmt = Parser::parse_sql(&AlopexDialect, "SELECT 1 LIMIT 1 WITH (ef_search = 16)")
        .expect("parse SQL")
        .pop()
        .expect("one statement");

    let error = Planner::new(&catalog)
        .plan(&stmt)
        .expect_err("non-KNN query options must be rejected");
    assert!(
        error
            .to_string()
            .contains("kNN query options require ORDER BY"),
        "{error}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_query_options_reject_unsafe_ef_search_before_execution() {
    let (_executor, catalog) =
        run_sql("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));");
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "SELECT id FROM items ORDER BY vector_distance(embedding, [0.0, 0.0], 'l2') ASC LIMIT 1 WITH (ef_search = 10000000000000)",
    )
    .expect("parse SQL")
    .pop()
    .expect("one statement");

    let error = Planner::new(&*catalog.read().expect("catalog lock"))
        .plan(&stmt)
        .expect_err("unsafe ef_search must be rejected before HNSW execution");
    assert!(error.to_string().contains("ef_search"), "{error}");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn enable_hnsw_true_forces_hnsw_plan_below_cost_threshold() {
    let sql = r#"
        CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
        CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;
        INSERT INTO items (id, embedding) VALUES
            (1, [0.0, 0.0]),
            (2, [1.0, 0.0]),
            (3, [2.0, 0.0]);
    "#;
    let (mut executor, catalog) = run_sql(sql);
    let plan = explain_text(
        &mut executor,
        &catalog,
        "EXPLAIN SELECT id FROM items ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2 WITH (enable_hnsw = true)",
    );
    assert!(
        plan.contains("HnswSearch index=idx_items_embedding k=2"),
        "{plan}"
    );

    let forced_query = "SELECT id FROM items ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2 WITH (enable_hnsw = true)";
    assert_eq!(query_ids(&mut executor, &catalog, forced_query), vec![1, 2]);
    let analyzed = explain_text(
        &mut executor,
        &catalog,
        &format!("EXPLAIN ANALYZE {forced_query}"),
    );
    assert!(analyzed.contains("HnswSearch"), "{analyzed}");
    assert!(analyzed.contains("nodes_visited="), "{analyzed}");
    assert!(!analyzed.contains("nodes_visited=0"), "{analyzed}");

    let filtered_plan = explain_text(
        &mut executor,
        &catalog,
        "EXPLAIN SELECT id FROM items WHERE id = 3 ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2 WITH (enable_hnsw = true)",
    );
    assert!(
        filtered_plan.contains("HnswSearchPostFilter"),
        "{filtered_plan}"
    );
    assert!(
        filtered_plan.contains("fallback=ExactKnnScan"),
        "{filtered_plan}"
    );

    let filtered_query = "SELECT id FROM items WHERE id = 3 ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2 WITH (enable_hnsw = true)";
    assert_eq!(query_ids(&mut executor, &catalog, filtered_query), vec![3]);
    let filtered_analyzed = explain_text(
        &mut executor,
        &catalog,
        &format!("EXPLAIN ANALYZE {filtered_query}"),
    );
    assert!(
        filtered_analyzed.contains("nodes_visited="),
        "{filtered_analyzed}"
    );
    assert!(
        !filtered_analyzed.contains("nodes_visited=0"),
        "{filtered_analyzed}"
    );
    assert!(
        filtered_analyzed.contains("fallback=ExactKnnScan"),
        "{filtered_analyzed}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn enable_hnsw_true_rejects_missing_hnsw_index() {
    let (mut executor, catalog) =
        run_sql("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));");
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "SELECT id FROM items ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2 WITH (enable_hnsw = true)",
    )
    .expect("parse query")
    .pop()
    .expect("one query");
    let plan = Planner::new(&*catalog.read().expect("catalog lock"))
        .plan(&stmt)
        .expect("plan query");

    let error = executor
        .execute(plan)
        .expect_err("enable_hnsw=true without an HNSW index must be rejected");
    assert!(
        error.to_string().contains("requires an HNSW index"),
        "{error}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn enable_hnsw_true_rejects_columnar_storage() {
    let (mut executor, catalog) = run_sql(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2)) WITH (storage='columnar');",
    );
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "SELECT id FROM items ORDER BY vector_distance(embedding, [0.5, 0.0], 'l2') ASC LIMIT 2 WITH (enable_hnsw = true)",
    )
    .expect("parse query")
    .pop()
    .expect("one query");
    let plan = Planner::new(&*catalog.read().expect("catalog lock"))
        .plan(&stmt)
        .expect("plan query");

    let error = executor
        .execute(plan)
        .expect_err("enable_hnsw=true on columnar storage must be rejected");
    assert!(
        error.to_string().contains("requires row storage"),
        "{error}"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_index_accepts_search_ef_default() {
    let (_executor, catalog) = run_sql(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
         CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (ef_search=256);",
    );
    assert_eq!(
        catalog
            .read()
            .unwrap()
            .get_index("idx_items_embedding")
            .unwrap()
            .get_option("ef_search"),
        Some("256")
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_index_rejects_unsafe_ef_search_without_leaving_catalog_state() {
    let (mut executor, catalog) =
        run_sql("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));");
    let stmt = Parser::parse_sql(
        &AlopexDialect,
        "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (ef_search=10000000000000)",
    )
    .expect("parse DDL")
    .pop()
    .expect("one statement");
    let plan = Planner::new(&*catalog.read().expect("catalog lock"))
        .plan(&stmt)
        .expect("plan DDL");

    let error = executor
        .execute(plan)
        .expect_err("unsafe HNSW index ef_search must be rejected");
    assert!(error.to_string().contains("ef_search"), "{error}");
    assert!(
        catalog
            .read()
            .expect("catalog lock")
            .get_index("idx_items_embedding")
            .is_none(),
        "failed CREATE INDEX must not leave an index in the catalog"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_query_ef_search_changes_recall_against_exact_path() {
    use std::collections::BTreeSet;

    const ROWS: usize = 8_193;
    const DIMENSIONS: usize = 128;
    const BATCH_SIZE: usize = 256;
    const K: usize = 10;
    const QUERIES: usize = 4;

    // Fixed xorshift32 input, not a process-random seed or periodic row-id fixture.
    // Integer coordinates keep squared L2 sums below 128 * 255^2 < 2^24,
    // so SIMD reduction order cannot change their f32 representation.
    let mut state = 0x4620_0816u32;
    let mut next_vector = || {
        (0..DIMENSIONS)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                (state >> 24) as u8
            })
            .collect::<Vec<_>>()
    };
    let vectors = (0..ROWS).map(|_| next_vector()).collect::<Vec<_>>();
    let queries = (0..QUERIES).map(|_| next_vector()).collect::<Vec<_>>();
    assert_eq!(vectors.iter().collect::<BTreeSet<_>>().len(), ROWS);
    let vector_sql = |vector: &[u8]| {
        format!(
            "[{}]",
            vector
                .iter()
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    };

    let (mut executor, catalog) =
        run_sql("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(128, L2));");
    for start in (0..ROWS).step_by(BATCH_SIZE) {
        let end = (start + BATCH_SIZE).min(ROWS);
        let values = (start..end)
            .map(|row| format!("({}, {})", row + 1, vector_sql(&vectors[row])))
            .collect::<Vec<_>>()
            .join(", ");
        execute_sql(
            &mut executor,
            &catalog,
            &format!("INSERT INTO items (id, embedding) VALUES {values};"),
        );
    }
    execute_sql(
        &mut executor,
        &catalog,
        "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (ef_search=64);",
    );

    let mut low_recall = 0;
    let mut high_recall = 0;
    let mut observations = Vec::new();
    for (query_index, query_vector) in queries.iter().enumerate() {
        // This oracle does not call either engine's distance function or top-k code.
        let mut oracle = vectors
            .iter()
            .enumerate()
            .map(|(row, vector)| {
                let distance = vector
                    .iter()
                    .zip(query_vector)
                    .map(|(&value, &query)| {
                        let difference = i64::from(value) - i64::from(query);
                        (difference * difference) as u64
                    })
                    .sum::<u64>();
                (distance, (row + 1) as i32)
            })
            .collect::<Vec<_>>();
        oracle.sort_unstable();
        assert!(
            oracle[K - 1].0 < oracle[K].0,
            "query {query_index} must have an unambiguous top-k boundary: {:?}",
            &oracle[..=K],
        );
        let expected = oracle[..K]
            .iter()
            .map(|&(_, id)| id)
            .collect::<BTreeSet<_>>();
        let query = format!(
            "SELECT id FROM items ORDER BY vector_distance(embedding, {}, 'l2') ASC LIMIT {K}",
            vector_sql(query_vector),
        );
        let exact_sql = format!("{query} WITH (enable_hnsw = false)");
        let exact_plan = explain_text(&mut executor, &catalog, &format!("EXPLAIN {exact_sql}"));
        assert!(exact_plan.starts_with("ExactKnnScan\n"), "{exact_plan}");
        let exact = query_ids(&mut executor, &catalog, &exact_sql);
        assert_eq!(exact.len(), K, "query {query_index}");
        assert_eq!(exact.iter().copied().collect::<BTreeSet<_>>(), expected);

        for (ef, recall) in [(K, &mut low_recall), (ROWS, &mut high_recall)] {
            let sql = format!("{query} WITH (ef_search = {ef})");
            let results = query_ids(&mut executor, &catalog, &sql);
            let actual = results.iter().copied().collect::<BTreeSet<_>>();
            assert_eq!(results.len(), K, "query {query_index}, ef={ef}");
            assert_eq!(
                actual.len(),
                K,
                "duplicate IDs: query {query_index}, ef={ef}"
            );
            assert!(actual.iter().all(|&id| (1..=ROWS as i32).contains(&id)));
            *recall += actual.intersection(&expected).count();
            if ef == ROWS {
                assert_eq!(
                    actual,
                    expected,
                    "query {query_index}, oracle={:?}",
                    &oracle[..=K]
                );
            }
            observations.push((query_index, ef, results, oracle[..=K].to_vec()));
            let plan = explain_text(&mut executor, &catalog, &format!("EXPLAIN ANALYZE {sql}"));
            assert!(
                plan.contains("HnswSearch index=idx_items_embedding k=10"),
                "{plan}"
            );
            assert!(
                plan.contains(&format!("ef_search={ef} fallback=none")),
                "{plan}"
            );
            assert!(
                plan.contains("nodes_visited=") && !plan.contains("nodes_visited=0"),
                "{plan}"
            );
        }
    }
    assert_eq!(high_recall, QUERIES * K);
    assert!(
        high_recall > low_recall,
        "higher ef_search must improve aggregate recall: low={low_recall}, high={high_recall}, observations={observations:?}"
    );
    eprintln!(
        "ef_search recall over {QUERIES} fixed queries: low={low_recall}/{}, high={high_recall}/{}",
        QUERIES * K,
        QUERIES * K
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explain_hnsw_replaces_logical_sort_and_scan_nodes() {
    const ROWS: u64 = 8_193;
    const DIMENSIONS: usize = 128;
    const BATCH_SIZE: u64 = 256;

    let (mut executor, catalog) =
        run_sql("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(128, L2));");
    let vector = format!(
        "[{}]",
        std::iter::repeat_n("0.0", DIMENSIONS)
            .collect::<Vec<_>>()
            .join(", ")
    );
    for start in (1..=ROWS).step_by(BATCH_SIZE as usize) {
        let end = (start + BATCH_SIZE - 1).min(ROWS);
        let values = (start..=end)
            .map(|id| format!("({id}, {vector})"))
            .collect::<Vec<_>>()
            .join(", ");
        execute_sql(
            &mut executor,
            &catalog,
            &format!("INSERT INTO items (id, embedding) VALUES {values};"),
        );
    }
    execute_sql(
        &mut executor,
        &catalog,
        "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW;",
    );

    let query = format!(
        "SELECT id FROM items ORDER BY vector_distance(embedding, {vector}, 'l2') ASC LIMIT 10"
    );
    let indexed = explain_text(&mut executor, &catalog, &format!("EXPLAIN {query}"));
    assert!(
        indexed.contains("Limit table=items\n  HnswSearch index=idx_items_embedding k=10\n"),
        "{indexed}"
    );
    assert!(!indexed.contains("Sort table=items"), "{indexed}");
    assert!(!indexed.contains("Scan table=items"), "{indexed}");

    let filtered = explain_text(
        &mut executor,
        &catalog,
        &format!(
            "EXPLAIN SELECT id FROM items WHERE id > 0 ORDER BY vector_distance(embedding, {vector}, 'l2') ASC LIMIT 10"
        ),
    );
    assert!(
        filtered.contains(
            "Limit table=items\n  Filter table=items\n    HnswSearchPostFilter index=idx_items_embedding k=10 fallback=ExactKnnScan\n"
        ),
        "{filtered}"
    );
    assert!(!filtered.contains("Sort table=items"), "{filtered}");
    assert!(!filtered.contains("Scan table=items"), "{filtered}");

    let analyzed = explain_text(&mut executor, &catalog, &format!("EXPLAIN ANALYZE {query}"));
    assert!(
        analyzed.contains("Limit table=items\n  HnswSearch index=idx_items_embedding k=10\n"),
        "{analyzed}"
    );
    assert!(analyzed.contains("elapsed_ns="), "{analyzed}");
    assert!(analyzed.contains("rows="), "{analyzed}");

    execute_sql(&mut executor, &catalog, "DROP INDEX idx_items_embedding;");
    let exact = explain_text(&mut executor, &catalog, &format!("EXPLAIN {query}"));
    assert!(exact.starts_with("ExactKnnScan\n"), "{exact}");
    assert!(exact.contains("Sort table=items"), "{exact}");
    assert!(exact.contains("Scan table=items"), "{exact}");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explain_analyze_reports_hnsw_search_statistics_and_fallback() {
    const ROWS: u64 = 8_193;
    const DIMENSIONS: usize = 128;
    const BATCH_SIZE: u64 = 256;

    let (mut executor, catalog) =
        run_sql("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(128, L2));");
    let vector = format!(
        "[{}]",
        std::iter::repeat_n("0.0", DIMENSIONS)
            .collect::<Vec<_>>()
            .join(", ")
    );
    for start in (1..=ROWS).step_by(BATCH_SIZE as usize) {
        let end = (start + BATCH_SIZE - 1).min(ROWS);
        let values = (start..=end)
            .map(|id| format!("({id}, {vector})"))
            .collect::<Vec<_>>()
            .join(", ");
        execute_sql(
            &mut executor,
            &catalog,
            &format!("INSERT INTO items (id, embedding) VALUES {values};"),
        );
    }
    execute_sql(
        &mut executor,
        &catalog,
        "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (ef_search=64);",
    );

    let query = format!(
        "SELECT id FROM items ORDER BY vector_distance(embedding, {vector}, 'l2') ASC LIMIT 10"
    );
    let indexed = explain_text(&mut executor, &catalog, &format!("EXPLAIN ANALYZE {query}"));
    assert!(
        indexed.contains("HnswSearch index=idx_items_embedding k=10\n    nodes_visited="),
        "{indexed}"
    );
    assert!(indexed.contains("distance_computations="), "{indexed}");
    assert!(indexed.contains("search_time_us="), "{indexed}");
    assert!(indexed.contains("ef_search=64 fallback=none"), "{indexed}");
    assert!(!indexed.contains("nodes_visited=0"), "{indexed}");

    let post_filter_without_fallback = explain_text(
        &mut executor,
        &catalog,
        &format!(
            "EXPLAIN ANALYZE SELECT id FROM items WHERE id >= 0 ORDER BY vector_distance(embedding, {vector}, 'l2') ASC LIMIT 1"
        ),
    );
    assert!(
        post_filter_without_fallback.contains(
            "HnswSearchPostFilter index=idx_items_embedding k=1 fallback=ExactKnnScan\n      nodes_visited="
        ),
        "{post_filter_without_fallback}"
    );
    assert!(
        post_filter_without_fallback.contains("ef_search=64 fallback=none"),
        "{post_filter_without_fallback}"
    );
    assert!(
        !post_filter_without_fallback.contains("nodes_visited=0"),
        "{post_filter_without_fallback}"
    );

    let overridden = explain_text(
        &mut executor,
        &catalog,
        &format!("EXPLAIN ANALYZE {query} WITH (ef_search = 16)"),
    );
    assert!(
        overridden.contains("ef_search=16 fallback=none"),
        "{overridden}"
    );

    let forced_exact = explain_text(
        &mut executor,
        &catalog,
        &format!("EXPLAIN {query} WITH (enable_hnsw = false)"),
    );
    assert!(forced_exact.starts_with("ExactKnnScan\n"), "{forced_exact}");

    let post_filter = explain_text(
        &mut executor,
        &catalog,
        &format!(
            "EXPLAIN ANALYZE SELECT id FROM items WHERE id = 0 ORDER BY vector_distance(embedding, {vector}, 'l2') ASC LIMIT 10"
        ),
    );
    assert!(
        post_filter.contains(
            "HnswSearchPostFilter index=idx_items_embedding k=10 fallback=ExactKnnScan\n      nodes_visited="
        ),
        "{post_filter}"
    );
    assert!(
        post_filter.contains("ef_search=64 fallback=ExactKnnScan"),
        "{post_filter}"
    );

    execute_sql(&mut executor, &catalog, "DROP INDEX idx_items_embedding;");
    let exact = explain_text(&mut executor, &catalog, &format!("EXPLAIN ANALYZE {query}"));
    assert!(exact.starts_with("ExactKnnScan\n"), "{exact}");
    assert!(
        exact.contains(
            "nodes_visited=0 distance_computations=0 search_time_us=0 ef_search=none fallback=none"
        ),
        "{exact}"
    );
}
