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
