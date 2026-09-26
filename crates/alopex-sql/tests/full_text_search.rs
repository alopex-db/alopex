use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, QueryResult};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn run(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> Result<Option<QueryResult>, String> {
    let mut result = None;
    for statement in Parser::parse_sql(&AlopexDialect, sql).map_err(|error| error.to_string())? {
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .map_err(|error| error.to_string())?;
        if let ExecutionResult::Query(query) =
            executor.execute(plan).map_err(|error| error.to_string())?
        {
            result = Some(query);
        }
    }
    Ok(result)
}

fn setup() -> (
    Executor<MemoryKV, MemoryCatalog>,
    Arc<RwLock<MemoryCatalog>>,
) {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    (
        Executor::new(Arc::new(MemoryKV::new()), Arc::clone(&catalog)),
        catalog,
    )
}

#[test]
fn roadmap_functions_are_deterministic_and_bounded() {
    let (mut executor, catalog) = setup();
    let result = run(
        &mut executor,
        &catalog,
        "SELECT TO_TSVECTOR('simple', 'Hello hello 世界') AS vector_value,
                TO_TSQUERY('simple', 'hello & 世界') AS query,
                PLAINTO_TSQUERY('simple', 'hello 世界') AS plain,
                WEBSEARCH_TO_TSQUERY('simple', '\"hello world\" -fox OR cat') AS web,
                TS_RANK(TO_TSVECTOR('the quick brown fox'),
                        PLAINTO_TSQUERY('quick fox')) AS rank,
                TS_HEADLINE('the quick brown fox', PLAINTO_TSQUERY('quick fox')) AS headline",
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Text("'hello':1,2 '世界':3".into()),
            SqlValue::Text("hello & 世界".into()),
            SqlValue::Text("hello & 世界".into()),
            SqlValue::Text("hello <-> world & !fox | cat".into()),
            SqlValue::Double(0.5),
            SqlValue::Text("the <b>quick</b> brown <b>fox</b>".into()),
        ]]
    );

    let malformed = run(
        &mut executor,
        &catalog,
        "SELECT TO_TSQUERY('simple', 'hello &')",
    )
    .unwrap_err();
    assert!(malformed.contains("TSQUERY"), "{malformed}");
}

#[test]
fn fts_index_and_scan_paths_remain_equivalent_across_lifecycle() {
    let (mut executor, catalog) = setup();
    run(
        &mut executor,
        &catalog,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO docs VALUES
           (1, 'the quick brown fox'),
           (2, 'quick database search'),
           (3, 'unrelated text')",
    )
    .unwrap();

    let query = "SELECT row_id, document, rank FROM
                 FTS_SEARCH('docs', 'body', 'quick') AS f
                 ORDER BY row_id";
    let scan_rows = run(&mut executor, &catalog, query).unwrap().unwrap().rows;

    run(
        &mut executor,
        &catalog,
        "CREATE INDEX docs_body_fts ON docs(body) USING FTS",
    )
    .unwrap();
    assert_eq!(
        catalog
            .read()
            .unwrap()
            .get_index("docs_body_fts")
            .unwrap()
            .get_option("fts_format_version"),
        Some("1")
    );
    let indexed_rows = run(&mut executor, &catalog, query).unwrap().unwrap().rows;
    assert_eq!(indexed_rows, scan_rows);

    run(
        &mut executor,
        &catalog,
        "UPDATE docs SET body = 'slow database' WHERE id = 2;
         DELETE FROM docs WHERE id = 1",
    )
    .unwrap();
    assert!(
        run(&mut executor, &catalog, query)
            .unwrap()
            .unwrap()
            .rows
            .is_empty()
    );

    run(
        &mut executor,
        &catalog,
        "INSERT INTO docs VALUES (4, 'quick rebuild');
         DROP INDEX docs_body_fts;
         CREATE INDEX docs_body_fts ON docs(body) USING FTS",
    )
    .unwrap();
    assert_eq!(
        run(&mut executor, &catalog, query).unwrap().unwrap().rows,
        vec![vec![
            SqlValue::BigInt(4),
            SqlValue::Text("quick rebuild".into()),
            SqlValue::Double(0.5),
        ]]
    );

    run(
        &mut executor,
        &catalog,
        "DROP INDEX docs_body_fts; INSERT INTO docs VALUES (5, 'searches records')",
    )
    .unwrap();
    let english =
        "SELECT row_id FROM FTS_SEARCH('docs', 'body', 'search', 'english') ORDER BY row_id";
    let scan_rows = run(&mut executor, &catalog, english).unwrap().unwrap().rows;
    assert_eq!(scan_rows, vec![vec![SqlValue::BigInt(5)]]);
    run(
        &mut executor,
        &catalog,
        "CREATE INDEX docs_body_fts ON docs(body) USING FTS WITH (config = english)",
    )
    .unwrap();
    assert_eq!(
        run(&mut executor, &catalog, english).unwrap().unwrap().rows,
        scan_rows
    );
}

#[test]
fn fts_search_exposes_composite_primary_key_for_duplicate_documents() {
    let (mut executor, catalog) = setup();
    run(
        &mut executor,
        &catalog,
        "CREATE TABLE reviews (
             review_id INTEGER,
             source TEXT,
             body TEXT,
             PRIMARY KEY (review_id, source)
         );
         INSERT INTO reviews VALUES (10, 'market', 'recomendo'), (10, 'store', 'recomendo');
         CREATE INDEX reviews_body_fts ON reviews(body) USING FTS",
    )
    .unwrap();

    let result = run(
        &mut executor,
        &catalog,
        "SELECT r.review_id, r.source
         FROM FTS_SEARCH('reviews', 'body', 'recomendo')
              AS f(row_id, document, rank, headline)
         JOIN reviews AS r ON r.review_id = f.review_id AND r.source = f.source
         ORDER BY r.source",
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(10), SqlValue::Text("market".into())],
            vec![SqlValue::Integer(10), SqlValue::Text("store".into())],
        ]
    );
}

#[test]
fn ts_match_operator_matches_ts_rank() {
    let (mut executor, catalog) = setup();
    run(
        &mut executor,
        &catalog,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO docs VALUES
           (1, 'the quick brown fox'),
           (2, 'quick database search'),
           (3, 'unrelated text');
         CREATE INDEX docs_body_fts ON docs(body) USING FTS",
    )
    .unwrap();

    let matches = run(
        &mut executor,
        &catalog,
        "SELECT id FROM docs
         WHERE TO_TSVECTOR(body) @@ PLAINTO_TSQUERY('quick')
         ORDER BY id",
    )
    .unwrap()
    .unwrap();
    let ranked = run(
        &mut executor,
        &catalog,
        "SELECT id FROM docs
         WHERE TS_RANK(TO_TSVECTOR(body), PLAINTO_TSQUERY('quick')) > 0
         ORDER BY id",
    )
    .unwrap()
    .unwrap();

    assert_eq!(matches.rows, ranked.rows);
    assert_eq!(
        matches.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );

    let row_dependent_query = run(
        &mut executor,
        &catalog,
        "SELECT id FROM docs
         WHERE TO_TSVECTOR(body) @@ PLAINTO_TSQUERY(body)
         ORDER BY id",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        row_dependent_query.rows,
        vec![
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
        ]
    );
}
