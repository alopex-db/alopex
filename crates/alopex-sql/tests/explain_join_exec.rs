use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

fn run(
    executor: &mut Executor<MemoryKV, MemoryCatalog>,
    catalog: &Arc<RwLock<MemoryCatalog>>,
    sql: &str,
) -> ExecutionResult {
    let mut statements =
        Parser::parse_sql(&AlopexDialect, sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    assert_eq!(statements.len(), 1, "{sql}");
    let plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statements.remove(0))
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    executor
        .execute(plan)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

#[test]
fn explain_and_analyze_execute_join_forms_and_nested_queries() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), catalog.clone());
    for table in ["a", "b", "c", "d"] {
        run(
            &mut executor,
            &catalog,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
        );
        run(
            &mut executor,
            &catalog,
            &format!("INSERT INTO {table} VALUES (1)"),
        );
    }

    let queries = [
        "SELECT a.id FROM a INNER JOIN b ON a.id = b.id",
        "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id",
        "SELECT a.id FROM a FULL OUTER JOIN b ON a.id = b.id",
        "SELECT a.id FROM a JOIN b USING (id)",
        "SELECT id FROM a NATURAL JOIN b",
        "SELECT a.id FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
        "SELECT a.id FROM a NATURAL JOIN b JOIN c ON b.id = c.id",
        "SELECT q.id FROM (SELECT id FROM a NATURAL JOIN b) q JOIN c ON q.id = c.id",
        "WITH q AS (SELECT id FROM a NATURAL JOIN b) SELECT q.id FROM q JOIN c ON q.id = c.id",
        "SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.id IN (SELECT c.id FROM c JOIN d ON c.id = d.id)",
    ];
    for query in queries {
        let ExecutionResult::Query(rows) = run(&mut executor, &catalog, query) else {
            panic!("expected SELECT rows: {query}");
        };
        assert_eq!(rows.rows.len(), 1, "{query}");

        for prefix in ["EXPLAIN", "EXPLAIN ANALYZE"] {
            let sql = format!("{prefix} {query}");
            let ExecutionResult::Query(plan) = run(&mut executor, &catalog, &sql) else {
                panic!("expected plan rows: {sql}");
            };
            let [row] = plan.rows.as_slice() else {
                panic!("expected one plan row: {sql}: {:?}", plan.rows);
            };
            let [SqlValue::Text(text)] = row.as_slice() else {
                panic!("expected plan text: {sql}: {row:?}");
            };
            assert!(text.contains("Join"), "{sql}: {text}");
            if prefix == "EXPLAIN ANALYZE" {
                assert!(text.contains("rows=1"), "{sql}: {text}");
            }
        }
    }
}
