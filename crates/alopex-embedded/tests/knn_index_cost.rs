use std::fmt::Write;
use std::time::Instant;

use alopex_embedded::Database;
use alopex_sql::ExecutionResult;

const DIMENSION: usize = 128;
const K: usize = 10;
const METRIC: &str = "COSINE";
const SIZES: [usize; 4] = [9_600, 16_000, 20_000, 40_000];
const RUNS: usize = 5;

#[test]
#[ignore = "run explicitly to publish the issue #461 performance evidence"]
fn sql_knn_hnsw_cost_does_not_track_table_rows() {
    let mut first_hnsw_ms = None;

    for rows in SIZES {
        let db = Database::open_in_memory().expect("open database");
        seed(&db, "items_exact", rows, false);
        seed(&db, "items_hnsw", rows, true);

        let exact = query("items_exact");
        let hnsw = query("items_hnsw");
        assert_hnsw_path(&db, &hnsw);

        db.execute_sql(&exact).expect("warm exact query");
        db.execute_sql(&hnsw).expect("warm hnsw query");

        let exact_ms = median_millis(&db, &exact);
        let hnsw_ms = median_millis(&db, &hnsw);
        let ratio = hnsw_ms / exact_ms;
        eprintln!("rows={rows} hnsw_ms={hnsw_ms:.3} exact_ms={exact_ms:.3} ratio={ratio:.3}");

        assert!(
            ratio < 1.0,
            "HNSW must beat exact at {rows} rows: {ratio:.3}"
        );
        if let Some(first_hnsw_ms) = first_hnsw_ms {
            assert!(
                hnsw_ms < first_hnsw_ms * 2.0,
                "HNSW latency must not grow with table rows: {hnsw_ms:.3}ms vs {first_hnsw_ms:.3}ms"
            );
        } else {
            first_hnsw_ms = Some(hnsw_ms);
        }
    }
}

fn seed(db: &Database, table: &str, rows: usize, hnsw: bool) {
    db.execute_sql(&format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSION}, {METRIC}));"
    ))
    .expect("create table");
    if hnsw {
        db.execute_sql(&format!(
            "CREATE INDEX idx_{table}_embedding ON {table} (embedding) \
             USING HNSW WITH (m = 8, ef_construction = 32, ef_search = 64);"
        ))
        .expect("create hnsw index");
    }

    for start in (0..rows).step_by(128) {
        let end = (start + 128).min(rows);
        let mut sql = format!("INSERT INTO {table} (id, embedding) VALUES ");
        for row_id in start..end {
            if row_id != start {
                sql.push(',');
            }
            write!(&mut sql, "({row_id}, [").expect("write row");
            for dimension in 0..DIMENSION {
                if dimension != 0 {
                    sql.push(',');
                }
                write!(&mut sql, "{}", (row_id + dimension) % 997).expect("write vector");
            }
            sql.push_str("]) ");
        }
        db.execute_sql(&sql).expect("insert rows");
    }
}

fn query(table: &str) -> String {
    let mut vector = vec!["0"; DIMENSION];
    vector[0] = "1";
    let vector = vector.join(",");
    format!(
        "SELECT id FROM {table} ORDER BY vector_distance(embedding, [{vector}], 'cosine') ASC LIMIT {K}"
    )
}

fn assert_hnsw_path(db: &Database, query: &str) {
    let ExecutionResult::Query(result) = db
        .execute_sql(&format!("EXPLAIN {query}"))
        .expect("explain hnsw query")
    else {
        panic!("EXPLAIN must return a query result");
    };
    let alopex_sql::SqlValue::Text(plan) = &result.rows[0][0] else {
        panic!("EXPLAIN must return text: {:?}", result.rows[0][0]);
    };
    assert!(
        plan.contains("HnswSearch"),
        "the indexed query must select HnswSearch: {plan}"
    );
}

fn median_millis(db: &Database, query: &str) -> f64 {
    let mut elapsed = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let start = Instant::now();
        db.execute_sql(query).expect("execute query");
        elapsed.push(start.elapsed().as_secs_f64() * 1_000.0);
    }
    elapsed.sort_by(f64::total_cmp);
    elapsed[RUNS / 2]
}
