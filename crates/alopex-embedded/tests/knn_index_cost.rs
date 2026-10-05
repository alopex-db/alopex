use std::env;
use std::fmt::Write;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

use alopex_embedded::Database;
use alopex_sql::ExecutionResult;

const DIMENSION: usize = 128;
const K: usize = 10;
const METRIC: &str = "COSINE";
const SIZES: [usize; 4] = [9_600, 16_000, 20_000, 40_000];
const RUNS: usize = 5;
const ROWS_ENV: &str = "ALOPEX_KNN_COST_ROWS";
const RESULT_ENV: &str = "ALOPEX_KNN_COST_RESULT";
const ARM_ENV: &str = "ALOPEX_KNN_COST_ARM";
const RAW_NDJSON_ENV: &str = "ALOPEX_KNN_COST_RAW_NDJSON";
const SOURCE_COMMIT_ENV: &str = "ALOPEX_KNN_COST_SOURCE_COMMIT";
const RUN_ID_ENV: &str = "ALOPEX_KNN_COST_RUN_ID";

#[test]
#[ignore = "run by parity-performance to publish issue #461 evidence"]
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

        let exact_samples = elapsed_millis(&db, &exact);
        let hnsw_samples = elapsed_millis(&db, &hnsw);
        let exact_ms = median_millis(&exact_samples);
        let hnsw_ms = median_millis(&hnsw_samples);
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

#[test]
#[ignore = "run one row count per process to publish issue #461 raw samples"]
fn sql_knn_hnsw_cost_single_size_writes_raw_samples() {
    let rows = selected_rows();
    let output = env::var_os(RESULT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("{RESULT_ENV} must name a raw-result file"));
    let db = Database::open_in_memory().expect("open database");
    seed(&db, "items_exact", rows, false);
    seed(&db, "items_hnsw", rows, true);

    let exact = query("items_exact");
    let hnsw = query("items_hnsw");
    assert_hnsw_path(&db, &hnsw);
    db.execute_sql(&exact).expect("warm exact query");
    db.execute_sql(&hnsw).expect("warm hnsw query");

    let exact_samples = elapsed_millis(&db, &exact);
    let hnsw_samples = elapsed_millis(&db, &hnsw);
    let exact_ms = median_millis(&exact_samples);
    let hnsw_ms = median_millis(&hnsw_samples);
    let ratio = hnsw_ms / exact_ms;
    fs::write(
        &output,
        format!(
            "{{\"rows\":{rows},\"metric\":\"{METRIC}\",\"exact_ms\":[{}],\"hnsw_ms\":[{}],\"exact_median_ms\":{exact_ms:.6},\"hnsw_median_ms\":{hnsw_ms:.6},\"ratio\":{ratio:.6}}}\n",
            samples_json(&exact_samples),
            samples_json(&hnsw_samples),
        ),
    )
    .expect("persist raw cost samples");

    assert!(
        ratio < 1.0,
        "HNSW must beat exact at {rows} rows: {ratio:.3}"
    );
}

fn selected_rows() -> usize {
    let rows = env::var(ROWS_ENV)
        .unwrap_or_else(|_| panic!("{ROWS_ENV} must select one supported row count"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{ROWS_ENV} must be an unsigned row count"));
    assert!(
        SIZES.contains(&rows),
        "{ROWS_ENV}={rows} is not one of {SIZES:?}"
    );
    rows
}

fn samples_json(samples: &[f64]) -> String {
    samples
        .iter()
        .map(|sample| format!("{sample:.6}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[test]
#[ignore = "run one SQL kNN arm per process to publish issue #461 raw evidence"]
fn sql_knn_hnsw_cost_process_separated_evidence() {
    let raw_path = env::var_os(RAW_NDJSON_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("{RAW_NDJSON_ENV} must name a new NDJSON file"));
    assert!(
        !raw_path.exists(),
        "{RAW_NDJSON_ENV} must not overwrite an existing artifact"
    );
    for rows in SIZES {
        for arm in ["exact", "hnsw"] {
            let status = Command::new(env::current_exe().expect("test executable"))
                .arg("--ignored")
                .arg("--exact")
                .arg("sql_knn_hnsw_cost_process_worker")
                .env(ROWS_ENV, rows.to_string())
                .env(ARM_ENV, arm)
                .env(RAW_NDJSON_ENV, &raw_path)
                .env(
                    SOURCE_COMMIT_ENV,
                    env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned()),
                )
                .env(
                    RUN_ID_ENV,
                    env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned()),
                )
                .status()
                .expect("start worker");
            assert!(status.success(), "worker failed for rows={rows} arm={arm}");
        }
    }
    let records = fs::read_to_string(&raw_path).expect("read raw artifact");
    assert_eq!(
        records.lines().count(),
        SIZES.len() * 2 * RUNS,
        "every rows/arm/sample must persist one record"
    );
    let mut first_hnsw_ns = None;
    for rows in SIZES {
        let exact_ns = median_nanos(raw_samples(&records, rows, "exact"));
        let hnsw_ns = median_nanos(raw_samples(&records, rows, "hnsw"));
        assert!(
            hnsw_ns < exact_ns,
            "HNSW must beat exact at {rows} rows: {hnsw_ns}ns vs {exact_ns}ns"
        );
        if let Some(first_hnsw_ns) = first_hnsw_ns {
            assert!(
                hnsw_ns < first_hnsw_ns * 2,
                "HNSW latency must not grow with table rows: {hnsw_ns}ns vs {first_hnsw_ns}ns"
            );
        } else {
            first_hnsw_ns = Some(hnsw_ns);
        }
    }
}

fn raw_samples(records: &str, rows: usize, arm: &str) -> Vec<u64> {
    let samples = records
        .lines()
        .filter_map(|line| {
            let record: serde_json::Value = serde_json::from_str(line).expect("decode raw record");
            (record["rows"].as_u64() == Some(rows as u64) && record["arm"].as_str() == Some(arm))
                .then(|| record["elapsed_ns"].as_u64().expect("raw elapsed_ns"))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        samples.len(),
        RUNS,
        "rows={rows} arm={arm} must persist {RUNS} samples"
    );
    samples
}

fn median_nanos(mut samples: Vec<u64>) -> u64 {
    samples.sort_unstable();
    samples[RUNS / 2]
}

#[test]
#[ignore = "spawned by sql_knn_hnsw_cost_process_separated_evidence"]
fn sql_knn_hnsw_cost_process_worker() {
    let rows = selected_rows();
    let arm = env::var(ARM_ENV).expect("worker arm");
    assert!(
        matches!(arm.as_str(), "exact" | "hnsw"),
        "unsupported arm {arm}"
    );
    let raw_path = env::var_os(RAW_NDJSON_ENV)
        .map(PathBuf::from)
        .expect("worker raw NDJSON path");
    let db = Database::open_in_memory().expect("open database");
    let table = format!("items_{arm}");
    seed(&db, &table, rows, arm == "hnsw");
    let query = query(&table);
    if arm == "hnsw" {
        assert_hnsw_path(&db, &query);
    }
    db.execute_sql(&query).expect("warm query");
    for (sample_index, elapsed_ms) in elapsed_millis(&db, &query).into_iter().enumerate() {
        append_raw_sample(&raw_path, rows, &arm, sample_index, elapsed_ms);
    }
}

fn append_raw_sample(path: &PathBuf, rows: usize, arm: &str, sample_index: usize, elapsed_ms: f64) {
    let record = serde_json::json!({
        "schema": "alopex-knn-cost-v1",
        "source_commit": env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned()),
        "run_id": env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned()),
        "surface": "embedded_sql",
        "phase": "search",
        "arm": arm,
        "rows": rows,
        "dimension": DIMENSION,
        "k": K,
        "metric": METRIC,
        "warmups": 1,
        "sample_index": sample_index,
        "elapsed_ns": (elapsed_ms * 1_000_000.0).round() as u64,
    });
    let mut output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open raw NDJSON");
    serde_json::to_writer(&mut output, &record).expect("encode raw sample");
    output.write_all(b"\n").expect("terminate raw sample");
    output.flush().expect("flush raw sample");
    output.sync_data().expect("sync raw sample");
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

fn elapsed_millis(db: &Database, query: &str) -> Vec<f64> {
    let mut elapsed = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let start = Instant::now();
        db.execute_sql(query).expect("execute query");
        elapsed.push(start.elapsed().as_secs_f64() * 1_000.0);
    }
    elapsed
}

fn median_millis(samples: &[f64]) -> f64 {
    let mut elapsed = samples.to_vec();
    elapsed.sort_by(f64::total_cmp);
    elapsed[RUNS / 2]
}
