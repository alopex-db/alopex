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
use serde::Deserialize;

const DIMENSION: usize = 128;
const K: usize = 10;
const METRIC: &str = "COSINE";
const SIZES: [usize; 4] = [9_600, 16_000, 20_000, 40_000];
const RUNS: usize = 5;
const TRIAL_ORDERS: [[&str; 2]; 2] = [["exact", "hnsw"], ["hnsw", "exact"]];
const ROWS_ENV: &str = "ALOPEX_KNN_COST_ROWS";
const ARM_ENV: &str = "ALOPEX_KNN_COST_ARM";
const TRIAL_ENV: &str = "ALOPEX_KNN_COST_TRIAL";
const ARM_ORDER_ENV: &str = "ALOPEX_KNN_COST_ARM_ORDER";
const RAW_NDJSON_ENV: &str = "ALOPEX_KNN_COST_RAW_NDJSON";
const SOURCE_COMMIT_ENV: &str = "ALOPEX_KNN_COST_SOURCE_COMMIT";
const RUN_ID_ENV: &str = "ALOPEX_KNN_COST_RUN_ID";

#[test]
fn raw_record_validator_requires_counterbalanced_sample_identity() {
    let source_commit = env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned());
    let run_id = env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned());
    let mut lines = Vec::new();
    for rows in SIZES {
        for (trial, arms) in TRIAL_ORDERS.into_iter().enumerate() {
            for (arm_order, arm) in arms.into_iter().enumerate() {
                for sample_index in 0..RUNS {
                    lines.push(
                        serde_json::json!({
                            "schema": "alopex-knn-cost-v1",
                            "source_commit": &source_commit,
                            "run_id": &run_id,
                            "surface": "embedded_sql",
                            "phase": "search",
                            "arm": arm,
                            "trial": trial,
                            "arm_order": arm_order,
                            "rows": rows,
                            "dimension": DIMENSION,
                            "k": K,
                            "metric": METRIC,
                            "warmups": 1,
                            "sample_index": sample_index,
                            "elapsed_ns": 1,
                        })
                        .to_string(),
                    );
                }
            }
        }
    }
    let records = format!("{}\n", lines.join("\n"));
    assert_eq!(
        validate_raw_records(&records).len(),
        SIZES.len() * TRIAL_ORDERS.len() * 2 * RUNS
    );

    let duplicate = records.replacen("\"sample_index\":0", "\"sample_index\":1", 1);
    assert!(
        std::panic::catch_unwind(|| validate_raw_records(&duplicate)).is_err(),
        "validator must reject a duplicate sample index"
    );
}

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
    if let Some(parent) = raw_path.parent() {
        fs::create_dir_all(parent).expect("create raw artifact directory");
    }
    for rows in SIZES {
        for (trial, arms) in TRIAL_ORDERS.into_iter().enumerate() {
            for (arm_order, arm) in arms.into_iter().enumerate() {
                let status = Command::new(env::current_exe().expect("test executable"))
                    .arg("--ignored")
                    .arg("--exact")
                    .arg("sql_knn_hnsw_cost_process_worker")
                    .env(ROWS_ENV, rows.to_string())
                    .env(ARM_ENV, arm)
                    .env(TRIAL_ENV, trial.to_string())
                    .env(ARM_ORDER_ENV, arm_order.to_string())
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
                assert!(
                    status.success(),
                    "worker failed for rows={rows} trial={trial} arm={arm}"
                );
            }
        }
    }
    let records = fs::read_to_string(&raw_path).expect("read raw artifact");
    let samples = validate_raw_records(&records);
    let mut first_hnsw_ns = None;
    for rows in SIZES {
        for trial in 0..TRIAL_ORDERS.len() {
            let exact_ns = median_nanos(raw_samples(&samples, rows, trial, "exact"));
            let hnsw_ns = median_nanos(raw_samples(&samples, rows, trial, "hnsw"));
            assert!(
                hnsw_ns < exact_ns,
                "HNSW must beat exact at rows={rows} trial={trial}: {hnsw_ns}ns vs {exact_ns}ns"
            );
        }
        let exact_ns = median_nanos(raw_samples_for_arm(&samples, rows, "exact"));
        let hnsw_ns = median_nanos(raw_samples_for_arm(&samples, rows, "hnsw"));
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

#[derive(Debug, Deserialize)]
struct RawSample {
    schema: String,
    source_commit: String,
    run_id: String,
    surface: String,
    phase: String,
    arm: String,
    trial: usize,
    arm_order: usize,
    rows: usize,
    dimension: usize,
    k: usize,
    metric: String,
    warmups: usize,
    sample_index: usize,
    elapsed_ns: u64,
}

fn validate_raw_records(records: &str) -> Vec<RawSample> {
    let source_commit = env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned());
    let run_id = env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned());
    let samples = records
        .lines()
        .enumerate()
        .map(|(line_number, line)| {
            serde_json::from_str::<RawSample>(line).unwrap_or_else(|error| {
                panic!("decode raw record at line {}: {error}", line_number + 1)
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        samples.len(),
        SIZES.len() * TRIAL_ORDERS.len() * 2 * RUNS,
        "every rows/trial/arm/sample must persist one record"
    );

    let mut seen = std::collections::BTreeSet::new();
    for sample in &samples {
        assert_eq!(sample.schema, "alopex-knn-cost-v1", "raw schema");
        assert_eq!(sample.source_commit, source_commit, "raw source commit");
        assert_eq!(sample.run_id, run_id, "raw run identity");
        assert_eq!(sample.surface, "embedded_sql", "raw surface");
        assert_eq!(sample.phase, "search", "raw phase");
        assert!(SIZES.contains(&sample.rows), "raw rows={}", sample.rows);
        assert!(
            sample.trial < TRIAL_ORDERS.len(),
            "raw trial={}",
            sample.trial
        );
        assert!(sample.arm_order < 2, "raw arm order={}", sample.arm_order);
        assert_eq!(
            sample.arm, TRIAL_ORDERS[sample.trial][sample.arm_order],
            "raw trial/arm order"
        );
        assert_eq!(sample.dimension, DIMENSION, "raw dimension");
        assert_eq!(sample.k, K, "raw k");
        assert_eq!(sample.metric, METRIC, "raw metric");
        assert_eq!(sample.warmups, 1, "raw warmups");
        assert!(
            sample.sample_index < RUNS,
            "raw sample index={}",
            sample.sample_index
        );
        assert!(sample.elapsed_ns > 0, "raw elapsed time");
        assert!(
            seen.insert((
                sample.rows,
                sample.trial,
                sample.arm.as_str(),
                sample.sample_index
            )),
            "duplicate raw sample rows={} trial={} arm={} sample_index={}",
            sample.rows,
            sample.trial,
            sample.arm,
            sample.sample_index
        );
    }

    for rows in SIZES {
        for (trial, arms) in TRIAL_ORDERS.into_iter().enumerate() {
            for arm in arms {
                for sample_index in 0..RUNS {
                    assert!(
                        seen.contains(&(rows, trial, arm, sample_index)),
                        "missing raw sample rows={rows} trial={trial} arm={arm} sample_index={sample_index}"
                    );
                }
            }
        }
    }
    samples
}

fn raw_samples(samples: &[RawSample], rows: usize, trial: usize, arm: &str) -> Vec<u64> {
    let samples = samples
        .iter()
        .filter(|sample| sample.rows == rows && sample.trial == trial && sample.arm == arm)
        .map(|sample| sample.elapsed_ns)
        .collect::<Vec<_>>();
    assert_eq!(
        samples.len(),
        RUNS,
        "rows={rows} trial={trial} arm={arm} must persist {RUNS} samples"
    );
    samples
}

fn raw_samples_for_arm(samples: &[RawSample], rows: usize, arm: &str) -> Vec<u64> {
    let samples = samples
        .iter()
        .filter(|sample| sample.rows == rows && sample.arm == arm)
        .map(|sample| sample.elapsed_ns)
        .collect::<Vec<_>>();
    assert_eq!(
        samples.len(),
        TRIAL_ORDERS.len() * RUNS,
        "rows={rows} arm={arm} must persist every counterbalanced sample"
    );
    samples
}

fn median_nanos(mut samples: Vec<u64>) -> u64 {
    assert!(!samples.is_empty(), "median needs samples");
    samples.sort_unstable();
    let middle = samples.len() / 2;
    if samples.len() % 2 == 1 {
        samples[middle]
    } else {
        samples[middle - 1] / 2
            + samples[middle] / 2
            + (samples[middle - 1] % 2 + samples[middle] % 2) / 2
    }
}

#[test]
#[ignore = "spawned by sql_knn_hnsw_cost_process_separated_evidence"]
fn sql_knn_hnsw_cost_process_worker() {
    let rows = selected_rows();
    let arm = env::var(ARM_ENV).expect("worker arm");
    let trial = selected_usize(TRIAL_ENV, TRIAL_ORDERS.len());
    let arm_order = selected_usize(ARM_ORDER_ENV, 2);
    assert!(
        matches!(arm.as_str(), "exact" | "hnsw"),
        "unsupported arm {arm}"
    );
    assert_eq!(
        arm, TRIAL_ORDERS[trial][arm_order],
        "worker arm must match counterbalanced trial order"
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
        append_raw_sample(
            &raw_path,
            rows,
            &arm,
            trial,
            arm_order,
            sample_index,
            elapsed_ms,
        );
    }
}

fn selected_usize(name: &str, upper_bound: usize) -> usize {
    let value = env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"));
    assert!(value < upper_bound, "{name}={value} is out of range");
    value
}

fn append_raw_sample(
    path: &PathBuf,
    rows: usize,
    arm: &str,
    trial: usize,
    arm_order: usize,
    sample_index: usize,
    elapsed_ms: f64,
) {
    let record = serde_json::json!({
        "schema": "alopex-knn-cost-v1",
        "source_commit": env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned()),
        "run_id": env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned()),
        "surface": "embedded_sql",
        "phase": "search",
        "arm": arm,
        "trial": trial,
        "arm_order": arm_order,
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
