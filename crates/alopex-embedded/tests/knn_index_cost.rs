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
use serde::{Deserialize, Serialize};

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
// Same similarity tolerance as the existing tie-aware reference oracle.
const TIE_EPSILON: f64 = 1e-7;
// Existing reference scale/search floor. With this single query and k=10,
// recall is discrete in 0.1 steps, so the floor requires a valid top-k set.
const MIN_RECALL: f64 = 0.95;

#[test]
fn result_oracle_rejects_empty_duplicate_and_wrong_neighbors() {
    let rows = SIZES[0];
    let acceptable = acceptable_ids(rows);
    let valid = acceptable
        .strict
        .iter()
        .chain(acceptable.boundary.iter().take(K - acceptable.strict.len()))
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(validate_ids(&valid, rows, &acceptable), 1.0);
    let mut tied = valid.clone();
    tied[K - 1] = *acceptable
        .boundary
        .iter()
        .find(|id| !valid.contains(id))
        .unwrap();
    assert_eq!(validate_ids(&tied, rows, &acceptable), 1.0);
    let mut duplicate = valid.clone();
    duplicate[0] = duplicate[1];
    let mut wrong = valid.clone();
    wrong[0] = (0..rows)
        .find(|id| !acceptable.strict.contains(id) && !acceptable.boundary.contains(id))
        .unwrap();
    let mut outside = valid.clone();
    outside[0] = rows;
    for invalid in [Vec::new(), duplicate, wrong, outside] {
        assert!(std::panic::catch_unwind(|| validate_ids(&invalid, rows, &acceptable)).is_err());
    }
    // Nine strictly better rows exist. Boundary ties cannot replace eight of them.
    let missing_better = std::iter::once(996)
        .chain((0..9).map(|index| 995 + index * 997))
        .collect::<Vec<_>>();
    assert!(std::panic::catch_unwind(|| validate_ids(&missing_better, rows, &acceptable)).is_err());
}

#[test]
fn raw_record_validator_requires_counterbalanced_sample_identity() {
    let source_commit = env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned());
    let run_id = env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned());
    let mut lines = Vec::new();
    for rows in SIZES {
        let acceptable = acceptable_ids(rows);
        let ids = acceptable
            .strict
            .iter()
            .chain(acceptable.boundary.iter().take(K - acceptable.strict.len()))
            .copied()
            .collect::<Vec<_>>();
        for (trial, arms) in TRIAL_ORDERS.into_iter().enumerate() {
            for (arm_order, arm) in arms.into_iter().enumerate() {
                for sample_index in 0..RUNS {
                    lines.push(
                        serde_json::json!({
                            "schema": "alopex-knn-cost-v3",
                            "kind": "sample",
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
                            "result_ids": ids,
                            "tie_aware_recall": 1.0,
                        })
                        .to_string(),
                    );
                }
                lines.push(serde_json::json!({
                    "schema": "alopex-knn-cost-v3", "kind": "worker_proof",
                    "source_commit": &source_commit, "run_id": &run_id,
                    "rows": rows, "arm": arm, "trial": trial, "arm_order": arm_order,
                    "analyzed_plan": if arm == "exact" { "ExactKnnScan" } else { "HnswSearch nodes_visited=1 distance_computations=1 ef_search=64 fallback=none" },
                }).to_string());
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
    for field in ["result_ids", "tie_aware_recall", "kind"] {
        let mut records = lines
            .iter()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        records[0][field] = match field {
            "result_ids" => serde_json::json!([]),
            "tie_aware_recall" => serde_json::json!(0.0),
            _ => serde_json::json!("unknown"),
        };
        let invalid = records
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            std::panic::catch_unwind(|| validate_raw_records(&invalid)).is_err(),
            "reject invalid {field}"
        );
    }
    let missing_proof = lines
        .iter()
        .filter(|line| !line.contains("worker_proof"))
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(std::panic::catch_unwind(|| validate_raw_records(&missing_proof)).is_err());
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

#[derive(Debug, Deserialize, Serialize)]
struct RawSample {
    schema: String,
    kind: String,
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
    result_ids: Vec<usize>,
    tie_aware_recall: f64,
}

#[derive(Debug, Deserialize)]
struct WorkerProof {
    schema: String,
    source_commit: String,
    run_id: String,
    rows: usize,
    arm: String,
    trial: usize,
    arm_order: usize,
    analyzed_plan: String,
}

fn validate_raw_records(records: &str) -> Vec<RawSample> {
    let source_commit = env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned());
    let run_id = env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned());
    let mut samples = Vec::new();
    let mut proofs: Vec<WorkerProof> = Vec::new();
    for (line_number, line) in records.lines().enumerate() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|error| {
            panic!("decode raw record at line {}: {error}", line_number + 1)
        });
        match value["kind"].as_str() {
            Some("sample") => {
                samples.push(serde_json::from_value::<RawSample>(value).expect("raw sample"))
            }
            Some("worker_proof") => {
                proofs.push(serde_json::from_value(value).expect("raw worker proof"))
            }
            _ => panic!("unknown raw record kind at line {}", line_number + 1),
        }
    }
    assert_eq!(
        samples.len(),
        SIZES.len() * TRIAL_ORDERS.len() * 2 * RUNS,
        "every rows/trial/arm/sample must persist one record"
    );

    let acceptable = SIZES
        .map(|rows| (rows, acceptable_ids(rows)))
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut seen = std::collections::BTreeSet::new();
    for sample in &samples {
        assert_eq!(sample.schema, "alopex-knn-cost-v3", "raw schema");
        assert_eq!(sample.kind, "sample");
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
        let recall = validate_ids(&sample.result_ids, sample.rows, &acceptable[&sample.rows]);
        assert_eq!(sample.tie_aware_recall, recall, "raw recall must match IDs");
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

    assert_eq!(
        proofs.len(),
        SIZES.len() * TRIAL_ORDERS.len() * 2,
        "every worker must leave a path proof"
    );
    let mut proof_seen = std::collections::BTreeSet::new();
    for proof in &proofs {
        assert_eq!(proof.schema, "alopex-knn-cost-v3");
        assert_eq!(proof.source_commit, source_commit);
        assert_eq!(proof.run_id, run_id);
        assert!(SIZES.contains(&proof.rows));
        assert!(proof.trial < TRIAL_ORDERS.len() && proof.arm_order < 2);
        assert_eq!(proof.arm, TRIAL_ORDERS[proof.trial][proof.arm_order]);
        validate_analyzed_plan(&proof.analyzed_plan, &proof.arm);
        assert!(
            proof_seen.insert((proof.rows, proof.trial, proof.arm.as_str())),
            "duplicate worker proof"
        );
    }
    for rows in SIZES {
        for (trial, arms) in TRIAL_ORDERS.into_iter().enumerate() {
            for arm in arms {
                assert!(
                    proof_seen.contains(&(rows, trial, arm)),
                    "missing worker proof"
                );
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
    let acceptable = acceptable_ids(rows);
    let exact = format!("{query} WITH (enable_hnsw = false)");
    validate_analyzed_plan(&explain_analyze(&db, &exact), "exact");
    validate_result(
        &db.execute_sql(&exact).expect("force-exact oracle"),
        rows,
        &acceptable,
    );
    db.execute_sql(&query).expect("warm query");
    // Keep the returned rows until after timing: validation and result destruction
    // are excluded. This differs from the old discarded-result timing boundary.
    for sample_index in 0..RUNS {
        let start = Instant::now();
        let result = db.execute_sql(&query).expect("execute query");
        let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
        let ids = validate_result(&result, rows, &acceptable);
        append_record(
            &raw_path,
            &RawSample {
                schema: "alopex-knn-cost-v3".into(),
                kind: "sample".into(),
                source_commit: env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned()),
                run_id: env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned()),
                surface: "embedded_sql".into(),
                phase: "search".into(),
                arm: arm.clone(),
                trial,
                arm_order,
                rows,
                dimension: DIMENSION,
                k: K,
                metric: METRIC.into(),
                warmups: 1,
                sample_index,
                elapsed_ns: (elapsed_ms * 1_000_000.0).round() as u64,
                tie_aware_recall: validate_ids(&ids, rows, &acceptable),
                result_ids: ids,
            },
        );
    }
    // Earlier samples remain durable even if this final execution/path check fails.
    let analyzed_plan = explain_analyze(&db, &query);
    validate_analyzed_plan(&analyzed_plan, &arm);
    append_record(
        &raw_path,
        &serde_json::json!({
            "schema": "alopex-knn-cost-v3", "kind": "worker_proof",
            "source_commit": env::var(SOURCE_COMMIT_ENV).unwrap_or_else(|_| "local".to_owned()),
            "run_id": env::var(RUN_ID_ENV).unwrap_or_else(|_| "local".to_owned()),
            "rows": rows, "arm": arm, "trial": trial, "arm_order": arm_order,
            "analyzed_plan": analyzed_plan,
        }),
    );
}

fn selected_usize(name: &str, upper_bound: usize) -> usize {
    let value = env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"));
    assert!(value < upper_bound, "{name}={value} is out of range");
    value
}

fn append_record(path: &PathBuf, record: &impl Serialize) {
    let mut output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open raw NDJSON");
    serde_json::to_writer(&mut output, record).expect("encode raw record");
    output.write_all(b"\n").expect("terminate raw sample");
    output.flush().expect("flush raw sample");
    output.sync_data().expect("sync raw sample");
}

#[derive(Debug)]
struct TopKOracle {
    strict: std::collections::BTreeSet<usize>,
    boundary: std::collections::BTreeSet<usize>,
}

fn acceptable_ids(rows: usize) -> TopKOracle {
    // Independent f64 oracle for the existing integer fixture and unit-axis query.
    // Neither SQL nor HNSW distance/top-k implementation is used here.
    let similarities = (0..rows)
        .map(|id| {
            let norm_squared = (0..DIMENSION)
                .map(|dimension| ((id + dimension) % 997) as f64)
                .map(|value| value * value)
                .sum::<f64>();
            (id % 997) as f64 / norm_squared.sqrt()
        })
        .collect::<Vec<_>>();
    let mut sorted = similarities.clone();
    sorted.sort_unstable_by(|a, b| b.total_cmp(a));
    let cutoff = sorted[K - 1];
    let mut oracle = TopKOracle {
        strict: Default::default(),
        boundary: Default::default(),
    };
    for (id, similarity) in similarities.into_iter().enumerate() {
        if similarity > cutoff + TIE_EPSILON {
            oracle.strict.insert(id);
        } else if similarity >= cutoff - TIE_EPSILON {
            oracle.boundary.insert(id);
        }
    }
    oracle
}

#[track_caller]
fn validate_ids(ids: &[usize], rows: usize, acceptable: &TopKOracle) -> f64 {
    assert_eq!(ids.len(), K, "query must return k rows");
    let unique = ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), K, "query must not return duplicate IDs");
    assert!(ids.iter().all(|id| *id < rows), "query ID outside fixture");
    // Maximum overlap with a valid top-k under the fixed tie tolerance: boundary
    // ties may fill only the slots not occupied by strictly better neighbors.
    let strict_hits = unique.intersection(&acceptable.strict).count();
    let boundary_hits = unique
        .intersection(&acceptable.boundary)
        .count()
        .min(K - acceptable.strict.len());
    let recall = (strict_hits + boundary_hits) as f64 / K as f64;
    assert!(
        recall >= MIN_RECALL,
        "tie-aware recall {recall} below {MIN_RECALL}; ids={ids:?}; acceptable={acceptable:?}"
    );
    recall
}

#[track_caller]
fn validate_result(result: &ExecutionResult, rows: usize, acceptable: &TopKOracle) -> Vec<usize> {
    let ExecutionResult::Query(result) = result else {
        panic!("search must return a query result");
    };
    let ids = result
        .rows
        .iter()
        .map(|row| {
            assert_eq!(row.len(), 1, "query must return only id");
            let alopex_sql::SqlValue::Integer(id) = row[0] else {
                panic!("query id must be an integer");
            };
            usize::try_from(id).expect("nonnegative query id")
        })
        .collect::<Vec<_>>();
    validate_ids(&ids, rows, acceptable);
    ids
}

fn explain_analyze(db: &Database, query: &str) -> String {
    let ExecutionResult::Query(result) = db
        .execute_sql(&format!("EXPLAIN ANALYZE {query}"))
        .expect("analyze query")
    else {
        panic!("ANALYZE must return a query result");
    };
    let alopex_sql::SqlValue::Text(plan) = &result.rows[0][0] else {
        panic!("ANALYZE must return plan text");
    };
    plan.clone()
}

fn validate_analyzed_plan(plan: &str, arm: &str) {
    if arm == "exact" {
        assert!(
            plan.contains("ExactKnnScan") && !plan.contains("HnswSearch"),
            "exact path: {plan}"
        );
    } else {
        assert_eq!(arm, "hnsw");
        assert!(
            plan.contains("HnswSearch") && !plan.contains("ExactKnnScan"),
            "hnsw path: {plan}"
        );
        let fields = plan.split_whitespace().collect::<Vec<_>>();
        assert!(
            fields.contains(&"fallback=none") && fields.contains(&"ef_search=64"),
            "hnsw controls: {plan}"
        );
        for field in ["nodes_visited=", "distance_computations="] {
            let value = fields
                .iter()
                .find_map(|token| token.strip_prefix(field))
                .expect("missing HNSW statistic")
                .parse::<u64>()
                .expect("integer HNSW statistic");
            assert!(value > 0, "HNSW must perform search work: {plan}");
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
