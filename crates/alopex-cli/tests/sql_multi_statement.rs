//! E2E tests for multi-statement SQL output (GitHub issue #26).
//!
//! Contract:
//! - `--output json` for the `sql` command always emits an array of
//!   per-statement result sets (a single statement yields a 1-element array).
//! - DDL/DML status results (`status`/`message`) are included as one result
//!   set per statement. `--quiet` omits status result sets.
//! - table/csv/tsv/jsonl emit one result block per statement, in order.
//! - Any failing statement makes the whole invocation exit non-zero.

use std::io::Write;
use std::process::{Command, Output, Stdio};

fn run_alopex(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_alopex"))
        .args(args)
        .output()
        .expect("spawn alopex")
}

fn run_alopex_with_stdin(args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_alopex"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn alopex");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input)
        .expect("write stdin");
    child.wait_with_output().expect("wait for alopex")
}

fn parse_json_stdout(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "status: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout)
        .unwrap_or_else(|err| panic!("stdout should be valid JSON: {err}\nstdout:\n{stdout}"))
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_multi_statement_emits_result_set_per_statement() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "SELECT 1 AS a; SELECT 2 AS b; SELECT 3 AS c",
    ]);

    let value = parse_json_stdout(&output);
    let sets = value.as_array().expect("top-level JSON array");
    assert_eq!(sets.len(), 3, "one result set per statement: {value}");

    let expectations = [("a", 1), ("b", 2), ("c", 3)];
    for (set, (column, expected)) in sets.iter().zip(expectations) {
        let rows = set.as_array().expect("result set is an array of rows");
        assert_eq!(rows.len(), 1, "result set: {set}");
        assert_eq!(
            rows[0].get(column).and_then(|v| v.as_i64()),
            Some(expected),
            "result set: {set}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_single_statement_is_one_element_array() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "SELECT 1 AS a",
    ]);

    let value = parse_json_stdout(&output);
    let sets = value.as_array().expect("top-level JSON array");
    assert_eq!(sets.len(), 1, "single statement yields 1 element: {value}");
    let rows = sets[0].as_array().expect("result set is an array of rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a").and_then(|v| v.as_i64()), Some(1));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_portable_metadata_uses_query_result_sets() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--quiet",
        "--output",
        "json",
        "sql",
        "CREATE TABLE items (id BIGINT, label TEXT); \
         SHOW TABLES; \
         DESCRIBE items; \
         SELECT table_name, column_name, ordinal_position \
         FROM information_schema.columns ORDER BY ordinal_position",
    ]);

    let value = parse_json_stdout(&output);
    let sets = value.as_array().expect("top-level JSON array");
    assert_eq!(
        sets.len(),
        3,
        "DDL is quiet while metadata remains query output"
    );
    assert_eq!(sets[0], serde_json::json!([{ "table_name": "items" }]));
    assert_eq!(
        sets[1],
        serde_json::json!([
            {"column_name":"id","column_type":"BIGINT","null":"YES","key":"","default":null,"extra":""},
            {"column_name":"label","column_type":"TEXT","null":"YES","key":"","default":null,"extra":""}
        ])
    );
    assert_eq!(
        sets[2],
        serde_json::json!([
            {"table_name":"items","column_name":"id","ordinal_position":1},
            {"table_name":"items","column_name":"label","ordinal_position":2}
        ])
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_lag_and_lead_preserve_exact_rows() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--quiet",
        "--output",
        "json",
        "sql",
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, region TEXT, value INTEGER); \
         INSERT INTO samples VALUES (1, 'east', 10); \
         INSERT INTO samples VALUES (2, 'east', 20); \
         INSERT INTO samples VALUES (3, 'west', 30); \
         INSERT INTO samples VALUES (4, 'west', 40); \
         SELECT id, \
                LAG(value, 1, -1) OVER (PARTITION BY region ORDER BY id) AS previous, \
                LEAD(value) OVER (PARTITION BY region ORDER BY id) AS following, \
                value - LAG(value, 1, value) OVER (PARTITION BY region ORDER BY id) AS delta \
         FROM samples ORDER BY id",
    ]);

    let value = parse_json_stdout(&output);
    assert_eq!(
        value,
        serde_json::json!([[
            {"id": 1, "previous": -1, "following": 20, "delta": 0},
            {"id": 2, "previous": 10, "following": null, "delta": 10},
            {"id": 3, "previous": -1, "following": 40, "delta": 0},
            {"id": 4, "previous": 30, "following": null, "delta": 10}
        ]])
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_grouped_window_composition_preserves_exact_rows() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--quiet",
        "--output",
        "json",
        "sql",
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, region TEXT, value INTEGER); \
         INSERT INTO samples VALUES (1, 'east', 10), (2, 'east', 20); \
         INSERT INTO samples VALUES (3, 'west', 30), (4, 'west', 40); \
         SELECT region, SUM(value) AS total, \
                RANK() OVER (ORDER BY SUM(value) DESC) AS sales_rank, \
                SUM(SUM(value)) OVER () AS retained_total \
         FROM samples GROUP BY region HAVING SUM(value) >= 30 \
         ORDER BY sales_rank, region",
    ]);

    assert_eq!(
        parse_json_stdout(&output),
        serde_json::json!([[{
            "region": "west", "total": 70, "sales_rank": 1, "retained_total": 100
        }, {
            "region": "east", "total": 30, "sales_rank": 2, "retained_total": 100
        }]])
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_includes_ddl_dml_status_per_statement() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "CREATE TABLE t (id INTEGER PRIMARY KEY); \
         INSERT INTO t (id) VALUES (1); \
         SELECT id FROM t",
    ]);

    let value = parse_json_stdout(&output);
    let sets = value.as_array().expect("top-level JSON array");
    assert_eq!(sets.len(), 3, "one result set per statement: {value}");

    let ddl = sets[0].as_array().expect("DDL status result set");
    assert_eq!(ddl.len(), 1);
    assert_eq!(ddl[0].get("status").and_then(|v| v.as_str()), Some("OK"));

    let dml = sets[1].as_array().expect("DML status result set");
    assert_eq!(dml.len(), 1);
    assert_eq!(dml[0].get("status").and_then(|v| v.as_str()), Some("OK"));
    assert!(
        dml[0]
            .get("message")
            .and_then(|v| v.as_str())
            .is_some_and(|msg| msg.contains("1 row(s) affected")),
        "DML message: {}",
        sets[1]
    );

    let select = sets[2].as_array().expect("SELECT result set");
    assert_eq!(select.len(), 1);
    assert_eq!(select[0].get("id").and_then(|v| v.as_i64()), Some(1));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_quiet_omits_status_result_sets() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--quiet",
        "--output",
        "json",
        "sql",
        "CREATE TABLE t (id INTEGER PRIMARY KEY); \
         INSERT INTO t (id) VALUES (1); \
         SELECT id FROM t",
    ]);

    let value = parse_json_stdout(&output);
    let sets = value.as_array().expect("top-level JSON array");
    assert_eq!(sets.len(), 1, "quiet keeps only query result sets: {value}");
    let rows = sets[0].as_array().expect("SELECT result set");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id").and_then(|v| v.as_i64()), Some(1));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_csv_emits_block_per_statement() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "csv",
        "sql",
        "SELECT 1 AS a; SELECT 2 AS b",
    ]);

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout, "a\n1\nb\n2\n", "one CSV block per statement");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_jsonl_emits_rows_for_every_statement() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "jsonl",
        "sql",
        "SELECT 1 AS a; SELECT 2 AS b",
    ]);

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<serde_json::Value> = stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl line"))
        .collect();
    assert_eq!(lines.len(), 2, "stdout:\n{stdout}");
    assert_eq!(lines[0].get("a").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(lines[1].get("b").and_then(|v| v.as_i64()), Some(2));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_table_emits_block_per_statement() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "table",
        "sql",
        "SELECT 1 AS a; SELECT 2 AS b",
    ]);

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    for needle in ["a", "1", "b", "2"] {
        assert!(
            stdout.contains(needle),
            "expected '{needle}' in table output:\n{stdout}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_multi_statement_error_exits_nonzero() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "SELECT 1 AS a; INSERT INTO missing_table (id) VALUES (1)",
    ]);

    assert!(
        !output.status.success(),
        "failing statement must produce a non-zero exit code\nstdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_maps_commit_barrier_steps_losslessly() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "BEGIN; CREATE TABLE items (id INTEGER PRIMARY KEY); \
         INSERT INTO items (id) VALUES (1); COMMIT; SELECT id FROM items",
    ]);

    let value = parse_json_stdout(&output);
    assert_eq!(value["execution_id"], "cli-execution");
    assert_eq!(value["transaction_id"], "cli-transaction");
    assert_eq!(value["success"], true);
    let steps = value["steps"].as_array().expect("ordered steps");
    assert_eq!(steps.len(), 4);
    assert_eq!(steps[0]["step_id"], "step-0");
    assert_eq!(steps[1]["result"]["kind"], "rows_affected");
    assert_eq!(steps[1]["result"]["affected_rows"], 1);
    assert_eq!(steps[2]["kind"], "commit");
    assert_eq!(steps[2]["commit"]["transaction_id"], "cli-transaction");
    assert_eq!(steps[3]["result"]["kind"], "query");
    assert_eq!(steps[3]["result"]["rows"], serde_json::json!([[1]]));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_enforces_read_only_transaction_characteristics() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "CREATE TABLE items (id INTEGER PRIMARY KEY); \
         BEGIN READ ONLY; INSERT INTO items VALUES (1); COMMIT",
    ]);

    assert!(!output.status.success(), "read-only mutation must fail");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let steps = value["steps"].as_array().expect("ordered steps");
    assert_eq!(steps.last().unwrap()["kind"], "error");
    assert!(steps.last().unwrap()["error"]["message"]
        .as_str()
        .unwrap()
        .contains("read-only"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_executes_savepoint_rollback_and_release() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "BEGIN; CREATE TABLE items (id INTEGER PRIMARY KEY); \
         INSERT INTO items VALUES (1); SAVEPOINT keep_one; \
         INSERT INTO items VALUES (2); ROLLBACK TO SAVEPOINT keep_one; \
         RELEASE SAVEPOINT keep_one; COMMIT; SELECT id FROM items ORDER BY id",
    ]);

    let value = parse_json_stdout(&output);
    assert_eq!(value["success"], true);
    let steps = value["steps"].as_array().expect("ordered steps");
    assert_eq!(
        steps.last().unwrap()["result"]["rows"],
        serde_json::json!([[1]])
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_rejects_explain_analyze_mutation_after_commit() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "BEGIN; CREATE TABLE items (id INTEGER PRIMARY KEY); \
         INSERT INTO items VALUES (1); COMMIT; \
         EXPLAIN ANALYZE INSERT INTO items VALUES (2)",
    ]);

    assert!(!output.status.success(), "post-commit mutation must fail");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let steps = value["steps"].as_array().expect("ordered steps");
    assert_eq!(steps.last().unwrap()["error"]["kind"], "post_commit_read");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_preserves_commit_when_post_read_fails_and_exits_nonzero() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--output",
        "json",
        "sql",
        "BEGIN; CREATE TABLE items (id INTEGER PRIMARY KEY); \
         INSERT INTO items (id) VALUES (1); COMMIT; SELECT id FROM missing",
    ]);

    assert!(!output.status.success(), "partial failure must be non-zero");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("stdout must remain machine-readable: {error}"));
    assert_eq!(value["success"], false);
    let steps = value["steps"].as_array().expect("ordered partial steps");
    assert_eq!(steps.len(), 4);
    assert_eq!(steps[2]["kind"], "commit");
    assert_eq!(steps[3]["kind"], "error");
    assert_eq!(steps[3]["error"]["kind"], "post_commit_read");
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_json_executes_advanced_dml_through_the_public_cli() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "--quiet",
        "--output",
        "json",
        "sql",
        "CREATE TABLE items (id BIGINT PRIMARY KEY, value TEXT); \
         CREATE TABLE incoming (id BIGINT, value TEXT); \
         INSERT INTO items VALUES (1, 'old'), (2, 'delete') RETURNING id; \
         INSERT INTO incoming VALUES (1, 'joined'), (2, 'unused'); \
         INSERT INTO items VALUES (1, 'conflict') ON CONFLICT (id) \
             DO UPDATE SET value = 'updated' RETURNING value; \
         UPDATE items SET value = incoming.value FROM incoming \
             WHERE items.id = incoming.id; \
         DELETE FROM items USING incoming \
             WHERE items.id = incoming.id AND incoming.id = 2 RETURNING items.id; \
         CREATE TABLE merge_target (id BIGINT PRIMARY KEY, value TEXT); \
         CREATE TABLE merge_source (id BIGINT, value TEXT); \
         INSERT INTO merge_target VALUES (1, 'old'); \
         INSERT INTO merge_source VALUES (1, 'merged'), (2, 'inserted'); \
         MERGE INTO merge_target USING merge_source \
             ON merge_target.id = merge_source.id \
             WHEN MATCHED THEN UPDATE SET value = merge_source.value \
             WHEN NOT MATCHED THEN INSERT (id, value) \
                 VALUES (merge_source.id, merge_source.value); \
         SELECT id, value FROM merge_target ORDER BY id",
    ]);

    let value = parse_json_stdout(&output);
    let sets = value.as_array().expect("top-level JSON array");
    assert_eq!(
        sets.last().unwrap(),
        &serde_json::json!([
            {"id": 1, "value": "merged"},
            {"id": 2, "value": "inserted"}
        ])
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn batch_cli_streams_copy_csv_through_process_stdio() {
    let imported = run_alopex_with_stdin(
        &[
            "--in-memory",
            "--batch",
            "--quiet",
            "--output",
            "json",
            "sql",
            "CREATE TABLE items (id INTEGER, label TEXT); \
             COPY items FROM STDIN WITH (FORMAT CSV, HEADER TRUE); \
             SELECT id, label FROM items ORDER BY id",
        ],
        b"id,label\n1,one\n2,two\n",
    );
    assert_eq!(
        parse_json_stdout(&imported),
        serde_json::json!([[{"id": 1, "label": "one"}, {"id": 2, "label": "two"}]])
    );

    let exported = run_alopex(&[
        "--in-memory",
        "--batch",
        "--quiet",
        "sql",
        "CREATE TABLE items (id INTEGER, label TEXT); \
         INSERT INTO items VALUES (1, 'one'), (2, 'two'); \
         COPY items TO STDOUT WITH (FORMAT CSV, HEADER TRUE)",
    ]);
    assert!(
        exported.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&exported.stdout),
        "id,label\n1,one\n2,two\n"
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn recursive_cte_resource_limit_reports_stable_error_code() {
    let output = run_alopex(&[
        "--in-memory",
        "--batch",
        "sql",
        "WITH RECURSIVE cycle(n) AS (\
             SELECT 1 UNION ALL SELECT n FROM cycle\
         ) SELECT n FROM cycle",
    ]);

    assert!(!output.status.success(), "an unbounded cycle must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ALOPEX-E003"), "stderr:\n{stderr}");
    assert!(
        stderr.contains("recursive CTE 'cycle' reached iteration limit"),
        "stderr:\n{stderr}"
    );
}
