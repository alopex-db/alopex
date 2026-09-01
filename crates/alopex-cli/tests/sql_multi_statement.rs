//! E2E tests for multi-statement SQL output (GitHub issue #26).
//!
//! Contract:
//! - `--output json` for the `sql` command always emits an array of
//!   per-statement result sets (a single statement yields a 1-element array).
//! - DDL/DML status results (`status`/`message`) are included as one result
//!   set per statement. `--quiet` omits status result sets.
//! - table/csv/tsv/jsonl emit one result block per statement, in order.
//! - Any failing statement makes the whole invocation exit non-zero.

use std::process::{Command, Output};

fn run_alopex(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_alopex"))
        .args(args)
        .output()
        .expect("spawn alopex")
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
