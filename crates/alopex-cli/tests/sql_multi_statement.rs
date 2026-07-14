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
