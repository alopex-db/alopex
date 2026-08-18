use std::io::Write;
use std::process::{Command, Stdio};

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn sql_reads_from_stdin_pipe() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_alopex"))
        .args(["--in-memory", "--output", "json", "sql"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn alopex");

    let sql = r#"
CREATE TABLE stdin_test (id INTEGER PRIMARY KEY);
INSERT INTO stdin_test (id) VALUES (1);
SELECT * FROM stdin_test;
WITH renamed(identifier) AS (SELECT 42) SELECT identifier FROM renamed;
WITH RECURSIVE counter(n) AS (
    SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 3
) SELECT n FROM counter ORDER BY n;
"#;

    {
        let stdin = child.stdin.as_mut().expect("stdin");
        stdin.write_all(sql.as_bytes()).expect("write stdin");
    }

    let output = child.wait_with_output().expect("wait");
    assert!(
        output.status.success(),
        "status: {:?}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let value: serde_json::Value =
        serde_json::from_str(&stdout).expect("json output should be JSON");
    let sets = value
        .as_array()
        .expect("json output should be an array of result sets");
    assert_eq!(
        sets.len(),
        5,
        "one result set per statement\nstdout:\n{stdout}"
    );
    let select_rows = sets[2].as_array().expect("SELECT result set");
    let found = select_rows.iter().any(|row| {
        row.get("id")
            .and_then(|v| v.as_i64())
            .is_some_and(|id| id == 1)
    });
    assert!(
        found,
        "expected SELECT result set to include id=1\nstdout:\n{}\nstderr:\n{}",
        stdout,
        String::from_utf8_lossy(&output.stderr)
    );

    let cte_rows = sets[3].as_array().expect("CTE SELECT result set");
    assert_eq!(cte_rows, &[serde_json::json!({ "identifier": 42 })]);

    let recursive_rows = sets[4].as_array().expect("recursive CTE SELECT result set");
    assert_eq!(
        recursive_rows,
        &[
            serde_json::json!({ "n": 1 }),
            serde_json::json!({ "n": 2 }),
            serde_json::json!({ "n": 3 }),
        ]
    );
}
