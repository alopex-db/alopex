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
CREATE TABLE stdin_test (id INTEGER PRIMARY KEY, qty INTEGER);
INSERT INTO stdin_test (id, qty) VALUES (1, 3), (2, 1), (3, 5);
SELECT * FROM stdin_test;
WITH renamed(identifier) AS (SELECT 42) SELECT identifier FROM renamed;
WITH RECURSIVE counter(n) AS (
    SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 3
) SELECT n FROM counter ORDER BY n;
SELECT id, SUM(qty) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS framed FROM stdin_test ORDER BY id;
SELECT id,
       FIRST_VALUE(id) OVER (ORDER BY qty) AS first_id,
       LAST_VALUE(id) OVER (ORDER BY qty) AS last_id,
       NTH_VALUE(id, 2) OVER (ORDER BY qty) AS second_id,
       NTILE(2) OVER (ORDER BY qty) AS bucket,
       PERCENT_RANK() OVER (ORDER BY qty) AS percent_rank,
       CUME_DIST() OVER (ORDER BY qty) AS cume_dist
FROM stdin_test ORDER BY id;
SELECT id, ROW_NUMBER() OVER ranked AS row_number
FROM stdin_test
WINDOW base AS (), ranked AS (base ORDER BY qty DESC, id)
QUALIFY row_number <= 2
ORDER BY id;
VALUES (2, 'b'), (1, 'a') ORDER BY column1;
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
        9,
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

    let frame_rows = sets[5].as_array().expect("window frame SELECT result set");
    assert_eq!(
        frame_rows,
        &[
            serde_json::json!({ "id": 1, "framed": 4 }),
            serde_json::json!({ "id": 2, "framed": 9 }),
            serde_json::json!({ "id": 3, "framed": 6 }),
        ]
    );

    let extended_window_rows = sets[6]
        .as_array()
        .expect("extended window SELECT result set");
    assert_eq!(
        extended_window_rows,
        &[
            serde_json::json!({
                "id": 1,
                "first_id": 2,
                "last_id": 1,
                "second_id": 1,
                "bucket": 1,
                "percent_rank": 0.5,
                "cume_dist": 2.0 / 3.0,
            }),
            serde_json::json!({
                "id": 2,
                "first_id": 2,
                "last_id": 2,
                "second_id": null,
                "bucket": 1,
                "percent_rank": 0.0,
                "cume_dist": 1.0 / 3.0,
            }),
            serde_json::json!({
                "id": 3,
                "first_id": 2,
                "last_id": 3,
                "second_id": 1,
                "bucket": 2,
                "percent_rank": 1.0,
                "cume_dist": 1.0,
            }),
        ]
    );

    let qualify_rows = sets[7]
        .as_array()
        .expect("named WINDOW/QUALIFY SELECT result set");
    assert_eq!(
        qualify_rows,
        &[
            serde_json::json!({ "id": 1, "row_number": 2 }),
            serde_json::json!({ "id": 3, "row_number": 1 }),
        ]
    );

    let values_rows = sets[8].as_array().expect("VALUES result set");
    assert_eq!(
        values_rows,
        &[
            serde_json::json!({ "column1": 1, "column2": "a" }),
            serde_json::json!({ "column1": 2, "column2": "b" }),
        ]
    );
}
