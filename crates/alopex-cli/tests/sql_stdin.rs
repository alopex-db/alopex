use std::io::Write;
use std::process::{Command, Stdio};

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
    let array = value.as_array().expect("json output should be array");
    let found = array.iter().any(|row| {
        row.get("id")
            .and_then(|v| v.as_i64())
            .is_some_and(|id| id == 1)
    });
    assert!(
        found,
        "expected JSON output to include id=1\nstdout:\n{}\nstderr:\n{}",
        stdout,
        String::from_utf8_lossy(&output.stderr)
    );
}
