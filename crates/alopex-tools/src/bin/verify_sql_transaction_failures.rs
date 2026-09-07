//! Reusable failure-conformance fixture for the public embedded SQL API.

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alopex_embedded::{Database, SqlSessionState};
use alopex_sql::{ExecutionResult, SqlValue};

const CHILD_MODE: &str = "ALOPEX_TXN_FAILURE_CHILD_MODE";
const DATABASE_PATH: &str = "ALOPEX_TXN_FAILURE_DATABASE";

fn child(mode: &str, path: &Path) {
    let database = Arc::new(Database::open(path).expect("child opens database"));
    let mut session = database.sql_session();
    session.execute_sql("BEGIN").expect("child begins");
    let id = match mode {
        "uncommitted" => 1,
        "committed" => 2,
        "rolled_back" => 3,
        _ => panic!("unknown child mode: {mode}"),
    };
    session
        .execute_sql(&format!("INSERT INTO tx_probe (id) VALUES ({id})"))
        .expect("child inserts");
    match mode {
        "committed" => {
            session.execute_sql("COMMIT").expect("commit acknowledged");
        }
        "rolled_back" => {
            session.execute_sql("ROLLBACK").expect("rollback completes");
        }
        "uncommitted" => {}
        _ => unreachable!(),
    }
    println!("READY");
    std::io::stdout().flush().expect("flush readiness marker");
    loop {
        std::thread::park();
    }
}

fn kill_child(mode: &str, path: &Path) {
    let mut process = Command::new(env::current_exe().expect("fixture executable"))
        .env(CHILD_MODE, mode)
        .env(DATABASE_PATH, path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn failure-injection child");
    let stdout = process.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next() {
            Some(Ok(line)) if line.trim() == "READY" => break,
            Some(Ok(_)) => {}
            Some(Err(error)) => panic!("child stdout failed: {error}"),
            None => panic!("child exited before readiness marker"),
        }
    }
    process.kill().expect("force child termination");
    process.wait().expect("reap child");
}

fn ids(path: &Path) -> Vec<i64> {
    let database = Database::open(path).expect("reopen database");
    let ExecutionResult::Query(result) = database
        .execute_sql("SELECT id FROM tx_probe ORDER BY id")
        .expect("query probe rows")
    else {
        panic!("SELECT must return rows");
    };
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(SqlValue::Integer(value)) => i64::from(value),
            Some(SqlValue::BigInt(value)) => value,
            value => panic!("unexpected id value: {value:?}"),
        })
        .collect()
}

fn verify_session_transitions(path: &Path) {
    let database = Arc::new(Database::open(path).expect("open transition database"));

    let mut disconnected = database.sql_session();
    disconnected
        .execute_sql("BEGIN")
        .expect("begin disconnect case");
    disconnected
        .execute_sql("INSERT INTO tx_probe (id) VALUES (4)")
        .expect("stage disconnected row");
    drop(disconnected);
    drop(database);
    assert_eq!(ids(path), vec![2]);

    let database = Arc::new(Database::open(path).expect("reopen transition database"));
    let mut recovered = database.sql_session();
    assert!(recovered.execute_sql("COMMIT").is_err());
    recovered
        .execute_sql("BEGIN")
        .expect("begin savepoint case");
    recovered
        .execute_sql("SAVEPOINT stable")
        .expect("savepoint");
    recovered
        .execute_sql("INSERT INTO tx_probe (id) VALUES (5)")
        .expect("stage row after savepoint");
    assert!(recovered
        .execute_sql("SELECT * FROM missing_table")
        .is_err());
    assert_eq!(recovered.state(), SqlSessionState::Failed);
    assert!(recovered.execute_sql("COMMIT").is_err());
    recovered
        .execute_sql("ROLLBACK TO SAVEPOINT stable")
        .expect("savepoint recovers failed session");
    assert_eq!(recovered.state(), SqlSessionState::Active);
    recovered
        .execute_sql("INSERT INTO tx_probe (id) VALUES (6)")
        .expect("stage recovered row");
    recovered
        .execute_sql("COMMIT")
        .expect("commit recovered session");

    let mut first = database.sql_session();
    let mut second = database.sql_session();
    first.execute_sql("BEGIN").expect("first concurrent begin");
    second
        .execute_sql("BEGIN")
        .expect("second concurrent begin");
    first
        .execute_sql("INSERT INTO tx_probe (id) VALUES (7)")
        .expect("first concurrent insert");
    second
        .execute_sql("INSERT INTO tx_probe (id) VALUES (7)")
        .expect("second concurrent insert");
    first
        .execute_sql("COMMIT")
        .expect("first concurrent commit");
    assert!(second.execute_sql("COMMIT").is_err());
    assert_eq!(second.state(), SqlSessionState::Failed);
    assert!(second.execute_sql("ROLLBACK").is_err());
    assert_eq!(second.state(), SqlSessionState::Idle);
    drop((first, second, recovered, database));
    assert_eq!(ids(path), vec![2, 6, 7]);
}

fn run() {
    if let Ok(mode) = env::var(CHILD_MODE) {
        let path = PathBuf::from(env::var(DATABASE_PATH).expect("child database path"));
        child(&mode, &path);
        return;
    }

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let root = env::temp_dir().join(format!(
        "alopex-txn-failure-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir(&root).expect("create fixture directory");
    let path = root.join("failure.alopex");

    let database = Database::open(&path).expect("create database");
    database
        .execute_sql("CREATE TABLE tx_probe (id INTEGER PRIMARY KEY)")
        .expect("create probe table");
    drop(database);

    kill_child("uncommitted", &path);
    assert!(ids(&path).is_empty(), "uncommitted crash leaked a row");
    kill_child("committed", &path);
    assert_eq!(ids(&path), vec![2], "acknowledged commit was not recovered");
    kill_child("rolled_back", &path);
    assert_eq!(ids(&path), vec![2], "rolled-back row survived restart");
    verify_session_transitions(&path);

    std::fs::remove_dir_all(&root).expect("remove fixture directory");
    println!("sql transaction failure conformance passed");
}

fn main() {
    run();
}
