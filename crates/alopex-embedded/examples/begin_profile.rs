//! Manual #684 diagnostic. Never execute from ordinary CI.
//! One bounded fixture per process; no throughput/load qualification is implied.
use alopex_embedded::{Database, TxnMode};
use alopex_sql::{executor::ExecutionResult, SqlValue};
use serde_json::json;
use std::{
    error::Error,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn check(result: ExecutionResult, rows: usize) -> Result<()> {
    let ExecutionResult::Query(result) = result else {
        return Err("expected query rows".into());
    };
    let expected = if rows == 0 {
        vec![]
    } else {
        vec![vec![SqlValue::Integer(0)]]
    };
    if result.rows != expected {
        return Err("query value mismatch".into());
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 || args[1] != "--manual" {
        return Err("usage: begin_profile --manual memory|disk ROWS ro|rw borrowed|owned noop|query|profile".into());
    }
    let backend = &args[2];
    let rows: usize = args[3].parse()?;
    if ![0, 100, 1000, 10000].contains(&rows) || !["memory", "disk"].contains(&backend.as_str()) {
        return Err("unsupported bounded diagnostic fixture".into());
    }
    let mode = match args[4].as_str() {
        "ro" => TxnMode::ReadOnly,
        "rw" => TxnMode::ReadWrite,
        _ => return Err("invalid mode".into()),
    };
    let owned = match args[5].as_str() {
        "owned" => true,
        "borrowed" => false,
        _ => return Err("invalid API".into()),
    };
    let operation = &args[6];
    if !["noop", "query", "profile"].contains(&operation.as_str()) {
        return Err("invalid operation".into());
    }
    let temporary = tempfile::Builder::new()
        .prefix("alopex-684-db-")
        .tempdir()?;
    let path = temporary.path().join("db");
    let open = || -> Result<Database> {
        Ok(if backend == "disk" {
            Database::open(&path)?
        } else {
            Database::new()
        })
    };
    let seed_started = Instant::now();
    let mut db = open()?;
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")?;
    if rows > 0 {
        let mut tx = db.begin(TxnMode::ReadWrite)?;
        for id in 0..rows {
            tx.execute_sql(&format!("INSERT INTO items VALUES ({id}, {})", id * 3))?;
        }
        tx.commit()?;
    }
    let seed_ns = seed_started.elapsed().as_nanos();
    let reopen_started = Instant::now();
    if backend == "disk" {
        drop(db);
        db = open()?;
    }
    let reopen_ns = reopen_started.elapsed().as_nanos();
    let db = Arc::new(db);
    println!(
        "{}",
        json!({"event":"ready", "pid":std::process::id(), "backend":backend, "rows":rows,
        "indexes":0, "api":args[5], "mode":args[4], "operation":operation, "seed_ns":seed_ns,
        "reopen_ns":reopen_ns, "cache_note":"first-after-open is not OS cold cache", "debug_assertions":cfg!(debug_assertions)})
    );
    std::io::stdout().flush()?;
    if operation == "profile" {
        std::thread::sleep(Duration::from_secs(1));
    }
    let profile_started = Instant::now();
    let mut iteration = 0;
    loop {
        let started = Instant::now();
        let (begin_ns, query_ns, rollback_ns) = if owned {
            let mut tx = db.clone().begin_owned_embedded_transaction(mode)?;
            let begin_ns = started.elapsed().as_nanos();
            let query_started = Instant::now();
            if operation == "query" {
                check(tx.execute_sql("SELECT id FROM items WHERE id = 0")?, rows)?;
            }
            let query_ns = if operation == "query" {
                Some(query_started.elapsed().as_nanos())
            } else {
                None
            };
            let rollback_started = Instant::now();
            tx.rollback()?;
            (begin_ns, query_ns, rollback_started.elapsed().as_nanos())
        } else {
            let mut tx = db.begin(mode)?;
            let begin_ns = started.elapsed().as_nanos();
            let query_started = Instant::now();
            if operation == "query" {
                check(tx.execute_sql("SELECT id FROM items WHERE id = 0")?, rows)?;
            }
            let query_ns = if operation == "query" {
                Some(query_started.elapsed().as_nanos())
            } else {
                None
            };
            let rollback_started = Instant::now();
            tx.rollback()?;
            (begin_ns, query_ns, rollback_started.elapsed().as_nanos())
        };
        if operation != "profile" {
            println!(
                "{}",
                json!({"event":"sample", "iteration":iteration, "first_after_open":iteration==0,
                "begin_ns":begin_ns, "query_ns":query_ns, "rollback_ns":rollback_ns})
            );
        }
        iteration += 1;
        if (operation == "profile" && profile_started.elapsed() >= Duration::from_secs(4))
            || (operation != "profile" && iteration == 6)
        {
            break;
        }
    }
    let ExecutionResult::Query(result) =
        db.execute_sql("SELECT id, value FROM items ORDER BY id")?
    else {
        return Err("expected final query rows".into());
    };
    let expected: Vec<_> = (0..rows)
        .map(|id| {
            vec![
                SqlValue::Integer(id as i32),
                SqlValue::Integer((id * 3) as i32),
            ]
        })
        .collect();
    if result.rows != expected {
        return Err("final complete state mismatch".into());
    }
    println!(
        "{}",
        json!({"event":"complete", "iterations":iteration, "all_rows_unchanged":true})
    );
    drop(db);
    temporary.close()?;
    Ok(())
}
