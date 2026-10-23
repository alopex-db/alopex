//! Manual instrumented-build acceptance. See scripts/compatibility/README.md.
#![cfg(all(alopex_wal_test_hooks, not(windows)))]
use alopex_core::lsm::wal::overflow::test_hooks;
use alopex_embedded::wal_overflow::{
    LsmOverflowConfig, OperationLimits, SpoolLimits, SyncMode, WalConfig,
};
use alopex_embedded::{Database, DatabaseOptions, Error, TxnMode};
use alopex_sql::{ExecutionResult, SqlValue};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOptions::new().with_wal_overflow(
            LsmOverflowConfig::new(
                SpoolLimits::new(16384, 32768, 256).unwrap(),
                OperationLimits::new(16384, 1024, 4096).unwrap(),
                131072,
                256,
                32768,
            )
            .with_inline_bytes(1024)
            .with_activation_bytes(4096)
            .with_container_fence_bytes(16384),
            WalConfig {
                segment_size: 92,
                max_segments: 1,
                sync_mode: SyncMode::EveryWrite,
            },
        ),
    )
    .unwrap()
}

fn stage(db: &Database, sql: bool) -> alopex_embedded::Transaction<'_> {
    let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
    if sql {
        tx.execute_sql(&format!(
            "INSERT INTO items VALUES (2, 'a', '{}'), (3, 'b', 'y')",
            "x".repeat(65)
        ))
        .unwrap();
    } else {
        tx.put(b"pending-a", &[7; 65]).unwrap();
        tx.put(b"pending-b", &[8]).unwrap();
    }
    tx
}

fn verify(db: &Database, sql: bool, committed: bool) {
    if sql {
        let ExecutionResult::Query(rows) = db
            .execute_sql("SELECT id, code, value FROM items ORDER BY id")
            .unwrap()
        else {
            panic!("query")
        };
        let mut expected = vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("old".into()),
            SqlValue::Text("ok".into()),
        ]];
        if committed {
            expected.push(vec![
                SqlValue::Integer(2),
                SqlValue::Text("a".into()),
                SqlValue::Text("x".repeat(65)),
            ]);
            expected.push(vec![
                SqlValue::Integer(3),
                SqlValue::Text("b".into()),
                SqlValue::Text("y".into()),
            ]);
        }
        assert_eq!(rows.rows, expected);
    } else {
        let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(tx.get(b"old").unwrap(), Some(b"ok".to_vec()));
        assert_eq!(
            tx.get(b"pending-a").unwrap(),
            committed.then(|| vec![7; 65])
        );
        assert_eq!(tx.get(b"pending-b").unwrap(), committed.then(|| vec![8]));
        assert_eq!(tx.get(b"retry").unwrap(), None);
        tx.rollback().unwrap();
    }
}

fn verify_indexes(db: &Database, committed: bool) {
    // Test the pending primary and unique keys independently, then rollback.
    for sql in [
        "INSERT INTO items VALUES (2, 'unused', 'z')",
        "INSERT INTO items VALUES (9, 'a', 'z')",
    ] {
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        assert_eq!(tx.execute_sql(sql).is_err(), committed, "{sql}");
        tx.rollback().unwrap();
    }
}

#[test]
#[ignore = "parent cases explicitly invoke this isolated helper"]
fn child() {
    let spec = std::env::var("ALOPEX_DB_BOUNDARY").unwrap();
    let fields: Vec<_> = spec.split(':').collect();
    let sql = fields[0] == "sql";
    let exit = fields[1] == "exit";
    let point: u8 = fields[2].parse().unwrap();
    let path = std::env::var_os("ALOPEX_DB_PATH").unwrap();
    let db = open(Path::new(&path));
    verify(&db, sql, false);
    let tx = stage(&db, sql);
    test_hooks::arm(point, exit).unwrap();
    println!("ARMED {spec}");
    std::io::stdout().flush().unwrap();
    let error = tx.commit().unwrap_err();
    assert!(!exit, "exit hook was not reached");
    assert!(
        matches!(error, Error::Core(alopex_core::Error::Io(ref e))
        if e.to_string() == "injected overflow persistence failure"),
        "{error:?}"
    );
    verify(&db, sql, false);
    if point >= 4 {
        let mut retry = db.begin(TxnMode::ReadWrite).unwrap();
        retry.put(b"retry", b"no").unwrap();
        assert!(
            matches!(retry.commit(), Err(Error::Core(alopex_core::Error::InvalidFormat(reason)))
            if reason == "WAL outcome uncertain; reopen before further writes")
        );
    }
    println!("ERROR_RETURNED {spec}");
    std::io::stdout().flush().unwrap();
    std::process::exit(0); // Do not let Database Drop mask recovery with converge.
}

fn boundary(sql: bool, exit: bool, point: u8) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.alopex");
    let db = open(&path);
    if sql {
        db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE, value TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO items VALUES (1, 'old', 'ok')")
            .unwrap();
    } else {
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"old", b"ok").unwrap();
        tx.commit().unwrap();
    }
    db.close().unwrap();
    drop(db);
    let spec = format!(
        "{}:{}:{point}",
        if sql { "sql" } else { "kv" },
        if exit { "exit" } else { "error" }
    );
    let log_path = dir.path().join("child.log");
    let log = fs::File::create(&log_path).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--ignored", "--nocapture"])
        .env("ALOPEX_DB_BOUNDARY", &spec)
        .env("ALOPEX_DB_PATH", &path)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!(
                "{spec} timed out: {}",
                fs::read_to_string(&log_path).unwrap()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let log = fs::read_to_string(log_path).unwrap();
    assert_eq!(
        status.code(),
        Some(if exit { 80 + i32::from(point) } else { 0 }),
        "{spec}: {log}"
    );
    assert!(log.contains(&format!("ARMED {spec}")), "{log}");
    if !exit {
        assert!(log.contains(&format!("ERROR_RETURNED {spec}")), "{log}");
    }
    for _ in 0..2 {
        let db = open(&path);
        verify(&db, sql, point >= 5);
        assert_eq!(db.wal_overflow_stats().unwrap().unwrap().gc_failures, 0);
        if sql {
            verify_indexes(&db, point >= 5);
        }
        db.close().unwrap();
        drop(db);
    }
}

macro_rules! cases {
    ($module:ident, $sql:literal) => {
        mod $module {
            use super::*;
            #[test]
            fn error_spool_write() {
                boundary($sql, false, 1);
            }
            #[test]
            fn exit_spool_write() {
                boundary($sql, true, 1);
            }
            #[test]
            fn error_spool_sync() {
                boundary($sql, false, 2);
            }
            #[test]
            fn exit_spool_sync() {
                boundary($sql, true, 2);
            }
            #[test]
            fn error_spool_publish() {
                boundary($sql, false, 3);
            }
            #[test]
            fn exit_spool_publish() {
                boundary($sql, true, 3);
            }
            #[test]
            fn error_before_reference() {
                boundary($sql, false, 4);
            }
            #[test]
            fn exit_before_reference() {
                boundary($sql, true, 4);
            }
            #[test]
            fn error_reference_append_sync() {
                boundary($sql, false, 5);
            }
            #[test]
            fn exit_reference_append_sync() {
                boundary($sql, true, 5);
            }
            #[test]
            fn error_reference_force_sync() {
                boundary($sql, false, 8);
            }
            #[test]
            fn exit_reference_force_sync() {
                boundary($sql, true, 8);
            }
        }
    };
}
cases!(kv, false);
cases!(sql, true);
