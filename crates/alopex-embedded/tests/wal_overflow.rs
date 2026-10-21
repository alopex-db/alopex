use alopex_embedded::wal_overflow::{
    LsmOverflowConfig, OperationLimits, SpoolLimits, SyncMode, WalConfig,
};
const WAL_SEGMENT_HEADER_SIZE: usize = 28;
use alopex_embedded::{Database, DatabaseOptions};

fn limits() -> LsmOverflowConfig {
    LsmOverflowConfig::new(
        SpoolLimits::new(16384, 32768, 256).unwrap(),
        OperationLimits::new(16384, 1024, 4096).unwrap(),
        131072,
        256,
        32768,
    )
    .with_inline_bytes(1024)
    .with_activation_bytes(4096)
    .with_container_fence_bytes(4096)
}
fn options() -> DatabaseOptions {
    DatabaseOptions::new().with_wal_overflow(
        limits(),
        WalConfig {
            segment_size: WAL_SEGMENT_HEADER_SIZE + 64,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
    )
}

#[test]
fn memory_mode_rejects_disk_overflow_options() {
    let opts = DatabaseOptions::in_memory().with_wal_overflow(limits(), WalConfig::default());
    assert!(Database::open_in_memory_with_options(opts).is_err());
    assert_eq!(Database::new().wal_overflow_stats().unwrap(), None);
}

#[test]
fn invalid_or_unsupported_overflow_open_does_not_create_storage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.alopex");
    let opts = DatabaseOptions::new().with_wal_overflow(
        limits(),
        WalConfig {
            segment_size: WAL_SEGMENT_HEADER_SIZE + 52,
            max_segments: 1,
            sync_mode: SyncMode::EveryWrite,
        },
    );
    assert!(Database::open_with_options(&path, opts).is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[cfg(windows)]
#[test]
fn windows_public_opt_in_refuses_before_creating_any_database_files() {
    let dir = tempfile::tempdir().unwrap();
    let result = Database::open_with_options(&dir.path().join("absent.alopex"), options());
    assert!(
        matches!(result, Err(alopex_embedded::Error::Core(alopex_core::Error::Io(e)))
        if e.kind() == std::io::ErrorKind::Unsupported)
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[cfg(not(windows))]
#[test]
fn public_sql_rows_and_unique_index_survive_overflow_commit_and_reopen() {
    use alopex_sql::{ExecutionResult, SqlValue};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sql.alopex");
    let db = Database::open_with_options(&path, options()).unwrap();
    db.execute_sql("CREATE TABLE items (id INT PRIMARY KEY, code TEXT UNIQUE)")
        .unwrap();
    let mut tx = db.begin(alopex_embedded::TxnMode::ReadWrite).unwrap();
    tx.execute_sql("INSERT INTO items VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    tx.commit().unwrap();
    let stats = db.wal_overflow_stats().unwrap().unwrap();
    assert!(stats.directory_bytes > 64);
    assert!(stats.directory_bytes <= stats.directory_limit_bytes);
    db.close().unwrap();
    drop(db);
    assert!(Database::open(&path).is_err());
    let db = Database::open_with_options(&path, options()).unwrap();
    let ExecutionResult::Query(rows) = db
        .execute_sql("SELECT id, code FROM items ORDER BY id")
        .unwrap()
    else {
        panic!("query")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("a".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())]
        ]
    );
    assert!(db.execute_sql("INSERT INTO items VALUES (3, 'a')").is_err());
    assert_eq!(db.wal_overflow_stats().unwrap().unwrap().gc_failures, 0);
}
