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

#[test]
fn invalid_overflow_budget_preserves_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing.alopex");
    std::fs::write(&path, b"old").unwrap();
    let invalid = LsmOverflowConfig::new(
        SpoolLimits::new(16384, 32768, 256).unwrap(),
        OperationLimits::new(16384, 1024, 4096).unwrap(),
        131072,
        0,
        32768,
    );
    let opts = DatabaseOptions::new().with_wal_overflow(invalid, WalConfig::default());
    assert!(matches!(
        Database::open_with_options(&path, opts),
        Err(alopex_embedded::Error::Core(
            alopex_core::Error::InvalidFormat(_)
        ))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), b"old");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn public_overflow_creates_nested_ancestors_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing/child/記録.alopex");
    let db = Database::open_with_options(&path, options()).unwrap();
    let mut tx = db.begin(alopex_embedded::TxnMode::ReadWrite).unwrap();
    tx.put(b"key", &[7; 65]).unwrap();
    tx.commit().unwrap();
    assert!(db.wal_overflow_stats().unwrap().unwrap().directory_bytes > 64);
    db.close().unwrap();
    drop(db);
    let db = Database::open_with_options(&path, options()).unwrap();
    let mut tx = db.begin(alopex_embedded::TxnMode::ReadOnly).unwrap();
    assert_eq!(tx.get(b"key").unwrap(), Some(vec![7; 65]));
    tx.rollback().unwrap();
    db.close().unwrap();
}

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

mod failure_acceptance {
    use super::*;
    use alopex_embedded::TxnMode;
    use std::{fs, path::Path};

    fn seed(path: &Path) -> Database {
        let db = Database::open_with_options(path, options()).unwrap();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"previous", b"ok").unwrap();
        tx.commit().unwrap();
        db
    }

    fn assert_reopened(path: &Path, db: Database) {
        db.close().unwrap();
        drop(db);
        let db = Database::open_with_options(path, options()).unwrap();
        let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(tx.get(b"previous").unwrap(), Some(b"ok".to_vec()));
        assert_eq!(tx.get(b"pending").unwrap(), None);
        tx.rollback().unwrap();
        db.close().unwrap();
    }

    #[test]
    fn explicit_rollback_preserves_prior_commit_without_creating_spool() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollback.alopex");
        let db = seed(&path);
        let before = db.wal_overflow_stats().unwrap();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"pending", &[7; 65]).unwrap(); // One byte beyond ring payload.
        tx.rollback().unwrap();
        assert_eq!(db.wal_overflow_stats().unwrap(), before);
        assert_reopened(&path, db);
    }

    #[test]
    fn dropped_transaction_preserves_prior_commit_without_creating_spool() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drop.alopex");
        let db = seed(&path);
        let before = db.wal_overflow_stats().unwrap();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"pending", &[7; 65]).unwrap();
        drop(tx);
        assert_eq!(db.wal_overflow_stats().unwrap(), before);
        assert_reopened(&path, db);
    }

    fn failed_inventory_preserves_prior_commit(foreign_file: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("io.alopex");
        let db = seed(&path);
        let before = db.wal_overflow_stats().unwrap();
        let spool = dir.path().join("io.alopex.d/overflow");
        let saved = dir.path().join("saved-overflow");
        fs::rename(&spool, &saved).unwrap();
        if foreign_file {
            fs::write(&spool, b"x").unwrap();
        }
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"pending", &[7; 65]).unwrap();
        let error = tx.commit().unwrap_err();
        if foreign_file {
            assert!(matches!(
                error,
                alopex_embedded::Error::Core(alopex_core::Error::InvalidFormat(_))
            ));
            assert_eq!(fs::read(&spool).unwrap(), b"x");
            fs::remove_file(&spool).unwrap();
        } else {
            assert!(
                matches!(error, alopex_embedded::Error::Core(alopex_core::Error::Io(e))
                if e.kind() == std::io::ErrorKind::NotFound)
            );
            assert!(!spool.exists());
        }
        fs::rename(&saved, &spool).unwrap();
        assert_eq!(db.wal_overflow_stats().unwrap(), before);
        assert_reopened(&path, db);
    }

    #[test]
    fn commit_missing_spool_directory_returns_io_and_preserves_prior_commit() {
        failed_inventory_preserves_prior_commit(false);
    }

    #[test]
    fn commit_foreign_spool_path_returns_format_error_without_deleting_it() {
        failed_inventory_preserves_prior_commit(true);
    }
}
