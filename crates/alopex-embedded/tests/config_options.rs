use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema, SegmentConfigV2};
use alopex_embedded::{Database, DatabaseOptions, EmbeddedConfig, Error, StorageMode, TxnMode};

fn two_rows() -> RecordBatch {
    RecordBatch::new(
        Schema {
            columns: vec![ColumnSchema {
                name: "id".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            }],
        },
        vec![Column::Int64(vec![1, 2])],
        vec![None],
    )
}

#[test]
fn published_six_field_literal_keeps_cache_settings() {
    let db = Database::open_with_config(EmbeddedConfig {
        path: None,
        storage_mode: StorageMode::InMemory,
        memory_limit: None,
        segment_config: Default::default(),
        hnsw_cache_bytes: Some(17),
        hnsw_cache_entries: Some(2),
    })
    .unwrap();
    assert_eq!(db.hnsw_cache_stats().limit_bytes, 17);
    assert_eq!(db.hnsw_cache_stats().limit_entries, 2);
}

#[test]
fn options_defaults_and_cache_disable_are_applied() {
    let defaults = Database::open_in_memory_with_options(DatabaseOptions::in_memory()).unwrap();
    assert_eq!(defaults.hnsw_cache_stats().limit_bytes, 256 * 1024 * 1024);
    assert_eq!(defaults.hnsw_cache_stats().limit_entries, 128);
    let disabled = Database::open_in_memory_with_options(
        DatabaseOptions::in_memory().with_hnsw_cache_limits(0, None),
    )
    .unwrap();
    assert_eq!(disabled.hnsw_cache_stats().limit_bytes, 0);
    assert_eq!(disabled.hnsw_cache_stats().limit_entries, 128);
}

#[test]
fn invalid_cache_count_is_rejected_before_creating_disk_storage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent");
    let result = Database::open_with_options(
        &path,
        DatabaseOptions::new().with_hnsw_cache_limits(17, Some(0)),
    );
    assert!(matches!(
        result,
        Err(Error::Core(alopex_core::Error::InvalidParameter { param, .. }))
            if param == "hnsw_cache_entries"
    ));
    assert!(!path.exists());
}

#[test]
fn disk_options_preserve_data_and_sql_catalog_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.alopex");
    {
        let db = Database::open_with_options(
            &path,
            DatabaseOptions::new().with_hnsw_cache_limits(29, Some(3)),
        )
        .unwrap();
        assert_eq!(db.storage_mode(), StorageMode::Disk);
        assert_eq!(db.hnsw_cache_stats().limit_bytes, 29);
        assert_eq!(db.hnsw_cache_stats().limit_entries, 3);
        db.execute_sql("CREATE TABLE options_check (id INT PRIMARY KEY)")
            .unwrap();
        db.execute_sql("INSERT INTO options_check VALUES (7)")
            .unwrap();
        let mut tx = db.begin(TxnMode::ReadWrite).unwrap();
        tx.put(b"key", b"value").unwrap();
        tx.commit().unwrap();
    }
    assert!(path.is_file());
    let openers: [fn(&std::path::Path) -> alopex_embedded::Result<Database>; 2] =
        [Database::open, |path| {
            Database::open_with_options(path, DatabaseOptions::new())
        }];
    for open in openers {
        let db = open(&path).unwrap();
        let mut tx = db.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(tx.get(b"key").unwrap(), Some(b"value".to_vec()));
        drop(tx);
        let result = db.execute_sql("SELECT id FROM options_check").unwrap();
        assert!(matches!(
            result,
            alopex_sql::ExecutionResult::Query(query)
                if query.rows == vec![vec![alopex_sql::SqlValue::Integer(7)]]
        ));
    }
}

#[test]
fn memory_options_ignore_disk_path_and_keep_memory_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("must-not-exist");
    let options = DatabaseOptions::new().with_memory_limit(1);
    assert!(options.memory_mode());
    assert_eq!(options.memory_limit(), Some(1));
    let db = Database::open_with_options(&path, options).unwrap();
    assert_eq!(db.storage_mode(), StorageMode::InMemory);
    assert!(!path.exists());
    let error = db
        .write_columnar_segment("bounded", two_rows())
        .unwrap_err();
    assert!(error.to_string().contains("memory limit exceeded"));
}

#[test]
fn segment_options_apply_to_memory_and_disk_open() {
    let dir = tempfile::tempdir().unwrap();
    for memory in [false, true] {
        let options = if memory {
            DatabaseOptions::in_memory()
        } else {
            DatabaseOptions::new()
        }
        .with_segment_config(SegmentConfigV2 {
            max_row_group_bytes: 1,
            ..Default::default()
        });
        let db = if memory {
            Database::open_in_memory_with_options(options)
        } else {
            Database::open_with_options(&dir.path().join("disk"), options)
        }
        .unwrap();
        assert!(db.write_columnar_segment("bounded", two_rows()).is_err());
    }
}

#[test]
fn memory_entry_point_still_rejects_disk_options() {
    assert!(matches!(
        Database::open_in_memory_with_options(DatabaseOptions::new()),
        Err(Error::Core(alopex_core::Error::InvalidFormat(message)))
            if message == "memory_mode must be enabled for in-memory open"
    ));
}
