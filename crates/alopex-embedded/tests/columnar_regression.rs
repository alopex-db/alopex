use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema};
use alopex_embedded::{Database, EmbeddedConfig, StorageMode};

fn make_batch() -> RecordBatch {
    let schema = Schema {
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
            ColumnSchema {
                name: "val".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
        ],
    };
    let cols = vec![Column::Int64(vec![1, 2]), Column::Int64(vec![10, 20])];
    RecordBatch::new(schema, cols, vec![None, None])
}

#[test]
fn columnar_projection_roundtrip_in_memory() {
    let db = Database::open_in_memory().unwrap();
    db.write_columnar_segment("t1", make_batch()).unwrap();

    let batches = db
        .read_columnar_segment("t1", 0, Some(&["val"]))
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.schema.columns.len(), 1);
    match &batch.columns[0] {
        Column::Int64(vals) => assert_eq!(vals, &vec![10, 20]),
        other => panic!("unexpected column: {:?}", other),
    }
}

#[test]
fn columnar_roundtrip_disk_mode() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(EmbeddedConfig {
        path: Some(dir.path().join("wal.log")),
        storage_mode: StorageMode::Disk,
        memory_limit: None,
        segment_config: Default::default(),
    })
    .unwrap();

    db.write_columnar_segment("t2", make_batch()).unwrap();
    let batches = db.read_columnar_segment("t2", 0, None).unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.schema.columns.len(), 2);
}
