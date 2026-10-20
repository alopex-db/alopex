//! Four-row legacy binary encoding contract; independent of SQL table limits.
use alopex_core::columnar::encoding::Column;
use serde_json::json;
use std::{fs, path::Path};

fn values() -> Vec<Vec<u8>> {
    ["apple", "apricot", "zebra", "yak"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect()
}

#[cfg(not(feature = "candidate-reader"))]
pub fn run(root: &Path, label: &str) {
    use alopex_core::columnar::encoding::LogicalType;
    use alopex_core::columnar::segment_v2::{
        ColumnSchema, RecordBatch, Schema, SegmentConfigV2, SegmentWriterV2,
    };
    println!("columnar codec old write: start");
    let mut writer = SegmentWriterV2::new(SegmentConfigV2 {
        row_group_size: 2,
        ..Default::default()
    });
    writer
        .write_batch(RecordBatch::new(
            Schema {
                columns: vec![ColumnSchema {
                    name: "value".into(),
                    logical_type: LogicalType::Binary,
                    nullable: false,
                    fixed_len: None,
                }],
            },
            vec![Column::Binary(values())],
            vec![None],
        ))
        .unwrap();
    let segment = writer.finish().unwrap();
    assert_eq!(segment.header.format_version, 2);
    assert_eq!(segment.meta.row_groups.len(), 2);
    assert!(segment.meta.row_groups.iter().all(|g| g.row_count == 2));
    assert_ne!(
        segment.meta.row_groups[0].column_chunks[0].encoding,
        segment.meta.row_groups[1].column_chunks[0].encoding
    );
    fs::write(root.join("legacy-columnar.bin"), &segment.data).unwrap();
    fs::write(
        root.join("legacy-columnar-metadata.json"),
        serde_json::to_vec(&segment.meta.row_groups).unwrap(),
    )
    .unwrap();
    super::write(
        &root.join(format!("{label}-result.json")),
        &json!({
            "rows":4,"row_groups":2,"format":2,"mixed_encodings":true,"old_self_read":false
        }),
    );
    println!("columnar codec old write: passed (4 rows, 2 different encodings)");
}

#[cfg(feature = "candidate-reader")]
pub fn run(root: &Path, label: &str) {
    use alopex_core::columnar::segment_v2::{InMemorySegmentSource, RowGroupMeta, SegmentReaderV2};
    let data = fs::read(root.join("legacy-columnar.bin")).unwrap();
    assert!(data.len() < 16_384, "four-row fixture exceeded byte budget");
    assert!(
        data.starts_with(b"ALXC\x02\x00"),
        "expected legacy V2 header"
    );
    let metadata: Vec<RowGroupMeta> =
        serde_json::from_slice(&fs::read(root.join("legacy-columnar-metadata.json")).unwrap())
            .unwrap();
    assert_eq!(metadata.len(), 2);
    assert_ne!(
        metadata[0].column_chunks[0].encoding,
        metadata[1].column_chunks[0].encoding
    );
    for phase in ["read", "reopen"] {
        println!("columnar codec candidate {phase}: start");
        let reader = SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(data.clone())))
            .unwrap()
            .with_legacy_row_group_metadata(&metadata)
            .unwrap();
        assert_eq!(reader.row_group_count(), 2);
        let batches = reader.read_columns(&[0]).unwrap();
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|b| b.num_rows() == 2));
        let actual: Vec<_> = batches
            .into_iter()
            .flat_map(|b| match b.columns.into_iter().next().unwrap() {
                Column::Binary(v) => v,
                other => panic!("unexpected column {other:?}"),
            })
            .collect();
        assert_eq!(actual, values());
        println!("columnar codec candidate {phase}: passed");
    }
    super::write(
        &root.join(format!("{label}-result.json")),
        &json!({
            "rows":4,"row_groups":2,"format":2,"mixed_encodings":true,
            "values_match_seed":true,"second_reopen_pass":true
        }),
    );
}
