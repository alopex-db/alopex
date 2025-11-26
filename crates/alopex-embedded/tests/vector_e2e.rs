use alopex_core::{Metric, MemoryKV, TxnMode};
use alopex_core::columnar::segment::{write_segment, SegmentMeta, SegmentReader};
use alopex_core::columnar::encoding::{Column, LogicalType, Encoding, Compression};
use alopex_embedded::Database;

fn key(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

#[test]
fn upsert_and_search_end_to_end_with_filter() {
    let db = Database::new();
    {
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.upsert_vector(b"k1", b"meta1", &[1.0, 0.0], Metric::Cosine).unwrap();
        txn.upsert_vector(b"k2", b"meta2", &[0.0, 1.0], Metric::Cosine).unwrap();
        txn.commit().unwrap();
    }

    let mut ro = db.begin(TxnMode::ReadOnly).unwrap();
    let res = ro.search_similar(&[1.0, 0.0], Metric::Cosine, 2, None).unwrap();
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].key, b"k1");

    // Filter out k1 to ensure filter-before-score works in API path.
    let filtered = ro
        .search_similar(&[1.0, 0.0], Metric::Cosine, 2, Some(&[key(b"k2")]))
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].key, b"k2");
}

#[test]
fn checksum_corruption_surfaces() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vec.alx");

    // Write a small segment with checksum.
    let meta = SegmentMeta {
        logical_type: LogicalType::Int64,
        encoding: Encoding::Plain,
        compression: Compression::None,
        chunk_rows: 4,
        chunk_checksum: true,
    };
    let col = Column::Int64(vec![1, 2, 3, 4]);
    write_segment(&path, &col, &meta).unwrap();

    // Corrupt payload.
    let mut bytes = fs::read(&path).unwrap();
    let header_len = 4 + 2 + 3 + 4 + 1 + 4; // magic + version + kind + chunk_rows + checksum flag + total_rows
    let payload_start = header_len + 8; // row_count(4) + encoded_len(4)
    if payload_start < bytes.len() {
        bytes[payload_start] ^= 0xFF;
    }
    fs::write(&path, &bytes).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    let err = reader.iter().next().unwrap().unwrap_err();
    assert!(format!("{err:?}").contains("CorruptedSegment"));
}
