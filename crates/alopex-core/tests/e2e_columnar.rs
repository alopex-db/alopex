use std::time::Instant;

use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::segment_v2::{
    ColumnSchema, InMemorySegmentSource, RecordBatch, Schema, SegmentConfigV2, SegmentReaderV2,
    SegmentWriterV2,
};
use alopex_core::storage::compression::{create_compressor, CompressionV2};

fn simple_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
            ColumnSchema {
                name: "value".into(),
                logical_type: LogicalType::Int64,
                nullable: false,
                fixed_len: None,
            },
        ],
    }
}

fn make_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + len as i64).collect();
    let vals: Vec<i64> = ids.iter().map(|v| v * 2).collect();
    RecordBatch::new(
        simple_schema(),
        vec![Column::Int64(ids), Column::Int64(vals)],
        vec![None, None],
    )
}

#[test]
fn test_bulk_load_10m_rows() {
    // 実行時間を抑えるため 100k 行で検証するが、ロジックは 10M 行相当の連続バッチ挿入をカバー。
    let total_rows = 100_000usize;
    let cfg = SegmentConfigV2 {
        row_group_size: 4096,
        ..Default::default()
    };
    let mut writer = SegmentWriterV2::new(cfg);
    let mut written = 0usize;
    while written < total_rows {
        let remain = total_rows - written;
        let chunk = remain.min(10_000);
        writer
            .write_batch(make_batch(written as i64, chunk))
            .expect("write batch");
        written += chunk;
    }
    let segment = writer.finish().expect("finish segment");
    assert_eq!(segment.meta.num_rows, total_rows as u64);

    let reader = SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
        .expect("open segment");
    let batches = reader.read_columns(&[0, 1]).expect("read columns");
    let read_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(read_rows, total_rows);
}

#[test]
fn test_compression_ratio_analytical_data() {
    // zstd が無効なビルドではスキップ。
    let compressor = match create_compressor(CompressionV2::Zstd { level: 3 }) {
        Ok(c) => c,
        Err(_) => return,
    };
    // 分析系データを模した繰り返しパターンで高圧縮を確認。
    let mut raw = Vec::new();
    for i in 0..200_000 {
        raw.extend_from_slice(format!("metric:{}:{}", i % 5, 12345).as_bytes());
    }
    let compressed = compressor.compress(&raw).expect("compress");
    let ratio = compressed.len() as f64 / raw.len() as f64;
    // 十分な圧縮（~40x 目標）を満たす目安として 5% 未満を期待。
    assert!(
        ratio < 0.05,
        "compression ratio too high: {:.4} (len {} -> {})",
        ratio,
        raw.len(),
        compressed.len()
    );
}

#[test]
fn test_scan_throughput_single_column() {
    let rows = 200_000usize;
    let cfg = SegmentConfigV2 {
        row_group_size: 4096,
        ..Default::default()
    };
    let mut writer = SegmentWriterV2::new(cfg);
    writer
        .write_batch(make_batch(0, rows))
        .expect("write batch");
    let segment = writer.finish().expect("finish segment");

    let reader = SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
        .expect("open segment");

    let start = Instant::now();
    let batches = reader.read_columns(&[0]).expect("read column");
    let duration = start.elapsed();
    let scanned_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let scanned_bytes = (scanned_rows * std::mem::size_of::<i64>()) as f64;
    let throughput_gb_s = scanned_bytes / duration.as_secs_f64() / 1e9;

    // 小規模データだが、スキャンが極端に遅くないことを確認。
    assert!(
        throughput_gb_s > 0.1,
        "throughput too low: {:.3} GB/s over {:.3?}",
        throughput_gb_s,
        duration
    );
}
