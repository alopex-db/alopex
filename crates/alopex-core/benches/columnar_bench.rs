use std::time::Instant;

use alopex_core::columnar::encoding::{Column, LogicalType};
use alopex_core::columnar::segment_v2::{
    ColumnSchema, InMemorySegmentSource, RecordBatch, Schema, SegmentConfigV2, SegmentReaderV2,
    SegmentWriterV2,
};
use alopex_core::storage::compression::{create_compressor, CompressionV2};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};

fn schema() -> Schema {
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

fn make_records(len: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..len as i64).collect();
    let vals: Vec<i64> = ids.iter().map(|v| v * 2).collect();
    RecordBatch::new(
        schema(),
        vec![Column::Int64(ids), Column::Int64(vals)],
        vec![None, None],
    )
}

fn bench_compression_ratio(c: &mut Criterion) {
    let data_size = 1_000_000; // 1MB 相当で短時間に圧縮率を確認
    let data = vec![b'a'; data_size];
    c.bench_function("compression_ratio_zstd_level3", |b| {
        b.iter(|| {
            let compressor = match create_compressor(CompressionV2::Zstd { level: 3 }) {
                Ok(c) => c,
                Err(_) => return,
            };
            let compressed = compressor.compress(&data).expect("compress");
            black_box(compressed.len());
        });
    });
}

fn bench_scan_throughput(c: &mut Criterion) {
    let cfg = SegmentConfigV2 {
        row_group_size: 4096,
        ..Default::default()
    };
    // 1M 行 ≒ 8MB 程度でスキャン速度を測定
    let rows = 1_000_000usize;
    let mut writer = SegmentWriterV2::new(cfg);
    writer
        .write_batch(make_records(rows))
        .expect("write batch for bench");
    let segment = writer.finish().expect("finish segment");

    c.bench_function("scan_single_column_lz4_like", |b| {
        b.iter_batched(
            || {
                SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
                    .expect("open for bench")
            },
            |reader| {
                let start = Instant::now();
                let batches = reader.read_columns(&[0]).expect("read column");
                let elapsed = start.elapsed();
                let rows_scanned: usize = batches.iter().map(|b| b.num_rows()).sum();
                black_box((rows_scanned, elapsed));
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_write_throughput(c: &mut Criterion) {
    let rows = 200_000usize;
    let batch = make_records(rows);
    c.bench_function("write_mixed_zstd_level1", |b| {
        b.iter(|| {
            let cfg = SegmentConfigV2 {
                compression: CompressionV2::Zstd { level: 1 },
                ..Default::default()
            };
            let mut writer = SegmentWriterV2::new(cfg);
            if let Err(_) = writer.write_batch(batch.clone()) {
                return;
            }
            let _ = writer.finish();
        });
    });
}

criterion_group!(
    benches,
    bench_compression_ratio,
    bench_scan_throughput,
    bench_write_throughput
);
criterion_main!(benches);
