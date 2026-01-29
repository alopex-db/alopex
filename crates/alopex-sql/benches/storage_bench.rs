use std::time::Duration;

use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{ColumnMetadata, TableMetadata};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{KeyEncoder, RowCodec, SqlValue, TableStorage};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

fn bench_rowcodec(c: &mut Criterion) {
    let row = vec![
        SqlValue::Integer(1),
        SqlValue::Text("alice".into()),
        SqlValue::Integer(20),
    ];
    c.bench_function("rowcodec_encode", |b| {
        b.iter(|| {
            let encoded = RowCodec::encode(black_box(&row));
            black_box(encoded);
        })
    });

    let encoded = RowCodec::encode(&row);
    c.bench_function("rowcodec_decode", |b| {
        b.iter(|| {
            let decoded = RowCodec::decode(black_box(&encoded)).unwrap();
            black_box(decoded);
        })
    });
}

fn bench_keyencoder(c: &mut Criterion) {
    c.bench_function("keyencoder_row_key", |b| {
        b.iter(|| {
            let key = KeyEncoder::row_key(1, black_box(42));
            black_box(key);
        })
    });

    let value = SqlValue::Text("alice".into());
    c.bench_function("keyencoder_index_key", |b| {
        b.iter(|| {
            let key = KeyEncoder::index_key(1, black_box(&value), 42).unwrap();
            black_box(key);
        })
    });
}

fn bench_table_storage_insert_scan(c: &mut Criterion) {
    let table_meta = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
        ],
    )
    .with_table_id(1);

    c.bench_function("table_storage_insert_scan", |b| {
        b.iter_batched(
            || {
                let store = MemoryKV::new();
                let store_static: &'static MemoryKV = Box::leak(Box::new(store));
                (store_static, table_meta.clone())
            },
            |(store, meta)| {
                let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
                {
                    let mut table = TableStorage::new(&mut txn, &meta);
                    for i in 0..100 {
                        let row = vec![SqlValue::Integer(i), SqlValue::Text(format!("user{i}"))];
                        table.insert(i as u64, &row).unwrap();
                    }
                    let iter = table.scan().unwrap();
                    for res in iter {
                        let _ = res.unwrap();
                    }
                }
                txn.commit_self().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = storage_micro;
    config = Criterion::default().measurement_time(Duration::from_millis(800));
    targets = bench_rowcodec, bench_keyencoder, bench_table_storage_insert_scan
}
criterion_main!(storage_micro);
