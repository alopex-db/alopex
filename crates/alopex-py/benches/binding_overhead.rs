use alopex_core::TxnMode;
use alopex_embedded::Database;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

fn bench_put_get(c: &mut Criterion) {
    c.bench_function("embedded_put_get", |b| {
        b.iter_batched(
            Database::new,
            |db| {
                let mut txn = db.begin(TxnMode::ReadWrite).expect("begin");
                txn.put(b"12345678", b"abcdefgh").expect("put");
                txn.commit().expect("commit");

                let mut txn = db.begin(TxnMode::ReadOnly).expect("begin");
                let _ = txn.get(b"12345678").expect("get");
                txn.commit().expect("commit");
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_put_get);
criterion_main!(benches);
