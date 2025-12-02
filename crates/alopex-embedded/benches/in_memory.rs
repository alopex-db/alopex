use alopex_embedded::{Database, DatabaseOptions, TxnMode};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use rand::Rng;
use tempfile::tempdir;

const WRITE_COUNT: usize = 2_000;
const VALUE: &[u8] = b"bench-value-abcdefgh";

fn setup_memory_db() -> Database {
    let opts = DatabaseOptions::in_memory();
    Database::open_in_memory_with_options(opts).unwrap()
}

fn setup_disk_db() -> (Database, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let wal = dir.path().join("bench.wal");
    let db = Database::open(&wal).unwrap();
    (db, dir)
}

fn populate(db: &Database) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(WRITE_COUNT);
    for i in 0..WRITE_COUNT {
        let key = format!("k{i}").into_bytes();
        keys.push(key.clone());
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(&key, VALUE).unwrap();
        txn.commit().unwrap();
    }
    keys
}

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    group.bench_function("memory", |b| {
        b.iter_batched(
            setup_memory_db,
            |db| {
                for i in 0..WRITE_COUNT {
                    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                    txn.put(format!("k{i}").as_bytes(), VALUE).unwrap();
                    txn.commit().unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("disk", |b| {
        b.iter_batched(
            setup_disk_db,
            |(db, _dir)| {
                for i in 0..WRITE_COUNT {
                    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                    txn.put(format!("k{i}").as_bytes(), VALUE).unwrap();
                    txn.commit().unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_read_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_latency");

    // Memory setup
    let mem_db = setup_memory_db();
    let mem_keys = populate(&mem_db);

    // Disk setup
    let (disk_db, _dir) = setup_disk_db();
    let disk_keys = populate(&disk_db);

    group.bench_function("memory_random_read", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..mem_keys.len());
            let mut txn = mem_db.begin(TxnMode::ReadOnly).unwrap();
            black_box(txn.get(&mem_keys[idx]).unwrap());
        });
    });

    group.bench_function("disk_random_read", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..disk_keys.len());
            let mut txn = disk_db.begin(TxnMode::ReadOnly).unwrap();
            black_box(txn.get(&disk_keys[idx]).unwrap());
        });
    });

    group.finish();
}

fn benches(c: &mut Criterion) {
    bench_write_throughput(c);
    bench_read_latency(c);
}

criterion_group!(memory_benches, benches);
criterion_main!(memory_benches);
