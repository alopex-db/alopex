use alopex_embedded::{Database, DatabaseOptions, TxnMode};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use rand::Rng;
use tempfile::tempdir;

const WRITE_COUNT: usize = 1_000;
const VALUE: &[u8] = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // 32 bytes

fn setup_memory_db() -> Database {
    Database::open_in_memory_with_options(DatabaseOptions::in_memory()).unwrap()
}

fn setup_disk_db() -> (Database, tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().unwrap();
    let wal = dir.path().join("bench.wal");
    let db = Database::open(&wal).unwrap();
    (db, dir, wal)
}

fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    group.bench_function("memory_write", |b| {
        b.iter_batched(
            setup_memory_db,
            |db| {
                for i in 0..WRITE_COUNT {
                    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                    let key = format!("key{}", i).into_bytes();
                    txn.put(&key, VALUE).unwrap();
                    txn.commit().unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("disk_write", |b| {
        b.iter_batched(
            setup_disk_db,
            |(db, _dir, _wal)| {
                for i in 0..WRITE_COUNT {
                    let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
                    let key = format!("key{}", i).into_bytes();
                    txn.put(&key, VALUE).unwrap();
                    txn.commit().unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn prepopulate_for_reads(memory: bool) -> (Database, Vec<Vec<u8>>, Option<tempfile::TempDir>) {
    if memory {
        let db = setup_memory_db();
        let keys = populate(&db);
        (db, keys, None)
    } else {
        let (db, dir, _wal) = setup_disk_db();
        let keys = populate(&db);
        (db, keys, Some(dir))
    }
}

fn populate(db: &Database) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(WRITE_COUNT);
    for i in 0..WRITE_COUNT {
        let key = format!("key{}", i).into_bytes();
        keys.push(key.clone());
        let mut txn = db.begin(TxnMode::ReadWrite).unwrap();
        txn.put(&key, VALUE).unwrap();
        txn.commit().unwrap();
    }
    keys
}

fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_latency");

    // Memory mode reads
    let (db_mem, keys_mem, _dir_mem) = prepopulate_for_reads(true);
    group.bench_function("memory_read_random", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..keys_mem.len());
            let key = &keys_mem[idx];
            let mut txn = db_mem.begin(TxnMode::ReadOnly).unwrap();
            black_box(txn.get(key).unwrap());
        });
    });

    // Disk mode reads
    let (db_disk, keys_disk, _dir_disk) = prepopulate_for_reads(false);
    group.bench_function("disk_read_random", |b| {
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let idx = rng.gen_range(0..keys_disk.len());
            let key = &keys_disk[idx];
            let mut txn = db_disk.begin(TxnMode::ReadOnly).unwrap();
            black_box(txn.get(key).unwrap());
        });
    });

    group.finish();
}

fn criterion_benches(c: &mut Criterion) {
    bench_writes(c);
    bench_reads(c);
}

criterion_group!(benches, criterion_benches);
criterion_main!(benches);
