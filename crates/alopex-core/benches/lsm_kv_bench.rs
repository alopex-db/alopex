//! LsmKV / MemoryKV の基本ベンチマーク（Phase 6.3）。
//!
//! 目的:
//! - シーケンシャル/ランダム書き込み（バッチ）でのスループット比較
//! - Point Get / Point Put のレイテンシ比較
//! - 範囲読み取り（scan_range）の比較
//!
//! 注意:
//! - 現段階の LsmKV は SSTable への永続 flush を伴わないため、主に WAL + MemTable のコストを測ります。
//! - 環境依存が大きいので、必要に応じて環境変数で規模を調整してください。
//!   - `ALOPEX_LSM_BENCH_N`: 書き込み/読み取りの件数（デフォルト 100_000）
//!   - `ALOPEX_LSM_BENCH_SCAN_LEN`: scan_range の範囲長（デフォルト 1_000）

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::lsm::memtable::MemTableConfig;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::{LsmKV, LsmKVConfig};
use alopex_core::types::TxnMode;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(default)
}

fn bench_config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            // 大量のベンチ反復でもリングが詰まりにくい容量を確保する。
            segment_size: 1024 * 1024,
            max_segments: 64,
            sync_mode: SyncMode::NoSync,
        },
        memtable: MemTableConfig {
            // ベンチ中は「自動 flush」による影響を避ける。
            flush_threshold: usize::MAX,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn key_for(i: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 8);
    k.extend_from_slice(b"k:");
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn value_for(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

struct LsmHarness {
    _dir: TempDir,
    store: LsmKV,
}

impl LsmHarness {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, _recovery) =
            LsmKV::open_with_config(dir.path(), bench_config()).expect("open lsmkv");
        Self { _dir: dir, store }
    }
}

fn bench_batch_writes(c: &mut Criterion) {
    let n = env_usize("ALOPEX_LSM_BENCH_N", 100_000);

    let mut group = c.benchmark_group("lsm_kv/batch_writes");
    group.measurement_time(Duration::from_secs(5));

    for &(label, shuffle) in &[("sequential", false), ("random", true)] {
        group.bench_with_input(BenchmarkId::new("memorykv", label), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut keys: Vec<u64> = (0..n as u64).collect();
                    if shuffle {
                        let mut rng = StdRng::seed_from_u64(42);
                        keys.shuffle(&mut rng);
                    }
                    keys
                },
                |keys| {
                    let store = MemoryKV::new();
                    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
                    for k in keys {
                        tx.put(key_for(k), value_for(k)).unwrap();
                    }
                    tx.commit_self().unwrap();
                    black_box(store)
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("lsmkv", label), &n, |b, &n| {
            b.iter_batched(
                || {
                    let harness = LsmHarness::new();
                    let mut keys: Vec<u64> = (0..n as u64).collect();
                    if shuffle {
                        let mut rng = StdRng::seed_from_u64(42);
                        keys.shuffle(&mut rng);
                    }
                    (harness, keys)
                },
                |(harness, keys)| {
                    let store = harness.store;
                    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
                    for k in keys {
                        tx.put(key_for(k), value_for(k)).unwrap();
                    }
                    tx.commit_self().unwrap();
                    black_box(store)
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_point_put(c: &mut Criterion) {
    let counter = AtomicU64::new(0);

    let mut group = c.benchmark_group("lsm_kv/point_put");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("memorykv", |b| {
        let store = MemoryKV::new();
        b.iter_batched(
            || {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                (key_for(i), value_for(i))
            },
            |(k, v)| {
                let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
                tx.put(k, v).unwrap();
                tx.commit_self().unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("lsmkv", |b| {
        let harness = LsmHarness::new();
        b.iter_batched(
            || {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                (key_for(i), value_for(i))
            },
            |(k, v)| {
                let mut tx = harness.store.begin(TxnMode::ReadWrite).unwrap();
                tx.put(k, v).unwrap();
                tx.commit_self().unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_point_get(c: &mut Criterion) {
    let n = env_usize("ALOPEX_LSM_BENCH_N", 100_000);

    let mut group = c.benchmark_group("lsm_kv/point_get");
    group.measurement_time(Duration::from_secs(5));

    group.bench_with_input(BenchmarkId::new("memorykv", n), &n, |b, &n| {
        let store = MemoryKV::new();
        {
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            for i in 0..n as u64 {
                tx.put(key_for(i), value_for(i)).unwrap();
            }
            tx.commit_self().unwrap();
        }
        let mut rng = StdRng::seed_from_u64(7);

        b.iter_batched(
            || key_for(rng.gen_range(0..n as u64)),
            |k| {
                let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
                let v = tx.get(&k).unwrap();
                black_box(v);
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_with_input(BenchmarkId::new("lsmkv", n), &n, |b, &n| {
        let harness = LsmHarness::new();
        {
            let mut tx = harness.store.begin(TxnMode::ReadWrite).unwrap();
            for i in 0..n as u64 {
                tx.put(key_for(i), value_for(i)).unwrap();
            }
            tx.commit_self().unwrap();
        }
        let mut rng = StdRng::seed_from_u64(7);

        b.iter_batched(
            || key_for(rng.gen_range(0..n as u64)),
            |k| {
                let mut tx = harness.store.begin(TxnMode::ReadOnly).unwrap();
                let v = tx.get(&k).unwrap();
                black_box(v);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_scan_range(c: &mut Criterion) {
    let n = env_usize("ALOPEX_LSM_BENCH_N", 100_000);
    let scan_len = env_usize("ALOPEX_LSM_BENCH_SCAN_LEN", 1_000) as u64;

    let mut group = c.benchmark_group("lsm_kv/scan_range");
    group.measurement_time(Duration::from_secs(5));

    group.bench_with_input(BenchmarkId::new("memorykv", n), &n, |b, &n| {
        let store = MemoryKV::new();
        {
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            for i in 0..n as u64 {
                tx.put(key_for(i), value_for(i)).unwrap();
            }
            tx.commit_self().unwrap();
        }

        b.iter_batched(
            || {
                let start = (n as u64 / 2).saturating_sub(scan_len / 2);
                let end = (start + scan_len).min(n as u64);
                (key_for(start), key_for(end))
            },
            |(start, end)| {
                let mut tx = store.begin(TxnMode::ReadOnly).unwrap();
                let it = tx.scan_range(&start, &end).unwrap();
                let mut count = 0usize;
                for (_k, _v) in it {
                    count += 1;
                }
                black_box(count);
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_with_input(BenchmarkId::new("lsmkv", n), &n, |b, &n| {
        let harness = LsmHarness::new();
        {
            let mut tx = harness.store.begin(TxnMode::ReadWrite).unwrap();
            for i in 0..n as u64 {
                tx.put(key_for(i), value_for(i)).unwrap();
            }
            tx.commit_self().unwrap();
        }

        b.iter_batched(
            || {
                let start = (n as u64 / 2).saturating_sub(scan_len / 2);
                let end = (start + scan_len).min(n as u64);
                (key_for(start), key_for(end))
            },
            |(start, end)| {
                let mut tx = harness.store.begin(TxnMode::ReadOnly).unwrap();
                let it = tx.scan_range(&start, &end).unwrap();
                let mut count = 0usize;
                for (_k, _v) in it {
                    count += 1;
                }
                black_box(count);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_batch_writes,
    bench_point_put,
    bench_point_get,
    bench_scan_range
);
criterion_main!(benches);
