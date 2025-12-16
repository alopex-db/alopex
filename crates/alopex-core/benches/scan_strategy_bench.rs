use std::collections::BTreeMap;
use std::sync::{Arc, Barrier, RwLock, RwLockReadGuard};
use std::thread;
use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use crossbeam_skiplist::SkipMap;
use rand::{rngs::StdRng, Rng, SeedableRng};

type Key = Vec<u8>;
type Value = u64;

const DATASET_SIZES: &[usize] = &[1_000, 100_000, 1_000_000];

#[derive(Clone, Copy, PartialEq)]
enum ScanMode {
    Full,
    Prefix,
    Range,
}

impl ScanMode {
    fn label(&self) -> &'static str {
        match self {
            ScanMode::Full => "full",
            ScanMode::Prefix => "prefix",
            ScanMode::Range => "range",
        }
    }
}

struct Dataset {
    map: Arc<RwLock<BTreeMap<Key, Value>>>,
    skiplist: Arc<SkipMap<Key, Value>>,
    prefix: Key,
    range_start: Key,
    range_end: Key,
}

impl Clone for Dataset {
    fn clone(&self) -> Self {
        Self {
            map: Arc::clone(&self.map),
            skiplist: Arc::clone(&self.skiplist),
            prefix: self.prefix.clone(),
            range_start: self.range_start.clone(),
            range_end: self.range_end.clone(),
        }
    }
}

fn build_dataset(size: usize) -> Dataset {
    let mut map = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(42);
    for i in 0..size {
        let key = format!("key_{:08}_{}", i, rng.gen::<u32>()).into_bytes();
        map.insert(key, i as u64);
    }

    let skip = SkipMap::new();
    for (k, v) in &map {
        skip.insert(k.clone(), *v);
    }

    // Scale prefix/range coverage with dataset size.
    // prefix_len controls how many digits are fixed: 7 -> ~10 items, 6 -> ~100, 5 -> ~1_000.
    let prefix_len = if size <= 10_000 {
        7
    } else if size <= 100_000 {
        6
    } else {
        5
    };
    let prefix_idx = size / 3;
    let full_prefix = format!("key_{:08}_", prefix_idx);
    let prefix = full_prefix.as_bytes()[..4 + prefix_len].to_vec();

    let range_span = std::cmp::max(1, size / 10);
    let range_start_idx = size / 2;
    let range_end_idx = (range_start_idx + range_span).min(size.saturating_sub(1));
    let range_start = format!("key_{:08}_", range_start_idx).into_bytes();
    let range_end = format!("key_{:08}_", range_end_idx).into_bytes();

    Dataset {
        map: Arc::new(RwLock::new(map)),
        skiplist: Arc::new(skip),
        prefix,
        range_start,
        range_end,
    }
}

struct GuardedIterator<'a> {
    guard: RwLockReadGuard<'a, BTreeMap<Key, Value>>,
}

impl<'a> GuardedIterator<'a> {
    fn new(guard: RwLockReadGuard<'a, BTreeMap<Key, Value>>) -> Self {
        Self { guard }
    }

    fn iter(
        &'a self,
        mode: ScanMode,
        prefix: &'a Key,
        range_start: &'a Key,
        range_end: &'a Key,
    ) -> Box<dyn Iterator<Item = (&'a Key, &'a Value)> + 'a> {
        match mode {
            ScanMode::Full => Box::new(self.guard.iter()),
            ScanMode::Prefix => Box::new(
                self.guard
                    .range(prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(prefix)),
            ),
            ScanMode::Range => Box::new(self.guard.range(range_start.clone()..range_end.clone())),
        }
    }

    fn fold_sum(&'a self, mode: ScanMode, prefix: &Key, range_start: &Key, range_end: &Key) -> u64 {
        self.iter(mode, prefix, range_start, range_end)
            .fold(0u64, |acc, (_, v)| black_box(acc.wrapping_add(*v)))
    }

    fn creation_only(&'a self, mode: ScanMode, prefix: &Key, range_start: &Key, range_end: &Key) {
        black_box(self.iter(mode, prefix, range_start, range_end).next());
    }
}

fn guard_in_struct(ds: &Dataset, mode: ScanMode) -> u64 {
    let guard = ds.map.read().expect("map lock poisoned");
    let iter = GuardedIterator::new(guard);
    iter.fold_sum(mode, &ds.prefix, &ds.range_start, &ds.range_end)
}

fn pre_collect(ds: &Dataset, mode: ScanMode) -> u64 {
    let guard = ds.map.read().expect("map lock poisoned");
    let buf: Vec<(Key, Value)> = match mode {
        ScanMode::Full => guard.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        ScanMode::Prefix => guard
            .range(ds.prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&ds.prefix))
            .map(|(k, v)| (k.clone(), *v))
            .collect(),
        ScanMode::Range => guard
            .range(ds.range_start.clone()..ds.range_end.clone())
            .map(|(k, v)| (k.clone(), *v))
            .collect(),
    };
    drop(guard);
    buf.iter()
        .fold(0u64, |acc, (_, v)| black_box(acc.wrapping_add(*v)))
}

fn snapshot_clone(ds: &Dataset, mode: ScanMode) -> u64 {
    let guard = ds.map.read().expect("map lock poisoned");
    let cloned = guard.clone();
    drop(guard);
    let iter: Box<dyn Iterator<Item = (&Key, &Value)>> = match mode {
        ScanMode::Full => Box::new(cloned.iter()),
        ScanMode::Prefix => Box::new(
            cloned
                .range(ds.prefix.clone()..)
                .take_while(|(k, _)| k.starts_with(&ds.prefix)),
        ),
        ScanMode::Range => Box::new(cloned.range(ds.range_start.clone()..ds.range_end.clone())),
    };
    iter.fold(0u64, |acc, (_, v)| black_box(acc.wrapping_add(*v)))
}

fn skiplist_scan(ds: &Dataset, mode: ScanMode) -> u64 {
    match mode {
        ScanMode::Full => ds.skiplist.iter().fold(0u64, |acc, entry| {
            black_box(acc.wrapping_add(*entry.value()))
        }),
        ScanMode::Prefix => ds
            .skiplist
            .range(ds.prefix.clone()..)
            .take_while(|entry| entry.key().starts_with(&ds.prefix))
            .fold(0u64, |acc, entry| {
                black_box(acc.wrapping_add(*entry.value()))
            }),
        ScanMode::Range => ds
            .skiplist
            .range(ds.range_start.clone()..ds.range_end.clone())
            .fold(0u64, |acc, entry| {
                black_box(acc.wrapping_add(*entry.value()))
            }),
    }
}

fn bench_modes(c: &mut Criterion, size: usize) {
    let dataset = Arc::new(build_dataset(size));
    let mut group = c.benchmark_group(format!("scan_strategies_{size}"));

    type Strategy = (&'static str, fn(&Dataset, ScanMode) -> u64);
    let strategies: &[Strategy] = &[
        ("guard", guard_in_struct),
        ("precollect", pre_collect),
        ("snapshot", snapshot_clone),
        ("skiplist", skiplist_scan),
    ];

    for &(name, func) in strategies {
        for mode in [ScanMode::Full, ScanMode::Prefix, ScanMode::Range] {
            // Skip combinations that don't make sense or add duplicate signal.
            if name == "snapshot" && mode == ScanMode::Prefix {
                continue;
            }
            let ds = Arc::clone(&dataset);
            group.bench_function(
                BenchmarkId::new(format!("{name}_{}", mode.label()), size),
                move |b| b.iter_batched(|| ds.clone(), |ds| func(&ds, mode), BatchSize::SmallInput),
            );
        }
    }

    group.finish();
}

fn bench_iter_creation(c: &mut Criterion) {
    for &size in DATASET_SIZES {
        let dataset = Arc::new(build_dataset(size));
        let mut group = c.benchmark_group(format!("iter_creation_{size}"));
        group.bench_function(BenchmarkId::new("guard_full_iter", size), |b| {
            let ds = Arc::clone(&dataset);
            b.iter(|| {
                let guard = ds.map.read().expect("map lock poisoned");
                let iter = GuardedIterator::new(guard);
                iter.creation_only(ScanMode::Full, &ds.prefix, &ds.range_start, &ds.range_end);
            });
        });
        group.bench_function(BenchmarkId::new("guard_prefix_iter", size), |b| {
            let ds = Arc::clone(&dataset);
            b.iter(|| {
                let guard = ds.map.read().expect("map lock poisoned");
                let iter = GuardedIterator::new(guard);
                iter.creation_only(ScanMode::Prefix, &ds.prefix, &ds.range_start, &ds.range_end);
            });
        });
        group.bench_function(BenchmarkId::new("guard_range_iter", size), |b| {
            let ds = Arc::clone(&dataset);
            b.iter(|| {
                let guard = ds.map.read().expect("map lock poisoned");
                let iter = GuardedIterator::new(guard);
                iter.creation_only(ScanMode::Range, &ds.prefix, &ds.range_start, &ds.range_end);
            });
        });
        group.bench_function(BenchmarkId::new("skiplist_full_iter", size), |b| {
            let ds = Arc::clone(&dataset);
            b.iter(|| {
                let mut iter = ds.skiplist.iter();
                black_box(iter.next());
            });
        });
        group.bench_function(BenchmarkId::new("skiplist_prefix_iter", size), |b| {
            let ds = Arc::clone(&dataset);
            b.iter(|| {
                let mut iter = ds.skiplist.range(ds.prefix.clone()..);
                black_box(iter.next());
            });
        });
        group.bench_function(BenchmarkId::new("skiplist_range_iter", size), |b| {
            let ds = Arc::clone(&dataset);
            b.iter(|| {
                let mut iter = ds
                    .skiplist
                    .range(ds.range_start.clone()..ds.range_end.clone());
                black_box(iter.next());
            });
        });
        group.finish();
    }
}

fn bench_concurrent_reads(c: &mut Criterion) {
    let size = 100_000;
    let dataset = Arc::new(build_dataset(size));
    for readers in [1_usize, 2, 4, 8] {
        let ds = Arc::clone(&dataset);
        c.bench_function(&format!("concurrent_guard_full_{readers}"), move |b| {
            b.iter_custom(|iters| {
                let barrier = Arc::new(Barrier::new(readers + 1));
                let mut handles = Vec::with_capacity(readers);
                for _ in 0..readers {
                    let ds = Arc::clone(&ds);
                    let barrier = barrier.clone();
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..iters {
                            black_box(guard_in_struct(&ds, ScanMode::Full));
                        }
                    }));
                }
                let start = Instant::now();
                barrier.wait();
                for handle in handles {
                    handle.join().expect("reader thread");
                }
                start.elapsed()
            });
        });
    }
}

fn bench_concurrent_skiplist_reads(c: &mut Criterion) {
    let size = 100_000;
    let dataset = Arc::new(build_dataset(size));
    for readers in [1_usize, 2, 4, 8] {
        let ds = Arc::clone(&dataset);
        c.bench_function(&format!("concurrent_skiplist_full_{readers}"), move |b| {
            b.iter_custom(|iters| {
                let barrier = Arc::new(Barrier::new(readers + 1));
                let mut handles = Vec::with_capacity(readers);
                for _ in 0..readers {
                    let ds = Arc::clone(&ds);
                    let barrier = barrier.clone();
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..iters {
                            black_box(skiplist_scan(&ds, ScanMode::Full));
                        }
                    }));
                }
                let start = Instant::now();
                barrier.wait();
                for handle in handles {
                    handle.join().expect("reader thread");
                }
                start.elapsed()
            });
        });
    }
}

fn bench_write_while_read(c: &mut Criterion) {
    let size = 100_000;
    let dataset = Arc::new(build_dataset(size));
    let ds = Arc::clone(&dataset);
    c.bench_function("write_while_read_guard_full", move |b| {
        b.iter_custom(|iters| {
            let barrier = Arc::new(Barrier::new(2));
            let reader_ds = Arc::clone(&ds);
            let reader_barrier = barrier.clone();
            let reader = thread::spawn(move || {
                reader_barrier.wait();
                for _ in 0..iters {
                    black_box(guard_in_struct(&reader_ds, ScanMode::Full));
                }
            });

            barrier.wait();
            let start = Instant::now();
            for i in 0..iters {
                let key = format!("hot_write_{i:08}").into_bytes();
                let mut guard = ds.map.write().expect("map lock poisoned");
                guard.insert(key, i);
            }
            reader.join().expect("reader thread");
            start.elapsed()
        });
    });
}

fn scan_strategy_bench(c: &mut Criterion) {
    for &size in DATASET_SIZES {
        bench_modes(c, size);
    }
    bench_iter_creation(c);
    bench_concurrent_reads(c);
    bench_concurrent_skiplist_reads(c);
    bench_write_while_read(c);
}

criterion_group!(benches, scan_strategy_bench);
criterion_main!(benches);
