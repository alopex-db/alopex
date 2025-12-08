//! Memory profiling binary for scan strategies.
//!
//! Run with:
//! ```
//! cargo run -p alopex-core --bin memory_profile --features memory-profiling
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crossbeam_skiplist::SkipMap;
use rand::{rngs::StdRng, Rng, SeedableRng};

#[cfg(feature = "memory-profiling")]
use dhat::{HeapStats, Profiler};

#[cfg(feature = "memory-profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

type Key = Vec<u8>;
type Value = u64;

struct Dataset {
    map: Arc<RwLock<BTreeMap<Key, Value>>>,
    skiplist: Arc<SkipMap<Key, Value>>,
    prefix: Key,
    range_start: Key,
    range_end: Key,
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

fn guard_range(ds: &Dataset) -> u64 {
    let guard = ds.map.read().expect("lock");
    guard
        .range(ds.range_start.clone()..ds.range_end.clone())
        .fold(0u64, |acc, (_, v)| acc.wrapping_add(*v))
}

fn precollect_range(ds: &Dataset) -> u64 {
    let guard = ds.map.read().expect("lock");
    let buf: Vec<(Key, Value)> = guard
        .range(ds.range_start.clone()..ds.range_end.clone())
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    drop(guard);
    buf.iter().fold(0u64, |acc, (_, v)| acc.wrapping_add(*v))
}

fn snapshot_range(ds: &Dataset) -> u64 {
    let guard = ds.map.read().expect("lock");
    let cloned = guard.clone();
    drop(guard);
    cloned
        .range(ds.range_start.clone()..ds.range_end.clone())
        .fold(0u64, |acc, (_, v)| acc.wrapping_add(*v))
}

fn skiplist_range(ds: &Dataset) -> u64 {
    ds.skiplist
        .range(ds.range_start.clone()..ds.range_end.clone())
        .fold(0u64, |acc, entry| acc.wrapping_add(*entry.value()))
}

#[cfg(feature = "memory-profiling")]
fn measure<F, R>(name: &str, f: F)
where
    F: FnOnce() -> R,
{
    let _profiler = Profiler::new_heap();
    let result = f();
    std::hint::black_box(result);
    let stats = HeapStats::get();
    println!(
        "{:<20} max_bytes={:>12} total_bytes={:>12} total_blocks={:>8}",
        name, stats.max_bytes, stats.total_bytes, stats.total_blocks
    );
}

#[cfg(not(feature = "memory-profiling"))]
fn measure<F, R>(_name: &str, _f: F)
where
    F: FnOnce() -> R,
{
    eprintln!("ERROR: memory-profiling feature not enabled");
    eprintln!("Run with: cargo run -p alopex-core --bin memory_profile --features memory-profiling");
    std::process::exit(1);
}

fn main() {
    println!("=== Memory Profile: Scan Strategies ===\n");

    for &size in &[1_000usize, 100_000, 1_000_000] {
        println!("--- Dataset size: {} ---", size);

        // Build dataset outside of measurement
        let dataset = build_dataset(size);

        measure("guard_range", || guard_range(&dataset));
        measure("precollect_range", || precollect_range(&dataset));
        measure("snapshot_range", || snapshot_range(&dataset));
        measure("skiplist_range", || skiplist_range(&dataset));

        println!();
    }

    println!("=== Full Scan (1M only) ===\n");
    let dataset = build_dataset(1_000_000);

    measure("guard_full", || {
        let guard = dataset.map.read().expect("lock");
        guard.iter().fold(0u64, |acc, (_, v)| acc.wrapping_add(*v))
    });

    measure("precollect_full", || {
        let guard = dataset.map.read().expect("lock");
        let buf: Vec<(Key, Value)> = guard.iter().map(|(k, v)| (k.clone(), *v)).collect();
        drop(guard);
        buf.iter().fold(0u64, |acc, (_, v)| acc.wrapping_add(*v))
    });

    measure("snapshot_full", || {
        let guard = dataset.map.read().expect("lock");
        let cloned = guard.clone();
        drop(guard);
        cloned.iter().fold(0u64, |acc, (_, v)| acc.wrapping_add(*v))
    });

    measure("skiplist_full", || {
        dataset
            .skiplist
            .iter()
            .fold(0u64, |acc, entry| acc.wrapping_add(*entry.value()))
    });

    println!("\n=== Done ===");
}
