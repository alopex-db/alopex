//! Bounded graph ownership and observations shared with transaction-local caches.

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use alopex_core::vector::hnsw::HnswIndex;
use alopex_sql::storage::HnswCacheObserver;
use serde::Serialize;

#[derive(Default)]
struct EntryMetrics {
    hits: AtomicU64,
    last_access: AtomicU64,
    last_access_ns: AtomicU64,
}

pub(crate) struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    oversize_skips: AtomicU64,
    invalidations: AtomicU64,
    load_ns_total: AtomicU64,
    sequence: AtomicU64,
    started: Instant,
    warned: Mutex<HashSet<String>>,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            oversize_skips: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            load_ns_total: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            started: Instant::now(),
            warned: Mutex::new(HashSet::new()),
        }
    }
}
impl Counters {
    fn reset_warning(&self, name: &str) {
        self.warned
            .lock()
            .expect("HNSW warning lock poisoned")
            .remove(name);
    }
    pub(crate) fn reset_warnings(&self) {
        self.warned
            .lock()
            .expect("HNSW warning lock poisoned")
            .clear();
    }
    fn touch(&self, entry: &EntryMetrics) {
        entry.hits.fetch_add(1, Ordering::Relaxed);
        entry.last_access.store(
            self.sequence.fetch_add(1, Ordering::Relaxed) + 1,
            Ordering::Relaxed,
        );
        entry.last_access_ns.store(
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
    pub(crate) fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn loaded(&self, elapsed_ns: u64) {
        self.load_ns_total.fetch_add(elapsed_ns, Ordering::Relaxed);
    }
    pub(crate) fn oversize(&self, name: &str, bytes: u64, limit: u64) {
        self.oversize_skips.fetch_add(1, Ordering::Relaxed);
        if self
            .warned
            .lock()
            .expect("HNSW warning lock poisoned")
            .insert(name.to_owned())
        {
            tracing::warn!(
                index = name,
                memory_bytes = bytes,
                limit_bytes = limit,
                "HNSW cache retention is disabled or too small; queries remain available without retention"
            );
        }
    }
}

#[derive(Clone)]
struct EntryMetadata {
    bytes: u64,
    metrics: Arc<EntryMetrics>,
}

/// Read-only map compatibility keeps existing cache inspection helpers simple;
/// all mutations pass through methods that maintain byte accounting.
#[derive(Clone)]
pub(crate) struct HnswCacheState {
    indices: HashMap<String, Arc<HnswIndex>>,
    metadata: HashMap<String, EntryMetadata>,
    pub(crate) used_bytes: u64,
    pub(crate) max_entries: usize,
    pub(crate) counters: Arc<Counters>,
}
impl Default for HnswCacheState {
    fn default() -> Self {
        Self {
            indices: HashMap::new(),
            metadata: HashMap::new(),
            used_bytes: 0,
            max_entries: crate::HNSW_CACHE_MAX_ENTRIES,
            counters: Arc::new(Counters::default()),
        }
    }
}
impl Deref for HnswCacheState {
    type Target = HashMap<String, Arc<HnswIndex>>;
    fn deref(&self) -> &Self::Target {
        &self.indices
    }
}
impl<'a> IntoIterator for &'a HnswCacheState {
    type Item = (&'a String, &'a Arc<HnswIndex>);
    type IntoIter = std::collections::hash_map::Iter<'a, String, Arc<HnswIndex>>;
    fn into_iter(self) -> Self::IntoIter {
        self.indices.iter()
    }
}
impl HnswCacheState {
    pub(crate) fn get(&self, name: &str) -> Option<&Arc<HnswIndex>> {
        let index = self.indices.get(name)?;
        self.counters.hits.fetch_add(1, Ordering::Relaxed);
        self.counters.touch(&self.metadata[name].metrics);
        Some(index)
    }
    pub(crate) fn insert(&mut self, name: String, index: Arc<HnswIndex>) {
        self.counters.reset_warning(&name);
        self.remove_untracked(&name);
        let bytes = index.stats().memory_bytes;
        let metrics = Arc::new(EntryMetrics::default());
        metrics.last_access.store(
            self.counters.sequence.fetch_add(1, Ordering::Relaxed) + 1,
            Ordering::Relaxed,
        );
        metrics.last_access_ns.store(
            self.counters
                .started
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.indices.insert(name.clone(), index);
        self.metadata.insert(name, EntryMetadata { bytes, metrics });
    }
    fn remove_untracked(&mut self, name: &str) -> Option<Arc<HnswIndex>> {
        if let Some(meta) = self.metadata.remove(name) {
            self.used_bytes = self.used_bytes.saturating_sub(meta.bytes);
        }
        self.indices.remove(name)
    }
    pub(crate) fn remove(&mut self, name: &str) -> Option<Arc<HnswIndex>> {
        self.counters.reset_warning(name);
        let removed = self.remove_untracked(name);
        if removed.is_some() {
            self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }
    pub(crate) fn clear(&mut self) {
        self.counters.reset_warnings();
        self.counters
            .invalidations
            .fetch_add(self.indices.len() as u64, Ordering::Relaxed);
        self.indices.clear();
        self.metadata.clear();
        self.used_bytes = 0;
    }
    pub(crate) fn reject_oversize(&mut self, name: &str, bytes: u64, limit: u64) {
        self.counters.oversize(name, bytes, limit);
        // Admission failure may retire an older retained version of this graph.
        // It is a capacity eviction, not an invalidation by committed data.
        if self.remove_untracked(name).is_some() {
            self.counters.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn trim(&mut self, bytes: u64, preserve: Option<&str>) {
        while (bytes == 0 && !self.indices.is_empty())
            || self.used_bytes > bytes
            || self.indices.len() > self.max_entries
        {
            let victim = self
                .metadata
                .iter()
                .filter(|(name, _)| Some(name.as_str()) != preserve)
                .min_by_key(|(name, meta)| {
                    (meta.metrics.last_access.load(Ordering::Relaxed), *name)
                })
                .map(|(name, _)| name.clone())
                .or_else(|| preserve.map(str::to_owned));
            let Some(victim) = victim else {
                break;
            };
            if self.remove_untracked(&victim).is_none() {
                break;
            }
            self.counters.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn observer(&self) -> Arc<dyn HnswCacheObserver> {
        Arc::new(Observation {
            counters: self.counters.clone(),
            entries: self
                .metadata
                .iter()
                .map(|(name, meta)| (name.clone(), meta.metrics.clone()))
                .collect(),
        })
    }
    pub(crate) fn stats(&self, limit_bytes: u64) -> HnswCacheStats {
        let mut indexes: Vec<_> = self
            .metadata
            .iter()
            .map(|(name, meta)| HnswCacheIndexStats {
                name: name.clone(),
                bytes: meta.bytes,
                hits: meta.metrics.hits.load(Ordering::Relaxed),
                last_access_ns: meta.metrics.last_access_ns.load(Ordering::Relaxed),
            })
            .collect();
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        let read = |value: &AtomicU64| value.load(Ordering::Relaxed);
        HnswCacheStats {
            limit_bytes,
            limit_entries: self.max_entries,
            used_bytes: self.used_bytes,
            entries: self.indices.len(),
            hits: read(&self.counters.hits),
            misses: read(&self.counters.misses),
            evictions: read(&self.counters.evictions),
            oversize_skips: read(&self.counters.oversize_skips),
            invalidations: read(&self.counters.invalidations),
            load_ns_total: read(&self.counters.load_ns_total),
            indexes,
        }
    }
}
struct Observation {
    counters: Arc<Counters>,
    entries: HashMap<String, Arc<EntryMetrics>>,
}
impl HnswCacheObserver for Observation {
    fn hit(&self, name: &str, shared: bool) {
        self.counters.hits.fetch_add(1, Ordering::Relaxed);
        if shared {
            if let Some(entry) = self.entries.get(name) {
                self.counters.touch(entry);
            }
        }
    }
    fn miss(&self) {
        self.counters.miss();
    }
    fn loaded(&self, elapsed_ns: u64) {
        self.counters.loaded(elapsed_ns);
    }
    fn oversize(&self, name: &str, bytes: u64, limit: u64) {
        self.counters.oversize(name, bytes, limit);
    }
}

/// Cache-owned graph estimates; active reader handles and temporary loads are separate.
#[derive(Debug, Clone, Serialize)]
pub struct HnswCacheStats {
    /// Maximum estimated graph bytes retained globally.
    pub limit_bytes: u64,
    /// Maximum retained graph count.
    pub limit_entries: usize,
    /// Estimated bytes owned by the retained global entries.
    pub used_bytes: u64,
    /// Number of globally retained graph entries.
    pub entries: usize,
    /// Graph reuse, including transaction-local hits.
    pub hits: u64,
    /// Actual graph load attempts.
    pub misses: u64,
    /// Globally retained entries removed by capacity limits or admission failure.
    pub evictions: u64,
    /// Graphs skipped because their byte budget cannot retain them.
    pub oversize_skips: u64,
    /// Retained entries invalidated by committed changes.
    pub invalidations: u64,
    /// Total time spent attempting graph loads, in nanoseconds.
    pub load_ns_total: u64,
    /// Current global entries; transaction-local private graphs are excluded.
    pub indexes: Vec<HnswCacheIndexStats>,
}
/// Access counters for one currently retained shared graph.
#[derive(Debug, Clone, Serialize)]
pub struct HnswCacheIndexStats {
    /// Index name.
    pub name: String,
    /// Estimated retained graph bytes.
    pub bytes: u64,
    /// Shared graph reuse, including explicit transaction access.
    pub hits: u64,
    /// Nanoseconds since cache initialization at last shared access.
    pub last_access_ns: u64,
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    use crate::Database;
    use alopex_core::vector::hnsw::HnswConfig;

    struct Warnings(Arc<AtomicU64>);
    impl tracing::Subscriber for Warnings {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() == tracing::Level::WARN
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    fn capture_warnings(test: impl FnOnce(&AtomicU64)) {
        let count = Arc::new(AtomicU64::new(0));
        tracing::subscriber::with_default(Warnings(count.clone()), || test(&count));
    }
    fn graph(name: &str) -> Arc<HnswIndex> {
        let mut index = HnswIndex::create(name, HnswConfig::default().with_dimension(2)).unwrap();
        index.upsert(b"row", &[1.0, 0.0], b"").unwrap();
        Arc::new(index)
    }
    fn reject(db: &Database, name: &str) {
        let mut cache = db.hnsw_cache.write().unwrap();
        Database::hnsw_cache_insert_bounded(&mut cache, name.into(), graph(name), 0);
    }

    #[test]
    fn fresh_rejection_counts_each_skip_but_warns_once() {
        capture_warnings(|warnings| {
            let db = Database::new();
            reject(&db, "a");
            reject(&db, "a");
            let stats = db.hnsw_cache_stats();
            assert_eq!(stats.oversize_skips, 2);
            assert_eq!(
                (
                    stats.invalidations,
                    stats.evictions,
                    stats.used_bytes,
                    stats.entries
                ),
                (0, 0, 0, 0)
            );
            assert_eq!(warnings.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn rejected_replacement_counts_one_capacity_eviction() {
        let db = Database::new();
        db.hnsw_cache
            .write()
            .unwrap()
            .insert("a".into(), graph("a"));
        assert!(db.hnsw_cache_stats().used_bytes > 0);
        reject(&db, "a");
        reject(&db, "a");
        let stats = db.hnsw_cache_stats();
        assert_eq!(stats.oversize_skips, 2);
        assert_eq!(stats.evictions, 1);
        assert_eq!(
            (stats.invalidations, stats.used_bytes, stats.entries),
            (0, 0, 0)
        );
    }

    #[test]
    fn retained_graph_rearms_only_its_own_warning() {
        capture_warnings(|warnings| {
            let db = Database::new();
            reject(&db, "a");
            reject(&db, "b");
            let mut cache = db.hnsw_cache.write().unwrap();
            Database::hnsw_cache_insert_bounded(&mut cache, "a".into(), graph("a"), u64::MAX);
            drop(cache);
            reject(&db, "a");
            reject(&db, "b");
            assert_eq!(warnings.load(Ordering::Relaxed), 3);
        });
    }

    #[test]
    fn changed_limits_rearm_but_identical_or_invalid_limits_do_not() {
        capture_warnings(|warnings| {
            let db = Database::new();
            db.set_hnsw_cache_limits(1, Some(2)).unwrap();
            reject(&db, "a");
            db.set_hnsw_cache_limits(1, Some(2)).unwrap();
            reject(&db, "a");
            assert!(db.set_hnsw_cache_limits(2, Some(0)).is_err());
            reject(&db, "a");
            assert_eq!(warnings.load(Ordering::Relaxed), 1);
            db.set_hnsw_cache_limits(2, None).unwrap();
            reject(&db, "a");
            assert_eq!(warnings.load(Ordering::Relaxed), 2);
            db.set_hnsw_cache_limits(2, Some(3)).unwrap();
            reject(&db, "a");
            assert_eq!(warnings.load(Ordering::Relaxed), 3);
        });
    }

    #[test]
    fn committed_invalidation_rearms_even_a_nonretained_name() {
        capture_warnings(|warnings| {
            let db = Database::new();
            reject(&db, "a");
            reject(&db, "b");
            db.commit_with_hnsw_cache_changes(
                || Ok::<_, ()>(((), Some(HashSet::from(["a".into()])))),
                || {},
            )
            .unwrap();
            reject(&db, "a");
            reject(&db, "b");
            assert_eq!(warnings.load(Ordering::Relaxed), 3);
            assert_eq!(db.hnsw_cache_stats().invalidations, 0);
        });
    }

    #[test]
    fn failed_commit_keeps_warning_and_retained_graph() {
        capture_warnings(|warnings| {
            let db = Database::new();
            reject(&db, "a");
            db.hnsw_cache
                .write()
                .unwrap()
                .insert("b".into(), graph("b"));
            let result = db.commit_with_hnsw_cache_changes(
                || Err::<((), Option<HashSet<String>>), _>(()),
                || {},
            );
            assert!(result.is_err());
            reject(&db, "a");
            assert_eq!(warnings.load(Ordering::Relaxed), 1);
            assert_eq!(db.hnsw_cache_stats().entries, 1);
            assert_eq!(db.hnsw_cache_stats().invalidations, 0);
        });
    }

    #[test]
    fn broad_invalidation_rearms_warnings_and_counts_only_retained_entries() {
        capture_warnings(|warnings| {
            let db = Database::new();
            reject(&db, "a");
            db.hnsw_cache
                .write()
                .unwrap()
                .insert("b".into(), graph("b"));
            db.commit_with_hnsw_cache_invalidation(|| Ok::<_, ()>(()))
                .unwrap();
            reject(&db, "a");
            assert_eq!(warnings.load(Ordering::Relaxed), 2);
            let stats = db.hnsw_cache_stats();
            assert_eq!(
                (
                    stats.invalidations,
                    stats.evictions,
                    stats.used_bytes,
                    stats.entries
                ),
                (1, 0, 0, 0)
            );
        });
    }

    #[test]
    fn selective_invalidation_preserves_other_retained_entries() {
        let db = Database::new();
        for name in ["a", "b"] {
            db.hnsw_cache
                .write()
                .unwrap()
                .insert(name.into(), graph(name));
        }
        db.commit_with_hnsw_cache_changes(
            || Ok::<_, ()>(((), Some(HashSet::from(["a".into()])))),
            || {},
        )
        .unwrap();
        let stats = db.hnsw_cache_stats();
        assert_eq!(
            (stats.invalidations, stats.evictions, stats.entries),
            (1, 0, 1)
        );
        assert_eq!(stats.indexes[0].name, "b");
        assert_eq!(stats.used_bytes, stats.indexes[0].bytes);
    }
}
