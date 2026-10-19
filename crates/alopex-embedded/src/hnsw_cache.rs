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
                "HNSW graph exceeds cache budget; queries remain available without retention"
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
        let removed = self.remove_untracked(name);
        if removed.is_some() {
            self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }
    pub(crate) fn clear(&mut self) {
        self.counters
            .invalidations
            .fetch_add(self.indices.len() as u64, Ordering::Relaxed);
        self.indices.clear();
        self.metadata.clear();
        self.used_bytes = 0;
    }
    pub(crate) fn retain(&mut self, mut retain: impl FnMut(&String, &Arc<HnswIndex>) -> bool) {
        let removed: Vec<_> = self
            .indices
            .iter()
            .filter(|(name, index)| !retain(name, index))
            .map(|(name, _)| name.clone())
            .collect();
        for name in removed {
            self.remove(&name);
        }
    }
    pub(crate) fn trim(&mut self, bytes: u64, preserve: Option<&str>) {
        while self.used_bytes > bytes || self.indices.len() > self.max_entries {
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
    /// Globally retained entries removed by LRU limits.
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
