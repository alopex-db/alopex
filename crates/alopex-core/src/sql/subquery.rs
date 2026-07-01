//! Generic subquery execution primitives.
//!
//! These helpers intentionally model the current naive execution shape: callers
//! provide the concrete subplan executor and this module supplies only the
//! reusable control-flow skeletons.

use std::collections::HashMap;
use std::hash::Hash;

/// Execute a nested subplan scan.
///
/// This is a thin wrapper around the provided executor closure.
pub fn nested_scan<T, E>(execute_subplan: impl FnOnce() -> Result<Vec<T>, E>) -> Result<Vec<T>, E> {
    execute_subplan()
}

/// Scan candidates until a match is found.
///
/// The predicate is fallible so SQL executors can preserve comparison errors
/// while still short-circuiting on the first matching candidate.
pub fn semi_join_probe<T, E>(
    candidates: &[T],
    mut is_match: impl FnMut(&T) -> Result<bool, E>,
) -> Result<bool, E> {
    for candidate in candidates {
        if is_match(candidate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// HashMap-backed materialization cache for subquery results.
///
/// Values are cloned out of the cache so callers can keep ownership semantics
/// equivalent to freshly materialized subquery output.
#[derive(Debug, Clone)]
pub struct MaterializeCache<K, V> {
    values: HashMap<K, V>,
}

impl<K, V> Default for MaterializeCache<K, V> {
    fn default() -> Self {
        Self {
            values: HashMap::new(),
        }
    }
}

impl<K: Hash + Eq, V: Clone> MaterializeCache<K, V> {
    /// Create an empty materialization cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the cached value for `key`, or compute and cache it.
    pub fn get_or_try_insert_with<E>(
        &mut self,
        key: K,
        materialize: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, E> {
        if let Some(value) = self.values.get(&key) {
            return Ok(value.clone());
        }

        let value = materialize()?;
        self.values.insert(key, value.clone());
        Ok(value)
    }
}

/// Create an empty materialization cache.
pub fn materialize_cache<K: Hash + Eq, V: Clone>() -> MaterializeCache<K, V> {
    MaterializeCache::new()
}

#[cfg(test)]
mod tests {
    use super::{materialize_cache, nested_scan, semi_join_probe};

    #[test]
    fn nested_scan_delegates_to_executor() {
        let rows = nested_scan(|| Ok::<_, ()>(vec![1, 2, 3])).unwrap();

        assert_eq!(rows, vec![1, 2, 3]);
    }

    #[test]
    fn semi_join_probe_short_circuits_on_match() {
        let mut visited = Vec::new();
        let matched = semi_join_probe(&[1, 2, 3], |value| {
            visited.push(*value);
            Ok::<_, ()>(*value == 2)
        })
        .unwrap();

        assert!(matched);
        assert_eq!(visited, vec![1, 2]);
    }

    #[test]
    fn materialize_cache_runs_uncached_materializer_once() {
        let mut cache = materialize_cache();
        let mut calls = 0;

        let first = cache
            .get_or_try_insert_with("subquery", || {
                calls += 1;
                Ok::<_, ()>(vec![1, 2])
            })
            .unwrap();
        let second = cache
            .get_or_try_insert_with("subquery", || {
                calls += 1;
                Ok::<_, ()>(vec![3, 4])
            })
            .unwrap();

        assert_eq!(first, vec![1, 2]);
        assert_eq!(second, vec![1, 2]);
        assert_eq!(calls, 1);
    }
}
