//! K-way merging iterator for sorted sources.
//!
//! # Ordering
//!
//! The iterator merges entries sorted by:
//! - `key` ascending (lexicographic),
//! - `timestamp` descending,
//! - `sequence` descending.
//!
//! This ensures that, for a given key, newer versions are observed first.
//!
//! # Duplicates
//!
//! When multiple sources contain identical versions (same key, timestamp, sequence), only one copy
//! is yielded.
//!
//! Note: the dedupe key is `(key, timestamp, sequence)` only. If corrupted inputs contain
//! conflicting values for the same version, the iterator will keep the first one it encounters and
//! drop the rest. Higher layers should validate SSTable integrity if stricter handling is needed.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::lsm::sstable::SSTableEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeapItem {
    entry: SSTableEntry,
    source_id: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap pops the greatest item. Define "greater" as:
        // key asc (smaller key is greater), then timestamp desc (larger ts is greater),
        // then sequence desc (larger seq is greater).
        match self.entry.key.cmp(&other.entry.key) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => match self.entry.timestamp.cmp(&other.entry.timestamp) {
                Ordering::Equal => match self.entry.sequence.cmp(&other.entry.sequence) {
                    Ordering::Equal => other.source_id.cmp(&self.source_id),
                    ord => ord,
                },
                ord => ord,
            },
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A k-way merging iterator over multiple sorted sources.
pub struct MergingIterator {
    sources: Vec<Box<dyn Iterator<Item = SSTableEntry>>>,
    heap: BinaryHeap<HeapItem>,
    last_yielded: Option<(Vec<u8>, u64, u64)>,
}

impl MergingIterator {
    /// Create a new merging iterator from sorted sources.
    ///
    /// Each source must already be sorted by `(key asc, timestamp desc, sequence desc)`.
    pub fn new(sources: Vec<Box<dyn Iterator<Item = SSTableEntry>>>) -> Self {
        let mut iter = Self {
            sources,
            heap: BinaryHeap::new(),
            last_yielded: None,
        };
        iter.prime();
        iter
    }

    fn prime(&mut self) {
        for source_id in 0..self.sources.len() {
            if let Some(entry) = self.sources[source_id].next() {
                self.heap.push(HeapItem { entry, source_id });
            }
        }
    }
}

impl Iterator for MergingIterator {
    type Item = SSTableEntry;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.heap.pop() {
            // Advance the source that produced this item.
            if let Some(next) = self.sources[item.source_id].next() {
                self.heap.push(HeapItem {
                    entry: next,
                    source_id: item.source_id,
                });
            }

            let key = item.entry.key.clone();
            let ts = item.entry.timestamp;
            let seq = item.entry.sequence;
            let ident = (key, ts, seq);
            if self.last_yielded.as_ref() == Some(&ident) {
                continue;
            }
            self.last_yielded = Some(ident);
            return Some(item.entry);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &[u8], value: &[u8], ts: u64, seq: u64) -> SSTableEntry {
        SSTableEntry {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            timestamp: ts,
            sequence: seq,
        }
    }

    fn del(key: &[u8], ts: u64, seq: u64) -> SSTableEntry {
        SSTableEntry {
            key: key.to_vec(),
            value: None,
            timestamp: ts,
            sequence: seq,
        }
    }

    #[test]
    fn merges_in_key_order_and_prioritizes_newer_versions() {
        // Source 0: a@20, a@10, b@5
        let s0 = vec![
            put(b"a", b"v2", 20, 1),
            put(b"a", b"v1", 10, 1),
            put(b"b", b"x", 5, 1),
        ];
        // Source 1: a@15 tombstone, c@7
        let s1 = vec![del(b"a", 15, 1), put(b"c", b"z", 7, 1)];

        let merged: Vec<_> =
            MergingIterator::new(vec![Box::new(s0.into_iter()), Box::new(s1.into_iter())])
                .collect();

        // Key order: a..., b..., c...
        assert_eq!(merged[0].key, b"a".to_vec());
        assert_eq!(merged[1].key, b"a".to_vec());
        assert_eq!(merged[2].key, b"a".to_vec());
        assert_eq!(merged[3].key, b"b".to_vec());
        assert_eq!(merged[4].key, b"c".to_vec());

        // Within key "a": newest timestamp first.
        assert_eq!(merged[0].timestamp, 20);
        assert_eq!(merged[1].timestamp, 15);
        assert_eq!(merged[2].timestamp, 10);
    }

    #[test]
    fn dedupes_identical_versions_across_sources() {
        let e = put(b"a", b"v", 10, 1);
        let merged: Vec<_> = MergingIterator::new(vec![
            Box::new(vec![e.clone()].into_iter()),
            Box::new(vec![e].into_iter()),
        ])
        .collect();
        assert_eq!(merged.len(), 1);
    }
}
