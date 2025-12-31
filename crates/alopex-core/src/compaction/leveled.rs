//! Leveled compaction planning and a reference in-memory compaction routine.
//!
//! This is an algorithmic implementation that is usable without the full single-file
//! in-place compaction plumbing. It selects compaction candidates based on the design rules
//! in spec §3.4.2/§3.4.3 and can merge SSTable entry streams into new SSTable runs.

use crate::compaction::merge_iter::MergingIterator;
use crate::error::{Error, Result};
use crate::lsm::sstable::SSTableEntry;
use crate::types::Key;

/// Leveled compaction configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeveledCompactionConfig {
    /// L0 → L1 trigger count (default: 4).
    pub l0_compaction_trigger: usize,
    /// Size multiplier for each level (default: 10).
    pub level_size_multiplier: usize,
    /// Maximum number of levels (default: 7).
    pub max_levels: usize,
    /// Target SSTable file size (bytes, default: 64MB).
    pub target_file_size: usize,
}

impl Default for LeveledCompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            level_size_multiplier: 10,
            max_levels: 7,
            target_file_size: 64 * 1024 * 1024,
        }
    }
}

/// Inclusive key range for an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    /// Smallest key in the table.
    pub first_key: Key,
    /// Largest key in the table.
    pub last_key: Key,
}

impl KeyRange {
    /// Returns true if the ranges overlap.
    pub fn overlaps(&self, other: &KeyRange) -> bool {
        !(self.last_key < other.first_key || other.last_key < self.first_key)
    }
}

/// Metadata for an SSTable used by the compaction planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableMeta {
    /// Stable identifier (logical).
    pub id: u64,
    /// Level number (0..max_levels-1).
    pub level: usize,
    /// Approximate size in bytes.
    pub size_bytes: u64,
    /// Key range covered by this table.
    pub key_range: KeyRange,
}

/// A compaction plan selecting input tables and output level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    /// Input level.
    pub input_level: usize,
    /// Output level (= input_level + 1).
    pub output_level: usize,
    /// SSTable IDs selected from input_level.
    pub input_ids: Vec<u64>,
    /// SSTable IDs selected from output_level that overlap input key-range.
    pub overlapping_output_ids: Vec<u64>,
}

/// Leveled compaction planner + reference compaction routine.
#[derive(Debug, Clone)]
pub struct LeveledCompaction {
    config: LeveledCompactionConfig,
}

impl LeveledCompaction {
    /// Create a new compaction planner.
    pub fn new(config: LeveledCompactionConfig) -> Result<Self> {
        if config.max_levels < 2 {
            return Err(Error::InvalidFormat("max_levels must be >= 2".into()));
        }
        if config.l0_compaction_trigger == 0 {
            return Err(Error::InvalidFormat(
                "l0_compaction_trigger must be >= 1".into(),
            ));
        }
        if config.level_size_multiplier < 2 {
            return Err(Error::InvalidFormat(
                "level_size_multiplier must be >= 2".into(),
            ));
        }
        if config.target_file_size == 0 {
            return Err(Error::InvalidFormat("target_file_size must be > 0".into()));
        }
        Ok(Self { config })
    }

    /// Pick a compaction plan from per-level SSTable metadata.
    ///
    /// The planner chooses:
    /// - L0 → L1 when `L0.len() >= l0_compaction_trigger`.
    /// - Otherwise, the first level `n` where `size(Ln) > target_file_size * multiplier^n`.
    pub fn pick_plan(&self, levels: &[Vec<SSTableMeta>]) -> Option<CompactionPlan> {
        if levels.len() < 2 {
            return None;
        }

        // L0 trigger.
        let l0 = &levels[0];
        if l0.len() >= self.config.l0_compaction_trigger {
            let input_ids: Vec<u64> = l0.iter().map(|m| m.id).collect();
            let input_range = merge_key_ranges(l0.iter().map(|m| &m.key_range));
            let overlaps = levels[1]
                .iter()
                .filter(|m| m.key_range.overlaps(&input_range))
                .map(|m| m.id)
                .collect();
            return Some(CompactionPlan {
                input_level: 0,
                output_level: 1,
                input_ids,
                overlapping_output_ids: overlaps,
            });
        }

        // Size trigger for levels 1..max_levels-2.
        let max = self.config.max_levels.min(levels.len());
        for level in 1..max.saturating_sub(1) {
            let total: u64 = levels[level].iter().map(|m| m.size_bytes).sum();
            let limit = self.level_size_limit(level);
            if total > limit {
                // TODO: choose a better candidate selection strategy (oldest run, smallest overlap,
                // largest size, etc.). For now we select the first table as a deterministic
                // placeholder.
                let input_meta = levels[level].first()?;
                let input_ids = vec![input_meta.id];
                let overlaps = levels[level + 1]
                    .iter()
                    .filter(|m| m.key_range.overlaps(&input_meta.key_range))
                    .map(|m| m.id)
                    .collect();
                return Some(CompactionPlan {
                    input_level: level,
                    output_level: level + 1,
                    input_ids,
                    overlapping_output_ids: overlaps,
                });
            }
        }

        None
    }

    fn level_size_limit(&self, level: usize) -> u64 {
        // Interpret "base_size" as target_file_size for level 1, then grow by multiplier^level.
        let mult = self.config.level_size_multiplier as u64;
        let base = self.config.target_file_size as u64;
        let pow = mult.saturating_pow(level as u32);
        base.saturating_mul(pow)
    }

    /// Compact multiple sorted entry streams and return output runs split by `target_file_size`.
    ///
    /// If `output_level` is the last level (`max_levels-1`), tombstones and older versions for that
    /// key are removed completely (spec §3.4.3).
    pub fn compact_entries(
        &self,
        sources: Vec<Box<dyn Iterator<Item = SSTableEntry>>>,
        output_level: usize,
    ) -> Result<Vec<Vec<SSTableEntry>>> {
        if output_level >= self.config.max_levels {
            return Err(Error::InvalidFormat("output_level out of range".into()));
        }

        let mut out_files: Vec<Vec<SSTableEntry>> = Vec::new();
        let mut current: Vec<SSTableEntry> = Vec::new();
        let mut current_bytes: usize = 0;

        let mut iter = MergingIterator::new(sources).peekable();
        let drop_tombstones = output_level + 1 >= self.config.max_levels;

        while let Some(entry) = iter.next() {
            if !drop_tombstones {
                push_splitting(
                    &mut out_files,
                    &mut current,
                    &mut current_bytes,
                    entry,
                    self.config.target_file_size,
                );
                continue;
            }

            // Bottom-level: if the newest visible version is a tombstone, drop the entire key.
            let key = entry.key.clone();
            if entry.value.is_none() {
                // Skip all remaining versions for this key.
                while let Some(next) = iter.peek() {
                    if next.key == key {
                        let _ = iter.next();
                    } else {
                        break;
                    }
                }
                continue;
            }

            push_splitting(
                &mut out_files,
                &mut current,
                &mut current_bytes,
                entry,
                self.config.target_file_size,
            );
        }

        if !current.is_empty() {
            out_files.push(current);
        }
        Ok(out_files)
    }
}

fn merge_key_ranges<'a, I>(ranges: I) -> KeyRange
where
    I: IntoIterator<Item = &'a KeyRange>,
{
    let mut it = ranges.into_iter();
    let first = it.next().expect("non-empty ranges");
    let mut min = first.first_key.clone();
    let mut max = first.last_key.clone();
    for r in it {
        if r.first_key < min {
            min = r.first_key.clone();
        }
        if r.last_key > max {
            max = r.last_key.clone();
        }
    }
    KeyRange {
        first_key: min,
        last_key: max,
    }
}

fn estimate_entry_size(entry: &SSTableEntry) -> usize {
    // Rough estimate for splitting; not the actual on-disk size.
    let val_len = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
    1 + 8 + 8 + 4 + 4 + entry.key.len() + val_len
}

fn push_splitting(
    out_files: &mut Vec<Vec<SSTableEntry>>,
    current: &mut Vec<SSTableEntry>,
    current_bytes: &mut usize,
    entry: SSTableEntry,
    target_bytes: usize,
) {
    let size = estimate_entry_size(&entry);
    if !current.is_empty() && current_bytes.saturating_add(size) > target_bytes {
        out_files.push(std::mem::take(current));
        *current_bytes = 0;
    }
    *current_bytes = current_bytes.saturating_add(size);
    current.push(entry);
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    fn meta(id: u64, level: usize, size: u64, first: &[u8], last: &[u8]) -> SSTableMeta {
        SSTableMeta {
            id,
            level,
            size_bytes: size,
            key_range: KeyRange {
                first_key: first.to_vec(),
                last_key: last.to_vec(),
            },
        }
    }

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
    fn picks_l0_trigger() {
        let comp = LeveledCompaction::new(LeveledCompactionConfig {
            l0_compaction_trigger: 2,
            ..Default::default()
        })
        .unwrap();
        let levels = vec![
            vec![meta(1, 0, 10, b"a", b"b"), meta(2, 0, 10, b"c", b"d")],
            vec![meta(10, 1, 10, b"b", b"c"), meta(11, 1, 10, b"x", b"z")],
        ];
        let plan = comp.pick_plan(&levels).unwrap();
        assert_eq!(plan.input_level, 0);
        assert_eq!(plan.output_level, 1);
        assert_eq!(plan.input_ids, vec![1, 2]);
        assert_eq!(plan.overlapping_output_ids, vec![10]);
    }

    #[test]
    fn picks_size_trigger_for_l1() {
        let comp = LeveledCompaction::new(LeveledCompactionConfig {
            l0_compaction_trigger: 100,
            target_file_size: 10,
            level_size_multiplier: 2,
            max_levels: 4,
        })
        .unwrap();
        let levels = vec![
            vec![],
            vec![meta(5, 1, 100, b"a", b"z")],
            vec![meta(6, 2, 10, b"m", b"n")],
            vec![],
        ];
        let plan = comp.pick_plan(&levels).unwrap();
        assert_eq!(plan.input_level, 1);
        assert_eq!(plan.output_level, 2);
        assert_eq!(plan.input_ids, vec![5]);
        assert_eq!(plan.overlapping_output_ids, vec![6]);
    }

    #[test]
    fn bottom_level_drops_tombstone_and_older_versions() {
        let comp = LeveledCompaction::new(LeveledCompactionConfig {
            max_levels: 3,
            target_file_size: 1024,
            ..Default::default()
        })
        .unwrap();
        let sources: Vec<Box<dyn Iterator<Item = SSTableEntry>>> = vec![
            Box::new(
                vec![
                    del(b"k", 20, 1),
                    put(b"k", b"old", 10, 1),
                    put(b"x", b"v", 1, 1),
                ]
                .into_iter(),
            ),
            Box::new(vec![put(b"a", b"1", 5, 1)].into_iter()),
        ];
        // output_level=2 is bottom for max_levels=3.
        let out = comp.compact_entries(sources, 2).unwrap();
        let flat: Vec<_> = out.into_iter().flatten().collect();
        assert!(flat.iter().all(|e| e.key != b"k".to_vec()));
        assert!(flat.iter().any(|e| e.key == b"x".to_vec()));
    }
}
