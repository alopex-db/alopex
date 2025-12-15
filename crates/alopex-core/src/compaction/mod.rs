//! Compaction-related utilities (merging iterator, leveled compaction planning).
//!
//! This module is primarily used by the LSM-tree engine, but is kept separate so it can be
//! unit-tested without the full `LsmKV` integration.

pub mod leveled;
pub mod merge_iter;
