//! Free space management for in-place compaction.
//!
//! This is a simple free-list allocator intended for managing reclaimed SSTable section ranges
//! inside a single `.alopex` file (spec §10.5.1).
//!
//! The manager stores free ranges as `(offset, size)` pairs, coalesces adjacent blocks, and
//! supports first-fit allocation.

use crate::error::{Error, Result};

/// Free space manager with coalescing.
#[derive(Debug, Default, Clone)]
pub struct FreeSpaceManager {
    free_list: Vec<(u64, u64)>,
}

impl FreeSpaceManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self { free_list: vec![] }
    }

    /// Return the current free list (sorted, coalesced).
    pub fn free_list(&self) -> &[(u64, u64)] {
        &self.free_list
    }

    /// Total free bytes (sum of sizes).
    pub fn total_free_bytes(&self) -> u64 {
        self.free_list.iter().map(|(_, s)| *s).sum()
    }

    /// Add a free region and coalesce adjacent regions.
    pub fn deallocate(&mut self, offset: u64, size: u64) -> Result<()> {
        if size == 0 {
            return Ok(());
        }
        let end = offset
            .checked_add(size)
            .ok_or_else(|| Error::InvalidFormat("free range overflow".into()))?;

        // Insert and keep sorted.
        self.free_list.push((offset, size));
        self.free_list.sort_by_key(|(o, _)| *o);

        // Coalesce.
        let mut out: Vec<(u64, u64)> = Vec::with_capacity(self.free_list.len());
        for (o, s) in self.free_list.drain(..) {
            let e = o
                .checked_add(s)
                .ok_or_else(|| Error::InvalidFormat("free range overflow".into()))?;
            if let Some((last_o, last_s)) = out.last_mut() {
                let last_e = last_o
                    .checked_add(*last_s)
                    .ok_or_else(|| Error::InvalidFormat("free range overflow".into()))?;
                if o <= last_e {
                    // Overlap or adjacency -> merge.
                    let new_e = last_e.max(e);
                    *last_s = new_e - *last_o;
                    continue;
                }
            }
            out.push((o, s));
        }
        self.free_list = out;

        // Sanity: new range end must be representable.
        let _ = end;
        Ok(())
    }

    /// Allocate a region of at least `size` bytes using first-fit.
    ///
    /// Returns the offset of the allocated region, or `None` if no free region is large enough.
    pub fn allocate(&mut self, size: u64) -> Option<u64> {
        if size == 0 {
            // Reject zero-sized allocations to avoid callers treating `0` as a valid offset.
            return None;
        }
        for i in 0..self.free_list.len() {
            let (off, sz) = self.free_list[i];
            if sz >= size {
                let alloc_off = off;
                let remaining = sz - size;
                if remaining == 0 {
                    self.free_list.remove(i);
                } else {
                    self.free_list[i] = (off + size, remaining);
                }
                return Some(alloc_off);
            }
        }
        None
    }

    /// Coalesce and sort the free list (useful after bulk updates).
    pub fn defragment(&mut self) -> Result<()> {
        let mut out = FreeSpaceManager::new();
        for (o, s) in std::mem::take(&mut self.free_list) {
            out.deallocate(o, s)?;
        }
        self.free_list = out.free_list;
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn allocate_and_deallocate_coalesces() {
        let mut f = FreeSpaceManager::new();
        f.deallocate(100, 50).unwrap();
        f.deallocate(150, 50).unwrap(); // adjacent
        assert_eq!(f.free_list(), &[(100, 100)]);

        let off = f.allocate(30).unwrap();
        assert_eq!(off, 100);
        assert_eq!(f.free_list(), &[(130, 70)]);

        f.deallocate(0, 10).unwrap();
        assert_eq!(f.free_list(), &[(0, 10), (130, 70)]);
        f.deallocate(10, 120).unwrap(); // bridges
        assert_eq!(f.free_list(), &[(0, 200)]);
    }

    #[test]
    fn allocate_returns_none_when_insufficient() {
        let mut f = FreeSpaceManager::new();
        f.deallocate(0, 10).unwrap();
        assert_eq!(f.allocate(11), None);
    }

    #[test]
    fn allocate_rejects_zero_size() {
        let mut f = FreeSpaceManager::new();
        f.deallocate(100, 10).unwrap();
        assert_eq!(f.allocate(0), None);
        assert_eq!(f.free_list(), &[(100, 10)]);
    }
}
