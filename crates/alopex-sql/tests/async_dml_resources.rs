#![cfg(feature = "tokio")]
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
struct Meter;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);
static ACTIVE: AtomicBool = AtomicBool::new(false);

fn allocated(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    if ACTIVE.load(Ordering::Relaxed) {
        REQUESTED.fetch_add(bytes, Ordering::Relaxed);
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() {
            allocated(layout.size());
        }
        result
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            allocated(size);
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: Meter = Meter;
static BASELINE: AtomicUsize = AtomicUsize::new(0);
mod measurement {
    use super::*;
    pub fn reset() {
        let live = LIVE.load(Ordering::Relaxed);
        BASELINE.store(live, Ordering::Relaxed);
        PEAK.store(live, Ordering::Relaxed);
        REQUESTED.store(0, Ordering::Relaxed);
        ACTIVE.store(true, Ordering::Relaxed);
    }
    pub fn finish() -> (Option<usize>, Option<usize>) {
        ACTIVE.store(false, Ordering::Relaxed);
        (
            Some(
                PEAK.load(Ordering::Relaxed)
                    .saturating_sub(BASELINE.load(Ordering::Relaxed)),
            ),
            Some(REQUESTED.load(Ordering::Relaxed)),
        )
    }
}

#[path = "support/committed_dml_cost.rs"]
mod committed_cost;
