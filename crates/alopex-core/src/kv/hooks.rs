//! Test-only hooks for I/O fault injection and crash simulation.

use std::io::Result as IoResult;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Hooks invoked around disk I/O for stress tests.
pub trait IoHooks: Send + Sync {
    /// Invoked before each WAL record is written.
    fn before_wal_write(&self, _data: &[u8]) -> IoResult<()> {
        Ok(())
    }

    /// Invoked after each WAL record is written.
    fn after_wal_write(&self, _data: &[u8]) -> IoResult<()> {
        Ok(())
    }

    /// Invoked before fsync.
    fn before_fsync(&self) -> IoResult<()> {
        Ok(())
    }

    /// Invoked after fsync.
    fn after_fsync(&self) -> IoResult<()> {
        Ok(())
    }

    /// Invoked before appending SST data.
    fn before_sst_write(&self, _data: &[u8]) -> IoResult<()> {
        Ok(())
    }

    /// Invoked at the start of compaction.
    fn on_compaction_start(&self) {}

    /// Invoked after compaction completes.
    fn on_compaction_end(&self) {}
}

/// Crash trigger location.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashOperation {
    /// WAL record write.
    WalWrite,
    /// WAL fsync.
    WalFsync,
    /// SST record write.
    SstWrite,
    /// SST footer/finalization.
    SstFinalize,
    /// In-memory or SST compaction.
    Compaction,
}

/// Crash timing relative to an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashTiming {
    /// Before the operation begins.
    Before,
    /// During the operation.
    During,
    /// After the operation finishes.
    After,
}

/// Crash trigger definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CrashPoint {
    /// Operation to target.
    pub operation: CrashOperation,
    /// Timing to trigger.
    pub timing: CrashTiming,
}

/// Simple crash simulator to panic at configured points.
pub struct CrashSimulator {
    crash_points: Vec<CrashPoint>,
    current_point: AtomicUsize,
    enabled: AtomicBool,
}

impl CrashSimulator {
    /// Create a simulator with no crash points.
    pub fn new() -> Self {
        Self {
            crash_points: Vec::new(),
            current_point: AtomicUsize::new(0),
            enabled: AtomicBool::new(true),
        }
    }

    /// Add a single crash point.
    pub fn add_crash_point(mut self, point: CrashPoint) -> Self {
        self.crash_points.push(point);
        self
    }

    /// Add multiple crash points.
    pub fn with_crash_points(mut self, points: Vec<CrashPoint>) -> Self {
        self.crash_points.extend(points);
        self
    }

    /// Trigger a crash if the next configured point matches.
    pub fn check_crash(&self, operation: CrashOperation, timing: CrashTiming) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let idx = self.current_point.load(Ordering::Relaxed);
        if let Some(point) = self.crash_points.get(idx) {
            if point.operation == operation && point.timing == timing {
                self.current_point.fetch_add(1, Ordering::Relaxed);
                panic!(
                    "CrashSimulator: simulated crash at {:?}/{:?}",
                    operation, timing
                );
            }
        }
    }

    /// Disable crash triggering.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    /// Enable crash triggering.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// Remaining crash points that have not fired.
    pub fn remaining_crash_points(&self) -> usize {
        let idx = self.current_point.load(Ordering::Relaxed);
        self.crash_points.len().saturating_sub(idx)
    }
}

impl Default for CrashSimulator {
    fn default() -> Self {
        Self::new()
    }
}
