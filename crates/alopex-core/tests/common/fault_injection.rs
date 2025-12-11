#![cfg(feature = "test-hooks")]

use rand::Rng;
use std::io::{Error, ErrorKind, Read, Result as IoResult, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Fault injection hooks used by file wrappers and IoHooks adapters.
pub trait FaultInjector: Send + Sync {
    /// Invoked before writing data.
    fn before_write(&self, _data: &[u8]) -> IoResult<()> {
        Ok(())
    }

    /// Invoked after reading data (can mutate buffer).
    fn after_read(&self, _data: &mut [u8]) -> IoResult<()> {
        Ok(())
    }

    /// Invoked before fsync.
    fn before_fsync(&self) -> IoResult<()> {
        Ok(())
    }
}

/// File wrapper that delegates to a fault injector.
pub struct FaultInjectingFile<F: Read + Write> {
    inner: F,
    injector: Arc<dyn FaultInjector>,
}

impl<F: Read + Write> FaultInjectingFile<F> {
    pub fn new(inner: F, injector: Arc<dyn FaultInjector>) -> Self {
        Self { inner, injector }
    }

    pub fn into_inner(self) -> F {
        self.inner
    }
}

impl<F: Read + Write> Write for FaultInjectingFile<F> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.injector.before_write(buf)?;
        self.inner.write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.injector.before_fsync()?;
        self.inner.flush()
    }
}

impl<F: Read + Write> Read for FaultInjectingFile<F> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.injector.after_read(&mut buf[..n])?;
        }
        Ok(n)
    }
}

/// I/O error injector with probabilistic error insertion.
pub struct IoErrorInjector {
    write_error_rate: f64,
    read_error_rate: f64,
    fsync_error_rate: f64,
    error_kind: ErrorKind,
    enabled: AtomicBool,
    injection_count: AtomicUsize,
}

impl IoErrorInjector {
    pub fn new() -> Self {
        Self {
            write_error_rate: 0.0,
            read_error_rate: 0.0,
            fsync_error_rate: 0.0,
            error_kind: ErrorKind::Other,
            enabled: AtomicBool::new(true),
            injection_count: AtomicUsize::new(0),
        }
    }

    pub fn with_write_error_rate(mut self, rate: f64) -> Self {
        self.write_error_rate = rate;
        self
    }

    pub fn with_read_error_rate(mut self, rate: f64) -> Self {
        self.read_error_rate = rate;
        self
    }

    pub fn with_fsync_error_rate(mut self, rate: f64) -> Self {
        self.fsync_error_rate = rate;
        self
    }

    pub fn with_error_kind(mut self, kind: ErrorKind) -> Self {
        self.error_kind = kind;
        self
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    pub fn injection_count(&self) -> usize {
        self.injection_count.load(Ordering::Relaxed)
    }

    fn should_inject(&self, rate: f64) -> bool {
        self.enabled.load(Ordering::Relaxed) && rate > 0.0 && rand::thread_rng().gen::<f64>() < rate
    }

    fn inject(&self) -> IoResult<()> {
        self.injection_count.fetch_add(1, Ordering::Relaxed);
        Err(Error::new(self.error_kind, "injected io error"))
    }
}

impl FaultInjector for IoErrorInjector {
    fn before_write(&self, _data: &[u8]) -> IoResult<()> {
        if self.should_inject(self.write_error_rate) {
            return self.inject();
        }
        Ok(())
    }

    fn after_read(&self, _data: &mut [u8]) -> IoResult<()> {
        if self.should_inject(self.read_error_rate) {
            return self.inject();
        }
        Ok(())
    }

    fn before_fsync(&self) -> IoResult<()> {
        if self.should_inject(self.fsync_error_rate) {
            return self.inject();
        }
        Ok(())
    }
}

/// Corruption modes applied to buffers.
#[derive(Clone, Copy)]
pub enum CorruptionMode {
    SingleBitFlip,
    RandomByte,
    PartialZero,
    Truncate,
}

/// Data corruption injector that mutates buffers after reads.
pub struct CorruptionInjector {
    corruption_rate: f64,
    mode: CorruptionMode,
    enabled: AtomicBool,
}

impl CorruptionInjector {
    pub fn new() -> Self {
        Self {
            corruption_rate: 0.0,
            mode: CorruptionMode::SingleBitFlip,
            enabled: AtomicBool::new(true),
        }
    }

    pub fn with_corruption_rate(mut self, rate: f64) -> Self {
        self.corruption_rate = rate;
        self
    }

    pub fn with_mode(mut self, mode: CorruptionMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    fn should_corrupt(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
            && self.corruption_rate > 0.0
            && rand::thread_rng().gen::<f64>() < self.corruption_rate
    }

    fn corrupt(&self, data: &mut [u8]) {
        if data.is_empty() {
            return;
        }
        match self.mode {
            CorruptionMode::SingleBitFlip => {
                let idx = rand::thread_rng().gen_range(0..data.len());
                let bit = 1u8 << (rand::thread_rng().gen_range(0..8));
                data[idx] ^= bit;
            }
            CorruptionMode::RandomByte => {
                let idx = rand::thread_rng().gen_range(0..data.len());
                data[idx] = rand::thread_rng().gen();
            }
            CorruptionMode::PartialZero => {
                let start = data.len() / 2;
                for b in &mut data[start..] {
                    *b = 0;
                }
            }
            CorruptionMode::Truncate => {
                let start = data.len() / 3;
                for b in &mut data[start..] {
                    *b = 0;
                }
            }
        }
    }
}

impl FaultInjector for CorruptionInjector {
    fn after_read(&self, data: &mut [u8]) -> IoResult<()> {
        if self.should_corrupt() {
            self.corrupt(data);
        }
        Ok(())
    }
}

// Re-export CrashSimulator types from alopex_core for test convenience.
pub use alopex_core::{CrashOperation, CrashPoint, CrashSimulator, CrashTiming};
