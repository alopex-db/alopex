//! Resource accounting and terminal-state contracts for bounded execution.
//!
//! A reservation must be acquired before this execution layer takes ownership
//! of an allocation.  The reservation is RAII-backed so every successful
//! reservation is released exactly once, including on error paths.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use crate::{DataFrameError, Result};

/// Options shared by bounded materialization and streaming execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamOptions {
    /// Maximum bytes owned by the execution pipeline at one time.
    pub memory_limit_bytes: u64,
    /// Maximum number of pipeline-owned batches at one time.
    pub max_in_flight_batches: NonZeroUsize,
    /// Requested source row cap. It never relaxes `memory_limit_bytes`.
    pub batch_rows: NonZeroUsize,
}

impl StreamOptions {
    /// Construct validated bounded-execution options.
    pub fn new(
        memory_limit_bytes: u64,
        max_in_flight_batches: NonZeroUsize,
        batch_rows: NonZeroUsize,
    ) -> Self {
        Self {
            memory_limit_bytes,
            max_in_flight_batches,
            batch_rows,
        }
    }
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 64 * 1024 * 1024,
            max_in_flight_batches: NonZeroUsize::MIN,
            batch_rows: NonZeroUsize::new(8_192).expect("8,192 is non-zero"),
        }
    }
}

/// The allocation domain that owns a reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceScope {
    /// Raw input bytes held by a source reader.
    Source,
    /// Decode buffers and Arrow arrays constructed by a source reader.
    Decode,
    /// A physical operator's intermediate state.
    Operator,
    /// Expression evaluation and memoization state.
    Expression,
    /// A batch waiting to be transferred to the consumer.
    Output,
    /// Batches retained while constructing a bounded materialized result.
    MaterializedOutput,
    /// Temporary spill state, if a bounded executor supports it.
    Spill,
}

impl ResourceScope {
    /// Stable scope name for diagnostics and telemetry.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Decode => "decode",
            Self::Operator => "operator",
            Self::Expression => "expression",
            Self::Output => "output",
            Self::MaterializedOutput => "materialized_output",
            Self::Spill => "spill",
        }
    }
}

/// Snapshot of currently pipeline-owned resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceUsage {
    /// Bytes currently reserved by the execution pipeline.
    pub reserved_bytes: u64,
    /// Batches currently reserved by the execution pipeline.
    pub reserved_batches: usize,
}

#[derive(Debug)]
struct BudgetState {
    usage: ResourceUsage,
}

/// Shared, allocation-before-ownership resource budget.
#[derive(Debug, Clone)]
pub struct ResourceBudget {
    memory_limit_bytes: u64,
    max_in_flight_batches: usize,
    state: Arc<Mutex<BudgetState>>,
}

impl ResourceBudget {
    /// Create a budget from public bounded-execution options.
    pub fn from_options(options: StreamOptions) -> Self {
        Self::new(options.memory_limit_bytes, options.max_in_flight_batches)
    }

    /// Create a budget with a byte limit and a positive in-flight batch limit.
    pub fn new(memory_limit_bytes: u64, max_in_flight_batches: NonZeroUsize) -> Self {
        Self {
            memory_limit_bytes,
            max_in_flight_batches: max_in_flight_batches.get(),
            state: Arc::new(Mutex::new(BudgetState {
                usage: ResourceUsage {
                    reserved_bytes: 0,
                    reserved_batches: 0,
                },
            })),
        }
    }

    /// Reserve byte ownership before allocating or retaining data.
    pub fn reserve(&self, scope: ResourceScope, bytes: u64) -> Result<ResourceReservation> {
        self.reserve_inner(scope, bytes, 0)
    }

    /// Reserve ownership for one decoded or retained batch before publishing it.
    pub fn reserve_batch(&self, scope: ResourceScope, bytes: u64) -> Result<ResourceReservation> {
        self.reserve_inner(scope, bytes, 1)
    }

    /// Return the configured byte limit.
    pub const fn memory_limit_bytes(&self) -> u64 {
        self.memory_limit_bytes
    }

    /// Return the configured pipeline-owned batch limit.
    pub const fn max_in_flight_batches(&self) -> usize {
        self.max_in_flight_batches
    }

    /// Obtain an accounting snapshot for tests, diagnostics, and stream status.
    pub fn usage(&self) -> ResourceUsage {
        self.state
            .lock()
            .expect("resource budget mutex poisoned")
            .usage
    }

    fn reserve_inner(
        &self,
        scope: ResourceScope,
        bytes: u64,
        batches: usize,
    ) -> Result<ResourceReservation> {
        let mut state = self.state.lock().expect("resource budget mutex poisoned");
        let observed_bytes = state.usage.reserved_bytes.saturating_add(bytes);
        let observed_batches = state.usage.reserved_batches.saturating_add(batches);

        if observed_bytes > self.memory_limit_bytes || observed_batches > self.max_in_flight_batches
        {
            return Err(DataFrameError::resource_limit_exceeded(
                self.memory_limit_bytes,
                observed_bytes,
                self.max_in_flight_batches,
                observed_batches,
                scope,
            ));
        }

        state.usage = ResourceUsage {
            reserved_bytes: observed_bytes,
            reserved_batches: observed_batches,
        };

        Ok(ResourceReservation {
            budget: self.clone(),
            bytes,
            batches,
            released: false,
        })
    }

    fn release(&self, bytes: u64, batches: usize) {
        let mut state = self.state.lock().expect("resource budget mutex poisoned");
        state.usage.reserved_bytes = state.usage.reserved_bytes.saturating_sub(bytes);
        state.usage.reserved_batches = state.usage.reserved_batches.saturating_sub(batches);
    }
}

/// A single ownership reservation released exactly once on `Drop` or `release`.
#[derive(Debug)]
pub struct ResourceReservation {
    budget: ResourceBudget,
    bytes: u64,
    batches: usize,
    released: bool,
}

impl ResourceReservation {
    /// Convert a byte-only reservation into the single batch ownership slot for a published
    /// result. The caller must first release the source batch slot after the source batch is no
    /// longer needed, so a `max_in_flight_batches` limit of one remains usable for transforms.
    pub fn promote_to_batch(&mut self, scope: ResourceScope) -> Result<()> {
        if self.released || self.batches != 0 {
            return Ok(());
        }

        let mut state = self
            .budget
            .state
            .lock()
            .expect("resource budget mutex poisoned");
        let observed_batches = state.usage.reserved_batches.saturating_add(1);
        if observed_batches > self.budget.max_in_flight_batches {
            return Err(DataFrameError::resource_limit_exceeded(
                self.budget.memory_limit_bytes,
                state.usage.reserved_bytes,
                self.budget.max_in_flight_batches,
                observed_batches,
                scope,
            ));
        }
        state.usage.reserved_batches = observed_batches;
        self.batches = 1;
        Ok(())
    }

    /// Release this reservation now. Repeated calls are no-ops.
    pub fn release(&mut self) {
        if !self.released {
            self.budget.release(self.bytes, self.batches);
            self.released = true;
        }
    }

    /// Retain only `bytes` of this reservation, returning any excess immediately.
    ///
    /// This is used for bounded metadata probing: reserve before an opaque parser allocates,
    /// then retain its measured metadata footprint rather than pessimistically pinning the full
    /// execution budget for the lifetime of a source.
    pub fn shrink_to(&mut self, bytes: u64) {
        let bytes = bytes.min(self.bytes);
        if !self.released && bytes < self.bytes {
            self.budget.release(self.bytes - bytes, 0);
            self.bytes = bytes;
        }
    }

    /// Return the bytes covered by this reservation.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for ResourceReservation {
    fn drop(&mut self) {
        self.release();
    }
}

/// Stable classes for stream terminal failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFailureClass {
    /// Plan or source has no streaming implementation.
    Unsupported,
    /// A configured resource bound was exceeded.
    ResourceLimit,
    /// The source could not be read.
    Source,
    /// Source content could not be decoded.
    Decode,
    /// The source or result schema is invalid.
    Schema,
    /// The caller cancelled execution.
    Cancelled,
    /// The caller closed execution before normal completion.
    Closed,
    /// An internal execution failure occurred.
    Internal,
}

impl StreamFailureClass {
    /// Stable diagnostic classification string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::ResourceLimit => "resource_limit",
            Self::Source => "source",
            Self::Decode => "decode",
            Self::Schema => "schema",
            Self::Cancelled => "cancelled",
            Self::Closed => "closed",
            Self::Internal => "internal",
        }
    }
}

/// Repeatable, structured terminal failure metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFailure {
    /// Stable diagnostic code; callers must not parse error display text.
    pub code: &'static str,
    /// Broad failure category.
    pub classification: StreamFailureClass,
}

impl StreamFailure {
    /// Construct structured terminal failure metadata.
    pub const fn new(code: &'static str, classification: StreamFailureClass) -> Self {
        Self {
            code,
            classification,
        }
    }
}

/// One-way lifecycle state for a `DataFrameStream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamTerminal {
    /// The stream may yield its next batch.
    Open,
    /// The source ended normally; later reads return end-of-stream.
    Exhausted,
    /// The consumer closed the stream before exhaustion.
    Closed,
    /// The consumer cancelled the stream before exhaustion.
    Cancelled,
    /// The stream failed; later reads reproduce this structured failure.
    Failed(StreamFailure),
}

impl StreamTerminal {
    /// Return the corresponding repeatable error, if this terminal state cannot yield.
    pub fn as_error(&self) -> Option<DataFrameError> {
        match self {
            Self::Open | Self::Exhausted => None,
            Self::Closed => Some(DataFrameError::stream_closed()),
            Self::Cancelled => Some(DataFrameError::stream_cancelled()),
            Self::Failed(failure) => Some(DataFrameError::stream_failed(
                failure.code,
                failure.classification,
            )),
        }
    }
}

/// Shared terminal state with idempotent transition semantics.
#[derive(Debug, Clone)]
pub struct StreamTerminalState {
    terminal: Arc<Mutex<StreamTerminal>>,
}

impl Default for StreamTerminalState {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamTerminalState {
    /// Start in the open state.
    pub fn new() -> Self {
        Self {
            terminal: Arc::new(Mutex::new(StreamTerminal::Open)),
        }
    }

    /// Return the current terminal state.
    pub fn status(&self) -> StreamTerminal {
        self.terminal
            .lock()
            .expect("stream terminal mutex poisoned")
            .clone()
    }

    /// Move from `Open` to a terminal state once; later transitions retain the first result.
    pub fn finish(&self, requested: StreamTerminal) -> StreamTerminal {
        debug_assert!(!matches!(requested, StreamTerminal::Open));
        let mut terminal = self
            .terminal
            .lock()
            .expect("stream terminal mutex poisoned");
        if matches!(*terminal, StreamTerminal::Open) {
            *terminal = requested;
        }
        terminal.clone()
    }

    /// Verify that another batch may be requested from this stream.
    pub fn ensure_open(&self) -> Result<()> {
        let terminal = self.status();
        match terminal {
            StreamTerminal::Open => Ok(()),
            StreamTerminal::Exhausted => Ok(()),
            _ => Err(terminal
                .as_error()
                .expect("non-open non-exhausted terminals have errors")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::{
        ResourceBudget, ResourceScope, StreamFailure, StreamFailureClass, StreamTerminal,
        StreamTerminalState,
    };
    use crate::DataFrameError;

    #[test]
    fn reservation_accounts_bytes_and_batches_and_releases_once() {
        let budget = ResourceBudget::new(16, NonZeroUsize::new(1).unwrap());
        let mut reservation = budget.reserve_batch(ResourceScope::Decode, 12).unwrap();
        assert_eq!(budget.usage().reserved_bytes, 12);
        assert_eq!(budget.usage().reserved_batches, 1);

        reservation.release();
        reservation.release();
        assert_eq!(budget.usage().reserved_bytes, 0);
        assert_eq!(budget.usage().reserved_batches, 0);
    }

    #[test]
    fn reservation_rejects_before_mutating_usage() {
        let budget = ResourceBudget::new(16, NonZeroUsize::new(1).unwrap());
        let err = budget.reserve_batch(ResourceScope::Output, 17).unwrap_err();
        assert!(matches!(err, DataFrameError::ResourceLimitExceeded { .. }));
        assert_eq!(budget.usage().reserved_bytes, 0);
        assert_eq!(budget.usage().reserved_batches, 0);
    }

    #[test]
    fn terminal_keeps_the_first_non_open_outcome() {
        let state = StreamTerminalState::new();
        let failure = StreamFailure::new("decode_failed", StreamFailureClass::Decode);
        assert_eq!(
            state.finish(StreamTerminal::Failed(failure.clone())),
            StreamTerminal::Failed(failure)
        );
        assert_eq!(state.finish(StreamTerminal::Cancelled), state.status());
        assert!(matches!(
            state.ensure_open(),
            Err(DataFrameError::StreamFailed {
                code: "decode_failed",
                ..
            })
        ));
    }
}
