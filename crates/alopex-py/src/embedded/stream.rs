//! Native synchronous stream lease lifecycle for owned local core sessions.
//!
//! Public SQL/scan entry points are introduced separately. This module establishes the only
//! ownership boundary they may use: an `OwnedKVScan` plus the core lease that opened it, kept in
//! one drop-safe state machine and registered with its database for close-time cleanup.

use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use alopex_core::kv::{
    OwnedKVScan, OwnedKVTransaction, OwnedReadLease, OwnedReadSession, OwnedTransactionLease,
    OwnedTransactionSession,
};
use alopex_core::txn::{OwnedLeaseOutcome, OwnedTransactionSessionStatus};
use alopex_core::{Key, Result as CoreResult, Value};
use alopex_embedded::{OwnedSqlRowOutcome, OwnedSqlStreamPlan};
use alopex_sql::storage::SqlValue;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

use crate::embedded::thread_mode::DatabaseControl;
use crate::error;

/// Observable terminal state for a synchronous owned stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamTerminal {
    /// The cursor may still advance.
    Open,
    /// The cursor reached its normal end.
    Exhausted,
    /// The caller closed the cursor before normal exhaustion.
    Closed,
    /// The caller cancelled the cursor.
    Cancelled,
    /// The source or conversion path failed.
    Failed,
}

impl StreamTerminal {
    fn from_outcome(outcome: OwnedLeaseOutcome) -> Self {
        match outcome {
            OwnedLeaseOutcome::Exhausted => Self::Exhausted,
            OwnedLeaseOutcome::Closed => Self::Closed,
            OwnedLeaseOutcome::Cancelled => Self::Cancelled,
            OwnedLeaseOutcome::Failed => Self::Failed,
        }
    }
}

enum CoreLease {
    Read {
        _session: OwnedReadSession,
        lease: OwnedReadLease,
    },
    #[allow(dead_code)]
    Transaction {
        session: OwnedTransactionSession,
        lease: OwnedTransactionLease,
    },
}

impl CoreLease {
    fn finish(self, outcome: OwnedLeaseOutcome) -> CoreResult<()> {
        match self {
            Self::Read { lease, .. } => lease.finish(outcome).map(|_| ()),
            Self::Transaction { lease, .. } => lease.finish(outcome).map(|_| ()),
        }
    }

    #[allow(dead_code)]
    fn transaction_status(&self) -> Option<OwnedTransactionSessionStatus> {
        match self {
            Self::Read { .. } => None,
            Self::Transaction { session, .. } => Some(session.status()),
        }
    }
}

struct StreamLeaseState {
    cursor: Option<Box<dyn OwnedKVScan>>,
    lease: Option<CoreLease>,
    terminal: StreamTerminal,
}

struct StreamLeaseInner {
    control: Arc<DatabaseControl>,
    state: Mutex<StreamLeaseState>,
}

/// Database-owned registry for native stream cleanup.
#[derive(Default)]
pub(crate) struct StreamLeaseRegistry {
    leases: Mutex<Vec<Weak<StreamLeaseInner>>>,
}

impl StreamLeaseRegistry {
    /// Register one newly opened stream. Finished streams are pruned on later registry access.
    fn register(&self, lease: &Arc<StreamLeaseInner>) {
        let mut leases = self.leases.lock().expect("stream registry lock poisoned");
        leases.retain(|weak| weak.strong_count() > 0);
        leases.push(Arc::downgrade(lease));
    }

    /// Close every live stream as part of database close, bypassing the now-closing access gate.
    pub(crate) fn close_all(&self) -> PyResult<()> {
        let leases = {
            let mut guard = self
                .leases
                .lock()
                .map_err(|_| error::to_py_err("stream registry lock poisoned"))?;
            let leases = guard.iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
            guard.retain(|weak| weak.strong_count() > 0);
            leases
        };
        for inner in leases {
            StreamLease { inner }.finish(OwnedLeaseOutcome::Closed)?;
        }
        Ok(())
    }
}

/// One synchronous native cursor lease. It is not a Python class until Task 4.7 supplies public
/// SQL/scan entry points and Python row conversion.
#[derive(Clone)]
pub(crate) struct StreamLease {
    inner: Arc<StreamLeaseInner>,
}

impl StreamLease {
    /// Open a read-only owned cursor and register it with the database close path.
    pub(crate) fn open_read(
        control: Arc<DatabaseControl>,
        registry: &StreamLeaseRegistry,
        session: OwnedReadSession,
        open: impl FnOnce(&mut dyn OwnedKVTransaction) -> CoreResult<Box<dyn OwnedKVScan>>,
    ) -> PyResult<Self> {
        let lease = session.acquire_lease().map_err(error::core_err)?;
        let cursor = match lease.with_transaction(open) {
            Ok(cursor) => cursor,
            Err(source) => {
                let _ = lease.finish(OwnedLeaseOutcome::Failed);
                return Err(error::core_err(source));
            }
        };
        Ok(Self::register(
            control,
            registry,
            cursor,
            CoreLease::Read {
                _session: session,
                lease,
            },
        ))
    }

    /// Open a transaction-owned cursor and register it with the database close path.
    #[allow(dead_code)]
    pub(crate) fn open_transaction(
        control: Arc<DatabaseControl>,
        registry: &StreamLeaseRegistry,
        session: OwnedTransactionSession,
        open: impl FnOnce(&mut dyn OwnedKVTransaction) -> CoreResult<Box<dyn OwnedKVScan>>,
    ) -> PyResult<Self> {
        let lease = session.acquire_lease().map_err(error::core_err)?;
        let cursor = match lease.with_transaction(open) {
            Ok(cursor) => cursor,
            Err(source) => {
                let _ = lease.finish(OwnedLeaseOutcome::Failed);
                return Err(error::core_err(source));
            }
        };
        Ok(Self::register(
            control,
            registry,
            cursor,
            CoreLease::Transaction { session, lease },
        ))
    }

    fn register(
        control: Arc<DatabaseControl>,
        registry: &StreamLeaseRegistry,
        cursor: Box<dyn OwnedKVScan>,
        lease: CoreLease,
    ) -> Self {
        let inner = Arc::new(StreamLeaseInner {
            control,
            state: Mutex::new(StreamLeaseState {
                cursor: Some(cursor),
                lease: Some(lease),
                terminal: StreamTerminal::Open,
            }),
        });
        registry.register(&inner);
        Self { inner }
    }

    fn ensure_native_open(&self) -> Result<(), NativeStreamError> {
        self.inner
            .control
            .ensure_open_for_native_worker()
            .map_err(|code| match code {
                "thread_mode_violation" => {
                    NativeStreamError::new(code, "database is restricted to its creating thread")
                }
                "stream_closed" => NativeStreamError::new(code, "stream is closed"),
                _ => NativeStreamError::new(code, "database stream control is unavailable"),
            })
    }

    /// Advance one core key/value entry without constructing or retaining a Python exception.
    ///
    /// This is the only cursor advance path used by the native async worker.  It has exactly the
    /// same one-way lease transitions as [`Self::next_entry`].
    fn next_entry_native(&self) -> Result<Option<(Key, Value)>, NativeStreamError> {
        self.ensure_native_open()?;
        let next = {
            let mut state = self.inner.state.lock().map_err(|_| {
                NativeStreamError::new("stream_failure", "stream state lock poisoned")
            })?;
            if state.terminal != StreamTerminal::Open {
                return Ok(None);
            }
            let cursor = state.cursor.as_mut().ok_or_else(|| {
                NativeStreamError::new("stream_closed", "stream cursor is closed")
            })?;
            cursor.next_entry()
        };
        match next {
            Ok(Some(entry)) => Ok(Some(entry)),
            Ok(None) => {
                self.finish_native(OwnedLeaseOutcome::Exhausted)?;
                Ok(None)
            }
            Err(source) => {
                let _ = self.finish_native(OwnedLeaseOutcome::Failed);
                Err(NativeStreamError::new("stream_failure", source.to_string()))
            }
        }
    }

    /// Advance one core key/value entry. Normal exhaustion is an idempotent end-of-stream.
    #[cfg(test)]
    pub(crate) fn next_entry(&self) -> PyResult<Option<(Key, Value)>> {
        self.next_entry_native()
            .map_err(NativeStreamError::into_py_err)
    }

    /// Explicitly close a stream. Later calls are harmless and yield no more results.
    pub(crate) fn close(&self) -> PyResult<()> {
        self.ensure_native_open()
            .and_then(|_| self.finish_native(OwnedLeaseOutcome::Closed))
            .map_err(NativeStreamError::into_py_err)
    }

    /// Return the terminal class without advancing the cursor.
    #[cfg(test)]
    pub(crate) fn terminal(&self) -> PyResult<StreamTerminal> {
        self.ensure_native_open()
            .and_then(|_| self.terminal_without_access_check_native())
            .map_err(NativeStreamError::into_py_err)
    }

    /// Read the one-way terminal state during database close without reopening the public access
    /// gate. The registry uses this path only after it has already stopped new leases.
    fn terminal_without_access_check(&self) -> PyResult<StreamTerminal> {
        self.terminal_without_access_check_native()
            .map_err(NativeStreamError::into_py_err)
    }

    fn terminal_without_access_check_native(&self) -> Result<StreamTerminal, NativeStreamError> {
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| NativeStreamError::new("stream_failure", "stream state lock poisoned"))?
            .terminal)
    }

    /// Return the core transaction state, if this stream belongs to a write transaction.
    #[allow(dead_code)]
    pub(crate) fn transaction_status(&self) -> PyResult<Option<OwnedTransactionSessionStatus>> {
        self.inner.control.ensure_open()?;
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("stream state lock poisoned"))?;
        Ok(state.lease.as_ref().and_then(CoreLease::transaction_status))
    }

    fn finish_native(&self, outcome: OwnedLeaseOutcome) -> Result<(), NativeStreamError> {
        let (mut cursor, lease) = {
            let mut state = self.inner.state.lock().map_err(|_| {
                NativeStreamError::new("stream_failure", "stream state lock poisoned")
            })?;
            if state.terminal != StreamTerminal::Open {
                return Ok(());
            }
            state.terminal = StreamTerminal::from_outcome(outcome);
            (state.cursor.take(), state.lease.take())
        };
        if let Some(cursor) = cursor.as_mut() {
            cursor
                .close()
                .map_err(|source| NativeStreamError::new("stream_failure", source.to_string()))?;
        }
        if let Some(lease) = lease {
            lease
                .finish(outcome)
                .map_err(|source| NativeStreamError::new("stream_failure", source.to_string()))?;
        }
        Ok(())
    }

    fn finish(&self, outcome: OwnedLeaseOutcome) -> PyResult<()> {
        self.finish_native(outcome)
            .map_err(NativeStreamError::into_py_err)
    }
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        let _ = self.finish(OwnedLeaseOutcome::Closed);
    }
}

const DEFAULT_STREAM_RESOURCE_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// A Python-independent stream failure used by the native async producer.
///
/// The producer deliberately cannot construct Python exceptions: it may run without the GIL and
/// must never retain a Python object while it owns a cursor or stream-state lock.  The receiving
/// Python thread maps this compact envelope to the established `AlopexError` contract.
#[derive(Clone, Debug)]
pub(crate) struct NativeStreamError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl NativeStreamError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn into_py_err(self) -> PyErr {
        error::stream_error(self.code, self.message)
    }
}

/// One decoded SQL row that is safe to move between native threads.
///
/// Python conversion is intentionally deferred until the consumer receives this value.  This
/// keeps producer backpressure independent of the GIL and bounds only Rust-owned row values.
#[derive(Debug)]
pub(crate) struct NativeSqlRow {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<SqlValue>,
}

impl NativeSqlRow {
    pub(crate) fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let output = PyDict::new(py);
        for (name, value) in self.columns.iter().zip(self.values) {
            output.set_item(name, crate::embedded::sql::sql_value_to_py(py, value)?)?;
        }
        output.into_py_any(py)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicTerminal {
    Open,
    Exhausted,
    Closed,
    Cancelled,
    TimedOut,
    ResourceLimit,
    Failed,
}

impl PublicTerminal {
    fn name(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Exhausted => "exhausted",
            Self::Closed => "closed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::ResourceLimit => "resource_limit",
            Self::Failed => "failed",
        }
    }
}

struct PySqlStreamState {
    plan: OwnedSqlStreamPlan,
    terminal: PublicTerminal,
    rows_delivered: usize,
    failure_message: Option<String>,
    failure_code: Option<&'static str>,
}

/// A synchronous local SQL stream backed only by an owned core read session.
///
/// The class holds no Python values between calls. Each `__next__` decodes and converts one row,
/// then releases that native row before the next call. The stream is deliberately local-only and
/// its plan has been preflight-validated before the owned cursor was opened.
#[pyclass(name = "SqlResultStream")]
pub struct PySqlResultStream {
    lease: StreamLease,
    state: Mutex<PySqlStreamState>,
    deadline: Option<Instant>,
    resource_limit_bytes: usize,
    transaction_session: Option<OwnedTransactionSession>,
}

impl PySqlResultStream {
    pub(crate) fn open_database(
        database: &alopex_embedded::Database,
        control: Arc<DatabaseControl>,
        registry: &StreamLeaseRegistry,
        plan: OwnedSqlStreamPlan,
        resource_limit_bytes: Option<usize>,
        timeout_seconds: Option<f64>,
    ) -> PyResult<Self> {
        let (resource_limit_bytes, deadline) =
            Self::stream_options(resource_limit_bytes, timeout_seconds)?;

        let cursor_plan = plan.clone();
        let session = database
            .begin_owned_read(Default::default())
            .map_err(error::embedded_err)?;
        let lease = StreamLease::open_read(control, registry, session, move |transaction| {
            cursor_plan.open_cursor(transaction)
        })?;
        Ok(Self {
            lease,
            state: Mutex::new(PySqlStreamState {
                plan,
                terminal: PublicTerminal::Open,
                rows_delivered: 0,
                failure_message: None,
                failure_code: None,
            }),
            deadline,
            resource_limit_bytes,
            transaction_session: None,
        })
    }

    /// Open a SQL cursor that retains an owned transaction session until one stream terminal
    /// outcome releases its lease.  The session remains available to the Python transaction so
    /// `commit()` can classify active, committable, and abort-required states.
    pub(crate) fn open_transaction(
        control: Arc<DatabaseControl>,
        registry: &StreamLeaseRegistry,
        session: OwnedTransactionSession,
        plan: OwnedSqlStreamPlan,
        resource_limit_bytes: Option<usize>,
        timeout_seconds: Option<f64>,
    ) -> PyResult<Self> {
        let (resource_limit_bytes, deadline) =
            Self::stream_options(resource_limit_bytes, timeout_seconds)?;
        let cursor_plan = plan.clone();
        let lease = StreamLease::open_transaction(
            control,
            registry,
            session.clone(),
            move |transaction| cursor_plan.open_cursor(transaction),
        )?;
        Ok(Self {
            lease,
            state: Mutex::new(PySqlStreamState {
                plan,
                terminal: PublicTerminal::Open,
                rows_delivered: 0,
                failure_message: None,
                failure_code: None,
            }),
            deadline,
            resource_limit_bytes,
            transaction_session: Some(session),
        })
    }

    fn stream_options(
        resource_limit_bytes: Option<usize>,
        timeout_seconds: Option<f64>,
    ) -> PyResult<(usize, Option<Instant>)> {
        let resource_limit_bytes =
            resource_limit_bytes.unwrap_or(DEFAULT_STREAM_RESOURCE_LIMIT_BYTES);
        if resource_limit_bytes == 0 {
            return Err(error::stream_error(
                "stream_resource_limit",
                "resource_limit_bytes must be positive",
            ));
        }
        let deadline = match timeout_seconds {
            Some(seconds) if !seconds.is_finite() || seconds < 0.0 => {
                return Err(error::stream_error(
                    "stream_timeout",
                    "timeout must be a finite non-negative number of seconds",
                ));
            }
            Some(seconds) => Some(
                Instant::now()
                    .checked_add(Duration::from_secs_f64(seconds))
                    .ok_or_else(|| error::stream_error("stream_timeout", "timeout is too large"))?,
            ),
            None => None,
        };
        Ok((resource_limit_bytes, deadline))
    }

    fn transaction_effect(&self) -> &'static str {
        match self
            .transaction_session
            .as_ref()
            .map(|session| session.status())
        {
            None => "none",
            Some(OwnedTransactionSessionStatus::Open)
            | Some(OwnedTransactionSessionStatus::Committable) => "committable",
            Some(OwnedTransactionSessionStatus::LeaseActive) => "active",
            Some(OwnedTransactionSessionStatus::MustAbort) => "must_abort",
            Some(OwnedTransactionSessionStatus::Committed)
            | Some(OwnedTransactionSessionStatus::RolledBack)
            | Some(OwnedTransactionSessionStatus::Closed) => "closed",
        }
    }

    fn set_terminal(
        &self,
        terminal: PublicTerminal,
        message: Option<String>,
        code: Option<&'static str>,
    ) -> PyResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("SQL stream state lock poisoned"))?;
        if state.terminal == PublicTerminal::Open {
            state.terminal = terminal;
            state.failure_message = message;
            state.failure_code = code;
        }
        Ok(())
    }

    fn set_terminal_native(
        &self,
        terminal: PublicTerminal,
        message: Option<String>,
        code: Option<&'static str>,
    ) -> Result<(), NativeStreamError> {
        let mut state = self.state.lock().map_err(|_| {
            NativeStreamError::new("stream_failure", "SQL stream state lock poisoned")
        })?;
        if state.terminal == PublicTerminal::Open {
            state.terminal = terminal;
            state.failure_message = message;
            state.failure_code = code;
        }
        Ok(())
    }

    /// Reconcile a registry-driven native close with the Python-visible terminal state. A
    /// `Database.close()` owns the lease registry rather than Python stream objects, so this
    /// check is needed for stream handles that outlive their database.
    fn sync_terminal_from_lease(&self) -> PyResult<()> {
        let lease_terminal = self.lease.terminal_without_access_check()?;
        let terminal = match lease_terminal {
            StreamTerminal::Open => return Ok(()),
            StreamTerminal::Exhausted => PublicTerminal::Exhausted,
            StreamTerminal::Closed => PublicTerminal::Closed,
            StreamTerminal::Cancelled => PublicTerminal::Cancelled,
            StreamTerminal::Failed => PublicTerminal::Failed,
        };
        self.set_terminal(terminal, None, None)
    }

    fn sync_terminal_from_lease_native(&self) -> Result<(), NativeStreamError> {
        let lease_terminal = self.lease.terminal_without_access_check_native()?;
        let terminal = match lease_terminal {
            StreamTerminal::Open => return Ok(()),
            StreamTerminal::Exhausted => PublicTerminal::Exhausted,
            StreamTerminal::Closed => PublicTerminal::Closed,
            StreamTerminal::Cancelled => PublicTerminal::Cancelled,
            StreamTerminal::Failed => PublicTerminal::Failed,
        };
        self.set_terminal_native(terminal, None, None)
    }

    /// Return `Ok(true)` for normal exhaustion, `Ok(false)` while open, and a stable native
    /// failure for every other terminal.  No Python object is constructed on this path.
    fn native_terminal_result(&self) -> Result<bool, NativeStreamError> {
        self.sync_terminal_from_lease_native()?;
        let state = self.state.lock().map_err(|_| {
            NativeStreamError::new("stream_failure", "SQL stream state lock poisoned")
        })?;
        match state.terminal {
            PublicTerminal::Open => Ok(false),
            PublicTerminal::Exhausted => Ok(true),
            PublicTerminal::Closed => {
                Err(NativeStreamError::new("stream_closed", "stream is closed"))
            }
            PublicTerminal::Cancelled => Err(NativeStreamError::new(
                "stream_cancelled",
                "stream was cancelled",
            )),
            PublicTerminal::TimedOut => {
                Err(NativeStreamError::new("stream_timeout", "stream timed out"))
            }
            PublicTerminal::ResourceLimit => Err(NativeStreamError::new(
                "stream_resource_limit",
                "stream row exceeds resource_limit_bytes",
            )),
            PublicTerminal::Failed => Err(NativeStreamError::new(
                state.failure_code.unwrap_or("stream_failure"),
                state.failure_message.as_deref().unwrap_or("stream failed"),
            )),
        }
    }

    fn timed_out(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    fn timeout_native(&self) -> Result<(), NativeStreamError> {
        self.lease.finish_native(OwnedLeaseOutcome::Cancelled)?;
        self.set_terminal_native(PublicTerminal::TimedOut, None, None)
    }

    fn fail_embedded_native(
        &self,
        source: alopex_embedded::Error,
    ) -> Result<(), NativeStreamError> {
        let code = source.sql_error_code().unwrap_or("stream_failure");
        let message = source.to_string();
        let _ = self.lease.finish_native(OwnedLeaseOutcome::Failed);
        self.set_terminal_native(PublicTerminal::Failed, Some(message), Some(code))
    }

    /// Advance and decode exactly one SQL row without touching Python state.
    ///
    /// The native async bridge invokes this method from its producer thread.  It keeps only a
    /// Rust row in its bounded channel and leaves Python dictionary construction to the receiving
    /// event-loop side after all stream locks have been released.
    pub(crate) fn next_native_row(&self) -> Result<Option<NativeSqlRow>, NativeStreamError> {
        if self.native_terminal_result()? {
            return Ok(None);
        }
        if self.timed_out() {
            self.timeout_native()?;
            return Err(NativeStreamError::new("stream_timeout", "stream timed out"));
        }

        loop {
            {
                let state = self.state.lock().map_err(|_| {
                    NativeStreamError::new("stream_failure", "SQL stream state lock poisoned")
                })?;
                if state.plan.is_exhausted() {
                    drop(state);
                    self.lease.finish_native(OwnedLeaseOutcome::Exhausted)?;
                    self.set_terminal_native(PublicTerminal::Exhausted, None, None)?;
                    return Ok(None);
                }
            }

            let entry = match self.lease.next_entry_native() {
                Ok(entry) => entry,
                Err(source) => {
                    let _ = self.lease.finish_native(OwnedLeaseOutcome::Failed);
                    self.set_terminal_native(
                        PublicTerminal::Failed,
                        Some(source.message.clone()),
                        Some(source.code),
                    )?;
                    return Err(source);
                }
            };
            if self.timed_out() {
                self.timeout_native()?;
                return Err(NativeStreamError::new("stream_timeout", "stream timed out"));
            }
            let Some((key, value)) = entry else {
                let terminal = self.lease.terminal_without_access_check_native()?;
                let public_terminal = match terminal {
                    StreamTerminal::Exhausted => PublicTerminal::Exhausted,
                    StreamTerminal::Closed => PublicTerminal::Closed,
                    StreamTerminal::Cancelled => PublicTerminal::Cancelled,
                    StreamTerminal::Failed | StreamTerminal::Open => PublicTerminal::Failed,
                };
                self.set_terminal_native(public_terminal, None, None)?;
                return match public_terminal {
                    PublicTerminal::Exhausted => Ok(None),
                    PublicTerminal::Closed => {
                        Err(NativeStreamError::new("stream_closed", "stream is closed"))
                    }
                    PublicTerminal::Cancelled => Err(NativeStreamError::new(
                        "stream_cancelled",
                        "stream was cancelled",
                    )),
                    _ => Err(NativeStreamError::new(
                        "stream_failure",
                        "stream ended unexpectedly",
                    )),
                };
            };

            let outcome = {
                let mut state = self.state.lock().map_err(|_| {
                    NativeStreamError::new("stream_failure", "SQL stream state lock poisoned")
                })?;
                state.plan.process_entry(key, value)
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(source) => {
                    let code = source.sql_error_code().unwrap_or("stream_failure");
                    let message = source.to_string();
                    self.fail_embedded_native(source)?;
                    return Err(NativeStreamError::new(code, message));
                }
            };
            match outcome {
                OwnedSqlRowOutcome::Skip => continue,
                OwnedSqlRowOutcome::Exhausted => {
                    self.lease.finish_native(OwnedLeaseOutcome::Exhausted)?;
                    self.set_terminal_native(PublicTerminal::Exhausted, None, None)?;
                    return Ok(None);
                }
                OwnedSqlRowOutcome::Row(values) => {
                    if estimated_row_bytes(&values) > self.resource_limit_bytes {
                        self.lease.finish_native(OwnedLeaseOutcome::Failed)?;
                        self.set_terminal_native(PublicTerminal::ResourceLimit, None, None)?;
                        return Err(NativeStreamError::new(
                            "stream_resource_limit",
                            "stream row exceeds resource_limit_bytes",
                        ));
                    }
                    if self.timed_out() {
                        self.timeout_native()?;
                        return Err(NativeStreamError::new("stream_timeout", "stream timed out"));
                    }
                    let columns = {
                        let mut state = self.state.lock().map_err(|_| {
                            NativeStreamError::new(
                                "stream_failure",
                                "SQL stream state lock poisoned",
                            )
                        })?;
                        state.rows_delivered += 1;
                        state
                            .plan
                            .columns()
                            .iter()
                            .map(|column| column.name.clone())
                            .collect()
                    };
                    return Ok(Some(NativeSqlRow { columns, values }));
                }
            }
        }
    }

    /// Close from a native bridge without relying on a Python exception or a GIL-bound method.
    pub(crate) fn close_native(&self) -> Result<(), NativeStreamError> {
        self.sync_terminal_from_lease_native()?;
        let is_open = self
            .state
            .lock()
            .map_err(|_| {
                NativeStreamError::new("stream_failure", "SQL stream state lock poisoned")
            })?
            .terminal
            == PublicTerminal::Open;
        if !is_open {
            return Ok(());
        }
        self.lease.finish_native(OwnedLeaseOutcome::Closed)?;
        self.set_terminal_native(PublicTerminal::Closed, None, None)
    }

    /// Cancel from a native bridge without relying on a Python exception or a GIL-bound method.
    pub(crate) fn cancel_native(&self) -> Result<(), NativeStreamError> {
        self.sync_terminal_from_lease_native()?;
        let is_open = self
            .state
            .lock()
            .map_err(|_| {
                NativeStreamError::new("stream_failure", "SQL stream state lock poisoned")
            })?
            .terminal
            == PublicTerminal::Open;
        if !is_open {
            return Ok(());
        }
        self.lease.finish_native(OwnedLeaseOutcome::Cancelled)?;
        self.set_terminal_native(PublicTerminal::Cancelled, None, None)
    }

    pub(crate) fn status_mapping(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.status(py)
    }

    pub(crate) fn next_row(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self
            .next_native_row()
            .map_err(NativeStreamError::into_py_err)?
        {
            Some(row) => row.into_py(py),
            None => Err(PyStopIteration::new_err(())),
        }
    }
}

#[pymethods]
impl PySqlResultStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.next_row(py)
    }

    pub(crate) fn close(&self) -> PyResult<()> {
        self.close_native().map_err(NativeStreamError::into_py_err)
    }

    fn cancel(&self) -> PyResult<()> {
        self.cancel_native().map_err(NativeStreamError::into_py_err)
    }

    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.sync_terminal_from_lease()?;
        let state = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("SQL stream state lock poisoned"))?;
        let status = PyDict::new(py);
        status.set_item("terminal", state.terminal.name())?;
        status.set_item("rows_delivered", state.rows_delivered)?;
        status.set_item("resource_limit_bytes", self.resource_limit_bytes)?;
        status.set_item("resource_scope", "sql_row")?;
        status.set_item("transaction_effect", self.transaction_effect())?;
        Ok(status.unbind())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.sync_terminal_from_lease()?;
        if self
            .state
            .lock()
            .map_err(|_| error::to_py_err("SQL stream state lock poisoned"))?
            .terminal
            == PublicTerminal::Open
        {
            self.lease.close()?;
            self.set_terminal(PublicTerminal::Closed, None, None)?;
        }
        Ok(false)
    }
}

fn estimated_row_bytes(row: &[SqlValue]) -> usize {
    row.iter()
        .map(|value| match value {
            SqlValue::Null => 1,
            SqlValue::Integer(_)
            | SqlValue::BigInt(_)
            | SqlValue::Double(_)
            | SqlValue::Timestamp(_)
            | SqlValue::Time(_) => 8,
            SqlValue::Float(_) => 4,
            SqlValue::Date(_) => 4,
            SqlValue::Interval { .. } => 16,
            SqlValue::Decimal(_) => 17,
            SqlValue::Json(value) => value.as_str().len(),
            SqlValue::Boolean(_) => 1,
            SqlValue::Text(value) => value.len(),
            SqlValue::Blob(value) => value.len(),
            SqlValue::Vector(values) => values.len().saturating_mul(std::mem::size_of::<f32>()),
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alopex_core::kv::OwnedSessionFactory;
    use alopex_core::MemoryKV;
    use alopex_core::TxnMode;
    use pyo3::exceptions::PyStopIteration;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
    use pyo3::Python;

    use super::{
        DatabaseControl, OwnedSqlStreamPlan, PySqlResultStream, StreamLease, StreamLeaseRegistry,
        StreamTerminal,
    };
    use crate::embedded::thread_mode::ThreadMode;

    fn error_code(py: Python<'_>, error: pyo3::PyErr) -> String {
        error
            .value(py)
            .getattr("code")
            .unwrap()
            .extract::<String>()
            .unwrap()
    }

    #[test]
    fn exhausted_transaction_stream_is_committable_and_cannot_yield_again() {
        pyo3::Python::initialize();
        let control = Arc::new(DatabaseControl::new(ThreadMode::Multi));
        let registry = StreamLeaseRegistry::default();
        let store = Arc::new(MemoryKV::new());
        let session = store
            .clone()
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();

        let seed = session.acquire_lease().unwrap();
        seed.with_transaction(|transaction| transaction.put(b"stream".to_vec(), b"value".to_vec()))
            .unwrap();
        seed.finish(alopex_core::txn::OwnedLeaseOutcome::Exhausted)
            .unwrap();

        let stream =
            StreamLease::open_transaction(control, &registry, session.clone(), |transaction| {
                transaction.scan_prefix(b"stream")
            })
            .unwrap();
        assert_eq!(
            stream.next_entry().unwrap(),
            Some((b"stream".to_vec(), b"value".to_vec()))
        );
        assert_eq!(stream.next_entry().unwrap(), None);
        assert_eq!(stream.next_entry().unwrap(), None);
        assert_eq!(stream.terminal().unwrap(), StreamTerminal::Exhausted);
        assert!(session.status().can_commit());
        session.commit().unwrap();
    }

    #[test]
    fn read_stream_lease_keeps_a_committed_memory_cursor_open() {
        let control = Arc::new(DatabaseControl::new(ThreadMode::Multi));
        let registry = StreamLeaseRegistry::default();
        let store = Arc::new(MemoryKV::new());
        let writer = store
            .clone()
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();
        let write_lease = writer.acquire_lease().unwrap();
        write_lease
            .with_transaction(|transaction| {
                transaction.put(b"stream:1".to_vec(), b"value".to_vec())
            })
            .unwrap();
        write_lease
            .finish(alopex_core::txn::OwnedLeaseOutcome::Exhausted)
            .unwrap();
        writer.commit().unwrap();

        let reader = store.clone().begin_owned_read(Default::default()).unwrap();
        let stream = StreamLease::open_read(control, &registry, reader, |transaction| {
            transaction.scan_prefix(b"stream:")
        })
        .unwrap();
        assert_eq!(
            stream.next_entry().unwrap(),
            Some((b"stream:1".to_vec(), b"value".to_vec()))
        );
        assert_eq!(stream.next_entry().unwrap(), None);
    }

    #[test]
    fn sql_plan_cursor_keeps_a_committed_table_scan_open() {
        let database = alopex_embedded::Database::new();
        database
            .execute_sql("CREATE TABLE stream_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        database
            .execute_sql("INSERT INTO stream_rows (id) VALUES (1)")
            .unwrap();

        let plan = OwnedSqlStreamPlan::preflight(&database, "SELECT id FROM stream_rows").unwrap();
        let cursor_plan = plan.clone();
        let session = database.begin_owned_read(Default::default()).unwrap();
        let stream = StreamLease::open_read(
            Arc::new(DatabaseControl::new(ThreadMode::Multi)),
            &StreamLeaseRegistry::default(),
            session,
            move |transaction| cursor_plan.open_cursor(transaction),
        )
        .unwrap();

        assert!(stream.next_entry().unwrap().is_some());
    }

    #[test]
    fn database_registry_close_requires_transaction_rollback() {
        pyo3::Python::initialize();
        let control = Arc::new(DatabaseControl::new(ThreadMode::Multi));
        let registry = StreamLeaseRegistry::default();
        let store = Arc::new(MemoryKV::new());
        let session = store
            .clone()
            .begin_owned_transaction(TxnMode::ReadWrite)
            .unwrap();
        let stream =
            StreamLease::open_transaction(control, &registry, session.clone(), |transaction| {
                transaction.scan_prefix(b"anything")
            })
            .unwrap();
        registry.close_all().unwrap();
        assert_eq!(stream.terminal().unwrap(), StreamTerminal::Closed);
        assert!(session.commit().is_err());
        assert!(session.rollback().is_ok());
    }

    #[test]
    fn python_sql_stream_matches_the_supported_normal_select_without_materializing_rows() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let database = alopex_embedded::Database::new();
            database
                .execute_sql(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, enabled BOOLEAN)",
                )
                .unwrap();
            database
                .execute_sql(
                    "INSERT INTO users (id, name, enabled) VALUES (1, 'one', true), (2, 'two', false), (3, 'three', true)",
                )
                .unwrap();
            let expected = database
                .execute_sql("SELECT name, id + 10 AS next_id FROM users WHERE enabled = true LIMIT 1 OFFSET 1")
                .unwrap();

            let plan = alopex_embedded::OwnedSqlStreamPlan::preflight(
                &database,
                "SELECT name, id + 10 AS next_id FROM users WHERE enabled = true LIMIT 1 OFFSET 1",
            )
            .unwrap();
            let control = Arc::new(DatabaseControl::new(ThreadMode::Multi));
            let registry = StreamLeaseRegistry::default();
            let stream =
                PySqlResultStream::open_database(&database, control, &registry, plan, None, None)
                    .unwrap();

            let row = stream.next_row(py).unwrap();
            let row = row.bind(py).cast::<PyDict>().unwrap();
            assert_eq!(
                row.get_item("name")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "three"
            );
            assert_eq!(
                row.get_item("next_id")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                13
            );
            assert!(stream
                .next_row(py)
                .unwrap_err()
                .is_instance_of::<PyStopIteration>(py));
            assert!(stream
                .next_row(py)
                .unwrap_err()
                .is_instance_of::<PyStopIteration>(py));
            let status = stream.status(py).unwrap();
            assert_eq!(
                status
                    .bind(py)
                    .get_item("terminal")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "exhausted"
            );

            let alopex_sql::ExecutionResult::Query(expected) = expected else {
                panic!("normal supported SELECT must return rows")
            };
            assert_eq!(expected.rows.len(), 1);
            assert_eq!(
                expected.rows[0][0],
                alopex_sql::SqlValue::Text("three".to_string())
            );
            assert_eq!(expected.rows[0][1], alopex_sql::SqlValue::Integer(13));
        });
    }

    #[test]
    fn python_sql_stream_records_close_timeout_and_resource_terminals() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let database = alopex_embedded::Database::new();
            database
                .execute_sql("CREATE TABLE values_table (id INTEGER PRIMARY KEY, payload TEXT)")
                .unwrap();
            database
                .execute_sql("INSERT INTO values_table (id, payload) VALUES (1, 'abcdefghijklmnopqrstuvwxyz')")
                .unwrap();

            let open = |limit, timeout| {
                let plan = alopex_embedded::OwnedSqlStreamPlan::preflight(
                    &database,
                    "SELECT payload FROM values_table",
                )
                .unwrap();
                PySqlResultStream::open_database(
                    &database,
                    Arc::new(DatabaseControl::new(ThreadMode::Multi)),
                    &StreamLeaseRegistry::default(),
                    plan,
                    limit,
                    timeout,
                )
                .unwrap()
            };

            let closed = open(None, None);
            closed.close().unwrap();
            closed.close().unwrap();
            assert_eq!(
                error_code(py, closed.next_row(py).unwrap_err()),
                "stream_closed"
            );

            let timed_out = open(None, Some(0.0));
            assert_eq!(
                error_code(py, timed_out.next_row(py).unwrap_err()),
                "stream_timeout"
            );
            assert_eq!(
                error_code(py, timed_out.next_row(py).unwrap_err()),
                "stream_timeout"
            );

            let limited = open(Some(8), None);
            assert_eq!(
                error_code(py, limited.next_row(py).unwrap_err()),
                "stream_resource_limit"
            );
            assert_eq!(
                error_code(py, limited.next_row(py).unwrap_err()),
                "stream_resource_limit"
            );
        });
    }

    #[test]
    fn registry_close_updates_python_sql_stream_terminal_after_database_is_closed() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let database = alopex_embedded::Database::new();
            let plan =
                alopex_embedded::OwnedSqlStreamPlan::preflight(&database, "SELECT 1").unwrap();
            let control = Arc::new(DatabaseControl::new(ThreadMode::Multi));
            let registry = StreamLeaseRegistry::default();
            let stream = PySqlResultStream::open_database(
                &database,
                control.clone(),
                &registry,
                plan,
                None,
                None,
            )
            .unwrap();

            assert!(control.begin_close().unwrap());
            registry.close_all().unwrap();
            control.finish_close().unwrap();

            assert_eq!(
                error_code(py, stream.next_row(py).expect_err("closed stream")),
                "stream_closed"
            );
            assert_eq!(
                error_code(py, stream.next_row(py).expect_err("repeat closed stream")),
                "stream_closed"
            );
            let status = stream.status(py).unwrap();
            assert_eq!(
                status
                    .bind(py)
                    .get_item("terminal")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "closed"
            );
            stream.close().unwrap();
            stream.cancel().unwrap();
        });
    }
}
