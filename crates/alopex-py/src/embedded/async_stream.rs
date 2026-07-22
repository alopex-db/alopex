//! Native bounded bridge for local SQL async streams.
//!
//! The bridge owns only Rust stream state and moves only [`NativeSqlRow`] values through a
//! bounded `sync_channel`.  Python values are created by the receiving `next()` call after the
//! producer has released every cursor/state lock.  No Tokio runtime, Python callback, or Python
//! queue participates in producer backpressure.

use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::embedded::stream::{NativeSqlRow, NativeStreamError, PySqlResultStream};
use crate::embedded::thread_mode::ThreadMode;
use crate::error;
use crate::types::{PyDataFrame, PyDataFrameStream};

#[derive(Clone, Debug)]
enum BridgeTerminal {
    Exhausted,
    Error(NativeStreamError),
}

enum NativeAsyncPayload {
    Sql(NativeSqlRow),
    DataFrame {
        frame: alopex_dataframe::DataFrame,
        control: Arc<crate::embedded::thread_mode::DatabaseControl>,
    },
}

impl NativeAsyncPayload {
    fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Sql(row) => row.into_py(py),
            Self::DataFrame { frame, control } => {
                Py::new(py, PyDataFrame::with_control(frame, control))?.into_py_any(py)
            }
        }
    }
}

/// A private, Rust-only handoff object returned by an executor-side receive.
///
/// It deliberately contains no Python row or DataFrame. The asyncio facade moves this object
/// back to the event-loop thread and calls `deliver_python()` there, after the bounded native
/// receive and every source lock have completed.
#[pyclass(name = "_NativeAsyncPayload", skip_from_py_object)]
pub(crate) struct PyNativeAsyncPayload {
    payload: Option<NativeAsyncPayload>,
}

#[pymethods]
impl PyNativeAsyncPayload {
    fn deliver_python(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.payload
            .take()
            .ok_or_else(|| {
                error::stream_error(
                    "stream_failure",
                    "native async payload was already delivered",
                )
            })?
            .into_py(py)
    }
}

enum NativeAsyncSource {
    Sql(Arc<PySqlResultStream>),
    DataFrame(Arc<PyDataFrameStream>),
}

impl NativeAsyncSource {
    fn next(&self) -> Result<Option<NativeAsyncPayload>, NativeStreamError> {
        match self {
            Self::Sql(stream) => stream
                .next_native_row()
                .map(|row| row.map(NativeAsyncPayload::Sql)),
            Self::DataFrame(stream) => stream.next_native_frame().map(|frame| {
                frame.map(|frame| NativeAsyncPayload::DataFrame {
                    frame,
                    control: stream.control(),
                })
            }),
        }
    }

    fn close(&self) -> Result<(), NativeStreamError> {
        match self {
            Self::Sql(stream) => stream.close_native(),
            Self::DataFrame(stream) => stream.close_native(),
        }
    }

    fn cancel(&self) -> Result<(), NativeStreamError> {
        match self {
            Self::Sql(stream) => stream.cancel_native(),
            Self::DataFrame(stream) => stream.cancel_native(),
        }
    }

    fn status_mapping(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Sql(stream) => stream.status_mapping(py).map(|status| status.into_any()),
            Self::DataFrame(stream) => stream.status_mapping(py).map(|status| status.into_any()),
        }
    }
}

enum NativeAsyncItem {
    Row(NativeAsyncPayload),
    Exhausted,
    Error(NativeStreamError),
}

enum NativeAsyncReceive {
    Pending,
    Item(Option<NativeAsyncPayload>),
}

#[derive(Default)]
struct BridgeState {
    cancelled: bool,
    /// `stop()` has taken responsibility for ending the source.  A worker that observes this
    /// must exit without racing a second source cancellation against the caller.
    source_stop_started: bool,
    discard_buffered: bool,
    consumer_waiting: bool,
    queued_rows: usize,
    ready_since: Option<Instant>,
    demand: usize,
    terminal: Option<BridgeTerminal>,
    worker_done: bool,
}

struct NativeAsyncBridgeInner {
    source: NativeAsyncSource,
    sender: SyncSender<NativeAsyncItem>,
    receiver: Mutex<Receiver<NativeAsyncItem>>,
    state: Mutex<BridgeState>,
    wake: Condvar,
    worker: Mutex<Option<JoinHandle<()>>>,
    prefetch_batches: usize,
    consumer_idle_timeout: Option<Duration>,
    single_thread: bool,
}

/// Internal Python object used by `alopex.asyncio`.
///
/// It is intentionally exported under an underscore-only name.  The documented async API is the
/// Python facade; this class is a transport-free ownership bridge, not a public client surface.
#[pyclass(name = "_NativeAsyncSqlResultStream", skip_from_py_object)]
pub(crate) struct PyNativeAsyncSqlResultStream {
    inner: Arc<NativeAsyncBridgeInner>,
}

impl PyNativeAsyncSqlResultStream {
    pub(crate) fn validate_options(
        prefetch_batches: usize,
        max_buffered_batches: usize,
        consumer_idle_timeout: Option<f64>,
    ) -> PyResult<()> {
        if max_buffered_batches == 0 {
            return Err(error::stream_error(
                "stream_resource_limit",
                "max_buffered_batches must be at least one",
            ));
        }
        if prefetch_batches > max_buffered_batches {
            return Err(error::stream_error(
                "stream_resource_limit",
                "prefetch_batches must be between zero and max_buffered_batches",
            ));
        }
        if consumer_idle_timeout.is_some_and(|seconds| !seconds.is_finite() || seconds < 0.0) {
            return Err(error::stream_error(
                "stream_timeout",
                "consumer_idle_timeout must be a finite non-negative number of seconds",
            ));
        }
        Ok(())
    }

    pub(crate) fn new(
        stream: PySqlResultStream,
        thread_mode: ThreadMode,
        prefetch_batches: usize,
        max_buffered_batches: usize,
        consumer_idle_timeout: Option<f64>,
    ) -> PyResult<Self> {
        Self::from_source(
            NativeAsyncSource::Sql(Arc::new(stream)),
            thread_mode,
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )
    }

    pub(crate) fn new_dataframe(
        stream: PyDataFrameStream,
        thread_mode: ThreadMode,
        prefetch_batches: usize,
        max_buffered_batches: usize,
        consumer_idle_timeout: Option<f64>,
    ) -> PyResult<Self> {
        Self::from_source(
            NativeAsyncSource::DataFrame(Arc::new(stream)),
            thread_mode,
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )
    }

    fn from_source(
        source: NativeAsyncSource,
        thread_mode: ThreadMode,
        prefetch_batches: usize,
        max_buffered_batches: usize,
        consumer_idle_timeout: Option<f64>,
    ) -> PyResult<Self> {
        Self::validate_options(
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )?;
        let (sender, receiver) = mpsc::sync_channel(max_buffered_batches);
        let inner = Arc::new(NativeAsyncBridgeInner {
            source,
            sender,
            receiver: Mutex::new(receiver),
            state: Mutex::new(BridgeState::default()),
            wake: Condvar::new(),
            worker: Mutex::new(None),
            prefetch_batches,
            consumer_idle_timeout: consumer_idle_timeout.map(Duration::from_secs_f64),
            single_thread: thread_mode == ThreadMode::Single,
        });
        if !inner.single_thread {
            NativeAsyncBridgeInner::start_worker(&inner);
        }
        Ok(Self { inner })
    }

    fn stop(&self, close: bool) -> Result<(), NativeStreamError> {
        self.inner.stop(close)
    }

    fn status_mapping(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let status = self.inner.source.status_mapping(py)?;
        let terminal = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("native async stream state lock poisoned"))?
            .terminal
            .clone();
        if let Some(BridgeTerminal::Error(error)) = terminal {
            let status = status.bind(py);
            match error.code {
                "stream_timeout" => status.set_item("terminal", "timed_out")?,
                "stream_closed" => status.set_item("terminal", "closed")?,
                "stream_cancelled" => status.set_item("terminal", "cancelled")?,
                "stream_resource_limit" => status.set_item("terminal", "resource_limit")?,
                _ => status.set_item("terminal", "failed")?,
            }
        }
        Ok(status.into_any())
    }
}

impl NativeAsyncBridgeInner {
    fn begin_consumer_wait(&self) -> Result<(), NativeStreamError> {
        let mut state = self.state.lock().map_err(|_| NativeStreamError {
            code: "stream_failure",
            message: "native async stream state lock poisoned".to_string(),
        })?;
        if state.consumer_waiting {
            return Err(NativeStreamError {
                code: "stream_busy",
                message: "only one __anext__ call may be in flight".to_string(),
            });
        }
        if let Some(terminal) = observable_terminal(&state) {
            return terminal_to_result(terminal);
        }
        state.consumer_waiting = true;
        if !self.single_thread && self.prefetch_batches == 0 {
            state.demand = state.demand.saturating_add(1);
        }
        self.wake.notify_all();
        Ok(())
    }

    fn end_consumer_wait(
        &self,
        consumed_row: bool,
    ) -> Result<Option<BridgeTerminal>, NativeStreamError> {
        let mut state = self.state.lock().map_err(|_| NativeStreamError {
            code: "stream_failure",
            message: "native async stream state lock poisoned".to_string(),
        })?;
        state.consumer_waiting = false;
        if consumed_row {
            state.queued_rows = state.queued_rows.saturating_sub(1);
            if state.queued_rows == 0 {
                state.ready_since = None;
            }
        }
        let terminal = if state.discard_buffered {
            state.terminal.clone()
        } else {
            None
        };
        self.wake.notify_all();
        Ok(terminal)
    }

    fn next_native(&self) -> Result<Option<NativeAsyncPayload>, NativeStreamError> {
        self.begin_consumer_wait()?;

        if self.single_thread {
            let result = self.source.next();
            let terminal = self.end_consumer_wait(false)?;
            if let Some(terminal) = terminal {
                return terminal_to_result(terminal).map(|_| None);
            }
            return result;
        }

        let item = self
            .receiver
            .lock()
            .map_err(|_| NativeStreamError {
                code: "stream_failure",
                message: "native async stream receiver lock poisoned".to_string(),
            })?
            .recv()
            .map_err(|_| NativeStreamError {
                code: "stream_failure",
                message: "native async stream worker stopped without a terminal outcome"
                    .to_string(),
            })?;

        let consumed_row = matches!(item, NativeAsyncItem::Row(_));
        let forced_terminal = self.end_consumer_wait(consumed_row)?;
        if let Some(terminal) = forced_terminal {
            return terminal_to_result(terminal).map(|_| None);
        }
        match item {
            NativeAsyncItem::Row(row) => Ok(Some(row)),
            NativeAsyncItem::Exhausted => Ok(None),
            NativeAsyncItem::Error(error) => Err(error),
        }
    }

    /// Poll once without blocking the caller's event-loop thread.
    ///
    /// The native worker remains responsible for source advancement and bounded buffering.  A
    /// demand-driven bridge records demand before returning `Pending`, so a later poll observes
    /// either the next Rust payload or its stable terminal outcome.
    fn try_next_native(&self) -> Result<NativeAsyncReceive, NativeStreamError> {
        if self.single_thread {
            return self.next_native().map(NativeAsyncReceive::Item);
        }
        self.begin_consumer_wait()?;
        let item = self
            .receiver
            .lock()
            .map_err(|_| NativeStreamError {
                code: "stream_failure",
                message: "native async stream receiver lock poisoned".to_string(),
            })?
            .try_recv();
        match item {
            Ok(item) => {
                let consumed_row = matches!(item, NativeAsyncItem::Row(_));
                let forced_terminal = self.end_consumer_wait(consumed_row)?;
                if let Some(terminal) = forced_terminal {
                    return terminal_to_result(terminal).map(|_| NativeAsyncReceive::Item(None));
                }
                match item {
                    NativeAsyncItem::Row(row) => Ok(NativeAsyncReceive::Item(Some(row))),
                    NativeAsyncItem::Exhausted => Ok(NativeAsyncReceive::Item(None)),
                    NativeAsyncItem::Error(error) => Err(error),
                }
            }
            Err(TryRecvError::Empty) => {
                if let Some(terminal) = self.end_consumer_wait(false)? {
                    return terminal_to_result(terminal).map(|_| NativeAsyncReceive::Item(None));
                }
                Ok(NativeAsyncReceive::Pending)
            }
            Err(TryRecvError::Disconnected) => {
                let _ = self.end_consumer_wait(false)?;
                Err(NativeStreamError {
                    code: "stream_failure",
                    message: "native async stream worker stopped without a terminal outcome"
                        .to_string(),
                })
            }
        }
    }

    fn stop(&self, close: bool) -> Result<(), NativeStreamError> {
        let terminal = BridgeTerminal::Error(NativeStreamError {
            code: if close {
                "stream_closed"
            } else {
                "stream_cancelled"
            },
            message: if close {
                "stream is closed".to_string()
            } else {
                "stream was cancelled".to_string()
            },
        });
        {
            let mut state = self.state.lock().map_err(|_| NativeStreamError {
                code: "stream_failure",
                message: "native async stream state lock poisoned".to_string(),
            })?;
            if state.terminal.is_some() && !state.discard_buffered {
                return Ok(());
            }
            state.cancelled = true;
            state.source_stop_started = true;
            state.discard_buffered = true;
            state.terminal = Some(terminal.clone());
            self.wake.notify_all();
        }
        // First stop and join the worker. A cursor advance and cursor close both own the same
        // native source, so serializing them avoids racing a close against an in-flight read.
        self.join_worker();

        // Cursor terminal cleanup is native and idempotent. It occurs outside bridge locks.
        let result = if close {
            self.source.close()
        } else {
            self.source.cancel()
        };

        // Wake a consumer currently blocked in `recv`.  If the bounded buffer already contains
        // an obsolete row, the next consumer observes `discard_buffered` and returns the same
        // terminal instead of exposing it as a successful result.
        let _ = self.sender.try_send(NativeAsyncItem::Error(match terminal {
            BridgeTerminal::Error(error) => error,
            BridgeTerminal::Exhausted => unreachable!("stop always creates an error terminal"),
        }));
        result
    }

    fn join_worker(&self) {
        let handle = self.worker.lock().ok().and_then(|mut worker| worker.take());
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    fn start_worker(this: &Arc<Self>) {
        let worker_inner = Arc::clone(this);
        let handle = thread::spawn(move || worker_inner.run_worker());
        *this
            .worker
            .lock()
            .expect("native async worker lock poisoned") = Some(handle);
    }

    fn run_worker(self: Arc<Self>) {
        loop {
            if !self.wait_until_readable() {
                break;
            }
            let item = match self.source.next() {
                Ok(Some(row)) => NativeAsyncItem::Row(row),
                Ok(None) => NativeAsyncItem::Exhausted,
                Err(error) => NativeAsyncItem::Error(error),
            };
            let terminal = !matches!(item, NativeAsyncItem::Row(_));
            if !self.push_item(item) || terminal {
                break;
            }
        }

        let should_cancel = self
            .state
            .lock()
            .map(|state| state.cancelled && !state.source_stop_started)
            .unwrap_or(true);
        if should_cancel {
            let _ = self.source.cancel();
        }
        if let Ok(mut state) = self.state.lock() {
            state.worker_done = true;
            self.wake.notify_all();
        }
    }

    fn wait_until_readable(&self) -> bool {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return false,
        };
        loop {
            if state.cancelled {
                return false;
            }
            let may_read = if self.prefetch_batches == 0 {
                state.demand > 0
            } else {
                state.queued_rows < self.prefetch_batches
            };
            if may_read {
                if self.prefetch_batches == 0 {
                    state.demand -= 1;
                }
                return true;
            }
            if self.idle_expired(&mut state) {
                return false;
            }
            state = self.wait_for_progress(state);
        }
    }

    fn push_item(&self, mut item: NativeAsyncItem) -> bool {
        loop {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return false,
            };
            if state.cancelled {
                return false;
            }
            let terminal = match &item {
                NativeAsyncItem::Row(_) => None,
                NativeAsyncItem::Exhausted => Some(BridgeTerminal::Exhausted),
                NativeAsyncItem::Error(error) => Some(BridgeTerminal::Error(error.clone())),
            };
            match self.sender.try_send(item) {
                Ok(()) => {
                    if terminal.is_none() {
                        state.queued_rows += 1;
                        state.ready_since.get_or_insert_with(Instant::now);
                    } else {
                        state.terminal = terminal;
                    }
                    self.wake.notify_all();
                    return true;
                }
                Err(TrySendError::Full(returned)) => {
                    item = returned;
                    if self.idle_expired(&mut state) {
                        return false;
                    }
                    let state = self.wait_for_progress(state);
                    drop(state);
                }
                Err(TrySendError::Disconnected(_)) => return false,
            }
        }
    }

    fn wait_for_progress<'a>(
        &self,
        state: std::sync::MutexGuard<'a, BridgeState>,
    ) -> std::sync::MutexGuard<'a, BridgeState> {
        let wait = self
            .consumer_idle_timeout
            .and_then(|idle| {
                state
                    .ready_since
                    .map(|ready| idle.saturating_sub(ready.elapsed()))
            })
            .filter(|duration| !duration.is_zero())
            .unwrap_or_else(|| Duration::from_millis(20));
        let (state, _) = self
            .wake
            .wait_timeout(state, wait)
            .expect("native async stream state lock poisoned while waiting");
        state
    }

    fn idle_expired(&self, state: &mut BridgeState) -> bool {
        let Some(timeout) = self.consumer_idle_timeout else {
            return false;
        };
        let Some(ready_since) = state.ready_since else {
            return false;
        };
        if state.consumer_waiting || ready_since.elapsed() < timeout {
            return false;
        }
        state.cancelled = true;
        state.discard_buffered = true;
        state.terminal = Some(BridgeTerminal::Error(NativeStreamError {
            code: "stream_timeout",
            message: "stream consumer idle timeout elapsed".to_string(),
        }));
        self.wake.notify_all();
        true
    }
}

fn observable_terminal(state: &BridgeState) -> Option<BridgeTerminal> {
    if state.discard_buffered || state.queued_rows == 0 {
        state.terminal.clone()
    } else {
        None
    }
}

fn terminal_to_result(terminal: BridgeTerminal) -> Result<(), NativeStreamError> {
    match terminal {
        BridgeTerminal::Exhausted => Ok(()),
        BridgeTerminal::Error(error) => Err(error),
    }
}

#[pymethods]
impl PyNativeAsyncSqlResultStream {
    /// Block only a native worker/executor thread while waiting for one bounded Rust row.
    /// Python dictionary conversion occurs after `detach` returns and no native stream lock is
    /// retained across that conversion.
    fn next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        match py.detach(move || inner.next_native()) {
            Ok(Some(row)) => row.into_py(py),
            Ok(None) => Err(PyStopIteration::new_err(())),
            Err(error) => Err(error.into_py_err()),
        }
    }

    /// Receive one Rust payload on an executor thread without constructing a Python row there.
    fn next_raw(&self, py: Python<'_>) -> PyResult<PyNativeAsyncPayload> {
        let inner = Arc::clone(&self.inner);
        match py.detach(move || inner.next_native()) {
            Ok(Some(payload)) => Ok(PyNativeAsyncPayload {
                payload: Some(payload),
            }),
            Ok(None) => Err(PyStopIteration::new_err(())),
            Err(error) => Err(error.into_py_err()),
        }
    }

    /// Poll one native payload without releasing the GIL or creating a Python row.
    fn poll_next_raw(&self) -> PyResult<PyNativeAsyncPayload> {
        match self.inner.try_next_native() {
            Ok(NativeAsyncReceive::Pending) => Err(error::stream_error(
                "stream_pending",
                "native async stream has no ready payload",
            )),
            Ok(NativeAsyncReceive::Item(Some(payload))) => Ok(PyNativeAsyncPayload {
                payload: Some(payload),
            }),
            Ok(NativeAsyncReceive::Item(None)) => Err(PyStopIteration::new_err(())),
            Err(error) => Err(error.into_py_err()),
        }
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.stop(true))
            .map_err(NativeStreamError::into_py_err)
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.stop(false))
            .map_err(NativeStreamError::into_py_err)
    }

    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.status_mapping(py)
    }
}

impl Drop for PyNativeAsyncSqlResultStream {
    fn drop(&mut self) {
        let _ = self.stop(false);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
    use pyo3::Python;

    use super::PyNativeAsyncSqlResultStream;
    use crate::embedded::stream::{PySqlResultStream, StreamLeaseRegistry};
    use crate::embedded::thread_mode::{DatabaseControl, ThreadMode};

    fn error_code(py: Python<'_>, error: pyo3::PyErr) -> String {
        error
            .value(py)
            .getattr("code")
            .unwrap()
            .extract::<String>()
            .unwrap()
    }

    #[test]
    fn native_worker_prefetches_bounded_rows_without_python_values() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let database = alopex_embedded::Database::new();
            database
                .execute_sql("CREATE TABLE async_rows (id INTEGER PRIMARY KEY)")
                .unwrap();
            database
                .execute_sql("INSERT INTO async_rows (id) VALUES (1), (2)")
                .unwrap();
            let plan = alopex_embedded::OwnedSqlStreamPlan::preflight(
                &database,
                "SELECT id FROM async_rows",
            )
            .unwrap();
            let stream = PySqlResultStream::open_database(
                &database,
                Arc::new(DatabaseControl::new(ThreadMode::Multi)),
                &StreamLeaseRegistry::default(),
                plan,
                None,
                None,
            )
            .unwrap();
            let stream =
                PyNativeAsyncSqlResultStream::new(stream, ThreadMode::Multi, 1, 1, None).unwrap();
            std::thread::sleep(Duration::from_millis(10));
            let mut row = stream.poll_next_raw().unwrap();
            let row = row.deliver_python(py).unwrap();
            let row = row.bind(py).cast::<PyDict>().unwrap();
            assert_eq!(
                row.get_item("id")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1
            );
            assert!(stream.next(py).is_ok());
            assert!(stream
                .next(py)
                .unwrap_err()
                .is_instance_of::<pyo3::exceptions::PyStopIteration>(py));
        });
    }

    #[test]
    fn native_worker_idle_timeout_discards_ready_row_and_is_repeatable() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let database = alopex_embedded::Database::new();
            let plan =
                alopex_embedded::OwnedSqlStreamPlan::preflight(&database, "SELECT 1 AS value")
                    .unwrap();
            let stream = PySqlResultStream::open_database(
                &database,
                Arc::new(DatabaseControl::new(ThreadMode::Multi)),
                &StreamLeaseRegistry::default(),
                plan,
                None,
                None,
            )
            .unwrap();
            let stream =
                PyNativeAsyncSqlResultStream::new(stream, ThreadMode::Multi, 1, 1, Some(0.001))
                    .unwrap();
            std::thread::sleep(Duration::from_millis(20));
            let first = stream.next(py).unwrap_err();
            assert_eq!(error_code(py, first), "stream_timeout");
            let second = stream.next(py).unwrap_err();
            assert_eq!(error_code(py, second), "stream_timeout");
        });
    }

    #[test]
    fn native_bridge_classifies_a_second_inflight_consumer_as_stream_busy() {
        pyo3::Python::initialize();
        Python::attach(|_| {
            let database = alopex_embedded::Database::new();
            let plan =
                alopex_embedded::OwnedSqlStreamPlan::preflight(&database, "SELECT 1 AS value")
                    .unwrap();
            let stream = PySqlResultStream::open_database(
                &database,
                Arc::new(DatabaseControl::new(ThreadMode::Single)),
                &StreamLeaseRegistry::default(),
                plan,
                None,
                None,
            )
            .unwrap();
            let stream =
                PyNativeAsyncSqlResultStream::new(stream, ThreadMode::Single, 0, 1, None).unwrap();

            stream.inner.begin_consumer_wait().unwrap();
            let error = stream.inner.begin_consumer_wait().unwrap_err();
            assert_eq!(error.code, "stream_busy");
            stream.inner.end_consumer_wait(false).unwrap();
        });
    }
}
