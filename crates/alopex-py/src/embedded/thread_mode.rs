//! Database thread-mode and close-state guards.
//!
//! This module deliberately guards only the base `PyDatabase` boundary. Transaction, stream, and
//! DataFrame inheritance are added by their dedicated Phase 4 tasks; all of them will share this
//! same control object instead of inventing a second close or thread policy.

use std::sync::Mutex;
use std::thread::{self, ThreadId};

use pyo3::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::error;

/// Python-selectable access mode for an embedded-local database.
#[pyclass(name = "ThreadMode", frozen, skip_from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PyThreadMode {
    mode: ThreadMode,
}

/// Internal thread policy shared by database-derived handles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ThreadMode {
    /// The database may be accessed from multiple Python threads.
    #[default]
    Multi,
    /// The database may be accessed only from the creating Python thread.
    Single,
}

impl ThreadMode {
    /// Parse the documented Python literal, defaulting to multi-thread mode.
    pub(crate) fn parse(value: Option<&str>) -> PyResult<Self> {
        match value.unwrap_or("multi") {
            "multi" => Ok(Self::Multi),
            "single" => Ok(Self::Single),
            _ => Err(PyValueError::new_err(
                "thread_mode must be either 'multi' or 'single'",
            )),
        }
    }

    fn repr(self) -> &'static str {
        match self {
            Self::Multi => "ThreadMode.MULTI",
            Self::Single => "ThreadMode.SINGLE",
        }
    }
}

impl From<ThreadMode> for PyThreadMode {
    fn from(mode: ThreadMode) -> Self {
        Self { mode }
    }
}

#[pymethods]
impl PyThreadMode {
    #[classattr]
    const MULTI: Self = Self {
        mode: ThreadMode::Multi,
    };
    #[classattr]
    const SINGLE: Self = Self {
        mode: ThreadMode::Single,
    };

    pub(crate) fn __repr__(&self) -> &'static str {
        self.mode.repr()
    }

    fn __richcmp__(&self, other: PyRef<'_, PyThreadMode>, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.mode == other.mode),
            CompareOp::Ne => Ok(self.mode != other.mode),
            _ => Ok(false),
        }
    }

    fn __hash__(&self) -> isize {
        match self.mode {
            ThreadMode::Multi => 1,
            ThreadMode::Single => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatabaseState {
    Open,
    Closing,
    Closed,
}

/// Shared base control for a database and later database-derived Python objects.
///
/// The thread check always precedes a state or storage acquisition. Closing flips the state before
/// rollback cleanup, preventing any later code from acquiring a new database lease mid-close.
pub(crate) struct DatabaseControl {
    mode: ThreadMode,
    owner_thread: Option<ThreadId>,
    state: Mutex<DatabaseState>,
}

impl DatabaseControl {
    /// Construct a control that records the creator only in single-thread mode.
    pub(crate) fn new(mode: ThreadMode) -> Self {
        Self {
            mode,
            owner_thread: (mode == ThreadMode::Single).then(|| thread::current().id()),
            state: Mutex::new(DatabaseState::Open),
        }
    }

    /// Return the configured policy.
    pub(crate) fn thread_mode(&self) -> ThreadMode {
        self.mode
    }

    /// Reject wrong-thread access before any database/storage lock is attempted.
    pub(crate) fn check_thread(&self) -> PyResult<()> {
        if self
            .owner_thread
            .is_some_and(|owner| owner != thread::current().id())
        {
            return Err(error::thread_mode_violation());
        }
        Ok(())
    }

    /// Check that a new database operation may begin.
    pub(crate) fn ensure_open(&self) -> PyResult<()> {
        self.check_thread()?;
        match *self
            .state
            .lock()
            .map_err(|_| error::to_py_err("database control lock poisoned"))?
        {
            DatabaseState::Open => Ok(()),
            DatabaseState::Closing => Err(error::to_py_err("database is closing")),
            DatabaseState::Closed => Err(error::to_py_err("database is closed")),
        }
    }

    /// Check access from a native worker without constructing a Python exception.
    ///
    /// The asynchronous stream bridge needs to decide whether it may advance an owned cursor
    /// while it deliberately holds neither the GIL nor a Python value.  The normal public
    /// methods continue to use [`Self::ensure_open`] so their existing exception types are
    /// unchanged.  A single-thread handle is intentionally rejected here: its async facade
    /// advances on the owning event-loop thread instead of moving storage work to this worker.
    pub(crate) fn ensure_open_for_native_worker(&self) -> Result<(), &'static str> {
        if self
            .owner_thread
            .is_some_and(|owner| owner != thread::current().id())
        {
            return Err("thread_mode_violation");
        }
        match *self.state.lock().map_err(|_| "stream_failure")? {
            DatabaseState::Open => Ok(()),
            DatabaseState::Closing | DatabaseState::Closed => Err("stream_closed"),
        }
    }

    /// Move from open to closing and return whether cleanup must run.
    pub(crate) fn begin_close(&self) -> PyResult<bool> {
        self.check_thread()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("database control lock poisoned"))?;
        match *state {
            DatabaseState::Open => {
                *state = DatabaseState::Closing;
                Ok(true)
            }
            DatabaseState::Closing => {
                Err(error::to_py_err("database close is already in progress"))
            }
            DatabaseState::Closed => Ok(false),
        }
    }

    /// Mark close complete only after every tracked resource has released.
    pub(crate) fn finish_close(&self) -> PyResult<()> {
        self.check_thread()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("database control lock poisoned"))?;
        *state = DatabaseState::Closed;
        Ok(())
    }

    /// Restore `Open` when rollback cleanup fails so callers can retry close safely.
    pub(crate) fn reopen_after_close_failure(&self) -> PyResult<()> {
        self.check_thread()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("database control lock poisoned"))?;
        if *state == DatabaseState::Closing {
            *state = DatabaseState::Open;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{DatabaseControl, ThreadMode};

    #[test]
    fn single_thread_control_rejects_other_thread_before_open_access() {
        pyo3::Python::initialize();
        let control = Arc::new(DatabaseControl::new(ThreadMode::Single));
        assert!(control.ensure_open().is_ok());
        let other = control.clone();
        let rejected = std::thread::spawn(move || other.ensure_open().is_err())
            .join()
            .unwrap();
        assert!(rejected);
    }

    #[test]
    fn close_blocks_new_access_and_can_recover_after_cleanup_failure() {
        pyo3::Python::initialize();
        let control = DatabaseControl::new(ThreadMode::Multi);
        assert!(control.begin_close().unwrap());
        assert!(control.ensure_open().is_err());
        control.reopen_after_close_failure().unwrap();
        assert!(control.ensure_open().is_ok());
        assert!(control.begin_close().unwrap());
        control.finish_close().unwrap();
        assert!(control.ensure_open().is_err());
        assert!(!control.begin_close().unwrap());
    }
}
