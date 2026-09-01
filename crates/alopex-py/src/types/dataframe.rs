use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use alopex_dataframe::expr::Scalar;
use alopex_dataframe::{
    col, concat as dataframe_concat, concat_str as dataframe_concat_str, lit,
    ConcatStrNullBehavior, DataFrame, DataFrameStream, Expr, LazyFrame, Series, StreamOptions,
};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, ListBuilder, StringArray,
    StringBuilder, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyTuple};

use crate::embedded::stream::NativeStreamError;
use crate::embedded::thread_mode::{DatabaseControl, ThreadMode};
use crate::error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Utf8,
    Int64,
    TimestampMicros,
    ListUtf8,
}

/// Python wrapper around the Rust alopex-dataframe API.
#[pyclass(name = "DataFrame", skip_from_py_object)]
#[derive(Clone)]
pub struct PyDataFrame {
    inner: DataFrame,
    control: Arc<DatabaseControl>,
}

impl PyDataFrame {
    pub(crate) fn new(inner: DataFrame) -> Self {
        Self::with_control(inner, Arc::new(DatabaseControl::new(ThreadMode::Multi)))
    }

    pub(crate) fn with_control(inner: DataFrame, control: Arc<DatabaseControl>) -> Self {
        Self { inner, control }
    }

    fn ensure_access(&self) -> PyResult<()> {
        self.control.ensure_open()
    }
}

#[pymethods]
impl PyDataFrame {
    #[new]
    #[pyo3(signature = (columns, schema = None))]
    fn py_new(columns: &Bound<'_, PyDict>, schema: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        dataframe_from_py(columns, schema).map(Self::new)
    }

    #[staticmethod]
    #[pyo3(signature = (columns, schema = None))]
    fn from_columns(
        columns: &Bound<'_, PyDict>,
        schema: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        dataframe_from_py(columns, schema).map(Self::new)
    }

    fn height(&self) -> PyResult<usize> {
        self.ensure_access()?;
        Ok(self.inner.height())
    }

    fn width(&self) -> PyResult<usize> {
        self.ensure_access()?;
        Ok(self.inner.width())
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.ensure_access()?;
        dataframe_to_py_dict(py, &self.inner)
    }

    fn str(&self, column: &str) -> PyResult<PyStringNamespace> {
        self.ensure_access()?;
        Ok(PyStringNamespace {
            df: self.inner.clone(),
            column: column.to_string(),
            control: self.control.clone(),
        })
    }

    fn dt(&self, column: &str) -> PyResult<PyDatetimeNamespace> {
        self.ensure_access()?;
        Ok(PyDatetimeNamespace {
            df: self.inner.clone(),
            column: column.to_string(),
            control: self.control.clone(),
        })
    }

    fn list(&self, column: &str) -> PyResult<PyListNamespace> {
        self.ensure_access()?;
        Ok(PyListNamespace {
            df: self.inner.clone(),
            column: column.to_string(),
            control: self.control.clone(),
        })
    }

    fn explode(&self, column: &str) -> PyResult<Self> {
        self.ensure_access()?;
        self.inner
            .explode(column)
            .map(|frame| Self::with_control(frame, self.control.clone()))
            .map_err(dataframe_err)
    }

    fn implode(&self) -> PyResult<Self> {
        self.ensure_access()?;
        self.inner
            .implode()
            .map(|frame| Self::with_control(frame, self.control.clone()))
            .map_err(dataframe_err)
    }

    /// Create a lazy plan while retaining this DataFrame's thread/close policy.
    fn lazy(&self) -> PyResult<PyLazyFrame> {
        self.ensure_access()?;
        Ok(PyLazyFrame::with_control(
            LazyFrame::from_dataframe(self.inner.clone()),
            self.control.clone(),
        ))
    }
}

/// Python expression wrapper used only to construct Phase 3 lazy plans.
#[pyclass(name = "Expr", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyExpr {
    inner: Expr,
    control: Arc<DatabaseControl>,
}

impl PyExpr {
    fn new(inner: Expr) -> Self {
        Self {
            inner,
            control: Arc::new(DatabaseControl::new(ThreadMode::Multi)),
        }
    }

    fn clone_inner(&self) -> PyResult<Expr> {
        self.control.ensure_open()?;
        Ok(self.inner.clone())
    }

    fn binary(&self, rhs: &PyExpr, build: impl FnOnce(Expr, Expr) -> Expr) -> PyResult<Self> {
        Ok(Self {
            inner: build(self.clone_inner()?, rhs.clone_inner()?),
            control: self.control.clone(),
        })
    }
}

#[pymethods]
impl PyExpr {
    fn alias(&self, name: String) -> PyResult<Self> {
        Ok(Self {
            inner: self.clone_inner()?.alias(name),
            control: self.control.clone(),
        })
    }

    fn add(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::add)
    }

    fn sub(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::sub)
    }

    fn mul(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::mul)
    }

    fn div(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::div)
    }

    fn eq(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::eq)
    }

    fn neq(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::neq)
    }

    fn gt(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::gt)
    }

    fn lt(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::lt)
    }

    fn ge(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::ge)
    }

    fn le(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::le)
    }

    fn and_(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::and_)
    }

    fn or_(&self, rhs: &PyExpr) -> PyResult<Self> {
        self.binary(rhs, Expr::or_)
    }

    fn not_(&self) -> PyResult<Self> {
        Ok(Self {
            inner: self.clone_inner()?.not_(),
            control: self.control.clone(),
        })
    }
}

/// Python lazy DataFrame facade over Phase 3's compiler and batch sources.
#[pyclass(name = "LazyFrame", skip_from_py_object)]
#[derive(Clone)]
pub struct PyLazyFrame {
    inner: LazyFrame,
    control: Arc<DatabaseControl>,
}

impl PyLazyFrame {
    pub(crate) fn with_control(inner: LazyFrame, control: Arc<DatabaseControl>) -> Self {
        Self { inner, control }
    }

    pub(crate) fn clone_inner(&self) -> PyResult<LazyFrame> {
        self.control.ensure_open()?;
        Ok(self.inner.clone())
    }

    pub(crate) fn options(
        memory_limit_bytes: Option<u64>,
        batch_rows: Option<usize>,
    ) -> PyResult<StreamOptions> {
        let memory_limit_bytes = memory_limit_bytes.unwrap_or(64 * 1024 * 1024);
        let batch_rows = batch_rows.unwrap_or(8_192);
        let max_batches = NonZeroUsize::new(1).expect("one is non-zero");
        let batch_rows = NonZeroUsize::new(batch_rows).ok_or_else(|| {
            error::stream_error("stream_resource_limit", "batch_rows must be positive")
        })?;
        if memory_limit_bytes == 0 {
            return Err(error::stream_error(
                "stream_resource_limit",
                "resource_limit_bytes must be positive",
            ));
        }
        Ok(StreamOptions::new(
            memory_limit_bytes,
            max_batches,
            batch_rows,
        ))
    }
}

#[pymethods]
impl PyLazyFrame {
    #[staticmethod]
    fn scan_csv(path: &str) -> PyResult<Self> {
        LazyFrame::scan_csv(path)
            .map(|inner| {
                Self::with_control(inner, Arc::new(DatabaseControl::new(ThreadMode::Multi)))
            })
            .map_err(dataframe_err)
    }

    #[staticmethod]
    fn scan_parquet(path: &str) -> PyResult<Self> {
        LazyFrame::scan_parquet(path)
            .map(|inner| {
                Self::with_control(inner, Arc::new(DatabaseControl::new(ThreadMode::Multi)))
            })
            .map_err(dataframe_err)
    }

    #[staticmethod]
    fn from_dataframe(dataframe: &PyDataFrame) -> PyResult<Self> {
        dataframe.ensure_access()?;
        Ok(Self::with_control(
            LazyFrame::from_dataframe(dataframe.inner.clone()),
            dataframe.control.clone(),
        ))
    }

    /// Construct strict vertical lazy concatenation with Phase 3's schema and row-order rules.
    #[staticmethod]
    fn concat(inputs: Vec<PyRef<'_, PyLazyFrame>>) -> PyResult<Self> {
        if inputs.len() < 2 {
            return Err(dataframe_err(
                alopex_dataframe::DataFrameError::invalid_operation(
                    "concat requires at least two LazyFrame inputs",
                ),
            ));
        }
        let control = inputs[0].control.clone();
        control.ensure_open()?;
        let plans = inputs
            .iter()
            .map(|input| input.clone_inner())
            .collect::<PyResult<Vec<_>>>()?;
        LazyFrame::concat(plans)
            .map(|inner| Self::with_control(inner, control))
            .map_err(dataframe_err)
    }

    fn select(&self, exprs: Vec<PyRef<'_, PyExpr>>) -> PyResult<Self> {
        self.control.ensure_open()?;
        let exprs = expressions_from_py(exprs)?;
        Ok(Self::with_control(
            self.inner.clone().select(exprs),
            self.control.clone(),
        ))
    }

    fn filter(&self, predicate: PyRef<'_, PyExpr>) -> PyResult<Self> {
        self.control.ensure_open()?;
        Ok(Self::with_control(
            self.inner.clone().filter(predicate.clone_inner()?),
            self.control.clone(),
        ))
    }

    fn with_columns(&self, exprs: Vec<PyRef<'_, PyExpr>>) -> PyResult<Self> {
        self.control.ensure_open()?;
        let exprs = expressions_from_py(exprs)?;
        Ok(Self::with_control(
            self.inner.clone().with_columns(exprs),
            self.control.clone(),
        ))
    }

    #[pyo3(signature = (*, streaming = false, resource_limit_bytes = None, batch_rows = None))]
    fn collect(
        &self,
        py: Python<'_>,
        streaming: bool,
        resource_limit_bytes: Option<u64>,
        batch_rows: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        self.control.ensure_open()?;
        if streaming {
            let stream = self
                .inner
                .clone()
                .collect_streaming(Self::options(resource_limit_bytes, batch_rows)?)
                .map_err(dataframe_err)?;
            return Py::new(
                py,
                PyDataFrameStream::with_control(stream, self.control.clone()),
            )
            .map(|stream| stream.into_any());
        }
        self.inner
            .clone()
            .collect()
            .map(|frame| PyDataFrame::with_control(frame, self.control.clone()))
            .map_err(dataframe_err)
            .and_then(|frame| Py::new(py, frame).map(|frame| frame.into_any()))
    }
}

/// Construct a column-reference expression for lazy DataFrame plans.
#[pyfunction(name = "col")]
pub(crate) fn py_col(name: String) -> PyExpr {
    PyExpr::new(col(&name))
}

/// Construct a supported scalar literal expression for lazy DataFrame plans.
#[pyfunction(name = "lit")]
pub(crate) fn py_lit(value: Bound<'_, PyAny>) -> PyResult<PyExpr> {
    Ok(PyExpr::new(lit(py_scalar(&value)?)))
}

/// Strict vertical eager concatenation using Phase 3's schema/null/row-order semantics.
#[pyfunction(name = "concat")]
pub(crate) fn py_concat(inputs: Vec<PyRef<'_, PyDataFrame>>) -> PyResult<PyDataFrame> {
    if inputs.len() < 2 {
        return Err(dataframe_err(
            alopex_dataframe::DataFrameError::invalid_operation(
                "concat requires at least two DataFrame inputs",
            ),
        ));
    }
    let control = inputs[0].control.clone();
    control.ensure_open()?;
    let frames = inputs
        .iter()
        .map(|input| {
            input.ensure_access()?;
            Ok(input.inner.clone())
        })
        .collect::<PyResult<Vec<_>>>()?;
    dataframe_concat(frames)
        .map(|inner| PyDataFrame::with_control(inner, control))
        .map_err(dataframe_err)
}

/// Construct a Phase 3 row-wise string concatenation expression.
#[pyfunction(name = "concat_str")]
#[pyo3(signature = (inputs, separator = "", *, null_behavior = "propagate", null_value = None))]
pub(crate) fn py_concat_str(
    inputs: Vec<PyRef<'_, PyExpr>>,
    separator: &str,
    null_behavior: &str,
    null_value: Option<String>,
) -> PyResult<PyExpr> {
    if inputs.len() < 2 {
        return Err(dataframe_err(
            alopex_dataframe::DataFrameError::invalid_operation(
                "concat_str requires at least two Expr inputs",
            ),
        ));
    }
    let control = inputs[0].control.clone();
    control.ensure_open()?;
    let expressions = expressions_from_py(inputs)?;
    let null_behavior = match null_behavior {
        "propagate" => ConcatStrNullBehavior::Propagate,
        "ignore" => ConcatStrNullBehavior::Ignore,
        "replace" => ConcatStrNullBehavior::Replace(null_value.ok_or_else(|| {
            PyValueError::new_err("null_value is required when null_behavior='replace'")
        })?),
        _ => {
            return Err(PyValueError::new_err(
                "null_behavior must be 'propagate', 'ignore', or 'replace'",
            ));
        }
    };
    dataframe_concat_str(expressions, separator, null_behavior)
        .map(|inner| PyExpr { inner, control })
        .map_err(dataframe_err)
}

fn expressions_from_py(inputs: Vec<PyRef<'_, PyExpr>>) -> PyResult<Vec<Expr>> {
    inputs
        .into_iter()
        .map(|input| input.clone_inner())
        .collect()
}

fn py_scalar(value: &Bound<'_, PyAny>) -> PyResult<Scalar> {
    if value.is_none() {
        return Ok(Scalar::Null);
    }
    if let Ok(value) = value.extract::<bool>() {
        return Ok(Scalar::Boolean(value));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(Scalar::Int64(value));
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(Scalar::Float64(value));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(Scalar::Utf8(value));
    }
    Err(PyTypeError::new_err(
        "lit accepts None, bool, int, float, or str",
    ))
}

/// Python-visible terminal state, kept independently from the Phase 3 stream so a database
/// close can record its effect even after the source handle has been released.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PyDataFrameTerminal {
    Open,
    Exhausted,
    Closed,
    Cancelled,
    TimedOut,
    Failed { code: &'static str, message: String },
}

impl PyDataFrameTerminal {
    fn name(&self) -> String {
        match self {
            Self::Open => "open".to_string(),
            Self::Exhausted => "exhausted".to_string(),
            Self::Closed => "closed".to_string(),
            Self::Cancelled => "cancelled".to_string(),
            Self::TimedOut => "timed_out".to_string(),
            Self::Failed { code, .. } => format!("failed:{code}"),
        }
    }

    fn result(&self) -> PyResult<()> {
        match self {
            Self::Open => Ok(()),
            Self::Exhausted => Err(pyo3::exceptions::PyStopIteration::new_err(())),
            Self::Closed => Err(error::stream_error("stream_closed", "stream is closed")),
            Self::Cancelled => Err(error::stream_error(
                "stream_cancelled",
                "stream was cancelled",
            )),
            Self::TimedOut => Err(error::stream_error("stream_timeout", "stream timed out")),
            Self::Failed { code, message } => Err(error::stream_error(code, message)),
        }
    }
}

struct PyDataFrameStreamState {
    stream: DataFrameStream,
    terminal: PyDataFrameTerminal,
    batches_delivered: usize,
}

struct PyDataFrameStreamInner {
    state: Mutex<PyDataFrameStreamState>,
}

/// Database-owned weak registry for Phase 3 sources that are exposed through Python.
///
/// The registry deliberately tracks only database-derived streams. Standalone `LazyFrame`
/// values have no database lifetime to inherit and are cleaned up by their own drop path.
#[derive(Default)]
pub(crate) struct DataFrameStreamRegistry {
    streams: Mutex<Vec<Weak<PyDataFrameStreamInner>>>,
}

impl DataFrameStreamRegistry {
    fn register(&self, stream: &Arc<PyDataFrameStreamInner>) {
        let mut streams = self
            .streams
            .lock()
            .expect("DataFrame stream registry lock poisoned");
        streams.retain(|weak| weak.strong_count() > 0);
        streams.push(Arc::downgrade(stream));
    }

    /// Close every live source while `DatabaseControl` is in `Closing`, bypassing the public
    /// access gate but retaining the exact terminal effect for later observation.
    pub(crate) fn close_all(&self) -> PyResult<()> {
        let streams = {
            let mut guard = self
                .streams
                .lock()
                .map_err(|_| error::to_py_err("DataFrame stream registry lock poisoned"))?;
            let streams = guard.iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
            guard.retain(|weak| weak.strong_count() > 0);
            streams
        };
        for stream in streams {
            close_dataframe_stream(&stream)?;
        }
        Ok(())
    }
}

fn dataframe_failure(error: &alopex_dataframe::DataFrameError) -> PyDataFrameTerminal {
    let code = match error {
        alopex_dataframe::DataFrameError::ResourceLimitExceeded { .. }
        | alopex_dataframe::DataFrameError::StreamFailed {
            code: "resource_limit_exceeded",
            ..
        } => "stream_resource_limit",
        alopex_dataframe::DataFrameError::StreamClosed
        | alopex_dataframe::DataFrameError::StreamFailed {
            code: "stream_closed",
            ..
        } => "stream_closed",
        alopex_dataframe::DataFrameError::StreamCancelled
        | alopex_dataframe::DataFrameError::StreamFailed {
            code: "stream_cancelled",
            ..
        } => "stream_cancelled",
        _ => "stream_failure",
    };
    PyDataFrameTerminal::Failed {
        code,
        message: error.to_string(),
    }
}

/// Convert a Phase 3 preflight/open error into the stable Python stream envelope.
pub(crate) fn streaming_dataframe_err(error: alopex_dataframe::DataFrameError) -> PyErr {
    let terminal = dataframe_failure(&error);
    match terminal {
        PyDataFrameTerminal::Failed { code, message } => {
            if matches!(
                error,
                alopex_dataframe::DataFrameError::StreamingUnsupported { .. }
                    | alopex_dataframe::DataFrameError::StreamingRequiresMaterialization { .. }
            ) {
                error::stream_error("unsupported_streaming_scan", message)
            } else {
                error::stream_error(code, message)
            }
        }
        _ => unreachable!("DataFrame failures always produce a failure terminal"),
    }
}

fn close_dataframe_stream(inner: &Arc<PyDataFrameStreamInner>) -> PyResult<()> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| error::to_py_err("DataFrame stream state lock poisoned"))?;
    if state.terminal != PyDataFrameTerminal::Open {
        return Ok(());
    }
    match state.stream.close() {
        Ok(()) => {
            state.terminal = PyDataFrameTerminal::Closed;
            Ok(())
        }
        Err(source) => {
            state.terminal = dataframe_failure(&source);
            state.terminal.result()
        }
    }
}

/// Incremental Python adapter for Phase 3's bounded `DataFrameStream`.
#[pyclass(name = "DataFrameStream", skip_from_py_object)]
pub struct PyDataFrameStream {
    inner: Arc<PyDataFrameStreamInner>,
    control: Arc<DatabaseControl>,
    deadline: Option<Instant>,
}

impl PyDataFrameStream {
    pub(crate) fn with_control(inner: DataFrameStream, control: Arc<DatabaseControl>) -> Self {
        Self::new(inner, control, None, None)
    }

    pub(crate) fn with_control_and_registry(
        inner: DataFrameStream,
        control: Arc<DatabaseControl>,
        registry: &DataFrameStreamRegistry,
        timeout_seconds: Option<f64>,
    ) -> PyResult<Self> {
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
        Ok(Self::new(inner, control, Some(registry), deadline))
    }

    fn new(
        stream: DataFrameStream,
        control: Arc<DatabaseControl>,
        registry: Option<&DataFrameStreamRegistry>,
        deadline: Option<Instant>,
    ) -> Self {
        let inner = Arc::new(PyDataFrameStreamInner {
            state: Mutex::new(PyDataFrameStreamState {
                stream,
                terminal: PyDataFrameTerminal::Open,
                batches_delivered: 0,
            }),
        });
        if let Some(registry) = registry {
            registry.register(&inner);
        }
        Self {
            inner,
            control,
            deadline,
        }
    }

    fn timed_out(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    fn terminal_result(&self) -> PyResult<()> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("DataFrame stream state lock poisoned"))?;
        state.terminal.result()
    }

    fn timeout_locked(state: &mut PyDataFrameStreamState) -> PyResult<()> {
        if state.terminal == PyDataFrameTerminal::Open {
            if let Err(source) = state.stream.cancel() {
                state.terminal = dataframe_failure(&source);
            } else {
                state.terminal = PyDataFrameTerminal::TimedOut;
            }
        }
        state.terminal.result()
    }

    fn native_terminal_result(terminal: &PyDataFrameTerminal) -> Result<bool, NativeStreamError> {
        match terminal {
            PyDataFrameTerminal::Open => Ok(false),
            PyDataFrameTerminal::Exhausted => Ok(true),
            PyDataFrameTerminal::Closed => Err(NativeStreamError {
                code: "stream_closed",
                message: "stream is closed".to_string(),
            }),
            PyDataFrameTerminal::Cancelled => Err(NativeStreamError {
                code: "stream_cancelled",
                message: "stream was cancelled".to_string(),
            }),
            PyDataFrameTerminal::TimedOut => Err(NativeStreamError {
                code: "stream_timeout",
                message: "stream timed out".to_string(),
            }),
            PyDataFrameTerminal::Failed { code, message } => Err(NativeStreamError {
                code,
                message: message.clone(),
            }),
        }
    }

    /// Advance one Phase 3 batch without holding or creating a Python object.
    ///
    /// This is consumed only by the native async bridge.  The returned `DataFrame` remains a
    /// Rust value until the asyncio consumer receives it, so the worker never needs the GIL.
    pub(crate) fn next_native_frame(&self) -> Result<Option<DataFrame>, NativeStreamError> {
        self.control
            .ensure_open_for_native_worker()
            .map_err(|code| NativeStreamError {
                code,
                message: match code {
                    "thread_mode_violation" => {
                        "database is restricted to its creating thread".to_string()
                    }
                    "stream_closed" => "stream is closed".to_string(),
                    _ => "DataFrame stream control is unavailable".to_string(),
                },
            })?;
        let mut state = self.inner.state.lock().map_err(|_| NativeStreamError {
            code: "stream_failure",
            message: "DataFrame stream state lock poisoned".to_string(),
        })?;
        if Self::native_terminal_result(&state.terminal)? {
            return Ok(None);
        }
        if self.timed_out() {
            if let Err(source) = state.stream.cancel() {
                state.terminal = dataframe_failure(&source);
            } else {
                state.terminal = PyDataFrameTerminal::TimedOut;
            }
            return Self::native_terminal_result(&state.terminal).map(|_| None);
        }
        match state.stream.next_batch() {
            Ok(Some(frame)) if self.timed_out() => {
                drop(frame);
                if let Err(source) = state.stream.cancel() {
                    state.terminal = dataframe_failure(&source);
                } else {
                    state.terminal = PyDataFrameTerminal::TimedOut;
                }
                Self::native_terminal_result(&state.terminal).map(|_| None)
            }
            Ok(Some(frame)) => {
                state.batches_delivered += 1;
                Ok(Some(frame))
            }
            Ok(None) => {
                state.terminal = PyDataFrameTerminal::Exhausted;
                Ok(None)
            }
            Err(source) => {
                state.terminal = dataframe_failure(&source);
                Self::native_terminal_result(&state.terminal).map(|_| None)
            }
        }
    }

    pub(crate) fn close_native(&self) -> Result<(), NativeStreamError> {
        let mut state = self.inner.state.lock().map_err(|_| NativeStreamError {
            code: "stream_failure",
            message: "DataFrame stream state lock poisoned".to_string(),
        })?;
        if state.terminal != PyDataFrameTerminal::Open {
            return Ok(());
        }
        match state.stream.close() {
            Ok(()) => {
                state.terminal = PyDataFrameTerminal::Closed;
                Ok(())
            }
            Err(source) => {
                state.terminal = dataframe_failure(&source);
                Self::native_terminal_result(&state.terminal).map(|_| ())
            }
        }
    }

    pub(crate) fn cancel_native(&self) -> Result<(), NativeStreamError> {
        let mut state = self.inner.state.lock().map_err(|_| NativeStreamError {
            code: "stream_failure",
            message: "DataFrame stream state lock poisoned".to_string(),
        })?;
        if state.terminal != PyDataFrameTerminal::Open {
            return Ok(());
        }
        match state.stream.cancel() {
            Ok(()) => {
                state.terminal = PyDataFrameTerminal::Cancelled;
                Ok(())
            }
            Err(source) => {
                state.terminal = dataframe_failure(&source);
                Self::native_terminal_result(&state.terminal).map(|_| ())
            }
        }
    }

    pub(crate) fn status_mapping(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.status(py)
    }

    pub(crate) fn control(&self) -> Arc<DatabaseControl> {
        self.control.clone()
    }
}

#[pymethods]
impl PyDataFrameStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self) -> PyResult<PyDataFrame> {
        self.control.check_thread()?;
        self.terminal_result()?;
        self.control.ensure_open()?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("DataFrame stream state lock poisoned"))?;
        state.terminal.result()?;
        if self.timed_out() {
            return Self::timeout_locked(&mut state).and_then(|_| unreachable!());
        }
        match state.stream.next_batch() {
            Ok(Some(frame)) if self.timed_out() => {
                drop(frame);
                Self::timeout_locked(&mut state).and_then(|_| unreachable!())
            }
            Ok(Some(frame)) => {
                state.batches_delivered += 1;
                Ok(PyDataFrame::with_control(frame, self.control.clone()))
            }
            Ok(None) => {
                state.terminal = PyDataFrameTerminal::Exhausted;
                Err(pyo3::exceptions::PyStopIteration::new_err(()))
            }
            Err(source) => {
                state.terminal = dataframe_failure(&source);
                state.terminal.result().and_then(|_| unreachable!())
            }
        }
    }

    fn close(&self) -> PyResult<()> {
        self.control.check_thread()?;
        if self.terminal_result().is_err() {
            return Ok(());
        }
        self.control.ensure_open()?;
        close_dataframe_stream(&self.inner)
    }

    fn cancel(&self) -> PyResult<()> {
        self.control.check_thread()?;
        if self.terminal_result().is_err() {
            return Ok(());
        }
        self.control.ensure_open()?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("DataFrame stream state lock poisoned"))?;
        match state.stream.cancel() {
            Ok(()) => {
                state.terminal = PyDataFrameTerminal::Cancelled;
                Ok(())
            }
            Err(source) => {
                state.terminal = dataframe_failure(&source);
                state.terminal.result()
            }
        }
    }

    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.control.check_thread()?;
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| error::to_py_err("DataFrame stream state lock poisoned"))?;
        let status = PyDict::new(py);
        status.set_item("terminal", state.terminal.name())?;
        status.set_item("batches_delivered", state.batches_delivered)?;
        status.set_item(
            "resource_limit_bytes",
            state.stream.budget().memory_limit_bytes(),
        )?;
        status.set_item("resource_scope", "dataframe_batch")?;
        status.set_item("transaction_effect", "none")?;
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
        if self.terminal_result().is_ok() {
            self.close()?;
        }
        Ok(false)
    }
}

#[pyclass(name = "StringNamespace", skip_from_py_object)]
#[derive(Clone)]
pub struct PyStringNamespace {
    df: DataFrame,
    column: String,
    control: Arc<DatabaseControl>,
}

#[pymethods]
impl PyStringNamespace {
    #[pyo3(signature = (output = None))]
    fn to_lowercase(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().to_lowercase(), output)
    }

    #[pyo3(signature = (output = None))]
    fn to_uppercase(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().to_uppercase(), output)
    }

    #[pyo3(signature = (pattern, output = None))]
    fn contains(&self, pattern: &str, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().contains(pattern), output)
    }

    #[pyo3(signature = (pattern, replacement, output = None))]
    fn replace(
        &self,
        pattern: &str,
        replacement: &str,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(
            col(&self.column).str().replace(pattern, replacement),
            output,
        )
    }

    #[pyo3(signature = (chars = None, output = None))]
    fn strip_chars(&self, chars: Option<&str>, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().strip_chars(chars), output)
    }

    #[pyo3(signature = (separator, output = None))]
    fn split(&self, separator: &str, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().split(separator), output)
    }

    #[pyo3(signature = (output = None))]
    fn len_chars(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).str().len_chars(), output)
    }

    #[pyo3(signature = (pattern, capture_group = 1, output = None))]
    fn extract(
        &self,
        pattern: &str,
        capture_group: usize,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(
            col(&self.column).str().extract(pattern, capture_group),
            output,
        )
    }
}

impl PyStringNamespace {
    fn with_expr(
        &self,
        expr: alopex_dataframe::Expr,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.control.ensure_open()?;
        let name = output.unwrap_or(&self.column);
        self.df
            .with_columns(vec![expr.alias(name)])
            .map(|frame| PyDataFrame::with_control(frame, self.control.clone()))
            .map_err(dataframe_err)
    }
}

#[pyclass(name = "DatetimeNamespace", skip_from_py_object)]
#[derive(Clone)]
pub struct PyDatetimeNamespace {
    df: DataFrame,
    column: String,
    control: Arc<DatabaseControl>,
}

#[pymethods]
impl PyDatetimeNamespace {
    #[pyo3(signature = (output = None))]
    fn year(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().year(), output)
    }

    #[pyo3(signature = (output = None))]
    fn month(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().month(), output)
    }

    #[pyo3(signature = (output = None))]
    fn day(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().day(), output)
    }

    #[pyo3(signature = (output = None))]
    fn weekday(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().weekday(), output)
    }

    #[pyo3(signature = (output = None))]
    fn to_string(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).dt().to_string(), output)
    }

    #[pyo3(signature = (from_offset, to_offset, output = None))]
    fn convert_time_zone(
        &self,
        from_offset: &str,
        to_offset: &str,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(
            col(&self.column)
                .dt()
                .convert_time_zone(from_offset, to_offset),
            output,
        )
    }
}

impl PyDatetimeNamespace {
    fn with_expr(
        &self,
        expr: alopex_dataframe::Expr,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.control.ensure_open()?;
        let name = output.unwrap_or(&self.column);
        self.df
            .with_columns(vec![expr.alias(name)])
            .map(|frame| PyDataFrame::with_control(frame, self.control.clone()))
            .map_err(dataframe_err)
    }
}

#[pyclass(name = "ListNamespace", skip_from_py_object)]
#[derive(Clone)]
pub struct PyListNamespace {
    df: DataFrame,
    column: String,
    control: Arc<DatabaseControl>,
}

#[pymethods]
impl PyListNamespace {
    #[pyo3(signature = (separator, null_value = None, output = None))]
    fn join(
        &self,
        separator: &str,
        null_value: Option<&str>,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).list().join(separator, null_value), output)
    }

    #[pyo3(signature = (output = None))]
    fn len(&self, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).list().len(), output)
    }

    #[pyo3(signature = (value, output = None))]
    fn contains(&self, value: &str, output: Option<&str>) -> PyResult<PyDataFrame> {
        self.with_expr(col(&self.column).list().contains(value), output)
    }
}

impl PyListNamespace {
    fn with_expr(
        &self,
        expr: alopex_dataframe::Expr,
        output: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        self.control.ensure_open()?;
        let name = output.unwrap_or(&self.column);
        self.df
            .with_columns(vec![expr.alias(name)])
            .map(|frame| PyDataFrame::with_control(frame, self.control.clone()))
            .map_err(dataframe_err)
    }
}

fn dataframe_from_py(
    columns: &Bound<'_, PyDict>,
    schema: Option<&Bound<'_, PyDict>>,
) -> PyResult<DataFrame> {
    let mut series = Vec::with_capacity(columns.len());
    for (name, values) in columns.iter() {
        let name = name.extract::<String>()?;
        let kind = schema_kind(schema, &name)?.unwrap_or_else(|| infer_kind(&values));
        series.push(series_from_py(&name, &values, kind)?);
    }
    DataFrame::new(series).map_err(dataframe_err)
}

fn schema_kind(schema: Option<&Bound<'_, PyDict>>, name: &str) -> PyResult<Option<ColumnKind>> {
    let Some(schema) = schema else {
        return Ok(None);
    };
    let Some(value) = schema.get_item(name)? else {
        return Ok(None);
    };
    let value = value.extract::<String>()?;
    match value.as_str() {
        "utf8" | "str" | "string" => Ok(Some(ColumnKind::Utf8)),
        "int64" | "int" => Ok(Some(ColumnKind::Int64)),
        "timestamp_micros" | "datetime" => Ok(Some(ColumnKind::TimestampMicros)),
        "list_utf8" | "list[str]" | "list" => Ok(Some(ColumnKind::ListUtf8)),
        other => Err(PyValueError::new_err(format!(
            "unsupported DataFrame schema for column '{name}': {other}"
        ))),
    }
}

fn infer_kind(values: &Bound<'_, PyAny>) -> ColumnKind {
    let Ok(iter) = values.try_iter() else {
        return ColumnKind::Utf8;
    };
    for item in iter.flatten() {
        if item.is_none() {
            continue;
        }
        if item.cast::<PyList>().is_ok() || item.cast::<PyTuple>().is_ok() {
            return ColumnKind::ListUtf8;
        }
        if item.extract::<i64>().is_ok() {
            return ColumnKind::Int64;
        }
        return ColumnKind::Utf8;
    }
    ColumnKind::Utf8
}

fn series_from_py(name: &str, values: &Bound<'_, PyAny>, kind: ColumnKind) -> PyResult<Series> {
    let array: ArrayRef = match kind {
        ColumnKind::Utf8 => Arc::new(StringArray::from(py_utf8_values(values)?)),
        ColumnKind::Int64 => Arc::new(Int64Array::from(py_i64_values(values)?)),
        ColumnKind::TimestampMicros => {
            Arc::new(TimestampMicrosecondArray::from(py_i64_values(values)?))
        }
        ColumnKind::ListUtf8 => list_utf8_from_py(values)?,
    };
    Series::from_arrow(name, vec![array]).map_err(dataframe_err)
}

fn py_utf8_values(values: &Bound<'_, PyAny>) -> PyResult<Vec<Option<String>>> {
    let mut out = Vec::new();
    for item in values.try_iter()? {
        let item = item?;
        if item.is_none() {
            out.push(None);
        } else {
            out.push(Some(item.extract::<String>()?));
        }
    }
    Ok(out)
}

fn py_i64_values(values: &Bound<'_, PyAny>) -> PyResult<Vec<Option<i64>>> {
    let mut out = Vec::new();
    for item in values.try_iter()? {
        let item = item?;
        if item.is_none() {
            out.push(None);
        } else {
            out.push(Some(item.extract::<i64>()?));
        }
    }
    Ok(out)
}

fn list_utf8_from_py(values: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for item in values.try_iter()? {
        let item = item?;
        if item.is_none() {
            builder.append(false);
            continue;
        }
        for value in item.try_iter()? {
            let value = value?;
            if value.is_none() {
                builder.values().append_null();
            } else {
                builder.values().append_value(value.extract::<String>()?);
            }
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn dataframe_to_py_dict(py: Python<'_>, df: &DataFrame) -> PyResult<Py<PyDict>> {
    let out = PyDict::new(py);
    for series in df.columns() {
        let values = PyList::empty(py);
        for array in series.to_arrow() {
            values.call_method1("extend", (array_to_py_list(py, &array)?,))?;
        }
        out.set_item(series.name(), values)?;
    }
    Ok(out.unbind())
}

fn array_to_py_list(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    match array.data_type() {
        DataType::Utf8 => utf8_array_to_py(py, array),
        DataType::Boolean => bool_array_to_py(py, array),
        DataType::Int32 => int32_array_to_py(py, array),
        DataType::Int64 => int64_array_to_py(py, array),
        DataType::UInt64 => uint64_array_to_py(py, array),
        DataType::Timestamp(TimeUnit::Microsecond, _) => timestamp_array_to_py(py, array),
        DataType::List(field) if field.data_type() == &DataType::Utf8 => {
            list_utf8_array_to_py(py, array)
        }
        other => Err(error::to_py_err(format!(
            "unsupported DataFrame column type for Python conversion: {other}"
        ))),
    }
}

fn utf8_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<StringArray>(array, "StringArray")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn bool_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<BooleanArray>(array, "BooleanArray")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn int32_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<Int32Array>(array, "Int32Array")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn int64_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<Int64Array>(array, "Int64Array")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn uint64_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<UInt64Array>(array, "UInt64Array")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn timestamp_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<TimestampMicrosecondArray>(array, "TimestampMicrosecondArray")?;
    let values = PyList::empty(py);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            values.append(py.None())?;
        } else {
            values.append(array.value(idx))?;
        }
    }
    Ok(values.unbind())
}

fn list_utf8_array_to_py(py: Python<'_>, array: &ArrayRef) -> PyResult<Py<PyList>> {
    let array = downcast_array::<ListArray>(array, "ListArray")?;
    let values = PyList::empty(py);
    for row in 0..array.len() {
        if array.is_null(row) {
            values.append(py.None())?;
            continue;
        }
        let item_array = array.value(row);
        values.append(utf8_array_to_py(py, &item_array)?.bind(py))?;
    }
    Ok(values.unbind())
}

fn downcast_array<'a, T: 'static>(array: &'a ArrayRef, expected: &str) -> PyResult<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        error::to_py_err(format!(
            "internal DataFrame conversion error: expected {expected}, got {}",
            array.data_type()
        ))
    })
}

fn dataframe_err(err: alopex_dataframe::DataFrameError) -> PyErr {
    error::to_py_err(err)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use alopex_dataframe::{DataFrame, LazyFrame, Series, StreamOptions};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use pyo3::types::{PyAnyMethods, PyDictMethods, PyList, PyModule};
    use pyo3::{Py, Python};

    use super::{
        DataFrameStreamRegistry, DatabaseControl, PyDataFrame, PyDataFrameStream, ThreadMode,
    };

    fn expression_dataframe(values: Vec<i64>, labels: Vec<&str>) -> DataFrame {
        DataFrame::new(vec![
            Series::from_arrow("a", vec![Arc::new(Int64Array::from(values)) as ArrayRef]).unwrap(),
            Series::from_arrow(
                "left",
                vec![Arc::new(StringArray::from(labels.clone())) as ArrayRef],
            )
            .unwrap(),
            Series::from_arrow(
                "right",
                vec![Arc::new(StringArray::from(labels)) as ArrayRef],
            )
            .unwrap(),
        ])
        .unwrap()
    }

    fn error_code(py: Python<'_>, error: pyo3::PyErr) -> String {
        error
            .value(py)
            .getattr("code")
            .unwrap()
            .extract::<String>()
            .unwrap()
    }

    #[test]
    fn lazyframe_streams_csv_as_finite_python_dataframes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dataframe-stream.csv");
        std::fs::write(&path, "value\n1\n2\n").unwrap();

        pyo3::Python::initialize();
        Python::attach(|py| {
            let options = StreamOptions::new(
                64 * 1024,
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            );
            let stream = LazyFrame::scan_csv(&path)
                .unwrap()
                .collect_streaming(options)
                .unwrap();
            let stream = PyDataFrameStream::with_control(
                stream,
                Arc::new(DatabaseControl::new(ThreadMode::Multi)),
            );
            assert_eq!(stream.__next__().unwrap().height().unwrap(), 1);
            assert_eq!(stream.__next__().unwrap().height().unwrap(), 1);
            assert!(stream.__next__().is_err());
            let status = stream.status(py).unwrap();
            let status = status.bind(py);
            assert_eq!(
                status
                    .get_item("terminal")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "exhausted"
            );
            assert_eq!(
                status
                    .get_item("batches_delivered")
                    .unwrap()
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                2
            );
            stream.close().unwrap();
        });
    }

    #[test]
    fn database_close_registry_closes_a_dataframe_source_and_preserves_terminal_effect() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("close-registry.csv");
        std::fs::write(&path, "value\n1\n2\n").unwrap();

        pyo3::Python::initialize();
        Python::attach(|py| {
            let stream = LazyFrame::scan_csv(&path)
                .unwrap()
                .collect_streaming(StreamOptions::new(
                    64 * 1024,
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroUsize::new(1).unwrap(),
                ))
                .unwrap();
            let control = Arc::new(DatabaseControl::new(ThreadMode::Multi));
            let registry = DataFrameStreamRegistry::default();
            let stream = PyDataFrameStream::with_control_and_registry(
                stream,
                control.clone(),
                &registry,
                None,
            )
            .unwrap();

            assert_eq!(stream.__next__().unwrap().height().unwrap(), 1);
            assert!(control.begin_close().unwrap());
            registry.close_all().unwrap();
            control.finish_close().unwrap();

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
            assert_eq!(
                error_code(py, stream.__next__().err().expect("closed stream")),
                "stream_closed"
            );
            assert_eq!(
                error_code(py, stream.__next__().err().expect("repeat closed stream")),
                "stream_closed"
            );
            stream.close().unwrap();
            stream.cancel().unwrap();
        });
    }

    #[test]
    fn dataframe_stream_timeout_is_terminal_and_repeatable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("timeout.csv");
        std::fs::write(&path, "value\n1\n").unwrap();

        pyo3::Python::initialize();
        Python::attach(|py| {
            let stream = LazyFrame::scan_csv(&path)
                .unwrap()
                .collect_streaming(StreamOptions::default())
                .unwrap();
            let stream = PyDataFrameStream::with_control_and_registry(
                stream,
                Arc::new(DatabaseControl::new(ThreadMode::Multi)),
                &DataFrameStreamRegistry::default(),
                Some(0.0),
            )
            .unwrap();

            assert_eq!(
                error_code(py, stream.__next__().err().expect("timed out stream")),
                "stream_timeout"
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
                "timed_out"
            );
            assert_eq!(
                error_code(
                    py,
                    stream.__next__().err().expect("repeat timed out stream")
                ),
                "stream_timeout"
            );
        });
    }

    #[test]
    fn dataframe_expression_bindings_use_phase_three_semantics() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "_alopex_dataframe_test").unwrap();
            crate::types::register(py, &module).unwrap();

            let dataframe = Py::new(
                py,
                PyDataFrame::new(expression_dataframe(vec![1, 2, 3], vec!["x", "m", "p"])),
            )
            .unwrap();
            let col = module.getattr("col").unwrap();
            let lit = module.getattr("lit").unwrap();
            let a = col.call1(("a",)).unwrap();
            let one = lit.call1((1_i64,)).unwrap();
            let predicate = a.call_method1("gt", (one.clone(),)).unwrap();
            let next = a
                .call_method1("add", (one,))
                .unwrap()
                .call_method1("alias", ("next",))
                .unwrap();
            let left = col.call1(("left",)).unwrap();
            let right = col.call1(("right",)).unwrap();
            let label = module
                .getattr("concat_str")
                .unwrap()
                .call1((PyList::new(py, [&left, &right]).unwrap(), "-"))
                .unwrap()
                .call_method1("alias", ("label",))
                .unwrap();

            let lazy = dataframe.bind(py).call_method0("lazy").unwrap();
            let result = lazy
                .call_method1("filter", (predicate,))
                .unwrap()
                .call_method1("select", (PyList::new(py, [&next, &label]).unwrap(),))
                .unwrap()
                .call_method0("collect")
                .unwrap();
            let rows = result
                .call_method0("to_dict")
                .unwrap()
                .cast_into::<pyo3::types::PyDict>()
                .unwrap();
            assert_eq!(
                rows.get_item("next")
                    .unwrap()
                    .unwrap()
                    .extract::<Vec<i64>>()
                    .unwrap(),
                vec![3, 4]
            );
            assert_eq!(
                rows.get_item("label")
                    .unwrap()
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap(),
                vec!["m-m", "p-p"]
            );

            let second = Py::new(
                py,
                PyDataFrame::new(expression_dataframe(vec![4], vec!["q"])),
            )
            .unwrap();
            let combined = module
                .getattr("concat")
                .unwrap()
                .call1((PyList::new(py, [&dataframe, &second]).unwrap(),))
                .unwrap();
            assert_eq!(
                combined
                    .call_method0("height")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                4
            );
        });
    }
}
