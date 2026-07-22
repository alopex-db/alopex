//! Tagged local scan inputs for the embedded-only Python streaming surface.
//!
//! A `LocalScan` is deliberately not an SQL string and never accepts a Python callback. Each
//! variant is validated before it can open an owned cursor or a Phase 3 batch source.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alopex_dataframe::{DataFrameStream, LazyFrame, StreamOptions};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::embedded::async_stream::PyNativeAsyncSqlResultStream;
use crate::embedded::stream::{PySqlResultStream, StreamLeaseRegistry};
use crate::embedded::thread_mode::DatabaseControl;
use crate::error;
use crate::types::{
    streaming_dataframe_err, DataFrameStreamRegistry, PyDataFrameStream, PyLazyFrame,
};

#[derive(Clone)]
enum LocalScanKind {
    Table {
        name: String,
        projection: Option<Vec<String>>,
    },
    Csv(PathBuf),
    Parquet(PathBuf),
    ColumnarSegment(String),
    LazyFrame(LazyFrame),
}

/// The complete local-only scan input algebra accepted by `Database.query_stream`.
#[pyclass(name = "LocalScan", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyLocalScan {
    kind: LocalScanKind,
}

impl PyLocalScan {
    /// Preflight the transaction-isolated subset of `LocalScan` before a transaction lease is
    /// opened.  A table scan uses the same owned SQL plan as `execute_sql_stream`; external
    /// DataFrame sources intentionally do not claim visibility of transaction-local writes.
    pub(crate) fn transaction_sql(&self) -> PyResult<String> {
        let LocalScanKind::Table { name, projection } = &self.kind else {
            return Err(error::stream_error(
                "unsupported_streaming_scan",
                "Transaction.query_stream supports only LocalScan.table because external DataFrame sources are not transaction-isolated",
            ));
        };
        let sql = table_sql(name, projection.as_deref())?;
        Ok(sql)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_database(
        &self,
        py: Python<'_>,
        database: &alopex_embedded::Database,
        control: Arc<DatabaseControl>,
        registry: &StreamLeaseRegistry,
        dataframe_registry: &DataFrameStreamRegistry,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        match &self.kind {
            LocalScanKind::Table { name, projection } => {
                let sql = table_sql(name, projection.as_deref())?;
                let plan = alopex_embedded::OwnedSqlStreamPlan::preflight(database, &sql)
                    .map_err(error::embedded_err)?;
                Py::new(
                    py,
                    PySqlResultStream::open_database(
                        database,
                        control,
                        registry,
                        plan,
                        resource_limit_bytes,
                        timeout,
                    )?,
                )
                .map(|stream| stream.into_any())
            }
            LocalScanKind::Csv(path) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = LazyFrame::scan_csv(path)
                    .and_then(|plan| plan.collect_streaming(options))
                    .map_err(streaming_dataframe_err)?;
                open_dataframe_stream(py, stream, control, dataframe_registry, timeout)
            }
            LocalScanKind::Parquet(path) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = LazyFrame::scan_parquet(path)
                    .and_then(|plan| plan.collect_streaming(options))
                    .map_err(streaming_dataframe_err)?;
                open_dataframe_stream(py, stream, control, dataframe_registry, timeout)
            }
            LocalScanKind::LazyFrame(lazyframe) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = lazyframe
                    .clone()
                    .collect_streaming(options)
                    .map_err(streaming_dataframe_err)?;
                open_dataframe_stream(py, stream, control, dataframe_registry, timeout)
            }
            LocalScanKind::ColumnarSegment(segment_id) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = database
                    .stream_columnar_segment_v08_by_id(segment_id, options)
                    .map_err(error::embedded_err)?;
                open_dataframe_stream(py, stream, control, dataframe_registry, timeout)
            }
        }
    }

    /// Open one of the five canonical local scan variants through the native async bridge.
    ///
    /// The source-specific Phase 3 preflight remains exactly the same as the synchronous path;
    /// only the bounded Rust result handoff differs.  No Python callback or eager materialization
    /// is introduced here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_native_async_database(
        &self,
        database: &alopex_embedded::Database,
        control: Arc<DatabaseControl>,
        registry: &StreamLeaseRegistry,
        dataframe_registry: &DataFrameStreamRegistry,
        resource_limit_bytes: Option<usize>,
        timeout: Option<f64>,
        prefetch_batches: usize,
        max_buffered_batches: usize,
        consumer_idle_timeout: Option<f64>,
    ) -> PyResult<PyNativeAsyncSqlResultStream> {
        PyNativeAsyncSqlResultStream::validate_options(
            prefetch_batches,
            max_buffered_batches,
            consumer_idle_timeout,
        )?;
        let thread_mode = control.thread_mode();
        match &self.kind {
            LocalScanKind::Table { name, projection } => {
                let sql = table_sql(name, projection.as_deref())?;
                let plan = alopex_embedded::OwnedSqlStreamPlan::preflight(database, &sql)
                    .map_err(error::embedded_err)?;
                let stream = PySqlResultStream::open_database(
                    database,
                    control,
                    registry,
                    plan,
                    resource_limit_bytes,
                    timeout,
                )?;
                PyNativeAsyncSqlResultStream::new(
                    stream,
                    thread_mode,
                    prefetch_batches,
                    max_buffered_batches,
                    consumer_idle_timeout,
                )
            }
            LocalScanKind::Csv(path) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = LazyFrame::scan_csv(path)
                    .and_then(|plan| plan.collect_streaming(options))
                    .map_err(streaming_dataframe_err)?;
                PyNativeAsyncSqlResultStream::new_dataframe(
                    PyDataFrameStream::with_control_and_registry(
                        stream,
                        control,
                        dataframe_registry,
                        timeout,
                    )?,
                    thread_mode,
                    prefetch_batches,
                    max_buffered_batches,
                    consumer_idle_timeout,
                )
            }
            LocalScanKind::Parquet(path) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = LazyFrame::scan_parquet(path)
                    .and_then(|plan| plan.collect_streaming(options))
                    .map_err(streaming_dataframe_err)?;
                PyNativeAsyncSqlResultStream::new_dataframe(
                    PyDataFrameStream::with_control_and_registry(
                        stream,
                        control,
                        dataframe_registry,
                        timeout,
                    )?,
                    thread_mode,
                    prefetch_batches,
                    max_buffered_batches,
                    consumer_idle_timeout,
                )
            }
            LocalScanKind::LazyFrame(lazyframe) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = lazyframe
                    .clone()
                    .collect_streaming(options)
                    .map_err(streaming_dataframe_err)?;
                PyNativeAsyncSqlResultStream::new_dataframe(
                    PyDataFrameStream::with_control_and_registry(
                        stream,
                        control,
                        dataframe_registry,
                        timeout,
                    )?,
                    thread_mode,
                    prefetch_batches,
                    max_buffered_batches,
                    consumer_idle_timeout,
                )
            }
            LocalScanKind::ColumnarSegment(segment_id) => {
                let options = dataframe_options(resource_limit_bytes)?;
                validate_timeout(timeout)?;
                let stream = database
                    .stream_columnar_segment_v08_by_id(segment_id, options)
                    .map_err(error::embedded_err)?;
                PyNativeAsyncSqlResultStream::new_dataframe(
                    PyDataFrameStream::with_control_and_registry(
                        stream,
                        control,
                        dataframe_registry,
                        timeout,
                    )?,
                    thread_mode,
                    prefetch_batches,
                    max_buffered_batches,
                    consumer_idle_timeout,
                )
            }
        }
    }
}

#[pymethods]
impl PyLocalScan {
    /// Construct a table scan with an optional finite list of plain column identifiers.
    #[staticmethod]
    #[pyo3(signature = (name, projection = None, predicate = None))]
    fn table(
        name: String,
        projection: Option<Vec<String>>,
        predicate: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        validate_identifier(&name)?;
        if let Some(projection) = &projection {
            for column in projection {
                validate_identifier(column)?;
            }
        }
        if predicate.is_some_and(|predicate| !predicate.is_none()) {
            return Err(error::stream_error(
                "unsupported_streaming_scan",
                "LocalScan.table predicate must use the future typed RowPredicate surface; Python callbacks and SQL fragments are not accepted",
            ));
        }
        Ok(Self {
            kind: LocalScanKind::Table { name, projection },
        })
    }

    /// Construct a Phase 3 CSV batch source scan.
    #[staticmethod]
    #[pyo3(signature = (path, options = None))]
    fn csv(path: String, options: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        reject_options(options, "CSV")?;
        Ok(Self {
            kind: LocalScanKind::Csv(PathBuf::from(path)),
        })
    }

    /// Construct a Phase 3 Parquet batch source scan.
    #[staticmethod]
    #[pyo3(signature = (path, options = None))]
    fn parquet(path: String, options: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        reject_options(options, "Parquet")?;
        Ok(Self {
            kind: LocalScanKind::Parquet(PathBuf::from(path)),
        })
    }

    /// Construct a V08 columnar segment scan descriptor.
    #[staticmethod]
    fn columnar_segment(segment_id: String) -> PyResult<Self> {
        if segment_id.is_empty() {
            return Err(error::stream_error(
                "unsupported_streaming_scan",
                "columnar segment id must not be empty",
            ));
        }
        Ok(Self {
            kind: LocalScanKind::ColumnarSegment(segment_id),
        })
    }

    /// Construct a scan from a previously created local lazy plan.
    #[staticmethod]
    fn lazyframe(lazyframe: PyRef<'_, PyLazyFrame>) -> PyResult<Self> {
        Ok(Self {
            kind: LocalScanKind::LazyFrame(lazyframe.clone_inner()?),
        })
    }
}

fn reject_options(options: Option<Bound<'_, PyAny>>, source: &str) -> PyResult<()> {
    if options.is_some_and(|options| !options.is_none()) {
        return Err(error::stream_error(
            "unsupported_streaming_scan",
            format!("{source} LocalScan options are not part of the v0.8 local scan contract"),
        ));
    }
    Ok(())
}

fn table_sql(name: &str, projection: Option<&[String]>) -> PyResult<String> {
    validate_identifier(name)?;
    let projection = match projection {
        None => "*".to_string(),
        Some([]) => {
            return Err(error::stream_error(
                "unsupported_streaming_scan",
                "LocalScan.table projection must not be empty",
            ));
        }
        Some(columns) => columns
            .iter()
            .map(|column| {
                validate_identifier(column)?;
                Ok(column.as_str())
            })
            .collect::<PyResult<Vec<_>>>()?
            .join(", "),
    };
    Ok(format!("SELECT {projection} FROM {name}"))
}

fn validate_identifier(identifier: &str) -> PyResult<()> {
    let mut bytes = identifier.bytes();
    let Some(first) = bytes.next() else {
        return Err(error::stream_error(
            "unsupported_streaming_scan",
            "local table and column identifiers must not be empty",
        ));
    };
    if !(first.is_ascii_alphabetic() || first == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(error::stream_error(
            "unsupported_streaming_scan",
            "LocalScan accepts only unquoted ASCII table and column identifiers",
        ));
    }
    Ok(())
}

fn dataframe_options(resource_limit_bytes: Option<usize>) -> PyResult<StreamOptions> {
    let resource_limit_bytes = resource_limit_bytes
        .map(|bytes| {
            u64::try_from(bytes).map_err(|_| {
                error::stream_error(
                    "stream_resource_limit",
                    "resource_limit_bytes exceeds the supported range",
                )
            })
        })
        .transpose()?;
    PyLazyFrame::options(resource_limit_bytes, None)
}

fn validate_timeout(timeout: Option<f64>) -> PyResult<()> {
    match timeout {
        Some(seconds) if !seconds.is_finite() || seconds < 0.0 => Err(error::stream_error(
            "stream_timeout",
            "timeout must be a finite non-negative number of seconds",
        )),
        Some(seconds)
            if Instant::now()
                .checked_add(Duration::from_secs_f64(seconds))
                .is_none() =>
        {
            Err(error::stream_error(
                "stream_timeout",
                "timeout is too large",
            ))
        }
        _ => Ok(()),
    }
}

fn open_dataframe_stream(
    py: Python<'_>,
    stream: DataFrameStream,
    control: Arc<DatabaseControl>,
    registry: &DataFrameStreamRegistry,
    timeout: Option<f64>,
) -> PyResult<Py<PyAny>> {
    Py::new(
        py,
        PyDataFrameStream::with_control_and_registry(stream, control, registry, timeout)?,
    )
    .map(|stream| stream.into_any())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pyo3::types::{PyAnyMethods, PyTypeMethods};
    use pyo3::Python;

    use super::{table_sql, LocalScanKind, PyLocalScan};
    use crate::embedded::stream::StreamLeaseRegistry;
    use crate::embedded::thread_mode::{DatabaseControl, ThreadMode};
    use crate::types::DataFrameStreamRegistry;

    fn error_code(py: Python<'_>, error: pyo3::PyErr) -> String {
        error
            .value(py)
            .getattr("code")
            .unwrap()
            .extract::<String>()
            .unwrap()
    }

    #[test]
    fn table_scan_sql_is_identifier_safe_and_never_accepts_sql_fragments() {
        assert_eq!(
            table_sql("users", Some(&["id".to_string(), "name".to_string()])).unwrap(),
            "SELECT id, name FROM users"
        );
        assert!(table_sql("users; DROP TABLE users", None).is_err());
        assert!(table_sql("users", Some(&["id + 1".to_string()])).is_err());
        let _ = std::any::TypeId::of::<PyLocalScan>();
    }

    #[test]
    fn csv_and_lazyframe_scans_open_bounded_dataframe_streams_and_observe_registry_close() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("local-scan.csv");
        std::fs::write(&path, "value\n1\n2\n").unwrap();

        pyo3::Python::initialize();
        Python::attach(|py| {
            let database = alopex_embedded::Database::new();
            let control = Arc::new(DatabaseControl::new(ThreadMode::Multi));
            let sql_registry = StreamLeaseRegistry::default();

            for scan in [
                PyLocalScan::csv(path.to_string_lossy().into_owned(), None).unwrap(),
                PyLocalScan {
                    kind: LocalScanKind::LazyFrame(
                        alopex_dataframe::LazyFrame::scan_csv(&path).unwrap(),
                    ),
                },
            ] {
                let dataframe_registry = DataFrameStreamRegistry::default();
                let stream = scan
                    .open_database(
                        py,
                        &database,
                        control.clone(),
                        &sql_registry,
                        &dataframe_registry,
                        Some(64 * 1024),
                        None,
                    )
                    .unwrap();
                let stream = stream.bind(py);
                assert_eq!(stream.get_type().name().unwrap(), "DataFrameStream");
                let batch = stream.call_method0("__next__").unwrap();
                assert_eq!(
                    batch
                        .call_method0("height")
                        .unwrap()
                        .extract::<usize>()
                        .unwrap(),
                    2
                );

                dataframe_registry.close_all().unwrap();
                assert_eq!(
                    stream
                        .getattr("status")
                        .unwrap()
                        .get_item("terminal")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    "closed"
                );
                assert_eq!(
                    error_code(py, stream.call_method0("__next__").unwrap_err()),
                    "stream_closed"
                );
            }
        });
    }
}
