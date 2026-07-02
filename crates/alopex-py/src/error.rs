#![allow(unused_doc_comments)]

use std::fmt::{self, Display};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::types::PyAnyMethods;
use pyo3::{PyErr, Python};

/// Python-visible base exception for Alopex bindings.
///
/// Exposed to Python as `alopex.AlopexError` in module initialization.
/// Used when an error does not map to `ValueError` or `RuntimeError`.
///
/// Examples:
///     >>> from alopex import AlopexError
///     >>> isinstance(AlopexError("message"), Exception)
///
/// Raises:
///     AlopexError: Raised when an operation fails with a generic error.
create_exception!(crate::error, PyAlopexError, PyException);

/// Public stable error codes for the Python bindings.
///
/// `ALOPEX-P` is already used by the SQL parser, so Python binding-specific
/// envelope codes use `ALOPEX-PY###` to avoid collisions across crates.
pub const ERROR_CODES: &[&str] = &[
    "ALOPEX-PY001",
    "ALOPEX-PY002",
    "ALOPEX-PY003",
    "ALOPEX-PY004",
    "ALOPEX-PY005",
    "ALOPEX-PY006",
    "ALOPEX-PY007",
    "ALOPEX-PY008",
    "ALOPEX-PY009",
    "ALOPEX-PY010",
    "ALOPEX-PY011",
    "ALOPEX-PY012",
    "ALOPEX-PY013",
    "ALOPEX-PY014",
    "ALOPEX-PY101",
    "ALOPEX-PY102",
    "ALOPEX-PY103",
    "ALOPEX-PY104",
    "ALOPEX-PY999",
];

const GENERIC_ERROR_CODE: &str = "ALOPEX-PY999";

/// Internal error enum for Alopex Python bindings.
///
/// Variants:
///     - CatalogNotFound, NamespaceNotFound, TableNotFound, ParentNotFound
///     - CatalogAlreadyExists, NamespaceAlreadyExists, TableExists
///     - WriteTargetNotFound, StorageLocationRequired, PrimaryKeyRequired
///     - UnsupportedFormat, PolarsNotInstalled, CloudAuthFailed, TypeConversionError
///
/// Exception mapping:
///     - *NotFound and ParentNotFound -> ValueError
///     - *AlreadyExists -> RuntimeError
///     - others -> AlopexError
///
/// Examples:
///     ```ignore
///     use crate::error::AlopexError;
///     let err = AlopexError::PolarsNotInstalled;
///     ```
///
/// Raises:
///     ValueError: For *NotFound and ParentNotFound variants when converted to PyErr.
///     RuntimeError: For *AlreadyExists variants when converted to PyErr.
///     AlopexError: For all other variants when converted to PyErr.
#[derive(Debug)]
pub enum AlopexError {
    CatalogNotFound(String),
    NamespaceNotFound(String),
    TableNotFound(String),
    ParentNotFound(String),
    CatalogAlreadyExists(String),
    NamespaceAlreadyExists(String),
    TableExists(String),
    WriteTargetNotFound(String),
    StorageLocationRequired,
    PrimaryKeyRequired,
    UnsupportedFormat(String),
    PolarsNotInstalled,
    CloudAuthFailed { provider: String, env_var: String },
    TypeConversionError { expected: String, actual: String },
}

impl Display for AlopexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlopexError::CatalogNotFound(name) => {
                write!(f, "カタログが見つかりません: {}", name)
            }
            AlopexError::NamespaceNotFound(name) => {
                write!(f, "ネームスペースが見つかりません: {}", name)
            }
            AlopexError::TableNotFound(name) => write!(f, "テーブルが見つかりません: {}", name),
            AlopexError::ParentNotFound(name) => {
                write!(f, "親リソースが見つかりません: {}", name)
            }
            AlopexError::CatalogAlreadyExists(name) => {
                write!(f, "カタログが既に存在します: {}", name)
            }
            AlopexError::NamespaceAlreadyExists(name) => {
                write!(f, "ネームスペースが既に存在します: {}", name)
            }
            AlopexError::TableExists(name) => write!(f, "テーブルが既に存在します: {}", name),
            AlopexError::WriteTargetNotFound(name) => {
                write!(f, "書き込み先テーブルが見つかりません: {}", name)
            }
            AlopexError::StorageLocationRequired => write!(f, "storage_location が必要です"),
            AlopexError::PrimaryKeyRequired => {
                write!(f, "delta_mode=\"merge\" には primary_key の指定が必要です")
            }
            AlopexError::UnsupportedFormat(format) => write!(
                f,
                "サポートされていないフォーマット: {}（v0.4.0 では PARQUET のみサポート）",
                format
            ),
            AlopexError::PolarsNotInstalled => write!(
                f,
                "polars が見つかりません。`pip install alopex[polars]` を実行してください"
            ),
            AlopexError::CloudAuthFailed { provider, env_var } => write!(
                f,
                "{} 認証に失敗しました。{} 環境変数を確認してください",
                provider, env_var
            ),
            AlopexError::TypeConversionError { expected, actual } => {
                write!(f, "型変換エラー: 期待={}, 実際={}", expected, actual)
            }
        }
    }
}

impl std::error::Error for AlopexError {}

impl AlopexError {
    pub fn code(&self) -> &'static str {
        match self {
            AlopexError::CatalogNotFound(_) => "ALOPEX-PY001",
            AlopexError::NamespaceNotFound(_) => "ALOPEX-PY002",
            AlopexError::TableNotFound(_) => "ALOPEX-PY003",
            AlopexError::ParentNotFound(_) => "ALOPEX-PY004",
            AlopexError::CatalogAlreadyExists(_) => "ALOPEX-PY005",
            AlopexError::NamespaceAlreadyExists(_) => "ALOPEX-PY006",
            AlopexError::TableExists(_) => "ALOPEX-PY007",
            AlopexError::WriteTargetNotFound(_) => "ALOPEX-PY008",
            AlopexError::StorageLocationRequired => "ALOPEX-PY009",
            AlopexError::PrimaryKeyRequired => "ALOPEX-PY010",
            AlopexError::UnsupportedFormat(_) => "ALOPEX-PY011",
            AlopexError::PolarsNotInstalled => "ALOPEX-PY012",
            AlopexError::CloudAuthFailed { .. } => "ALOPEX-PY013",
            AlopexError::TypeConversionError { .. } => "ALOPEX-PY014",
        }
    }
}

fn core_error_code(err: &alopex_core::Error) -> &'static str {
    match err {
        alopex_core::Error::NotFound => "ALOPEX-PY101",
        alopex_core::Error::TxnClosed => "ALOPEX-PY102",
        alopex_core::Error::TxnReadOnly => "ALOPEX-PY103",
        alopex_core::Error::TxnConflict => "ALOPEX-PY104",
        _ => GENERIC_ERROR_CODE,
    }
}

fn with_code(err: PyErr, code: &'static str) -> PyErr {
    Python::with_gil(|py| {
        err.value(py)
            .setattr("code", code)
            .expect("Python exception instances must accept stable code attributes");
    });
    err
}

impl From<AlopexError> for PyErr {
    fn from(err: AlopexError) -> PyErr {
        let code = err.code();
        let message = err.to_string();
        match &err {
            AlopexError::CatalogNotFound(_)
            | AlopexError::NamespaceNotFound(_)
            | AlopexError::TableNotFound(_)
            | AlopexError::ParentNotFound(_) => with_code(PyValueError::new_err(message), code),
            AlopexError::CatalogAlreadyExists(_) | AlopexError::NamespaceAlreadyExists(_) => {
                with_code(PyRuntimeError::new_err(message), code)
            }
            _ => with_code(PyAlopexError::new_err(message), code),
        }
    }
}

/// Convert a Display error into a Python exception.
///
/// Args:
///     err: Any error that implements Display.
///
/// Returns:
///     PyErr: Python exception wrapping the message.
///
/// Examples:
///     ```ignore
///     use crate::error::to_py_err;
///     let err = to_py_err("oops");
///     ```
///
/// Raises:
///     AlopexError: Raised in Python with the provided message.
#[allow(dead_code)]
pub fn to_py_err<E: Display>(err: E) -> PyErr {
    with_code(PyAlopexError::new_err(err.to_string()), GENERIC_ERROR_CODE)
}

/// Convert embedded catalog errors into Python exceptions.
///
/// Conversion rules:
///     - CatalogNotFound, NamespaceNotFound, TableNotFound -> ValueError
///     - CatalogAlreadyExists, NamespaceAlreadyExists -> RuntimeError
///     - TableAlreadyExists, UnsupportedDataSourceFormat -> AlopexError
///     - other errors -> AlopexError
///
/// Examples:
///     ```ignore
///     use crate::error::embedded_err;
///     let err = embedded_err(alopex_embedded::Error::CatalogNotFound("main".to_string()));
///     ```
///
/// Raises:
///     ValueError: For not-found errors.
///     RuntimeError: For already-exists errors.
///     AlopexError: For other embedded errors.
#[allow(dead_code)]
pub fn embedded_err(err: alopex_embedded::Error) -> PyErr {
    match err {
        alopex_embedded::Error::CatalogNotFound(name) => AlopexError::CatalogNotFound(name).into(),
        alopex_embedded::Error::NamespaceNotFound(catalog_name, namespace_name) => {
            AlopexError::NamespaceNotFound(format!("{}.{}", catalog_name, namespace_name)).into()
        }
        alopex_embedded::Error::TableNotFound(name) => AlopexError::TableNotFound(name).into(),
        alopex_embedded::Error::CatalogAlreadyExists(name) => {
            AlopexError::CatalogAlreadyExists(name).into()
        }
        alopex_embedded::Error::NamespaceAlreadyExists(catalog_name, namespace_name) => {
            AlopexError::NamespaceAlreadyExists(format!("{}.{}", catalog_name, namespace_name))
                .into()
        }
        alopex_embedded::Error::TableAlreadyExists(name) => AlopexError::TableExists(name).into(),
        alopex_embedded::Error::UnsupportedDataSourceFormat(format) => {
            AlopexError::UnsupportedFormat(format).into()
        }
        alopex_embedded::Error::Core(err) => core_err(err),
        other => to_py_err(other),
    }
}

/// Convert core engine errors into a Python exception.
///
/// Args:
///     err (alopex_core::Error): Core engine error value.
///
/// Returns:
///     PyErr: Python exception wrapping the message.
///
/// Examples:
///     ```ignore
///     use crate::error::core_err;
///     let err = core_err(alopex_core::Error::Internal("oops".into()));
///     ```
///
/// Raises:
///     AlopexError: Raised in Python with the provided message.
#[allow(dead_code)]
pub fn core_err(err: alopex_core::Error) -> PyErr {
    let code = core_error_code(&err);
    with_code(PyAlopexError::new_err(err.to_string()), code)
}
