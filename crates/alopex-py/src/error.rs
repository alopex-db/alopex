#![allow(unused_doc_comments)]

use std::fmt::{self, Display};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::PyErr;

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

impl From<AlopexError> for PyErr {
    fn from(err: AlopexError) -> PyErr {
        match &err {
            AlopexError::CatalogNotFound(_)
            | AlopexError::NamespaceNotFound(_)
            | AlopexError::TableNotFound(_)
            | AlopexError::ParentNotFound(_) => PyValueError::new_err(err.to_string()),
            AlopexError::CatalogAlreadyExists(_) | AlopexError::NamespaceAlreadyExists(_) => {
                PyRuntimeError::new_err(err.to_string())
            }
            _ => PyAlopexError::new_err(err.to_string()),
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
    PyAlopexError::new_err(err.to_string())
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
        other => PyAlopexError::new_err(other.to_string()),
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
    to_py_err(err)
}
