use std::fmt::{self, Display};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::PyErr;

create_exception!(crate::error, PyAlopexError, PyException);

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

#[allow(dead_code)]
pub fn to_py_err<E: Display>(err: E) -> PyErr {
    PyAlopexError::new_err(err.to_string())
}

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

#[allow(dead_code)]
pub fn core_err(err: alopex_core::Error) -> PyErr {
    to_py_err(err)
}
