use std::collections::HashMap;
use std::env;
use std::sync::Mutex;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::{prepare_freethreaded_python, Python};

use super::credentials::{auto_resolve_credentials, mask_sensitive_values};
use super::{validate_identifier, validate_storage_location, PyCatalogInfo, PyColumnInfo};
use super::{PyNamespaceInfo, PyTableInfo};
use crate::error::{AlopexError, PyAlopexError};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn init_python() {
    prepare_freethreaded_python();
}

fn set_env(key: &str, value: Option<&str>) -> Option<String> {
    let prev = env::var(key).ok();
    match value {
        Some(value) => env::set_var(key, value),
        None => env::remove_var(key),
    }
    prev
}

fn restore_env(key: &str, prev: Option<String>) {
    match prev {
        Some(value) => env::set_var(key, value),
        None => env::remove_var(key),
    }
}

#[test]
fn test_catalog_info_conversion() {
    let info = alopex_embedded::catalog::CatalogInfo {
        name: "main".to_string(),
        comment: Some("comment".to_string()),
        storage_root: Some("file:///tmp".to_string()),
    };
    let py_info = PyCatalogInfo::from(info);
    assert_eq!(py_info.name, "main");
    assert_eq!(py_info.comment.as_deref(), Some("comment"));
    assert_eq!(py_info.storage_root.as_deref(), Some("file:///tmp"));
}

#[test]
fn test_namespace_info_conversion() {
    let info = alopex_embedded::catalog::NamespaceInfo {
        name: "ns".to_string(),
        catalog_name: "main".to_string(),
        comment: Some("note".to_string()),
        storage_root: Some("s3://bucket/ns".to_string()),
    };
    let py_info = PyNamespaceInfo::from(info);
    assert_eq!(py_info.name, "ns");
    assert_eq!(py_info.catalog_name, "main");
    assert_eq!(py_info.comment.as_deref(), Some("note"));
    assert_eq!(py_info.storage_root.as_deref(), Some("s3://bucket/ns"));
}

#[test]
fn test_column_info_conversion() {
    let info = alopex_embedded::catalog::ColumnInfo {
        name: "id".to_string(),
        type_name: "INT".to_string(),
        position: 0,
        nullable: false,
        comment: Some("pk".to_string()),
    };
    let py_info = PyColumnInfo::from(info);
    assert_eq!(py_info.name, "id");
    assert_eq!(py_info.type_name, "INT");
    assert_eq!(py_info.position, 0);
    assert!(!py_info.nullable);
    assert_eq!(py_info.comment.as_deref(), Some("pk"));
}

#[test]
fn test_table_info_conversion() {
    let info = alopex_embedded::catalog::TableInfo {
        name: "tbl".to_string(),
        catalog_name: "main".to_string(),
        namespace_name: "ns".to_string(),
        storage_location: Some("file:///tmp/data.parquet".to_string()),
        data_source_format: Some("parquet".to_string()),
        columns: vec![alopex_embedded::catalog::ColumnInfo {
            name: "id".to_string(),
            type_name: "INT".to_string(),
            position: 0,
            nullable: false,
            comment: None,
        }],
    };

    let py_info = PyTableInfo::from(info);
    assert_eq!(py_info.name, "tbl");
    assert_eq!(py_info.catalog_name, "main");
    assert_eq!(py_info.namespace_name, "ns");
    assert_eq!(py_info.table_type, "MANAGED");
    assert_eq!(
        py_info.storage_location.as_deref(),
        Some("file:///tmp/data.parquet")
    );
    assert_eq!(py_info.data_source_format.as_deref(), Some("PARQUET"));
    assert!(py_info.primary_key.is_none());
    assert!(py_info.comment.is_none());
    assert_eq!(py_info.columns.len(), 1);
    assert_eq!(py_info.columns[0].name, "id");
    assert_eq!(py_info.columns[0].type_name, "INT");
}

#[test]
fn test_auto_resolve_credentials_s3() {
    let _guard = ENV_LOCK.lock().unwrap();
    let prev_access = set_env("AWS_ACCESS_KEY_ID", Some("access"));
    let prev_secret = set_env("AWS_SECRET_ACCESS_KEY", Some("secret"));
    let prev_region = set_env("AWS_REGION", Some("ap-northeast-1"));

    let resolved = auto_resolve_credentials("s3://bucket/path").unwrap();
    assert_eq!(
        resolved.get("aws_access_key_id").map(String::as_str),
        Some("access")
    );
    assert_eq!(
        resolved.get("aws_secret_access_key").map(String::as_str),
        Some("secret")
    );
    assert_eq!(
        resolved.get("aws_region").map(String::as_str),
        Some("ap-northeast-1")
    );

    restore_env("AWS_ACCESS_KEY_ID", prev_access);
    restore_env("AWS_SECRET_ACCESS_KEY", prev_secret);
    restore_env("AWS_REGION", prev_region);
}

#[test]
fn test_auto_resolve_credentials_gcs() {
    let _guard = ENV_LOCK.lock().unwrap();
    let prev = set_env("GOOGLE_APPLICATION_CREDENTIALS", Some("/tmp/gcs.json"));

    let resolved = auto_resolve_credentials("gs://bucket/path").unwrap();
    assert_eq!(
        resolved.get("service_account_path").map(String::as_str),
        Some("/tmp/gcs.json")
    );

    restore_env("GOOGLE_APPLICATION_CREDENTIALS", prev);
}

#[test]
fn test_auto_resolve_credentials_azure() {
    let _guard = ENV_LOCK.lock().unwrap();
    let prev_account = set_env("AZURE_STORAGE_ACCOUNT", Some("account"));
    let prev_key = set_env("AZURE_STORAGE_KEY", Some("key"));

    let resolved = auto_resolve_credentials("az://container/path").unwrap();
    assert_eq!(
        resolved.get("account_name").map(String::as_str),
        Some("account")
    );
    assert_eq!(resolved.get("account_key").map(String::as_str), Some("key"));

    restore_env("AZURE_STORAGE_ACCOUNT", prev_account);
    restore_env("AZURE_STORAGE_KEY", prev_key);
}

#[test]
fn test_mask_sensitive_values() {
    let mut input = HashMap::new();
    input.insert("aws_secret_access_key".to_string(), "secret".to_string());
    input.insert("Token".to_string(), "token".to_string());
    input.insert("plain".to_string(), "value".to_string());

    let masked = mask_sensitive_values(&input);
    assert_eq!(
        masked.get("aws_secret_access_key").map(String::as_str),
        Some("***")
    );
    assert_eq!(masked.get("Token").map(String::as_str), Some("***"));
    assert_eq!(masked.get("plain").map(String::as_str), Some("value"));
}

#[test]
fn test_validate_identifier() {
    assert!(validate_identifier("valid_name").is_ok());
    assert!(validate_identifier("_valid123").is_ok());
    assert!(validate_identifier("1invalid").is_err());
    assert!(validate_identifier("bad-name").is_err());
    assert!(validate_identifier("").is_err());
}

#[test]
fn test_validate_storage_location() {
    assert!(validate_storage_location("/tmp/data.parquet").is_ok());
    assert!(validate_storage_location("file:///tmp/data.parquet").is_ok());
    assert!(validate_storage_location("s3://bucket/path").is_ok());
    assert!(validate_storage_location("gs://bucket/path").is_ok());
    assert!(validate_storage_location("az://container/path").is_ok());
    assert!(validate_storage_location("abfs://container/path").is_ok());
    assert!(validate_storage_location("ftp://example.com/data").is_err());
    assert!(validate_storage_location("  ").is_err());
}

#[test]
fn test_error_conversion_value_error() {
    init_python();
    Python::with_gil(|py| {
        let err: pyo3::PyErr = AlopexError::CatalogNotFound("missing".to_string()).into();
        assert!(err.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn test_error_conversion_runtime_error() {
    init_python();
    Python::with_gil(|py| {
        let err: pyo3::PyErr = AlopexError::CatalogAlreadyExists("dup".to_string()).into();
        assert!(err.is_instance_of::<PyRuntimeError>(py));
    });
}

#[test]
fn test_error_conversion_custom_error() {
    init_python();
    Python::with_gil(|py| {
        let err: pyo3::PyErr = AlopexError::UnsupportedFormat("CSV".to_string()).into();
        assert!(err.is_instance_of::<PyAlopexError>(py));
    });
}
