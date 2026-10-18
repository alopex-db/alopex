use std::sync::Arc;

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::{Catalog, ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::executor::ExecutorError;
use alopex_sql::executor::bulk::{
    CopyOptions, CopySecurityConfig, FileFormat, execute_copy, validate_file_path,
};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::TxnBridge;

fn assert_denied(error: ExecutorError, path: &std::path::Path, allowed: &std::path::Path) {
    let rendered = error.to_string();
    assert!(!rendered.contains(allowed.to_str().unwrap()), "{rendered}");
    match error {
        ExecutorError::PathValidationFailed {
            path: actual,
            reason,
        } => {
            assert_eq!(actual, path.to_str().unwrap());
            assert_eq!(reason, "path not in allowed directories");
        }
        other => panic!("expected uniform path denial, got {other:?}"),
    }
}

#[test]
fn restricted_paths_hide_existence_and_allowed_directories() {
    let root = tempfile::tempdir().unwrap();
    let allowed = root.path().join("private-allowed-directory");
    std::fs::create_dir(&allowed).unwrap();
    let existing = root.path().join("existing.csv");
    std::fs::write(&existing, "1\n").unwrap();
    let missing = root.path().join("missing.csv");
    let nested_missing = root.path().join("missing-parent/file.csv");
    let config = CopySecurityConfig {
        allowed_base_dirs: Some(vec![allowed.clone()]),
        allow_symlinks: false,
    };
    for path in [&existing, &missing, &nested_missing] {
        assert_denied(
            validate_file_path(path.to_str().unwrap(), &config).unwrap_err(),
            path,
            &allowed,
        );
    }
}

#[test]
fn copy_csv_and_parquet_share_uniform_path_denial() {
    let root = tempfile::tempdir().unwrap();
    let allowed = root.path().join("private-allowed-directory");
    std::fs::create_dir(&allowed).unwrap();
    let existing = root.path().join("existing.data");
    // Invalid Parquet is intentional: denial must happen before parsing bytes.
    std::fs::write(&existing, "1\n").unwrap();
    let missing = root.path().join("missing.data");
    let config = CopySecurityConfig {
        allowed_base_dirs: Some(vec![allowed.clone()]),
        allow_symlinks: false,
    };
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let mut catalog = MemoryCatalog::new();
    catalog
        .create_table(TableMetadata::new(
            "items",
            vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        ))
        .unwrap();
    for format in [FileFormat::Csv, FileFormat::Parquet] {
        for path in [&existing, &missing] {
            let mut txn = bridge.begin_write().unwrap();
            let error = execute_copy(
                &mut txn,
                &catalog,
                "items",
                path.to_str().unwrap(),
                format,
                CopyOptions { header: false },
                &config,
            )
            .unwrap_err();
            assert_denied(error, path, &allowed);
        }
    }
}

#[test]
fn restricted_paths_accept_allowed_file_and_hide_allowed_missing() {
    let root = tempfile::tempdir().unwrap();
    // macOS temp paths can use /var, whose canonical spelling is /private/var.
    let allowed = root.path().canonicalize().unwrap();
    let existing = allowed.join("existing.csv");
    std::fs::write(&existing, "1\n").unwrap();
    let missing = allowed.join("missing.csv");
    let config = CopySecurityConfig {
        allowed_base_dirs: Some(vec![allowed]),
        allow_symlinks: false,
    };
    validate_file_path(existing.to_str().unwrap(), &config).unwrap();
    match validate_file_path(missing.to_str().unwrap(), &config).unwrap_err() {
        ExecutorError::PathValidationFailed { path, reason } => {
            assert_eq!(path, missing.to_str().unwrap());
            assert_eq!(reason, "path not in allowed directories");
        }
        other => panic!("expected generic unresolved-path denial, got {other:?}"),
    }
}

#[test]
fn unrestricted_paths_preserve_missing_and_regular_file_behavior() {
    let root = tempfile::tempdir().unwrap();
    let existing = root.path().join("existing.csv");
    std::fs::write(&existing, "1\n").unwrap();
    let missing = root.path().join("missing.csv");
    let config = CopySecurityConfig::default();
    validate_file_path(existing.to_str().unwrap(), &config).unwrap();
    assert!(matches!(
        validate_file_path(missing.to_str().unwrap(), &config),
        Err(ExecutorError::FileNotFound(path)) if path == missing.to_str().unwrap()
    ));
    assert!(matches!(
        validate_file_path(root.path().to_str().unwrap(), &config),
        Err(ExecutorError::PathValidationFailed { reason, .. }) if reason == "path is not a regular file"
    ));
}
