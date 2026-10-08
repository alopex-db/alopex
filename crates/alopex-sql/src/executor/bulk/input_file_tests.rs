//! Unix-only private-owner tests for the authorization-to-open boundary.

use super::{CopySecurityConfig, open_input_file_with_after_validation};
use crate::executor::ExecutorError;
use ::parquet::arrow::ArrowWriter;
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::cell::Cell;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn parquet_file(path: &Path, value: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .expect("fixture: valid one-column batch");
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn values(file: File) -> Vec<i64> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("fixture: valid Parquet metadata")
        .build()
        .unwrap();
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.expect("fixture: valid Parquet batch");
        assert_eq!(batch.num_columns(), 1);
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        values.extend((0..column.len()).map(|index| column.value(index)));
    }
    values
}

fn config(roots: Vec<PathBuf>, allow_symlinks: bool) -> CopySecurityConfig {
    CopySecurityConfig {
        allowed_base_dirs: Some(roots),
        allow_symlinks,
    }
}

fn file_link(target: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link).expect("fixture: create file symlink");
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(target, link).expect("fixture: create file symlink");
}

fn directory_link(target: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link).expect("fixture: create directory symlink");
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(target, link).expect("fixture: create directory symlink");
}

fn race_failure(result: Result<File, ExecutorError>) -> Option<String> {
    match result {
        Ok(file) => {
            let actual = values(file);
            (actual != vec![11]).then(|| format!("expected original [11], got {actual:?}"))
        }
        Err(ExecutorError::PathValidationFailed { reason, .. })
            if reason == "path not in allowed directories" =>
        {
            None
        }
        Err(error) => Some(format!("unexpected error classification: {error:?}")),
    }
}

#[test]
fn issue577_open_leaf_replacement_stays_with_authorized_root() {
    let mut failures = Vec::new();
    for allow_symlinks in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let allowed = root.join("allowed");
        fs::create_dir(&allowed).unwrap();
        let input = allowed.join("input.parquet");
        let outside = root.join("outside.parquet");
        parquet_file(&input, 11);
        parquet_file(&outside, 99);
        let reached = Cell::new(false);
        let result = open_input_file_with_after_validation(
            input.to_str().unwrap(),
            &config(vec![allowed], allow_symlinks),
            || {
                reached.set(true);
                fs::rename(&input, root.join("original.parquet")).unwrap();
                file_link(&outside, &input);
            },
        );
        assert!(reached.get(), "fixture: authorization gate was not reached");
        if let Some(failure) = race_failure(result) {
            failures.push(format!("allow_symlinks={allow_symlinks}: {failure}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn issue577_open_parent_replacement_stays_with_authorized_root() {
    let mut failures = Vec::new();
    for allow_symlinks in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let allowed = root.join("allowed");
        let child = allowed.join("child");
        let outside = root.join("outside");
        fs::create_dir_all(&child).unwrap();
        fs::create_dir(&outside).unwrap();
        let input = child.join("input.parquet");
        parquet_file(&input, 11);
        parquet_file(&outside.join("input.parquet"), 99);
        let reached = Cell::new(false);
        let result = open_input_file_with_after_validation(
            input.to_str().unwrap(),
            &config(vec![allowed.clone()], allow_symlinks),
            || {
                reached.set(true);
                fs::rename(&child, allowed.join("original-child")).unwrap();
                directory_link(&outside, &child);
            },
        );
        assert!(reached.get(), "fixture: authorization gate was not reached");
        if let Some(failure) = race_failure(result) {
            failures.push(format!("allow_symlinks={allow_symlinks}: {failure}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn issue577_open_static_absolute_leaf_link_preserves_policy() {
    let temp = tempfile::tempdir().unwrap();
    let allowed = temp.path().canonicalize().unwrap();
    let target = allowed.join("target.parquet");
    let input = allowed.join("link.parquet");
    parquet_file(&target, 11);
    file_link(&target, &input);
    let file = open_input_file_with_after_validation(
        input.to_str().unwrap(),
        &config(vec![allowed.clone()], true),
        || {},
    )
    .unwrap();
    assert_eq!(values(file), vec![11]);
    let error = open_input_file_with_after_validation(
        input.to_str().unwrap(),
        &config(vec![allowed], false),
        || panic!("static leaf symlink must be rejected before the gate"),
    )
    .unwrap_err();
    assert!(
        matches!(error, ExecutorError::PathValidationFailed { reason, .. }
        if reason == "symbolic links not allowed")
    );
}

#[test]
fn issue577_open_absolute_link_selects_the_authorized_target_root() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let first = root.join("first");
    let second = root.join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    let target = second.join("target.parquet");
    let input = first.join("link.parquet");
    parquet_file(&target, 22);
    file_link(&target, &input);
    let file = open_input_file_with_after_validation(
        input.to_str().unwrap(),
        &config(vec![first.clone(), second], true),
        || {},
    )
    .unwrap();
    assert_eq!(values(file), vec![22]);
    let error = open_input_file_with_after_validation(
        input.to_str().unwrap(),
        &config(vec![first], true),
        || panic!("outside target must be rejected before the gate"),
    )
    .unwrap_err();
    assert!(
        matches!(error, ExecutorError::PathValidationFailed { reason, .. }
        if reason == "path not in allowed directories")
    );
}

#[test]
fn issue577_open_static_absolute_ancestor_link_keeps_regular_leaf() {
    let temp = tempfile::tempdir().unwrap();
    let allowed = temp.path().canonicalize().unwrap();
    let target_dir = allowed.join("target-directory");
    let link = allowed.join("directory-link");
    fs::create_dir(&target_dir).unwrap();
    parquet_file(&target_dir.join("input.parquet"), 11);
    directory_link(&target_dir, &link);
    let input = link.join("input.parquet");
    let file = open_input_file_with_after_validation(
        input.to_str().unwrap(),
        &config(vec![allowed], false),
        || {},
    )
    .unwrap();
    assert_eq!(values(file), vec![11]);
}
