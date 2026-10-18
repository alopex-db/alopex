use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;

use alopex_sql::Span;
use alopex_sql::catalog::ColumnMetadata;
use alopex_sql::executor::query::iterator::{RowIterator, SortIterator, VecIterator};
use alopex_sql::executor::{ExecutorError, MemoryPolicy, Result, Row, SpillPolicy};
use alopex_sql::planner::typed_expr::{SortExpr, TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;

fn sort_key() -> SortExpr {
    SortExpr {
        expr: TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "test".into(),
                column: "id".into(),
                column_index: 0,
            },
            resolved_type: ResolvedType::Integer,
            span: Span::default(),
        },
        asc: true,
        nulls_first: false,
    }
}

#[test]
fn sort_input_error_after_first_spill_removes_run() {
    struct ErrorAfterRun {
        schema: Vec<ColumnMetadata>,
        directory: PathBuf,
        yielded: bool,
        observed_run: Rc<Cell<bool>>,
    }
    impl RowIterator for ErrorAfterRun {
        fn next_row(&mut self) -> Option<Result<Row>> {
            if !self.yielded {
                self.yielded = true;
                return Some(Ok(Row::new(1, vec![SqlValue::Integer(1)])));
            }
            assert_eq!(std::fs::read_dir(&self.directory).unwrap().count(), 1);
            self.observed_run.set(true);
            Some(Err(ExecutorError::BulkLoad(
                "controlled input failure after spill".into(),
            )))
        }
        fn schema(&self) -> &[ColumnMetadata] {
            &self.schema
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let observed = Rc::new(Cell::new(false));
    let input = ErrorAfterRun {
        schema: vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        directory: dir.path().to_path_buf(),
        yielded: false,
        observed_run: observed.clone(),
    };
    let policy = MemoryPolicy::new(
        Some(1),
        SpillPolicy::SpillToDisk {
            directory: dir.path().to_path_buf(),
        },
    );
    let error = match SortIterator::new_with_policy(input, &[sort_key()], Some(policy)) {
        Ok(_) => panic!("expected the input failure"),
        Err(error) => error,
    };
    assert!(
        observed.get(),
        "the test must observe a completed spill run"
    );
    assert!(
        matches!(&error, ExecutorError::BulkLoad(message) if message == "controlled input failure after spill")
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn sort_spill_success_preserves_order_and_removes_runs() {
    let dir = tempfile::tempdir().unwrap();
    let input = VecIterator::new(
        vec![
            Row::new(2, vec![SqlValue::Integer(2)]),
            Row::new(1, vec![SqlValue::Integer(1)]),
        ],
        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    );
    let policy = MemoryPolicy::new(
        Some(1),
        SpillPolicy::SpillToDisk {
            directory: dir.path().to_path_buf(),
        },
    );
    let mut sorted = SortIterator::new_with_policy(input, &[sort_key()], Some(policy)).unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    assert_eq!(
        sorted.next_row().unwrap().unwrap().values,
        vec![SqlValue::Integer(1)]
    );
    assert_eq!(
        sorted.next_row().unwrap().unwrap().values,
        vec![SqlValue::Integer(2)]
    );
    assert!(sorted.next_row().is_none());
    drop(sorted);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn sort_merge_initialization_error_removes_all_runs() {
    struct CorruptLaterRun {
        schema: Vec<ColumnMetadata>,
        directory: PathBuf,
        runs: Vec<PathBuf>,
        yielded: usize,
        corrupted: Rc<Cell<bool>>,
    }
    impl RowIterator for CorruptLaterRun {
        fn next_row(&mut self) -> Option<Result<Row>> {
            if self.yielded > 0 {
                let new_paths: Vec<_> = std::fs::read_dir(&self.directory)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| !self.runs.contains(path))
                    .collect();
                assert_eq!(new_paths.len(), 1, "each row must create one real run");
                self.runs.push(new_paths[0].clone());
            }
            if self.yielded == 3 {
                // The first reader opens successfully, the second fails after its
                // row-id header, and the third run has no reader owner yet.
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&self.runs[1])
                    .unwrap();
                file.set_len(8).unwrap();
                drop(file);
                assert_eq!(std::fs::metadata(&self.runs[1]).unwrap().len(), 8);
                self.corrupted.set(true);
                return None;
            }
            self.yielded += 1;
            Some(Ok(Row::new(
                self.yielded as u64,
                vec![SqlValue::Integer(self.yielded as i32)],
            )))
        }
        fn schema(&self) -> &[ColumnMetadata] {
            &self.schema
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let corrupted = Rc::new(Cell::new(false));
    let input = CorruptLaterRun {
        schema: vec![ColumnMetadata::new("id", ResolvedType::Integer)],
        directory: dir.path().to_path_buf(),
        runs: Vec::new(),
        yielded: 0,
        corrupted: corrupted.clone(),
    };
    let policy = MemoryPolicy::new(
        Some(1),
        SpillPolicy::SpillToDisk {
            directory: dir.path().to_path_buf(),
        },
    );
    let error = match SortIterator::new_with_policy(input, &[sort_key()], Some(policy)) {
        Ok(_) => panic!("expected the truncated run to fail merge initialization"),
        Err(error) => error,
    };
    assert!(corrupted.get());
    assert!(
        matches!(&error, ExecutorError::InvalidOperation { operation, .. } if operation == "sort spill"),
        "{error:?}"
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn sort_merge_early_drop_removes_all_runs() {
    let dir = tempfile::tempdir().unwrap();
    let input = VecIterator::new(
        vec![
            Row::new(3, vec![SqlValue::Integer(3)]),
            Row::new(1, vec![SqlValue::Integer(1)]),
            Row::new(2, vec![SqlValue::Integer(2)]),
        ],
        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    );
    let policy = MemoryPolicy::new(
        Some(1),
        SpillPolicy::SpillToDisk {
            directory: dir.path().to_path_buf(),
        },
    );
    let mut sorted = SortIterator::new_with_policy(input, &[sort_key()], Some(policy)).unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 3);
    assert_eq!(
        sorted.next_row().unwrap().unwrap().values,
        vec![SqlValue::Integer(1)]
    );
    // Two rows remain unread: cleanup must not depend on reaching EOF.
    drop(sorted);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
