use std::cell::RefCell;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::executor::{EvaluationError, ExecutorError};
use crate::storage::SqlValue;

thread_local! {
    static STATEMENT_TIMESTAMPS: RefCell<Vec<i64>> = const { RefCell::new(Vec::new()) };
}

/// Scope guard for the timestamp fixed at the start of a SQL statement.
pub(crate) struct StatementTimestampGuard {
    active: bool,
}

/// Fix the current UTC timestamp for the duration of the outermost statement.
///
/// Nested execution shares the existing timestamp, which keeps subexpressions
/// and subqueries within one statement consistent.
pub(crate) fn begin_statement() -> StatementTimestampGuard {
    let active = STATEMENT_TIMESTAMPS.with(|timestamps| {
        let mut timestamps = timestamps.borrow_mut();
        if timestamps.is_empty() {
            timestamps.push(utc_now_micros());
            true
        } else {
            false
        }
    });
    StatementTimestampGuard { active }
}

impl Drop for StatementTimestampGuard {
    fn drop(&mut self) {
        if self.active {
            STATEMENT_TIMESTAMPS.with(|timestamps| {
                timestamps.borrow_mut().pop();
            });
        }
    }
}

pub(crate) fn current_statement_timestamp() -> i64 {
    STATEMENT_TIMESTAMPS.with(|timestamps| {
        timestamps
            .borrow()
            .last()
            .copied()
            .unwrap_or_else(utc_now_micros)
    })
}

fn utc_now_micros() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

/// Evaluation context holds a borrowed row for zero-copy access.
pub struct EvalContext<'a> {
    row: &'a [SqlValue],
    statement_timestamp: i64,
}

impl<'a> EvalContext<'a> {
    /// Create a new evaluation context for the given row slice.
    pub fn new(row: &'a [SqlValue]) -> Self {
        Self {
            row,
            statement_timestamp: current_statement_timestamp(),
        }
    }

    /// Get a column value by index.
    pub fn get(&self, index: usize) -> Result<&'a SqlValue, ExecutorError> {
        self.row.get(index).ok_or(ExecutorError::Evaluation(
            EvaluationError::InvalidColumnRef { index },
        ))
    }

    /// Return the UTC timestamp fixed at the start of this statement.
    pub(crate) fn statement_timestamp(&self) -> i64 {
        self.statement_timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_existing_column_returns_value() {
        let row = vec![SqlValue::Integer(1), SqlValue::Text("a".into())];
        let ctx = EvalContext::new(&row);
        assert!(matches!(ctx.get(0), Ok(SqlValue::Integer(1))));
    }

    #[test]
    fn get_out_of_range_errors() {
        let row = vec![SqlValue::Integer(1)];
        let ctx = EvalContext::new(&row);
        let err = ctx.get(2).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::InvalidColumnRef { index }) => {
                assert_eq!(index, 2)
            }
            other => panic!("unexpected error {other:?}"),
        }
    }
}
