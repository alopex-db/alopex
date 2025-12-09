use crate::executor::{EvaluationError, ExecutorError};
use crate::storage::SqlValue;

/// Evaluation context holds a borrowed row for zero-copy access.
pub struct EvalContext<'a> {
    row: &'a [SqlValue],
}

impl<'a> EvalContext<'a> {
    /// Create a new evaluation context for the given row slice.
    pub fn new(row: &'a [SqlValue]) -> Self {
        Self { row }
    }

    /// Get a column value by index.
    pub fn get(&self, index: usize) -> Result<&'a SqlValue, ExecutorError> {
        self.row
            .get(index)
            .ok_or_else(|| ExecutorError::Evaluation(EvaluationError::InvalidColumnRef { index }))
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
