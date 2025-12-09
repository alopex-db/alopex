use crate::executor::Result;
use crate::storage::SqlValue;

use super::EvalContext;

pub fn eval_column_ref(index: usize, ctx: &EvalContext<'_>) -> Result<SqlValue> {
    Ok(ctx.get(index)?.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{EvaluationError, ExecutorError};

    #[test]
    fn returns_column_value() {
        let row = vec![SqlValue::Integer(5)];
        let ctx = EvalContext::new(&row);
        assert_eq!(eval_column_ref(0, &ctx).unwrap(), SqlValue::Integer(5));
    }

    #[test]
    fn invalid_index_errors() {
        let row = vec![SqlValue::Integer(5)];
        let ctx = EvalContext::new(&row);
        let err = eval_column_ref(2, &ctx).unwrap_err();
        match err {
            ExecutorError::Evaluation(EvaluationError::InvalidColumnRef { index }) => {
                assert_eq!(index, 2)
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
