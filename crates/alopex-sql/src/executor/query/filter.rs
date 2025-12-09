use crate::executor::Result;
use crate::executor::evaluator::EvalContext;
use crate::storage::SqlValue;

use super::Row;

/// Filter rows using a predicate; only Boolean true is retained.
pub fn execute_filter(
    rows: Vec<Row>,
    predicate: &crate::planner::typed_expr::TypedExpr,
) -> Result<Vec<Row>> {
    let mut filtered = Vec::new();
    for row in rows {
        let ctx = EvalContext::new(&row.values);
        let value = crate::executor::evaluator::evaluate(predicate, &ctx)?;
        if let SqlValue::Boolean(true) = value {
            filtered.push(row);
        }
    }
    Ok(filtered)
}
