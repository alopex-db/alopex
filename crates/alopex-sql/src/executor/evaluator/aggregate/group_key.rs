use crate::executor::evaluator::{EvalContext, evaluate};
use crate::executor::{Result, Row};
use crate::planner::typed_expr::{TypedExpr, TypedExprKind};

use super::sql_value_key::write_value_bytes;

/// Hashable group key representation for multi-column GROUP BY.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    bytes: Vec<u8>,
}

impl GroupKey {
    /// Create a group key from a row and key expressions.
    pub fn from_row(row: &Row, key_exprs: &[TypedExpr]) -> Result<Self> {
        if key_exprs.is_empty() {
            return Ok(Self::empty());
        }

        let ctx = EvalContext::new(&row.values);
        let mut bytes = Vec::new();
        for expr in key_exprs {
            match &expr.kind {
                TypedExprKind::ColumnRef { column_index, .. } => {
                    let value = ctx.get(*column_index)?;
                    write_value_bytes(value, &mut bytes);
                }
                _ => {
                    let value = evaluate(expr, &ctx)?;
                    write_value_bytes(&value, &mut bytes);
                }
            }
        }

        Ok(Self { bytes })
    }

    /// Create an empty key for global aggregation.
    pub fn empty() -> Self {
        Self { bytes: Vec::new() }
    }
}
