//! Conditional scalar functions and their short-circuit evaluators.

use std::cmp::Ordering;

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

macro_rules! wrappers {
    ($($fn_name:ident => $name:literal),+ $(,)?) => {
        $(fn $fn_name(values: &[SqlValue]) -> Result<SqlValue> { eval_named($name, values) })+
        pub fn eval_for(name: &str) -> Option<fn(&[SqlValue]) -> Result<SqlValue>> {
            match name { $( $name => Some($fn_name), )+ _ => None }
        }
    };
}

wrappers!(
    eval_coalesce => "coalesce", eval_nullif => "nullif", eval_ifnull => "ifnull",
    eval_iif => "iif", eval_greatest => "greatest", eval_least => "least",
);

pub fn lazy_eval_for(name: &str) -> Option<super::registry::LazyEvalFn> {
    match name {
        "coalesce" => Some(eval_coalesce_lazy),
        "ifnull" => Some(eval_ifnull_lazy),
        "iif" => Some(eval_iif_lazy),
        _ => None,
    }
}

fn eval_named(name: &str, values: &[SqlValue]) -> Result<SqlValue> {
    match name {
        "coalesce" => Ok(values
            .iter()
            .find(|v| !v.is_null())
            .cloned()
            .unwrap_or(SqlValue::Null)),
        "ifnull" => Ok(values
            .first()
            .filter(|v| !v.is_null())
            .cloned()
            .or_else(|| values.get(1).cloned())
            .unwrap_or(SqlValue::Null)),
        "nullif" => {
            let left = values.first().cloned().unwrap_or(SqlValue::Null);
            let right = values.get(1).cloned().unwrap_or(SqlValue::Null);
            if equal_values(&left, &right) {
                Ok(SqlValue::Null)
            } else {
                Ok(left)
            }
        }
        "iif" => Ok(if matches!(values.first(), Some(SqlValue::Boolean(true))) {
            values.get(1).cloned().unwrap_or(SqlValue::Null)
        } else {
            values.get(2).cloned().unwrap_or(SqlValue::Null)
        }),
        "greatest" | "least" => extrema(values, name == "greatest"),
        _ => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(name.into()),
        )),
    }
}

fn eval_coalesce_lazy(args: &[TypedExpr], ctx: &super::EvalContext<'_>) -> Result<SqlValue> {
    for arg in args {
        let value = super::evaluate(arg, ctx)?;
        if !value.is_null() {
            return Ok(value);
        }
    }
    Ok(SqlValue::Null)
}

fn eval_ifnull_lazy(args: &[TypedExpr], ctx: &super::EvalContext<'_>) -> Result<SqlValue> {
    let first = super::evaluate(args.first().ok_or_else(|| invalid("ifnull"))?, ctx)?;
    if first.is_null() {
        super::evaluate(args.get(1).ok_or_else(|| invalid("ifnull"))?, ctx)
    } else {
        Ok(first)
    }
}

fn eval_iif_lazy(args: &[TypedExpr], ctx: &super::EvalContext<'_>) -> Result<SqlValue> {
    let condition = super::evaluate(args.first().ok_or_else(|| invalid("iif"))?, ctx)?;
    let selected = matches!(condition, SqlValue::Boolean(true));
    super::evaluate(
        args.get(if selected { 1 } else { 2 })
            .ok_or_else(|| invalid("iif"))?,
        ctx,
    )
}

fn invalid(name: &str) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::UnsupportedFunction(format!(
        "{name}: invalid arguments"
    )))
}

fn equal_values(left: &SqlValue, right: &SqlValue) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    if let (Some(a), Some(b)) = (number(left), number(right)) {
        return a == b;
    }
    left == right
}

fn extrema(values: &[SqlValue], greatest: bool) -> Result<SqlValue> {
    let mut best: Option<SqlValue> = None;
    for value in values.iter().filter(|v| !v.is_null()) {
        if let Some(current) = &best {
            let Some(ordering) = compare_values(current, value) else {
                return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "Comparable numeric values".into(),
                    actual: format!("{} vs {}", current.type_name(), value.type_name()),
                }));
            };
            if (greatest && ordering == Ordering::Less)
                || (!greatest && ordering == Ordering::Greater)
            {
                best = Some(value.clone());
            }
        } else {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(SqlValue::Null))
}

fn number(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Integer(v) => Some(*v as f64),
        SqlValue::BigInt(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v as f64),
        SqlValue::Double(v) => Some(*v),
        _ => None,
    }
}
fn compare_values(left: &SqlValue, right: &SqlValue) -> Option<Ordering> {
    match (number(left), number(right)) {
        (Some(a), Some(b)) => a.partial_cmp(&b),
        _ => left.partial_cmp(right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_propagation_and_comparison_functions() {
        let coalesce = eval_for("coalesce").unwrap();
        assert_eq!(
            coalesce(&[SqlValue::Null, SqlValue::Text("ok".into())]).unwrap(),
            SqlValue::Text("ok".into())
        );
        assert_eq!(
            eval_for("nullif").unwrap()(&[SqlValue::Integer(1), SqlValue::Integer(1)]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            eval_for("greatest").unwrap()(&[SqlValue::Integer(2), SqlValue::Integer(5)]).unwrap(),
            SqlValue::Integer(5)
        );
        assert_eq!(
            eval_for("least").unwrap()(&[SqlValue::Integer(2), SqlValue::Integer(5)]).unwrap(),
            SqlValue::Integer(2)
        );
    }
}
