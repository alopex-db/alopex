//! Execution of planner-internal standard predicate functions.

use crate::ast::expr::{
    BinaryOp, INTERNAL_ROW_BETWEEN, INTERNAL_ROW_DISTINCT, INTERNAL_ROW_EQ, INTERNAL_ROW_GT,
    INTERNAL_ROW_GTEQ, INTERNAL_ROW_IN, INTERNAL_ROW_LT, INTERNAL_ROW_LTEQ, INTERNAL_ROW_NEQ,
    INTERNAL_TRUTH_FALSE, INTERNAL_TRUTH_TRUE, INTERNAL_TRUTH_UNKNOWN,
};
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::{EvalContext, binary_op, evaluate};

pub(crate) fn evaluate_internal_predicate(
    name: &str,
    args: &[TypedExpr],
    ctx: &EvalContext<'_>,
) -> Option<Result<SqlValue>> {
    let (base, metadata) = name.split_once(':').unwrap_or((name, ""));
    let recognized = matches!(
        base,
        INTERNAL_TRUTH_TRUE
            | INTERNAL_TRUTH_FALSE
            | INTERNAL_TRUTH_UNKNOWN
            | INTERNAL_ROW_EQ
            | INTERNAL_ROW_NEQ
            | INTERNAL_ROW_LT
            | INTERNAL_ROW_LTEQ
            | INTERNAL_ROW_GT
            | INTERNAL_ROW_GTEQ
            | INTERNAL_ROW_DISTINCT
            | INTERNAL_ROW_BETWEEN
            | INTERNAL_ROW_IN
    );
    if !recognized {
        return None;
    }

    Some((|| {
        let values = args
            .iter()
            .map(|arg| evaluate(arg, ctx))
            .collect::<Result<Vec<_>>>()?;
        if matches!(
            base,
            INTERNAL_TRUTH_TRUE | INTERNAL_TRUTH_FALSE | INTERNAL_TRUTH_UNKNOWN
        ) {
            return evaluate_truth(base, metadata, &values);
        }

        let mut fields = metadata.split(':');
        let width = fields
            .next()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|width| *width > 0)
            .ok_or_else(|| malformed(name))?;
        match base {
            INTERNAL_ROW_EQ => compare_rows(&values, width, BinaryOp::Eq),
            INTERNAL_ROW_NEQ => compare_rows(&values, width, BinaryOp::Neq),
            INTERNAL_ROW_LT => compare_rows(&values, width, BinaryOp::Lt),
            INTERNAL_ROW_LTEQ => compare_rows(&values, width, BinaryOp::LtEq),
            INTERNAL_ROW_GT => compare_rows(&values, width, BinaryOp::Gt),
            INTERNAL_ROW_GTEQ => compare_rows(&values, width, BinaryOp::GtEq),
            INTERNAL_ROW_DISTINCT => {
                let negated = parse_flag(fields.next(), name)?;
                evaluate_distinct(&values, width, negated)
            }
            INTERNAL_ROW_BETWEEN => {
                let negated = parse_flag(fields.next(), name)?;
                evaluate_between(&values, width, negated)
            }
            INTERNAL_ROW_IN => {
                let negated = parse_flag(fields.next(), name)?;
                evaluate_in(&values, width, negated)
            }
            _ => Err(malformed(name)),
        }
    })())
}

fn evaluate_truth(base: &str, metadata: &str, values: &[SqlValue]) -> Result<SqlValue> {
    if values.len() != 1 {
        return Err(malformed(base));
    }
    let matches = match (base, &values[0]) {
        (INTERNAL_TRUTH_TRUE, SqlValue::Boolean(value)) => *value,
        (INTERNAL_TRUTH_FALSE, SqlValue::Boolean(value)) => !*value,
        (INTERNAL_TRUTH_UNKNOWN, SqlValue::Null) => true,
        (INTERNAL_TRUTH_TRUE | INTERNAL_TRUTH_FALSE, SqlValue::Null)
        | (INTERNAL_TRUTH_UNKNOWN, SqlValue::Boolean(_)) => false,
        (_, other) => {
            return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                expected: "Boolean".into(),
                actual: other.type_name().into(),
            }));
        }
    };
    let negated = parse_flag(Some(metadata), base)?;
    Ok(SqlValue::Boolean(if negated { !matches } else { matches }))
}

fn compare_rows(values: &[SqlValue], width: usize, op: BinaryOp) -> Result<SqlValue> {
    if values.len() != width.saturating_mul(2) {
        return Err(malformed("row comparison"));
    }
    let (left, right) = values.split_at(width);
    match op {
        BinaryOp::Eq | BinaryOp::Neq => {
            let mut unknown = false;
            for (left, right) in left.iter().zip(right) {
                match binary_op::eval_binary_values(&BinaryOp::Eq, left.clone(), right.clone())? {
                    SqlValue::Boolean(false) => {
                        return Ok(SqlValue::Boolean(matches!(op, BinaryOp::Neq)));
                    }
                    SqlValue::Boolean(true) => {}
                    SqlValue::Null => unknown = true,
                    other => return Err(non_boolean(other)),
                }
            }
            if unknown {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Boolean(matches!(op, BinaryOp::Eq)))
            }
        }
        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
            for (left, right) in left.iter().zip(right) {
                match binary_op::eval_binary_values(&BinaryOp::Eq, left.clone(), right.clone())? {
                    SqlValue::Boolean(true) => continue,
                    SqlValue::Boolean(false) => {
                        return binary_op::eval_binary_values(&op, left.clone(), right.clone());
                    }
                    SqlValue::Null => return Ok(SqlValue::Null),
                    other => return Err(non_boolean(other)),
                }
            }
            Ok(SqlValue::Boolean(matches!(
                op,
                BinaryOp::LtEq | BinaryOp::GtEq
            )))
        }
        _ => Err(malformed("row comparison")),
    }
}

fn evaluate_distinct(values: &[SqlValue], width: usize, negated: bool) -> Result<SqlValue> {
    if values.len() != width.saturating_mul(2) {
        return Err(malformed("IS DISTINCT FROM"));
    }
    let (left, right) = values.split_at(width);
    let mut distinct = false;
    for (left, right) in left.iter().zip(right) {
        let field_distinct = match (left, right) {
            (SqlValue::Null, SqlValue::Null) => false,
            (SqlValue::Null, _) | (_, SqlValue::Null) => true,
            _ => match binary_op::eval_binary_values(&BinaryOp::Eq, left.clone(), right.clone())? {
                SqlValue::Boolean(equal) => !equal,
                other => return Err(non_boolean(other)),
            },
        };
        if field_distinct {
            distinct = true;
            break;
        }
    }
    Ok(SqlValue::Boolean(if negated {
        !distinct
    } else {
        distinct
    }))
}

fn evaluate_between(values: &[SqlValue], width: usize, negated: bool) -> Result<SqlValue> {
    if values.len() != width.saturating_mul(3) {
        return Err(malformed("row BETWEEN"));
    }
    let lower = compare_rows(&values[..width * 2], width, BinaryOp::GtEq)?;
    let mut upper_values = Vec::with_capacity(width * 2);
    upper_values.extend_from_slice(&values[..width]);
    upper_values.extend_from_slice(&values[width * 2..]);
    let upper = compare_rows(&upper_values, width, BinaryOp::LtEq)?;
    negate(sql_and(lower, upper)?, negated)
}

fn evaluate_in(values: &[SqlValue], width: usize, negated: bool) -> Result<SqlValue> {
    if values.len() < width || !(values.len() - width).is_multiple_of(width) {
        return Err(malformed("row IN"));
    }
    let subject = &values[..width];
    let mut unknown = false;
    for candidate in values[width..].chunks_exact(width) {
        let mut pair = Vec::with_capacity(width * 2);
        pair.extend_from_slice(subject);
        pair.extend_from_slice(candidate);
        match compare_rows(&pair, width, BinaryOp::Eq)? {
            SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(!negated)),
            SqlValue::Boolean(false) => {}
            SqlValue::Null => unknown = true,
            other => return Err(non_boolean(other)),
        }
    }
    if unknown {
        Ok(SqlValue::Null)
    } else {
        Ok(SqlValue::Boolean(negated))
    }
}

fn sql_and(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    binary_op::eval_binary_values(&BinaryOp::And, left, right)
}

fn negate(value: SqlValue, negated: bool) -> Result<SqlValue> {
    if !negated {
        return Ok(value);
    }
    match value {
        SqlValue::Boolean(value) => Ok(SqlValue::Boolean(!value)),
        SqlValue::Null => Ok(SqlValue::Null),
        other => Err(non_boolean(other)),
    }
}

fn parse_flag(value: Option<&str>, name: &str) -> Result<bool> {
    match value {
        Some("0") => Ok(false),
        Some("1") => Ok(true),
        _ => Err(malformed(name)),
    }
}

fn malformed(name: &str) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::UnsupportedFunction(name.to_string()))
}

fn non_boolean(value: SqlValue) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::TypeMismatch {
        expected: "Boolean".into(),
        actual: value.type_name().into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truth_predicate_matrix_is_total() {
        let cases = [
            (SqlValue::Boolean(true), [true, false, false]),
            (SqlValue::Boolean(false), [false, true, false]),
            (SqlValue::Null, [false, false, true]),
        ];
        let predicates = [
            INTERNAL_TRUTH_TRUE,
            INTERNAL_TRUTH_FALSE,
            INTERNAL_TRUTH_UNKNOWN,
        ];

        for (value, expected) in cases {
            for (predicate, expected) in predicates.iter().zip(expected) {
                assert_eq!(
                    evaluate_truth(predicate, "0", std::slice::from_ref(&value)).unwrap(),
                    SqlValue::Boolean(expected)
                );
                assert_eq!(
                    evaluate_truth(predicate, "1", std::slice::from_ref(&value)).unwrap(),
                    SqlValue::Boolean(!expected)
                );
            }
        }
    }
}
