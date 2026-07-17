//! Evaluation of LIKE-family operators.

use regex::RegexBuilder;

use crate::ast::expr::PatternMatchKind;
use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::planner::typed_expr::TypedExpr;
use crate::storage::SqlValue;

use super::{EvalContext, evaluate};

pub fn evaluate_pattern(
    expr: &TypedExpr,
    pattern: &TypedExpr,
    escape: Option<&TypedExpr>,
    negated: bool,
    kind: PatternMatchKind,
    ctx: &EvalContext<'_>,
) -> Result<SqlValue> {
    let value = evaluate(expr, ctx)?;
    let pattern_value = evaluate(pattern, ctx)?;
    if value.is_null() || pattern_value.is_null() {
        return Ok(SqlValue::Null);
    }
    let (SqlValue::Text(value), SqlValue::Text(pattern)) = (value, pattern_value) else {
        return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Text".into(),
            actual: "non-text pattern operand".into(),
        }));
    };
    let escape = if let Some(escape_expr) = escape {
        match evaluate(escape_expr, ctx)? {
            SqlValue::Text(value) if value.chars().count() <= 1 => value.chars().next(),
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => {
                return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                    expected: "single Text escape".into(),
                    actual: "invalid escape".into(),
                }));
            }
        }
    } else {
        None
    };
    let matched = match kind {
        PatternMatchKind::Like => sql_pattern_match(&value, &pattern, escape, false)?,
        PatternMatchKind::ILike => sql_pattern_match(&value, &pattern, escape, true)?,
        PatternMatchKind::Glob => glob_match(&value, &pattern)?,
        PatternMatchKind::SimilarTo => similar_to_match(&value, &pattern)?,
    };
    Ok(SqlValue::Boolean(if negated { !matched } else { matched }))
}

fn similar_to_match(value: &str, pattern: &str) -> Result<bool> {
    let mut regex = String::from("^(?:");
    let mut escaped = false;
    for ch in pattern.chars() {
        if escaped {
            regex.push_str(&regex::escape(&ch.to_string()));
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '%' {
            regex.push_str(".*");
        } else if ch == '_' {
            regex.push('.');
        } else {
            regex.push(ch);
        }
    }
    if escaped {
        return Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedExpression("dangling escape in SIMILAR TO pattern".into()),
        ));
    }
    regex.push_str(")$");
    RegexBuilder::new(&regex)
        .build()
        .map(|re| re.is_match(value))
        .map_err(|error| {
            ExecutorError::Evaluation(EvaluationError::InvalidRegex {
                pattern: pattern.into(),
                reason: error.to_string(),
            })
        })
}

fn sql_pattern_match(
    value: &str,
    pattern: &str,
    escape: Option<char>,
    insensitive: bool,
) -> Result<bool> {
    let mut regex = String::from("^(?:");
    let mut escaped = false;
    for ch in pattern.chars() {
        if escaped {
            regex.push_str(&regex::escape(&ch.to_string()));
            escaped = false;
            continue;
        }
        if Some(ch) == escape {
            escaped = true;
            continue;
        }
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            other => regex.push_str(&regex::escape(&other.to_string())),
        }
    }
    if escaped {
        return Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedExpression("dangling ESCAPE character".into()),
        ));
    }
    regex.push_str(")$");
    RegexBuilder::new(&regex)
        .case_insensitive(insensitive)
        .build()
        .map(|re| re.is_match(value))
        .map_err(|error| {
            ExecutorError::Evaluation(EvaluationError::InvalidRegex {
                pattern: pattern.into(),
                reason: error.to_string(),
            })
        })
}

fn glob_match(value: &str, pattern: &str) -> Result<bool> {
    let mut regex = String::from("^(?:");
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '[' => {
                regex.push('[');
                for inner in chars.by_ref() {
                    regex.push(inner);
                    if inner == ']' {
                        break;
                    }
                }
            }
            other => regex.push_str(&regex::escape(&other.to_string())),
        }
    }
    regex.push_str(")$");
    regex::Regex::new(&regex)
        .map(|re| re.is_match(value))
        .map_err(|error| {
            ExecutorError::Evaluation(EvaluationError::InvalidRegex {
                pattern: pattern.into(),
                reason: error.to_string(),
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::{Literal, PatternMatchKind};
    use crate::ast::span::Span;
    use crate::planner::types::ResolvedType;

    fn text(value: &str) -> TypedExpr {
        TypedExpr::literal(
            Literal::String(value.into()),
            ResolvedType::Text,
            Span::empty(),
        )
    }

    fn eval(kind: PatternMatchKind, value: &str, pattern: &str) -> SqlValue {
        let ctx = EvalContext::new(&[]);
        evaluate_pattern(&text(value), &text(pattern), None, false, kind, &ctx).unwrap()
    }

    #[test]
    fn like_family_has_distinct_matching_rules() {
        assert_eq!(
            eval(PatternMatchKind::Like, "Alice", "A%"),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            eval(PatternMatchKind::ILike, "Alice", "a%"),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            eval(PatternMatchKind::Glob, "a.sql", "*.sql"),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            eval(PatternMatchKind::SimilarTo, "alice", "(alice|bob)%"),
            SqlValue::Boolean(true)
        );
    }
}
