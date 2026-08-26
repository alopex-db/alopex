//! Numeric and trigonometric scalar functions.

use crate::executor::{EvaluationError, ExecutorError, Result};
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
    eval_abs => "abs", eval_sign => "sign", eval_round => "round",
    eval_floor => "floor", eval_ceil => "ceil", eval_ceiling => "ceiling",
    eval_trunc => "trunc", eval_mod => "mod", eval_power => "power",
    eval_pow => "pow", eval_sqrt => "sqrt", eval_exp => "exp", eval_ln => "ln",
    eval_log => "log", eval_log10 => "log10", eval_random => "random",
    eval_cbrt => "cbrt", eval_cot => "cot", eval_log2 => "log2",
    eval_acosh => "acosh", eval_asinh => "asinh", eval_atanh => "atanh",
    eval_cosh => "cosh", eval_sinh => "sinh", eval_tanh => "tanh", eval_isnan => "isnan",
    eval_sin => "sin", eval_cos => "cos", eval_tan => "tan", eval_asin => "asin",
    eval_acos => "acos", eval_atan => "atan", eval_atan2 => "atan2",
    eval_degrees => "degrees", eval_radians => "radians", eval_pi => "pi",
);

fn eval_named(name: &str, values: &[SqlValue]) -> Result<SqlValue> {
    if name == "random" {
        return Ok(SqlValue::Double(rand::random()));
    }
    if name == "pi" {
        return Ok(SqlValue::Double(std::f64::consts::PI));
    }
    if values.iter().any(SqlValue::is_null) {
        return Ok(SqlValue::Null);
    }
    let Some(first) = values.first() else {
        return Ok(SqlValue::Null);
    };

    let unary_float =
        |f: fn(f64) -> f64| -> Result<SqlValue> { Ok(SqlValue::Double(f(as_f64(first)?))) };
    match name {
        "abs" => unary_preserve(first, |v| v.abs()),
        "sign" => Ok(SqlValue::Integer(if as_f64(first)? > 0.0 {
            1
        } else if as_f64(first)? < 0.0 {
            -1
        } else {
            0
        })),
        "floor" => unary_preserve(first, |v| v.floor()),
        "ceil" | "ceiling" => unary_preserve(first, |v| v.ceil()),
        "round" | "trunc" => {
            let value = as_f64(first)?;
            let digits = values.get(1).map(as_f64).transpose()?.unwrap_or(0.0);
            let factor = 10_f64.powf(digits);
            let rounded = if name == "round" {
                (value * factor).round() / factor
            } else {
                (value * factor).trunc() / factor
            };
            preserve_numeric(first, rounded)
        }
        "mod" => {
            let rhs = as_f64(values.get(1).ok_or_else(|| invalid_args(name))?)?;
            if rhs == 0.0 {
                return Ok(SqlValue::Null);
            }
            preserve_numeric(first, as_f64(first)? % rhs)
        }
        "power" | "pow" => binary_float(values, |a, b| a.powf(b)),
        "sqrt" => domain_float(first, |v| if v >= 0.0 { Some(v.sqrt()) } else { None }),
        "exp" => unary_float(|v| v.exp()),
        "ln" => domain_float(first, |v| if v > 0.0 { Some(v.ln()) } else { None }),
        "log" => {
            if values.len() == 1 {
                domain_float(first, |v| if v > 0.0 { Some(v.log10()) } else { None })
            } else {
                let base = as_f64(first)?;
                let value = as_f64(values.get(1).ok_or_else(|| invalid_args(name))?)?;
                if base > 0.0 && base != 1.0 && value > 0.0 {
                    Ok(SqlValue::Double(value.log(base)))
                } else {
                    Ok(SqlValue::Null)
                }
            }
        }
        "log10" => domain_float(first, |v| if v > 0.0 { Some(v.log10()) } else { None }),
        "cbrt" => unary_float(|v| v.cbrt()),
        "cot" => unary_float(|v| 1.0 / v.tan()),
        "log2" => domain_float(first, |v| if v > 0.0 { Some(v.log2()) } else { None }),
        "acosh" => domain_float(first, |v| if v >= 1.0 { Some(v.acosh()) } else { None }),
        "asinh" => unary_float(|v| v.asinh()),
        "atanh" => domain_float(first, |v| {
            if (-1.0..1.0).contains(&v) {
                Some(v.atanh())
            } else {
                None
            }
        }),
        "cosh" => unary_float(|v| v.cosh()),
        "sinh" => unary_float(|v| v.sinh()),
        "tanh" => unary_float(|v| v.tanh()),
        "isnan" => Ok(SqlValue::Boolean(as_f64(first)?.is_nan())),
        "sin" => unary_float(|v| v.sin()),
        "cos" => unary_float(|v| v.cos()),
        "tan" => unary_float(|v| v.tan()),
        "asin" => domain_float(first, |v| {
            if (-1.0..=1.0).contains(&v) {
                Some(v.asin())
            } else {
                None
            }
        }),
        "acos" => domain_float(first, |v| {
            if (-1.0..=1.0).contains(&v) {
                Some(v.acos())
            } else {
                None
            }
        }),
        "atan" => unary_float(|v| v.atan()),
        "atan2" => binary_float(values, |y, x| y.atan2(x)),
        "degrees" => unary_float(|v| v.to_degrees()),
        "radians" => unary_float(|v| v.to_radians()),
        _ => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(name.into()),
        )),
    }
}

fn invalid_args(name: &str) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::UnsupportedFunction(format!(
        "{name}: invalid arguments"
    )))
}

fn as_f64(value: &SqlValue) -> Result<f64> {
    match value {
        SqlValue::Integer(v) => Ok(*v as f64),
        SqlValue::BigInt(v) => Ok(*v as f64),
        SqlValue::Float(v) => Ok(*v as f64),
        SqlValue::Double(v) => Ok(*v),
        other => Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
            expected: "Numeric".into(),
            actual: other.type_name().into(),
        })),
    }
}

fn preserve_numeric(original: &SqlValue, value: f64) -> Result<SqlValue> {
    Ok(match original {
        SqlValue::Integer(_) => SqlValue::Integer(value as i32),
        SqlValue::BigInt(_) => SqlValue::BigInt(value as i64),
        SqlValue::Float(_) => SqlValue::Float(value as f32),
        SqlValue::Double(_) => SqlValue::Double(value),
        other => {
            return Err(ExecutorError::Evaluation(EvaluationError::TypeMismatch {
                expected: "Numeric".into(),
                actual: other.type_name().into(),
            }));
        }
    })
}

fn unary_preserve(value: &SqlValue, f: fn(f64) -> f64) -> Result<SqlValue> {
    preserve_numeric(value, f(as_f64(value)?))
}

fn domain_float(value: &SqlValue, f: fn(f64) -> Option<f64>) -> Result<SqlValue> {
    Ok(f(as_f64(value)?)
        .map(SqlValue::Double)
        .unwrap_or(SqlValue::Null))
}

fn binary_float(values: &[SqlValue], f: fn(f64, f64) -> f64) -> Result<SqlValue> {
    let left = as_f64(values.first().ok_or_else(|| invalid_args("numeric"))?)?;
    let right = as_f64(values.get(1).ok_or_else(|| invalid_args("numeric"))?)?;
    Ok(SqlValue::Double(f(left, right)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(name: &str, values: &[SqlValue]) -> SqlValue {
        eval_for(name).expect("registered numeric function")(values).unwrap()
    }

    #[test]
    fn logarithms_use_documented_bases() {
        assert_eq!(
            eval("log", &[SqlValue::Double(100.0)]),
            SqlValue::Double(2.0)
        );
        let ln = eval("ln", &[SqlValue::Double(std::f64::consts::E)]);
        assert!(matches!(ln, SqlValue::Double(value) if (value - 1.0).abs() < 1e-10));
        assert_eq!(eval("log", &[SqlValue::Double(-1.0)]), SqlValue::Null);
    }

    #[test]
    fn domain_errors_and_nulls_are_sql_null() {
        assert_eq!(eval("sqrt", &[SqlValue::Double(-1.0)]), SqlValue::Null);
        assert_eq!(eval("acos", &[SqlValue::Double(2.0)]), SqlValue::Null);
        assert_eq!(eval("abs", &[SqlValue::Null]), SqlValue::Null);
    }

    #[test]
    fn portable_math_functions_cover_values_domains_and_non_finite_results() {
        let cases = [
            ("cbrt", -8.0, -2.0),
            ("cot", std::f64::consts::FRAC_PI_4, 1.0),
            ("log2", 8.0, 3.0),
            ("acosh", 1.0, 0.0),
            ("asinh", 0.0, 0.0),
            ("atanh", 0.0, 0.0),
            ("cosh", 0.0, 1.0),
            ("sinh", 0.0, 0.0),
            ("tanh", 0.0, 0.0),
        ];
        for (name, input, expected) in cases {
            let SqlValue::Double(actual) = eval(name, &[SqlValue::Double(input)]) else {
                panic!("{name} did not return DOUBLE");
            };
            assert!((actual - expected).abs() < 1e-12, "{name}: {actual}");
        }

        assert_eq!(eval("log2", &[SqlValue::Double(0.0)]), SqlValue::Null);
        assert_eq!(eval("acosh", &[SqlValue::Double(0.5)]), SqlValue::Null);
        assert_eq!(eval("atanh", &[SqlValue::Double(1.0)]), SqlValue::Null);
        assert!(matches!(
            eval("cot", &[SqlValue::Double(0.0)]),
            SqlValue::Double(value) if value.is_infinite()
        ));
        assert!(matches!(
            eval("cosh", &[SqlValue::Double(1_000.0)]),
            SqlValue::Double(value) if value.is_infinite()
        ));
        assert_eq!(
            eval("isnan", &[SqlValue::Double(f64::NAN)]),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            eval("isnan", &[SqlValue::Double(f64::INFINITY)]),
            SqlValue::Boolean(false)
        );
        assert_eq!(eval("isnan", &[SqlValue::Null]), SqlValue::Null);
    }

    #[test]
    fn random_is_in_unit_interval() {
        match eval("random", &[]) {
            SqlValue::Double(value) => assert!((0.0..1.0).contains(&value)),
            other => panic!("unexpected random result: {other:?}"),
        }
    }
}
