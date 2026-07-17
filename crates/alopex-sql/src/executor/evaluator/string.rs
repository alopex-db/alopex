//! String and regular-expression scalar functions.

use regex::RegexBuilder;

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
    eval_length => "length", eval_char_length => "char_length", eval_octet_length => "octet_length",
    eval_upper => "upper", eval_lower => "lower", eval_initcap => "initcap",
    eval_substr => "substr", eval_left => "left", eval_right => "right", eval_trim => "trim",
    eval_ltrim => "ltrim", eval_rtrim => "rtrim", eval_replace => "replace", eval_instr => "instr",
    eval_strpos => "strpos", eval_concat => "concat", eval_concat_ws => "concat_ws",
    eval_repeat => "repeat", eval_reverse => "reverse", eval_lpad => "lpad", eval_rpad => "rpad",
    eval_split_part => "split_part", eval_regexp_replace => "regexp_replace",
    eval_regexp_match => "regexp_match", eval_regexp_matches => "regexp_matches",
);

fn eval_named(name: &str, values: &[SqlValue]) -> Result<SqlValue> {
    if name == "concat" {
        return Ok(SqlValue::Text(values.iter().filter_map(as_text).collect()));
    }
    if name == "concat_ws" {
        let sep = match values.first() {
            Some(SqlValue::Text(s)) => s.as_str(),
            Some(SqlValue::Null) | None => "",
            _ => return Err(type_error("Text", values.first())),
        };
        return Ok(SqlValue::Text(
            values[1..]
                .iter()
                .filter_map(as_text)
                .collect::<Vec<_>>()
                .join(sep),
        ));
    }
    if values.iter().any(SqlValue::is_null) {
        return Ok(SqlValue::Null);
    }
    let text = |index: usize| -> Result<&str> {
        values
            .get(index)
            .and_then(as_text)
            .ok_or_else(|| type_error("Text", values.get(index)))
    };
    match name {
        "length" => Ok(SqlValue::Integer(match values.first() {
            Some(SqlValue::Text(s)) => s.chars().count(),
            Some(SqlValue::Blob(b)) => b.len(),
            _ => 0,
        } as i32)),
        "char_length" => Ok(SqlValue::Integer(text(0)?.chars().count() as i32)),
        "octet_length" => Ok(SqlValue::Integer(match values.first() {
            Some(SqlValue::Text(s)) => s.len(),
            Some(SqlValue::Blob(b)) => b.len(),
            _ => 0,
        } as i32)),
        "upper" => Ok(SqlValue::Text(text(0)?.to_uppercase())),
        "lower" => Ok(SqlValue::Text(text(0)?.to_lowercase())),
        "initcap" => Ok(SqlValue::Text(initcap(text(0)?))),
        "substr" => substring(values, text(0)?, 1),
        "left" => take_side(values, text(0)?, true),
        "right" => take_side(values, text(0)?, false),
        "trim" => {
            let (chars, value) = if values.len() == 1 {
                (" \t\n\r", text(0)?)
            } else {
                (text(0)?, text(1)?)
            };
            Ok(SqlValue::Text(
                value.trim_matches(|c| chars.contains(c)).into(),
            ))
        }
        "ltrim" | "rtrim" => {
            let (chars, value) = if values.len() == 1 {
                (" \t\n\r", text(0)?)
            } else {
                (text(0)?, text(1)?)
            };
            let result = if name == "ltrim" {
                value.trim_start_matches(|c| chars.contains(c))
            } else {
                value.trim_end_matches(|c| chars.contains(c))
            };
            Ok(SqlValue::Text(result.into()))
        }
        "replace" => Ok(SqlValue::Text(text(0)?.replace(text(1)?, text(2)?))),
        "instr" | "strpos" => {
            let haystack = text(0)?;
            let needle = text(1)?;
            Ok(SqlValue::Integer(
                haystack
                    .find(needle)
                    .map(|i| haystack[..i].chars().count() as i32 + 1)
                    .unwrap_or(0),
            ))
        }
        "repeat" => {
            let count = numeric_i64(values.get(1))?;
            Ok(SqlValue::Text(if count <= 0 {
                String::new()
            } else {
                text(0)?.repeat(count as usize)
            }))
        }
        "reverse" => Ok(SqlValue::Text(text(0)?.chars().rev().collect())),
        "lpad" => pad(values, text(0)?, true),
        "rpad" => pad(values, text(0)?, false),
        "split_part" => {
            let delimiter = text(1)?;
            let index = numeric_i64(values.get(2))?;
            if index <= 0 {
                return Ok(SqlValue::Null);
            }
            Ok(SqlValue::Text(
                text(0)?
                    .split(delimiter)
                    .nth((index - 1) as usize)
                    .unwrap_or("")
                    .into(),
            ))
        }
        "regexp_replace" => {
            if text(0)?.len() > 1_048_576 {
                return Err(ExecutorError::Evaluation(EvaluationError::InvalidRegex {
                    pattern: text(1)?.into(),
                    reason: "input exceeds 1 MiB".into(),
                }));
            }
            let regex = compile_regex(text(1)?, "")?;
            Ok(SqlValue::Text(
                regex.replace_all(text(0)?, text(2)?).into_owned(),
            ))
        }
        "regexp_match" => regexp_match(text(0)?, text(1)?, ""),
        "regexp_matches" => regexp_match(
            text(0)?,
            text(1)?,
            values.get(2).and_then(as_text).unwrap_or(""),
        ),
        _ => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(name.into()),
        )),
    }
}

fn as_text(value: &SqlValue) -> Option<&str> {
    match value {
        SqlValue::Text(s) => Some(s),
        _ => None,
    }
}

fn type_error(expected: &str, value: Option<&SqlValue>) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::TypeMismatch {
        expected: expected.into(),
        actual: value.map(SqlValue::type_name).unwrap_or("missing").into(),
    })
}

fn numeric_i64(value: Option<&SqlValue>) -> Result<i64> {
    match value {
        Some(SqlValue::Integer(v)) => Ok(*v as i64),
        Some(SqlValue::BigInt(v)) => Ok(*v),
        Some(other) => Err(type_error("Integer", Some(other))),
        None => Err(type_error("Integer", None)),
    }
}

fn initcap(value: &str) -> String {
    value
        .split_inclusive(char::is_whitespace)
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
                None => String::new(),
            }
        })
        .collect()
}

fn substring(values: &[SqlValue], value: &str, start_index: usize) -> Result<SqlValue> {
    let start = numeric_i64(values.get(start_index))?;
    let chars: Vec<char> = value.chars().collect();
    let begin = if start <= 0 { 0 } else { (start - 1) as usize }.min(chars.len());
    let end = match values.get(start_index + 1) {
        Some(_) => begin
            .saturating_add(numeric_i64(values.get(start_index + 1))?.max(0) as usize)
            .min(chars.len()),
        None => chars.len(),
    };
    Ok(SqlValue::Text(chars[begin..end].iter().collect()))
}

fn take_side(values: &[SqlValue], value: &str, left: bool) -> Result<SqlValue> {
    let count = numeric_i64(values.get(1))?.max(0) as usize;
    let chars: Vec<char> = value.chars().collect();
    let result = if left {
        chars.into_iter().take(count).collect()
    } else {
        chars
            .into_iter()
            .rev()
            .take(count)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    };
    Ok(SqlValue::Text(result))
}

fn pad(values: &[SqlValue], value: &str, left: bool) -> Result<SqlValue> {
    let length = numeric_i64(values.get(1))?.max(0) as usize;
    let fill = if values.len() > 2 {
        as_text(values.get(2).unwrap()).ok_or_else(|| type_error("Text", values.get(2)))?
    } else {
        " "
    };
    let fill_chars: Vec<char> = if fill.is_empty() {
        vec![' ']
    } else {
        fill.chars().collect()
    };
    let value_chars: Vec<char> = value.chars().take(length).collect();
    let padding = (0..length.saturating_sub(value_chars.len()))
        .map(|index| fill_chars[index % fill_chars.len()]);
    let result: Vec<char> = if left {
        padding.chain(value_chars).collect()
    } else {
        value_chars.into_iter().chain(padding).collect()
    };
    Ok(SqlValue::Text(result.into_iter().collect()))
}

fn compile_regex(pattern: &str, flags: &str) -> Result<regex::Regex> {
    if pattern.len() > 4096 {
        return Err(ExecutorError::Evaluation(EvaluationError::InvalidRegex {
            pattern: pattern.into(),
            reason: "pattern exceeds 4 KiB".into(),
        }));
    }
    let mut builder = RegexBuilder::new(pattern);
    builder
        .case_insensitive(flags.contains('i'))
        .dot_matches_new_line(flags.contains('s'))
        .multi_line(flags.contains('m'));
    builder.build().map_err(|error| {
        ExecutorError::Evaluation(EvaluationError::InvalidRegex {
            pattern: pattern.into(),
            reason: error.to_string(),
        })
    })
}

fn regexp_match(value: &str, pattern: &str, flags: &str) -> Result<SqlValue> {
    if value.len() > 1_048_576 {
        return Err(ExecutorError::Evaluation(EvaluationError::InvalidRegex {
            pattern: pattern.into(),
            reason: "input exceeds 1 MiB".into(),
        }));
    }
    let regex = compile_regex(pattern, flags)?;
    Ok(regex
        .find(value)
        .map(|m| SqlValue::Text(m.as_str().into()))
        .unwrap_or(SqlValue::Null))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(name: &str, values: &[SqlValue]) -> SqlValue {
        eval_for(name).expect("registered string function")(values).unwrap()
    }

    #[test]
    fn length_distinguishes_characters_and_bytes() {
        assert_eq!(
            eval("length", &[SqlValue::Text("あ".into())]),
            SqlValue::Integer(1)
        );
        assert_eq!(
            eval("length", &[SqlValue::Blob(vec![1, 2, 3])]),
            SqlValue::Integer(3)
        );
        assert_eq!(
            eval("octet_length", &[SqlValue::Text("あ".into())]),
            SqlValue::Integer(3)
        );
    }

    #[test]
    fn standard_string_functions_and_regex_return_expected_values() {
        assert_eq!(
            eval(
                "substr",
                &[
                    SqlValue::Text("abcdef".into()),
                    SqlValue::Integer(2),
                    SqlValue::Integer(3),
                ],
            ),
            SqlValue::Text("bcd".into())
        );
        assert_eq!(
            eval(
                "regexp_match",
                &[
                    SqlValue::Text("abc123".into()),
                    SqlValue::Text(r"[0-9]+".into())
                ],
            ),
            SqlValue::Text("123".into())
        );
        let invalid = eval_for("regexp_match").unwrap()(&[
            SqlValue::Text("abc".into()),
            SqlValue::Text("[".into()),
        ]);
        assert!(matches!(
            invalid,
            Err(ExecutorError::Evaluation(
                EvaluationError::InvalidRegex { .. }
            ))
        ));
    }

    #[test]
    fn padding_handles_truncation_and_multicharacter_fill() {
        assert_eq!(
            eval(
                "lpad",
                &[SqlValue::Text("abc".into()), SqlValue::Integer(2)]
            ),
            SqlValue::Text("ab".into())
        );
        assert_eq!(
            eval(
                "lpad",
                &[
                    SqlValue::Text("abc".into()),
                    SqlValue::Integer(6),
                    SqlValue::Text("xy".into()),
                ],
            ),
            SqlValue::Text("xyxabc".into())
        );
        assert_eq!(
            eval(
                "rpad",
                &[SqlValue::Text("abc".into()), SqlValue::Integer(5)]
            ),
            SqlValue::Text("abc  ".into())
        );
    }
}
