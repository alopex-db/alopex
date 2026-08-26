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
    eval_ascii => "ascii", eval_chr => "chr", eval_bit_length => "bit_length",
    eval_starts_with => "starts_with", eval_ends_with => "ends_with",
    eval_translate => "translate", eval_levenshtein => "levenshtein",
    eval_upper => "upper", eval_lower => "lower", eval_initcap => "initcap",
    eval_substr => "substr", eval_left => "left", eval_right => "right", eval_trim => "trim",
    eval_ltrim => "ltrim", eval_rtrim => "rtrim", eval_replace => "replace", eval_instr => "instr",
    eval_strpos => "strpos", eval_concat => "concat", eval_concat_ws => "concat_ws",
    eval_repeat => "repeat", eval_reverse => "reverse", eval_lpad => "lpad", eval_rpad => "rpad",
    eval_split_part => "split_part", eval_regexp_replace => "regexp_replace",
    eval_regexp_match => "regexp_match", eval_regexp_matches => "regexp_matches",
    eval_regexp_like => "regexp_like",
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
        "ascii" => Ok(SqlValue::Integer(
            text(0)?.chars().next().map_or(0, |ch| ch as i32),
        )),
        "chr" => {
            let value = numeric_i64(values.first())?;
            let character = u32::try_from(value)
                .ok()
                .filter(|value| *value != 0)
                .and_then(char::from_u32)
                .ok_or_else(|| {
                    invalid_argument(name, "value is not a valid non-zero Unicode scalar")
                })?;
            Ok(SqlValue::Text(character.to_string()))
        }
        "bit_length" => {
            let bytes = match values.first() {
                Some(SqlValue::Text(value)) => value.len(),
                Some(SqlValue::Blob(value)) => value.len(),
                _ => 0,
            };
            let bits = bytes
                .checked_mul(8)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or(ExecutorError::Evaluation(EvaluationError::Overflow))?;
            Ok(SqlValue::Integer(bits))
        }
        "starts_with" => Ok(SqlValue::Boolean(text(0)?.starts_with(text(1)?))),
        "ends_with" => Ok(SqlValue::Boolean(text(0)?.ends_with(text(1)?))),
        "translate" => Ok(SqlValue::Text(translate(text(0)?, text(1)?, text(2)?))),
        "levenshtein" => Ok(SqlValue::Integer(levenshtein(text(0)?, text(1)?)?)),
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
            validate_regex_input(text(0)?, text(1)?)?;
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
        "regexp_like" => regexp_like(
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

fn invalid_argument(function: &str, reason: &str) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::InvalidArgument {
        function: function.into(),
        reason: reason.into(),
    })
}

fn translate(value: &str, from: &str, to: &str) -> String {
    let from: Vec<char> = from.chars().collect();
    let to: Vec<char> = to.chars().collect();
    value
        .chars()
        .filter_map(|character| {
            from.iter()
                .position(|candidate| *candidate == character)
                .map_or(Some(character), |index| to.get(index).copied())
        })
        .collect()
}

fn levenshtein(left: &str, right: &str) -> Result<i32> {
    if left.len() > 1_000_000 || right.len() > 1_000_000 {
        return Err(invalid_argument(
            "levenshtein",
            "each input must be at most one million bytes",
        ));
    }
    let mut left: Vec<char> = left.chars().collect();
    let mut right: Vec<char> = right.chars().collect();
    if left.len() < right.len() {
        std::mem::swap(&mut left, &mut right);
    }
    if left.len().saturating_mul(right.len()) > 1_000_000 {
        return Err(invalid_argument(
            "levenshtein",
            "inputs exceed one million comparison cells",
        ));
    }

    let mut costs: Vec<usize> = (0..=right.len()).collect();
    for (left_index, left_char) in left.iter().enumerate() {
        let mut diagonal = costs[0];
        costs[0] = left_index + 1;
        for (right_index, right_char) in right.iter().enumerate() {
            let above = costs[right_index + 1];
            costs[right_index + 1] = if left_char == right_char {
                diagonal
            } else {
                1 + diagonal.min(above).min(costs[right_index])
            };
            diagonal = above;
        }
    }
    i32::try_from(costs[right.len()])
        .map_err(|_| ExecutorError::Evaluation(EvaluationError::Overflow))
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
    validate_regex_flags(pattern, flags)?;
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

fn validate_regex_flags(pattern: &str, flags: &str) -> Result<()> {
    let mut seen = [false; 3];
    for flag in flags.chars() {
        let index = match flag {
            'i' => 0,
            'm' => 1,
            's' => 2,
            _ => {
                return Err(ExecutorError::Evaluation(EvaluationError::InvalidRegex {
                    pattern: pattern.into(),
                    reason: format!("unsupported flag '{flag}'"),
                }));
            }
        };
        if std::mem::replace(&mut seen[index], true) {
            return Err(ExecutorError::Evaluation(EvaluationError::InvalidRegex {
                pattern: pattern.into(),
                reason: format!("duplicate flag '{flag}'"),
            }));
        }
    }
    Ok(())
}

fn validate_regex_input(value: &str, pattern: &str) -> Result<()> {
    if value.len() > 1_048_576 {
        return Err(ExecutorError::Evaluation(EvaluationError::InvalidRegex {
            pattern: pattern.into(),
            reason: "input exceeds 1 MiB".into(),
        }));
    }
    Ok(())
}

fn regexp_match(value: &str, pattern: &str, flags: &str) -> Result<SqlValue> {
    validate_regex_input(value, pattern)?;
    let regex = compile_regex(pattern, flags)?;
    Ok(regex
        .find(value)
        .map(|m| SqlValue::Text(m.as_str().into()))
        .unwrap_or(SqlValue::Null))
}

fn regexp_like(value: &str, pattern: &str, flags: &str) -> Result<SqlValue> {
    validate_regex_input(value, pattern)?;
    Ok(SqlValue::Boolean(
        compile_regex(pattern, flags)?.is_match(value),
    ))
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

    #[test]
    fn portable_string_functions_are_unicode_aware() {
        assert_eq!(
            eval("ascii", &[SqlValue::Text("あ".into())]),
            SqlValue::Integer('あ' as i32)
        );
        assert_eq!(
            eval("ascii", &[SqlValue::Text(String::new())]),
            SqlValue::Integer(0)
        );
        assert_eq!(
            eval("chr", &[SqlValue::Integer('猫' as i32)]),
            SqlValue::Text("猫".into())
        );
        assert_eq!(
            eval("bit_length", &[SqlValue::Text("あ".into())]),
            SqlValue::Integer(24)
        );
        assert_eq!(
            eval(
                "starts_with",
                &[
                    SqlValue::Text("東京駅".into()),
                    SqlValue::Text("東京".into())
                ]
            ),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            eval(
                "ends_with",
                &[SqlValue::Text("東京駅".into()), SqlValue::Text("駅".into())]
            ),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            eval(
                "translate",
                &[
                    SqlValue::Text("ábca".into()),
                    SqlValue::Text("aá".into()),
                    SqlValue::Text("XY".into()),
                ]
            ),
            SqlValue::Text("YbcX".into())
        );
        assert_eq!(
            eval(
                "translate",
                &[
                    SqlValue::Text("abc".into()),
                    SqlValue::Text("abc".into()),
                    SqlValue::Text("XY".into()),
                ]
            ),
            SqlValue::Text("XY".into())
        );
        assert_eq!(
            eval(
                "levenshtein",
                &[SqlValue::Text("猫".into()), SqlValue::Text("子猫".into())]
            ),
            SqlValue::Integer(1)
        );
    }

    #[test]
    fn regex_like_shares_flag_and_error_rules() {
        assert_eq!(
            eval(
                "regexp_like",
                &[
                    SqlValue::Text("Abc\nxyz".into()),
                    SqlValue::Text("^abc".into()),
                    SqlValue::Text("im".into()),
                ]
            ),
            SqlValue::Boolean(true)
        );

        for name in ["regexp_matches", "regexp_like"] {
            for flags in ["x", "ii"] {
                let error = eval_for(name).unwrap()(&[
                    SqlValue::Text("abc".into()),
                    SqlValue::Text("abc".into()),
                    SqlValue::Text(flags.into()),
                ])
                .unwrap_err();
                assert!(matches!(
                    error,
                    ExecutorError::Evaluation(EvaluationError::InvalidRegex { .. })
                ));
            }
        }
    }

    #[test]
    fn chr_rejects_invalid_unicode_scalars() {
        for value in [0, -1, 0xd800, 0x11_0000] {
            let error = eval_for("chr").unwrap()(&[SqlValue::Integer(value)]);
            assert!(matches!(
                error,
                Err(ExecutorError::Evaluation(
                    EvaluationError::InvalidArgument { .. }
                ))
            ));
        }
    }

    #[test]
    fn levenshtein_rejects_excessive_work() {
        let left = "a".repeat(1_001);
        let right = "b".repeat(1_000);
        let error =
            eval_for("levenshtein").unwrap()(&[SqlValue::Text(left), SqlValue::Text(right)]);
        assert!(matches!(
            error,
            Err(ExecutorError::Evaluation(
                EvaluationError::InvalidArgument { .. }
            ))
        ));

        let error = eval_for("levenshtein").unwrap()(&[
            SqlValue::Text(String::new()),
            SqlValue::Text("a".repeat(1_000_001)),
        ]);
        assert!(matches!(
            error,
            Err(ExecutorError::Evaluation(
                EvaluationError::InvalidArgument { .. }
            ))
        ));
    }
}
