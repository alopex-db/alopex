//! Hash, UUID, and binary encoding scalar functions.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use md5::{Digest, Md5};
use sha2::Sha256;

use crate::executor::{EvaluationError, ExecutorError, Result};
use crate::storage::SqlValue;

/// Maximum byte length accepted by hash and encoding functions.
pub const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;

macro_rules! wrappers {
    ($($fn_name:ident => $name:literal),+ $(,)?) => {
        $(fn $fn_name(values: &[SqlValue]) -> Result<SqlValue> { eval_named($name, values) })+
        pub fn eval_for(name: &str) -> Option<fn(&[SqlValue]) -> Result<SqlValue>> {
            match name { $( $name => Some($fn_name), )+ _ => None }
        }
    };
}

wrappers!(
    eval_sha256 => "sha256", eval_md5 => "md5", eval_simhash => "simhash",
    eval_hamming_distance => "hamming_distance", eval_gen_random_uuid => "gen_random_uuid",
    eval_uuidv7 => "uuidv7", eval_hex => "hex", eval_unhex => "unhex",
    eval_encode => "encode", eval_decode => "decode",
);

fn invalid(function: &str, reason: impl Into<String>) -> ExecutorError {
    ExecutorError::Evaluation(EvaluationError::InvalidArgument {
        function: function.into(),
        reason: reason.into(),
    })
}

fn input_bytes<'a>(function: &str, value: &'a SqlValue) -> Result<Option<&'a [u8]>> {
    let bytes = match value {
        SqlValue::Text(value) => value.as_bytes(),
        SqlValue::Blob(value) => value.as_slice(),
        SqlValue::Null => return Ok(None),
        other => {
            return Err(invalid(
                function,
                format!("expected Text or Blob, got {}", other.type_name()),
            ));
        }
    };
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(invalid(
            function,
            format!("input exceeds {MAX_INPUT_BYTES} bytes"),
        ));
    }
    Ok(Some(bytes))
}

fn text_input<'a>(function: &str, value: &'a SqlValue) -> Result<Option<&'a str>> {
    match value {
        SqlValue::Text(value) => {
            if value.len() > MAX_INPUT_BYTES {
                return Err(invalid(
                    function,
                    format!("input exceeds {MAX_INPUT_BYTES} bytes"),
                ));
            }
            Ok(Some(value))
        }
        SqlValue::Null => Ok(None),
        other => Err(invalid(
            function,
            format!("expected Text, got {}", other.type_name()),
        )),
    }
}

fn format_name<'a>(function: &str, value: &'a SqlValue) -> Result<Option<&'a str>> {
    let SqlValue::Text(format) = value else {
        return if value.is_null() {
            Ok(None)
        } else {
            Err(invalid(function, "format must be Text"))
        };
    };
    Ok(Some(format))
}

fn decode_format(function: &str, format: &str) -> Result<&'static str> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => Ok("hex"),
        "base64" => Ok("base64"),
        _ => Err(invalid(function, format!("unsupported format '{format}'"))),
    }
}

fn eval_named(name: &str, values: &[SqlValue]) -> Result<SqlValue> {
    let expected = match name {
        "hamming_distance" | "encode" | "decode" => 2,
        "gen_random_uuid" | "uuidv7" => 0,
        _ => 1,
    };
    if values.len() != expected {
        return Err(invalid(
            name,
            format!("expected {expected} argument(s), got {}", values.len()),
        ));
    }
    match name {
        "sha256" => {
            let Some(bytes) = input_bytes(name, &values[0])? else {
                return Ok(SqlValue::Null);
            };
            Ok(SqlValue::Blob(Sha256::digest(bytes).to_vec()))
        }
        "md5" => {
            let Some(bytes) = input_bytes(name, &values[0])? else {
                return Ok(SqlValue::Null);
            };
            Ok(SqlValue::Text(format!("{:x}", Md5::digest(bytes))))
        }
        "simhash" => {
            let Some(value) = text_input(name, &values[0])? else {
                return Ok(SqlValue::Null);
            };
            let mut sums = [0i32; 64];
            let mut count = 0;
            for token in value
                .as_bytes()
                .split(|byte| byte.is_ascii_whitespace())
                .filter(|token| !token.is_empty())
            {
                let digest = Sha256::digest(token);
                let hash = u64::from_be_bytes(
                    digest[..8]
                        .try_into()
                        .expect("sha256 digest has at least 8 bytes"),
                );
                count += 1;
                for (bit, sum) in sums.iter_mut().enumerate() {
                    if hash & (1u64 << bit) != 0 {
                        *sum += 1;
                    }
                }
            }
            let hash = if count == 0 {
                0
            } else {
                sums.iter().enumerate().fold(0u64, |result, (bit, sum)| {
                    if *sum > 0 {
                        result | (1u64 << bit)
                    } else {
                        result
                    }
                })
            };
            Ok(SqlValue::BigInt(hash as i64))
        }
        "hamming_distance" => match (&values[0], &values[1]) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            (SqlValue::BigInt(left), SqlValue::BigInt(right)) => Ok(SqlValue::Integer(
                ((*left as u64) ^ (*right as u64)).count_ones() as i32,
            )),
            _ => Err(invalid(name, "arguments must be BigInt")),
        },
        "gen_random_uuid" => Ok(SqlValue::Text(uuid::Uuid::new_v4().to_string())),
        "uuidv7" => Ok(SqlValue::Text(uuid::Uuid::now_v7().to_string())),
        "hex" => {
            let Some(bytes) = input_bytes(name, &values[0])? else {
                return Ok(SqlValue::Null);
            };
            Ok(SqlValue::Text(hex::encode_upper(bytes)))
        }
        "unhex" => {
            let Some(value) = text_input(name, &values[0])? else {
                return Ok(SqlValue::Null);
            };
            hex::decode(value)
                .map(SqlValue::Blob)
                .map_err(|error| invalid(name, error.to_string()))
        }
        "encode" => {
            let Some(bytes) = input_bytes(name, &values[0])? else {
                return Ok(SqlValue::Null);
            };
            let Some(format) = format_name(name, &values[1])? else {
                return Ok(SqlValue::Null);
            };
            match decode_format(name, format)? {
                "hex" => Ok(SqlValue::Text(hex::encode(bytes))),
                "base64" => Ok(SqlValue::Text(STANDARD.encode(bytes))),
                _ => unreachable!(),
            }
        }
        "decode" => {
            let Some(value) = text_input(name, &values[0])? else {
                return Ok(SqlValue::Null);
            };
            let Some(format) = format_name(name, &values[1])? else {
                return Ok(SqlValue::Null);
            };
            match decode_format(name, format)? {
                "hex" => hex::decode(value)
                    .map(SqlValue::Blob)
                    .map_err(|error| invalid(name, error.to_string())),
                "base64" => {
                    if value.len() % 4 != 0 || value.bytes().any(|byte| byte.is_ascii_whitespace())
                    {
                        return Err(invalid(
                            name,
                            "base64 must be padded and contain no whitespace",
                        ));
                    }
                    STANDARD
                        .decode(value)
                        .map(SqlValue::Blob)
                        .map_err(|error| invalid(name, error.to_string()))
                }
                _ => unreachable!(),
            }
        }
        _ => Err(ExecutorError::Evaluation(
            EvaluationError::UnsupportedFunction(name.into()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(name: &str, values: &[SqlValue]) -> SqlValue {
        eval_for(name).expect("hash/encoding function must be registered")(values)
            .expect("evaluation should succeed")
    }

    #[test]
    fn hash_functions_return_documented_values() {
        assert_eq!(
            eval("md5", &[SqlValue::Text("abc".into())]),
            SqlValue::Text("900150983cd24fb0d6963f7d28e17f72".into())
        );
        assert_eq!(
            eval("sha256", &[SqlValue::Text("abc".into())]),
            SqlValue::Blob(vec![
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ])
        );
    }

    #[test]
    fn encoding_round_trips_and_nulls() {
        assert_eq!(
            eval("hex", &[SqlValue::Blob(vec![0xab, 0xcd])]),
            SqlValue::Text("ABCD".into())
        );
        assert_eq!(
            eval(
                "decode",
                &[
                    SqlValue::Text("aGVsbG8=".into()),
                    SqlValue::Text("base64".into()),
                ]
            ),
            SqlValue::Blob(b"hello".to_vec())
        );
        assert_eq!(eval("unhex", &[SqlValue::Null]), SqlValue::Null);
    }

    #[test]
    fn uuid_functions_are_registered_and_non_deterministic() {
        let first = eval("gen_random_uuid", &[]);
        let second = eval("uuidv7", &[]);
        assert!(matches!(&first, SqlValue::Text(value) if value.len() == 36));
        assert!(matches!(&second, SqlValue::Text(value) if value.len() == 36));
        assert_ne!(first, second);
    }

    #[test]
    fn wrong_arity_returns_error_without_panicking() {
        assert!(eval_for("sha256").unwrap()(&[]).is_err());
        assert!(eval_for("hamming_distance").unwrap()(&[SqlValue::BigInt(1)]).is_err());
    }

    #[test]
    fn hamming_distance_uses_i64_bit_patterns() {
        assert_eq!(
            eval(
                "hamming_distance",
                &[SqlValue::BigInt(-1), SqlValue::BigInt(0)]
            ),
            SqlValue::Integer(64)
        );
    }

    #[test]
    fn simhash_and_invalid_arguments_follow_the_contract() {
        assert_eq!(
            eval("simhash", &[SqlValue::Text("   ".into())]),
            SqlValue::BigInt(0)
        );
        assert_eq!(eval("simhash", &[SqlValue::Null]), SqlValue::Null);

        let error = eval_for("decode").expect("decode is registered")(&[
            SqlValue::Text("!".into()),
            SqlValue::Text("base64".into()),
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            ExecutorError::Evaluation(EvaluationError::InvalidArgument { .. })
        ));

        let error = eval_for("encode").expect("encode is registered")(&[
            SqlValue::Blob(vec![1]),
            SqlValue::Text("unknown".into()),
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            ExecutorError::Evaluation(EvaluationError::InvalidArgument { .. })
        ));
    }

    #[test]
    fn uuid_versions_are_v4_and_v7() {
        let v4 = match eval("gen_random_uuid", &[]) {
            SqlValue::Text(value) => uuid::Uuid::parse_str(&value).unwrap(),
            other => panic!("unexpected v4 value: {other:?}"),
        };
        let v7 = match eval("uuidv7", &[]) {
            SqlValue::Text(value) => uuid::Uuid::parse_str(&value).unwrap(),
            other => panic!("unexpected v7 value: {other:?}"),
        };
        assert_eq!(v4.get_version_num(), 4);
        assert_eq!(v7.get_version_num(), 7);
    }
}
