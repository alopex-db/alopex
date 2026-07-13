//! DataFrame-oriented primitives shared by higher-level crates.

use regex::Regex;

use crate::{Error, Result};

pub mod cast;
pub mod partition_scan;

const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// Nullable string column used by core DataFrame namespace primitives.
pub type Utf8Column = Vec<Option<String>>;
/// Nullable boolean column used by core DataFrame namespace primitives.
pub type BoolColumn = Vec<Option<bool>>;
/// Nullable unsigned integer column used by core DataFrame namespace primitives.
pub type UIntColumn = Vec<Option<usize>>;
/// Nullable timestamp column using microseconds since Unix epoch.
pub type TimestampMicrosColumn = Vec<Option<i64>>;
/// Nullable list column with nullable elements.
pub type ListColumn<T> = Vec<Option<Vec<Option<T>>>>;

/// Result of exploding one list column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExplodedColumn<T> {
    /// Exploded values. Null and empty source lists both produce one null row.
    pub values: Vec<Option<T>>,
    /// Source row index for each exploded value.
    pub source_rows: Vec<usize>,
}

/// Convert UTF-8 strings to lowercase while preserving nulls.
pub fn str_to_lowercase(values: &[Option<String>]) -> Utf8Column {
    values
        .iter()
        .map(|value| value.as_ref().map(|text| text.to_lowercase()))
        .collect()
}

/// Convert UTF-8 strings to uppercase while preserving nulls.
pub fn str_to_uppercase(values: &[Option<String>]) -> Utf8Column {
    values
        .iter()
        .map(|value| value.as_ref().map(|text| text.to_uppercase()))
        .collect()
}

/// Return whether each UTF-8 value matches a regular expression pattern.
pub fn str_contains(values: &[Option<String>], pattern: &str) -> Result<BoolColumn> {
    let regex = compile_regex("str.contains", pattern)?;
    Ok(values
        .iter()
        .map(|value| value.as_ref().map(|text| regex.is_match(text)))
        .collect())
}

/// Replace all regular expression matches in each UTF-8 value.
pub fn str_replace(
    values: &[Option<String>],
    pattern: &str,
    replacement: &str,
) -> Result<Utf8Column> {
    let regex = compile_regex("str.replace", pattern)?;
    Ok(values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map(|text| regex.replace_all(text, replacement).into_owned())
        })
        .collect())
}

/// Strip characters from both ends of each UTF-8 value.
pub fn str_strip_chars(values: &[Option<String>], chars: Option<&str>) -> Utf8Column {
    values
        .iter()
        .map(|value| {
            value.as_ref().map(|text| match chars {
                Some(chars) => text.trim_matches(|ch| chars.contains(ch)).to_string(),
                None => text.trim().to_string(),
            })
        })
        .collect()
}

/// Split each UTF-8 value by a literal separator.
pub fn str_split(values: &[Option<String>], separator: &str) -> ListColumn<String> {
    values
        .iter()
        .map(|value| {
            value.as_ref().map(|text| {
                text.split(separator)
                    .map(|part| Some(part.to_string()))
                    .collect()
            })
        })
        .collect()
}

/// Count Unicode scalar values in each UTF-8 value.
pub fn str_len_chars(values: &[Option<String>]) -> UIntColumn {
    values
        .iter()
        .map(|value| value.as_ref().map(|text| text.chars().count()))
        .collect()
}

/// Extract the first regular expression match or capture group from each value.
pub fn str_extract(
    values: &[Option<String>],
    pattern: &str,
    capture_group: usize,
) -> Result<Utf8Column> {
    let regex = compile_regex("str.extract", pattern)?;
    Ok(values
        .iter()
        .map(|value| {
            value.as_ref().and_then(|text| {
                regex.captures(text).and_then(|captures| {
                    captures
                        .get(capture_group)
                        .map(|matched| matched.as_str().to_string())
                })
            })
        })
        .collect())
}

/// Extract UTC year from timestamp micros.
pub fn dt_year(values: &[Option<i64>]) -> Vec<Option<i32>> {
    values
        .iter()
        .map(|value| value.map(timestamp_parts).map(|parts| parts.year))
        .collect()
}

/// Extract UTC month from timestamp micros, in the range 1..=12.
pub fn dt_month(values: &[Option<i64>]) -> Vec<Option<u32>> {
    values
        .iter()
        .map(|value| value.map(timestamp_parts).map(|parts| parts.month))
        .collect()
}

/// Extract UTC day of month from timestamp micros, in the range 1..=31.
pub fn dt_day(values: &[Option<i64>]) -> Vec<Option<u32>> {
    values
        .iter()
        .map(|value| value.map(timestamp_parts).map(|parts| parts.day))
        .collect()
}

/// Extract ISO weekday from timestamp micros, where Monday is 1 and Sunday is 7.
pub fn dt_weekday(values: &[Option<i64>]) -> Vec<Option<u32>> {
    values
        .iter()
        .map(|value| value.map(|micros| iso_weekday(timestamp_days(micros))))
        .collect()
}

/// Format timestamp micros as UTC RFC3339-like text.
pub fn dt_to_string(values: &[Option<i64>]) -> Utf8Column {
    values
        .iter()
        .map(|value| value.map(format_timestamp_micros))
        .collect()
}

/// Convert naive local timestamp micros between fixed-offset time zones.
///
/// Offsets use `Z`, `+HH:MM`, or `-HH:MM`. The returned value is the target
/// local timestamp represented with the same microsecond epoch encoding.
pub fn dt_convert_time_zone(
    values: &[Option<i64>],
    from_offset: &str,
    to_offset: &str,
) -> Result<TimestampMicrosColumn> {
    let from = parse_offset_seconds(from_offset)?;
    let to = parse_offset_seconds(to_offset)?;
    let delta_micros = i64::from(to - from)
        .checked_mul(MICROS_PER_SECOND)
        .ok_or_else(|| invalid_parameter("dt.convert_time_zone", "offset delta overflow"))?;
    values
        .iter()
        .map(|value| {
            value
                .map(|micros| {
                    micros.checked_add(delta_micros).ok_or_else(|| {
                        invalid_parameter("dt.convert_time_zone", "timestamp overflow")
                    })
                })
                .transpose()
        })
        .collect()
}

/// Join string list elements with a separator.
///
/// Null lists return null. Null elements use `null_value` when provided;
/// otherwise the whole row returns null.
pub fn list_join(
    values: &[Option<Vec<Option<String>>>],
    separator: &str,
    null_value: Option<&str>,
) -> Utf8Column {
    values
        .iter()
        .map(|list| {
            list.as_ref().and_then(|items| {
                let mut parts = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Some(text) => parts.push(text.as_str()),
                        None => parts.push(null_value?),
                    }
                }
                Some(parts.join(separator))
            })
        })
        .collect()
}

/// Return list lengths while preserving null lists.
pub fn list_len<T>(values: &[Option<Vec<Option<T>>>]) -> UIntColumn {
    values
        .iter()
        .map(|list| list.as_ref().map(Vec::len))
        .collect()
}

/// Return whether each list contains a non-null needle value.
pub fn list_contains<T: PartialEq>(values: &[Option<Vec<Option<T>>>], needle: &T) -> BoolColumn {
    values
        .iter()
        .map(|list| {
            list.as_ref()
                .map(|items| items.iter().any(|item| item.as_ref() == Some(needle)))
        })
        .collect()
}

/// Explode one list column into values and source row indexes.
pub fn explode_list<T: Clone>(values: &[Option<Vec<Option<T>>>]) -> ExplodedColumn<T> {
    let mut exploded = Vec::new();
    let mut source_rows = Vec::new();
    for (row_idx, list) in values.iter().enumerate() {
        match list {
            Some(items) if !items.is_empty() => {
                for item in items {
                    exploded.push(item.clone());
                    source_rows.push(row_idx);
                }
            }
            Some(_) | None => {
                exploded.push(None);
                source_rows.push(row_idx);
            }
        }
    }
    ExplodedColumn {
        values: exploded,
        source_rows,
    }
}

/// Implode values into nullable lists according to contiguous group lengths.
pub fn implode_by_group_lengths<T: Clone>(
    values: &[Option<T>],
    group_lengths: &[usize],
) -> Result<ListColumn<T>> {
    let expected = group_lengths
        .iter()
        .try_fold(0usize, |acc, len| acc.checked_add(*len))
        .ok_or_else(|| invalid_parameter("df.implode", "group length overflow"))?;
    if expected != values.len() {
        return Err(invalid_parameter(
            "df.implode",
            format!("group lengths sum to {expected}, expected {}", values.len()),
        ));
    }

    let mut offset = 0usize;
    let mut output = Vec::with_capacity(group_lengths.len());
    for &len in group_lengths {
        let end = offset + len;
        output.push(Some(values[offset..end].to_vec()));
        offset = end;
    }
    Ok(output)
}

fn compile_regex(operation: &str, pattern: &str) -> Result<Regex> {
    Regex::new(pattern).map_err(|err| invalid_parameter(operation, err.to_string()))
}

fn invalid_parameter(param: impl Into<String>, reason: impl Into<String>) -> Error {
    Error::InvalidParameter {
        param: param.into(),
        reason: reason.into(),
    }
}

#[derive(Clone, Copy)]
struct TimestampParts {
    year: i32,
    month: u32,
    day: u32,
    hour: i64,
    minute: i64,
    second: i64,
    micros: i64,
}

fn timestamp_parts(micros: i64) -> TimestampParts {
    let seconds = micros.div_euclid(MICROS_PER_SECOND);
    let micros_remainder = micros.rem_euclid(MICROS_PER_SECOND);
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    TimestampParts {
        year,
        month,
        day,
        hour: second_of_day / 3_600,
        minute: (second_of_day % 3_600) / 60,
        second: second_of_day % 60,
        micros: micros_remainder,
    }
}

fn timestamp_days(micros: i64) -> i64 {
    micros
        .div_euclid(MICROS_PER_SECOND)
        .div_euclid(SECONDS_PER_DAY)
}

fn iso_weekday(days_since_epoch: i64) -> u32 {
    (days_since_epoch + 3).rem_euclid(7) as u32 + 1
}

fn format_timestamp_micros(micros: i64) -> String {
    let parts = timestamp_parts(micros);
    if parts.micros == 0 {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            parts.year, parts.month, parts.day, parts.hour, parts.minute, parts.second
        )
    } else {
        let mut fraction = format!("{:06}", parts.micros);
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{}Z",
            parts.year, parts.month, parts.day, parts.hour, parts.minute, parts.second, fraction
        )
    }
}

fn parse_offset_seconds(offset: &str) -> Result<i32> {
    if offset == "Z" || offset == "+00:00" || offset == "-00:00" {
        return Ok(0);
    }
    let bytes = offset.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' || !matches!(bytes[0], b'+' | b'-') {
        return Err(invalid_parameter(
            "dt.convert_time_zone",
            "offset must be Z, +HH:MM, or -HH:MM",
        ));
    }
    let hours = offset[1..3].parse::<i32>().map_err(|_| {
        invalid_parameter(
            "dt.convert_time_zone",
            "offset hour must be two decimal digits",
        )
    })?;
    let minutes = offset[4..6].parse::<i32>().map_err(|_| {
        invalid_parameter(
            "dt.convert_time_zone",
            "offset minute must be two decimal digits",
        )
    })?;
    if hours > 23 || minutes > 59 {
        return Err(invalid_parameter(
            "dt.convert_time_zone",
            "offset is out of range",
        ));
    }
    let seconds = hours * 3_600 + minutes * 60;
    Ok(if bytes[0] == b'-' { -seconds } else { seconds })
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[Option<&str>]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| value.map(str::to_string))
            .collect()
    }

    #[test]
    fn string_namespace_primitives_preserve_nulls_and_unicode() {
        let input = strings(&[Some(" Alopex "), None, Some("Straße42")]);

        assert_eq!(
            str_to_lowercase(&input),
            strings(&[Some(" alopex "), None, Some("straße42")])
        );
        assert_eq!(
            str_to_uppercase(&input),
            strings(&[Some(" ALOPEX "), None, Some("STRASSE42")])
        );
        assert_eq!(
            str_strip_chars(&input, None),
            strings(&[Some("Alopex"), None, Some("Straße42")])
        );
        assert_eq!(str_len_chars(&input), vec![Some(8), None, Some(8)]);
        assert_eq!(
            str_contains(&input, r"\d+$").unwrap(),
            vec![Some(false), None, Some(true)]
        );
        assert_eq!(
            str_replace(&input, r"\d+", "#").unwrap(),
            strings(&[Some(" Alopex "), None, Some("Straße#")])
        );
        assert_eq!(
            str_extract(&input, r"(\p{Alphabetic}+)(\d+)", 1).unwrap(),
            strings(&[None, None, Some("Straße")])
        );
    }

    #[test]
    fn string_split_returns_nullable_list_column() {
        let input = strings(&[Some("a,b,"), None, Some("one")]);

        assert_eq!(
            str_split(&input, ","),
            vec![
                Some(strings(&[Some("a"), Some("b"), Some("")])),
                None,
                Some(strings(&[Some("one")]))
            ]
        );
    }

    #[test]
    fn datetime_primitives_extract_and_format_utc_parts() {
        let input = vec![Some(0), Some(1_704_067_200_123_000), None];

        assert_eq!(dt_year(&input), vec![Some(1970), Some(2024), None]);
        assert_eq!(dt_month(&input), vec![Some(1), Some(1), None]);
        assert_eq!(dt_day(&input), vec![Some(1), Some(1), None]);
        assert_eq!(dt_weekday(&input), vec![Some(4), Some(1), None]);
        assert_eq!(
            dt_to_string(&input),
            strings(&[
                Some("1970-01-01T00:00:00Z"),
                Some("2024-01-01T00:00:00.123Z"),
                None
            ])
        );
    }

    #[test]
    fn timezone_conversion_uses_fixed_offsets_and_checks_errors() {
        let input = vec![Some(0), None];

        assert_eq!(
            dt_convert_time_zone(&input, "Z", "+09:00").unwrap(),
            vec![Some(32_400_000_000), None]
        );
        assert!(matches!(
            dt_convert_time_zone(&input, "UTC", "+09:00"),
            Err(Error::InvalidParameter { param, .. }) if param == "dt.convert_time_zone"
        ));
    }

    #[test]
    fn list_namespace_primitives_handle_null_lists_and_elements() {
        let input = vec![
            Some(vec![Some("a".to_string()), None, Some("c".to_string())]),
            None,
            Some(Vec::new()),
        ];

        assert_eq!(
            list_join(&input, "-", Some("NULL")),
            strings(&[Some("a-NULL-c"), None, Some("")])
        );
        assert_eq!(
            list_join(&input, "-", None),
            strings(&[None, None, Some("")])
        );
        assert_eq!(list_len(&input), vec![Some(3), None, Some(0)]);
        assert_eq!(
            list_contains(&input, &"c".to_string()),
            vec![Some(true), None, Some(false)]
        );
    }

    #[test]
    fn explode_and_implode_primitives_are_deterministic() {
        let input = vec![Some(vec![Some(1), None, Some(3)]), Some(Vec::new()), None];

        let exploded = explode_list(&input);
        assert_eq!(exploded.values, vec![Some(1), None, Some(3), None, None]);
        assert_eq!(exploded.source_rows, vec![0, 0, 0, 1, 2]);
        assert_eq!(
            implode_by_group_lengths(&exploded.values, &[3, 1, 1]).unwrap(),
            vec![
                Some(vec![Some(1), None, Some(3)]),
                Some(vec![None]),
                Some(vec![None])
            ]
        );
        assert!(matches!(
            implode_by_group_lengths(&exploded.values, &[2]),
            Err(Error::InvalidParameter { param, .. }) if param == "df.implode"
        ));
    }
}
