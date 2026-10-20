use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, Cursor, Read};

use crate::catalog::TableMetadata;
use crate::executor::{ExecutorError, Result};
use crate::storage::SqlValue;

use super::{BulkReader, CopyField, CopySchema, parse_value};

/// 簡易 CSV リーダー。
pub struct CsvReader {
    schema: CopySchema,
    reader: CsvInput,
    types: Vec<crate::planner::types::ResolvedType>,
}

impl CsvReader {
    pub fn open(path: &str, table_meta: &TableMetadata, header: bool) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| ExecutorError::BulkLoad(format!("failed to open CSV: {e}")))?;
        Self::from_reader(file, table_meta, header)
    }

    pub fn from_content(content: String, table_meta: &TableMetadata, header: bool) -> Result<Self> {
        Self::from_reader(Cursor::new(content.into_bytes()), table_meta, header)
    }

    pub fn from_reader(
        reader: impl Read + 'static,
        table_meta: &TableMetadata,
        header: bool,
    ) -> Result<Self> {
        let mut reader = CsvInput::new(Box::new(reader));
        let header_names = if header {
            Some(
                reader
                    .next_record()?
                    .map(|(record, _)| record)
                    .unwrap_or_default()
                    .iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let schema_fields = table_meta
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| CopyField {
                name: header_names
                    .as_ref()
                    .and_then(|names| names.get(idx))
                    .map(|s| s.to_string()),
                data_type: Some(col.data_type.clone()),
            })
            .collect();
        Ok(Self {
            schema: CopySchema {
                fields: schema_fields,
            },
            reader,
            types: table_meta
                .columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect(),
        })
    }
}

impl BulkReader for CsvReader {
    fn schema(&self) -> &CopySchema {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Vec<SqlValue>>>> {
        let mut batch = Vec::with_capacity(max_rows);
        for _ in 0..max_rows {
            let Some((record, mut quoted)) = self.reader.next_record()? else {
                break;
            };
            let mut parts = record.iter().map(str::to_string).collect::<Vec<_>>();
            if parts.len() != self.types.len()
                && matches!(
                    self.types.last(),
                    Some(crate::planner::types::ResolvedType::Vector { .. })
                )
                && parts.len() > self.types.len()
            {
                let tail = parts.split_off(self.types.len().saturating_sub(1));
                parts.push(tail.join(","));
                quoted.truncate(self.types.len());
            }
            if parts.len() != self.types.len() {
                return Err(ExecutorError::BulkLoad(format!(
                    "column count mismatch in row: expected {}, got {}",
                    self.types.len(),
                    parts.len()
                )));
            }
            batch.push(
                parts
                    .iter()
                    .zip(quoted)
                    .zip(&self.types)
                    .map(|((raw, quoted), data_type)| parse_csv_value(raw, quoted, data_type))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok((!batch.is_empty()).then_some(batch))
    }
}

fn parse_csv_value(
    raw: &str,
    quoted: bool,
    ty: &crate::planner::types::ResolvedType,
) -> Result<SqlValue> {
    // Keep legacy unquoted NULL inputs, but never erase quoted text.
    if !quoted && (raw == r"\N" || raw.trim().eq_ignore_ascii_case("null")) {
        return Ok(SqlValue::Null);
    }
    parse_value(raw, ty)
}

/// Retain only the current raw record plus the CSV reader's bounded read-ahead.
/// The csv crate still owns parsing, UTF-8 validation and field boundaries.
struct CapturedInput {
    source: Box<dyn Read>,
    bytes: VecDeque<u8>,
    consumed: u64,
}

impl Read for CapturedInput {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self.source.read(output)?;
        self.bytes.extend(&output[..count]);
        Ok(count)
    }
}

struct CsvInput {
    reader: ::csv::Reader<CapturedInput>,
}

impl CsvInput {
    fn new(source: Box<dyn Read>) -> Self {
        Self {
            reader: ::csv::ReaderBuilder::new()
                // Handle the header explicitly so raw records are drained one by one.
                .has_headers(false)
                .flexible(true)
                .from_reader(CapturedInput {
                    source,
                    bytes: VecDeque::new(),
                    consumed: 0,
                }),
        }
    }

    fn next_record(&mut self) -> Result<Option<(::csv::StringRecord, Vec<bool>)>> {
        let mut record = ::csv::StringRecord::new();
        if !self
            .reader
            .read_record(&mut record)
            .map_err(|error| ExecutorError::BulkLoad(error.to_string()))?
        {
            return Ok(None);
        }
        let end = self.reader.position().byte();
        let captured = self.reader.get_mut();
        let first = captured.consumed == 0;
        let count = (end - captured.consumed) as usize;
        let raw = captured.bytes.drain(..count).collect::<Vec<_>>();
        captured.consumed = end;
        let raw = if first {
            raw.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&raw)
        } else {
            &raw
        };
        let quoted = quoted_fields(raw);
        if quoted.len() != record.len() {
            return Err(ExecutorError::BulkLoad(
                "CSV quote metadata does not match parsed fields".into(),
            ));
        }
        Ok(Some((record, quoted)))
    }
}

/// Match the default csv crate dialect: doubled quotes, comma, CR/LF/CRLF,
/// ignored blank lines, and permissive text following a closing quote.
fn quoted_fields(raw: &[u8]) -> Vec<bool> {
    #[derive(Clone, Copy)]
    enum State {
        Start,
        Plain,
        Quoted,
        AfterQuote,
    }
    let start = raw
        .iter()
        .take_while(|&&b| b == b'\r' || b == b'\n')
        .count();
    let raw = &raw[start..];
    let mut flags = vec![raw.first() == Some(&b'"')];
    let mut state = State::Start;
    for &byte in raw {
        state = match (state, byte) {
            (State::Start, b'"') | (State::AfterQuote, b'"') => State::Quoted,
            (State::Quoted, b'"') => State::AfterQuote,
            (State::Quoted, _) => State::Quoted,
            (_, b',') => {
                flags.push(false);
                State::Start
            }
            (_, b'\r' | b'\n') => break,
            (State::Start, _) => State::Plain,
            _ => State::Plain,
        };
        if matches!(state, State::Quoted) {
            *flags.last_mut().unwrap() = true;
        }
    }
    flags
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::types::ResolvedType;
    use proptest::prelude::*;

    fn decode(input: &[u8]) -> Vec<Vec<SqlValue>> {
        let mut reader = CsvInput::new(Box::new(Cursor::new(input.to_vec())));
        let mut rows = Vec::new();
        while let Some((record, quoted)) = reader.next_record().unwrap() {
            rows.push(
                record
                    .iter()
                    .zip(quoted)
                    .map(|(value, quoted)| {
                        parse_csv_value(value, quoted, &ResolvedType::Text).unwrap()
                    })
                    .collect(),
            );
        }
        rows
    }

    #[test]
    fn null_marker_and_quoted_literals_are_distinct() {
        assert_eq!(
            decode(b"\\N,\"\\N\",NULL,\"NULL\",,\"\", null ,\" null \"\n"),
            vec![vec![
                SqlValue::Null,
                SqlValue::Text(r"\N".into()),
                SqlValue::Null,
                SqlValue::Text("NULL".into()),
                SqlValue::Text("".into()),
                SqlValue::Text("".into()),
                SqlValue::Null,
                SqlValue::Text(" null ".into()),
            ]]
        );
    }

    #[test]
    fn quote_metadata_handles_bom_blank_lines_crlf_and_multiline_fields() {
        assert_eq!(
            decode(b"\xef\xbb\xbf\r\n\r\n\"a,\"\"b\r\nc\",\\N\r\n\r\nx,\"NULL\""),
            vec![
                vec![SqlValue::Text("a,\"b\r\nc".into()), SqlValue::Null],
                vec![SqlValue::Text("x".into()), SqlValue::Text("NULL".into())],
            ]
        );
    }

    #[test]
    fn legacy_unquoted_values_keep_their_meaning() {
        assert_eq!(
            decode(b"a,, null ,  spaces  ,\\path,\\\\N\n"),
            vec![vec![
                SqlValue::Text("a".into()),
                SqlValue::Text("".into()),
                SqlValue::Null,
                SqlValue::Text("  spaces  ".into()),
                SqlValue::Text(r"\path".into()),
                SqlValue::Text(r"\\N".into()),
            ]]
        );
        assert_eq!(
            parse_csv_value("NULL", false, &ResolvedType::Integer).unwrap(),
            SqlValue::Null
        );
        assert!(parse_csv_value("NULL", true, &ResolvedType::Integer).is_err());
    }

    #[test]
    fn tiny_reads_preserve_quote_state_and_drain_consumed_records() {
        struct OneByte(Cursor<Vec<u8>>);
        impl Read for OneByte {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                self.0.read(&mut bytes[..1])
            }
        }
        let mut input = CsvInput::new(Box::new(OneByte(Cursor::new(
            b"\"a\"\"b\nc\",\\N\r\n\"\\N\",end".to_vec(),
        ))));
        let (first, flags) = input.next_record().unwrap().unwrap();
        assert_eq!(first.iter().collect::<Vec<_>>(), vec!["a\"b\nc", r"\N"]);
        assert_eq!(flags, vec![true, false]);
        assert!(input.reader.get_ref().bytes.len() <= 1);
        let (second, flags) = input.next_record().unwrap().unwrap();
        assert_eq!(second.iter().collect::<Vec<_>>(), vec![r"\N", "end"]);
        assert_eq!(flags, vec![true, false]);
        assert!(input.next_record().unwrap().is_none());
    }

    #[test]
    fn invalid_utf8_is_not_accepted_as_text() {
        let mut input = CsvInput::new(Box::new(Cursor::new(vec![0xff, b'\n'])));
        assert!(input.next_record().is_err());
    }

    #[test]
    fn text_starting_with_bom_is_not_a_file_bom() {
        let value = SqlValue::Text("\u{feff}text".into());
        let encoded = super::super::csv_value(&value) + "\n";
        assert_eq!(decode(encoded.as_bytes()), vec![vec![value]]);
    }

    #[test]
    fn writer_preserves_each_reserved_or_special_text_as_a_single_field() {
        for text in [
            "", r"\N", "NULL", " nUlL ", ",", "\"", "\n", "\r", "\u{feff}",
        ] {
            let value = SqlValue::Text(text.into());
            let encoded = super::super::csv_value(&value) + "\n";
            assert_eq!(decode(encoded.as_bytes()), vec![vec![value]], "{text:?}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn text_and_null_roundtrip_through_shared_writer(
            values in prop::collection::vec(
                prop::option::of(prop_oneof![
                    prop::sample::select(vec!["", r"\N", "NULL", " null ", ",", "\"", "\n", "\r", "\u{feff}"])
                        .prop_map(str::to_owned),
                    prop::collection::vec(any::<char>(), 0..20)
                        .prop_map(|chars| chars.into_iter().collect::<String>())
                ]),
                1..5
            )
        ) {
            let expected = values.into_iter().map(|value| match value {
                Some(value) => SqlValue::Text(value),
                None => SqlValue::Null,
            }).collect::<Vec<_>>();
            let line = expected.iter().map(super::super::csv_value).collect::<Vec<_>>().join(",") + "\n";
            prop_assert_eq!(decode(line.as_bytes()), vec![expected]);
        }
    }
}
