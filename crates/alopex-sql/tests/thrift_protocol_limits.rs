//! Small boundary regressions for the Thrift security dependency update.
//! A declared length of nine and an eight-byte limit exercise rejection without
//! attempting a large allocation, even if a future dependency loses its limit.

use std::io::Cursor;

use thrift::protocol::{TBinaryInputProtocol, TCompactInputProtocol, TInputProtocol};
use thrift::{Error, ProtocolErrorKind, TConfiguration};

fn config() -> TConfiguration {
    TConfiguration::builder()
        .max_string_size(Some(8))
        .build()
        .unwrap()
}

fn assert_size_limit<T: std::fmt::Debug>(result: thrift::Result<T>) {
    assert!(matches!(
        result.unwrap_err(),
        Error::Protocol(error) if error.kind == ProtocolErrorKind::SizeLimit
    ));
}

#[test]
fn binary_bytes_reject_length_before_reading_payload() {
    let mut input = TBinaryInputProtocol::with_config(Cursor::new([0, 0, 0, 9]), true, config());
    assert_size_limit(input.read_bytes());
}

#[test]
fn binary_string_rejects_length_before_reading_payload() {
    let mut input = TBinaryInputProtocol::with_config(Cursor::new([0, 0, 0, 9]), true, config());
    assert_size_limit(input.read_string());
}

#[test]
fn non_strict_message_name_rejects_length_before_reading_payload() {
    let mut input = TBinaryInputProtocol::with_config(Cursor::new([0, 0, 0, 9]), false, config());
    assert_size_limit(input.read_message_begin());
}

#[test]
fn compact_bytes_reject_length_before_reading_payload() {
    let mut input = TCompactInputProtocol::with_config(Cursor::new([9]), config());
    assert_size_limit(input.read_bytes());
}

#[test]
fn binary_bytes_accept_exact_limit() {
    let mut input = TBinaryInputProtocol::with_config(
        Cursor::new([0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8]),
        true,
        config(),
    );
    assert_eq!(input.read_bytes().unwrap(), [1, 2, 3, 4, 5, 6, 7, 8]);
}

#[test]
fn compact_bytes_accept_exact_limit() {
    let mut input =
        TCompactInputProtocol::with_config(Cursor::new([8, 1, 2, 3, 4, 5, 6, 7, 8]), config());
    assert_eq!(input.read_bytes().unwrap(), [1, 2, 3, 4, 5, 6, 7, 8]);
}
