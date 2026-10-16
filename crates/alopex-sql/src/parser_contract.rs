//! The runtime compatibility gate, shared by SQL and staged payload decoding.

use crate::error::{ParserError, Result};

const PARSER_CONTRACT_DESCRIPTOR: &str = include_str!("../nim-sql-parser/PARSER_CONTRACT_VERSION");

pub(crate) fn ensure_linked_parser_contract(linked_parser_contract: &str) -> Result<()> {
    ensure_parser_contract(PARSER_CONTRACT_DESCRIPTOR.trim(), linked_parser_contract)
}

pub(crate) fn ensure_parser_contract(expected: &str, linked_parser_contract: &str) -> Result<()> {
    if linked_parser_contract == expected {
        return Ok(());
    }
    Err(ParserError::UnexpectedToken {
        line: 0,
        column: 0,
        expected: format!("linked Nim parser contract {expected}"),
        found: format!("linked Nim parser contract {linked_parser_contract}"),
    })
}
