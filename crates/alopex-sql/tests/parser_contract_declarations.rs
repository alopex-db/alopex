//! The parser wire contract is declared in more than one place, and a bump
//! that updates only some of them is invisible until release staging: a
//! release engineer following the CHANGELOG stages assets labelled with the
//! stale identifier, which the runtime exported-contract gate then rejects.
//!
//! `nim-sql-parser/PARSER_CONTRACT_VERSION` is the single source of truth.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is <repo>/crates/alopex-sql.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repository root above crates/alopex-sql")
        .to_path_buf()
}

fn current_contract() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("nim-sql-parser")
        .join("PARSER_CONTRACT_VERSION");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .trim()
        .to_string()
}

/// The `[Unreleased] > Changed` section names the release's current contract
/// as "The Nim parser wire contract is `X`."; every superseded identifier is
/// written as "The Nim parser wire contract `X` added/changed ...".
#[test]
fn the_changelog_declares_the_current_parser_contract() {
    let expected = current_contract();
    let changelog_path = repo_root().join("CHANGELOG.md");
    let changelog = std::fs::read_to_string(&changelog_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", changelog_path.display()));

    const PREFIX: &str = "The Nim parser wire contract is `";
    let declarations: Vec<&str> = changelog
        .lines()
        .filter_map(|line| line.trim_start_matches("- ").strip_prefix(PREFIX))
        .filter_map(|rest| rest.split('`').next())
        .collect();

    let current = declarations
        .first()
        .unwrap_or_else(|| panic!("{PREFIX}...` is missing from {}", changelog_path.display()));
    assert_eq!(
        *current, expected,
        "CHANGELOG.md declares parser contract `{current}` for the unreleased \
         section while PARSER_CONTRACT_VERSION is `{expected}`"
    );
}

/// The Rust consumer pin gates every payload decode, so it has to name the
/// same identifier the parser exports.
#[test]
fn the_rust_consumer_pin_matches_the_parser_contract() {
    let expected = current_contract();
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("build_support.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));

    const PREFIX: &str = "pub(crate) const REQUIRED_CONTRACT_VERSION: &str = \"";
    let declared = source
        .lines()
        .find_map(|line| line.trim_start().strip_prefix(PREFIX))
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| {
            panic!(
                "REQUIRED_CONTRACT_VERSION is missing from {}",
                path.display()
            )
        });

    assert_eq!(
        declared, expected,
        "build_support.rs requires contract `{declared}` while \
         PARSER_CONTRACT_VERSION is `{expected}`"
    );
}
