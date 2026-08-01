//! Closed Phase 4 cross-surface transaction register.
//!
//! This module deliberately names every approved SQL/API row.  It is included
//! by the Rust parity fixture, while the Python parity fixture reads the same
//! stable identifiers from this source file.  Keeping it outside a product
//! crate prevents an adapter from silently redefining the approved register.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurfaceMode {
    Distributed,
    SingleRangeOnly,
    LocalOnly,
    PreExecutionUnsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceRow {
    pub id: String,
    pub mode: SurfaceMode,
}

pub const COMMON_OUTCOME_FIELDS: &[&str] = &[
    "outcome_version",
    "transaction_id",
    "request_id",
    "participating_ranges",
    "read_point",
    "schema_version",
    "data_epoch",
    "isolation",
    "state",
    "failure_class",
    "reason_code",
    "routing",
    "retryable",
    "idempotency",
];

pub fn numbered_ids(prefix: &str, end_inclusive: u8) -> Vec<String> {
    (1..=end_inclusive)
        .map(|number| format!("{prefix}{number:02}"))
        .collect()
}

fn rows(
    prefix: &str,
    end: u8,
    groups: &[(SurfaceMode, std::ops::RangeInclusive<u8>)],
) -> Vec<SurfaceRow> {
    let mut result = Vec::new();
    for (mode, range) in groups {
        for number in range.clone() {
            result.push(SurfaceRow {
                id: format!("{prefix}{number:02}"),
                mode: *mode,
            });
        }
    }
    let expected = numbered_ids(prefix, end);
    let actual = result.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "{prefix} register must be closed and ordered"
    );
    result
}

pub fn sql_transaction_ids() -> Vec<String> {
    numbered_ids("SQL-TXN-", 51)
}

pub fn api_surface_rows() -> Vec<SurfaceRow> {
    let mut all = Vec::new();
    all.extend(rows(
        "API-E",
        13,
        &[
            (SurfaceMode::SingleRangeOnly, 1..=5),
            (SurfaceMode::LocalOnly, 6..=10),
            (SurfaceMode::PreExecutionUnsupported, 11..=13),
        ],
    ));
    all.extend(rows(
        "API-H",
        17,
        &[
            (SurfaceMode::Distributed, 1..=3),
            (SurfaceMode::SingleRangeOnly, 4..=9),
            (SurfaceMode::Distributed, 10..=11),
            (SurfaceMode::PreExecutionUnsupported, 12..=17),
        ],
    ));
    all.extend(rows(
        "API-G",
        20,
        &[
            (SurfaceMode::Distributed, 1..=5),
            (SurfaceMode::PreExecutionUnsupported, 6..=6),
            (SurfaceMode::SingleRangeOnly, 7..=9),
            (SurfaceMode::PreExecutionUnsupported, 10..=13),
            (SurfaceMode::LocalOnly, 14..=17),
            (SurfaceMode::PreExecutionUnsupported, 18..=20),
        ],
    ));
    all.extend(rows(
        "API-C",
        12,
        &[
            (SurfaceMode::SingleRangeOnly, 1..=6),
            (SurfaceMode::Distributed, 7..=9),
            (SurfaceMode::PreExecutionUnsupported, 10..=12),
        ],
    ));
    all.extend(rows(
        "API-P",
        22,
        &[
            (SurfaceMode::LocalOnly, 1..=9),
            (SurfaceMode::PreExecutionUnsupported, 10..=11),
            (SurfaceMode::LocalOnly, 12..=19),
            (SurfaceMode::PreExecutionUnsupported, 20..=22),
        ],
    ));
    all.extend(rows("API-S", 8, &[(SurfaceMode::Distributed, 1..=8)]));
    all
}

pub fn inherited_surface_ids() -> Vec<String> {
    let mut ids = numbered_ids("API-HI", 43);
    ids.extend(numbered_ids("API-CI", 55));
    ids
}
