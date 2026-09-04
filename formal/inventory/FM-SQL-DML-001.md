# FM-SQL-DML-001 inventory

- Purpose: verify that MERGE multiple-match rejection and stale concurrent advanced DML never publish rejected rows, and that a fresh retry starts from the committed winner
- Target implementation: `crates/alopex-embedded/tests/advanced_dml.rs`, `scripts/reference_tests/sql_v0811_advanced_dml.py`
- Invariants: `RejectedOrConflictedDoesNotPublish`, `OnlyCommitPublishes`
- Tool: TLA+, because statement rejection and optimistic commit conflicts are state transitions
- Model/config: `formal/tla/sql/AdvancedDmlAtomicity.tla`, `formal/tla/sql/AdvancedDmlAtomicity.cfg`
- Scenario: `formal/scenarios/FM-SQL-DML-001.md`
- Verification: `tlc formal/tla/sql/AdvancedDmlAtomicity.tla`
- Evidence: `formal/evidence/FM-SQL-DML-001_20260904_summary.md`
- Status: active
- Change policy: rerun TLC and both Rust advanced-DML atomicity tests when commit or rejection semantics change
- Retirement: only when a successor model covers the same local atomicity and conflict boundary
