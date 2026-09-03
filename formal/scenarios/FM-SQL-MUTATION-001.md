# FM-SQL-MUTATION-001 implementation scenarios

| Model transition or invariant | Implementation evidence |
| --- | --- |
| `StageConstraintViolation`, `RejectInvalidCommit`, cascade limit | `crates/alopex-embedded/tests/constraints.rs` and `executor/dml/constraints.rs` |
| `MergeMultipleMatch`, `FailureDoesNotPublishRows` | `crates/alopex-embedded/tests/advanced_dml.rs::merge_rejects_multiple_source_matches_atomically` |
| `CopyStart`, `CopyPublish`, `Crash` | `crates/alopex-sql/tests/copy_bulk_load.rs` and `crates/alopex-embedded/tests/copy_sql.rs` |
| `NextVal`, `Commit`, `Rollback` | `crates/alopex-embedded/tests/sequence.rs` and `executor/ddl/sequence.rs` |
| Concurrent allocation, conflict, retry | `formal/tla/sql/SequenceConcurrent.tla` and `crates/alopex-embedded/tests/sequence.rs` |
| Public release reuse | `scripts/release/verify-release/run.sh` runs the v0.8.11 SQL fixture against the exact package version |
