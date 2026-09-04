# FM-SQL-MUTATION-001 implementation scenarios

| Model transition or invariant | Implementation evidence |
| --- | --- |
| `StageConstraintViolation`, `RejectInvalidCommit`, cascade limit | `crates/alopex-embedded/tests/constraints.rs` and `executor/dml/constraints.rs` |
| `MergeMultipleMatch`, `FailureDoesNotPublishRows` | `crates/alopex-embedded/tests/advanced_dml.rs::merge_rejects_multiple_source_matches_atomically` |
| `CopyStart`, `CopyPublish`, `Crash`, `RecoverCopyTemporary` | `crates/alopex-sql/src/executor/bulk/mod.rs::copy_temp_recovery_removes_only_stale_same_destination_files` and `crates/alopex-embedded/tests/copy_sql.rs` |
| `NextVal`, `Commit`, `Rollback` | `crates/alopex-embedded/tests/sequence.rs` and `executor/ddl/sequence.rs` |
| Concurrent allocation, conflict, retry | `formal/tla/sql/SequenceConcurrent.tla` and `crates/alopex-embedded/tests/sequence.rs` |
| Remote allocation fails closed | `formal/tla/sql/SequenceConcurrent.tla::RemoteNeverAllocates` and `crates/alopex-sql/src/distributed_read/catalog_v0_8.rs::sequence_allocation_remains_local_only_before_remote_transport` |
| Backup/restore preserves sequence state and ownership | `crates/alopex-embedded/tests/sequence.rs::sequence_state_and_ownership_survive_single_file_backup_restore` |
| Public release reuse | `scripts/release/verify-release/run.sh` runs the v0.8.11 SQL fixture against the exact package version |
