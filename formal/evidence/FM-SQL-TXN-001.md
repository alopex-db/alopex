# FM-SQL-TXN-001 verification evidence

## Date

2026-09-04 (Asia/Tokyo)

## Working Tree

- Repository: `alopex`
- Branch: `feature/v0.8.11-completion`
- Base commit: `53a1503`
- The reviewed model and implementation changes were uncommitted during verification.

## Command

```text
tlc formal/tla/sql/SqlTransactionLifecycle.tla
```

## Result

TLC completed with exit code 0. TLC generated 906 states, found 218 distinct states, exhausted the queue, reached depth 13, and reported no error.

The run used checksum-pinned TLA+ Tools v1.7.4 (revision `5a47802`), matching CI.

The reusable implementation fixture also completed successfully:

```text
cargo run --manifest-path crates/alopex-tools/Cargo.toml \
  --bin verify-sql-transaction-failures
sql transaction failure conformance passed
```

## Invariants

- `TypeOK`
- `OnlyDurableWritesAreVisible`
- `AcknowledgedCommitsAreDurable`
- `DiscardedWritesNeverBecomeDurable`
- `IdleHasNoStagedWrites`
- `DeadProcessHasNoActiveTransaction`
- `PostCommitReadRequiresBarrier`
- `CommitFailureDoesNotPublish`
- `CommitBarrierPublishesDurably`

## Counterexample

TLC found no counterexample in the complete bounded state graph. The model now explores commit failure, rollback failure, and post-commit read success/failure in addition to the original lifecycle.

## Follow-up

PR CI now runs the model, and the v0.8 implementation gate runs the reusable failure fixture. A future design that defines client retry behavior for termination during the internal `COMMIT` interval must extend this model with a non-atomic commit protocol.
