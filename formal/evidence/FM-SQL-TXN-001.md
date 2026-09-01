# FM-SQL-TXN-001 verification evidence

## Date

2026-09-01 (Asia/Tokyo)

## Working Tree

- Repository: `alopex`
- Branch: `feature/v0.8.11-completion`
- Base commit: `39ebeea`
- Model and fixture changes were uncommitted during verification.

## Command

```text
tlc formal/tla/sql/SqlTransactionLifecycle.tla
```

## Result

TLC completed with exit code 0. TLC generated 308 states, found 82 distinct states, exhausted the queue, reached depth 11, and reported no error.

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

## Counterexample

TLC found no counterexample in the complete bounded state graph. An initial draft reused one symbolic write identifier after a prior commit; TLC correctly rejected that abstraction because the identifier could then be both historical durable data and a later rolled-back write. The final model assigns each bounded write identifier once.

## Follow-up

No model or implementation follow-up is required for the v0.8.11 scope. A future design that defines client retry behavior for termination during the internal `COMMIT` interval must extend this model with a non-atomic commit protocol.
