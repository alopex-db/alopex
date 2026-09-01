# FM-SQL-PREP-001 verification evidence

## Date

2026-09-01 (Asia/Tokyo)

## Working tree

- Repository: `alopex`
- Branch: `feature/v0.8.11-completion`
- Model changes were uncommitted during verification.

## Command

```text
tlc formal/tla/sql/PreparedStatementLifecycle.tla
```

## Result

TLC completed with exit code 0. TLC generated 169 states, found 40 distinct states, exhausted the queue, reached depth 9, and reported no error.

## Invariants

- `TypeOK`
- `FinalizedHasNoBindings`
- `ExecutionUsesKnownSchema`

## Counterexample

TLC found no counterexample in the complete bounded state graph. Finalized states are intentional terminal states, so the configuration disables deadlock reporting while retaining invariant checks.
