# Evidence: FM-SQL-FK-001

## Date

2026-09-04 (Asia/Tokyo)

## Working Tree

- Repository: `alopex`
- Branch: `feature/v0.8.11-completion`
- Model and test changes were uncommitted during verification.

## Command

```text
tlc formal/tla/sql/ForeignKeyCommitRace.tla

CARGO_TARGET_DIR=/home/roomtv/works/alopex-db/alopex/target \
NIM_SQL_PARSER_LIB_DIR=crates/alopex-sql/nim-sql-parser \
ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD=1 \
cargo test -p alopex-embedded --features lane_ci --test constraints \
  concurrent_parent_delete
```

## Result

TLC completed with exit code 0. TLC generated 7 states, found all 5 distinct reachable states, exhausted the queue at depth 3, and reported no error.

The Rust integration target completed with exit code 0. Both commit-order tests passed (2 passed, 0 failed).

## Invariants

- `TypeOK`
- `ReferentialIntegrity`
- `ConflictDoesNotPublish`

## Counterexample

TLC found no invariant counterexample in the complete bounded state graph. An initial run reported the expected terminal state as a deadlock; the final model represents the completed pair with an explicit `Done` stutter transition.

## Follow-up

Run both commit-order Rust integration tests whenever the production commit-conflict boundary changes.
