# FM-SQL-FK-001: concurrent foreign-key commits

## Purpose

The model checks the fixed two-transaction race between deleting a parent row and inserting a child row that references it. Both statements are valid in their starting snapshots, but only the first commit may publish.

## Responsibility boundary

- `alopex-sql` validates each statement against its transaction snapshot.
- `alopex-core` owns commit-conflict detection and rejects the stale second commit.
- The Rust integration test and TLC evidence observe that one commit order cannot publish an orphan row.

## Target implementation paths

- `crates/alopex-sql/src/executor/dml/constraints.rs`
- `crates/alopex-sql/src/storage/bridge.rs`
- `crates/alopex-core/src/kv/memory.rs`
- `crates/alopex-embedded/tests/constraints.rs`

## Invariants

- `ReferentialIntegrity`: a committed child row always has a committed parent row.
- `ConflictDoesNotPublish`: a transaction rejected for conflict never publishes its staged delete or insert.

## Tool choice

TLA+ models the two legal commit orders and the conflict transition that follows the first commit.

## Artifacts

- Model: `formal/tla/sql/ForeignKeyCommitRace.tla`
- TLC configuration: `formal/tla/sql/ForeignKeyCommitRace.cfg`
- Implementation mapping: `formal/scenarios/FM-SQL-FK-001.md`
- Verification evidence: `formal/evidence/FM-SQL-FK-001_20260904_summary.md`

## Verification command

`tlc formal/tla/sql/ForeignKeyCommitRace.tla`

## Status

Active.

## Change policy

Any change to foreign-key commit validation, SQL range-change journaling, or KV conflict detection must rerun TLC and both commit-order integration tests.

## Retirement condition

Retire this model only when a replacement model covers both commit orders and preserves the same two invariants for the production transaction protocol.
