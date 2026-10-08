# DML statement performance comparison

The runner measures prepared in-process SQL execution for the old Alopex source,
the changed source, and SQLite. The Rust harness calls the actual product APIs;
it does not contain a substitute query implementation. Build and measurement
are separate operations. No performance result is implied by manifest preparation.

## Prepare and build

The operator runs `python3 scripts/performance/dml_statement_v0816.py prepare
--fixed <fixed-worktree> --baseline <baseline-worktree> --output <new-directory>`.
The command creates three standalone Cargo manifests and copies the candidate
lock. The operator runs `cargo metadata --offline --format-version 1
--manifest-path <directory>/<engine>/Cargo.toml` and checks lock identities before
building. The manifests reference one source at
`crates/alopex-sql/benches/support/dml_statement_perf.rs` and preserve the product
release settings (opt-level 3, LTO, one codegen unit).

The operator supplies the normal parser environment and a shared Cargo target,
then builds one engine with `cargo build --offline --release --manifest-path
<directory>/<engine>/Cargo.toml --bin dml_statement_perf`. The operator completes
that engine's measurements and records/removes its owned executable and
source-specific artifacts before building the next engine. The operator must
not delete shared third-party dependencies or another task's artifacts.

## Execute one cell

The runner's `run --help` lists required identity inputs and the finite cases.
Each invocation takes an already built binary, one engine, one case, one size,
and a new raw JSONL path. It pins the process to CPU 1 by default and enforces a
180-second timeout. Timeout cleanup terminates the whole owned process group.
The binary's positional interface is `ENGINE CASE ROWS RAW_JSONL`; the runner
creates the file and provenance first, then the binary appends observations.

The harness uses sizes 600 and 1025, one warmup, and seven measured executions.
Each execution runs in a fresh explicit transaction and rolls back after
verification. Setup, planning/preparation, plan cloning, transaction start,
verification, rollback, and raw output are outside the measured interval.
The interval contains statement execution and consumption of the affected-row
result. Each iteration checks every resulting row outside that interval.

The workloads are ordinary UPDATE, independent MAX UPDATE, IN DELETE,
correlated MAX UPDATE, and DELETE with two independent IN results. Both engines
use an in-memory database, integer primary keys, identical source/target data,
and identical SQL. SQLite enables foreign keys and uses memory temporary
storage and one thread. Its runtime version and compile options are recorded.

## Evidence contract

The JSONL starts with an `identity` record containing engine, case, size, phase,
CPU, source revision/tree digest, harness/runner/manifest/lock/binary/parser
digests, and fixed sampling/noise settings. Each `sample` includes engine,
case, rows, phase, iteration, warmup, elapsed_ns, affected_rows, result_check,
and status. The runner ends with a `completion` record. SQLite also emits a
`reference` metadata record. The runner preserves partial records on failure.
The sibling `.log` and `.process-time` retain diagnostics and GNU time's
whole-process max RSS, CPU time, and block I/O, not statement-only memory costs.

The `render` command requires exactly eight valid samples per successful cell,
excludes warmup, and uses the median of the seven measured samples. It retains
missing/failed cells as failures and rejects duplicate cell files. A separate
directory preserves retries rather than overwriting an earlier run.
The renderer validates required provenance fields and allowed engines, cases,
and sizes. All cells must share CPU, phase, sampling, noise, harness, and runner
identities; each engine must retain one source/revision/manifest/lock/binary/parser
identity. Any identity mismatch fails the comparison set instead of permitting
a mixture of runs to establish parity.
The default acceptance ratio is fixed/baseline and fixed/SQLite at most 1.05.
The three-engine, five-case, two-size matrix has 30 cells and 10 comparisons.
The operator does not call unexecuted, timed-out, or failed cells passing.

## Separate forced-spill resource cells

The ordinary 30 cells do not exceed the default cache memory threshold.
They do not measure disk-backed range replay. The additional resource matrix
has four cells: baseline/fixed × 600/1025, using the same AsyncTxnBridge SQL
API, MemoryPolicy 4096 bytes with SpillToDisk, one warmup/seven measurements,
and two independent IN results with a final false predicate (no DML writes).
That phase includes async SQL parsing/planning and must not be merged into the
prepared-execute latency comparison. The two engines report elapsed time,
spill bytes/files, cleanup, and whole-process RSS/I/O separately.

The source now implements those four cells in `dml_spill_perf.rs`; compilation
and measurements remain unverified. The operator selects `--suite forced-spill`
for prepare, run, and render, using separate manifest and raw directories.
The generated manifests enable the same core/SQL tokio features for both
engines and preserve the release profile. The binary is `dml_spill_perf`.
The runner requires `--case two_membership_no_write --spill-parent <existing-directory>`.
Both engines must use the same spill-parent filesystem, CPU, sampling and
artifact identities. The runner supplies an owned temporary child directory
and removes it even after a timeout; the harness checks product file cleanup
before that removal. The renderer rejects missing process-resource metrics,
mixed phases/budgets/filesystems, and a fixed-engine run without an observed
shared spill file. The baseline's zero spill observation is a measured cost,
not a claim that its old cache enforces the 4096-byte policy.

The async harness uses one current-thread runtime and at most one blocking
worker, all pinned to the selected CPU. Each iteration builds a new identical
memory fixture in a transaction before timing. The timed SQL is
`DELETE FROM hw WHERE id IN (SELECT id FROM hw) AND id IN (SELECT v FROM hw) AND id<0`.
The harness checks zero affected rows, all original values, and cleanup outside
the clock, then rolls back and immediately flushes/syncs its sample.
Spill metrics describe the statement; GNU time RSS/I/O describes the whole
process including fixture and verification. The renderer retains both units
separately. Its two comparisons use the same ns/statement median and fixed
5% latency gate; no resource metric is divided by latency or called parity.

SQLite has no established equivalent of this exact 4096-byte
MemoryPolicy contract in this plan; the reference cell is an explicit
capability/configuration gap, not a parity pass. Functional cache spill tests
do not substitute for these unmeasured resource costs.
