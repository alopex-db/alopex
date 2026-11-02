# SQLite composite boundaries and diagnosis (#712)

This follows #788 and isolates the adopted SQLite composite workload. It adds a manual diagnostic and small harness tests; it does not change Rust product behavior or establish release performance acceptance.

## Reproduction and identities

Native and harness source: `0e95066b7a80874a4a4771dc78ee4ae3806f3ce0`, parent #788 `37f0b0df8f5d6e6a0af26e3cafde3b7efe953ea9`. Cargo files and product crates are unchanged from that parent. The original source is `d9ff8783b8d18a63d167204e55902ea6cd90dcd1` plus the run-20261021 fairness patch SHA256 `9b2dde147fce3a1ce820c65fb91627595d75832820cce34526f7abb575abd68b`; the corrected measurement file has SHA256 `7ba8a8e9da92987d2f0dbdc6ef3d98f2a12e5c4dc087651ae279a1dd6fbf1800`.

Nine original helper bodies are extracted unchanged into `workloads/sqlite_composite_v0816.py`, with AST comparisons and source hashes recorded. The original sequence is COUNT, ordered SELECT with COALESCE/LIMIT16, INSERT/savepoint/UPDATE/rollback-to/release/full rollback, prepared SELECT, and a valid foreign-key/CHECK INSERT followed by rollback. Both engines reuse one in-memory connection per case. Alopex uses public transaction/statement APIs; SQLite retains the original executescript and cached connection execution behavior. The pysqlite3 0.5.4 source defaults to 100 cached statements.

The 34,250-row fixture and final state digest `bdb0d074606282cb84cbb4445c7bb6c7544057d81fd2ee0dc98f3ea834e3840b` match all six original Alopex/SQLite trial reports. A three-row fixture provides a small control. No disk transaction or durability scenario is substituted for this workload; #684 disk begin evidence covers a separate contract.

Reference SQLite is **3.46.1**, built from its official amalgamation using pysqlite3 **0.5.4** source with `-O2 -g0`. The downloaded sdist matches its PyPI digest. Archive, source and native hashes, SQLite source_id, compile options and settings are recorded. The original Linux environment used pysqlite3-binary0.5.4/Python3.11.11; this macOS arm64 run uses a locally built wrapper/Python3.13.16. They are not the same binary environment.

Alopex uses one shared-target `cargo build --offline --locked --profile test -p alopex-py` with extension-module configuration, test opt-level1, debug/incremental0, jobs2. The verified v0.8.16 parser is a development override, not release-asset qualification. Reference and native builds were serialized. Thread environment requests RAYON/TOKIO/OMP/OPENBLAS=1; macOS CPU affinity is not pinned. Compiler optimization and runtime differences prevent interpreting this baseline as a matched release comparison or comparing its ratios with the historical Linux 9.551x as a version regression.

## Two explicit measurement contracts

The **original lane** invokes the extracted original workload without changing its body. SELECT results are discarded inside that operation. After every timed operation, an independent typed full-state oracle verifies every bench row, parent row, empty child table and rollback. Query results are independently checked before and after each case, outside timing. Original per-sample return remains None.

This local SQLite build reports `PRAGMA foreign_keys=0` by default. The original runner never explicitly enabled it. Consequently, this lane preserves the historical operation boundary but **does not establish constraint-equivalent performance**. The original Linux PRAGMA value was not recorded and cannot be inferred from this build.

The **checked lane** explicitly enables and verifies SQLite foreign_keys=1. It retains actual aggregate, ordered and prepared results for exact type, value, column, length and order validation after every timer. Ordered SELECT gains an `AS value` alias. Result disposal is outside the primary timer, unlike the original lane, so the two lanes are not pooled. A positive cycle precedes separate negative FK and CHECK preflights; only the expected error category is accepted. A separate savepoint preflight proves the update is visible before rollback-to, the old value is restored afterward, the pre-savepoint insertion remains, and full rollback clears the transaction.

Checked timing has no internal phase clocks. A separate observation loop records the five component intervals and validates results/state outside its timers. Setup, fixture validation, constraint preflights, plan observations, reporting and profile runs are excluded from primary samples. Profiles validate the last result and final state, not every internal profiled iteration. Plan observations use each engine's EXPLAIN interface and are diagnostic, not equivalent planning APIs.

Both manual entry points reject CI before importing native packages or creating work directories. Ordinary CI gets only tiny fake/native-independent harness tests. No workload or performance threshold is added to normal CI.

## Measurements

Two sizes × four operations × three trials produced **24 passing timing cells, 504 measured samples and 72 warmups**. Engine order is Alopex/SQLite, SQLite/Alopex, Alopex/SQLite within each lane. Two separate four-second profile workers also passed. There are 546 separately measured plan calls and 78 plan warmups across all 26 workers. The 12 checked timing cells add 252 measured five-phase cycles and 36 warm cycles: 1,260 measured intervals plus 180 warm intervals. All raw values are retained.

Times below are medians of three trial medians, in milliseconds. This is baseline diagnosis, with no before/after improvement claim.

| Rows | Lane | Alopex ms | SQLite ms | Alopex/SQLite |
|---|---|---:|---:|---:|
| 3 | Original, SQLite FK off | 0.153459 | 0.010875 | 14.111 |
| 3 | Checked, SQLite FK on | 0.159250 | 0.011917 | 13.363 |
| 34250 | Original, SQLite FK off | 5.776750 | 0.440834 | 13.104 |
| 34250 | Checked, SQLite FK on | 5.846916 | 0.442875 | 13.202 |

The first three-row original trial was slower for both engines: Alopex trial medians 0.219750/0.153459/0.153292ms, SQLite 0.029542/0.010833/0.010875ms. Its cause is unproven and it remains in the raw data; there was no repeat-until-green run. Checked large-case Alopex trial medians are 5.848125/5.846916/5.827959ms, SQLite 0.442875/0.447750/0.439375ms.

| Separate checked phase | Alopex 3 rows | SQLite 3 rows | Alopex 34250 rows | SQLite 34250 rows |
|---|---:|---:|---:|---:|
| COUNT aggregate | 0.031125 | 0.001125 | 5.655542 | 0.404667 |
| Ordered SELECT | 0.037375 | 0.001125 | 0.067792 | 0.007708 |
| Savepoint/rollback cycle | 0.036875 | 0.005583 | 0.070834 | 0.009833 |
| Prepared SELECT | 0.028959 | 0.000833 | 0.039709 | 0.003167 |
| Constraint INSERT/rollback | 0.020167 | 0.002292 | 0.026125 | 0.006666 |

Independent phase medians need not sum to the primary median. The large-case cost is concentrated in COUNT, not in transaction begin alone. Primary measured block-IO counters were zero in every cell. CPU times broadly track wall time. Largest timing-worker process RSS was 97,681,408 bytes for Alopex and 51,085,312 bytes for SQLite, including runtime, fixture, hashing and full-value validation; these are not engine-only memory measurements. Full per-trial medians, MAD, CPU, RSS and separate plan observations are in the JSON.

## Profile evidence and next action

Alopex's checked profile completed 651 iterations. cProfile attributes 3.858 seconds of its 3.987-second composite interval to the aggregate closure; the other four closures total about0.127 seconds. The two begin calls per cycle account for0.005 seconds across1,302 calls. SQLite completed8,943 profile iterations, with3.694 seconds in its aggregate closure out of3.903 seconds in the composite. These are instrumented observations, not substitutes for uninstrumented timings.

One Alopex native stack shows1,503 inclusive query-execution samples, of which a948-sample branch builds the iterator pipeline and946 reaches execute_scan. Within that path,520 reach TableScanIterator::next, with memory merged-iterator key/value cloning and allocation visible beneath it; RowCodec decoding is also observed. Aggregate/filter evaluation appears in another branch. These are nested observations, not independent percentages to sum.

Next owner/action: Codex will inspect and reduce avoidable key/value copying and eager row materialization in the in-memory COUNT scan path, preserving snapshot visibility, write-set overlays, row validation and SQL NULL/filter semantics. A focused small regression/property suite must accompany any behavior change, followed by a fixed before/after use of this same matrix. Disk #684 begin optimizations cannot be used to declare this memory workload accepted. Candidate integration and the final frozen-release/reference comparison remain required.

## Verification and cleanup

`python3 -B -m unittest discover -s scripts/performance/tests -p 'test_*_profile.py'`: **52 passed**, including nine new small cases. They cover original/checked operation order, optional phase clocks, statement finalization on failure, transaction rollback on failure, typed results, three-row SQLite savepoint/constraint behavior, rejection of unrelated errors, timer/validation/disposal boundaries, and early CI rejection.

The first test attempt failed because a test closed its SQLite connection inside a context manager that subsequently used it. The test now uses contextlib.closing; the initial log is preserved. The first native build rejected the generated parser SHA256SUMS line order; library hashes were correct. Restoring the exact dynamic-then-static contract made the second build pass without disabling checks. SDK-observation/development-parser warnings remain in logs. The analysis script initially rejected sample's normal profile-progress stderr; it now verifies those exact three lines for profile workers while requiring empty timing stderr. No matrix was rerun for these analysis checks.

Target: alopex/scripts/performance/sqlite_composite_profile.py and its manual wrapper/reference/tests. Mutation check: not run; no Rust product behavior changed. Survivors: not applicable. Strengthening: nine small focused harness cases. Verification:52 harness tests, nine-function AST extraction, one successful reference build and one successful native build after the preserved setup failure, all24 timing cells and two profiles, source/hash/state/settings audit. No Rust mutation/coupling/Miri/Kani execution was needed for this harness-only stage.

All26 worker PIDs were confirmed absent, connections closed and case fixtures removed. Owned native/reference packages, reference source/build dependencies, downloads, parser/archive and four native target exports were deleted. Compact source-bound logs/scripts and shared Cargo dependencies remain. Host initial allocation19,211,739,136 bytes, observed peak19,363,942,400 bytes, after cleanup19,214,278,656 bytes, within the3GB additional bound and40GB stop/50GB ceiling. Cases were serialized with180-second/2GiB limits.

The companion JSON contains identities, raw observations, profiles, source extraction, commands, initial failures, successful logs and cleanup receipts. This PR is a diagnostic handoff. #712 acceptance, candidate CI/integration, RC/package qualification, release publication, postrelease checks and main merge remain tracked by #690.
