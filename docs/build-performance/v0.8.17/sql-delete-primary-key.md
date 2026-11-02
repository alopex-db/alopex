# #712: primary-key DELETE lookup

DELETE now uses the existing single-column primary-key equality lookup before
applying the original predicate and deletion logic. Indexed misses do not fall
back to a scan. Unsupported predicate shapes retain the bounded scan path.
There is no on-disk format, public API, sync-policy or durability change.

Implementation source: `355cc2663e56d2a8b138ab70b72dffdd61332b0c`.
The before product is candidate `e4d5f59e67b93d8e3f4a491dc725cb96a47639f8`,
built at `34ce28d5c15377837cb78b15a261cfa2c05757a8`. The same unchanged manual
harness from #773 measures both sides. The build-source and harness-source
identities are separate; a harness checkout is not the before binary's source.

## Functional evidence

All new cases use three target rows with SQL keys distinct from physical row
IDs. Eleven new tests and six existing joined DELETE tests passed:

- Four row-read tests prove hit/miss/NULL/reversed equality avoid enumeration,
  check secondary index removal and primary-key reuse, and preserve fallback
  results for absent PK, OR/AND, non-PK and floating-point comparisons.
- Seven async tests cover commit/reuse, rollback of row and indexes, foreign-key
  failure followed by commit, cascade, empty join source, self-source alias and
  DELETE without a predicate. Existing six alias tests also pass.
- On unchanged product, the new hit, miss/NULL and reversed-equality tests fail
  at the row-read assertions; the fallback case passes. This is an observed
  negative control, not a wall-clock performance assertion.
- Four scoped mutations in the affected executor's predicate, join and affected
  count handling are caught. No survivors or timeouts. A generated Default
  return is separately unviable because ExecutionResult has no Default.
- Focused Clippy with denied warnings, `cargo fmt --all -- --check`, and
  `git diff --check` pass. Full composed candidate/platform CI remains required.

The first mutation invocation rejected incompatible CLI flags before execution.
The second invocation exposed a build-phase scope mismatch: trailing test
arguments limited execution but not `cargo test --no-run`. It was interrupted,
and the final invocation uses `-C=--test -C=<target>` for both build and test.
Neither unsuccessful attempt is counted as a passing mutation check.

Scoped cargo-coupling reviewed `executor/dml --changed-since main
--impact-depth 4`. Existing cascade coupling links DELETE through constraints
to INSERT/MERGE/UPDATE. This change reuses the existing lookup boundary and
leaves that cascade structure intact; FK and joined tests cover its relevant
runtime behavior. The analyzer's summary and hotspot cycle labels differ, so
its score is not a completion gate. Its blind spots include runtime shared
state/timing, implicit duplicated logic, sync/async distance and macro/cfg
expansion. No new unsafe or bounded-arithmetic algorithm was introduced;
Miri/Kani were not run. No surviving mutant required a new property test.

## Manual comparison contract

Measurements use the retained before binaries and the newly built fixed binary,
with identical Python sources, checksum-pinned parser, build profile and storage
sync defaults. Both sides run three fresh-process trials for each combination of
3/1,000/10,000 rows and embedded/server public APIs. The order is
before/after/after/before/before/after. Each cell has 3 warmups and 21 measured
DELETE operations. All operation results and reopened complete table contents
are checked; deleted rows are restored outside the next timer.

The report retains every trial median/MAD and all raw warmups and measurements,
with no exclusions. The summary uses the median of the three trial medians.
The predeclared diagnostic noise band is the larger of 5% of the before median
and three times its measured MAD; this is not a release acceptance threshold.
Every total-time regression is reported regardless of that band.

Conditions match [the diagnosis](sql-delete-baseline.md): macOS arm64/M5 Pro,
Python3.13.16/Rust1.96.0, test profile opt1, debug/incremental0, build jobs2,
Rayon2/Tokio2/OMP2/OpenBLAS1, seed42, INTEGER PK and 32-byte TEXT, warm OS cache,
no CPU pinning. Server uses its unchanged BatchSync(1024,10ms) default and a
persistent loopback HTTP connection. Embedded defaults remain unchanged.
Embedded and server timings do not isolate transport overhead.

Each cell has a 180s deadline, 2GiB sampled per-process RSS stop and 2GB fixture
limit; the host is measured and stopped below 40GB. Builds and measurements run
serially. Performance tests are manual only, and remain outside ordinary CI.
Lifetime CPU includes setup and verification; no per-operation I/O improvement
or physical power-loss guarantee is claimed.

## Initial comparison results

Median of three trial medians, milliseconds. Every result is retained in [the compact report](sql-delete-primary-key.json).

| API | Rows | Execute before | Execute after | Total before | Total after | Total change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| embedded | 3 | 0.030 | 0.030 | 3.038 | 3.984 | +31.13% |
| embedded | 1000 | 0.785 | 0.027 | 6.915 | 5.978 | -13.54% |
| embedded | 10000 | 37.164 | 0.041 | 63.171 | 25.853 | -59.08% |
| server | 3 | 0.229 | 0.251 | 3.993 | 4.012 | +0.48% |
| server | 1000 | 0.804 | 0.217 | 4.991 | 4.011 | -19.63% |
| server | 10000 | 21.478 | 0.210 | 27.138 | 4.000 | -85.26% |

All 36 cells passed: 108 warmups and 756 measured operations. Every phase sum
equals its total, every same-input case has identical input-key and reopened
state hashes, and all 36 fixtures were removed. No outliers were excluded.

The 3-row embedded total increase of **31.13%** exceeds the predeclared noise
band (0.946ms increase versus 0.324ms band). Commit increases by 0.921ms while
execute changes by -0.000624ms. Before trial commit medians are
2.995/3.908/2.983ms; after are 3.916/3.938/2.997ms. This result is not dismissed
as noise. A separately recorded bounded follow-up compares the same small
DELETE against noop transactions with fixed before/after/after/before/before/
after/after/before order. It does not replace or pool the original samples.

The server 3-row total also increases (+0.48%, 0.019ms), within its predeclared
0.200ms diagnostic band. All larger-row total medians improve. These internal
results do not establish a public-release or reference-product speedup.

## Bounded small-case follow-up

All 16 follow-up cells passed (48 warmups and 336 measured operations), with
matching input/final-state hashes and complete fixture removal. These samples
remain separate from the initial comparison.

| Operation | Total before (ms) | Total after (ms) | Change | Commit before (ms) | Commit after (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| noop | 0.006563 | 0.006521 | -0.63% | 0.003438 | 0.003438 |
| DELETE | 3.016479 | 3.020021 | +0.12% | 2.966438 | 2.978271 |

The initial +31.13% DELETE regression did not reproduce in this fixed four-trial
follow-up. DELETE execute falls from 0.030021ms to 0.025271ms here. Both builds
have approximately 3ms commit medians in all four follow-up trials, while the
initial series included approximately 4ms medians on both builds. This supports
an inconsistent commit-time shift, but does not identify its cause or prove
that small-table regressions are impossible. The noop control does not write
and therefore does not reproduce the durability work of DELETE commit. No
further repetitions were added to seek a preferred result.

## Resources and cleanup

Across the 52 cells, sampled client RSS peaked at 121,651,200 bytes and server
RSS at 46,792,704 bytes. CPU lifetime receipts include setup/validation and are
retained in the raw report; these values are not per-operation CPU or I/O proof.
Host storage peaked at 18,466,422,784 bytes and ended at
17,774,637,056 bytes after cleanup. All 52 fixtures,
owned test executables, before/after server and Python native outputs, copied
packages and pinned parser/archive were removed. Artifact hashes and cleanup
receipts remain in the report; compact execution logs are retained in
`/private/tmp/alopex-712-delete-fix`. Shared Cargo dependencies remain for the
active release work. No worktree was created; `.codex/worktrees/` is untouched.

## Remaining integration and acceptance

PR #774 is stacked on diagnosis PR #773. After required CI succeeds, integrate
#773, retarget #774 to the candidate and verify the resulting composition before
merging it. Neither this focused change nor the diagnostic comparison accepts
all of #712. Published0.8.16/reference comparisons, the other SQL/KV/DataFrame/
HNSW/CSV scopes, candidate qualification, release and post-release main merge
remain in the #690 plan. Embedded transaction journal cost is owned by #684/
#770 and is not included in this implementation.
