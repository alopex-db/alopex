# #712: public SQL DELETE diagnosis

This manual diagnosis identifies a concrete first optimization for #712. It does
not accept #712, claim a speedup, or replace the published/reference comparison.
The candidate product is `e4d5f59e67b93d8e3f4a491dc725cb96a47639f8`; build source
is `34ce28d5c15377837cb78b15a261cfa2c05757a8`; measured harness source is
`a3092d56bb7aae406477d2721a5cc0758a0751a2`. The intervening changes are Python
supervision/timing only. Product Rust, Cargo and parser sources are identical.

## Observation and selected follow-up

At 10,000 rows the public server DELETE operation takes a median 26.175 ms,
including 20.624 ms executing SQL, 0.125 ms beginning the transaction, and
5.518 ms committing. The same server's 10,000-row noop and single-key SELECT
controls take 0.233 and 0.463 ms overall. The existing connection is reused.
Communication alone does not explain the DELETE-specific growth.

A separate native server stack sample contains 966 inclusive DELETE samples;
923 are under `delete::fetch_batch`, including 842 at
`TableStorage::range_scan`. The code starts at row id zero and repeatedly scans
the remaining table in 512-row batches even for primary-key equality. The mixed
sample also includes restore/validation and waits: these counts are scoped to
DELETE stacks, not process-wide CPU utilization or latency percentages.

The selected first change is to reuse `lookup_primary_key_equality`, already
used by UPDATE, for supported single-column primary-key DELETE predicates.
Retain exact predicate evaluation, foreign-key/secondary-index updates,
RETURNING, and rollback, and retain the bounded scan fallback for other shapes.
Minimal functional tests must prove both semantics and the avoided table scan.
The optimization is not implemented in this baseline PR.

Embedded begin/commit also grows with row count. Its stack shows
`LocalRangeChangeJournal::capture`/`stage`; #684/#770 owns that work. It is not
reimplemented here, and its pending change is not silently included in this
candidate baseline.

## Measurements

Median milliseconds, 3 warmups plus 21 measured operations in each fresh process.
Each row is one diagnostic cell, not three independent benchmark trials. All
raw phase samples and MADs are in [the compact report](sql-delete-baseline.json).

| Surface | Rows | Begin | Execute DELETE | Commit | Handle release | Overall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| embedded | 3 | 0.016 | 0.031 | 3.889 | 0.000 | 3.959 |
| server | 3 | 0.134 | 0.213 | 3.612 | 0.001 | 3.968 |
| embedded | 1000 | 0.765 | 0.742 | 5.272 | 0.000 | 6.880 |
| server | 1000 | 0.136 | 0.861 | 4.068 | 0.001 | 5.019 |
| embedded | 10000 | 6.658 | 36.312 | 18.714 | 0.001 | 61.670 |
| server | 10000 | 0.125 | 20.624 | 5.518 | 0.001 | 26.175 |

Noop/query/delete across embedded/server and 3/1,000/10,000 rows produced 18
successful cells, 54 warmups and 378 measured samples. Nine corresponding
surface pairs have identical input-key and final-state hashes. Every operation
checks the selected result, and each cell reconnects/reopens before checking all
remaining rows, values, types and order. DELETE restores its removed row outside
the next measurement, except after the last operation, matching the original
seed42 plan. The final state therefore contains N-1 rows for DELETE and N rows
for controls. The phase sum exactly equals overall time in all 432 records.

SQL construction, fixture population, restore and verification are outside the
timer. Handle release has its own phase and remains inside overall operation
time; a previous handle cannot leak into the next begin. The controls use
explicit READ_WRITE transactions; their single SELECT is not the original
100-request random-get case. Four fake-handle/value regression tests passed,
and the supervisor rejects CI before creating output or accessing binaries.
The 3-row smoke (two cells) and separate profile runs (two cells) are excluded
from the 378-sample timing set. All 22 fixtures were removed.

## Conditions and limits

- macOS 27.0.1 arm64, Apple M5 Pro, 18 logical CPUs, 48 GiB RAM; no CPU pinning.
- Python 3.13.16, Rust 1.96.0; `--profile test` opt-level1, debug/incremental0,
  jobs2 and `RUSTFLAGS=-D warnings`. No local builds or tests during sampling.
- Rayon2, Tokio2, OMP2, OpenBLAS1; warm OS cache. Server runs on loopback with
  auth/TLS disabled and persistent HTTP connection. These are internal
  embedded/server diagnostics, not a PostgreSQL comparison.
- Inputs are INTEGER PRIMARY KEY and 32-byte TEXT, seed42. Public storage and
  sync defaults are unchanged. Server uses default LSM BatchSync(1024,10 ms).
  Embedded's public defaults are retained; the two APIs also differ in journal
  and commit responsibilities. Do not attribute their entire difference to HTTP.
- The parser is the checksum-pinned published 0.8.16 contract0.28 archive with
  an explicit development override, not strict release parser qualification.
- Sampled client RSS peaked at 119,570,432 bytes; sampled server RSS at
  45,842,432 bytes. Client/server lifetime CPU values include setup/validation.
  They are retained separately, not interpreted as per-DELETE CPU usage.
  Product I/O was not measured here. No memory/I/O improvement is claimed.
- Each cell has a 180-second deadline, 2 GiB per-process RSS stop, 2 GB logical
  fixture bound, and host storage supervision below 40 GB. All loops are
  bounded; performance execution is never added to ordinary CI.

The independent checks, complete native samples, smoke and raw per-process logs
are retained under `/private/tmp/alopex-712-delete-profile`. Native artifact
hashes, source identities, selected stack lines and all timed samples are in the
compact report. Host storage peaked at 17,500,901,376 bytes and ended at
17,370,382,336 after removing six duplicate native outputs and every fixture.
One before server binary, one before Python package and the pinned parser are
retained by Codex solely for the immediate paired optimization comparison; remove
them after that comparison. Shared Cargo dependencies remain for active release
work. No worktree was created; existing `.codex/worktrees/` was preserved.

## Reproduction and remaining acceptance

Build the fixed server and Python package with the recorded profile/parser and
use a matching copied Python package in `PYTHONPATH`. On a measured macOS host:

```sh
python scripts/performance/run_sql_delete_profile.py \
  --python /absolute/python --server /absolute/alopex-server \
  --output /new/diagnostic-directory
python scripts/performance/run_sql_delete_profile.py \
  --python /absolute/python --server /absolute/alopex-server \
  --output /new/profile-directory --rows 10000 --operations delete --profile
```

Next implement and test the primary-key lookup, then compare fixed before/after
under the same diagnostic conditions. Full #712 acceptance still requires all
six write/random SQL and four independent SQL cases against public0.8.16,
fixed before/after and PostgreSQL with the original input sizes, 3 warmups,
21 samples and 3 trials, declared noise bounds, and server/client CPU/memory/I/O.
The SQL streaming, KV, DataFrame, HNSW and CSV scopes remain open. This limited
diagnosis neither defers them nor changes the release's staged acceptance.
