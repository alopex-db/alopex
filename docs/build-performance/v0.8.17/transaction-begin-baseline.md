# #684: transaction begin diagnostic baseline

At product source `5fe5cbc19dd8fa1bbebe3e7add3a708dda3c2a0f`, disk-backed
READ_WRITE begin has a row-dependent cost even without a query or mutation.
Native stack samples attribute the observed begin cost to
`LocalRangeChangeJournal::capture`: LSM does not expose `journal_pending_writes`,
so capture scans/materializes the SQL row/index keyspace at transaction start.
This establishes a concrete target for remediation; no product fix or #684
acceptance is claimed by this diagnostic PR.

## Observed boundary

One process owns one fixture. The dataset contains 0, 100 or 1,000 rows in a
single table with `id INTEGER PRIMARY KEY, value INTEGER`; value is `3 * id`.
There are no HNSW indexes. Rows are seeded in one transaction and disk databases
are closed/reopened before measurement. Every process checks all final rows,
values and ordering after the timed operations. The fixture is then deleted.

Each combination records one first-after-open sample separately, followed by five
warm samples. `noop` measures begin and rollback without SQL/INSERT.
`query` records a point SELECT and rollback separately. Native query timing
includes checking the single-row result; Python checks it immediately after the
query timer. Comparisons between API implementations are diagnostic, not speedup
claims. First-after-open does not mean a cold operating-system page cache.

The accepted dataset contains 72 successful timing processes and 432 samples (72 first-after-open,
360 warm), across memory/disk, RO/RW, noop/query, and native borrowed/owned plus
public Python. All 72 fixtures were reclaimed. Separate four-second native sampling loops obtained two stack profiles without
mixing those iterations into the timing results. Their fixtures were reclaimed too.
The first 24 Python cases and one Python profile are retained but excluded: the
previous transaction handle could be destroyed inside the next begin assignment.
The worker now explicitly destroys that handle outside timing, and only those 24
cases were rerun. A fake-handle regression test and a negative probe reproduce the
old lifetime error and verify the correction. All 99 executed fixture processes
(including excluded runs) were reclaimed.

Warm noop begin medians in milliseconds:

| API / backing | Mode | 0 rows | 100 rows | 1,000 rows |
| --- | --- | ---: | ---: | ---: |
| Rust borrowed / disk | RW | 0.00183 | 0.05950 | 0.48071 |
| Rust owned / disk | RW | 0.00233 | 0.12075 | 0.89600 |
| Python public 0.8.16 / disk | RW | 0.00133 | 0.08717 | 0.72217 |
| Rust borrowed / disk | RO | 0.00021 | 0.00025 | 0.00017 |
| Rust owned / disk | RO | 0.00038 | 0.00029 | 0.00025 |
| Python public 0.8.16 / disk | RO | 0.00050 | 0.00050 | 0.00050 |

The corresponding memory RW medians at 1,000 rows were 0.00071 ms (borrowed),
0.00100 ms (owned), and 0.00088 ms (public Python). Small nanosecond-scale
measurements are noisy; the disk RW scaling and direct native stack evidence,
not ratios between those tiny values, determine the next action.

## Attribution and limits

In the native borrowed profile, all 1,368 sampled stacks under `Database::begin`
passed through journal capture. In the owned profile, 1,446 of 1,447 sampled
begin stacks passed through capture and its owned cursor collection. These are
sample counts within the begin subtree, not percentages of total CPU time or
precise wall-clock durations. The borrowed path scans into a BTreeMap; the owned
path walks its LSM cursor and materializes the journal snapshot.

The corrected public Python measurements also reproduced row-dependent RW
initialization. Its earlier native stack sample is retained as excluded evidence:
it used the pre-correction handle lifecycle and its native symbols are stripped.
The corresponding source path uses `begin_owned_embedded_transaction`, but do not
substitute native candidate measurements for a source-matched Python before/after
comparison.

No evidence here attributes cost to HNSW cache misses, INSERT graph copying or
lock contention. No HNSW graph is present and no INSERT occurs in the timed
transaction. This keeps #684 separate from #681.

## Reproduction and resource rules

- Host: Apple M5 Pro, macOS 27.0.1 arm64, 18 logical CPUs, 51,539,607,552 bytes RAM.
- Native source: candidate `5fe5cbc1`; Cargo test profile (`opt-level=1`),
  incremental/debug off, build jobs 2. Public Python: wheel 0.8.16, CPython 3.13.16.
- Native parser: published v0.8.16 contract 0.28.0 with explicit development
  override. This is not parser or package release qualification.
- Runtime: Rayon/OMP threads 2, OpenBLAS threads 1. No concurrent product builds
  or other performance runs during timing/profile execution.
- Per process: 90-second timeout, sampled RSS stop 2 GiB. Runner stops on failed
  live RSS measurement, host storage 40 GB, or additional filesystem growth over
  2 GB. One fixture at a time; no normal CI execution.
- Sampled RSS maxima were 25,919,488 bytes for timing and 16,089,088 bytes for
  accepted native profiling. Sampling every 100 ms can miss short-process peaks; these are lower
  bounds, not memory acceptance results. Disk and memory scaling acceptance is
  still required.

Build the example only after the required host inventory and bounded output
plan, using the shared Cargo target and approved parser setup:

```sh
cargo build --offline --profile test -p alopex-embedded --example begin_profile
python3 scripts/performance/run_begin_profile.py \
  --native /absolute/shared-target/debug/examples/begin_profile \
  --python /absolute/public-wheel-venv/bin/python \
  --output /absolute/new-evidence-directory --rows 0 100 1000
```

Add `--profile --rows 1000` with a different output directory for separate macOS
stack sampling. The Python worker and native example also accept one explicit
bounded case. The runner refuses an existing output directory.

Detailed samples, MADs, stack traces, binary/parser identities and failure logs
remain in local evidence `/private/tmp/alopex-684`. Initial example compile errors
used the wrong expected SQL integer type; the final build and focused Clippy
passed. Guard probes reject RSS overflow, unavailable live RSS measurements and
missing completed results, and remove their fixture directories.

## Remaining acceptance and next change

Implement write-local before-images for both borrowed and owned LSM transactions,
so journal capture can avoid the initial complete scan while preserving exact
journal deltas and snapshot semantics. Validate snapshot visibility,
read-your-writes, conflicts, rollback/savepoints, concurrent updates and reopen
with minimum-sized fixtures. Account for retained before-image memory and errors;
do not silently substitute an empty original value on a failed read.

Then repeat the fixed-source comparison on both public Python and Rust entrypoints.
Before/after acceptance remains at least 10% improvement and an absolute median
difference above `3 * max(MAD_before, MAD_after)`, with zero semantic regressions.
HNSW index counts 0/1/2, memory/disk resource scaling, autocommit, cold/warm
classification and remaining sizes still require evidence. The existing 0/100/1,000
row result is enough to select the first repair; do not expand to 10,000 rows just
to repeat that discovery. Candidate integration and release qualification remain
separate gates.
