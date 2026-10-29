# Public Python HNSW begin and resource comparison

This extends #684 / #770 with manual disk measurements using the two existing
[source-qualified Python wheels](transaction-begin-python-comparison.md).
No product build or normal CI performance execution was added. The product
sources remain `b5ad28540edcd8aacb91ff0d2d3dae8f4fd873bf` (before) and
`e6c64f3b505ae54a14e3a6803c551a19e0c7a203` (after, product-equivalent to
`90b8b0b560ae79fd6bfd78a3a3e588ae3422a7ad`). Each child verifies its loaded
extension SHA against the recorded wheel identity.

## Fixed protocol

Use `scripts/performance/run_begin_acceptance.py` and `begin_acceptance.py`
on native macOS. Install each qualified wheel offline into one venv, serially:

```sh
python scripts/performance/run_begin_acceptance.py \
  --python /path/to/venv/bin/python --identity /path/to/source-identity.json \
  --output /path/to/new-output-directory
```

The identity JSON records `product_source`, `product_equivalent_to`,
`wheel_sha256` and `extension_sha256`. The runner records these plus harness
hashes. Same host, CPython 3.13.16 and dev/opt-level-1 builds as the linked
comparison; both builds use the documented development parser override.

- 0/100/1,000 rows, always the same schema with two 2D vector columns; only
  index count changes: 0/1/2. Zero rows means an empty table, not no catalog.
- Prepare and close each fixture in a separate seed process. Fresh reader
  processes cover RO/RW noop, RO/RW query and query autocommit.
- Each reader records one first-after-open and five warm samples. Explicitly
  warm every requested index before sample 1; both indexes fit in 8 MiB/two
  entries. A fresh reader is not an OS-cold cache. Indexed warm queries must
  increase the shared-cache hit count.
- The zero-index query uses exact scanning. Indexed queries explicitly require
  HNSW. Compare each configuration before/after; do not infer cross-index
  query speedups from this test. Every query checks the expected nearest row.
- Explicit transactions time begin, query and rollback separately. Autocommit
  records `execute_sql` total only; it is not an isolated RW begin measurement.
  Process RSS and fixture allocation snapshots occur outside timed intervals.
  This instrumentation between phases differs from the earlier bare timing
  worker; compare only matching workers, not timings across reports.
- 1,000-row RW noop additionally runs two fresh readers with one sample each:
  three first-begin RSS deltas per index count. The predeclared memory criterion
  is median reduction greater than `3 * max(MAD_before, MAD_after)`.
- Timing criterion: at least 10% median reduction and difference above the same
  noise bound, using the five warm samples. No extra samples were selected to
  improve an unfavorable result.

Accepted evidence is **120 processes (18 seed + 102 reader), 552 reader
samples**, across both sources. All final rows and both vectors are checked;
every fixture is removed before the next configuration. The maximum is 1,000
rows, with no large load/performance lane added to ordinary CI.

## READ_WRITE noop begin

Warm medians, milliseconds. All nonempty configurations meet the timing
criterion; no empty-table improvement is claimed.

| Rows | Indexes | Before | After | Criterion |
| ---: | ---: | ---: | ---: | --- |
| 0 | 0 | 0.022583 | 0.020542 | Not met |
| 0 | 1 | 0.026417 | 0.023000 | Not met |
| 0 | 2 | 0.025125 | 0.021583 | Not met |
| 100 | 0 | 0.564250 | 0.017250 | Met |
| 100 | 1 | 0.562875 | 0.018959 | Met |
| 100 | 2 | 0.573708 | 0.019000 | Met |
| 1,000 | 0 | 4.935375 | 0.015250 | Met |
| 1,000 | 1 | 4.967584 | 0.021333 | Met |
| 1,000 | 2 | 4.932750 | 0.022000 | Met |

At 1,000 rows the reductions are 99.55–99.69%. This supports the earlier
profile evidence that full SQL capture at RW begin caused the row-dependent
cost. Index count does not explain the removed full-row scan. RO controls are
recorded separately; this change does not claim a general RO speedup.

## First-begin retained RSS

Native `ps` RSS in KiB is converted to bytes. Each value below is the median of
three fresh-reader deltas `RSS_after_begin - RSS_before_begin`, in KiB.
This measures retained process RSS at phase boundaries, not allocations or
an instantaneous peak inside begin. macOS page granularity affects the MAD.

| Indexes | Before delta | After delta | Reduction | Noise bound | Criterion |
| ---: | ---: | ---: | ---: | ---: | --- |
| 0 | 960 | 304 | 656 | 0 | Met |
| 1 | 1008 | 352 | 656 | 48 | Met |
| 2 | 1040 | 336 | 704 | 144 | Met |

The fixed 1,000-row comparison observes a 65.08–68.33% reduction in this RSS
increment. The unchanged baseline remains nonzero: Python/native transaction
setup and other resident pages are included. Three local repetitions do not
establish a universal memory bound or a maximum supported database size.

`getrusage(RUSAGE_SELF).ru_maxrss` is bytes on this native macOS host, verified
against its `getrusage(2)` manual. The largest reader peak before final all-row
validation was 78,856,192 bytes before and 78,233,600 after. This whole-process
high-water mark includes import/open/warm/query and is not used as proof of
begin-phase improvement. Parent 100 ms sampled RSS may miss short-lived peaks.

## Query and autocommit remain separate

At 1,000 rows, warm medians in milliseconds:

| Indexes | RW query before | RW query after | Autocommit before | Autocommit after |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 2.617459 | 2.591209 | 0.523625 | 0.528209 |
| 1 | 0.145583 | 0.128708 | 0.131333 | 0.136500 |
| 2 | 0.152791 | 0.137292 | 0.131708 | 0.131125 |

These 1,000-row query/autocommit comparisons do not meet the improvement
criterion. The main result is RW begin; it must not be reported as a measured
HNSW query or autocommit improvement. Full phase samples/MADs, including RO and
rollback, are retained in `comparison.json`.

## Disk use and cleanup

Fixture allocated bytes at 1,000 rows were 647,168 / 999,424 / 1,351,680 for
0/1/2 indexes, identical before/after. Empty-table allocation was 147,456 bytes;
100-row allocation was 196,608 / 237,568 / 278,528. No allocation or logical-file
size change occurred across the measured RW noop begin/rollback boundaries.
The default legacy WAL is sparse: corresponding 1,000-row logical fixture sizes
were 537,376,681 / 537,728,978 / 538,081,196 bytes. Neither logical ring size nor
this allocation observation is an RSS limit.

The guard enforces a 90-second process limit, 2 GiB RSS stop, 2 GB additional
host filesystem usage, 16 GiB free-space reserve and the 40 GB Alopex host stop.
Host inventory was 16,085,573,632 bytes before setup and at most 16,124,440,576
at recorded checkpoints, and 16,087,113,728 bytes after cleanup. Venv and all
fixtures are reclaimed. The two existing
wheels (24,606,990 bytes) remain owned by #684 for the unresolved first-write
comparison; remove them after that evidence is recorded. No worktree was made.

## Invalid attempts, regression checks and remaining acceptance

The first zero-index query incorrectly required HNSW. Its failed run is
excluded; the fixture was removed. The worker was corrected before accepted
comparisons. During a later one-sample reader, `ps` observed process exit before
the parent reaped it. The monitor falsely reported missing live RSS. It now
waits at most 50 ms to distinguish exit from an unmeasurable live process;
a still-live process fails closed and is killed/reaped.

Three monitor unit tests pass, including six bounded failure subcases:
missing/failed/zero RSS, RSS limit, storage growth, timeout, nonzero child exit,
and successful exit-race handling. They use fake children, not performance
loads. Rust product code is unchanged; no Rust rebuild, mutation, coupling,
Kani or Miri repetition is needed for these diagnostic Python changes.

```sh
python3 -m unittest discover -s scripts/performance/tests -p test_begin_acceptance.py -v
```

Completed 0/100-row baseline configurations were retained from `before/`.
The partial 1,000-row configuration was excluded and rerun in `before-1000/`;
`after/` contains all fixed-product configurations. The worker hash is identical
across every accepted run. The runner changed only exit/RSS monitoring between
the smaller baseline configurations and the remaining runs. Both runner hashes
are retained, and excluded logs are not overwritten.

This is development evidence. Native Rust HNSW/index/resource coverage,
first-write overhead, OS CI and composed candidate acceptance remain. #684 and
v0.8.17 release readiness are not declared complete.

Evidence directory: `/private/tmp/alopex-684-grid` (`plan.json`, per-run identity,
raw JSONL, selected `results.json`, `comparison.json`, `summarize.py`, storage and
cleanup records). Worker SHA-256:
`6a661662beac7c4fda6b138cda752fe92bcd0b14b81545228f73ceae957122ea`.
Final runner SHA-256:
`06f06f7c39b908905fb54610ac8456e612b459a6cffb892249ed2f6ee48497b2`.

## First-write follow-up

The [bounded scalar first-write comparison](transaction-first-write-comparison.md)
now records time, phase RSS, initial regression flags and their fixed balanced
replication. Both retained wheels and the follow-up venv have been reclaimed;
prior wheel retention notes above describe the earlier handoff. Native
index/resource coverage, OS CI and candidate integration remain.
