# First-write cost after removing full SQL capture at begin

This #684 / #770 development comparison checks whether the LSM begin fix merely
moves excessive cost to the first write. It reuses the two source/hash-qualified
wheels from the [public Python comparison](transaction-begin-python-comparison.md):
`b5ad28540edcd8aacb91ff0d2d3dae8f4fd873bf` before and
`e6c64f3b505ae54a14e3a6803c551a19e0c7a203` after (product-equivalent to
`90b8b0b560ae79fd6bfd78a3a3e588ae3422a7ad`). No Rust rebuild or product-source
change was made. Every seed and reader checked the loaded extension SHA.

## Protocol and predeclared decision

Same native macOS host, CPython 3.13.16 and dev/opt-level-1 wheels as the linked
report. Use `first_write_profile.py` through `run_first_write_profile.py`:

```sh
python scripts/performance/run_first_write_profile.py \
  --python /path/to/venv/bin/python --identity /path/to/source-identity.json \
  --output /path/to/new-output-directory
```

- 100/1,000 rows, identical two-vector-column fixtures, 0/1/2 HNSW indexes.
  Seed in a separate process and reclaim each configuration immediately.
- Two readers per fixture: one distinct key and eight distinct keys. Each runs
  six transactions: first-after-open plus five warm samples. Warm both requested
  indexes before sample 1. This is not an OS-cold-cache measurement.
- Time begin, first scalar UPDATE (`id=0`, `value += 1`), repeated UPDATE of the
  same key (`value += 2`), seven additional scalar UPDATEs when requested, and
  rollback separately. Both vector columns remain unchanged.
- Validate the updated values inside the transaction, outside timing, then
  rollback. Validate every original row and vector after all iterations.
- Primary criterion fixed before execution: the median sum of begin + first
  UPDATE improves by at least 10%, and its difference exceeds
  `3 * max(MAD_before, MAD_after)`. This sum excludes instrumentation between
  phases and must not be called end-to-end transaction wall time.
- Flag first-UPDATE regressions independently when its median increases by
  more than 10% **and** exceeds the same noise bound. Keep any flagged condition
  visible even if the combined time improves. Repeat/additional-write phase
  results use the same descriptive flags.

Main matrix: **36 processes (12 seed + 24 reader), 144 reader samples**. All
processes passed row/value validation and all fixtures were reclaimed. The
existing tested guard is reused: 90 seconds per child, RSS below 2 GiB, additional
host storage below 2 GB, 16 GiB free reserve, and 40 GB host stop. Performance
execution is manual and was not added to ordinary CI.

## Main matrix

Warm medians in milliseconds. Every begin + first UPDATE sum meets the primary
criterion (68.22–96.45% reduction). A distinct-key count of 8 includes the first
key, its repeated update and seven other updates; it is nine UPDATE statements.

| Rows | Indexes | Distinct keys | First UPDATE before | First UPDATE after | Begin + first before | Begin + first after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 0 | 1 | 0.129667 | 0.149542 | 0.706292 | 0.168584 |
| 100 | 0 | 8 | 0.118041 | 0.122667 | 0.679666 | 0.140042 |
| 100 | 1 | 1 | 0.141000 | 0.142958 | 0.698166 | 0.167583 |
| 100 | 1 | 8 | 0.141792 | 0.137208 | 0.688334 | 0.165624 |
| 100 | 2 | 1 | 0.132625 | 0.185708 | 0.691417 | 0.219750 |
| 100 | 2 | 8 | 0.131333 | 0.157458 | 0.691250 | 0.189917 |
| 1,000 | 0 | 1 | 0.155375 | 0.162750 | 5.010458 | 0.180250 |
| 1,000 | 0 | 8 | 0.151083 | 0.158458 | 4.837999 | 0.171874 |
| 1,000 | 1 | 1 | 0.225917 | 0.236209 | 5.159792 | 0.261459 |
| 1,000 | 1 | 8 | 0.186125 | 0.184292 | 5.054416 | 0.208667 |
| 1,000 | 2 | 1 | 0.214166 | 0.231209 | 5.048999 | 0.255375 |
| 1,000 | 2 | 8 | 0.188708 | 0.209917 | 5.165166 | 0.234209 |

The sum of begin + all measured writes also meets the improvement criterion in
all 12 configurations. That combined result does not dismiss isolated phase
regressions. The initial 100-row/two-index measurements flagged:

- First UPDATE, one-key reader: 0.132625 → 0.185708 ms (+40.02%), increase
  0.053083 ms versus a 0.031248 ms noise bound.
- Additional seven UPDATEs, eight-key reader: 0.445292 → 0.536083 ms (+20.39%),
  increase 0.090791 ms versus a 0.063876 ms noise bound.

Both original results remain in `comparison.json`; no failed measurement was
removed or replaced to obtain a favorable result.

## Fixed replication of the flagged condition

Before further execution, `replication-plan.json` fixed a balanced product
order **before/after/after/before/before/after/after/before**. Only the flagged
100-row/two-index configuration was rerun, with both key-count readers. Each
source therefore has four independent reader-process medians per key count;
each median still uses exactly five warm samples. This adds **24 processes
(8 seed + 16 reader), 96 reader samples**. No extra rows, index sizes or product
builds were introduced.

The decision uses the median and MAD of the four process medians, with the same
10% and three-MAD regression thresholds. Initial results stay separate.

| Phase | Before (ms) | After (ms) | Increase | Absolute increase (ms) | Noise bound (ms) | Flag repeated? |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| First UPDATE, one key | 0.150792 | 0.157750 | 4.61% | 0.006958 | 0.011250 | No |
| First UPDATE, eight keys | 0.137063 | 0.143750 | 4.88% | 0.006687 | 0.018499 | No |
| Seven additional UPDATEs | 0.437395 | 0.455417 | 4.12% | 0.018021 | 0.024501 | No |

The large initial flags did not reproduce under this balanced process order.
The smaller positive first-write differences are compatible with the extra
per-key snapshot lookup, but these timings do not identify their cause. Do not
claim zero overhead or erase the initial observations. Begin + first UPDATE
still improves by 74.21% (one key) and 76.24% (eight keys) in this replication.
No further timing repetition was performed after this bounded question was
answered.

## Memory, disk and semantic observations

All phase snapshots are outside timed intervals. At 1,000 rows, the initial
first UPDATE raised RSS by roughly 3.2–3.3 MB on both products. The total delta
from before begin through all writes was lower on the fixed product in each
of the six main 1,000-row readers:

| Indexes | Distinct keys | Before begin through writes, before (KiB) | After (KiB) |
| ---: | ---: | ---: | ---: |
| 0 | 1 | 4240 | 3568 |
| 0 | 8 | 4320 | 3696 |
| 1 | 1 | 4256 | 3648 |
| 1 | 8 | 4448 | 3760 |
| 2 | 1 | 4192 | 3664 |
| 2 | 8 | 4480 | 3712 |

These are single fresh-reader phase deltas per main configuration, not a new
statistical memory acceptance or instantaneous allocation peak. The separate
[begin-only RSS experiment](transaction-begin-hnsw-resources.md) supplies the
three-reader begin comparison. Maximum native reader `ru_maxrss` before final
validation was 79,134,720 bytes before and 78,577,664 after in the main matrix;
this whole-process peak does not isolate begin or write allocation.

No measured phase changed fixture allocation or logical file sizes. This is an
uncommitted UPDATE/rollback experiment; it does not measure commit persistence,
throughput, large transactions or vector-value updates/HNSW graph mutation.
Those must not be inferred from these scalar UPDATE results.

Every transaction checked the changed scalar values before rollback. Every
reader checked all original rows and both vector columns afterward. All 60
processes across main and replication exited successfully and all fixtures were
reclaimed. Rust semantic/mutation evidence remains bound to the unchanged
product commit; these diagnostic Python files do not require a repeated Rust
build, cargo-mutants, coupling, Kani or Miri run.

## Source identity, cleanup and next acceptance

The main matrix ran identical workers/runners before and after. Between main
and replication, the runner gained `--rows`/`--indexes` selectors to avoid
rerunning unflagged conditions. The measurement worker and shared guard were
unchanged; all eight replication batches have identical harness hashes.
The exact original main-matrix runner is also retained and its recorded SHA
verified. Measurement worker SHA-256:
`5cccd646bb14b6a3903eb76e592bcd510b5bc0f144fdc552b2c352695d959605`;
final runner SHA-256:
`2dc83fd71068ec34ad7d004b33129dee206eb7a4238242f4d92f325a31a205ec`.

Evidence: `/private/tmp/alopex-684-first-write` contains both plans, source and
harness identities, raw phase samples, results, `comparison.json`,
`replication-comparison.json`, summary scripts, resource logs and cleanup record.
The source-qualified wheels were reused without network/build work, then both
wheels (24,606,990 bytes total) and the venv were removed after recording. Logs,
hashes and source identities remain; deleted wheels are no longer reusable.
No worktree or per-issue Cargo target was created.
Host storage was 16,087,203,840 bytes before setup, at most 16,125,710,336 at
recorded checkpoints, and 16,063,713,280 after cleanup.

This closes the bounded public Python first-scalar-write comparison. Native
borrowed/owned index/resource evidence, required OS CI and candidate composition
remain before #684 acceptance. #770 remains stacked on #769 and is not a
release-ready or merged candidate claim.
