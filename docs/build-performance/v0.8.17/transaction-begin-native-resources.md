# Native indexed begin resource evidence and #684 development reconciliation

This fills the remaining native borrowed/owned index and phase-RSS conditions
for #684 / #770. It adds a manual diagnostic example and runner, without changing
production Rust behavior or adding performance execution to ordinary CI.
Development evidence is ready for integration review; issue and candidate
acceptance still require CI and candidate composition.

## Source identity and bounded scope

| Item | Before | After |
| --- | --- | --- |
| Build source | `b5ad28540edcd8aacb91ff0d2d3dae8f4fd873bf` | `b8bca4d958a2cf35f9439b217186301f0a15338f` |
| Product equivalence | candidate `5fe5cbc1` | fix `90b8b0b5` |
| Binary SHA-256 | `1d036a59cc87ac19fea146814d54676be8f15a6c50ed8cde01fc41a50ddc3919` | `852c9a63ab351747ebbb22b5128bb1882949df8c2e5db0e7e312599ff2570bfe` |

Use `crates/alopex-embedded/examples/begin_resources.rs` and
`scripts/performance/run_native_begin_resources.py`. Both sources ran the exact
same diagnostic code. The baseline checkout restored only diagnostic files;
production diffs were checked empty. The original branch was restored after
baseline execution. Each process hashes its running executable and the runner
compares that with the recorded binary digest.

Native macOS arm64, the same host as the prior reports. Sequential builds used
the shared target, test profile `opt-level=1`, debug/incremental off, two jobs,
and the same published v0.8.16 contract-0.28.0 parser development override.
Parser archive SHA-256:
`8ac1f708efdfdbf6c85a65aebd34a368de41e798d9441a2785abad011cec038d`.
This is not release-package qualification or a comparison with a published
release build.

```sh
cargo build --offline --locked --profile test -p alopex-embedded --example begin_resources
python3 scripts/performance/run_native_begin_resources.py \
  --binary /absolute/shared-target/debug/examples/begin_resources \
  --product-source <exact-checkout-commit> --output /path/to/new-output
```

The new condition is fixed at 1,000 rows with two 2D vector columns and indexes
0/1/2. Existing native 0/100/1,000-row, memory/disk and RO/RW evidence is reused,
not rerun for freshness. Seed each fixture in a separate process. Each public
borrowed/owned RW noop or query reader, plus autocommit query, records one
first-after-open and five warm samples. Warm all requested indexes before sample
1; both fit in an 8 MiB/two-entry cache, and indexed warm queries must increase
cache hits. Zero-index queries use exact scanning. Fresh reader does not mean
OS-cold page cache.

Each ownership route/index combination adds two fresh noop readers with one
sample, giving three first-begin RSS deltas. Main matrix: **60 processes
(6 seed + 54 reader), 204 reader samples**. Every query verifies the nearest row;
every process validates all rows and both vectors. All fixtures are removed
before the next configuration.

Predeclared timing criterion: warm noop begin median reduction at least 10%
and difference above `3 * max(MAD_before, MAD_after)`. Memory criterion: the
median of three first-begin retained RSS deltas decreases by more than the same
three-MAD bound. All snapshots and file scans occur outside timed intervals.
Compare matching workers only; observer overhead differs from the earlier bare
native/Python timing workers.

## Native begin timing

Warm medians in milliseconds. All six conditions meet the timing criterion.

| Indexes | API | Before | After | Reduction |
| ---: | --- | ---: | ---: | ---: |
| 0 | borrowed | 0.580542 | 0.003875 | 99.33% |
| 0 | owned | 4.922917 | 0.003292 | 99.93% |
| 1 | borrowed | 0.641333 | 0.002541 | 99.60% |
| 1 | owned | 4.961208 | 0.004458 | 99.91% |
| 2 | borrowed | 0.660500 | 0.003250 | 99.51% |
| 2 | owned | 5.006208 | 0.005541 | 99.89% |

## First-begin retained RSS

`ps` RSS is converted from KiB to bytes. The table reports KiB deltas
`RSS_after_begin - RSS_before`, each the median of three fresh readers.
All six conditions meet the predeclared memory criterion. This is retained
process RSS at phase boundaries, not allocation counts, begin-internal peak or
a universal memory bound. Zero MAD can reflect native page granularity.

| Indexes | API | Before delta | After delta | Reduction | Noise bound |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | borrowed | 1312 | 208 | 1104 | 48 |
| 0 | owned | 1024 | 256 | 768 | 144 |
| 1 | borrowed | 1376 | 224 | 1152 | 0 |
| 1 | owned | 1056 | 288 | 768 | 48 |
| 2 | borrowed | 1376 | 224 | 1152 | 0 |
| 2 | owned | 1024 | 288 | 736 | 48 |

This is a 71.88–84.15% reduction in the measured RSS increment. No measured
begin/query/rollback phase changed fixture allocated or logical file bytes.
Maximum parent-sampled RSS in the main runs was 49,561,600 bytes before and
49,856,512 after (includes seed processes). Sampling every 100 ms can miss peaks;
it is a safety observation and is not the phase-memory acceptance metric.

## Query/autocommit and observed timing flag

Query and rollback have separate raw intervals. Autocommit records public
`execute_sql` total only. No query/autocommit improvement is claimed from the
begin result. Main warm medians in milliseconds:

| Indexes | Borrowed query before/after | Owned query before/after | Autocommit before/after |
| ---: | ---: | ---: | ---: |
| 0 | 0.491083 / 0.463916 | 2.598917 / 2.636125 | 0.445875 / 0.531875 |
| 1 | 0.075250 / 0.078792 | 0.098958 / 0.108917 | 0.066542 / 0.080083 |
| 2 | 0.109500 / 0.081916 | 0.117500 / 0.105083 | 0.064708 / 0.079125 |

The initial zero-index autocommit median increased 19.29% (0.445875 →
0.531875 ms), with an 0.086000 ms difference above the 0.023499 ms three-MAD
bound. It was preserved rather than hidden by the begin improvement.

A separate plan fixed **before/after/after/before/before/after/after/before**
order for that condition only: four independent reader processes per source,
each one first plus five warm samples, **16 processes / 48 reader samples**.
Its decision uses the median/MAD of four process warm medians and flags an
increase only if greater than 10% and three times the larger MAD. The fixed
product binary was retained; the serial build had overwritten the baseline
executable, so only the baseline was rebuilt. Its SHA exactly matched the first
baseline executable. No measurement code or production source changed.

The balanced replication gave **0.480959 → 0.483104 ms (+0.45%)**. Difference
0.002146 ms was below the 0.012875 ms noise bound. The large initial flag did not
repeat. Its original data remains in the report and raw logs; this does not
prove a universal absence of query regressions or identify the source of the
initial variation. No additional repeats followed this fixed check.

## Requirement-by-requirement development evidence

| #684 requirement | Source-bound development evidence | Candidate status |
| --- | --- | --- |
| Reproducible public Python and corresponding Rust begin/query/rollback measurements | `begin_profile`, `begin_resources`, Python workers; phase intervals and exact complete-state validation | Integration/CI pending |
| Row/index/memory/disk relationships and profiled cause | [Baseline](transaction-begin-baseline.md): full SQL journal capture in begin profile; row scaling. [Python index/resources](transaction-begin-hnsw-resources.md) and this native matrix: index0/1/2, phase RSS and disk | Integration/CI pending |
| Minimum cause-directed fix and before/after time/memory under fixed criteria | `90b8b0b5` write-local before-images; native/Python begin and RSS criteria met; [first-write comparison](transaction-first-write-comparison.md) records separate tradeoff checks | Integration/CI pending |
| Snapshot, read-your-writes, conflicts, rollback, overlapping updates, reopen regressions | [LSM fix](transaction-begin-lsm-fix.md): focused Rust20, 24-case tiny property, mutation10 caught; [Python](transaction-begin-python-comparison.md): three public tests on both source-matched wheels | Integration/CI pending |
| Do not substitute HNSW warm-query gain for transaction gain | Noop begin is primary, query/autocommit and initial flags reported separately; no general query/whole-transaction claim | Integration/CI pending |

The seven production/regression-test source hashes from the earlier Rust
verification were checked against the current tree and match. Production source
diff from `90b8b0b5` is empty. The earlier focused tests, property, coupling and
mutation evidence are therefore retained rather than rerun solely for freshness.
This diagnostic-only change adds no unsafe path; Miri/Kani are not applicable.
The new example built on both sources, ran all ownership/query routes, passed
`cargo fmt --all -- --check` and focused Clippy with `-D warnings`:

```sh
cargo clippy --offline --locked --profile test -p alopex-embedded --example begin_resources -- -D warnings
```

## Resource cleanup and handoff

All **76 processes / 252 reader samples** across main and fixed replication
passed value/hash checks; all fixtures were reclaimed. Guards remained 90
seconds per process, RSS 2 GiB, runtime filesystem growth 2 GB, 16 GiB free-space
reserve and host stop 40 GB. Builds were serialized with a declared additional
3 GB budget and no build ran during timing.

Host inventory: 16,063,811,584 bytes before setup, maximum recorded
16,190,201,856, and 16,065,028,096 after cleanup. Nine owned generated example/
copy/fingerprint files and parser archive/extraction were removed. Shared Cargo
dependencies remain for active release work. No new worktree, venv or wheel was
created. The original branch is restored and pre-existing `.codex/worktrees/`
remains untouched.

Evidence directory `/private/tmp/alopex-684-native-grid` holds plans, logs,
source/binary/harness hashes, comparison summaries, exact diagnostic sources,
source reconciliation and cleanup record. Example SHA-256:
`46b2d10fb79e41d5c0b5c2bfb5352d8df0a0f7091152c8ca8744c7459faa0b4d`.
Runner SHA-256:
`6f995c79993e6431d7a66c6fc1bc0b270330554ad77c614db7cf1e059d0f3cf4`.

The remaining #684 gate is required CI plus integration of #769 followed by
#770 into the candidate and verification of that composition. These development
results do not close #684, qualify release packages, or complete v0.8.17.
