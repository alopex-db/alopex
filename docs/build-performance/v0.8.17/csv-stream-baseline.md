# Plain CSV full-consumption diagnosis (#712)

## Original contract and source identity

This follows #786 and diagnoses the adopted plain CSV streaming case without substituting first-batch latency for total consumption. There is no Rust product change in this PR.

- Native/harness source: `a004721ef0aba151049634edb78e65bf0431fe2d`. Product Cargo files/crates are identical to `a5d4e1bc7763142fda2b166401d89a29f5f2caeb` after the lazy comparison-error change.
- Original source `d9ff8783b8d18a63d167204e55902ea6cd90dcd1` plus the run-20261021 fairness patch SHA256 `9b2dde147fce3a1ce820c65fb91627595d75832820cce34526f7abb575abd68b` yields corrected measurement file SHA256 `7ba8a8e9da92987d2f0dbdc6ef3d98f2a12e5c4dc087651ae279a1dd6fbf1800`.
- `_streaming` and `_write_csv` are extracted unchanged into `scripts/performance/workloads/csv_streaming_v0816.py`, retained as a source reference. AST tests compare the original consumer bodies with the new driver. Its extracted-file hash is in the JSON plan.
- The original writer reconstructs 34,250 rows with columns id/label, and NULL label every tenth row. The CSV is **530,326 bytes**, SHA256 `eec24557dcbdf71821b2e7afad1a7083dfb3256b71b57adf2e7ec387e1fd20ff`, exactly matching the original environment manifest. The complete-result digest `98afeb725a4f51925adec4ad654a07088175c9652d7ec7c29d8a19bf475f5707` matches the original report. A three-row control adds a 32-byte fixture, not a substitute for the adopted case.
- Reference: Polars and polars-runtime-32 **1.43.2**, matching the original environment. All installed package files and runtime libraries are hashed. The runtime reports thread_pool_size=1 under POLARS_MAX_THREADS=1.

One shared-target native build used `env PYO3_BUILD_EXTENSION_MODULE=1 cargo build --offline --locked --profile test -p alopex-py`, test opt-level 1, incremental/debug 0 and jobs 2. The parser archive SHA256 `8ac1f708efdfdbf6c85a65aebd34a368de41e798d9441a2785abad011cec038d` and library identities were verified; this is a development override, not release-asset qualification. Native and Python files, imported helpers, fixture and result hashes are verified for the measured workers.

## Timing and correctness boundaries

Each worker retains the original sequence: separate plan observations, separate fully materialized collect observations, then full `collect_batches(chunk_size=8192)` consumption. Each phase has three warmups and 21 measured samples. The plan and collect temporaries are disposed before the end clock, matching the original `_samples` boundary. Stream consumer bodies match the corrected original AST. Alopex's original total timestamp precedes context exit; outside-total and CPU/IO observations separately include close/return overhead. Polars's original consumer stays unchanged and fully exhausts its iterator.

An additional independent typed oracle runs on **both full collect and full stream results before and after each case**, outside all timers. It verifies every row, exact int/string/None types, column order, physical input order, lengths and duplicates/missing values. Every timed stream sample checks the full row count. No expression projection, filter or alternate fast path is inserted. Validation streams use close when available; Polars1.43.2 CollectBatches has no close hook and is fully exhausted. The before/after input file hash must match.

All data are warmed before timing; RAYON/TOKIO/OMP/OPENBLAS request one worker, and the Polars pool count is checked. macOS CPU affinity is not pinned. Native test-opt1 and distributed reference wheels are **diagnostic configurations, not matched release builds**. These results cannot be compared with the original Linux 2.301x to infer a version regression, or used as final release/reference qualification.

## Measurements

Two sizes × two engines × three trials: **12 cells, 252 stream measurements, 36 warmups**, all passed. Engine order reverses in trial 2. There are separately 252 measured plan calls and 252 measured collect calls, each with 36 warmups; two profile workers add 96 preparation plan/collect calls. The two profiles are excluded from timing medians.

Times are milliseconds, medians of three trial medians. Every raw observation and paired ratio is retained in the JSON.

| Input rows | Alopex stream ms | Polars stream ms | Stream time ratio | Alopex collect ms | Polars collect ms |
|---|---:|---:|---:|---:|---:|
| 3 | 0.058500 | 0.161917 | 0.361 | 0.030583 | 0.125375 |
| 34250 | 2.595333 | 0.963166 | 2.695 | 0.979333 | 0.948583 |

For 34,250 rows, the full-stream ratio is **2.695x**, with paired trial ratios 2.626/2.695/2.766. The collect times are 0.979333/0.948583ms, ratio 1.032. This identifies a cost concentrated in the streaming path; it does not establish release parity for collect. The small control favors Alopex and remains visible.

| Case | Plan ms | First batch ms | Remaining ms | First-batch rows | Process peak RSS bytes |
|---|---:|---:|---:|---|---:|
| 3-alopex | 0.000292 | 0.057833 | 0.001000 | [3] | 40,763,392 |
| 3-polars | 0.004750 | 0.151250 | 0.010792 | [3] | 78,938,112 |
| 34250-alopex | 0.000291 | 0.628000 | 1.971459 | [8192] | 49,889,280 |
| 34250-polars | 0.004333 | 0.905916 | 0.061125 | [8192] | 89,194,496 |

Both larger cases return 8,192 rows in the first batch. Alopex's earlier first batch (0.628ms versus0.906ms) does not offset its longer full consumption (2.595ms versus0.963ms). Engines can perform different amounts of work before emitting the first batch; first latency is not used to declare the full-stream gap resolved. Independent medians need not add exactly to median total. Plan construction APIs also need not perform identical work, so no plan-time product ratio is claimed.

Process RSS is the macOS getrusage high-water in bytes, including setup, runtime loading, hashing and full-value validation. It is not engine-only memory, and no memory advantage is claimed. Raw CPU/IO observations include close and observation overhead, while validation/output are outside those deltas.

## Profile evidence and next target

Alopex's separate profile completed 1,441 iterations: cProfile records 3.978 consumer seconds,3.006 in remaining-batch consumption, 0.833 in the first next call and 0.135 in collect_batches creation. Polars completed 2,943 iterations. Profiles include instrumentation/disposal overhead and are not substituted for the timing samples.

Within one Alopex remaining-batch native stack, 1,174 inclusive samples reach the streaming source. A framing branch has 757 samples, including 435 at `BoundedCsvFramer::next_frame` and 302 in a path through `BufReader::read`; another 403-sample branch reaches Arrow decoding. These are inclusive hierarchical observations, not independent CPU percentages or values to add across nesting levels.

Source inspection confirms `next_frame` calls `read_byte` for every input byte, and `read_byte` invokes `BufReader::read` with a one-byte buffer. Quote tracking and capacity checks also run per byte. This is a concrete investigation target supported by the profile. It does not prove that all remaining cost is framing or that a proposed implementation is already faster.

Next owner/action: Codex will consume buffered slices in the bounded CSV framer, preserving physical order, quotes/escaped quotes/multiline records, header handling, EOF and IO-error behavior, byte caps, over-limit tail draining, and budget ownership. Small fragmented-reader/model tests and a fixed before/after comparison will verify the change. Arrow decode remains a separate observed cost.

## Development checks and cleanup

`python3 -B -m unittest discover -s scripts/performance/tests -p 'test_*_profile.py'`: **43 passed**, including seven new tiny fake-batch tests. Tests cover exact original consumer ASTs, late batches/counting without conversion, Alopex context-close timing/error cleanup, full typed/NULL/order validation, disposal inside plan timing, and CI rejection before native import/work creation. Ordinary CI runs these small tests; both the manual driver and supervisor reject CI. Diff checks passed. There is no product Rust change, so mutation/coupling/Miri/Kani were not run for this harness-only stage.

The pinned reference install and single native build succeeded. Build SDK-observation and development-parser warnings are retained. All 14 worker PIDs were absent and all case work directories removed. Reference packages, generated input CSVs, native package, parser/archive and four native target exports were deleted after source-bound evidence capture. Shared Cargo dependencies and compact evidence remain. Host observed peak: 19,427,274,752 bytes; after cleanup: 19,093,962,752 bytes, below the 40 GB stop / 50 GB ceiling. Each worker used the 180-second / 2 GiB RSS limit and the task stayed inside its 2 GB additional bound.

The companion JSON stores all raw values, identities, profiles, reproduction commands, logs and cleanup receipts. Candidate CI/integration, #712's remaining adopted scenarios and fixed release/reference guards, release/package qualification/publication, postrelease checks and main merge remain under #690. This is development diagnostic evidence only.
