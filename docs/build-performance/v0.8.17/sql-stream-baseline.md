# SQL streaming result-boundary diagnosis (#712)

## Scope and provenance

This follows #784 and preserves the adopted 34,250-row SQL streaming fixture, query and original full-consumption metric. It adds a separate lane that materializes Python dictionaries on both engines. It establishes where to investigate next; it changes no Rust/product behavior and is not #712 acceptance or release qualification.

- Native and harness source: `1ae5b4b10f3196ad87495318652603a2f9abe459`; Cargo files/crates are byte-identical to `057197ad7193d0e2414080620ca1fb4cb638651f`.
- Original Alopex source: `d9ff8783b8d18a63d167204e55902ea6cd90dcd1`, corrected by the docs run-20261021 harness patch SHA256 `9b2dde147fce3a1ce820c65fb91627595d75832820cce34526f7abb575abd68b`.
- Corrected complete measurement file SHA256 `7ba8a8e9da92987d2f0dbdc6ef3d98f2a12e5c4dc087651ae279a1dd6fbf1800`. Extracted `_sql_streaming` function SHA256 `30f7355ea08824ddcf726047fba7024b738c1866e7e8d0e29142464918eda56c`; retained in `scripts/performance/workloads/sql_streaming_v0816.py`. Unit tests AST-compare both original consumer bodies.
- `_populate_sql` matches the corrected original function. Alopex uses `Database.new()` and INSERT batches of 100; DataFusion uses 8,192-row Arrow input batches in one partition. The fixture has integer id, double value equal to id, and boolean enabled for even ids. Query: `SELECT id, value FROM bench WHERE enabled = true`, with no ORDER BY.
- Reference packages: DataFusion 50.0.0 and PyArrow 26.0.0, matching the original environment. Every package file and native library is hashed in the JSON, along with Alopex's package, parser and all imported harness helpers.

One serialized shared-target build used `cargo build --offline --locked --profile test -p alopex-py`, test opt-level 1, incremental/debug disabled, jobs 2, and Python 3.13.16. DataFusion/PyArrow are distributed wheels. **These are diagnostic build configurations, not matched release builds.** No published v0.8.16 binary was measured. Ratios below must not be compared directly with the original Linux 46.522x to infer a version regression or release parity.

## Two distinct timing contracts

`original` copies the original consumers: Alopex counts all Python dictionary rows; DataFusion counts all Arrow batch rows without `to_pylist`. Both fully consume the result and validate the count for every timed iteration; full typed values are checked before and after each case. The original Alopex total timestamp is captured before context exit. Outer CPU/IO and `outside_original_total_ns` additionally observe that close/return boundary.

`rows` includes complete Python-list/dictionary materialization for both engines. Every warmup and measured output is checked outside the timer for exact column order, int/float types, all values and multiplicity. Because SQL has no ORDER BY, rows are canonicalized by id outside timing; the harness does not add a sort to the query or claim ordered SQL output. Results are disposed outside the next sample timer.

First-output units differ: one row for Alopex, one Arrow batch for original DataFusion, and the first whole batch converted to Python rows for DataFusion `rows`. Therefore first-output times are phase observations, not a like-for-like product latency comparison. The independent medians of first and remaining phases need not sum to the median total.

Three plan warmups plus 21 plan samples precede consumption in each worker, matching the original prewarming order. EXPLAIN JSON and DataFusion logical_plan are different APIs; their raw observations are preserved without a plan-time product ratio. Ready-time hashes/setup are outside all consumption timings.

## Manual measurements

3 / 34,250 input rows × four operations × three trials, three warmups and 21 measured samples per cell: **24 cells, 504 measured consumptions, 72 warmups**, all passed. This is 2 / 17,125 output rows. Engine order reverses in trial 2. There are also 504 separate measured plan calls and 72 plan warmups; the four profile workers have another 96 preparation plan calls. None are counted as consumption samples.

Both engines run on the same macOS host with warm data. RAYON/TOKIO/OMP/OPENBLAS environment requests one worker; DataFusion explicitly uses one target partition. CPU affinity is not pinned. These ratios include different storage/execution designs as well as the stated output boundary; they do not isolate conversion as the only cause.

Times are milliseconds, medians of three trial medians. Paired trial ratios and every raw sample are retained in the companion JSON.

| Input / output rows | Lane | Alopex ms | DataFusion ms | Alopex / DataFusion time |
|---|---|---:|---:|---:|
| 3 / 2 | original | 0.019417 | 0.088292 | 0.220 |
| 3 / 2 | rows | 0.018000 | 0.090250 | 0.199 |
| 34250 / 17125 | original | 14.703334 | 0.134000 | 109.726 |
| 34250 / 17125 | rows | 14.433041 | 1.043458 | 13.832 |

At 34,250 rows, the original boundary is 109.73x and the equal Python-row boundary is 13.83x in these diagnostic builds. The paired original ratios are 114.63 / 104.19 / 109.58; equal-row ratios are 13.98 / 13.84 / 12.86. Python conversion changes the measured gap substantially but does not remove it. The small two-output-row controls favor Alopex and remain visible; they do not replace the adopted larger case.

| 34,250 input case | First output ms | Remaining ms | CPU including close ms | Process peak RSS bytes |
|---|---:|---:|---:|---:|
| alopex-original | 0.026250 | 14.670584 | 14.688000 | 71,696,384 |
| alopex-rows | 0.044875 | 14.383959 | 14.458000 | 72,515,584 |
| datafusion-original | 0.105917 | 0.028500 | 0.135000 | 85,032,960 |
| datafusion-rows | 0.541625 | 0.505917 | 1.047000 | 85,032,960 |

RSS is the macOS getrusage high-water in bytes, including setup, hashing, full-value validation and library loading; it is not engine-only memory. Periodic RSS is also retained. CPU/block deltas surround consumption and close, before result validation/output, and include observation overhead. No whole-process zero-IO or memory advantage is claimed.

## Profiles and next implementation target

Four separate profiles passed: Alopex original 241 iterations, DataFusion original 24,203, Alopex rows 250, DataFusion rows 2,703. These are not timing samples. Profiles validate preflight and the final output; intermediate outputs are not validated within the profiling window. cProfile/native sampling includes disposal and profiling overhead; inclusive stack counts are not exclusive percentages.

Alopex original cProfile records 4.000 seconds in the consumer, 3.991 in full remaining-row consumption and only 0.009 in stream creation. Its native sample reaches `PySqlResultStream::next_native_row`, `OwnedSqlStreamPlan::process_entry`, row decode and Python conversion. One inclusive predicate branch contains 293 process_entry samples, 279 eval_binary_op, 210 compare and 196 formatting samples; these nested counts must not be added together.

Source inspection confirms that both fallible branches in `alopex-sql/src/executor/evaluator/binary_op.rs::compare` use eager `ok_or(ExecutorError::Evaluation(... format!(...)))`. Thus successful boolean/float/other comparable values construct and drop a type-mismatch error string. This is a concrete unnecessary allocation path observed during `enabled = true`, not a claim that it explains the whole gap.

Next owner/action: Codex will make comparison error construction lazy, preserve successful comparison/NULL/NaN/type-error semantics with tiny regression tests, and use this fixed baseline to verify the effect. Cursor decoding, repeated stream metadata conversion and the remaining reference gap stay open; no unmeasured speedup is claimed.

## Checks, resource bounds and cleanup

`python3 -B -m unittest discover -s scripts/performance/tests -p 'test_*_profile.py'`: **36 passed**, including seven new tiny fake-stream tests. They cover original consumer ASTs, timer/context-close boundary, late batches, equal-row materialization, wrong values/types/NULL/multiplicity/columns and CI rejection before native import/work creation. Ordinary CI discovers only these small tests; manual measurements reject CI. Diff checks passed. No Rust product behavior changed, so mutation/coupling/Miri/Kani were not run for this harness-only change.

The first dependency installation failed on sandbox DNS; its failure log is retained. The network-enabled retry installed exactly two pinned packages successfully. The single native build succeeded; SDK observation warnings and the explicit development-parser warning are preserved. Parser archive/library hashes were checked; this development override is not release artifact qualification.

Each case was bounded to 180 seconds and 2 GiB RSS, plus a 2 GB task-growth bound and host 40 GB stop / 50 GB ceiling. All 28 worker PIDs were absent, all case work directories removed, and the task's reference installation, native package, parser/archive and four native target exports deleted after evidence capture. Shared Cargo dependencies and compact source-bound logs remain. Host observed peak: 19,395,358,720 bytes; after cleanup: 19,054,604,288 bytes.

The companion JSON contains full raw samples, package/source identities, profiles, commands, failed/successful verification logs and cleanup receipts. #712's other accepted scenarios, fixed release-build/reference comparisons, candidate integration/acceptance, release artifacts/publication and post-release main merge remain tracked under #690.
