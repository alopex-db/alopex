# CSV lazy expressions and numeric Parquet guards (#712)

## Contract and identity

This follows #783's batch-local concat string buffer reuse with the accepted file-based cases. CSV runs all twelve expressions; Parquet runs the eleven numeric/logical expressions. File scan, plan construction, reading and full collect/all-batch materialization are timed. Python conversion, complete result validation, result and source/projection-plan disposal are outside the timer. Every batch is consumed; no first-batch latency is substituted. Batch streams are explicitly closed inside the operation. Known Parquet string type failures remain excluded, and the same-name API capability gaps are not treated as performance failures.

- Parent #783: `c9bb7c9682d879b5b8e5ae6bd615aa754d236f9b`.
- Before native build: `8d404614f3989ed8dc3732b657d5ac746e70977b` (before the concat implementation, with the regression tests already present).
- After native/harness build: `ca7a27126436a6acadded468d0081d99cdcdbc00`; product Cargo files/crates are unchanged from implementation `057197ad7193d0e2414080620ca1fb4cb638651f`.
- Two serialized native builds used one shared target, test profile, incremental/debug disabled and jobs 2. Before was built from its detached fixed commit, then the branch was restored for after. No additional worktree was created. Independent copies prevented the second build from overwriting the first native package.
- All non-native Python package bytes match between builds. Every worker verifies the native, driver, workload, input and full-result identities. The companion JSON contains these identities, raw samples, profiles, fixture-writer hashes, commands and cleanup evidence.

The byte-identical original `polars_entrypoints_v0816.py` from docs commit `017e52996ad4130c6a52064153c4f8d93d08e123` is retained with its original version-gated runner. The current-source adapter does not impersonate v0.8.16. Its API call sequence was checked against the pinned runner with fake operations. Six small new harness tests also check complete late-batch consumption, timer boundaries, source/projection-plan disposal, full types/NULL/order/value rejection, excluded cases, and CI rejection before native import/workdir creation.

The original fixture writer ran once with Polars / polars-runtime-32 1.43.2 and one worker. Both 34,250-row files exactly match the original confirmed report's SHA256:

- CSV: `eec24557dcbdf71821b2e7afad1a7083dfb3256b71b57adf2e7ec387e1fd20ff` (530,326 bytes).
- Parquet: `2552dc14bcfebe5a3fa177e64154e1b80dbabfa386bc344dbe751d1b5f7e7a79` (78,699 bytes).

Three-row fixtures are additional diagnostics, not replacements for the original case. Both builds use the same immutable files. Fixture-only Polars was removed before native measurements; it was not a measured comparison product.

## Manual results

Each side: 3 / 34,250 rows × four cases × three trials, with three warmups and 21 samples. Total: **48 timed cells, 1,008 measurements, 144 warmups**, all successful. Trial order was before/after, after/before, before/after. Four separate CSV profiles also completed. Every timed warmup/sample verifies all values, exact Python scalar types, NULLs and column/row order against the independent original oracle; profile runs verify preflight and the last output only.

Files were warmed before timing, RAYON/TOKIO/OMP/OPENBLAS requested one worker, and macOS CPU affinity was not pinned. This is a current-source internal comparison, not published v0.8.16/Polars or final candidate qualification. The original Linux/Polars ratios must not be multiplied by these local changes to infer a new reference ratio.

Times are milliseconds and medians of three trial medians. Before measuring, the diagnostic band was fixed at max(5% of before median, three times pooled before MAD). All increases and paired trial ratios are retained. Being within the band does not prove noise.

| Rows / case | Before ms | After ms | Change | Diagnostic band ms |
|---|---:|---:|---:|---:|
| 3-csv-batches | 0.106667 | 0.108167 | +1.41% | 0.020247 |
| 3-csv-collect | 0.077250 | 0.074917 | -3.02% | 0.013500 |
| 3-parquet-numeric-batches | 0.086875 | 0.085666 | -1.39% | 0.007122 |
| 3-parquet-numeric-collect | 0.062667 | 0.062041 | -1.00% | 0.003999 |
| 34250-csv-batches | 5.230041 | 4.191125 | -19.86% | 0.261502 |
| 34250-csv-collect | 4.231041 | 3.267083 | -22.78% | 0.211552 |
| 34250-parquet-numeric-batches | 1.151334 | 1.155333 | +0.35% | 0.072873 |
| 34250-parquet-numeric-collect | 1.014041 | 1.085916 | +7.09% | 0.178749 |

At 34,250 rows, CSV collect decreased 22.78% (throughput ×1.295) and all-batches decreased 19.86% (×1.248). The paired after/before time ratios were 0.766/0.783/0.769 for collect and 0.817/0.801/0.813 for batches.

Numeric Parquet collect increased **7.09%** in the median-of-medians comparison. Its paired trial ratios were 1.071/1.017/0.996; its 0.071875 ms increase is inside the predeclared 0.178749 ms band. Numeric batches increased 0.35%. These increases are not hidden, attributed to noise, or treated as unconditional proof of no regression. The final fixed-candidate/reference acceptance retains this uncertainty. No increase exceeded the predeclared band, so no freshness-only repetition was added.

## Profiles and resources

After CSV collect profiling completed 1,196 operations, with 3.971 cumulative operation seconds and 3.938 seconds in native collect. All-batches profiling completed 904 operations, with 3.988 cumulative operation seconds; native samples reach `PyDataFrameStream.__next__`, CSV decode/flush, primitive conversion and expression projection. The 0.096 seconds recorded at `collect_batches` creation is not the full stream-consumption cost. Iteration and native work remain within the enclosing operation. Inclusive sampled stacks are not exclusive percentages and are not summed.

Operation CPU medians and process-lifetime RSS high-water (bytes) for the larger cells:

| Side / case | Operation CPU ms | Process peak RSS bytes |
|---|---:|---:|
| before / 34250-csv-batches | 5.202000 | 103,022,592 |
| before / 34250-csv-collect | 4.227000 | 108,527,616 |
| before / 34250-parquet-numeric-batches | 1.146000 | 98,353,152 |
| before / 34250-parquet-numeric-collect | 1.047000 | 99,942,400 |
| after / 34250-csv-batches | 4.197000 | 102,957,056 |
| after / 34250-csv-collect | 3.260000 | 108,429,312 |
| after / 34250-parquet-numeric-batches | 1.188000 | 98,074,624 |
| after / 34250-parquet-numeric-collect | 1.073000 | 100,876,288 |

The worker now reports `getrusage` high-water with macOS byte units, complementing periodic supervisory RSS samples that can miss short processes. It includes fixture hashing, oracle/setup and library loading, so it is not engine-only memory. CPU/block observations surround the timed operation and include observation overhead; raw-output IO and validation are outside those deltas. Raw resource fields are preserved without claiming zero whole-process IO or a memory advantage.

## Development checks, cleanup and next action

`python3 -B -m unittest discover -s scripts/performance/tests -p 'test_*_profile.py'`: **29 passed**, including six new tiny fake-operation tests. The initial run had one test expecting AssertionError where the explicit column-order guard raises RuntimeError; the test expectation was corrected and that failed log is preserved. A separate source/plan disposal test was added before native measurement. Diff checks passed. No Rust product behavior changed in this PR: the focused Rust, mutation and Clippy evidence belongs to #783; no new Rust mutation/coupling/Miri/Kani run was needed. Ordinary CI runs only the tiny harness tests; the measurement driver rejects CI.

All 52 worker PIDs were absent and all per-case work directories removed. Both native package copies, generated CSV/Parquet inputs, parser/archive and native target exports were deleted after hashes/results were captured. Fixture-writer dependencies were removed earlier. Shared Cargo dependencies and compact evidence remain. Host observed peak: 19,249,774,592 bytes; after cleanup: 19,048,108,032 bytes, below the 40 GB stop / 50 GB ceiling.

Next owner/action: Codex will profile the accepted SQL streaming case, preserving both the original fully consumed API comparison and an equal-result-representation diagnosis. The plain CSV streaming case, remaining SQL/KV/HNSW guards and published/reference comparison remain open. Candidate inclusion/CI, #712 stage acceptance, release qualification/publication and post-release main merge remain under #690. This report proves this bounded development comparison only.
