# DataFrame phase baseline (#712)

## Contract and source

This manual diagnosis separates construction, twelve-expression computation on an existing frame, Python conversion of an existing result, and the original complete materialization contract. It changes the diagnostic harness only; product Rust/Python code is unchanged from #781. The next implementation target is allocation inside expression evaluation, with row-wise string assembly as a hypothesis to test, not an established exclusive cause.

- Parent: #781 `e26304a0060bfea1ef6ac9c67eaa3434de19d0b6`.
- Native build source: `e1c210fb0f113e675122477599f2b157e40901ff`; product crates identical to `751b734701e6fe0a4092c53a11025c5f66bf47ba`.
- Measured harness: `4dab0a88798d2d9b636adb1f53c55347b01831cb`.
- Pinned workload: `alopex-db/docs` commit `017e52996ad4130c6a52064153c4f8d93d08e123`, `scripts/performance/dataframe_phases_v0816.py`, SHA256 `d0303975720a43755dae5628338b0a0d6bb099d81d0b43d1b8fc8688259af0e2`.
- Its vendored copy is byte-identical, including the original version gate. The current-source adapter overrides API initialization only; it does not impersonate a published 0.8.16 package. Native/package/harness/input/result hashes and raw events are retained in the companion JSON.
- One serialized extension build: `env PYO3_BUILD_EXTENSION_MODULE=1 cargo build --offline --locked --profile test -p alopex-py`, shared target, incremental/debug disabled, jobs 2. The hash-verified 0.8.16 contract 0.28.0 parser override is development evidence, not release-asset qualification.

## Manual method and results

Three trials × two row counts (3 / 34,250) × four phases = 24 timed cells, with three warmups and 21 measured operations per cell: 504 measurements and 72 warmups. Each cell has a fully validated preflight. Every warmup/sample validates all values, exact Python types, NULL propagation, row and column order, and the original snapshot (input, next_id, explode, concat, result, height, width and first series name) outside its timer. Result disposal also happens outside the next timer. Input labels are NULL every tenth row; items have two strings per row.

RAYON/TOKIO/OMP/OPENBLAS thread environment variables were all 1. macOS CPU affinity was not pinned. Results are a current-source internal baseline, not the original Linux environment, a published v0.8.16 comparison, a Polars ratio or final #712 acceptance. Four additional profiles ran separately from timing, with preflight and last-output validation only; their intermediate outputs are deliberately not fully validated inside the sampling window.

Times are milliseconds. Median is the median of three trial medians; MAD is the pooled absolute deviation. The phases are not additive: complete includes the original items-list regeneration and extra snapshot/explode/concat operations.

| Rows / phase | Median ms | Pooled MAD ms | Operation CPU median ms |
|---|---:|---:|---:|
| 3-complete | 0.063167 | 0.000875 | 0.064000 |
| 3-compute | 0.043250 | 0.000541 | 0.044000 |
| 3-construct | 0.002250 | 0.000291 | 0.003000 |
| 3-convert | 0.002625 | 0.000125 | 0.003000 |
| 34250-complete | 29.172708 | 0.388250 | 29.166000 |
| 34250-compute | 2.383250 | 0.053917 | 2.381000 |
| 34250-construct | 4.329042 | 0.096417 | 4.364000 |
| 34250-convert | 2.641917 | 0.074792 | 2.648000 |

All 28 timed/profile cells passed. Operation input/output block counter deltas were zero; these describe the warm in-memory operation window, including instrumentation overhead, and exclude validation, raw logging and library loading. They do not establish zero whole-process IO. Periodic RSS sampling missed the working interval of some short 3-row processes; their small sampled RSS values are lower bounds, not credible process peak measurements or evidence of a memory advantage. The unmodified sampled values remain in the JSON.

## Profile interpretation

- Construct profile: 891 operations, approximately 3.993 seconds in frame construction.
- Compute profile: 1,559 operations, 3.988 seconds cumulative in the operation, including 3.948 seconds in native `LazyFrame.collect`. Python expression construction is about 0.026 seconds.
- Convert profile: 1,305 operations, approximately 3.451 seconds in `DataFrame.to_dict`.
- Complete profile: 115 operations, 3.651 seconds cumulative; its original snapshot accounts for 2.627 seconds, including 1.039 seconds across `to_dict` and 0.624 seconds in `explode`. These inclusive times must not be summed as independent phase costs.
- Native compute sampling reaches `BatchExprEvaluator::evaluate`, `evaluate_uncached`, vector growth and `realloc`. Sampling may also include startup sleep/disposal; inclusive stack counts are not exclusive CPU percentages. Inlined offsets do not conclusively identify the allocating expression.
- Source inspection found a fresh per-row `String` in `eval_concat_str`. Reusing a row buffer is the next bounded hypothesis, preserving NULL Propagate/Ignore/Replace, separators, Unicode and empty-input behavior. Existing deterministic expression reuse is already implemented and is not duplicated here.

## Verification and continuation

`python3 -B -m unittest discover -s scripts/performance/tests -p 'test_*_profile.py'`: 23 tests passed, including six new tiny fake-operation tests covering all phase boundaries, full typed/NULL/ordered oracle rejection, result disposal, unknown phases and CI rejection before native import/workdir creation. The six new tests passed again after adding operation resource observations; this is not 29 unique tests. Diff checks passed. Ordinary CI runs these tiny harness tests only; the native/performance runner rejects CI and requires explicit manual invocation.

Target: performance harness and pinned workload only. Mutation check: not run (no Rust behavior change). Survivors: not applicable. Strengthening: six focused harness tests. Coupling, proptest, Kani and Miri: not run for this harness-only change. Candidate composition/CI and published/reference measurements remain pending.

All 28 owned worker PIDs were absent on cleanup, and all work directories were removed. Parser archive/extraction, copied Python native package and four owned target exports were deleted after hash/evidence capture. Shared Cargo dependencies and compact logs remain. Host peak observed: 18,919,825,408 bytes; post-cleanup: 18,831,933,440 bytes, below the 40 GB stop and 50 GB ceiling.

Next owner/action: Codex will test the row-buffer hypothesis in `eval_concat_str` with minimal semantic regression cases and a bounded before/after comparison. CSV lazy expressions, Parquet guards, stream/representation comparisons, remaining SQL/vector/CSV cases and final composed-candidate/reference acceptance remain in #712; this baseline closes none of those requirements. Candidate integration, release qualification, publication and main merge remain tracked by #690.
