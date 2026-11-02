# Reuse the concat_str row buffer (#712)

## Behavior and identity

`BatchExprEvaluator::eval_concat_str` created and grew a temporary String for every row. It now clears and reuses one String within the current batch, borrowing it when appending to Arrow's copying builder. NULL Propagate/Ignore/Replace, separators, Unicode, empty strings, empty batches and input type rejection retain their existing semantics. Capacity may retain the largest assembled row until this batch ends; it is not a persistent cache and is released on return. No disk format, public API or cross-module contract changes.

- Parent #782: `1bba6031119c9a9fa509575e03638311d6cc8124`.
- Test-only source before implementation: `8d404614`.
- Implementation/native source: `057197ad7193d0e2414080620ca1fb4cb638651f`.
- Retained baseline native source: `e1c210fb0f113e675122477599f2b157e40901ff`; baseline harness `4dab0a88798d2d9b636adb1f53c55347b01831cb`.
- All measured harness and non-native Python package bytes match #782. The original docs workload remains byte-identical. Source, package, native, parser, input and full-result hashes are in the companion JSON; its baseline report hash pins the earlier raw evidence.

## Focused verification

`cargo test --offline --locked -p alopex-dataframe --lib physical::expr_eval::tests` passed all six tests before and after the implementation. Two new tests use at most seven rows and three input columns: long-to-short transitions, NULL in middle/last/all inputs, empty replacement/separator/string/batch, and Unicode. Existing tests also cover non-string rejection and batch-local deterministic-expression reuse.

An explicit negative control removed `output.clear()`: the cross-row test failed with prior-row text leaking into later outputs. The original source was restored. Four prelisted cargo-mutants 27.1.0 mutations (separator condition, included count, NULL disjunction, zero-included condition) were all caught, with zero survivors, timeouts or unviable cases. The first invocation was rejected before execution because `--jobs` is incompatible with `--in-place`; the corrected in-place run was serialized. Both invocation logs are retained separately.

Scoped Clippy `cargo clippy --offline --locked -p alopex-dataframe --lib --tests -- -D warnings`, fmt and diff checks passed. One optimized test-profile Python extension was built with incremental/debug disabled, jobs 2 and the shared target. The explicit hash-verified parser override is development evidence only. The build emitted SDK environment-discovery warnings and the expected local-parser identity limitation; it completed successfully.

Target: `alopex-dataframe/src/physical/expr_eval.rs::eval_concat_str`. Mutation check: four selected mutants caught plus the explicit missing-clear regression. Survivors: none. Strengthening: two minimal semantic tests. Coupling was not run because the change is one private function in one file with no new module edge. No unsafe or bounded-index logic was introduced; Miri/Kani and additional property testing were not run. No load or performance test was added to ordinary CI.

## Manual comparison

Unchanged #782 four-phase workload: 3 / 34,250 rows, twelve expressions, one requested RAYON/TOKIO/OMP/OPENBLAS worker, three trials × eight cells, three warmups and 21 measured operations per cell (504 measurements, 72 warmups). Every warmup/sample verifies the full typed/NULL/ordered original snapshot outside timing; setup and result disposal retain the original phase boundaries. All 24 cells passed. Two separate four-second compute/complete profiles passed with preflight and final-result validation.

Values are milliseconds and medians of three trial medians. Before measurements are retained evidence, not rebuilt or temporally interleaved AB/BA. Before execution, the diagnostic band was fixed at max(5% of baseline median, three times pooled baseline MAD). Every increase remains visible. Being inside that band does not establish noise or final release acceptance.

| Rows / operation | Before ms | After ms | Change | Diagnostic band ms |
|---|---:|---:|---:|---:|
| 3-complete | 0.063167 | 0.064000 | +1.32% | 0.003158 |
| 3-compute | 0.043250 | 0.043916 | +1.54% | 0.002162 |
| 3-construct | 0.002250 | 0.002375 | +5.56% | 0.000873 |
| 3-convert | 0.002625 | 0.002583 | -1.60% | 0.000375 |
| 34250-complete | 29.172708 | 27.868917 | -4.47% | 1.458635 |
| 34250-compute | 2.383250 | 1.465041 | -38.53% | 0.161751 |
| 34250-construct | 4.329042 | 4.419250 | +2.08% | 0.289251 |
| 34250-convert | 2.641917 | 2.665250 | +0.88% | 0.224376 |

The 34,250-row compute reduction is approximately 38.53%. Complete materialization is 4.47% lower in these timed trials; that smaller change is within the diagnostic band and is not a significance claim. All observed increases are within the predeclared band, including the 3-row construction increase of 0.125 microseconds (+5.56%). Construction and conversion product paths were unchanged. MacOS affinity was not pinned, and this is not a published 0.8.16/Polars comparison or the original Linux baseline.

The compute profile completed 2,922 operations: 3.973 seconds cumulative operation time, including 3.911 seconds in native collect. Complete profiling completed 105 operations; conversion, explode and Python materialization remain visible. Profiling has different iteration counts and overhead; it is not an additional timing trial or an exclusive allocation-cost proof. Native samples retain their inclusive stack counts without summing overlapping frames. The unchanged cProfile/native profile text is included in the JSON.

Operation CPU at 34,250 rows changed from about 2.381 to 1.447 ms for compute. Operation block counters remained zero, without implying zero whole-process IO. Periodic RSS samples can miss short workers and are not process high-water measurements. No memory reduction is claimed (34,250-row sampled compute RSS is about 161 MB versus about 160 MB before).

## Cleanup and continuation

All 26 owned worker PIDs were absent and all fixture directories removed. Copied native package, parser/archive, four native target exports and the owned DataFrame test executable were removed after source/evidence capture. Shared dependencies and compact logs remain. Host observed peak: 19,150,692,352 bytes; after cleanup: 19,041,726,464 bytes, below the 40 GB stop / 50 GB ceiling.

Next owner/action: Codex will verify the accepted CSV lazy twelve-expression workload and numeric Parquet guards against fixed before/after sources. Candidate integration/CI, remaining #712 stream/representation/SQL/vector/CSV requirements, published/reference acceptance, release qualification and publication, then main merge remain open under #690. This report supports development handoff of this focused change only.
