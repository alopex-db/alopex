# Memory scan key-copy reduction (#712)

## Change and evidence boundary

The #789 SQLite composite profile identified the in-memory COUNT scan as the dominant component, with key/value cloning and allocation visible in `MergedScanIter`. This change removes redundant copies within that private iterator. Key comparisons borrow the existing owned peek, the selected key moves into the output, and `record_read` clones a key only when read tracking is enabled. Bounds/prefix/version checks now precede allocation of the next owned key/value peek.

Visible output ownership, ascending order, write-set priority, tombstones, start-version filtering and conflict-read versions are preserved. The existing map read guard, raw-pointer-to-range bridge and lifetimes are unchanged; no new unsafe code is introduced. SQL row decoding/materialization, query plans, persistence formats and public APIs are unchanged. This is a bounded reduction in one observed cost, not a claim that the entire SQLite gap is resolved.

- Parent #789: `d7ccc4a21f97084d694cbd742286f3335f360b67`.
- Before native source: `507eef6c0d8f5cd9eaaf804dbe3ae2fca064056f` (new tests, original product behavior).
- After native source: `59779774a596ffbd9a4d76f668f7b23322bfbbf3`.
- Both builds use the shared target, `env PYO3_BUILD_EXTENSION_MODULE=1 cargo build --offline --locked --profile test -p alopex-py`, test opt1, debug/incremental0 and jobs2. Python/package files except the extension are byte-identical. Native/parser/harness hashes are retained in JSON.
- Fixed v0.8.16 parser archive SHA256 `8ac1f708efdfdbf6c85a65aebd34a368de41e798d9441a2785abad011cec038d`; development override only, not release-asset qualification.

The previous diagnostic native binary was deliberately reclaimed. Rebuilding the before side enables a fixed interleaved comparison for this change; the old cross-time diagnostic samples are not substituted for paired evidence.

## Functional verification

`cargo test --offline --locked -p alopex-core --lib kv::memory::tests` passed **25 tests** before implementation, after implementation and on final source after a Clippy-directed loop rewrite. There are two new tests: a fixed model scenario and a 24-case proptest. Generated fixtures have at most 16 seed entries, 8 later writes and 12 pending writes over 24 distinct two-byte keys and one-byte values. Fixed cases additionally cover empty snapshots, overwrite/delete collisions, later-version invisibility and a pending entry beyond the seed range. These are small functional tests, not load or performance tests.

For prefix, half-open range and scan-from APIs, the independent ordered-map oracle verifies every key/value and the exact read-set key/version map. It checks both read-only and read-write modes, including reads consumed by an overlaid tombstone. The model preserves the current memory store's latest-version behavior: a post-snapshot overwrite hides that key instead of reconstructing a previous value. Existing conflict/WAL/reopen/owned-cursor tests also pass.

Target: `alopex/crates/alopex-core/src/kv/memory.rs::MergedScanIter`.
Mutation check: cargo-mutants 27.1.0, serial in-place on changed lines, shared target, 30-second test/180-second build bounds, same focused test module. Initial source `4a90b12435540e6488e9e575981481ed54254c6a`: **11 mutations, 6 caught, 5 unviable, 0 missed**. The five unviable function replacements fail denied unused-field/unused-variable/dead-code lints; they are not counted as test detections.

Clippy initially rejected `while let data_iter.next()` in favor of a `for ... by_ref()` loop. After that rewrite, final-source focused tests pass and the five `advance_data` mutations were rerun: **4 caught, 1 unviable, 0 missed**. Exact source-text hashes confirm `advance_write`, `record_read` and `Iterator::next` are unchanged from their initial mutation run. Both runs and the original Clippy failure remain in the JSON. Mutation-generated proptest seeds are preserved as evidence and removed from the source tree; no surviving behavior gap was found.

Survivors: none executable passed. Strengthening: bounded reference-model proptest and explicit edge scenarios. Miri is unavailable (no installed nightly/Miri toolchain); the pre-existing unsafe guard bridge was not changed. Kani was not run because this BTreeMap/lock/transaction path is covered by the bounded model rather than a small pure arithmetic state machine. No cargo-coupling run was needed for a local single-file iterator change with unchanged module/crate interfaces. Final focused Clippy, formatting and diff checks pass.

## Fixed manual before/after comparison

The unchanged #789 driver retains separate original and checked composite boundaries. Both use 3 and 34,250 rows, the same in-memory fixture and five-operation sequence. The large result digest remains `bdb0d074606282cb84cbb4445c7bb6c7544057d81fd2ee0dc98f3ea834e3840b`. Every primary sample validates complete typed fixture/rollback state outside the timer. Checked samples also validate actual aggregate/ordered/prepared results; independent FK/CHECK and savepoint preflights run outside timing. Original return/disposal timing is not pooled with checked result-retention timing.

Three trials use before/after, after/before, before/after order. Two sizes × two lanes × two sides × three trials produce **24 timing cells, 504 measured samples and 72 warmups**, all passing. Two separate checked large-case profile workers pass. Separate plan observations total 546 measured calls and 78 warmups across all 26 workers. Checked phase observations total 252 measured cycles and 36 warm cycles, each containing five intervals. No SQLite/reference binary was rebuilt or rerun for this Alopex-only change. This measures the patch's effect; final published-version/reference acceptance remains a separate obligation.

The predeclared diagnostic band is max(5% of before median, 3×pooled before MAD). Times are milliseconds, medians of three trial medians. Raw trial values, MAD, paired ratios and every increase are retained.

| Rows | Lane | Before | After | Change | Band ms |
|---|---|---:|---:|---:|---:|
| 3 | original | 0.164167 | 0.166583 | +1.47% | 0.010872 |
| 3 | checked | 0.167791 | 0.167125 | -0.40% | 0.008390 |
| 34,250 | original | 6.726584 | 5.765458 | -14.29% | 0.669627 |
| 34,250 | checked | 6.675916 | 5.801541 | -13.10% | 0.662748 |

| Separate checked phases | COUNT | Ordered SELECT | Savepoint/rollback | Prepared SELECT | Constraint INSERT/rollback |
|---|---:|---:|---:|---:|---:|
| before 3-alopex-checked | 0.033166 | 0.039958 | 0.039417 | 0.031167 | 0.021666 |
| before 34250-alopex-checked | 6.325875 | 0.119542 | 0.137625 | 0.056083 | 0.030583 |
| after 3-alopex-checked | 0.033459 | 0.039792 | 0.039750 | 0.031292 | 0.021708 |
| after 34250-alopex-checked | 5.507250 | 0.133167 | 0.141584 | 0.057083 | 0.030958 |

The large original composite improves 14.29%, with paired trial ratios 0.857/0.875/0.835. The checked composite improves 13.10%, with paired ratios 0.860/0.878/0.847. All three pairs favor the change in both lanes, despite the third trial being faster on both sides. The checked aggregate phase falls from 6.325875 to 5.507250 ms (-12.94%). This locates the main improvement in the profiled scan-heavy component.

The small original case rises 1.47% (+2.416 microseconds), within its 10.872-microsecond declared band; its paired ratios are 0.997/1.015/0.998. The small checked case falls 0.40%. These controls are retained rather than omitted.

The large checked ordered phase rises from 0.119542 to 0.133167 ms (+11.40%, +13.625 microseconds). Its trial pairs are 0.119542/0.149750, 0.121208/0.133167, and 0.063125/0.065958 ms, all higher after the patch. Applying the same diagnostic formula to the pooled phase observations yields an 89.124-microsecond band because the pre-change phase MAD is 29.708 microseconds. The increase lies inside that broad variability band, but this does not prove the ordered phase is unaffected. Savepoint/prepared/constraint phase medians also rise 2.88%/1.78%/1.23%. The common third-trial timing shift and phase increases have no proven cause; raw data and this uncertainty remain for frozen-candidate acceptance. No rerun was used to replace these results, and no claim of universal per-operation improvement is made.

All primary measured input/output block counters are zero. Large checked CPU medians fall from 6.582 to 5.745 ms, while process peak RSS remains 97,681,408 bytes on both sides; no memory reduction is claimed. Separate four-second profiles complete 586 before and 702 after cycles. COUNT remains dominant (3.835/3.826 profiled seconds across those differing iteration counts), and native sampling still shows owned output copying, row decoding and eager scan construction. The removed comparison/read-only tracking copies are one part of that remaining path, not its elimination.


These are warm, one-host test-opt1 diagnostic builds with one requested runtime worker and no macOS CPU pinning. CPU/block-IO and per-worker RSS are recorded; RSS includes runtime loading, fixtures, hashing and full-value validation. Profiles are instrumented separate observations, and their nested inclusive samples must not be added as exclusive percentages. Independent phase medians need not sum exactly to the primary median. No result establishes matched release/reference parity or closes #712.

## Cleanup and continuation

All 26 workers were confirmed absent; connections closed and per-case work directories were removed. Both native packages, parser/archive, the owned focused core test executable and four native target exports were reclaimed. Compact evidence and shared Cargo dependencies remain. Initial host usage: 19,215,532,032 bytes; observed peak: 19,405,328,384; final: 19,232,206,848. Builds were serialized within the 3 GB additional bound, below the 40 GB stop/50 GB ceiling. Manual workers used 180-second/2 GiB limits.

Next owner/action: Codex will integrate the performance stack once source-bound required CI is green and verify the adopted cases on the frozen candidate composition. Further scan/materialization and remaining #712 targets retain their measured gaps; HNSW/#681 and final public-version/reference guards remain in scope. Candidate integration, package/RC qualification, release publication, postrelease verification and main merge remain under #690. This report supports a Draft PR handoff only.
