# Python transaction tracking retention (#712)

## Contract and source identity

Repeated `Database.begin()` followed by rollback and handle destruction left a `Weak<PyTransactionInner>` in the database registry. A dead Weak still retains the Arc allocation until Database.close. The corrected registry rotates at most eight existing entries per successful begin, removes expired handles, and preserves live handles for close/retry. This is amortized cleanup: the last expired handle and a backlog behind live entries can remain until later begin/close; it is not immediate reclamation or a bound on genuinely live handles.

- Implementation and after native source: `36c445466b705f15223d534b2ca4a252e09baafc`.
- Parent: #779 `e69b179ae32961703fef102a6538dcea61430471`; before native `181caf7435d24ad4e89604cc52f37d6e54181a0b`, product identical to #778.
- Exactly the same Python package files, parser and harness hashes on both sides; only the native extension differs. Source and package hashes, observations and raw measurements are in `transaction-tracking.json`.
- Draft stacked on #779. Candidate integration, candidate CI and full #712 acceptance remain pending; these results do not qualify v0.8.17 for release.

## Focused verification

- Before implementation, the three-iteration tiny regression failed at the second begin/drop: two retained entries instead of one. The initial attempt failed to locate libpython3.13 before running tests; supplying the existing Python library path fixed the validation setup.
- After implementation: `cargo test --offline --locked -p alopex-py --lib tracking_`: 5 passed; `... --lib close_`: 14 passed, 17 unique tests in total. Four new tests cover RO/RW handle reclamation, the eight-entry work budget, rotation past live entries, and retention of a failed live rollback for close retry.
- Scoped Clippy (`-p alopex-py --lib --tests -- -D warnings`), format and diff checks passed. Existing parser provenance warning is retained; this local parser is not release staging evidence.
- Coupling ran first over `crates/alopex-py/src/embedded`, `--changed-since main --impact-depth 4`: C, 3 high / 0 critical. Existing database/transaction tracking and lock/GIL coordination remain accepted; no new public or module boundary. Main-relative results include previous v0.8.17 changes. Syntax analysis cannot prove PyO3 cfg/macro, shared-state or GIL timing; lifecycle tests cover the affected boundary.
- `cargo mutants --no-config --in-place -p alopex-py -f crates/alopex-py/src/embedded/database.rs -F sweep_expired_transactions --baseline skip --timeout 30 --build-timeout 180 -- ... --lib tracking_`: four prelisted mutants, one caught, three unviable, no survivors. Unviable: no-op has unused argument, `<0` and `>=0` are invalid unsigned comparisons under warnings-as-errors. A first command with incompatible `--in-place -j 1` ran no mutants; the serial invocation corrected that. The before-fix regression independently detects absent sweeping.
- No new property test was required by a survivor. Miri/Kani and whole-workspace builds were not run: no new unsafe path or arithmetic algorithm. All tests use tiny fixtures; manual repetition/profile workloads are excluded from ordinary CI.

## Bounded memory diagnosis

Each fresh process used three persisted keys, 1,000 batches of 100 RO begin/rollback calls, a 512 MiB peak-RSS stop, and a removed temporary DB. The Python-only control calls a no-op. These are peak RSS observations at the first and final batch, not a measurement of memory returned to the OS after Database.close.

| Mode | Before first → final bytes | After first → final bytes |
|---|---:|---:|
| none | 36,765,696 → 115,785,728 | 36,978,688 → 37,011,456 |
| per-call | 36,929,536 → 115,884,032 | 36,782,080 → 36,814,848 |
| continuous | 36,995,072 → 115,949,568 | 36,847,616 → 36,896,768 |
| python-only | 36,683,776 → 36,716,544 | 36,683,776 → 36,716,544 |

Native memory previously grew by about 79 MB regardless of cProfile mode. With the fix, all three native modes plateaued by batch 10, with only 32–49 KB increase from the first batch. All four modes completed without reaching the guard.

## Manual timing and completed profiles

Same seed 7, 10-byte keys, 32-byte values, persisted SST fixtures, 3/10,000 keys. Three trials × eight cells, each with three warmups and 21 measured samples: 504 measurements and 72 warmups. Every result validates copied byte types, order and full unchanged state after close/reopen. Point operations below time 100 separate transactions; range times one complete range of min(rows, 1,000) items. These are medians of trial medians, not individual-request latency.

Before is the retained baseline from #779; this comparison is not temporally interleaved AB/BA and does not establish a cross-version/reference performance claim. The diagnostic band was recorded before after-measurements: max(5% baseline median, 3 × pooled baseline MAD). No cell increased; improvements within the band are not established wins.

| Keys / operation | Before ms | After ms | Change |
|---|---:|---:|---:|
| 10000-begin-rollback | 0.047792 | 0.042791 | -10.46% |
| 10000-random-get-hit | 0.327750 | 0.327167 | -0.18% |
| 10000-random-get-miss | 0.072125 | 0.065333 | -9.42% |
| 10000-range-scan | 2.624417 | 2.565416 | -2.25% |
| 3-begin-rollback | 0.049500 | 0.045209 | -8.67% |
| 3-random-get-hit | 0.121291 | 0.116334 | -4.09% |
| 3-random-get-miss | 0.073500 | 0.065250 | -11.22% |
| 3-range-scan | 0.020042 | 0.018083 | -9.77% |

The four separate four-second profiles now completed. The prior begin/rollback profile stopped at 2,183,692,288 bytes and remains recorded as a failure in `kv-read-baseline.json`; it is not replaced by this success.

| Profile | Iterations (100 sessions for point operations) | Peak sampled RSS bytes |
|---|---:|---:|
| begin-rollback | 56,611 | 83,902,464 |
| random-get-hit | 10,768 | 84,033,536 |
| random-get-miss | 32,362 | 83,853,312 |
| range-scan | 1,374 | 84,918,272 |

cProfile isolates public calls: hit cost is predominantly `Transaction.get` (2.930 s of 3.845 s total measured operation time), and range cost is predominantly `scan_range` (3.739 s of 3.754 s). Native samples include validation/setup and show SSTableReader::get_cached / owned_visible_at and kv_scan::read_page paths. These are investigation leads, not exclusive-time percentages or completed KV optimization acceptance.

## Cleanup and continuation

- Measured host peak: 18,685,136,896 bytes; after cleanup: 18,520,571,904 bytes, below the 40 GB stop and 50 GB limit.
- All 28 measured/profile processes ended, all 28 fixture directories were removed. The four memory-probe processes exited successfully and removed their temporary DBs. Both before/after native packages, retained parser, target exports and owned test executable were removed after recording hashes; shared Cargo dependencies and compact evidence remain.
- Next: investigate the persisted KV `get_cached`/range-page paths against the completed profiles, with a bounded implementation and focused regression contract before any further build. #712 still requires remaining workload improvements and composed-candidate/reference comparison. #779 and this PR require CI and dependency-order candidate integration. Full release/RC/main work remains pending.
