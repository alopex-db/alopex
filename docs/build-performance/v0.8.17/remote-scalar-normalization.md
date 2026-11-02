# Remote scalar SQL value normalization (#712)

The existing #776 public-call profile identified `_rows_to_dicts` and
`_unwrap_sql_value` as server-client SELECT result costs. This change adds a
14-line exact-built-in-type path for Integer, BigInt, Timestamp, Boolean, Text,
and Double payloads. Single-key wire shape is checked first; all other payloads
use the existing strict validation and conversion. This preserves bool/integer
separation, Double integer-to-float conversion, Float binary32 rounding, NULL,
unknown-tag errors, column order and duplicate-column handling. No HTTP protocol,
public API, native code, on-disk format or dependency changes.

## Identities and method

- Before Python source: `4abffca70a9ee5cb57046206663b65ddbaaca733` (#777).
- After Python source/native build: `cad7d51488fe0f9874beb4b913267ce10f113c34`.
- Rust source is identical between these commits. Build native/server once;
  both Python packages share the same native bytes via hardlinks. Every package
  file other than `remote.py` is byte-identical. The fixed published 0.8.16
  aarch64 parser is an internal validation input, not candidate qualification.
- Apple M5 Pro, 18 logical CPUs, 48 GiB memory, macOS 27.0.1, Python 3.13.16.
- Existing `scripts/performance/run_sql_query_profile.py`, unchanged.
  Before/after/after/before/before/after; three trials per side; one preflight,
  three warmups and 21 measured calls per cell. Server surface only: native and
  embedded behavior are unchanged, so no duplicate embedded timing was run.
- 36 cells, 756 measured calls and 108 warmups. Original 34,250-row fixtures for
  aggregate, ordered LIMIT and SELECT-all; three-row read controls and the
  original one-row INSERT/rollback control. The original SELECT-all returns
  17,125 rows. Setup, result validation and final-state verification stay
  outside timed public calls.
- Every result's values, Python types and order contract were validated; every
  fixture's complete unchanged state was checked after reconnect. Matched
  native hashes, expected-result hashes and final-state hashes across all cells.
- Manual local performance execution only; no load/performance test added to CI.
  Each case has a 180-second limit, 2 GiB per-process RSS stop, 2 GB fixture
  limit and 40 GB host-storage stop. All fixtures were removed immediately.

## Observed public-call times

Median of three trial medians, milliseconds:

| Case | Before | After | Change |
|---|---:|---:|---:|
| server-1-insert-rollback | 0.444208 | 0.451208 | +1.58% |
| server-3-ordered | 0.332750 | 0.361583 | +8.67% |
| server-3-select-all | 0.259083 | 0.260666 | +0.61% |
| server-34250-aggregate | 10.979834 | 11.080125 | +0.91% |
| server-34250-ordered | 3.650875 | 3.655416 | +0.12% |
| server-34250-select-all | 31.455792 | 29.427291 | -6.45% |

The intended SELECT-all case decreased by 6.45%. The other five medians
increased, including the three-row ordered case (+8.67%, +28.833 microseconds).
Every increase is within the predeclared diagnostic band, the larger of 5% of
baseline median and three times pooled baseline MAD. That is a description of
measurement uncertainty, **not proof that increases are noise or a release
acceptance threshold**. No extra trials were run to seek a preferred result.
Full trial medians, MADs, bands and raw samples are in the adjacent JSON.

An earlier isolated exact-source-function probe (three/17,125 result rows,
three warmups/21 samples/three trials per side) suggested a 22.64% reduction for
17,125 rows. It selected the hypothesis; it is separate from the public API
comparison and is not an end-to-end speedup claim.

## Semantic verification and cleanup

- 34 tiny normalization cases pass on before and after; 138 existing HTTP client
  cases pass after: 172 unique cases. Covers mixed types in one column, invalid
  earlier values under duplicate names, exact result types and subclass fallback.
- Four deliberate faults are caught: allowing bool under Integer, retaining int
  under Double, bypassing Float narrowing, and accepting an unknown integer tag.
  Restored module hash matches the tested source; all 34 cases pass again.
- One bounded native build and `git diff --check` pass. No Rust behavior changes;
  cargo-mutants/coupling/Miri/Kani were not run for this Python-only change.
  No unresolved negative control or new unsafe behavior.
- All 72 owned client/server PIDs were confirmed gone; all 36 DB fixtures removed.
- Peak host allocation 18,184,978,432 bytes;
  after cleanup 18,010,005,504 bytes. Removed 36 owned paths,
  including both packages, shared native outputs/server and downloaded parser.
  Kept compact logs/identities and pre-existing shared dependencies. The reused
  pytest environment was unchanged. Hardlink logical sizes are not summed as
  physical storage savings.

This is a development comparison against the #777 parent, not a comparison to
published 0.8.16 or other engines, candidate acceptance, or completion of #712.
It must not be added arithmetically to gains from #774, #777 or the separate
#770 branch. Required PR/candidate CI, combined effects and the other adopted
#712 workloads remain outstanding. Local receipt directory:
`/private/tmp/alopex-712-remote-conversion`.
