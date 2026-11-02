# Tiny public Python KV/SQL overflow cost comparison

#682 — 2026-10-30. **Measured; overall issue and release acceptance remain open.**

Before: `d995c550152e734ce821eb3de93180b533aad2f7` (pre-inline overflow).
After: `9eb64643add97432b1a27dd259b3a513fe02942e`, whose product source is candidate
`e4d5f59e67b93d8e3f4a491dc725cb96a47639f8`; the additional commit adds only the
measurement driver and its tests. Subsequent report changes do not change the
measured product or driver. The intervening candidate includes other fixes, so
these timings alone do not attribute every difference to inline WAL selection.

Both extensions were built on the same Apple M5 Pro Mac (18 logical CPUs,
48 GiB RAM, macOS 27.0.1), with the same Rust/Python toolchains, test profile
opt-level1, two build jobs, incremental/debug symbols disabled, and the same
published 0.8.16 parser contract0.28.0 development override. They are ordinary
extensions without the private fault hook, not qualified release wheels.
Source/tree/native/parser/lock/Python-source digests and tool versions are in
[the JSON report](wal-overflow-public-api-cost.json).

## Conditions and verification

Each fresh database holds one prior record and two pending records, with
16- or 2048-byte values. Both builds use the same public `WalOverflowOptions`:
one 4096-byte payload ring, EveryWrite synchronization, explicit finite overflow
budgets, and the public API's default checkpoint/converge policy. SQL uses an
integer primary key and a separately created secondary B-tree on `code`.
Schema creation, index creation, prior commit, input preparation, begin, staging,
target commit and reopen each have separate clocks. Empty-table index creation
here is not a large index-build benchmark.

The writer exits without destructors after recording the synchronous commit
and live WAL/spool footprint. Two independent readers then reopen and check
all values. SQL also checks the indexable hit/miss query results; the plan is
not asserted at three rows, so this is not proof of B-tree search throughput.
Fixtures are removed after each case. Process-exit replay is not device power
loss or hardware-fault qualification.

All **48 writers and 96 readers passed**, including one warmup and five measured
cases per build/surface/value-size cell. An earlier eight-writer/16-reader smoke
also passed; its timings are not pooled. The build order was fixed but run order
alternated by round. OS caches were warm; each process/database was fresh. No
CPU affinity was imposed on macOS. Runtime thread settings were Rayon2, OMP2,
OpenBLAS1. No local builds or test suites ran during the formal measurement.

Every child has a 30-second timeout, each fixture a 1 MiB logical-size bound,
and the writer checks a 1 GiB whole-process peak RSS bound. Maximum observed
fixture size was **15,710 bytes**, maximum writer peak RSS **40,583,168 bytes**.
The peak is process lifetime through commit, including import/preparation, not
incremental commit allocation. WAL/spool bytes are file lengths before close,
including prior/schema/index work, not physical write traffic. Allocated blocks
and every raw sample are retained in JSON.

## Results

Commit medians use the five non-warmup samples. No outliers are removed and no
noise exemption is applied. Ratios compare only the same API and data shape.

| Surface | Value bytes | Before commit (µs) | After commit (µs) | After / before | Before spool bytes | After spool bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Python KV | 16 | 9834.875 | 3795.708 | 0.386 | 290 | 0 |
| Python KV | 2048 | 10821.208 | 10021.041 | 0.926 | 4546 | 4424 |
| Python SQL | 16 | 9886.167 | 3740.917 | 0.378 | 2754 | 0 |
| Python SQL | 2048 | 9788.000 | 10847.292 | 1.108 | 11266 | 9620 |

WAL length was 4144 bytes for every cell. Small KV/SQL cases avoided spool files
on the candidate. The large SQL commit median increased **10.8%**; it is retained
as an observed cost, not rewritten as an improvement or dismissed as noise.
The predeclared ratio>1.20 investigation flags are empty, but that policy is not
an automatic performance acceptance gate. Five tiny samples cannot establish
general latency, throughput, cold-cache behavior or release performance.

KV staging also increased from 2.875 to 11.334 µs (16-byte values) and 3.083 to
11.583 µs (2048-byte values). These costs remain separate from commit. Median
whole-process peak RSS increased from 35,831,808 to 36,061,184 bytes for small
KV and from 40,222,720 to 40,402,944 bytes for small SQL. The JSON includes all
other medians, commit MADs, index/preparation clocks and reopen times.

An independent sorted-middle recalculation matched the commit/stage/RSS/file
medians and ratios. All 96 reader receipts and both copied Python source sets
were rechecked against their recorded hashes. The prior Linux Rust-direct KV
results and this macOS public-Python comparison are separate experiments.
Published 0.8.16 has no matching public overflow settings; this comparison does
not pretend its default legacy ring is the same configuration.

## Development checks and reproduction

The driver is `scripts/compatibility/public_api_cost.py`. Four small standard
library tests reject missing/duplicate/failed/unreopened measurements, keep
warmups out of medians, reject CI and Python `-O` before product import/output,
and check WAL/spool/total accounting and symlink rejection. Both native builds,
ordinary imports, smoke and formal measurements passed. No product Rust changed,
so mutation/coupling/Miri were not rerun. No performance job was added to CI.

Build each pinned source with `cargo build --locked --profile test -p alopex-py`
under the host storage watchdog, using one shared target and the settings above.
Copy its own Python package and native extension to separate owned directories.
Each directory must contain `build-identity.json` with `source_sha`,
`native_sha256`, `profile`, `python`, `parser_sha256` and `rustc`; the driver checks
the native hash/source and equality of the toolchain/profile/parser fields.
Retain the full package source hashes and Cargo.lock digest as in this report.
Run `public_api_cost.py --before-package <before> --after-package <after>
--before-sha <before SHA> --after-sha <after SHA> --output <new directory>`;
first use `--smoke` in a separate new directory. Never add these runs to CI.

The measurement did not fail. The first post-run host-metadata read was denied
by the local sandbox; a dedicated read-only metadata query succeeded. No timed
sample was repeated because of that metadata collection error.

Host storage started at 16,099,069,952 bytes, peaked at 16,543,416,320 and ended
at 16,384,172,032 after cleanup. Both copied packages, parser download/extraction,
all fixtures and four owned native output files (87,009,296 bytes by pathname)
were removed after saving hashes. Compact evidence and the single shared Cargo
dependency cache remain for active release development. No new worktree was
created; unrelated worktrees were preserved.

Windows public API qualification is tracked separately in #771. Candidate
integration/CI, the remaining #682 acceptance conditions, release assets and
the broader #712 performance scope remain open.
