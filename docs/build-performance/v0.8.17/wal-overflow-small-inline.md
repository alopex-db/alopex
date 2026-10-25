# Bounded small overflow inline comparison

#682 — 2026-10-25

**Status: measured; overall acceptance remains open.**

Baseline: `d9ff8783b8d18a63d167204e55902ea6cd90dcd1` (v0.8.16). Previous overflow: `d995c550152e734ce821eb3de93180b533aad2f7`. Changed source: `77a2e6a8058260ff223d5ab64550f80969cc6d0c`, tree `eebd11bc64160def0767e36594faadff3f00fd53`. The report-only commit does not change measured runtime code.

The changed path writes a bounded inline v3 batch when the operation stream is at most 1 KiB, within the configured inline recovery budget, and leaves space for a subsequent fixed-size reference. It preserves reservation/replay checks, checkpoint pressure, synchronous commit and poisoned-writer recovery. Larger or tightly packed writes retain the spool path. The existing v3 reader handles both records; no new public API or disk format is introduced.

All four modes use the same probe, public Rust LsmKV API, EveryWrite synchronization and 4096-byte ring. Each database contains one prior key and two pending keys, with 16- or 2048-byte values. Automatic checkpointing is disabled. One CPU is pinned, order rotates, and each cell has one warmup plus five measured fresh-process/fresh-DB cases with warm OS cache. Each child has a 30-second timeout. This is a manual micro-case comparison outside ordinary CI.

The original three-mode comparison remains in [the prior report](wal-overflow-kv-cost.md). Previous overflow was rebuilt for this four-mode run; timings across the two runs are not pooled.

| Value bytes | Mode | Outcome | Commit median (µs) | Peak process RSS median (KiB) | WAL bytes | Spool bytes | Reopen median (µs) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | baseline | committed | 14043.040 | 2824 | 4144 | 0 | 9065.126 |
| 16 | previous-overflow | committed | 20001.691 | 2904 | 4144 | 290 | 7347.467 |
| 16 | candidate-legacy | committed | 15888.199 | 2900 | 4144 | 0 | 6793.445 |
| 16 | candidate-overflow | committed | 3431.459 | 2908 | 4144 | 0 | 6511.829 |
| 2048 | baseline | capacity_refused | — | 2836 | 4144 | 0 | 7688.637 |
| 2048 | previous-overflow | committed | 18485.575 | 2884 | 4144 | 4546 | 6537.503 |
| 2048 | candidate-legacy | capacity_refused | — | 2908 | 4144 | 0 | 8323.373 |
| 2048 | candidate-overflow | committed | 20168.940 | 2912 | 4144 | 4424 | 6924.472 |

Small overflow after/before ratio: **0.172×** (82.8% lower median). Candidate legacy/baseline: **1.131×**. Candidate overflow/baseline: **0.244×**. Candidate overflow/candidate legacy: **0.216×**.

The predeclared >1.20 baseline-ratio investigation flags are **none**. Five tiny samples do not establish general latency or throughput. All samples are retained, with no outlier removal, noise exemption or repeated run selected for a better result.

All **48 write/reopen pairs** verified expected state: 36 successful commits and 12 expected legacy capacity refusals, including warmups. Legacy 2048-byte refusal latency is not compared with successful commit latency. Successful cases recover both pending keys; refusals retain only the prior key. The writer exits without destructor checkpointing. This verifies process-exit replay, not power-loss durability.

Maximum fixture logical bytes: **8,787**. Maximum whole-process VmHWM: **2,948 KiB**. RSS includes open and prior commit, not incremental transaction allocation. WAL/spool lengths are not physical write traffic. Raw samples include allocated filesystem blocks; preparation, staging, commit and reopen clocks remain separate.

The 13 new focused tests cover Put/Delete/empty values, mixed inline/spool replay, 1 KiB/configured budget/free-ring boundaries, replay checkpoint accounting, old-reader refusal, real write error, uncertain append recovery and all three sync policies. The LSM suite passed 288 tests with three existing child-entrypoint ignores; the final exact-ring test then passed separately. A 32-case property test checks the encoding upper bound. Focused Clippy and format checks passed.

Mutation check selected 17 changed-path capacity, selection, version and sync mutants: 15 caught, two unviable, zero missed/timeouts. Unviable cases are a missing Default implementation and Rust parsing the mutated cast/comparison as generic arguments. Scope is explicit; this is not whole-module mutation coverage. Coupling inspected 32 LSM modules with AST fallback after dependency metadata fetch failed; existing module hotspots and the private LSM→WAL boundary are retained. Dynamic ordering, cfg/macros and implicit algorithm duplication remain analysis blind spots.

The new inline tests cover actual write refusal and injected append/sync boundaries. The earlier reference-path syscall injection evidence does not independently qualify the new inline path against actual sync syscall EIO. That remains a focused follow-up, alongside public Database/SQL/Python path validation, Windows public API support and real-device power-loss acceptance. No unsafe path changed; Miri was not applicable, and Kani was not run.

Overflow budgets and all source/binary/probe/driver/lock hashes are in the JSON. Local parser use was a published 0.8.16 development override, not release qualification. Initial local parser configuration and mutation CLI failures were corrected; their logs are retained. This comparison excludes SQL, Python, indexes, cold cache, throughput, large data and Windows performance.

See [reproduction](../../../scripts/compatibility/README.md#bounded-kv-cost-comparison-manual-linux-measurement) and [identities, verification and raw samples](wal-overflow-small-inline.json).
