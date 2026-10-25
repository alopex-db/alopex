# Bounded WAL overflow KV cost comparison
#682 — 2026-10-25

**Status: measured; overall performance acceptance remains open.**

Baseline: `d9ff8783b8d18a63d167204e55902ea6cd90dcd1` (v0.8.16). Candidate: `a58b77b136e35faa6a0bffc44ea22c0bb688f74e`. The same probe source, release profile, public Rust LsmKV API, EveryWrite synchronization and 4096-byte ring were used for all three modes.

Each database contains one prior key and two pending keys. Values are 16 or 2048 bytes. Automatic checkpointing is disabled. One CPU is pinned, engine order rotates, and each cell has one warmup plus five measured fresh-process/fresh-DB cases with warm OS cache. No index is built. Each child has a 30-second timeout. These are manual micro-cases, absent from ordinary CI.

Overflow budgets are explicit micro-case settings: payload 16,384, spool file 32,768, fragment 256, key 16, value 4,096, directory 131,072, replay 32,768, inline 1,024, activation 8,192 and container fence 4,096 bytes. These are not default-workload performance settings.

| Value bytes | Mode | Outcome | Commit median (µs) | Peak process RSS median (KiB) | WAL bytes | Spool bytes | Reopen median (µs) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | baseline | committed | 13690.835 | 2832 | 4144 | 0 | 8868.856 |
| 16 | candidate-legacy | committed | 14140.673 | 2900 | 4144 | 0 | 8256.538 |
| 16 | candidate-overflow | committed | 20647.746 | 2884 | 4144 | 290 | 7250.757 |
| 2048 | baseline | capacity_refused | — | 2864 | 4144 | 0 | 8312.409 |
| 2048 | candidate-legacy | capacity_refused | — | 2888 | 4144 | 0 | 8424.007 |
| 2048 | candidate-overflow | committed | 18659.644 | 2900 | 4144 | 4546 | 6430.044 |

Small-case commit median ratios: candidate legacy/baseline **1.033×**, candidate overflow/baseline **1.508×**, candidate overflow/candidate legacy **1.460×**.

The predeclared >1.20 baseline-ratio investigation flags are: **candidate-overflow**. These observations are not an automatic performance sign-off. Five tiny samples cannot establish general latency or throughput. No outlier is removed, and no noise exemption is applied.

The candidate source routes every opt-in commit through a spool reference (`append_overflow_write_set`), including small transactions. This is a concrete follow-up boundary for investigating observed opt-in cost; these timings alone do not attribute the complete cost to a particular syscall. Keep legacy changes and opt-in cost separate.

All **36 write/reopen pairs** verified the expected state: 24 successful commits and 12 expected legacy capacity refusals (including warmups). The 2048-byte legacy refusals are capability gaps, not comparable successful commit timings. Reopen after a refusal retained only the prior key; successful cases recovered both pending keys. The writer exited without destructor checkpointing. This verifies replay after process exit, not device power loss.

Maximum fixture logical bytes: **8,788**. Maximum observed whole-process VmHWM: **2,944 KiB**. WAL/spool are file lengths before close, not physical write traffic. Allocated filesystem blocks and every raw sample are retained in the JSON. VmHWM includes database open and the prior commit; it is not incremental transaction allocation.

Preparation, staging, commit and reopen have separate clocks. This comparison does not cover SQL, Python, indexes, cold cache, large data, throughput or Windows. #682 also retains its Windows public API and real-device durability acceptance work.

The initial probe compile failure (`rollback` instead of `rollback_self`) and Clippy spelling failure were corrected before measurement. All three binaries were rebuilt from the corrected identical probe; final release builds and focused Clippy passed. Initial failure logs remain in local evidence. Product Rust code and normal CI were not changed.

See [reproduction and measurement limits](../../../scripts/compatibility/README.md#bounded-kv-cost-comparison-manual-linux-measurement) and [source identities, medians and raw samples](wal-overflow-kv-cost.json). Binary digests, toolchain and per-build lockfile digests bind the measurement; the compact locks and build logs are retained locally.
