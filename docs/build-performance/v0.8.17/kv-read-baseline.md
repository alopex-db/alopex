# Persisted public KV read baseline and transaction retention (#712)

The SQL memtable measurements do not establish KV behavior after durable
fixture creation, flush, close and reopen. This diagnostic preserves the
original KV workload from `alopex-db/docs/scripts/performance/write_kv_native.py`
(docs commit `0c8fcb096f4e70aefdfad48e69f22df0b91aa141`, blob
`2e1e1d94a7dd8e232d2717cf9b92bbb8f326fbce`). All24 request plans match the
original exactly for hit/miss/range at both3 and10,000 keys. Keys are10 bytes,
values32 bytes, seed7; point samples contain100 independent read transactions,
range samples consume up to1000 copied byte pairs in one transaction.

## Source and completed measurement

Product source is unchanged from parent `3919d3f74678a2e9ad3eb467b5b1261a625bc6cb`
(#778). Native build source is `181caf7435d24ad4e89604cc52f37d6e54181a0b`;
measurement harness source is `2e997326527534b9b4be3bf3b390d626b4d06820`.
Both commits change only the harness/tests. Exact native/package/harness hashes
are in the JSON. One `cargo build --offline --locked --profile test -p alopex-py`
uses the existing shared target, jobs2, no incremental/debug info and the fixed
published0.8.16 arm64 parser. The parser override is internal validation only.

All24 timed cells passed: four cases,3/10,000-key fixtures, three trials,
one preflight plus3warmups/21measurements each (504 measured,72 warmups).
Begin/rollback is an additional empty-session diagnostic, not an original
product-comparison row. All sample bytes/types/order and complete state after
close/reopen matched independent expected data. Reopened file inventories
contain an SST (`sst/1.sst`) for both fixture sizes. All raw samples and hashes
are retained, and the summary arithmetic was independently recomputed.

Times below are medians of three trial medians. Point/control units are **ms per
100 separate sessions**, not one-request p50; range is ms per complete range
and one read session. Pooled MAD is in the same units.

| Case | Median ms | MAD ms |
|---|---:|---:|
| embedded-10000-begin-rollback | 0.047792 | 0.001667 |
| embedded-10000-random-get-hit | 0.327750 | 0.010542 |
| embedded-10000-random-get-miss | 0.072125 | 0.001125 |
| embedded-10000-range-scan | 2.624417 | 0.058667 |
| embedded-3-begin-rollback | 0.049500 | 0.001875 |
| embedded-3-random-get-hit | 0.121291 | 0.003499 |
| embedded-3-random-get-miss | 0.073500 | 0.001833 |
| embedded-3-range-scan | 0.020042 | 0.000833 |

These are internal baseline values, not published0.8.16/LMDB comparisons or
proof of an improvement. Snapshot/get/range/Python phase attribution is still
incomplete.

## Profiling stopped at the memory guard

The separate4-second begin/rollback profile reached the2GiB client RSS stop
(2,183,692,288 bytes). The supervisor terminated its owned process and
removed its DB. It did not complete: zero successful profile cases, one failed,
three not started. Its partial native stack is a failure artifact, not a valid
completed cost profile. The24 completed timing cells remain separate evidence.

A bounded follow-up used the identical native package with only3 keys. Four
fresh processes ran1000 batches of100 begin/rollback operations (or a no-op
Python-only control), with a512MiB per-process peak-RSS stop. All completed:

| Mode | Peak after first batch | Peak after1000 batches |
|---|---:|---:|
| none | 36,765,696 B | 115,785,728 B |
| per-call | 36,929,536 B | 115,884,032 B |
| continuous | 36,995,072 B | 115,949,568 B |
| python-only | 36,683,776 B | 36,716,544 B |

Native calls grow peak RSS similarly with no profiler, per-call cProfile, and
continuous cProfile. The Python-only control stays nearly flat. Thus the
observed growth is not specific to cProfile enable/disable. These are peak RSS
observations, not proof of RSS reclamation after database close. The exact
bounded probe source and checkpoints are embedded in the JSON.

`crates/alopex-py/src/embedded/database.rs::PyDatabase::begin` appends a
`Weak<PyTransactionInner>` to `txns` on every begin. It never removes expired
entries there; `rollback_tracked_for_database_close` only retains/removes them
when the database closes. A Weak keeps its Arc allocation until it is removed,
even after the strong transaction handle has been dropped. This gives a
source-backed explanation for accumulation during a long-lived Database.

The next product change must reclaim expired tracking entries while preserving
all live handles for database-close rollback and retry. It must also avoid an
unbounded scan of all live entries on every begin. Add tiny ownership/lifecycle
regressions first, then repeat the bounded memory control and finish the four
profiles. Do not hide this problem by reducing profile duration or dropping
close/lifecycle guarantees. This PR contains the diagnosis, not that fix.

## Verification, budgets and retained assets

Seven new tiny harness cases plus ten existing SQL harness cases pass, including
under `CI=true`. They validate fixed byte plans, session boundaries, full range
consumption, cleanup on read errors, result types/order, and CI rejection before
native import/output creation. Ordinary CI runs only these17 functional cases;
performance execution is guarded and remains manual. The shared supervisor now
allows omitted `--server` only for embedded-only runs; existing server selection
still requires a server binary. YAML parse and diff check pass. No product Rust
behavior changes, so cargo-mutants/coupling/Miri/Kani were not run.

Host allocation peaked at18,335,436,800 bytes; after target-export cleanup it is18,285,228,032 bytes. All25 timed/failed-profile PIDs were confirmed gone, all25 fixtures removed, and the four memory-probe processes exited with their temporary directories cleaned. The failed profile is explicitly retained.

Codex retains exactly one source-bound baseline native package and the pinned
parser (38,349,334 logical bytes) for the immediate tracking fix comparison.
Target export links were removed so a future build cannot overwrite that
baseline. This avoids a redundant baseline rebuild. Remove these retained assets
immediately after the fix comparison/profiles; shared dependencies and compact
receipts remain. Receipt directory: `/private/tmp/alopex-712-kv-profile`.

Draft diagnostic handoff only: product retention is unresolved, profiling is
incomplete, and PR/candidate integration and #712 reference qualification remain
outstanding. The host memory stop and incomplete cases must not be reported as
successful acceptance.
