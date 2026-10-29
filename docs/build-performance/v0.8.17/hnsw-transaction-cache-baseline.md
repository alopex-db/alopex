# #681: transaction HNSW cache baseline

The manually requested 100,000-row × 384-dimension case completed on 2026-10-29.
The observed cost justifies fixing both whole-graph copying/full-node persistence
on INSERT and repeated loading by consecutive transactions. These fixes and the
issue's functional acceptance are still open. This is one development measurement,
not a release performance qualification or a latency threshold.

## Identity and bounds

- Candidate base: `a40e39cb3d833363c8683faee5701b267d3917dc`.
- Measurement HEAD: `5e05f0149e93e92455e59fe8c3a4cdfbec19e578`.
- Example binary built at `6f850bd6`; the example source is unchanged at the
  measurement HEAD. Binary/example/runner hashes and compact raw events are in
  [the evidence JSON](hnsw-transaction-cache-baseline.json).
- macOS aarch64, 51,539,607,552 bytes physical RAM, 18 logical CPUs.
- Cargo test profile (`opt-level=1`), incremental/debug info disabled, build jobs 2;
  Rayon and OMP runtime threads limited to 2.
- Explicit published 0.8.16 parser override for development. This does not qualify
  a v0.8.17 parser package or wheel.
- In-memory fixture, streamed input with one generated vector at a time; two-row,
  four-dimension harness smoke succeeded before the requested full case.
- Runtime RSS stop: 8 GB; timeout: 900 seconds; storage stop: 40 GB. Initial/final
  host storage: 13,449,752,576 / 13,449,998,336 bytes. No limit was reached.

## Observations

| Operation | Elapsed time | Workload sampled peak RSS |
| --- | ---: | ---: |
| Seed 100,000 rows | 1,848.118 ms | 942,669,824 bytes |
| Build HNSW | 174,249.297 ms | 1,509,670,912 bytes |
| First cold transaction, query and rollback | 238.852 ms | 1,524,957,184 bytes |
| Second cold transaction, query and rollback | 221.146 ms | 1,525,071,872 bytes |
| Begin warmed writer | 0.022 ms | 1,525,104,640 bytes |
| First one-row INSERT | 712.189 ms | 1,975,140,352 bytes |
| Second one-row INSERT | 555.081 ms | 1,979,957,248 bytes |

The graph reports 180,643,400 bytes. After each cold transaction ends, shared cache
entries remain zero and misses increase from 1 to 2. The second transaction loads
the graph again. INSERT increases sampled RSS by about 450 MB relative to writer
begin, and the second INSERT still takes about 555 ms.

Phase RSS is sampled with `ps` every approximately 100 ms and at event boundaries;
these are observed lower bounds on exact phase maxima. Event handshakes are outside
the measured operation durations. `RUSAGE_CHILDREN` reports 1,979,957,248 bytes,
but includes monitor utilities as well as the workload. The original runner's
`process_lifetime_peak_rss_bytes` label was too specific; the evidence JSON and
current runner use `children_lifetime_peak_rss_bytes`. The raw measurements were
not rerun or changed to make this label correction.

Source inspection agrees with the measurement: `fork_owned` clones the graph;
staged writes snapshot the graph; `commit_staged` marks every node for persistence
when insertions are present. SQL graphs loaded within explicit transactions are
not published when those transactions end. The one-size measurement does not by
itself establish asymptotic complexity; that conclusion comes from these loops.

## Manual use

Build `issue681_hnsw_first_write` with the repository's per-host storage watchdog,
one shared target directory and the documented parser environment. Then run:

```sh
python3 scripts/performance/issue681_manual.py \
  target/debug/examples/issue681_hnsw_first_write \
  --output /private/tmp/alopex-681-comparison \
  --rows 100000 --dimensions 384 --timeout 900 --rss-limit 8000000000
```

The output directory must not exist. The runner rejects CI execution and larger
inputs/budgets. The executable requires the runner's explicit environment and
stdin acknowledgements. No workflow invokes this workload; normal CI only builds
the example where its existing target selection requires it. Run the full case
again only when a concrete implementation change needs comparison. Preserve compact
logs and identities, then remove the dedicated executable and fixture outputs.

## Remaining acceptance

- Replace the measured write-path copying/persistence and verify isolated readers,
  rollback and conflicts before another scale comparison.
- Publish safely reusable transaction-loaded graphs, with epoch and mutation
  guards so stale or uncommitted graphs cannot enter the shared cache.
- Complete tiny DELETE, NULL-update, TRUNCATE and conflict cases. Initial two-row
  regressions found old-reader result loss after NULL-update/TRUNCATE commit;
  the one-key MemoryKV reproduction also fails all six get/scan cases. Track the
  fix in [#748](https://github.com/alopex-db/alopex/issues/748); this version is not
  accepted for snapshot correctness.
- Reuse the existing two-index eviction test with tiny graphs and a correspondingly
  small cache budget; there is no reason to put two 182 MB graphs in normal CI.
