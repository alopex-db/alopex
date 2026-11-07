# Transaction phase diagnostics

This manual run investigates the 15.28% warm autocommit increase observed in
run 42405858776. It does not replace that run or qualify the release.

Use the existing before source `b5ad28540edcd8aacb91ff0d2d3dae8f4fd873bf`
and candidate source `4b88e0844b0a6cc1dab93a69a9e7bffbd8a1f39c` wheel assets,
their original build receipts, and the qualified test environment image.
The execution profile supplies runtime parameters. No product rebuild is needed.

## Fixed scope

- One fixture size: 1000 rows, no HNSW indexes, two-dimensional vectors.
- Three trials per subject, ordered before/candidate, candidate/before,
  before/candidate. Six cells, executed serially on one managed VM.
- Each cell uses a separate seed process followed by fresh readers for RO noop,
  RO query, RW noop, RW query, and autocommit query. Each regular reader records
  the first operation and five warm operations. Two additional fresh RW noop
  readers retain the original first-begin sampling design.
- The original SQL, returned-row checks, full final row/vector validation,
  extension digest checks, and lifecycle ownership checks remain in use.
- Each reader records 21 no-op timing controls before measurement.
- Collect `EXPLAIN` after measurement and require `ExactKnnScan`.
- Per-process deadline: 300 seconds; per-cell deadline: 420 seconds;
  matrix deadline: 3600 seconds. A cell is not started without its full budget.
- Use the existing capacity checks, offline container, resource bounds,
  encrypted collection, ownership checks, and cleanup. No automatic reruns.

## Observations and interpretation

For every begin, query, rollback, or complete autocommit call, save elapsed time,
process CPU time, calling-thread CPU time, minor/major page faults, voluntary and
involuntary context switches, block I/O operation counts, and physical read/write
byte deltas. Do not substitute zero for missing or decreasing counters.

CPU timing slightly encloses the elapsed-time interval. Resource and I/O snapshots
enclose the clocks and include observation overhead. Retain all no-op controls;
do not subtract them to invent a precise wait duration. Short begin/rollback
measurements can be dominated by this overhead. Process CPU includes background
threads; caller CPU does not. CPU/wall ratios alone do not prove a scheduler,
frequency, lock, filesystem, or product-code cause.

Keep all three trial medians and their MADs. The independent auditor verifies
source identities, raw hashes, process coverage, final data checks, query path,
and deleted run state before summarizing each observation. Summaries always keep
`performance_acceptance=false`; zero baselines have no ratio. The next action
must follow the observed distinction, including an inconclusive result.

The Python autocommit entry and embedded SQL driver are unchanged between the
selected sources. This SELECT uses a read-only borrowed transaction and does
not capture a write journal. Its KNN shortcut precedes B-tree query tracing.
The remaining relevant source changes include LSM visible-range construction
and SST entry decoding. These are investigation targets, not established causes.
