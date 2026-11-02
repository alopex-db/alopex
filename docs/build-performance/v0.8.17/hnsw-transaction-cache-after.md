# #681: transaction HNSW cache after remediation

The requested 100,000-row × 384-dimension manual case completed at product commit
`2085048699010852da9ccd1abbd9a80fef6d05fd` on 2026-10-29. The observed first INSERT and
second-transaction read costs are lower after the MemoryKV snapshot, graph copy-on-write,
dirty-write/checksum/statistics delta, safe cold-publication, begin and savepoint changes.
This is one development comparison. Candidate integration and release qualification remain open.

## Same workload and bounded execution

- Same local macOS arm64 host; 18 logical CPUs and 51,539,607,552 bytes RAM.
- Cargo test profile (`opt-level=1`), incremental/debug disabled, build jobs 2; runtime Rayon/OMP threads 2.
- In-memory DB, one generated input vector at a time, 256 MiB cache. Seed/build and query/INSERT durations are separate.
- 8 GB RSS stop, 900-second timeout, 40 GB host storage stop, 1 GB additional runtime storage budget. No limit reached in the successful run.
- The example and runner were temporarily materialized from #749 (`85ed5cfc`). The example hash equals the baseline; runner differences only correct the child-RSS field name/scope.
- Product sources stayed at the recorded HEAD/tree. The temporary files were removed after measurement; #749 remains the canonical harness PR.
- The initial sandbox attempt failed before any RSS sample because `ps` was denied. Its process exited before a bounded retry with host process-read access. The failed run is retained.
- Published 0.8.16 parser development override; this is not parser/package release qualification.

## Observations

| Operation | Before | After | After / before |
| --- | ---: | ---: | ---: |
| Seed rows | 1,848.118 ms | 1,910.694 ms | 1.0339 |
| Build HNSW | 174,249.297 ms | 191,442.375 ms | 1.0987 |
| First cold query + rollback | 238.852 ms | 241.163 ms | 1.0097 |
| Second transaction query + rollback | 221.146 ms | 0.626 ms | 0.0028 |
| Begin warmed writer | 0.022 ms | 0.027 ms | 1.2100 |
| First one-row INSERT | 712.189 ms | 5.130 ms | 0.0072 |
| Second one-row INSERT | 555.081 ms | 4.953 ms | 0.0089 |

First INSERT's sampled RSS increase relative to writer begin changed from 450,035,712 to 163,840 bytes.
The sampled writer-begin RSS itself increased from 1,525,104,640 to 1,587,707,904 bytes; memory did not decrease in every phase.
First-INSERT sampled RSS was 1,975,140,352 before and 1,587,871,744 after.

After the first cold transaction ends, the shared cache retains one graph. The second
ends with the same miss count (1) and a hit, instead of loading again. Retained graph
bytes are 180,644,168, versus the baseline 180,643,400; both fit the same cache budget.

Index construction increased from 174.249 to 191.442 seconds (about 9.87%). This single
pair does not isolate source changes from measurement variation. Keep it as an unresolved
performance observation for dedicated qualification; do not claim broad absence of regression.

`ps` samples every approximately 100 ms and at event boundaries. Phase RSS values are
lower bounds on exact maxima; event handshakes are outside operation timings.
`RUSAGE_CHILDREN` includes the workload and monitor utilities. INSERT timings include
SQL/HNSW work before rollback, not durable transaction commit. One-size observations
do not establish asymptotic complexity or statistical speedup guarantees.

## Evidence and remaining work

- [Compact events, identities, comparisons and raw digests](hnsw-transaction-cache-after.json).
- [Baseline report and conditions](https://github.com/alopex-db/alopex/blob/85ed5cfc703b3da1332433cdffe5efb3d66bc505/docs/build-performance/v0.8.17/hnsw-transaction-cache-baseline.md).
- The functional behavior is covered separately by the small snapshot, cold-publication, savepoint, statistics and two-index eviction suites in #750–#760; this measurement does not replace them.
- Host storage peak 13,911,904,256 bytes; after removing 4 generated example artifacts and the two temporary source files, 13,849,800,704 bytes. Compact logs/shared dependencies retained for active release work.
- No performance/load workload is added to normal CI.
- Integrate dependencies and verify the candidate before accepting #681. Retain the index-build observation for controlled performance investigation.
- RC artifacts, the other #690 scope, publication/post-release checks and main merge are still outstanding.
