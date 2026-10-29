# Two-index cache eviction acceptance

`hnsw_two_index_eviction` exercises public `Database` SQL transaction APIs with
two tables containing two rows each, two 2-dimensional HNSW indexes, and an
estimated byte budget large enough for either index but not their sum. The
entry limit permits both indexes, isolating the byte-budget behavior. Index
names and sizes come from `Database.hnsw_cache_stats()` after individual loads;
the test does not assume allocator sizes or private catalog naming.

Four cases alternate indexes across borrowed/owned read-only transactions,
using commit or rollback. Each first query must miss, the repeated query must
reuse the graph, and transaction completion must retain the last index within
the budget. The public stats must report one global eviction per replacement,
without counting it as committed-data invalidation or an oversized graph.

Two further cases alternate indexes within one borrowed/owned transaction.
Switching back must reload the evicted local graph, global retention remains
empty until completion, and a later transaction reuses the published survivor.
Every query checks the actual nearest row, not just counters.

These six cases are ordinary functional tests, also listed in the Windows
native runner. They use four rows total and no timing or throughput threshold.
The separate 100,000-row × 384-dimension first-write/RSS comparison remains a
manual measurement under the host storage and execution budgets. Passing
these cases proves bounded retention behavior, not the physical memory peak
or latency of large graphs.
