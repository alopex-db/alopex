# Alopex v0.8 Distributed Read SQL Coverage

This matrix is generated from the closed `RemoteReadCatalogV0_8` contract. A `local_only` row never permits an implicit local fallback from a cluster profile.

| ID | Public SQL surface | Remote status | Prerequisite | Normal outcome | Rejection/failure |
| --- | --- | --- | --- | --- | --- |
| `select.one_table.read_only` | One-table SELECT with projection, WHERE, ORDER BY, LIMIT, OFFSET | `remote_supported` | Closed catalog and fenced retained read point | Prepared globally equivalent result | Classified failure; no local fallback |
| `select.aggregate.basic` | COUNT, SUM, TOTAL, AVG, MIN, MAX, GROUP BY, HAVING, DISTINCT | `remote_supported` | Closed aggregate descriptor and global finalization budget | Prepared aggregate result | Classified failure |
| `select.aggregate.ordered_string` | GROUP_CONCAT, STRING_AGG | `remote_supported` | Ordered raw-value finalization budget | Prepared ordered aggregate result | Classified failure |
| `scalar.deterministic` | Explicit deterministic scalar list | `remote_supported` | Explicit v0.8 function identity | Prepared remote read | Unlisted function rejected before transport |
| `scalar.json_local_only` | JSON and JSONB scalar functions | `local_only` | Local execution profile | JSON/JSONB evaluation by the local executor | `json_local_only` before transport |
| `scalar.temporal_local_only` | Temporal scalar functions | `local_only` | Local execution profile | Temporal evaluation by the local executor | `temporal_local_only` before transport |
| `scalar.local_only` | JSON, nested, full-text, temporal, vector, random/UUID, statistics, and cache-control scalar functions | `local_only` | Local execution profile | local executor behavior | Explicit local-only classification |
| `statement.ddl` | CREATE/DROP TABLE, CREATE/DROP INDEX | `pre_execution_rejection` | Local schema workflow | v0.7.4 local behavior | `ddl_not_supported_remote` |
| `statement.dml` | INSERT, UPDATE, DELETE | `pre_execution_rejection` | Local transaction workflow | v0.7.4 local behavior | `dml_not_supported_remote` |
| `statement.pragma` | PRAGMA | `local_only` | Local execution profile | v0.7.4 local behavior | `pragma_local_only` |
| `relation.join` | JOIN | `pre_execution_rejection` | Local execution profile | v0.7.4 local behavior | `join_not_supported_remote` |
| `relation.subquery` | Scalar, IN, EXISTS, quantified subqueries | `pre_execution_rejection` | Local execution profile | v0.7.4 local behavior | `subquery_not_supported_remote` |
| `relation.compound_window` | Compound and window forms | `pre_execution_rejection` | Future catalog version | Not a v0.8 remote-read form | `function_not_in_remote_catalog` |
| `scalar.standard_predicate` | truth, distinctness, and row-value predicates | `local_only` | local execution profile | three-valued evaluation by the local executor | `standard_predicate_local_only` before transport |
| `scalar.try_cast` | TRY_CAST safe conversion | `local_only` | local execution profile | NULL-on-conversion-failure evaluation by the local executor | `try_cast_local_only` before transport |
| `pagination.fetch_with_ties` | FETCH ... WITH TIES peer-preserving row limits | `local_only` | local execution profile | peer-preserving limit evaluation by the local executor | `fetch_with_ties_local_only` before transport |
| `relation.distinct_on` | SELECT DISTINCT ON deterministic first-row deduplication | `local_only` | local execution profile | deterministic per-key first-row evaluation by the local executor | `distinct_on_local_only` before transport |
| `scalar.aggregate_filter` | Aggregate FILTER (WHERE ...) per-aggregate row filtering | `local_only` | local execution profile | per-aggregate predicate filtering by the local executor | `aggregate_filter_local_only` before transport |
| `scalar.ordered_aggregate` | Aggregate-local ORDER BY and WITHIN GROUP ordered-set aggregates (PERCENTILE_DISC) | `local_only` | local execution profile | ordered aggregate evaluation by the local executor | `ordered_aggregate_local_only` before transport |
| `scalar.nested_aggregate` | ARRAY_AGG and JSON/JSONB collection aggregates | `local_only` | local execution profile | nested collection aggregation by the local executor | `nested_aggregate_local_only` before transport |
| `aggregate.grouping_sets` | GROUPING SETS / ROLLUP / CUBE multi-set aggregation and GROUPING/GROUPING_ID | `local_only` | local execution profile | single-pass multi-set aggregation by the local executor | `grouping_sets_local_only` before transport |
| `relation.lateral_join` | LATERAL joins over a correlated relation | `pre_execution_rejection` | Local execution profile | Per-left-row correlated evaluation by the local executor | `lateral_join_not_supported_remote` before transport |
| `relation.table_function` | FROM-clause table functions (UNNEST, GENERATE_SERIES, FTS_SEARCH) | `local_only` | Local execution profile | Row generation by the local executor | `table_function_not_supported_remote` before transport |
| `relation.recursive_cte` | Recursive common table expressions | `pre_execution_rejection` | Local execution profile | Bounded local fixed-point evaluation | `recursive_cte_not_supported_remote` |
| `transaction.multi_statement` | Existing Transaction API workflow | `local_only` | Local transaction workflow | v0.7.4 local transaction behavior | Explicit pre-execution classification |

The catalog contains the explicit scalar identities. Adding a scalar function elsewhere does not make it remote-supported.

## CLI routing and terminal status

The existing forms remain `alopex sql [QUERY]` and `alopex sql --file FILE`.
`--read-mode {local,inherit,strong,stale}` and `--routing-report {human,json}`
do not add a second SQL command. `local` remains the compatibility default.

Remote candidates require a profile with `execution_scope = "cluster"`, a
server endpoint, and a `[cluster_read]` block. A cluster profile's
`permitted_read_modes` limits `strong` and `stale` overrides; its
`default_read_mode` is used for `inherit` or an omitted option. The server's
committed policy remains authoritative. A profile that is only a legacy server
profile remains local, and a cluster profile never falls back to its optional
local path after a connection failure.

Rows keep their selected table/JSON/CSV/TSV/JSONL format on stdout. When
requested, the routing report is emitted only to stderr: `human` is a readable
field list and `json` is one versioned JSON object. It reports requested and
effective mode, decision, range/freshness values when available, retry/failover
counts, and a terminal outcome. Unknown or unavailable coordinator evidence is
left absent rather than synthesized from local state.

| Terminal outcome | Exit code | Meaning |
| --- | ---: | --- |
| `success` | 0 | Prepared distributed result completed successfully. |
| `unsupported` | 5 | Query form, remote DDL/DML/transaction, or protocol form is outside the closed catalog. |
| `authorization_failure` | 4 | Caller or delegation authorization was rejected. |
| `retryable_failure` | 3 | A request/target/read-point failure may be retried with compatible evidence. |
| `terminal_failure` | 1 | Capability/prerequisite is unavailable or the failure is not retryable. |
| `cancelled` | 130 | Client or server cancellation reached the registered execution. |

In particular, `capability_unavailable` is a classified terminal failure, not
permission to run the SQL through a local route.
