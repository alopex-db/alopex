# EXPLAIN and query introspection

## Contract at a glance

`EXPLAIN statement` plans but does not execute `statement`. `EXPLAIN ANALYZE
statement` executes it and reports total elapsed nanoseconds and result or
affected-row count. Supported output forms are human-readable text,
`EXPLAIN (FORMAT JSON)`, and the combined PostgreSQL-style
`EXPLAIN (ANALYZE, FORMAT JSON)`.

The JSON document has schema name `alopex.explain` and version `1`. Consumers
must select behavior by `schema` and `version`; new optional fields may appear
within version 1. The text format is for people and may change between Alopex
releases.

## Plan model

Alopex currently executes `LogicalPlan` directly, so `physical_plan.engine` is
`logical-direct` and its tree mirrors the logical plan. The v0.8.11 milestone
is single-node; `distributed_plan` therefore contains one local fragment. The
document also exposes the optimizer rule inventory used by this planner and
marks whether KNN pattern detection applied to this plan.

Plan nodes contain operator and relation names, but never expression values,
SQL literals, or prepared bind values. Applications must not treat EXPLAIN as
an echo or logging surface for submitted SQL.

B-tree access is adaptive: candidate materialization is bounded by 1,024 row
IDs and 8 MiB, or a smaller execution memory policy. Plain EXPLAIN appends
`adaptive=true`, the effective `max_candidates` and `max_bytes`, and
`fallback=Scan` to an eligible `IndexScan`. ANALYZE records the executed path:
`actual_path=IndexScan fallback=none`, or a `Scan` with `planned=IndexScan`
and `fallback=CandidateCap` or `fallback=ByteBudget`. A planned B-tree node
that did not run (for example, when `ExactKnnScan` performed the query) is
named `NotExecuted`, with `planned=IndexScan`, `actual_path=NotExecuted` and
zero loops. Mixed index/scan loops are named `Adaptive`, also retaining
`planned=IndexScan`. Plain EXPLAIN continues to show the planned `IndexScan`.

`fallback` summarizes every observed fallback reason (`Mixed` when both
reasons occurred); `last_fallback` describes only the last loop. Text exposes
`candidate_cap_fallbacks` and `byte_budget_fallbacks`; JSON exposes the same
counts in `actual.fallback_counts`. A later successful index loop does not
erase earlier fallback evidence. When candidate enumeration stops at the
cap, text uses `candidates>=N` and JSON sets `actual.candidates_complete=false`.
`N` is the observed lower bound, not the total number of matching rows.
Byte-budget fallback alone does not imply incomplete candidate enumeration.

Candidate and loop
counters aggregate repeated probes of the same index and predicate within
the statement, including a SELECT source inside DML. JSON exposes these
limits in `adaptive` and observed counters/path in `actual` on the physical
scan node and the single-scan `access_path`; its logical tree stays unchanged.
The asynchronous INSERT SELECT source uses the same effective memory policy
as its EXPLAIN report. Attribution of identical probes from separate CTE/view
occurrences remains tracked in #679; aggregate counters must not be read as
independent per-occurrence measurements.

## Side effects and failure boundaries

- Plain `EXPLAIN` uses a read-only transaction even when the nested statement
  is DML or DDL.
- `EXPLAIN ANALYZE` uses the nested statement's normal transaction mode and
  therefore has its normal side effects.
- Planning and execution errors are returned as errors. The enclosing
  transaction rolls back partial writes; Alopex does not turn a failed
  execution into a successful report.
- CLI cancellation and deadline checks run before ANALYZE starts and while its
  result rows are emitted. A cancellation observed before execution causes no
  side effect. The synchronous executor does not yet interrupt an operator
  already running in the current thread.

Transaction control and nested EXPLAIN statements are rejected. Query, DML,
DDL, and PRAGMA statements are accepted.

## Public surfaces

Rust callers use `Database::execute_sql`, Python callers use
`Database.execute_sql`, and the CLI accepts the same SQL through its query,
file, and stdin inputs. Every surface receives the normal one-column query
result (`QUERY PLAN` for text and `query_plan` for JSON).

## Reference behavior adopted

The syntax and execute/non-execute distinction follow PostgreSQL's
[EXPLAIN contract](https://www.postgresql.org/docs/current/sql-explain.html).
The direct JSON result follows the programmatic pattern used by
[DuckDB EXPLAIN JSON](https://github.com/duckdb/duckdb/discussions/7575).
Alopex keeps its own versioned schema because its current executor has no
separate physical-plan layer.
