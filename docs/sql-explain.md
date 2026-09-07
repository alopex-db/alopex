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
