# SQL VALUES Queries

## Overview

Alopex v0.8.8 treats `VALUES` as a relational query body as well as INSERT
syntax. The feature ships with Alopex under the single Alopex version and tag;
parser contract `0.22.0` is internal compatibility metadata, not an independent
release lane.

Supported query positions are:

- top level: `VALUES (1, 'a'), (2, 'b')`;
- derived table: `FROM (VALUES ...) AS t(id, label)`;
- non-recursive CTE body: `WITH t(id) AS (VALUES (1), (2)) ...`;
- either side of `UNION`, `UNION ALL`, `INTERSECT`, or `EXCEPT`;
- scalar, `IN`, and `EXISTS` subqueries;
- the existing direct INSERT constructor: `INSERT INTO t VALUES (...)`;
- an INSERT query source ending in `VALUES`, such as
  `INSERT INTO t WITH seed(n) AS (VALUES (1)) VALUES (2)`.

`ORDER BY`, `LIMIT`, and `OFFSET` apply to the complete query expression. A
bare constructor exposes positional names `column1`, `column2`, and so on.

## Row Shape and Types

Every row must have the width of the first row. A derived-table column alias
list must have the query width, and its names must be unique. The planner
infers one common type per column:

- NULL adopts the other non-NULL row type;
- `INTEGER` and `BIGINT` widen to `BIGINT`;
- a mixed numeric column containing `FLOAT` or `DOUBLE` widens to `DOUBLE`;
- incompatible non-numeric types fail before execution.

Expressions are evaluated lazily per row in the streaming executor. Aggregate,
window, and subquery expressions remain invalid in a constructor row; scalar
expressions and scalar functions use the normal expression rules. When VALUES
is itself a subquery, scalar expressions may reference the current outer row;
materialized and streaming execution produce the same result.

## Controlled Failures

| Condition | Public code |
| --- | --- |
| Missing row or `VALUES ()` | `ALOPEX-P001` |
| Different row widths | `ALOPEX-T011` |
| Incompatible common type | `ALOPEX-T001` |
| Derived alias-list width mismatch | `ALOPEX-T012` |
| Duplicate derived alias | `ALOPEX-T007` |

Set-operation width and type failures retain `ALOPEX-T008` and
`ALOPEX-T001`. Unsupported recursive shapes remain fail-closed under the
recursive CTE contract.

## Parser Contract Migration

Contract `0.7.0` adds the `Values` statement and `QueryBody` wire variants and
changes CTE, derived-table, and set-operation query positions from a SELECT
object to a tagged `QueryBody`. A contract `0.6.0` producer or consumer must
not be relabeled: producer and consumer identifiers are checked before decode.
The four v0.8.8 native parser targets, their checksums, the Python wheels, and
the release manifest must therefore be staged and verified together.

Historical v0.8.4-v0.8.7 parser assets stay immutable. Local development may
use an explicit freshly built parser with the opt-in environment gate, but
release staging uses the strict target-record and exported-contract checks in
[`RELEASING.md`](../RELEASING.md).

## Verification Matrix

| Boundary | Evidence |
| --- | --- |
| Parser and wire | Nim parser/MessagePack suites; Rust bridge decode |
| Planner and executor | empty/one/many rows, aliases, CTE, set/tail, widening, stable errors |
| Execution modes | materialized and streaming exact-row parity, including correlation |
| Public APIs | Rust SQL, Embedded streaming, CLI JSON, Python dictionaries |
| Differential reference | DuckDB 1.5.5 and DataFusion 54.0.0 fixture |
| Release | Python and Rust v0.8 demos run by the public verifier |

Reference grammar and behavior were checked against the current
[DuckDB VALUES documentation](https://duckdb.org/docs/current/sql/query_syntax/values),
[DuckDB FROM documentation](https://duckdb.org/docs/stable/sql/query_syntax/from),
and [DataFusion SELECT documentation](https://datafusion.apache.org/user-guide/sql/select.html).
