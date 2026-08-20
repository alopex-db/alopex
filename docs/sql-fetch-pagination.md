# FETCH FIRST/NEXT, OFFSET n ROWS, and WITH TIES pagination

Alopex v0.8.8 supports the SQL-standard pagination tail on every SQL surface
(Rust, CLI, Embedded, Python): `OFFSET n [ROW | ROWS]`,
`FETCH { FIRST | NEXT } [count] { ROW | ROWS } { ONLY | WITH TIES }`, and
constant-expression counts for `LIMIT`/`OFFSET`/`FETCH` (issue #152). Parser
contract `0.10.0` carries the new `limit_with_ties` field; it is compatibility
metadata inside the unified Alopex release, not a separate parser lane.

## Grammar and desugaring

```
query_tail := { LIMIT (ALL | count) | OFFSET count [ROW | ROWS] | FETCH ... }
FETCH      := FETCH (FIRST | NEXT) [count] (ROW | ROWS) (ONLY | WITH TIES)
```

- `FETCH { FIRST | NEXT } [count] { ROW | ROWS } ONLY` desugars to
  `LIMIT count`; an omitted count defaults to 1. `FIRST`/`NEXT` and
  `ROW`/`ROWS` are pure synonyms.
- `FETCH ... WITH TIES` desugars to the same limit plus a `ties` marker that
  carries a copy of the ORDER BY sort keys into the plan's `Limit` node.
- `OFFSET n [ROW | ROWS]` is independent of `LIMIT` and may appear alone.
- `LIMIT ALL` and `LIMIT NULL` / `FETCH FIRST NULL ROWS ...` mean "no limit";
  `OFFSET NULL` means "no offset".

The logical plan keeps the existing `Limit { limit: Option<u64>, offset:
Option<u64> }` contract with concrete plan-time values, so the distributed
plan contract carries resolved numbers, never expressions. WITH TIES adds
`ties: Option<Vec<SortExpr>>` to that node and is classified `local_only`
(`fetch_with_ties_local_only`) by the v0.8 remote-read catalog; plain
FETCH/OFFSET desugar onto the already remote-supported limit/offset operators.

## WITH TIES semantics

After the limit is exhausted, rows keep flowing while their full ORDER BY key
tuple compares equal to the final counted row's key (its peers). NULL keys are
peers of NULL keys. Rows discarded by OFFSET never revive, even when they are
peers of the boundary row. `FETCH FIRST 0 ROWS WITH TIES` returns no rows.
The peer scan re-evaluates the sort keys once per emitted row; a future
optimization may hand the keys from Sort to Limit, but correctness never
depends on it.

## Decisions

- **D1**: `FETCH { FIRST | NEXT } [count] { ROW | ROWS } ONLY` desugars to
  `LIMIT count` (count omission = 1). SQL standard, PostgreSQL, and DuckDB all
  agree that FIRST/NEXT and ROW/ROWS are synonyms.
- **D2**: The pagination tail accepts at most one limit-setting clause (LIMIT
  or FETCH) and at most one OFFSET, in any order, matching the PostgreSQL
  grammar (`LIMIT n OFFSET m`, `OFFSET m LIMIT n`, `OFFSET m FETCH ...`,
  `FETCH ... OFFSET m`). Duplicates fail with `multiple LIMIT clauses are not
  allowed` / `multiple OFFSET clauses are not allowed`, following PostgreSQL's
  "multiple LIMIT/OFFSET clauses not allowed". This amends the original plan's
  three-form list to the exact PostgreSQL acceptance set.
- **D3**: `WITH TIES` requires ORDER BY; without it planning fails with
  `FETCH ... WITH TIES requires ORDER BY` (PostgreSQL 42P20 equivalent). Peer
  comparison uses every ORDER BY key with the sort comparator, so NULL = NULL
  is a peer match. OFFSET-discarded rows never revive. DuckDB 1.5.5 does not
  implement WITH TIES, so all tie semantics follow PostgreSQL 16.
- **D4**: LIMIT/OFFSET/FETCH counts accept constant expressions (literals,
  arithmetic, CAST, CASE, deterministic scalar functions over constants) and
  are const-folded at plan time. Column references fail empty-scope name
  resolution; subqueries are rejected with `subquery in LIMIT`/`OFFSET`
  (PostgreSQL allows them; scope decision for this issue); aggregate and
  window functions are rejected explicitly. The count is evaluated exactly
  once at plan time, so any function result is frozen into the plan before
  routing.
- **D5**: The count type must be an integer type (SMALLINT/INTEGER/BIGINT
  resolve to Integer/BigInt). FLOAT/DOUBLE/TEXT/BOOLEAN and other types fail
  with a `BIGINT` type mismatch, matching PostgreSQL's "argument of LIMIT must
  be type bigint" (DuckDB also requires integers).
- **D6**: NULL handling follows PostgreSQL: `LIMIT NULL` and
  `FETCH FIRST NULL ROWS ...` mean no limit, `OFFSET NULL` means offset 0.
  `LIMIT ALL` is accepted and produces no limit.
- **D7**: Negative counts fail at plan time with `LIMIT must not be negative`
  / `OFFSET must not be negative` (PostgreSQL; SQLite's negative-means-
  unlimited is deliberately not followed). Values that do not fit in a
  non-negative 64-bit range (for example `LIMIT 9223372036854775808`) fail
  with a bounded `LIMIT expression is invalid: ...` diagnostic.
- **D8**: The `?` placeholder is a lexer token that fails in expression
  position with `bind parameters are not yet supported; pass literal values
  instead (prepared statements are tracked by issue #166)` instead of the
  previous confusing column-not-found. The Python surface's documented
  `execute_sql(sql, params)` text substitution keeps working (`LIMIT ?` with
  params) and can be replaced by true binds under issue #166. `$n` notation
  is out of scope.
- **D9**: The FETCH count accepts any parser expression (a superset of
  PostgreSQL's literal-or-parenthesized restriction; the planner's constant
  rule still bounds what executes). `ONLY`/`WITH TIES` cannot be omitted, per
  the standard. `FETCH ... PERCENT` (DuckDB/SQL Server extension) fails with
  `FETCH ... PERCENT is not supported`.
- **D10**: Without ORDER BY, LIMIT/OFFSET/FETCH select an arbitrary n rows —
  the same indeterminate-order contract as PostgreSQL. Tests assert only row
  counts for unordered pagination.
- **D11**: Distributed read: `FETCH ... WITH TIES` is `local_only` with
  rejection code `fetch_with_ties_local_only` and coverage id
  `pagination.fetch_with_ties`. Plain FETCH/OFFSET desugar to limit/offset and
  stay inside `select.one_table.read_only`'s remote-supported surface.
- **D12**: FFI contract: `Select`/`Values` wire maps always write
  `limit_with_ties: bool`; `PARSER_CONTRACT_VERSION` is bumped 0.9.0 → 0.10.0.
  The byte-frozen staged continuous-aggregate payload keeps its historical 12
  fields; a continuous-aggregate query containing WITH TIES is rejected before
  staging (`staged continuous aggregate query cannot contain FETCH ... WITH
  TIES`), following the WINDOW/QUALIFY precedent.
- **D13**: FETCH/WITH TIES inside a recursive CTE body joins the existing
  rejection, now reading `ORDER BY, LIMIT, OFFSET, or FETCH inside a recursive
  common table expression`.
- **D14**: The KNN top-k optimizer never fires for a `ties` limit (the exact
  peer set requires the full sort), and embedded streaming rejects WITH TIES
  (`FETCH ... WITH TIES is not streamable`; ordered pagination was already not
  streamable).

## Reference behavior

| Case | Alopex | PostgreSQL 16.14 | DuckDB 1.5.5 |
| --- | --- | --- | --- |
| `FETCH FIRST n ROWS ONLY` | = `LIMIT n` | same | same |
| `OFFSET n ROWS` without LIMIT | accepted | accepted | accepted |
| `FETCH ... WITH TIES` | peers kept via ORDER BY keys | same | not implemented |
| `LIMIT NULL` / `OFFSET NULL` | no limit / no offset | same | same |
| `LIMIT -1` | error | error | error |
| count expression | constant expressions, folded at plan time | expressions incl. subqueries | constant expressions |
| `LIMIT ?` (bind) | dedicated parse error (#166) | prepared statements | prepared statements |

Pinned row-level expectations live in
`crates/alopex-sql/tests/fixtures/fetch_pagination_reference.json`
(PostgreSQL 16.14 / DuckDB 1.5.5, documentation-pinned; engines are not
executed in CI).
