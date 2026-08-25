# GROUPING SETS / ROLLUP / CUBE (issue #149)

Multi-set aggregation for `GROUP BY`: one logical `Aggregate` evaluates
several grouping sets in a single pass and reports set membership through the
`GROUPING`/`GROUPING_ID` functions. Alopex follows PostgreSQL semantics; where
PostgreSQL has no syntax (`GROUPING_ID`), DuckDB behavior is adopted.

```sql
SELECT region, product, SUM(amount) AS total,
       GROUPING(region, product) AS gid
FROM sales
GROUP BY CUBE(region, product)
ORDER BY gid, region NULLS FIRST, product NULLS FIRST;
```

## Grammar

```
group_by_clause := GROUP BY group_by_item [, group_by_item]*
group_by_item   := expr
                 | ROLLUP '(' expr [, expr]* ')'
                 | CUBE   '(' expr [, expr]* ')'
                 | GROUPING SETS '(' grouping_set [, grouping_set]* ')'
                 | '(' ')'
grouping_set    := '(' ')' | expr | '(' expr [, expr]* ')'
```

`ROLLUP`, `CUBE`, `GROUPING`, and `SETS` are contextual keywords, not reserved
words: `SELECT rollup FROM t GROUP BY rollup` still parses as a column
reference, and a `GROUPING SETS` construct is recognized only by the
`SETS` + `(` lookahead.

## Decision log

- **D1 — Grammar scope.** `GROUP BY` items are `expr`, `ROLLUP(e1, …, en)`,
  `CUBE(e1, …, en)`, `GROUPING SETS(set, …)`, and the bare empty set `()`.
  Nesting `ROLLUP`/`CUBE`/`GROUPING SETS` inside `GROUPING SETS`, and
  composite elements `(a, b)` inside `ROLLUP`/`CUBE`, are syntax errors in v1
  (flat DuckDB-style grammar; PostgreSQL's nested forms remain future work —
  the wire shape already carries arbitrary set lists, so this can be lifted
  without another contract break). `ROLLUP()`, `CUBE()`, and
  `GROUPING SETS ()` are syntax errors.
- **D2 — Mixing with ordinary keys is a cross product.** `GROUP BY a,
  ROLLUP(b, c)` produces `{a} × {(b, c), (b), ()}` — three sets. This matches
  SQL standard, PostgreSQL, and DuckDB.
- **D3 — Duplicate sets are preserved.** `GROUPING SETS ((a), (a))` emits
  each group row twice (PostgreSQL: every listed set produces rows
  independently). The executor keys each set with a distinct set-id prefix,
  so no deduplication can occur.
- **D4 — `GROUPING` returns a BIGINT bitmask.** The leftmost argument is the
  most significant bit; a bit is 1 when the argument's key is *excluded*
  (placeholder side) from the row's grouping set. `GROUPING_ID` is accepted
  as an alias (DuckDB compatibility; PostgreSQL has no such name). Zero
  arguments is an error; because the mask is a BIGINT, at most 63 arguments
  are accepted (PostgreSQL's `integer` bound is 31; ours is wider and
  documented here).
- **D5 — `GROUPING` placement.** Allowed in the SELECT list, `HAVING`,
  `ORDER BY`, and window arguments/specs of a grouped query block. Each
  argument must be a grouping expression of that query (`arguments to
  GROUPING must be grouping expressions of the query`). `GROUPING` in
  `WHERE` (`GROUPING is not allowed in WHERE`), inside `GROUP BY` keys
  (`GROUPING is not allowed in GROUP BY`), inside aggregate arguments
  (`GROUPING cannot appear inside aggregate function arguments`), or in an
  ungrouped query (`GROUPING is only allowed in grouped queries`) is a
  planner error. With a plain `GROUP BY` (no modifiers) `GROUPING` is valid
  and folds to constant `0`, matching PostgreSQL.
- **D6 — Resource bounds.** The expanded set count is capped at 4096
  (PostgreSQL-compatible): exceeding it — a 13-column `CUBE`, or a cross
  product past the cap — fails at plan time with `too many grouping sets
  (max 4096)`. The union key list is capped at 63 keys (`too many grouping
  columns (max 63)`). At execution the pre-existing group limit (1,000,000
  groups) applies to the group total *across all sets* and fails with the
  established `ResourceExhausted` error. Work is `O(rows × sets)`: every row
  accumulates once per set, so a legal 12-column `CUBE` is accepted but
  expensive by construction.
- **D7 — Placeholder NULL vs. data NULL.** Both print as SQL NULL in the
  output row; only `GROUPING`/`GROUPING_ID` separates them (SQL standard /
  PostgreSQL / DuckDB agree). Within one set, grouping on real NULL values
  behaves exactly as plain `GROUP BY` does; the set-id key prefix keeps sets
  from sharing hash groups.
- **D8 — Key expressions are column references.** Grouping-set elements
  inherit the existing `GROUP BY expressions must be column references`
  constraint; expression keys stay out of scope, matching plain `GROUP BY`.
- **D9 — Execution strategy.** With grouping sets present the plan always
  runs the single-threaded, single-pass hash `AggregateIterator` with
  set-id-prefixed keys. Parallel aggregation and spill/streaming aggregation
  are bypassed; a memory-policy overrun errors instead of spilling. Sets
  that group over no key (the `()` set of a `ROLLUP`/`CUBE`/explicit list)
  emit exactly one row even for empty input, like global aggregation.
- **D10 — FFI contract 0.13.0.** `Select.group_by` changed from `[Expr]?` to
  `[GroupByItem]?` (`variant`: `Expr`/`Rollup`/`Cube`/`GroupingSets`), so
  `PARSER_CONTRACT_VERSION` moved 0.12.0 → 0.13.0. The staged
  continuous-aggregate payload keeps its frozen `[Expr]` byte shape; the Nim
  parser rejects grouping-set modifiers inside `CREATE CONTINUOUS AGGREGATE`
  before encoding. See `docs/ffi-ast-contract.md`.
- **D11 — HAVING / ORDER BY / window composition.** Supported. The hidden
  `__grouping_id` BIGINT column sits at the end of the aggregate output
  schema and flows through the grouped-window stage; the planner rewrites
  `GROUPING(e1, …, en)` into integer arithmetic over that column
  (`((__grouping_id / 2^(K-1-i)) % 2)` terms), so `HAVING GROUPING(x) = 1`,
  `ORDER BY GROUPING(x)`, and window arguments all evaluate without executor
  support. `HAVING` runs after the grouping id joins the row, before any
  window stage.
- **D12 — Backward compatibility.** A `GROUP BY` without modifiers plans
  `grouping_sets: None` and keeps the pre-existing plan and execution paths
  bit-for-bit (no hidden column, duplicate plain keys are not deduplicated,
  parallel/spill/DISTINCT paths unchanged). Distributed reads classify
  grouping-set aggregation as `grouping_sets_local_only`
  (`docs/distributed-read.md`).

## Expansion rules

Each `GROUP BY` item contributes a list of sets; the item lists combine by
cross product in source order (D2):

| Item | Sets |
| --- | --- |
| `expr` | `{expr}` |
| `ROLLUP(e1, …, en)` | `(e1, …, en), (e1, …, en-1), …, ()` — n+1 prefixes |
| `CUBE(e1, …, en)` | all `2^n` subsets |
| `GROUPING SETS (s1, …, sm)` | the listed sets, in order |
| `()` | the single empty set |

Keys are unioned by expression identity in first-appearance order; the union
list defines both the output columns and the `GROUPING` bit positions.

## Examples

```sql
-- Real NULL (g = 0) vs. rollup placeholder (g = 1):
SELECT region, SUM(amount) AS total, GROUPING(region) AS g
FROM sales GROUP BY ROLLUP(region) ORDER BY g, region NULLS FIRST;

-- Subtotals per region plus per product plus a grand total:
SELECT region, product, COUNT(*) AS c
FROM sales GROUP BY GROUPING SETS ((region), (product), ())
ORDER BY GROUPING(region, product), region NULLS FIRST, product NULLS FIRST;

-- Empty grouping set = one row over all input:
SELECT COUNT(*), SUM(amount) FROM sales GROUP BY ();
```

## Related documents

- `docs/ffi-ast-contract.md` — `GroupByItem` wire shape, contract 0.13.0.
- `docs/distributed-read.md` — `aggregate.grouping_sets` remains local-only.
- `docs/sql-aggregate-filter-within-group.md` — aggregate clauses that
  compose with grouping sets.
