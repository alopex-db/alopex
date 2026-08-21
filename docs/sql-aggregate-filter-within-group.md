# Aggregate FILTER, ordered aggregates, and WITHIN GROUP

Issue #148 adds three aggregate clauses to Alopex SQL (parser contract
`0.12.0`):

```sql
-- Per-aggregate row filtering (SQL:2003 T612)
agg(args) FILTER (WHERE predicate)

-- Aggregate-local ordering inside the argument list
STRING_AGG(name, ',' ORDER BY v DESC, name ASC)
GROUP_CONCAT(name ORDER BY v NULLS FIRST)

-- Ordered-set aggregate
PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY sort_expr)
```

All three clauses may be combined with each other and with `GROUP BY` /
`HAVING`; `HAVING` recognizes filtered/ordered aggregates that also appear in
the plan. The v0.8 distributed-read catalog classifies every new form as
local-only (`aggregate_filter_local_only` / `ordered_aggregate_local_only`).

## Semantics

- `FILTER (WHERE p)`: rows where `p` is not `TRUE` (i.e. `FALSE` or NULL) are
  skipped before the accumulator — and before any `DISTINCT` deduplication —
  sees them. `COUNT` over an all-excluded group returns `0`; `SUM`/`AVG`/...
  return NULL.
- Aggregate-local `ORDER BY` orders the aggregated values of
  order-sensitive aggregates (`GROUP_CONCAT`, `STRING_AGG`). Ties are decided
  by the remaining sort keys; `ASC`/`DESC` and `NULLS FIRST`/`LAST` follow the
  ordinary ORDER BY rules (default `ASC NULLS LAST`).
- `PERCENTILE_DISC(f) WITHIN GROUP (ORDER BY e)` sorts the non-NULL values of
  `e`, then returns the element at `max(ceil(f * n) - 1, 0)` — the first value
  whose cumulative distribution reaches `f`. An empty (or fully filtered)
  group returns NULL.

## Reference engines

| Behaviour | Alopex | PostgreSQL 16 | DuckDB 1.5.5 | DataFusion 54 |
|---|---|---|---|---|
| `FILTER` before `DISTINCT` | yes | yes | yes | yes |
| `COUNT(*) FILTER (WHERE FALSE)` | `0` | `0` | `0` | `0` |
| `SUM(x ORDER BY y)` (order-insensitive) | validated, then discarded | executes the sort | validated, then discarded (documented) | validated, then discarded |
| `DISTINCT` + `ORDER BY` key not in args | error | error | accepted | error |
| `PERCENTILE_DISC` | yes | yes | yes | not supported |
| `FILTER` on window aggregates | explicit error | supported | supported | not supported |
| `WITHIN GROUP` + `OVER` | error | error | error | n/a |

The pinned row-level fixture lives in
`crates/alopex-sql/tests/fixtures/aggregate_filter_reference.json`
(`cargo test -p alopex-sql --features lane_ci --test aggregate_filter_ordered`).

## Decisions

- **D1** — `agg(x) FILTER (WHERE p)` is supported on every existing aggregate
  (`COUNT`/`SUM`/`TOTAL`/`AVG`/`MIN`/`MAX`/`GROUP_CONCAT`/`STRING_AGG`) and on
  the new `PERCENTILE_DISC`. Rows whose predicate is not `TRUE` (`FALSE` or
  NULL) are excluded before `DISTINCT` deduplication — identical to
  PostgreSQL 16 and DuckDB 1.5.5 (SQL:2003 T612).
- **D2** — Combining the new clauses with `OVER` is parsed and transported
  through the FFI, then rejected by the planner with a stable error:
  `FILTER ... OVER` is `unsupported_feature` ("future": PostgreSQL allows it,
  but the Alopex window frame executor does not implement per-frame filtering
  yet), while `WITHIN GROUP ... OVER` and aggregate `ORDER BY ... OVER` are
  `invalid_expression` (PostgreSQL rejects both).
- **D3** — Aggregate-local `ORDER BY` is parsed, name-resolved, and
  type-checked on every aggregate, but only order-sensitive aggregates
  (`GROUP_CONCAT`, `STRING_AGG`, ordered-set aggregates) execute it.
  Order-insensitive aggregates discard the validated ordering (result
  unchanged, sort cost avoided) — DuckDB's documented behaviour; PostgreSQL
  executes the redundant sort but returns the same rows.
- **D4** — `DISTINCT` with aggregate `ORDER BY` requires every sort expression
  to appear in the argument list ("in an aggregate with DISTINCT, ORDER BY
  expressions must appear in the argument list") — the PostgreSQL rule,
  stricter than DuckDB, keeping post-deduplication sort keys well-defined.
- **D5** — The WITHIN GROUP foundation ships with one ordered-set aggregate to
  prove it: `PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)`. The
  fraction must be a numeric literal in `[0, 1]`; NULL sort values are
  excluded; an empty group yields NULL; selection is
  `index = max(ceil(f * n) - 1, 0)` over the sorted values — PostgreSQL
  semantics. `PERCENTILE_CONT` / `MODE` remain for issue #154, which only
  needs new `AggregateFunction` variants and accumulators on the same
  ordered-input path.
- **D6** — Misuse is rejected by the planner with stable messages: plain
  aggregate + `WITHIN GROUP` → "WITHIN GROUP is only valid for ordered-set
  aggregate functions"; `PERCENTILE_DISC` without `WITHIN GROUP` → "WITHIN
  GROUP (ORDER BY ...) is required for PERCENTILE_DISC"; `WITHIN GROUP` +
  `DISTINCT` → error (PostgreSQL); argument `ORDER BY` combined with
  `WITHIN GROUP` → Nim parse error.
- **D7** — The FILTER predicate must be BOOLEAN (or NULL). Aggregates inside
  the predicate → "aggregate functions are not allowed in FILTER" (PostgreSQL
  wording); window functions → error; subqueries are `unsupported_feature`
  in v1 (aligned with the existing aggregate-argument restriction; PostgreSQL
  allows them, documented as a future extension).
- **D8** — `FILTER` and `WITHIN` are not reserved words (no lexer change).
  The parser consumes them as clauses only when the one-token lookahead sees
  `(` / `GROUP`, so `SELECT count(x) filter FROM t` still parses as an
  implicit alias. A query that used `filter`/`within` as an implicit alias
  directly followed by `(`/`GROUP` must now write `AS` (breaking note in the
  CHANGELOG) — PostgreSQL's `col_name_keyword` treatment.
- **D9** — `COUNT(*) FILTER (WHERE p)` counts rows where `p` is `TRUE` (0 for
  empty/fully-excluded groups); `DISTINCT` + `FILTER` applies filter → then
  distinct. All reference engines agree.
- **D10** — Aggregates that differ only in `filter` or `order_by` are distinct
  physical aggregates. Both `AggregateSignature` definitions
  (`planner/mod.rs` and `planner/type_checker.rs`) carry `filter_key` and
  `order_key` (Debug-string identity, matching `expr_key`); `order_key` is
  populated only for order-sensitive aggregates so a discarded `ORDER BY`
  (D3) still deduplicates with the unordered spelling. The `PERCENTILE_DISC`
  fraction participates through the separator slot and the sort value through
  `order_key`.
- **D11** — Parallel/distributed boundary: `FILTER` applies per input row and
  commutes with Partial/Final splitting, so filtered aggregates may run in
  parallel. Ordered aggregates (non-empty `order_by`, `PERCENTILE_DISC`)
  force Single mode via `should_use_single_for_parallel`; their accumulators
  reject `state()`/`merge()` with `invalid_aggregate_state`. The v0.8
  remote-read catalog classifies both clause families as local-only
  (`aggregate_filter_local_only`, `ordered_aggregate_local_only`) with rows
  in `docs/distributed-read-sql-matrix.json`.
- **D12** — FFI contract bump `0.11.0` → `0.12.0` (the plan predates the
  FETCH `0.10.0` and DISTINCT ON `0.11.0` bumps; minor-bump procedure is
  unchanged). The `FunctionCall` wire map stays at its historical 6 keys when
  no aggregate clause is present — required because the byte-frozen staged
  continuous-aggregate payload shares the expression writer — and grows to 9
  keys (`order_by: [OrderByExpr]`, `within_group: [OrderByExpr]`,
  `filter: Expr|nil`) whenever any clause is present. The Rust reader takes
  absent keys as defaults (`#[serde(default)]`), and the staged
  continuous-aggregate validator rejects the new clauses before encoding.
- **D13** — Columnar projection pushdown covers the whole `FunctionCall`. A
  columnar scan materializes exactly the columns the pushed projection names
  and fills every other column with `NULL`, and the planner installs the
  SELECT projection into the `Scan` node even for aggregate queries. A column
  that appears only inside a `FILTER` predicate, an aggregate-local
  `ORDER BY`, or an `OVER (...)` partition/order key is therefore collected
  alongside the arguments; missing one was silent, not an error
  (`SUM(v) FILTER (WHERE flag > 0)` returned `NULL` because `flag` read as
  `NULL` in every row).

## Public error surface

All planner rejections surface as `ALOPEX-T007` (invalid expression),
`ALOPEX-T001` (type mismatch, e.g. non-BOOLEAN FILTER), or `ALOPEX-F001`
(unsupported feature, e.g. `FILTER ... OVER`, subquery in FILTER) with the
messages listed under D6/D7 — no internal `TypedExpr` debug output leaks to
CLI/Python surfaces.

## Window boundary

`WindowFunction::Aggregate` shares `AggregateExpr` with grouped aggregation.
The planner rejects the clause/`OVER` combinations first (D2); the window
executor additionally guards against filtered/ordered aggregates reaching a
frame evaluation, so a future planner regression fails loudly instead of
silently ignoring a `FILTER`. See `docs/sql-window-frames.md`.

## Extension points for issue #154

- `AggregateFunction::PercentileDisc { fraction }` → add `PercentileCont`,
  `Mode` variants.
- `PercentileDiscAccumulator` → siblings reuse `update_ordered` buffering and
  the `compare_ordered_keys` sort; only `finalize` differs (interpolation for
  `PERCENTILE_CONT`, most-frequent-value for `MODE`).
- `is_ordered_set_aggregate_name` (type checker) and
  `is_order_sensitive_aggregate` (planner) are the only registries to extend.
