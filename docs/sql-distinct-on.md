# SELECT DISTINCT ON deterministic first-row deduplication

Alopex v0.8.x implements the PostgreSQL/DuckDB form
`SELECT DISTINCT ON (expr [, ...]) select_list ... ORDER BY ...` (issue #150).
For each group of rows whose ON key expressions compare equal, exactly one row
is returned. Unlike PostgreSQL — whose documentation calls the surviving row
"unpredictable" unless ORDER BY makes it unique — Alopex defines the winner
deterministically (D4), so results never depend on physical row order,
insertion order, or storage layout. Parser contract `0.11.0` carries the new
`distinct_on` field; it is compatibility metadata of the unified Alopex
release, not a separate parser release lane.

## Grammar

```
SELECT DISTINCT ON ( key_expr [, ...] ) select_list
FROM ...
[WHERE ...]
[ORDER BY ...]
[LIMIT ... | OFFSET ... | FETCH ...]
```

- Parentheses are mandatory and the key list holds at least one expression;
  `DISTINCT ON key` and `DISTINCT ON ()` are parse errors.
- `SELECT DISTINCT` without `ON` keeps its existing rewrite to a grouped plan
  and is unchanged.

## Planning and execution model

The planner types the ON keys (after select-list alias substitution), verifies
the ORDER BY prefix contract (D2), and synthesizes one complete sort
specification:

```
[matched ORDER BY prefix] ++ [implicit unreached keys ASC NULLS LAST]
    ++ [user ORDER BY tail] ++ [all input columns ASC NULLS LAST]
```

`LogicalPlan::DistinctOn { input, key_count, order_by }` sorts the input by
that specification (reusing the spill-capable sort, so external merge runs use
the same full comparator and stay deterministic) and emits only the first row
of each group whose leading `key_count` sort keys compare equal. The node
emits rows already ordered, so no separate Sort node is planned (D8), and
LIMIT/OFFSET apply above it, i.e. after deduplication.

## Decisions

- **D1 — Syntax and scope.** `SELECT DISTINCT ON (expr [, ...]) select_list`
  with mandatory parentheses, following PostgreSQL and DuckDB. The clause is a
  SELECT-core feature: it does not exist on `VALUES` and is independent of the
  aggregate-argument `DISTINCT` (`COUNT(DISTINCT x)`). Plain `SELECT DISTINCT`
  behavior is unchanged.
- **D2 — ORDER BY prefix contract.** When ORDER BY is present, its leading
  items must consist of ON-key expressions (compared by typed structural
  signature after alias substitution; duplicate keys deduplicate). Any
  permutation of the keys is accepted, matching PostgreSQL's
  `transformDistinctOnClause`. Two situations raise
  `error[ALOPEX-T014]: SELECT DISTINCT ON expressions must match initial ORDER
  BY expressions` (PostgreSQL 42P10 equivalent): a key-matching item appearing
  after a non-key item already ended the prefix, and an ON key the prefix
  never reached while non-key tail items exist. If ORDER BY simply runs out
  before covering every key, the unreached keys are appended as implicit
  `ASC NULLS LAST` sort keys.
- **D3 — ORDER BY may be omitted.** PostgreSQL and DuckDB accept this but
  leave the surviving row unpredictable. Alopex has no warning channel, so
  instead of warning it strengthens the semantics: keys sort implicitly
  `ASC NULLS LAST` and D4 makes the winner value-deterministic. Omission is
  not an error.
- **D4 — Tie contract (Alopex determinism extension).** The winner within a
  key group is chosen by the user's ORDER BY tail; any remaining tie resolves
  by comparing every input column of the DistinctOn input relation (the
  pre-projection base row) in schema order, `ASC NULLS LAST`. Rows identical
  in every column are interchangeable, so the result is fully
  value-deterministic and independent of physical input order. Note for the
  `ROW_NUMBER` rewrite: `QUALIFY ROW_NUMBER() OVER (PARTITION BY keys ORDER BY
  tail) = 1` is only guaranteed to match DISTINCT ON when the tail makes the
  winner unique (e.g. includes a key that is unique per row), because the
  window rewrite's tie handling depends on its own input order.
- **D5 — NULL keys.** NULLs are equal for distinctness (one row per NULL key
  group), matching PostgreSQL DISTINCT ON / `IS NOT DISTINCT FROM` grouping.
  Sort placement defaults to `NULLS LAST`, consistent with the existing ORDER
  BY default.
- **D6 — Key expression resolution.** ON keys resolve like ORDER BY items:
  select-list aliases substitute first, then the expression types against the
  FROM scope, so non-projected columns are usable. Ordinals (`DISTINCT ON
  (1)`) are not supported, matching the existing ORDER BY behavior. Aggregate
  and window functions in a key raise `InvalidExpression`; subqueries in a key
  are rejected in v1 (sort-key evaluation is row-local).
- **D7 — v1 combination scope.** DISTINCT ON combined with GROUP
  BY/aggregates/HAVING, window functions/QUALIFY, or trailing set operations
  (`UNION`/`INTERSECT`/`EXCEPT` attached to the same SELECT core) is rejected
  with `UnsupportedFeature`. PostgreSQL allows more; these are staged for a
  later version because the deduplication order interacts with those stages.
  DISTINCT ON inside subqueries, CTE bodies, derived tables, and as a nested
  set-operation operand is supported (a nested operand owns no trailing ORDER
  BY, so the prefix contract is trivially satisfied).
- **D8 — Output order and pagination.** The DistinctOn node emits rows sorted
  by the effective specification, whose leading items are exactly the user's
  ORDER BY prefix (D2 guarantees the tail only exists when every key was
  consumed), so the output always satisfies the user's ORDER BY and no extra
  Sort node is planned. LIMIT/OFFSET/FETCH apply after deduplication.
- **D9 — FFI contract.** The public `Select` map gains the always-written
  `distinct_on` key (expression array, empty when absent) directly after
  `distinct`; the fixed key count grows 14 → 15 (16 with `WITH`, which now
  exceeds the fixmap range and uses a map16 header). `PARSER_CONTRACT_VERSION`
  is bumped 0.10.0 → 0.11.0 across every pin (the plan predating the FETCH
  merge said 0.9.0 → 0.10.0 and 13 → 14 keys; the FETCH pagination commit
  already consumed those numbers). The staged continuous-aggregate payload
  keeps its frozen 12-field byte contract and rejects `DISTINCT ON` before
  encoding.
- **D10 — distinct flag exclusivity.** The parser cannot produce both
  `distinct: true` and a non-empty `distinct_on`; the planner defends against
  hand-built ASTs with `InvalidExpression`.
- **D11 — Distributed reads.** `LogicalPlan::DistinctOn` classifies as
  `distinct_on_local_only` in the v0.8 remote-read catalog (entry
  `relation.distinct_on`), keeping the conservative local-only posture.
- **D12 — Error code.** The prefix-contract violation is a planner error and
  uses the next planner type-error code `ALOPEX-T014` (the implementation plan
  suggested an `ALOPEX-P*` code, but `P` codes are parse errors; planner
  errors use the `T`/`F` series).
- **D13 — `FETCH ... WITH TIES` peer keys.** DISTINCT ON plans no `Sort` node
  (D8), so the Limit cannot read its peer specification from one. The planner
  hands it the *user's* ORDER BY instead — the leading `ORDER BY` items of the
  effective specification, which `build_distinct_on_sort_spec` always places
  first. The implicit ON keys and the all-column tie-breaker tail (D3/D4) are
  deliberately excluded: they make every surviving row unique, which would
  silently degrade `WITH TIES` to a plain `LIMIT`. `WITH TIES` with no ORDER
  BY at all stays the PostgreSQL 42P20 error `FETCH ... WITH TIES requires
  ORDER BY`. Matches PostgreSQL 16.

## Cost note and future work

The all-column tie-breaker (D4) materializes sort keys for every input column,
roughly doubling sort-key memory versus a bare ORDER BY. A comparator-fallback
design was rejected because spill-run merges compare encoded keys only, which
would break determinism under external sort. A future optimization may shorten
the tie-breaker when the planner can prove a prefix is already unique (e.g. a
PRIMARY KEY column appears in the keys or tail).

## Reference behavior

`crates/alopex-sql/tests/fixtures/distinct_on_reference.json` pins expected
rows for the shared fixture against PostgreSQL 16.14 and DuckDB 1.5.5
semantics (documented behavior; the engines are not executed in CI). Cases
whose winner PostgreSQL leaves unpredictable additionally pin the D4
determinism extension. Coverage spans the Nim parser and MessagePack tests,
`crates/alopex-sql/tests/distinct_on.rs` (T1–T16: prefix permutation and
mismatch errors, NULL groups, alias keys, tie determinism against reversed
insertion order, DISTINCT and `ROW_NUMBER`-rewrite equivalences, LIMIT after
dedupe, CTE/derived-table nesting), planner shape tests, and the Embedded,
CLI, and Python surfaces.
