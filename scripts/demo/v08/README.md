# v0.8.x embedded-local and SQL demo coverage

The v0.8 release verifier exercises positional, value, ranking, distribution,
and aggregate windows. It covers `LAG`/`LEAD`,
`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`, `NTILE`, `PERCENT_RANK`, `CUME_DIST`,
explicit `ROWS`/`RANGE` frames, named `WINDOW` inheritance, `QUALIFY`, and
composition with grouped aggregation, `HAVING`, projection, `DISTINCT`, and
outer `ORDER BY`. Both the Python and Rust
demos compare complete result sets, including physical neighbors, peer
boundaries, partition resets, defaults, NULLs, frame-local value selection,
bucket allocation, cumulative distributions, and post-window deduplication.

`demo_embedded_v08.sh` is the single Rust Embedded API walkthrough used by
post-release verification. It builds against local path dependencies during
development and against exact-version crates.io packages in the release
verifier.

| Scenario | Observable capability | Requirements/design source |
| --- | --- | --- |
| `EMB-01-storage-durability` | memory options/limits, snapshot, clone, clear, persist, file URI reopen and flush | inherited local compatibility; Phase 1 R5 |
| `EMB-02-kv-transactions` | KV CRUD, prefix scan, commit, rollback | Phase 2 R1 local Transaction compatibility |
| `EMB-03-persisted-transaction-manager` | named transaction metadata, staged reads, commit and rollback | Phase 2 R1 local multi-operation workflow |
| `EMB-04-local-sql-matrix` | DDL/DML, SELECT clauses, JOIN, subquery, aggregates, scalar/hash/encoding, TIMESTAMP, Vector SQL, PRAGMA, and v0.8.x alias/REAL/set-operation/CASE/CTE/window contracts; every query is verified by full row-value equality, not row presence | Phase 2 R1 local SQL baseline; #122-#130, #141-#144 |
| `EMB-05-catalog-cluster-diagnostics` | catalog/namespace/table/index observation, cache invalidation, single-node cluster status and routing diagnostics | Phase 1 R1/R2 local diagnostic boundary |
| `EMB-06-owned-and-sql-streams` | callback/iterator/owned SQL streams, owned transaction commit/rollback and preflight rejection | Phase 4 R1/R2 Rust backend contract |
| `EMB-07-dataframe-columnar` | SQL-to-DataFrame with FLOAT-to-Arrow-Float32 preservation, columnar projection/scan/stats/index, and fail-closed legacy-to-V08 streaming boundary | Phase 3 R1/R2; v0.8.5 #93 |
| `EMB-08-vector-hnsw` | flat Vector API, metric semantics, HNSW lifecycle and lower-is-closer public distance | inherited Vector SQL/API and v0.8.5 fix |
| `EMB-09-large-values` | chunked blob and typed-payload round trips | inherited embedded storage surface |
| `EMB-10-fail-closed-boundaries` | read-only write rejection, dimension mismatch, fenced-read unavailability, S3 feature boundary and unsupported stream preflight | Phase 1/2 prerequisite and structured rejection contracts |

The script covers every roadmap-observable capability that is callable through
the default Rust `alopex-embedded` package. The following are intentionally not
fabricated as Embedded API successes:

- authoritative multi-node metadata management and distributed reads require a
  compatible external cluster foundation; the demo instead proves single-node
  diagnostics and fenced-read rejection without local fallback;
- bounded CSV/Parquet `LazyFrame` execution belongs to `alopex-dataframe`, and
  Python sync/async/DataFrame bindings belong to `alopex`; their existing
  release demos remain separate;
- a V08 columnar stream consumes an externally provisioned chunked segment.
  Phase 3 explicitly excludes a public V08 segment writer, so the Embedded demo
  proves the required `requires_v08_chunked_layout` rejection for legacy V2;
- S3 is outside the default v0.8 local package profile and requires the `s3`
  Cargo feature and external credentials.

## v0.8.x SQL correctness fixture

`demo_sql_v08.py` and `EMB-04-local-sql-matrix` share this deliberately
non-trivial table:

```sql
CREATE TABLE sales (
  id INTEGER PRIMARY KEY,
  region TEXT,
  amount REAL,
  qty INTEGER,
  bonus REAL
);
INSERT INTO sales VALUES (1, 'east',  100.0, 3, 10.0);
INSERT INTO sales VALUES (2, 'east',  200.0, 1, NULL);
INSERT INTO sales VALUES (3, 'west',  150.0, 5, 20.0);
INSERT INTO sales VALUES (4, 'west',  150.0, 2, NULL);
INSERT INTO sales VALUES (5, 'north',  50.0, 0, 5.0);
```

The duplicate `amount = 150.0` distinguishes `UNION` from `UNION ALL` and
`RANK` from `DENSE_RANK`. The NULL bonuses exercise CASE and window aggregate
NULL handling. Set-operation asymmetry uses IDs rather than the coincidentally
nested region sets:

```text
left  (amount >= 150): {2, 3, 4}
right (qty <= 2):      {2, 4, 5}
left-only: {3}; right-only: {5}; intersection: {2, 4}
```

The Python demo performs exactly 59 result/error checks. The Rust EMB-04
scenario performs 24 v0.8.x checks: 22 exact result checks and two fail-closed
checks. The matrix grows cumulatively: CTE column aliases, recursive CTEs,
positional/value/distribution windows, explicit frames, and grouped-window
composition are all positive checks; their former unsupported-feature
rejections are gone while the inherited set-operation, CASE, CTE-shadowing,
and alias-scope coverage remains.
`check_rows` reports both expected and actual rows on failure. The nondeterministic
`NOW()` value is not claimed as an equality check; the inherited TIMESTAMP check
uses the stored constant instead. Vector-distance fixture values yield exact
distances 0 and 1 so the equality contract does not depend on approximate square
roots.

## v0.8.x SQL decisions captured by the demos

These are contract decisions, not observations inferred from whichever binary
happened to run:

1. An unqualified `ORDER BY` name resolves to a matching projection alias
   before a same-named base column. Projection aliases are also visible to
   `HAVING`, but do not leak into `WHERE` or `GROUP BY`; the latter two cases
   must fail with `ALOPEX-C003`.
2. CASE result branches use a common numeric type (`INTEGER` with `DOUBLE`
   produces `DOUBLE`). Incompatible branches such as integer and text are
   rejected with a type mismatch. Omitting ELSE produces NULL.
3. `INTERSECT` binds more tightly than `UNION`/`EXCEPT`; operators at the same
   precedence are left-associative and each operator's `ALL` applies only at
   that step. Consequently `1 UNION ALL 1 UNION 1` yields one row,
   `1 UNION 1 UNION ALL 1` yields two rows, and
   `1 UNION 2 INTERSECT 2` yields `{1, 2}`.
4. A CTE joined to a real table has ordinary bag-join multiplicity. Joining the
   `amount >= 150` CTE back to `sales` by region produces exactly
   `(1,2), (2,2), (3,3), (3,4), (4,3), (4,4)` for
   `(sales.id, cte.id)`; no implicit deduplication is allowed.
5. A CTE name shadows a same-named base table for that statement. The demo's
   `WITH sales AS (SELECT id + 100 AS id FROM sales WHERE id = 1)` must use the
   base table inside the CTE body, then shadow it outside and return 101.
6. A CTE column-name list renames its query output by position without changing
   values or types. Its length must match the query width, names must be unique,
   and quoted names preserve case while bare names use normal identifier folding.
7. `LAG(value [, offset [, default]])` and `LEAD(...)` use offset 1 and NULL by
   default. Offset and default expressions use the current row; the value uses
   the addressed row. NULL values are respected rather than skipped, and the
   default is used only when the target falls outside its partition.
8. Positional lookups use the whole partition, independently of aggregate
   frames. `ROWS` uses physical sorted positions; `RANGE` expands value bounds
   to complete peer groups and follows ASC/DESC and NULL placement. Equal
   window `ORDER BY` keys retain stable upstream order; add a unique final key
   when a ROWS statement must define a portable total order.
9. Grouped windows follow aggregate, `HAVING`, window, projection, `DISTINCT`,
   then outer `ORDER BY`. An aggregate result may feed a window argument or
   window-local ordering; `HAVING`-filtered groups never re-enter the window,
   and `DISTINCT` removes equal projected window rows only after evaluation.
10. `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE` select from the effective
    frame and preserve NULL values. `NTH_VALUE` is one-based and returns NULL
    for an empty frame or an index beyond it. `NTILE` assigns larger buckets
    first, while `PERCENT_RANK` and `CUME_DIST` use peer-aware rank and peer-end
    boundaries; a single-row partition returns `0.0` and `1.0` respectively.
11. Named windows are scoped to one query block. Forward references are
    allowed, inheritance composes partition/order/frame exactly once, cycles
    and conflicting overrides fail deterministically, and `QUALIFY` runs after
    window evaluation but before projection, `DISTINCT`, and outer `ORDER BY`.
    Projection aliases are visible to `QUALIFY`.
12. SQL NULL is represented as Python `None` and Rust `SqlValue::Null`; the
   unordered comparator retains a type tag so NULL cannot compare equal to a
   textual or numeric sentinel.
13. SQL BOOLEAN is represented as Python `bool` (`True`/`False`) and Rust
   `SqlValue::Boolean`, never integer 1/0. The Python canonicalizer is
   type-sensitive specifically because Python otherwise considers `True == 1`.
14. `VALUES` is a query body, not only INSERT syntax. Top-level constructors,
    derived tables with positional aliases, CTE bodies, set operations, and
    `ORDER BY`/`LIMIT` preserve exact row order and common column types.
15. Truth predicates are total Boolean tests, `IS DISTINCT FROM` is null-safe,
    and row equality/ordering follows SQL three-valued and left-to-right
    lexicographic rules. Row-width mismatches fail with `ALOPEX-T013`.

Recursive CTEs use the bounded fixed-point contract added for v0.8.7; invalid
recursive shapes, dependency cycles, and exhausted iteration or memory budgets
fail closed instead of falling back to a non-recursive plan. Detailed
window-frame behavior and resource limits are in
[`docs/sql-window-frames.md`](../../../docs/sql-window-frames.md).
Named-window and `QUALIFY` rules are in
[`docs/sql-named-window-qualify.md`](../../../docs/sql-named-window-qualify.md).
The constructor grammar, type rules, errors, and parser-contract migration are
in [`docs/sql-values-query.md`](../../../docs/sql-values-query.md).
Standard truth, distinctness, row comparison, and error rules are in
[`docs/sql-standard-predicates.md`](../../../docs/sql-standard-predicates.md).
