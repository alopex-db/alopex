# v0.8.5 embedded-local and v0.8.6 SQL demo coverage

`demo_embedded_v085.sh` is the single Rust Embedded API walkthrough used by
post-release verification. It builds against local path dependencies during
development and against exact-version crates.io packages in the release
verifier.

| Scenario | Observable capability | Requirements/design source |
| --- | --- | --- |
| `EMB-01-storage-durability` | memory options/limits, snapshot, clone, clear, persist, file URI reopen and flush | inherited local compatibility; Phase 1 R5 |
| `EMB-02-kv-transactions` | KV CRUD, prefix scan, commit, rollback | Phase 2 R1 local Transaction compatibility |
| `EMB-03-persisted-transaction-manager` | named transaction metadata, staged reads, commit and rollback | Phase 2 R1 local multi-operation workflow |
| `EMB-04-local-sql-matrix` | DDL/DML, SELECT clauses, JOIN, subquery, aggregates, scalar/hash/encoding, TIMESTAMP, Vector SQL, PRAGMA, and v0.8.6 alias/REAL/set-operation/CASE/CTE/window contracts; every query is verified by full row-value equality, not row presence | Phase 2 R1 local SQL baseline; #122-#130 |
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

## v0.8.6 SQL correctness fixture

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

The Python demo performs exactly 52 assertions: 30 ordered comparisons, 10
unordered row-multiset comparisons, and 12 expected errors. The Rust EMB-04
scenario first converts its inherited eight queries to exact row-value checks,
then performs 18 v0.8.6 checks (12 result checks and six fail-closed checks).
The initial 13-check plan was expanded rather than deleting feature coverage:
the five added Rust checks pin CTE shadowing, the second EXCEPT direction,
explicit RANGE rejection, and both WHERE/GROUP BY alias scope boundaries
directly at the Embedded API surface. The existing CASE result check was also
expanded to pin `INTEGER`/`DOUBLE` numeric promotion without consuming another
check slot.
`check_rows` reports both expected and actual rows on failure. The nondeterministic
`NOW()` value is not claimed as an equality check; the inherited TIMESTAMP check
uses the stored constant instead. Vector-distance fixture values yield exact
distances 0 and 1 so the equality contract does not depend on approximate square
roots.

## v0.8.6 SQL decisions captured by the demos

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
   `WITH sales AS (SELECT 101 AS id)` must return 101 rather than a base-table
   row.
6. Explicit `ROWS BETWEEN` and `RANGE BETWEEN` frames are outside v0.8.6 and
   must be rejected by name. Supported implicit frames are the whole partition
   without window `ORDER BY`, and cumulative through the current row with it.
7. SQL NULL is represented as Python `None` and Rust `SqlValue::Null`; the
   unordered comparator retains a type tag so NULL cannot compare equal to a
   textual or numeric sentinel.
8. SQL BOOLEAN is represented as Python `bool` (`True`/`False`) and Rust
   `SqlValue::Boolean`, never integer 1/0. The Python canonicalizer is
   type-sensitive specifically because Python otherwise considers `True == 1`.

The following are intentionally excluded from positive v0.8.6 coverage and are
instead checked as explicit rejection contracts:

- `LAG()` and `LEAD()` require positional partition access not provided by the
  current accumulator model;
- explicit `ROWS BETWEEN` and `RANGE BETWEEN` frame specifications are not
  implemented;
- recursive CTEs remain outside the non-recursive CTE scope.
