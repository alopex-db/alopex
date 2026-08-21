# LATERAL, table functions, and relation alias column lists (issue #151)

Three related FROM-clause features:

- `LATERAL (subquery)` — a derived table that may reference the FROM items to
  its left, re-evaluated once per left row.
- FROM-clause table functions — `UNNEST(v)` today, with `GENERATE_SERIES`
  reserved for issue #157.
- `AS t(c1, c2, …)` — a relation alias column-name list, now accepted for base
  tables, CTE references, derived tables, and table functions alike.

```sql
SELECT p.id, top.val
FROM parent AS p
CROSS JOIN LATERAL (
    SELECT c.val FROM child AS c WHERE c.parent_id = p.id
    ORDER BY c.val DESC LIMIT 1
) AS top
ORDER BY p.id;

SELECT p.id, u.unnest FROM parent AS p, UNNEST(p.emb) AS u;
```

Alopex follows PostgreSQL semantics. Where PostgreSQL and DuckDB differ, the
choice is recorded below.

## Grammar

```
from_item      := table_ref
                | '(' query ')' alias_clause
                | LATERAL '(' query ')' alias_clause
                | [LATERAL] func_name '(' [expr [, expr]*] ')' [alias_clause]
                | from_item join_op from_item [join_condition]
table_ref      := name [alias_clause]
alias_clause   := [AS] name [ '(' column_name [, column_name]* ')' ]
join_op        := ',' | [INNER] JOIN | LEFT [OUTER] JOIN | CROSS JOIN
                | RIGHT [OUTER] JOIN | FULL [OUTER] JOIN | NATURAL ...
```

`LATERAL` is a **contextual** keyword, not a reserved word (D14): it introduces
a FROM item only when a subquery or a table function follows it. A relation
named `lateral` keeps working.

## Decision log

- **D1 — Where LATERAL may appear.** `LATERAL` is accepted directly before a
  parenthesized subquery or a table function, in any FROM position: after a
  comma, after `CROSS JOIN`, after `[INNER] JOIN … ON`, and after
  `LEFT [OUTER] JOIN … ON`. It follows the PostgreSQL grammar. `LATERAL` before
  a plain table name is not a LATERAL item — see D14 for what happens instead.
- **D2 — Table functions are implicitly lateral.** A FROM-clause table function
  may reference the FROM items to its left with or without the `LATERAL`
  keyword, matching PostgreSQL and DuckDB. `FROM d, UNNEST(d.emb) AS u` is
  therefore legal as written.
- **D3 — RIGHT and FULL LATERAL are rejected.** `RIGHT JOIN LATERAL` and
  `FULL JOIN LATERAL` fail in planning with the stable code `ALOPEX-T015`. The
  correlated side is evaluated per left row, so it cannot also be the
  null-supplying side. PostgreSQL rejects the same shape; DuckDB supports only
  INNER/LEFT/CROSS. Supported: `Inner`, `Left`, `Cross`.
- **D4 — Without LATERAL the boundary stands.** A derived table without
  `LATERAL` still cannot see the enclosing FROM items; naming one is a
  resolution error. Passing the scope through unconditionally would silently
  turn a typo into a correlated reference. This is standard SQL and
  PostgreSQL.
- **D5 — The table-function registry is closed.** `UNNEST` and
  `GENERATE_SERIES` are the only names the FROM clause resolves.
  `GENERATE_SERIES` is a reserved hook: planning rejects it with a message
  naming issue #157, which is distinguishable from the unknown-function error
  (`ALOPEX-C007`, `table function 'x' does not exist`). Alopex does not follow
  PostgreSQL in letting an arbitrary scalar function appear in FROM; the
  surface stays closed.
- **D6 — `UNNEST` operates on `VECTOR`.** Exactly one argument, of type
  `VECTOR(n)` or `NULL`; anything else is `ALOPEX-T001` with expected type
  `VECTOR`. Output is one `FLOAT` column, one row per element in element
  order. `UNNEST(NULL)` and an empty vector produce zero rows, as in
  PostgreSQL. Alopex has no general `ARRAY`/`LIST` type, so `VECTOR` is the
  unnestable type; PostgreSQL unnests `anyarray` and DuckDB unnests `LIST`.
  Multi-argument `UNNEST` and `WITH ORDINALITY` are out of scope.
- **D7 — Default names.** With no alias, the relation and its column are both
  named after the function in lowercase (`unnest`), as PostgreSQL names them.
  An alias renames the relation; an alias column list also renames the column.
- **D8 — Alias column lists require exact arity.** `AS t(c1, …, cn)` is
  accepted for base tables, CTE references, derived tables, and table
  functions. `n` must equal the relation width — both too few and too many are
  `ALOPEX-T012` — and a repeated name is rejected (`ALOPEX-T007`). PostgreSQL
  permits a short list that renames only a prefix; Alopex requires the exact
  count so that every relation kind behaves like the derived-table rule that
  already shipped. Renaming affects only the relation the query sees; the
  physical scan keeps the stored column names.
- **D9 — Correlated binding reuses the correlated-subquery convention.** At
  execution the outer row of a lateral item is `left join row ++ enclosing
  outer row`. At planning the left scope is rebased to the left row's own base
  and the enclosing scope is shifted past the left row's width; the single
  `offset_scope` applied when the lateral relation is planned then supplies the
  inner width and the one nesting level. Column-index and `scope_level` rules
  are unchanged from correlated subqueries.
- **D10 — Execution is a per-left-row nested loop.** `LogicalPlan::LateralJoin`
  materializes the left input and re-executes the right sub-plan once per left
  row with that row as the outer row. Hash/equi-join strategies, plan caching,
  and hoisting a non-correlated lateral out of the loop are out of scope; cost
  is O(left rows × right cost). The node carries `right_schema`, so a
  `LEFT JOIN LATERAL` pads a left row with NULLs even when the left input is
  empty or the right side never produced a row. The `ON` condition is any
  boolean expression over the concatenated `(left, right)` row, not just
  `ON TRUE`, matching PostgreSQL and DuckDB.
- **D11 — LATERAL may be the first FROM item.** `FROM LATERAL (…) AS l, t` is
  syntactically accepted; with nothing to its left it sees only the enclosing
  query's outer scope, so a forward reference to a later item is an ordinary
  resolution error. PostgreSQL behaves the same way.
- **D12 — FFI contract 0.13.0 → 0.14.0.** `FromItem.Table` gains
  `columns: [string]`, `FromItem.Derived` gains `lateral: bool`, and a new
  `FromItem.Function` variant carries `{name, args, alias, columns, lateral}`.
  The bump follows this repository's convention of moving the minor component
  for every wire-shape change (0.9.0 → … → 0.13.0 were all minor steps); the
  patch component has never been used. The staged continuous-aggregate payload
  keeps its frozen 4-key `Table` and 5-key `Derived` maps: LATERAL and table
  functions are already rejected there by the single-source rule, and the
  staged validator rejects a table alias column list before encoding.
- **D13 — Remote reads stay local.** The v0.8 remote-read catalog rejects
  `LateralJoin` before transport (`lateral_join_not_supported_remote`,
  pre-execution rejection) and classifies `TableFunction` as local-only
  (`table_function_not_supported_remote`). Both appear in
  `docs/distributed-read-sql-matrix.json`.
- **D14 — LATERAL stays a contextual keyword.** Reserving `lateral` would break
  every existing query that uses it as a relation name, column name, or alias.
  Instead the parser treats a `lateral` identifier as the keyword only when the
  next tokens are `(` + `SELECT`/`VALUES`/`WITH`, or an identifier followed by
  `(`. This mirrors the contextual `ROLLUP`/`CUBE`/`GROUPING SETS` handling
  added in issue #149. The visible consequence is that `FROM LATERAL t`, a
  syntax error in PostgreSQL, parses in Alopex as the relation `lateral`
  aliased `t`; the diagnostic then names the missing relation `lateral` rather
  than the LATERAL misuse. This supersedes the original plan to reserve the
  word, and the CHANGELOG therefore records no breaking change.
- **D15 — Columnar filter fusion must not cross a correlation boundary.** The
  materializing pipeline fuses a `Filter` over a columnar `Scan` into the scan
  itself, which evaluates the predicate with every column index resolved
  against the scanned table. A correlated predicate also carries outer-row
  indexes past that width, so fusion would read the wrong column. Fusion is now
  used only when the predicate can be evaluated by the scan alone; a correlated
  predicate — or one holding a subquery, which needs transaction access — is
  evaluated by the `Filter` operator over a columnar scan widened to the local
  columns the predicate reads, with the out-of-range outer indexes dropped
  rather than widening the scan. The same widening fixes a latent defect for
  subquery predicates over columnar storage, which previously reached the
  in-scan evaluator. The regression test drives a `LATERAL` join whose right
  side filters a columnar table.

## Errors

| Situation | Code |
| --- | --- |
| Unknown FROM-clause function | `ALOPEX-C007` |
| `UNNEST` on a non-vector argument | `ALOPEX-T001` |
| `UNNEST` with other than one argument | `ALOPEX-T007` |
| Alias column list width mismatch | `ALOPEX-T012` |
| Alias column list repeats a name | `ALOPEX-T007` |
| `RIGHT`/`FULL JOIN LATERAL` | `ALOPEX-T015` |
| `GENERATE_SERIES` in FROM | `ALOPEX-F001` (names issue #157) |
| Outer reference from a non-LATERAL derived table | `ALOPEX-C001`/`ALOPEX-C003` |

## Engine comparison

Documented behavior of the referenced versions; a design reference, not
executed conformance evidence.

| Behavior | Alopex | PostgreSQL 17 | DuckDB 1.5.5 | DataFusion 54.0.0 |
| --- | --- | --- | --- | --- |
| `CROSS JOIN LATERAL` / `LEFT JOIN LATERAL` | yes | yes | yes | not planned |
| `RIGHT`/`FULL JOIN LATERAL` | rejected (`ALOPEX-T015`) | rejected | not supported | not planned |
| Implicit LATERAL for table functions | yes (D2) | yes | yes | yes |
| `LATERAL` reserved word | contextual (D14) | reserved | reserved | reserved |
| Unnestable type | `VECTOR` (D6) | `anyarray` | `LIST` | `List`/`LargeList` |
| Alias column list on a base table | yes, exact arity (D8) | yes, prefix allowed | yes | limited |
| Alias column list arity below width | error (D8) | allowed | allowed | — |
| `UNNEST(NULL)` | 0 rows | 0 rows | 0 rows | 0 rows |
| Non-LATERAL derived sees outer FROM | no (D4) | no | no | no |

## Performance

A lateral join re-plans and re-runs its right side for every left row. Keep the
left input small, or push the selective predicate into the left side. There is
no hoisting of a non-correlated lateral subquery and no per-row plan cache; both
are deliberately out of scope for this issue (D10).

## Related documents

- `docs/ffi-ast-contract.md` — `FromItem` wire shape, contract 0.14.0.
- `docs/distributed-read.md` — `relation.lateral_join` and
  `relation.table_function` coverage rows.
- `docs/sql-values-query.md` — derived-table alias lists that predate this
  issue.
