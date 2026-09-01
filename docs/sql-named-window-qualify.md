# Named WINDOW and QUALIFY

Named window definitions and post-window filtering are part of the unified
Alopex v0.8.x SQL surface. The parser contract is `0.21.0`; there is no separate
SQL-parser version or release lane.

## Current contract

- `WINDOW name AS (...)` defines a window within one `SELECT` query block.
- `OVER name` uses a definition directly; `OVER (name ...)` inherits it.
- Definitions may refer forward to another definition in the same block.
- `QUALIFY` filters after all window values are computed and before projection,
  `DISTINCT`, outer `ORDER BY`, and `LIMIT`.
- Projection aliases are visible to `QUALIFY` and outer `ORDER BY`, but not to
  `WHERE` or `GROUP BY`.
- Duplicate definitions, undefined references, inheritance cycles, and
  conflicting overrides are deterministic planning errors.

The implementation and typed logical plan are the semantic source of truth.
MessagePack, Python, CLI, Embedded, demos, and release assets expose the same
behavior from the same Alopex tag.

## Syntax and inheritance

```sql
SELECT id,
       ROW_NUMBER() OVER ranked AS row_number,
       SUM(amount) OVER running AS running_total
FROM sales
WINDOW ranked AS (base ORDER BY amount DESC, id),
       running AS (
         base ORDER BY id
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ),
       base AS (PARTITION BY region)
QUALIFY row_number = 1
ORDER BY id;
```

An inherited specification follows these rules:

- `PARTITION BY` cannot be added or replaced by the derived specification.
- `ORDER BY` may be added only when the inherited specification has none.
- A frame may be added only when the inherited specification has none.
- Omitted parts are copied from the resolved base definition.
- Names compare case-insensitively inside the query block.

Each nested subquery, CTE body, and set-operation input is a new query block.
Named windows do not leak into or out of those blocks. A `QUALIFY` clause must
be Boolean and the same block must contain at least one window function; the
window may appear only in `QUALIFY` and need not be projected.

## Logical evaluation order

The planner constructs these stages for grouped and ungrouped queries:

```text
FROM/JOIN → WHERE → GROUP/aggregate → HAVING → Window
          → QUALIFY Filter → Project → DISTINCT → ORDER BY → LIMIT/OFFSET
```

This order makes a `QUALIFY` query equivalent to filtering an outer query over
a subquery that computes the same window expressions. Window expressions used
only by `QUALIFY` remain hidden from the user-facing output schema.

## Parser and release lifecycle

`Select.windows`, `Select.qualify`, and `WindowSpec.base` enter the MessagePack
AST in parser contract `0.6.0`. An older `0.5.0` consumer could ignore those map
fields and return unfiltered or differently ordered rows, so producer and
consumer identifiers must match before decoding. Relabeling a `0.5.0` binary
or sidecar is forbidden; the runtime exported-contract check rejects it.

Every Alopex release containing this contract regenerates the Linux x86_64,
Windows x86_64, macOS x86_64, and macOS arm64 parser archives from the tagged
source. The target records, archive/library digests, vendor manifest, Python
native copies, and release evidence must all identify the same Alopex version
and parser contract.

| Path or artifact | Current responsibility | Target responsibility | Action | Replacement/deletion condition | Proof |
| --- | --- | --- | --- | --- | --- |
| `nim-sql-parser/src/` | syntax and MessagePack producer | contract-0.7 parser source | keep/extend | never deleted | Nim parser and wire tests |
| Rust AST and planner | typed schema and execution order | query-block resolution and QUALIFY staging | keep/extend | never deleted | public-behavior and plan-shape tests |
| v0.8.7 / contract-0.5 assets | immutable release history | historical input only | keep | never relabel or use for current release | strict mismatch tests |
| four current parser archives | current release producer | v0.8.8 / contract-0.7 assets | replace at release staging | all target attestations pass | target record, SHA, native smoke |
| Python/CLI/Embedded demos | public behavior examples | release-blocking named-window/QUALIFY evidence | keep/extend | obsolete rejection examples are absent | exact row comparisons in release verifier |

The four-target build is a publication blocker. Local source builds and their
self-consistent sidecars are development evidence only and are deleted after
the task; they never replace tagged release assets.

## Validation matrix

| Layer | Required evidence |
| --- | --- |
| Parser | direct reference, inheritance, forward reference, multiple definitions |
| Wire | `windows`, `qualify`, and `base` round-trip with current contract `0.21.0` |
| Planner | undefined/duplicate/cycle/override errors and exact stage order |
| Executor | alias filtering, QUALIFY-only window, grouped/HAVING/DISTINCT composition |
| Public surfaces | Rust, CLI, Embedded, Python, and both v0.8 demos compare exact rows |
| Compatibility | `0.4.0` and `0.5.0` producers/consumers fail before decode |
| Release | all four archives and wheel-native copies share manifest and library digests |
