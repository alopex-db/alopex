# SQL window frames

This document is the public contract for aggregate and value window frames in
the Alopex v0.8.x release line. The SQL AST and execution semantics in
`alopex-sql` are the source of truth; Python, CLI, Embedded, demos, and parser
assets expose that same Alopex release rather than a separately versioned
SQL-parser feature lane.

## Supported syntax

`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `FIRST_VALUE`, `LAST_VALUE`, and
`NTH_VALUE` accept explicit `ROWS` and `RANGE` frames inside `OVER (...)`:

```sql
SUM(qty) OVER (
  PARTITION BY region
  ORDER BY id
  ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
)

SUM(qty) OVER (
  ORDER BY amount
  RANGE BETWEEN 50 PRECEDING AND CURRENT ROW
)

NTH_VALUE(amount, 2) OVER (
  PARTITION BY region
  ORDER BY id
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

The supported bounds are `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`,
`n FOLLOWING`, and `UNBOUNDED FOLLOWING`. The short form `ROWS n PRECEDING` (or
`RANGE n PRECEDING`) means `BETWEEN n PRECEDING AND CURRENT ROW`. Offsets are
non-negative unsigned 64-bit integer literals.

An explicit frame requires `ORDER BY`. Explicit frames on `ROW_NUMBER`, `RANK`,
`DENSE_RANK`, `NTILE`, `PERCENT_RANK`, `CUME_DIST`, `LAG`, or `LEAD` are
rejected rather than silently ignored. `LAG` and `LEAD` continue to address the
whole partition.

## Semantics

- `ROWS` counts physical rows in the window-local sorted partition.
- `RANGE CURRENT ROW` expands to every row with the complete equal sort key.
- Partition, peer, and rank equality reuse the sort comparator: signed zeroes
  are equal, NaN values are equal only to NaN values, and vectors use the same
  element-wise rule in lexicographic order.
- A numeric `RANGE n PRECEDING/FOLLOWING` requires exactly one numeric
  `ORDER BY` expression. Direction is reversed for descending order.
- A NULL current range key uses its NULL peer group for finite/current bounds;
  explicit `NULLS FIRST` or `NULLS LAST` controls where that group occurs.
- Bounds never cross a `PARTITION BY` boundary.
- A valid start position after the end position is an empty frame. `COUNT`
  returns zero; the other supported aggregates and all value functions return
  NULL.
- Aggregate `FILTER (WHERE ...)`, aggregate-local `ORDER BY`, and
  `WITHIN GROUP` are grouped-aggregation clauses (issue #148); combining any
  of them with `OVER` is a stable planner error. See
  [`sql-aggregate-filter-within-group.md`](sql-aggregate-filter-within-group.md).
- `FIRST_VALUE` and `LAST_VALUE` evaluate their value expression at the first
  or last row of the effective frame. `NTH_VALUE` is one-based, evaluates its
  index against the current row, and returns NULL when the frame has fewer than
  that many rows. A NULL, zero, negative, or non-integer index is rejected.
- `UNBOUNDED FOLLOWING` cannot start a frame, `UNBOUNDED PRECEDING` cannot end
  one, and category-reversed bounds are deterministic planning errors.

Without an explicit frame, `OVER ()` spans the whole partition. Aggregate and
value functions with `OVER (ORDER BY ...)` use
`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` and include complete peer
groups. Writing that default frame explicitly uses the same linear execution
path and has identical resource behavior.

## Resource and failure contract

The generic aggregate explicit-frame evaluator is bounded to 1,000,000 input
visits across all partitions of each planned window expression. RANGE boundary
discovery for aggregate and value functions has the same expression-wide
1,000,000-probe bound before quadratic scanning begins. The explicit default
`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is semantically identical
to the implicit ordered frame and uses the existing linear peer-aware path, so
this generic cap does not create a size-dependent semantic difference.
Exceeding either applicable limit for other frames returns `ResourceExhausted`;
checked offset arithmetic returns a deterministic window-frame overflow error.
Parser overflow, invalid bounds, wrong RANGE types, and multiple RANGE sort
keys fail without partial rows.

Window materialization also uses the query `MemoryPolicy` as one shared byte
budget. Input rows, sortable copies and keys, partition/range slots, output
slots and dynamic values, and accumulator state (including MIN/MAX dynamic
values, string aggregation buffers, and DISTINCT deduplication keys) are
charged while their lifetimes overlap. Aggregate input clones and repeated
finalize templates are charged for their temporary lifetimes as well. Retained
row payloads are released as output is drained. The window operator has no
disk-spill representation of its complete state, so it fails closed with
`ResourceExhausted` at the byte limit even when another operator could use a
configured spill directory.

Implicit whole-partition and cumulative frames retain their linear aggregate or
peer-aware value path. Explicit aggregate frame evaluation is otherwise
`O(partition_rows * average_frame_width)`; RANGE boundary discovery for
aggregate and value functions is bounded `O(partition_rows²)` and fails before
exceeding the probe cap.

## Parser contract and release assets

`WindowSpec.frame` is an additive optional map field introduced by parser
contract `0.5.0`. The identifier is bumped because wire decodability alone is
not semantic compatibility: a `0.4.0` consumer can ignore `frame` and execute
the implicit default, returning a silently wrong result for newly accepted
ROWS/RANGE SQL. Producer and consumer versions must therefore match before
decode. The contract remains compatibility metadata within the Alopex release;
it does not create an independent parser release lane.

A captured `0.4.0` Nim producer payload still proves that the new Rust AST
defaults a missing frame to `None`. In the opposite direction, the runtime
contract gate rejects a `0.5.0` producer for a `0.4.0` consumer before payload
decode; unknown-field tolerance is not treated as compatibility.

The parser binary is still release-specific. Every Alopex v0.8.x release that
contains this feature must regenerate and verify all four parser targets:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `x86_64-pc-windows-msvc`
- `x86_64-unknown-linux-gnu`

For each target the library, `CONTRACT_VERSION`, `SHA256SUMS`, and
`BUILD_IDENTITY.json` feed the parser archive, asset envelope, vendor manifest,
Python wheel native copy, and release evidence. Historical v0.8.4 contract
`0.4.0` vendor binaries and sidecars remain unchanged and are rejected by the
current `0.16.0` requirement. Alopex version, source-tree digest, archive digest,
and library digest must all match. The release verifier runs the Python and
Rust v0.8 demos with positive aggregate/value ROWS/RANGE and distribution
result checks, so a stale parser asset fails closed.

The explicit local-development override proves only a regular target library
and self-consistent `CONTRACT_VERSION`/`SHA256SUMS` sidecars. It cannot prove
that bytes relabeled as `0.16.0` export that contract. Rust therefore compares
`alopex_parser_version()` with the compiled `0.16.0` descriptor before payload
preflight or decode. Only strict four-target release staging proves asset
identity. The release workflow may use the explicit path to link a target CLI
after that same job verifies the freshly generated target record and runs its
native exported-contract smoke; this does not turn the override itself into
release evidence. Later crate staging and publication use the retargeted
manifest through the strict default resolver.

| Path/artifact | Current role | Target role | Action | Replacement/deletion condition | Proof |
| --- | --- | --- | --- | --- | --- |
| `nim-sql-parser/src/` | parser source | frame-capable parser source | keep/extend | never deleted | Nim parser and MessagePack tests |
| four vendor target directories | immutable historical v0.8.4 / 0.4.0 and v0.8.7 / 0.5.0 assets | regenerated v0.8.8 / contract 0.14.0 binaries | replace only from release staging | only after all target attestations pass; never relabel old bytes | manifest/archive/library SHA checks |
| parser asset envelope and vendor manifest | bind Alopex version to target digests | bind the frame-capable release | replace at release packaging | old manifest is invalid for the new Alopex version | release join verifier |
| Python native parser copies | wheel runtime parser | same verified target parser | replace during wheel assembly | wheel must not retain the older library digest | wheel-content verifier and Python demo |
| `demo_sql_v08.py` / embedded demo | human-readable verification | release-blocking frame results | keep/extend | no legacy frame-rejection path remains | release verifier executes both demos |
| nested `alopex-tools/v08` manifest smoke fixture | immutable v0.8.4 evidence over the checked-in historical vendor | historical audit only | preserve its 0.4.0/0.8.4 pins | superseded for current publication by the four-target release workflow smoke | fixture unit tests; release workflow loads each newly built archive |
