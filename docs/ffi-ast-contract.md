# Alopex Query Parser FFI AST MessagePack Contract

This document defines the SQL and PromQL MessagePack payloads emitted by
`nim-sql-parser/src/alopex_sql_parser.nim`. It is the wire contract for the
Nim parser boundary.

## Contract Overview

- Current contract version: `0.20.0`, returned by `alopex_parser_version()`.
- Contract `0.20.0` adds transaction-control variants. `START TRANSACTION`
  normalizes to `Begin`; transaction characteristics and savepoint names are
  retained.
- Alopex v0.8.4 is the first release whose public producer emits the
  `CreateContinuousAggregate` variant. The variant is owned by Skulk; Alopex
  transports and validates it but does not execute the statement.
- SQL entrypoint: `alopex_parse_sql`, returning an array of SQL statements.
- PromQL entrypoint: `alopex_parse_promql`, returning one PromQL expression.
- Both parse entrypoints return `CParseResult` and allocate success/error
  buffers that the caller releases with `alopex_free_buffer`.
- A non-zero parse error is returned as `prkError`; no Nim exception crosses
  the C ABI boundary.
- Contract `0.20.0` is compatibility metadata inside the Alopex release; it is
  not an independent parser feature or release lane.

## Encoding Rules

- Root payload: `Statements = [Statement, ...]`.
- Object encoding: MessagePack map with string keys.
- Variant encoding: internally tagged map. Every enum-like value has a
  `"variant": "<VariantName>"` string field plus the fields listed below.
- Optional values: MessagePack `nil`.
- Sequences: MessagePack arrays.
- Field names are snake_case and match the Rust target AST field names.
- Rust side expectation: `rmp-serde` with `serde` derives, using
  `#[serde(tag = "variant")]` for enum-like wire variants and default struct
  field names for maps.

### Version and Compatibility Boundary

The linked Nim shared library, the Rust crate, and the staged payload must all
report exactly `0.20.0`. A mismatch is rejected before MessagePack decoding;
callers must not attempt to interpret a payload produced by another contract.
The v0.8.2 and v0.8.3 releases remain immutable historical `0.3.0` releases:
they do not emit `CreateContinuousAggregate` and must continue to be consumed
by a `0.3.0` binding. Alopex v0.8.4-v0.8.6 remain historical `0.4.0`
releases, and v0.8.7 remains the historical `0.5.0` release. This document
describes the current `0.20.0` surface and does not retroactively change them.

### Input, Payload, and Resource Bounds

These limits are part of the FFI contract and are enforced before unbounded
allocation or recursive decoding:

| Boundary | Limit | Controlled failure |
| --- | ---: | --- |
| SQL UTF-8 input | 1,048,576 bytes | `ALOPEX-P001`, input-too-large diagnostic |
| SQL syntactic nesting | 128 levels | `ALOPEX-P006`/bounded syntax diagnostic |
| PromQL expression nesting | 64 levels | bounded PromQL syntax diagnostic |
| MessagePack payload | 1,048,576 bytes | `ALOPEX-P001`, bounded-payload diagnostic |
| MessagePack nesting | 128 levels | `ALOPEX-P001`, depth diagnostic |
| MessagePack values | 65,536 values | `ALOPEX-P001`, collection-limit diagnostic |

The C boundary also rejects a negative length, a null input pointer, and an
interior NUL byte. The decoder rejects truncated payloads, the reserved marker
`0xc1`, trailing bytes, unknown variants, mismatched outer/kind spans, and
malformed continuous-aggregate shapes. Additive fields on map-encoded structs
are permitted only when their compatibility behavior is explicit below. A
non-zero parse result is returned as `prkError`; no Nim exception crosses the C
ABI. `ALOPEX-P007` identifies an internal Nim invariant defect and is distinct
from invalid user SQL.

## Common Types

`Location`

| Field | Type |
| --- | --- |
| `line` | integer |
| `column` | integer |

`Span`

| Field | Type |
| --- | --- |
| `start` | `Location` |
| `end` | `Location` |

## Statement

`Statement = { "kind": StatementKind, "span": Span }`

`StatementKind` variants:

| Variant | Fields |
| --- | --- |
| `Select` | `with: WithClause?`, `distinct: bool`, `distinct_on: [Expr]`, `projection: [SelectItem]`, `from: [FromItem]`, `selection: Expr?`, `group_by: [GroupByItem]?`, `having: Expr?`, `windows: [NamedWindow]`, `qualify: Expr?`, `set_operations: [SetOperation]`, `order_by: [OrderByExpr]`, `limit: Expr?`, `offset: Expr?`, `limit_with_ties: bool` |
| `Values` | `with: WithClause?`, `rows: [[Expr]]`, `set_operations: [SetOperation]`, `order_by: [OrderByExpr]`, `limit: Expr?`, `offset: Expr?`, `limit_with_ties: bool`, `span: Span` |
| `Insert` | `table: string`, `columns: [string]?`, `source: InsertSource`, `span: Span` |
| `Begin` | `isolation_level: TransactionIsolationLevel?`, `access_mode: TransactionAccessMode?` |
| `SetTransaction` | `isolation_level: TransactionIsolationLevel?`, `access_mode: TransactionAccessMode?` |
| `Commit` | none |
| `Rollback` | none |
| `Savepoint` | `name: string` |
| `RollbackToSavepoint` | `name: string` |
| `ReleaseSavepoint` | `name: string` |
| `Update` | `table: string`, `assignments: [Assignment]`, `selection: Expr?`, `span: Span` |
| `Delete` | `table: string`, `selection: Expr?`, `span: Span` |
| `CreateTable` | `if_not_exists: bool`, `name: string`, `columns: [ColumnDef]`, `constraints: [TableConstraint]`, `with_options: [IndexOption]`, `span: Span` |
| `DropTable` | `if_exists: bool`, `name: string`, `span: Span` |
| `CreateIndex` | `if_not_exists: bool`, `name: string`, `table: string`, `column: string`, `method: IndexMethod?`, `options: [IndexOption]`, `span: Span` |
| `DropIndex` | `if_exists: bool`, `name: string`, `span: Span` |
| `CreateContinuousAggregate` | `name: string`, `name_span: Span`, `query: Select`, `options: [ContinuousAggregateOption]`, `span: Span` |

`TransactionIsolationLevel` is one of `ReadUncommitted`, `ReadCommitted`,
`RepeatableRead`, or `Serializable`. `TransactionAccessMode` is `ReadOnly` or
`ReadWrite`. The wire contract preserves every parsed value; the SQL session
separately rejects isolation levels the engine cannot provide.

`CreateContinuousAggregate.query` is a nested `Select` object with an explicit
`"variant": "Select"` field. `ContinuousAggregateOption` preserves ordered
options and source locations:

`ContinuousAggregateOption = { "key": string, "key_span": Span, "value": string, "value_span": Span, "span": Span }`

The canonical v0.4.0 grammar requires exactly two options, in this order:
`retention`, then `refresh_interval`. The query must contain exactly one source
measurement and the `time_bucket(...)` grouping expression uses the contextual
identifier alias `time`. Quoted identifiers, escaped quotes, multiline text,
and inclusive source spans are preserved by the Nim lexer.

### Common Table Expressions

`WithClause = { "recursive": bool, "ctes": [CommonTableExpr], "span": Span }`

`QueryBody` variants:

| Variant | Fields |
| --- | --- |
| `Select` | the `Select` fields listed above |
| `Values` | the `Values` fields listed above |

`SetOperation = { "operator": SetOperator, "all": bool, "right": QueryBody, "span": Span }`.
`SetOperator` is `Union`, `Intersect`, or `Except`.

`CommonTableExpr = { "name": string, "columns": [string], "query": QueryBody, "span": Span }`

`columns` preserves the declared order in `WITH c(first_name, second_name) AS
(...)`; an omitted list is encoded as an empty array. The field was introduced
in contract `0.4.0`. Contract `0.7.0` changes `query` from an untagged SELECT
object to the tagged `QueryBody` union so a CTE can contain `VALUES`. This is an
intentional incompatible cutover checked before decode, and it ships inside
the Alopex v0.8.8 release rather than on a separate parser release lane.

`SelectItem` variants:

| Variant | Fields |
| --- | --- |
| `Wildcard` | `span: Span` |
| `QualifiedWildcard` | `table: string`, `span: Span` |
| `Expr` | `expr: Expr`, `alias: string?`, `span: Span` |

`InsertSource` variants:

| Variant | Fields |
| --- | --- |
| `Values` | `values: [[Expr]]` |
| `Select` | `select: Select` |
| `Query` | `query: QueryBody` |

`Select` preserves the pre-0.7 wire shape for ordinary `INSERT ... SELECT`.
`Query` carries query-body forms introduced by contract `0.7.0`, including an
`INSERT` source whose `WITH` clause ends in `VALUES`.

`OrderByExpr = { "expr": Expr, "asc": bool?, "nulls_first": bool?, "span": Span }`

`Assignment = { "column": string, "value": Expr, "span": Span }`

## FromItem And Join

`FromItem` variants:

| Variant | Fields |
| --- | --- |
| `Table` | `name: string`, `alias: string?`, `columns: [string]`, `span: Span` |
| `Join` | `left: FromItem`, `right: FromItem`, `join_type: JoinType`, `condition: Expr?`, `using: [string]?`, `span: Span` |
| `Derived` | `subquery: QueryBody`, `alias: string?`, `columns: [string]`, `lateral: bool`, `span: Span` |
| `Function` | `name: string`, `args: [Expr]`, `alias: string?`, `columns: [string]`, `lateral: bool`, `with_ordinality: bool`, `span: Span` |

`JoinType` is a string: `Inner`, `Left`, `Right`, `Full`, or `Cross`.

`columns` is the relation alias column-name list (`AS t(c1, c2)`); it is always
written and empty when the clause is absent. `lateral` records an explicit
`LATERAL` keyword. `with_ordinality` records the corresponding table-function
suffix. `Function` carries a FROM-clause table function; its `args` are ordinary
expressions and may reference earlier FROM items whether or not `lateral` is set.

## Expr And Subquery

`Expr = { "kind": ExprKind, "span": Span }`

`ExprKind` variants:

| Variant | Fields |
| --- | --- |
| `Literal` | `literal: Literal` |
| `ColumnRef` | `table: string?`, `column: string` |
| `BinaryOp` | `left: Expr`, `op: BinaryOp`, `right: Expr` |
| `UnaryOp` | `op: UnaryOp`, `operand: Expr` |
| `Case` | `operand: Expr?`, `branches: [CaseWhen]`, `else_expr: Expr?` |
| `FunctionCall` | `name: string`, `args: [Expr]`, `distinct: bool`, `star: bool`, [`order_by: [OrderByExpr]`, `within_group: [OrderByExpr]`, `filter: Expr?`,] `over: WindowSpec?` — the three bracketed aggregate-clause keys are written together only when at least one clause is present (contract `0.12.0`); clause-free calls keep the historical 6-key map |
| `Cast` | `expr: Expr`, `target_type: DataType` |
| `Between` | `expr: Expr`, `low: Expr`, `high: Expr`, `negated: bool` |
| `Like` | `expr: Expr`, `pattern: Expr`, `escape: Expr?`, `negated: bool` |
| `InList` | `expr: Expr`, `list: [Expr]`, `negated: bool` |
| `IsNull` | `expr: Expr`, `negated: bool` |
| `VectorLiteral` | `values: [float]` |
| `ScalarSubquery` | `subquery: Statement` |
| `InSubquery` | `expr: Expr`, `subquery: Statement`, `negated: bool` |
| `Exists` | `subquery: Statement`, `negated: bool` |
| `Quantified` | `expr: Expr`, `op: BinaryOp`, `quantifier: Quantifier`, `subquery: Statement` |

`CaseWhen = { "when": Expr, "then": Expr }`. A missing `operand`
denotes searched CASE; a missing `else_expr` denotes the implicit NULL result.

`Literal` variants:

| Variant | Fields |
| --- | --- |
| `Number` | `value: string` |
| `String` | `value: string` |
| `Interval` | `value: string` |
| `Boolean` | `value: bool` |
| `Null` | none |

`BinaryOp` is a string: `Add`, `Sub`, `Mul`, `Div`, `Mod`, `Eq`, `Neq`,
`Lt`, `Gt`, `LtEq`, `GtEq`, `And`, `Or`, `StringConcat`, `BitAnd`, `BitOr`,
`BitXor`, `ShiftLeft`, or `ShiftRight`.

`UnaryOp` is a string: `Not`, `Minus`, or `BitNot`.

`Quantifier` is a string: `Any` or `All`.

### Window specifications

`NamedWindow = { "name": string, "spec": WindowSpec, "span": Span }`

`WindowSpec = { "base": string?, "partition_by": [Expr], "order_by": [OrderByExpr], "frame": WindowFrame? }`

`WindowFrame = { "units": WindowFrameUnits, "start_bound": WindowFrameBound, "end_bound": WindowFrameBound }`

`WindowFrameUnits` is `Rows` or `Range`. `WindowFrameBound` is an internally
tagged variant: `UnboundedPreceding`, `Preceding(value: u64)`, `CurrentRow`,
`Following(value: u64)`, or `UnboundedFollowing`.

`frame` is introduced by contract `0.5.0`. Although the map field is additive,
an older consumer would ignore a frame emitted for newly accepted SQL and then
execute the implicit default frame, producing a silently different result.
Therefore producer and consumer contract identifiers must match before decode;
`0.4.0` and `0.5.0` are intentionally incompatible. A captured `0.4.0`
producer payload still decodes into the new AST with `frame = None`, but that
one-way migration property is not permission to load a `0.4.0` parser. See
[`sql-window-frames.md`](sql-window-frames.md) for execution semantics and the
four-target asset lifecycle.

`Select.windows`, `Select.qualify`, and `WindowSpec.base` are introduced by
contract `0.6.0`. They are not compatible with a `0.5.0` consumer: ignoring
`qualify` can return rows that SQL explicitly filters out, and ignoring `base`
can change partitioning, ordering, and frame semantics. Producer and consumer
identifiers therefore match before decoding. See
[`sql-named-window-qualify.md`](sql-named-window-qualify.md) for query-block
scope, inheritance rules, and logical evaluation order.

The staged `CreateContinuousAggregate.query` payload deliberately retains its
historical 11-field Select shape. Its validator therefore rejects top-level
`WINDOW` and `QUALIFY` clauses before encoding instead of silently omitting
their semantics. The validator also rejects `FETCH ... WITH TIES` (contract
`0.10.0`) before encoding because the frozen payload has no
`limit_with_ties` key; a plain `FETCH ... ONLY` or `OFFSET n ROWS` desugars
onto the frozen `limit`/`offset` keys. The validator likewise rejects
`DISTINCT ON` (contract `0.11.0`) because the frozen payload has no
`distinct_on` key. It also rejects aggregate `FILTER`, `WITHIN GROUP`, and
aggregate-local `ORDER BY` (contract `0.12.0`) because the frozen 6-key
`FunctionCall` map cannot express them, `ROLLUP`/`CUBE`/`GROUPING SETS`
(contract `0.13.0`) because the frozen `group_by` keeps its `[Expr]` shape,
and a table alias column list (contract `0.14.0`) because the frozen `Table`
map has no `columns` key. Ordinary public `SELECT` payloads carry the full
`0.14.0` fields described below, including `Select.group_by: [GroupByItem]`
(`0.13.0`) and the widened `FromItem` (`0.14.0`); both are mandatory,
always-written parts of the current wire.

Contract `0.7.0` introduces `StatementKind::Values` and the tagged `QueryBody`
shape for CTE, derived-table, and set-operation positions. A `0.6.0` consumer
cannot safely decode these positions and a `0.6.0` producer cannot express
them, so both directions are rejected at the exported-version gate. See
[`sql-values-query.md`](sql-values-query.md) for SQL semantics and the release
asset lifecycle.

Contract `0.8.0` adds dedicated expression variants for portable predicates:

- `Row = { "items": [Expr] }`
- `TruthPredicate = { "expr": Expr, "value": True | False | Unknown,
  "negated": bool }`
- `IsDistinctFrom = { "left": Expr, "right": Expr, "negated": bool }`

A `0.7.0` producer cannot express these nodes, so producer and consumer must
match at the exported-version gate. See
[`sql-standard-predicates.md`](sql-standard-predicates.md) for the type and
three-valued execution contract.

Contract `0.9.0` adds the dedicated
`TryCast = { "expr": Expr, "target_type": DataType }` expression variant. A
`0.8.0` producer cannot distinguish conversion failure from ordinary CAST,
so it is rejected before decode. See
[`sql-try-cast.md`](sql-try-cast.md) for the conversion matrix, error boundary,
and release lifecycle.

Contract `0.10.0` adds the `limit_with_ties: bool` field to `Select` and
`Values` (always written) and detaches `OFFSET` from `LIMIT`: `OFFSET n
[ROW | ROWS]` is accepted without a `LIMIT`, and `FETCH { FIRST | NEXT }
[count] { ROW | ROWS } { ONLY | WITH TIES }` desugars onto the `limit` key
with `limit_with_ties` set for `WITH TIES`. A `0.9.0` consumer would silently
drop tie semantics, so producer and consumer must match at the
exported-version gate. The staged continuous-aggregate payload stays frozen
and rejects `WITH TIES` before encoding. See
[`sql-fetch-pagination.md`](sql-fetch-pagination.md) for grammar, desugaring,
and execution semantics.

Contract `0.11.0` adds the `distinct_on: [Expr]` field to `Select` (always
written, empty when the clause is absent, placed after `distinct`). It carries
the `SELECT DISTINCT ON (expr, ...)` key expressions; `distinct` stays `false`
when `distinct_on` is non-empty because the grammar keeps the two forms
mutually exclusive. A `0.10.0` consumer would silently return duplicate rows,
so producer and consumer must match at the exported-version gate. The staged
continuous-aggregate payload stays frozen and rejects `DISTINCT ON` before
encoding. See [`sql-distinct-on.md`](sql-distinct-on.md) for the ORDER BY
prefix contract, determinism guarantee, and execution semantics.

Contract `0.12.0` widens `FunctionCall` for aggregate clauses (issue #148):
when any of `FILTER (WHERE ...)`, `WITHIN GROUP (ORDER BY ...)`, or an
argument-list `ORDER BY` is present, the map carries three additional keys —
`order_by: [OrderByExpr]`, `within_group: [OrderByExpr]`, and
`filter: Expr | nil` — inserted between `star` and `over`. Clause-free calls
keep the historical 6-key map, which keeps the byte-frozen staged
continuous-aggregate payload unchanged; the Rust reader treats the absent keys
as empty/none defaults. A `0.11.0` consumer would silently drop filter or
ordering semantics, so producer and consumer must match at the
exported-version gate. The staged continuous-aggregate validator rejects the
new clauses before encoding. See
[`sql-aggregate-filter-within-group.md`](sql-aggregate-filter-within-group.md)
for grammar, semantics, and the decision log.

Contract `0.13.0` changes `Select.group_by` from `[Expr]?` to
`[GroupByItem]?` (issue #149):

`GroupByItem` variants:

| Variant | Fields |
| --- | --- |
| `Expr` | `expr: Expr` |
| `Rollup` | `exprs: [Expr]` |
| `Cube` | `exprs: [Expr]` |
| `GroupingSets` | `sets: [[Expr]]` |

An ordinary `GROUP BY a, b` becomes two `Expr` items, so the item order and
expression payloads are unchanged apart from the added variant tagging. A
`0.12.0` consumer cannot decode the tagged items, so producer and consumer
must match at the exported-version gate. `GROUP BY ()` encodes as one
`GroupingSets` item with a single empty set. The staged continuous-aggregate
payload keeps its frozen `[Expr]` shape; the parser rejects ROLLUP/CUBE/
GROUPING SETS inside continuous aggregates before encoding. See
[`sql-grouping-sets.md`](sql-grouping-sets.md) for grammar, semantics, and the
decision log.

Contract `0.14.0` widens `FromItem` for LATERAL, table functions, and relation
alias column lists (issue #151): `Table` gains `columns: [string]` (always
written, empty when absent), `Derived` gains `lateral: bool`, and a new
`Function` variant carries a FROM-clause table function. A `0.13.0` consumer
cannot decode the `Function` variant and would read the extra keys as unknown
fields, so producer and consumer must match at the exported-version gate. The
staged continuous-aggregate payload keeps its frozen 4-key `Table` and 5-key
`Derived` maps; the parser's single-source rule already rejects LATERAL and
table functions there, and the staged validator rejects a table alias column
list before encoding. See
[`sql-lateral-table-functions.md`](sql-lateral-table-functions.md) for grammar,
semantics, and the decision log.

## DDL Types

`ColumnDef = { "name": string, "data_type": DataType, "constraints": [ColumnConstraint], "span": Span }`

`DataType` variants:

| Variant | Fields |
| --- | --- |
| `Integer` | none |
| `Int` | none |
| `BigInt` | none |
| `Float` | none |
| `Double` | none |
| `Text` | none |
| `Blob` | none |
| `Boolean` | none |
| `Bool` | none |
| `Timestamp` | none |
| `Date` | none |
| `Time` | none |
| `Interval` | none |
| `Decimal` | `precision: u8`, `scale: u8` |
| `Json` | none |
| `Array` | `element: DataType` |
| `Map` | `key: DataType`, `value: DataType` |
| `Struct` | `fields: [{ name: string, data_type: DataType }]` |
| `Vector` | `dimension: u32`, `metric: VectorMetric?` |

`VectorMetric` is a string: `Cosine`, `L2`, or `Inner`.

`ColumnConstraint` variants:

| Variant | Fields |
| --- | --- |
| `NotNull` | `span: Span` |
| `PrimaryKey` | `span: Span` |
| `Unique` | `span: Span` |
| `Default` | `value: Expr`, `span: Span` |

`TableConstraint` variants:

| Variant | Fields |
| --- | --- |
| `PrimaryKey` | `columns: [string]`, `span: Span` |

`IndexMethod` is a string: `BTree` or `Hnsw`.

`IndexOption = { "key": string, "value": string, "span": Span }`

## Nim To Rust Mapping

| Nim source shape | MessagePack wire shape | Rust target shape |
| --- | --- | --- |
| `nkSelect` | `Statement.kind.variant = "Select"` | `StatementKind::Select` |
| `nkJoin` / `nkFromJoin` | `FromItem.variant = "Join"` | `FromItem::Join` |
| `nkFromDerived` | `FromItem.variant = "Derived"` | `FromItem::Derived` |
| `nkFromFunction` | `FromItem.variant = "Function"` | `FromItem::Function` |
| `nkScalarSubquery` | `ExprKind.variant = "ScalarSubquery"` | `ExprKind::ScalarSubquery` |
| `nkInSubquery` | `ExprKind.variant = "InSubquery"` | `ExprKind::InSubquery` |
| `nkExists` | `ExprKind.variant = "Exists"` | `ExprKind::Exists` |
| `nkQuantified` | `ExprKind.variant = "Quantified"` | `ExprKind::Quantified` |
| `nkCase` | `ExprKind.variant = "Case"` | `ExprKind::Case` |
| `nkWindowFrame` | `WindowSpec.frame` | `WindowFrame` |
| `nkDataTypeVector` / `VECTOR(...)` type node | `DataType.variant = "Vector"` | `DataType::Vector` |
| `ARRAY` / `LIST` type node | `DataType.variant = "Array"`, recursive `element` | `DataType::Array` |
| `MAP` type node | `DataType.variant = "Map"`, recursive `key` / `value` | `DataType::Map` |
| `STRUCT` type node | `DataType.variant = "Struct"`, named recursive `fields` | `DataType::Struct` |
| `nkCreateContinuousAggregate` | `StatementKind.kind.variant = "CreateContinuousAggregate"` | `StatementKind::CreateContinuousAggregate` |

The Nim implementation uses msgpack4nim low-level map writers so Rust-facing
keys can remain `distinct`, `from`, `end`, and other Rust AST names without
using Nim reserved words as Nim identifiers.

## PromQL Payload

The PromQL root is one `PromExpr`:

`PromExpr = { "kind": PromExprKind, "span": PromSpan }`

PromQL positions use zero-based byte offsets and one-based lines/columns.
The end position is exclusive.

`PromPosition`

| Field | Type |
| --- | --- |
| `line` | integer |
| `column` | integer |
| `offset` | integer |

`PromSpan = { "start": PromPosition, "end": PromPosition }`

`PromDuration`

| Field | Type |
| --- | --- |
| `raw` | string |
| `milliseconds` | signed 64-bit integer |

Durations accept descending, non-repeated `ms/s/m/h/d/w/y` units. `y` is
365 days and `w` is 7 days. Range durations are positive; an offset duration
may have a leading sign.

`PromExprKind` variants:

| Variant | Fields |
| --- | --- |
| `VectorSelector` | `metric: string?`, `matchers: [PromLabelMatcher]`, `offset: PromDuration?` |
| `MatrixSelector` | `selector: PromExpr`, `range: PromDuration`, `offset: PromDuration?` |
| `NumberLiteral` | `value: string` |
| `StringLiteral` | `value: string` |
| `FunctionCall` | `name: string`, `args: [PromExpr]` |
| `Aggregate` | `op: string`, `expr: PromExpr`, `grouping: [string]?`, `without: bool` |
| `BinaryOp` | `left: PromExpr`, `op: PromBinaryOp`, `right: PromExpr` |
| `UnaryOp` | `op: PromUnaryOp`, `expr: PromExpr` |
| `Paren` | `expr: PromExpr` |

`PromLabelMatcher`

| Field | Type |
| --- | --- |
| `name` | string |
| `op` | `Equal`, `NotEqual`, `Regex`, or `NotRegex` |
| `value` | string |
| `span` | `PromSpan` |

`PromBinaryOp` is `Add`, `Sub`, `Mul`, `Div`, `Mod`, or `Pow`. Precedence
matches Prometheus: addition/subtraction, then multiplication/division/modulo,
then right-associative power.

`PromUnaryOp` is `Plus` or `Minus`.

The grammar entrypoint accepts metric selectors, all four label matcher
operators, range selectors, offset, function calls, `sum/avg/max/min/count`
aggregation with `by` or `without`, numeric/string literals, parentheses, and
the arithmetic operators above. Expression nesting is limited to 64. Syntax
errors report line, column, byte offset, and the nearby token.
