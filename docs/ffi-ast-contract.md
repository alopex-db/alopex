# Alopex Query Parser FFI AST MessagePack Contract

This document defines the SQL and PromQL MessagePack payloads emitted by
`nim-sql-parser/src/alopex_sql_parser.nim`. It is the wire contract for the
Nim parser boundary.

## Contract Overview

- Current contract version: `0.5.0`, returned by `alopex_parser_version()`.
- Alopex v0.8.4 is the first release whose public producer emits the
  `CreateContinuousAggregate` variant. The variant is owned by Skulk; Alopex
  transports and validates it but does not execute the statement.
- SQL entrypoint: `alopex_parse_sql`, returning an array of SQL statements.
- PromQL entrypoint: `alopex_parse_promql`, returning one PromQL expression.
- Both parse entrypoints return `CParseResult` and allocate success/error
  buffers that the caller releases with `alopex_free_buffer`.
- A non-zero parse error is returned as `prkError`; no Nim exception crosses
  the C ABI boundary.
- Contract `0.5.0` is compatibility metadata inside the Alopex release; it is
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
report exactly `0.5.0`. A mismatch is rejected before MessagePack decoding;
callers must not attempt to interpret a payload produced by another contract.
The v0.8.2 and v0.8.3 releases remain immutable historical `0.3.0` releases:
they do not emit `CreateContinuousAggregate` and must continue to be consumed
by a `0.3.0` binding. Alopex v0.8.4-v0.8.6 remain historical `0.4.0`
releases. This document describes the current `0.5.0` surface and does not
retroactively change those releases.

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
| `Select` | `with: WithClause?`, `distinct: bool`, `projection: [SelectItem]`, `from: [FromItem]`, `selection: Expr?`, `group_by: [Expr]?`, `having: Expr?`, `set_operations: [SetOperation]`, `order_by: [OrderByExpr]`, `limit: Expr?`, `offset: Expr?` |
| `Insert` | `table: string`, `columns: [string]?`, `source: InsertSource`, `span: Span` |
| `Update` | `table: string`, `assignments: [Assignment]`, `selection: Expr?`, `span: Span` |
| `Delete` | `table: string`, `selection: Expr?`, `span: Span` |
| `CreateTable` | `if_not_exists: bool`, `name: string`, `columns: [ColumnDef]`, `constraints: [TableConstraint]`, `with_options: [IndexOption]`, `span: Span` |
| `DropTable` | `if_exists: bool`, `name: string`, `span: Span` |
| `CreateIndex` | `if_not_exists: bool`, `name: string`, `table: string`, `column: string`, `method: IndexMethod?`, `options: [IndexOption]`, `span: Span` |
| `DropIndex` | `if_exists: bool`, `name: string`, `span: Span` |
| `CreateContinuousAggregate` | `name: string`, `name_span: Span`, `query: Select`, `options: [ContinuousAggregateOption]`, `span: Span` |

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

`CommonTableExpr = { "name": string, "columns": [string], "query": Statement, "span": Span }`

`columns` preserves the declared order in `WITH c(first_name, second_name) AS
(...)`; an omitted list is encoded as an empty array. The field is an additive
part of contract `0.4.0`: current Rust consumers use an empty-list default when
reading an older payload, while older map consumers ignore the new field. It
therefore ships with the Alopex v0.8 line rather than as a separate parser
release. Nested non-recursive `WITH` queries use the same `Statement` shape.

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

`OrderByExpr = { "expr": Expr, "asc": bool?, "nulls_first": bool?, "span": Span }`

`Assignment = { "column": string, "value": Expr, "span": Span }`

## FromItem And Join

`FromItem` variants:

| Variant | Fields |
| --- | --- |
| `Table` | `name: string`, `alias: string?`, `span: Span` |
| `Join` | `left: FromItem`, `right: FromItem`, `join_type: JoinType`, `condition: Expr?`, `using: [string]?`, `span: Span` |
| `Derived` | `subquery: Statement`, `alias: string?`, `span: Span` |

`JoinType` is a string: `Inner`, `Left`, `Right`, `Full`, or `Cross`.

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
| `FunctionCall` | `name: string`, `args: [Expr]`, `distinct: bool`, `star: bool`, `over: WindowSpec?` |
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
`Lt`, `Gt`, `LtEq`, `GtEq`, `And`, `Or`, or `StringConcat`.

`UnaryOp` is a string: `Not` or `Minus`.

`Quantifier` is a string: `Any` or `All`.

### Window specifications

`WindowSpec = { "partition_by": [Expr], "order_by": [OrderByExpr], "frame": WindowFrame? }`

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
| `nkScalarSubquery` | `ExprKind.variant = "ScalarSubquery"` | `ExprKind::ScalarSubquery` |
| `nkInSubquery` | `ExprKind.variant = "InSubquery"` | `ExprKind::InSubquery` |
| `nkExists` | `ExprKind.variant = "Exists"` | `ExprKind::Exists` |
| `nkQuantified` | `ExprKind.variant = "Quantified"` | `ExprKind::Quantified` |
| `nkCase` | `ExprKind.variant = "Case"` | `ExprKind::Case` |
| `nkWindowFrame` | `WindowSpec.frame` | `WindowFrame` |
| `nkDataTypeVector` / `VECTOR(...)` type node | `DataType.variant = "Vector"` | `DataType::Vector` |
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
