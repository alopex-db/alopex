# Alopex Query Parser FFI AST MessagePack Contract

This document defines the SQL and PromQL MessagePack payloads emitted by
`nim-sql-parser/src/alopex_sql_parser.nim`. It is the wire contract for the
Nim parser boundary.

## Contract Overview

- Contract version: `0.3.0`, returned by `alopex_parser_version()`.
- SQL entrypoint: `alopex_parse_sql`, returning an array of SQL statements.
- PromQL entrypoint: `alopex_parse_promql`, returning one PromQL expression.
- Both parse entrypoints return `CParseResult` and allocate success/error
  buffers that the caller releases with `alopex_free_buffer`.
- A non-zero parse error is returned as `prkError`; no Nim exception crosses
  the C ABI boundary.

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
| `Select` | `distinct: bool`, `projection: [SelectItem]`, `from: [FromItem]`, `selection: Expr?`, `group_by: [Expr]?`, `having: Expr?`, `order_by: [OrderByExpr]`, `limit: Expr?`, `offset: Expr?` |
| `Insert` | `table: string`, `columns: [string]?`, `source: InsertSource`, `span: Span` |
| `Update` | `table: string`, `assignments: [Assignment]`, `selection: Expr?`, `span: Span` |
| `Delete` | `table: string`, `selection: Expr?`, `span: Span` |
| `CreateTable` | `if_not_exists: bool`, `name: string`, `columns: [ColumnDef]`, `constraints: [TableConstraint]`, `with_options: [IndexOption]`, `span: Span` |
| `DropTable` | `if_exists: bool`, `name: string`, `span: Span` |
| `CreateIndex` | `if_not_exists: bool`, `name: string`, `table: string`, `column: string`, `method: IndexMethod?`, `options: [IndexOption]`, `span: Span` |
| `DropIndex` | `if_exists: bool`, `name: string`, `span: Span` |

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
| `FunctionCall` | `name: string`, `args: [Expr]`, `distinct: bool`, `star: bool` |
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
| `nkDataTypeVector` / `VECTOR(...)` type node | `DataType.variant = "Vector"` | `DataType::Vector` |

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
