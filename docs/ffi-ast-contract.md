# Alopex SQL FFI AST MessagePack Contract

This document defines the MessagePack payload emitted by
`nim-sql-parser/src/alopex_sql_parser.nim` and consumed by the future Rust FFI
bridge. It is the wire contract for the Nim parser boundary.

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
| `Insert` | `table: string`, `columns: [string]?`, `values: [[Expr]]`, `span: Span` |
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
| `Expr` | `expr: Expr`, `alias: string?`, `span: Span` |

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
