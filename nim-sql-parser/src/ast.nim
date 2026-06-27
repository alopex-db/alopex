## AST definitions for Alopex SQL parser
##
## C ABI compatible structures for FFI with Rust.

type
  SqlNodeKind* = enum
    nkSelect
    nkInsert
    nkUpdate
    nkDelete
    nkCreateTable
    nkDropTable
    nkIdentifier
    nkStringLit
    nkIntLit
    nkFloatLit
    nkBoolLit
    nkNull
    nkStar          ## "*" wildcard
    nkColumnRef     ## table.column or column
    nkBinaryOp
    nkUnaryOp
    nkFunctionCall
    nkAlias         ## expr AS name
    nkFromClause
    nkWhereClause
    nkOrderByClause
    nkGroupByClause
    nkHavingClause
    nkLimitClause
    nkJoin
    nkColumnDef
    nkTypeName
    nkConstraint
    nkExprList

  BinaryOpKind* = enum
    opEq, opNeq, opLt, opLe, opGt, opGe
    opAdd, opSub, opMul, opDiv, opMod
    opAnd, opOr
    opLike, opNotLike, opIn, opNotIn, opBetween, opNotBetween, opIs

  UnaryOpKind* = enum
    opNot, opNeg, opIsNull, opIsNotNull

  JoinKind* = enum
    jkInner, jkLeft, jkRight, jkFull, jkCross

  SqlNode* = ref object
    case kind*: SqlNodeKind
    of nkIdentifier, nkStringLit:
      strVal*: string
    of nkIntLit:
      intVal*: int64
    of nkFloatLit:
      floatVal*: float64
    of nkBoolLit:
      boolVal*: bool
    of nkBinaryOp:
      binOp*: BinaryOpKind
      binLeft*, binRight*: SqlNode
    of nkUnaryOp:
      unOp*: UnaryOpKind
      unOperand*: SqlNode
    of nkJoin:
      joinKind*: JoinKind
      joinLeft*, joinRight*, joinCond*: SqlNode
    of nkAlias:
      aliasExpr*: SqlNode
      aliasName*: string
    of nkColumnDef:
      colName*: string
      colType*: SqlNode
      colConstraints*: seq[SqlNode]
    else:
      children*: seq[SqlNode]

proc newNode*(kind: SqlNodeKind): SqlNode =
  SqlNode(kind: kind)

proc newIdent*(name: string): SqlNode =
  SqlNode(kind: nkIdentifier, strVal: name)

proc newStringLit*(val: string): SqlNode =
  SqlNode(kind: nkStringLit, strVal: val)

proc newIntLit*(val: int64): SqlNode =
  SqlNode(kind: nkIntLit, intVal: val)

proc newFloatLit*(val: float64): SqlNode =
  SqlNode(kind: nkFloatLit, floatVal: val)

proc newBoolLit*(val: bool): SqlNode =
  SqlNode(kind: nkBoolLit, boolVal: val)

proc newNull*(): SqlNode =
  SqlNode(kind: nkNull)

proc newStar*(): SqlNode =
  SqlNode(kind: nkStar)

proc newBinaryOp*(op: BinaryOpKind, left, right: SqlNode): SqlNode =
  SqlNode(kind: nkBinaryOp, binOp: op, binLeft: left, binRight: right)

proc newUnaryOp*(op: UnaryOpKind, operand: SqlNode): SqlNode =
  SqlNode(kind: nkUnaryOp, unOp: op, unOperand: operand)

proc `$`*(node: SqlNode): string =
  ## Debug representation of AST node
  if node == nil:
    return "nil"
  case node.kind
  of nkIdentifier:
    result = "Ident(" & node.strVal & ")"
  of nkStringLit:
    result = "Str('" & node.strVal & "')"
  of nkIntLit:
    result = "Int(" & $node.intVal & ")"
  of nkFloatLit:
    result = "Float(" & $node.floatVal & ")"
  of nkBoolLit:
    result = "Bool(" & $node.boolVal & ")"
  of nkNull:
    result = "NULL"
  of nkStar:
    result = "*"
  of nkBinaryOp:
    result = "BinOp(" & $node.binOp & ", " & $node.binLeft & ", " & $node.binRight & ")"
  of nkUnaryOp:
    result = "UnaryOp(" & $node.unOp & ", " & $node.unOperand & ")"
  of nkAlias:
    result = "Alias(" & $node.aliasExpr & " AS " & node.aliasName & ")"
  of nkColumnDef:
    result = "ColDef(" & node.colName & " " & $node.colType & ")"
  else:
    result = $node.kind & "("
    for i, child in node.children:
      if i > 0: result &= ", "
      result &= $child
    result &= ")"
