## SQL Lexer for Alopex SQL parser
##
## Tokenizes SQL input into a stream of tokens for the parser.

import std/[strutils, tables]

type
  TokenKind* = enum
    # Literals
    tkIdent, tkString, tkInteger, tkFloat
    # Keywords
    tkSelect, tkFrom, tkWhere, tkAnd, tkOr, tkNot
    tkInsert, tkInto, tkValues, tkUpdate, tkSet, tkDelete
    tkCreate, tkDrop, tkTable, tkAlter, tkIndex
    tkPragma
    tkJoin, tkInner, tkLeft, tkRight, tkFull, tkOuter, tkCross, tkOn
    tkNatural, tkUsing
    tkAs, tkNull, tkTrue, tkFalse
    tkOrder, tkBy, tkAsc, tkDesc, tkNulls, tkFirst, tkLast
    tkGroup, tkHaving, tkLimit, tkOffset
    tkLike, tkILike, tkGlob, tkSimilar, tkTo, tkFor, tkIn, tkBetween, tkIs, tkExists, tkAny, tkSome
    tkDistinct, tkAll, tkCast, tkCase, tkWhen, tkThen, tkElse, tkEnd, tkNow
    tkPrimary, tkKey, tkForeign, tkReferences
    tkUnique, tkCheck, tkDefault, tkConstraint, tkEscape, tkWith
    tkInt, tkBigint, tkSmallint, tkFloatType, tkDouble, tkDecimal
    tkVarchar, tkChar, tkText, tkBlob, tkBoolean, tkBool
    tkTimestamp, tkDate, tkTime, tkVector, tkInterval
    tkHnsw, tkBtree, tkCosine, tkL2
    tkIf, tkNotKw
    # Symbols
    tkStar, tkComma, tkDot, tkSemicolon
    tkLParen, tkRParen, tkLBracket, tkRBracket
    tkEq, tkNeq, tkLt, tkLe, tkGt, tkGe
    tkPlus, tkMinus, tkSlash, tkPercent, tkPipePipe
    # Special
    tkEof

  Token* = object
    kind*: TokenKind
    value*: string
    line*: int
    col*: int

  Lexer* = object
    input: string
    pos: int
    line: int
    col: int

const Keywords = {
  "select": tkSelect, "from": tkFrom, "where": tkWhere,
  "and": tkAnd, "or": tkOr, "not": tkNot,
  "insert": tkInsert, "into": tkInto, "values": tkValues,
  "update": tkUpdate, "set": tkSet, "delete": tkDelete,
  "create": tkCreate, "drop": tkDrop, "table": tkTable,
  "pragma": tkPragma,
  "alter": tkAlter, "index": tkIndex,
  "join": tkJoin, "inner": tkInner, "left": tkLeft,
  "right": tkRight, "full": tkFull, "outer": tkOuter,
  "cross": tkCross, "on": tkOn, "natural": tkNatural, "using": tkUsing,
  "as": tkAs, "null": tkNull, "true": tkTrue, "false": tkFalse,
  "order": tkOrder, "by": tkBy, "asc": tkAsc, "desc": tkDesc,
  "nulls": tkNulls, "first": tkFirst, "last": tkLast,
  "group": tkGroup, "having": tkHaving, "limit": tkLimit, "offset": tkOffset,
  "like": tkLike, "ilike": tkILike, "glob": tkGlob, "similar": tkSimilar, "to": tkTo, "for": tkFor,
  "in": tkIn, "between": tkBetween, "is": tkIs,
  "exists": tkExists, "any": tkAny, "some": tkSome,
  "distinct": tkDistinct, "all": tkAll, "cast": tkCast, "now": tkNow,
  "case": tkCase, "when": tkWhen, "then": tkThen, "else": tkElse, "end": tkEnd,
  "primary": tkPrimary, "key": tkKey, "foreign": tkForeign,
  "references": tkReferences, "unique": tkUnique, "check": tkCheck,
  "default": tkDefault, "constraint": tkConstraint, "escape": tkEscape,
  "with": tkWith,
  "int": tkInt, "integer": tkInt, "bigint": tkBigint, "smallint": tkSmallint,
  "float": tkFloatType, "double": tkDouble, "decimal": tkDecimal,
  "varchar": tkVarchar, "char": tkChar, "text": tkText,
  "blob": tkBlob, "boolean": tkBoolean, "bool": tkBool,
  "timestamp": tkTimestamp, "date": tkDate, "time": tkTime,
  "interval": tkInterval,
  "vector": tkVector, "hnsw": tkHnsw, "btree": tkBtree,
  "cosine": tkCosine, "l2": tkL2,
  "if": tkIf,
}.toTable

proc initLexer*(input: string): Lexer =
  Lexer(input: input, pos: 0, line: 1, col: 1)

proc peek(lex: Lexer): char =
  if lex.pos < lex.input.len:
    lex.input[lex.pos]
  else:
    '\0'

proc advance(lex: var Lexer): char =
  result = lex.peek()
  if result == '\n':
    inc lex.line
    lex.col = 1
  else:
    inc lex.col
  inc lex.pos

proc skipWhitespace(lex: var Lexer) =
  while lex.pos < lex.input.len:
    let c = lex.peek()
    if c in {' ', '\t', '\n', '\r'}:
      discard lex.advance()
    elif c == '-' and lex.pos + 1 < lex.input.len and lex.input[lex.pos + 1] == '-':
      # Line comment
      while lex.pos < lex.input.len and lex.peek() != '\n':
        discard lex.advance()
    elif c == '/' and lex.pos + 1 < lex.input.len and lex.input[lex.pos + 1] == '*':
      # Block comment
      discard lex.advance() # /
      discard lex.advance() # *
      while lex.pos < lex.input.len:
        if lex.peek() == '*' and lex.pos + 1 < lex.input.len and lex.input[lex.pos + 1] == '/':
          discard lex.advance() # *
          discard lex.advance() # /
          break
        discard lex.advance()
    else:
      break

proc readString(lex: var Lexer): Token =
  let startLine = lex.line
  let startCol = lex.col
  let quote = lex.advance() # consume opening quote
  var value = ""
  while lex.pos < lex.input.len:
    let c = lex.advance()
    if c == quote:
      if lex.peek() == quote:
        # Escaped quote ('')
        value &= $c
        discard lex.advance()
      else:
        break
    else:
      value &= $c
  Token(kind: tkString, value: value, line: startLine, col: startCol)

proc readNumber(lex: var Lexer): Token =
  let startLine = lex.line
  let startCol = lex.col
  var value = ""
  var isFloat = false
  while lex.pos < lex.input.len and lex.peek() in {'0'..'9'}:
    value &= $lex.advance()
  if lex.peek() == '.' and lex.pos + 1 < lex.input.len and lex.input[lex.pos + 1] in {'0'..'9'}:
    isFloat = true
    value &= $lex.advance() # dot
    while lex.pos < lex.input.len and lex.peek() in {'0'..'9'}:
      value &= $lex.advance()
  if isFloat:
    Token(kind: tkFloat, value: value, line: startLine, col: startCol)
  else:
    Token(kind: tkInteger, value: value, line: startLine, col: startCol)

proc readIdentOrKeyword(lex: var Lexer): Token =
  let startLine = lex.line
  let startCol = lex.col
  var value = ""
  while lex.pos < lex.input.len and lex.peek() in {'a'..'z', 'A'..'Z', '0'..'9', '_'}:
    value &= $lex.advance()
  let lower = value.toLowerAscii()
  if lower in Keywords:
    Token(kind: Keywords[lower], value: value, line: startLine, col: startCol)
  else:
    Token(kind: tkIdent, value: value, line: startLine, col: startCol)

proc nextToken*(lex: var Lexer): Token =
  lex.skipWhitespace()
  if lex.pos >= lex.input.len:
    return Token(kind: tkEof, value: "", line: lex.line, col: lex.col)

  let startLine = lex.line
  let startCol = lex.col
  let c = lex.peek()

  case c
  of '\'', '"':
    return lex.readString()
  of '0'..'9':
    return lex.readNumber()
  of 'a'..'z', 'A'..'Z', '_':
    return lex.readIdentOrKeyword()
  of '*':
    discard lex.advance()
    return Token(kind: tkStar, value: "*", line: startLine, col: startCol)
  of ',':
    discard lex.advance()
    return Token(kind: tkComma, value: ",", line: startLine, col: startCol)
  of '.':
    discard lex.advance()
    return Token(kind: tkDot, value: ".", line: startLine, col: startCol)
  of ';':
    discard lex.advance()
    return Token(kind: tkSemicolon, value: ";", line: startLine, col: startCol)
  of '(':
    discard lex.advance()
    return Token(kind: tkLParen, value: "(", line: startLine, col: startCol)
  of ')':
    discard lex.advance()
    return Token(kind: tkRParen, value: ")", line: startLine, col: startCol)
  of '[':
    discard lex.advance()
    return Token(kind: tkLBracket, value: "[", line: startLine, col: startCol)
  of ']':
    discard lex.advance()
    return Token(kind: tkRBracket, value: "]", line: startLine, col: startCol)
  of '+':
    discard lex.advance()
    return Token(kind: tkPlus, value: "+", line: startLine, col: startCol)
  of '-':
    discard lex.advance()
    return Token(kind: tkMinus, value: "-", line: startLine, col: startCol)
  of '/':
    discard lex.advance()
    return Token(kind: tkSlash, value: "/", line: startLine, col: startCol)
  of '%':
    discard lex.advance()
    return Token(kind: tkPercent, value: "%", line: startLine, col: startCol)
  of '|':
    discard lex.advance()
    if lex.peek() == '|':
      discard lex.advance()
      return Token(kind: tkPipePipe, value: "||", line: startLine, col: startCol)
    return Token(kind: tkIdent, value: "|", line: startLine, col: startCol)
  of '=':
    discard lex.advance()
    return Token(kind: tkEq, value: "=", line: startLine, col: startCol)
  of '<':
    discard lex.advance()
    if lex.peek() == '=':
      discard lex.advance()
      return Token(kind: tkLe, value: "<=", line: startLine, col: startCol)
    elif lex.peek() == '>':
      discard lex.advance()
      return Token(kind: tkNeq, value: "<>", line: startLine, col: startCol)
    return Token(kind: tkLt, value: "<", line: startLine, col: startCol)
  of '>':
    discard lex.advance()
    if lex.peek() == '=':
      discard lex.advance()
      return Token(kind: tkGe, value: ">=", line: startLine, col: startCol)
    return Token(kind: tkGt, value: ">", line: startLine, col: startCol)
  of '!':
    discard lex.advance()
    if lex.peek() == '=':
      discard lex.advance()
      return Token(kind: tkNeq, value: "!=", line: startLine, col: startCol)
    return Token(kind: tkIdent, value: "!", line: startLine, col: startCol)
  else:
    discard lex.advance()
    return Token(kind: tkIdent, value: $c, line: startLine, col: startCol)
