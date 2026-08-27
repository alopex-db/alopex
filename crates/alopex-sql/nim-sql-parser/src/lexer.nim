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
    tkOver, tkPartition, tkWindow, tkQualify, tkRows, tkRange
    tkGroup, tkHaving, tkLimit, tkOffset
    tkFetch, tkNext, tkTies, tkOnly, tkRow
    tkLike, tkILike, tkGlob, tkSimilar, tkTo, tkFor, tkIn, tkBetween, tkIs, tkExists, tkAny, tkSome
    tkDistinct, tkAll, tkUnion, tkIntersect, tkExcept
    tkCast, tkCase, tkWhen, tkThen, tkElse, tkEnd, tkNow
    tkPrimary, tkKey, tkForeign, tkReferences
    tkUnique, tkCheck, tkDefault, tkConstraint, tkEscape, tkWith, tkRecursive
    tkInt, tkBigint, tkSmallint, tkFloatType, tkReal, tkDouble, tkDecimal
    tkVarchar, tkChar, tkText, tkBlob, tkBoolean, tkBool
    tkTimestamp, tkDate, tkTime, tkVector, tkInterval, tkJson, tkJsonb
    tkHnsw, tkBtree, tkCosine, tkL2
    tkIf, tkNotKw
    # Symbols
    tkStar, tkComma, tkDot, tkColon, tkSemicolon
    tkLParen, tkRParen, tkLBracket, tkRBracket
    tkEq, tkNeq, tkLt, tkLe, tkGt, tkGe
    tkPlus, tkMinus, tkSlash, tkPercent, tkPipePipe
    tkArrow, tkArrowText, tkPathArrow, tkPathArrowText
    tkBitAnd, tkBitOr, tkBitXor, tkBitNot, tkShiftLeft, tkShiftRight
    tkQuestion
    # Special
    tkEof

  Token* = object
    kind*: TokenKind
    value*: string
    line*: int
    col*: int
    endLine*: int
    endCol*: int

  Lexer* = object
    input: string
    pos: int
    line: int
    col: int
    lastLine: int
    lastCol: int

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
  "over": tkOver, "partition": tkPartition, "window": tkWindow,
  "qualify": tkQualify,
  "rows": tkRows, "range": tkRange,
  "group": tkGroup, "having": tkHaving, "limit": tkLimit, "offset": tkOffset,
  "fetch": tkFetch, "next": tkNext, "ties": tkTies, "only": tkOnly, "row": tkRow,
  "like": tkLike, "ilike": tkILike, "glob": tkGlob, "similar": tkSimilar, "to": tkTo, "for": tkFor,
  "in": tkIn, "between": tkBetween, "is": tkIs,
  "exists": tkExists, "any": tkAny, "some": tkSome,
  "distinct": tkDistinct, "all": tkAll,
  "union": tkUnion, "intersect": tkIntersect, "except": tkExcept,
  "cast": tkCast, "now": tkNow,
  "case": tkCase, "when": tkWhen, "then": tkThen, "else": tkElse, "end": tkEnd,
  "primary": tkPrimary, "key": tkKey, "foreign": tkForeign,
  "references": tkReferences, "unique": tkUnique, "check": tkCheck,
  "default": tkDefault, "constraint": tkConstraint, "escape": tkEscape,
  "with": tkWith, "recursive": tkRecursive,
  "int": tkInt, "integer": tkInt, "bigint": tkBigint, "smallint": tkSmallint,
  "float": tkFloatType, "real": tkReal, "double": tkDouble,
  "decimal": tkDecimal, "numeric": tkDecimal,
  "varchar": tkVarchar, "char": tkChar, "text": tkText,
  "blob": tkBlob, "boolean": tkBoolean, "bool": tkBool,
  "timestamp": tkTimestamp, "date": tkDate, "time": tkTime,
  "interval": tkInterval,
  "json": tkJson, "jsonb": tkJsonb,
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
  lex.lastLine = lex.line
  lex.lastCol = lex.col
  if result == '\n':
    inc lex.line
    lex.col = 1
  else:
    inc lex.col
  inc lex.pos

proc makeToken(lex: Lexer; kind: TokenKind; value: string;
               startLine, startCol: int): Token =
  Token(kind: kind, value: value, line: startLine, col: startCol,
        endLine: lex.lastLine, endCol: lex.lastCol)

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
  lex.makeToken(tkString, value, startLine, startCol)

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
    lex.makeToken(tkFloat, value, startLine, startCol)
  else:
    lex.makeToken(tkInteger, value, startLine, startCol)

proc readIdentOrKeyword(lex: var Lexer): Token =
  let startLine = lex.line
  let startCol = lex.col
  var value = ""
  while lex.pos < lex.input.len and lex.peek() in {'a'..'z', 'A'..'Z', '0'..'9', '_'}:
    value &= $lex.advance()
  let lower = value.toLowerAscii()
  if lower in Keywords:
    lex.makeToken(Keywords[lower], value, startLine, startCol)
  else:
    lex.makeToken(tkIdent, value, startLine, startCol)

proc nextToken*(lex: var Lexer): Token =
  lex.skipWhitespace()
  if lex.pos >= lex.input.len:
    return Token(kind: tkEof, value: "", line: lex.line, col: lex.col,
                 endLine: lex.line, endCol: lex.col)

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
    return lex.makeToken(tkStar, "*", startLine, startCol)
  of ',':
    discard lex.advance()
    return lex.makeToken(tkComma, ",", startLine, startCol)
  of '.':
    discard lex.advance()
    return lex.makeToken(tkDot, ".", startLine, startCol)
  of ';':
    discard lex.advance()
    return lex.makeToken(tkSemicolon, ";", startLine, startCol)
  of '(':
    discard lex.advance()
    return lex.makeToken(tkLParen, "(", startLine, startCol)
  of ')':
    discard lex.advance()
    return lex.makeToken(tkRParen, ")", startLine, startCol)
  of '[':
    discard lex.advance()
    return lex.makeToken(tkLBracket, "[", startLine, startCol)
  of ']':
    discard lex.advance()
    return lex.makeToken(tkRBracket, "]", startLine, startCol)
  of '+':
    discard lex.advance()
    return lex.makeToken(tkPlus, "+", startLine, startCol)
  of '-':
    discard lex.advance()
    if lex.peek() == '>':
      discard lex.advance()
      if lex.peek() == '>':
        discard lex.advance()
        return lex.makeToken(tkArrowText, "->>", startLine, startCol)
      return lex.makeToken(tkArrow, "->", startLine, startCol)
    return lex.makeToken(tkMinus, "-", startLine, startCol)
  of '#':
    discard lex.advance()
    if lex.peek() == '>':
      discard lex.advance()
      if lex.peek() == '>':
        discard lex.advance()
        return lex.makeToken(tkPathArrowText, "#>>", startLine, startCol)
      return lex.makeToken(tkPathArrow, "#>", startLine, startCol)
    return lex.makeToken(tkIdent, "#", startLine, startCol)
  of '/':
    discard lex.advance()
    return lex.makeToken(tkSlash, "/", startLine, startCol)
  of '%':
    discard lex.advance()
    return lex.makeToken(tkPercent, "%", startLine, startCol)
  of '?':
    discard lex.advance()
    return lex.makeToken(tkQuestion, "?", startLine, startCol)
  of '|':
    discard lex.advance()
    if lex.peek() == '|':
      discard lex.advance()
      return lex.makeToken(tkPipePipe, "||", startLine, startCol)
    return lex.makeToken(tkBitOr, "|", startLine, startCol)
  of '&':
    discard lex.advance()
    return lex.makeToken(tkBitAnd, "&", startLine, startCol)
  of '^':
    discard lex.advance()
    return lex.makeToken(tkBitXor, "^", startLine, startCol)
  of '~':
    discard lex.advance()
    return lex.makeToken(tkBitNot, "~", startLine, startCol)
  of '=':
    discard lex.advance()
    return lex.makeToken(tkEq, "=", startLine, startCol)
  of ':':
    discard lex.advance()
    return lex.makeToken(tkColon, ":", startLine, startCol)
  of '<':
    discard lex.advance()
    if lex.peek() == '<':
      discard lex.advance()
      return lex.makeToken(tkShiftLeft, "<<", startLine, startCol)
    elif lex.peek() == '=':
      discard lex.advance()
      return lex.makeToken(tkLe, "<=", startLine, startCol)
    elif lex.peek() == '>':
      discard lex.advance()
      return lex.makeToken(tkNeq, "<>", startLine, startCol)
    return lex.makeToken(tkLt, "<", startLine, startCol)
  of '>':
    discard lex.advance()
    if lex.peek() == '>':
      discard lex.advance()
      return lex.makeToken(tkShiftRight, ">>", startLine, startCol)
    elif lex.peek() == '=':
      discard lex.advance()
      return lex.makeToken(tkGe, ">=", startLine, startCol)
    return lex.makeToken(tkGt, ">", startLine, startCol)
  of '!':
    discard lex.advance()
    if lex.peek() == '=':
      discard lex.advance()
      return lex.makeToken(tkNeq, "!=", startLine, startCol)
    return lex.makeToken(tkIdent, "!", startLine, startCol)
  else:
    discard lex.advance()
    return lex.makeToken(tkIdent, $c, startLine, startCol)
