## Lexer for the PromQL subset used by Skulk.

import promql_ast

type
  PromTokenKind* = enum
    ptIdent
    ptNumber
    ptDuration
    ptString
    ptOffset
    ptBy
    ptWithout
    ptLBrace
    ptRBrace
    ptLBracket
    ptRBracket
    ptLParen
    ptRParen
    ptComma
    ptEqual
    ptNotEqual
    ptRegex
    ptNotRegex
    ptPlus
    ptMinus
    ptStar
    ptSlash
    ptPercent
    ptPower
    ptEof

  PromToken* = object
    kind*: PromTokenKind
    value*: string
    span*: PromSpan

  PromLexer* = object
    input: string
    pos: int
    line: int
    column: int

proc initPromLexer*(input: string): PromLexer =
  PromLexer(input: input, line: 1, column: 1)

proc position(lex: PromLexer): PromPosition =
  PromPosition(line: lex.line, column: lex.column, offset: lex.pos)

proc peek(lex: PromLexer; lookahead = 0): char =
  let index = lex.pos + lookahead
  if index < lex.input.len:
    lex.input[index]
  else:
    '\0'

proc advance(lex: var PromLexer): char =
  result = lex.peek()
  if result == '\n':
    inc lex.line
    lex.column = 1
  else:
    inc lex.column
  inc lex.pos

proc token(lex: PromLexer; kind: PromTokenKind; value: string;
           start: PromPosition): PromToken =
  PromToken(kind: kind, value: value,
    span: PromSpan(start: start, `end`: lex.position()))

proc fail(lex: PromLexer; start: PromPosition; near, message: string) {.noreturn.} =
  raise newException(PromQlParseError,
    "PromQL parse error at line " & $start.line & ", col " & $start.column &
    ", offset " & $start.offset & " near '" & near & "': " & message)

proc skipWhitespace(lex: var PromLexer) =
  while lex.peek() in {' ', '\t', '\r', '\n'}:
    discard lex.advance()

proc readIdent(lex: var PromLexer): PromToken =
  let start = lex.position()
  var value = ""
  while lex.peek() in {'a'..'z', 'A'..'Z', '0'..'9', '_', ':'}:
    value &= lex.advance()
  let kind = case value
    of "offset": ptOffset
    of "by": ptBy
    of "without": ptWithout
    else: ptIdent
  lex.token(kind, value, start)

proc readEscapedString(lex: var PromLexer; quote: char): PromToken =
  let start = lex.position()
  discard lex.advance()
  var value = ""
  while lex.peek() != '\0':
    let c = lex.advance()
    if c == quote:
      return lex.token(ptString, value, start)
    if c == '\\':
      if lex.peek() == '\0':
        lex.fail(start, "\\", "unterminated escape sequence")
      let escaped = lex.advance()
      case escaped
      of 'n': value &= '\n'
      of 'r': value &= '\r'
      of 't': value &= '\t'
      of '\\': value &= '\\'
      of '"': value &= '"'
      of '\'': value &= '\''
      else:
        lex.fail(start, "\\" & $escaped, "unsupported escape sequence")
    else:
      value &= c
  lex.fail(start, $quote, "unterminated string literal")

proc readRawString(lex: var PromLexer): PromToken =
  let start = lex.position()
  discard lex.advance()
  var value = ""
  while lex.peek() != '\0':
    let c = lex.advance()
    if c == '`':
      return lex.token(ptString, value, start)
    value &= c
  lex.fail(start, "`", "unterminated raw string literal")

proc consumeDigits(lex: var PromLexer; value: var string) =
  while lex.peek() in {'0'..'9'}:
    value &= lex.advance()

proc readNumberOrDuration(lex: var PromLexer): PromToken =
  let start = lex.position()
  var value = ""
  if lex.peek() == '.':
    value &= lex.advance()
  lex.consumeDigits(value)
  if lex.peek() == '.' and lex.peek(1) in {'0'..'9'}:
    value &= lex.advance()
    lex.consumeDigits(value)

  if lex.peek() in {'e', 'E'}:
    value &= lex.advance()
    if lex.peek() in {'+', '-'}:
      value &= lex.advance()
    if lex.peek() notin {'0'..'9'}:
      lex.fail(start, value, "invalid numeric exponent")
    lex.consumeDigits(value)
    return lex.token(ptNumber, value, start)

  if lex.peek() in {'m', 's', 'h', 'd', 'w', 'y'}:
    while lex.peek() in {'0'..'9', 'a'..'z', 'A'..'Z', '.'}:
      value &= lex.advance()
    return lex.token(ptDuration, value, start)

  lex.token(ptNumber, value, start)

proc nextToken*(lex: var PromLexer): PromToken =
  lex.skipWhitespace()
  let start = lex.position()
  let c = lex.peek()
  case c
  of '\0':
    return lex.token(ptEof, "", start)
  of 'a'..'z', 'A'..'Z', '_', ':':
    return lex.readIdent()
  of '0'..'9':
    return lex.readNumberOrDuration()
  of '.':
    if lex.peek(1) in {'0'..'9'}:
      return lex.readNumberOrDuration()
    lex.fail(start, ".", "unexpected character")
  of '"', '\'':
    return lex.readEscapedString(c)
  of '`':
    return lex.readRawString()
  of '{':
    discard lex.advance()
    return lex.token(ptLBrace, "{", start)
  of '}':
    discard lex.advance()
    return lex.token(ptRBrace, "}", start)
  of '[':
    discard lex.advance()
    return lex.token(ptLBracket, "[", start)
  of ']':
    discard lex.advance()
    return lex.token(ptRBracket, "]", start)
  of '(':
    discard lex.advance()
    return lex.token(ptLParen, "(", start)
  of ')':
    discard lex.advance()
    return lex.token(ptRParen, ")", start)
  of ',':
    discard lex.advance()
    return lex.token(ptComma, ",", start)
  of '+':
    discard lex.advance()
    return lex.token(ptPlus, "+", start)
  of '-':
    discard lex.advance()
    return lex.token(ptMinus, "-", start)
  of '*':
    discard lex.advance()
    return lex.token(ptStar, "*", start)
  of '/':
    discard lex.advance()
    return lex.token(ptSlash, "/", start)
  of '%':
    discard lex.advance()
    return lex.token(ptPercent, "%", start)
  of '^':
    discard lex.advance()
    return lex.token(ptPower, "^", start)
  of '=':
    discard lex.advance()
    if lex.peek() == '~':
      discard lex.advance()
      return lex.token(ptRegex, "=~", start)
    return lex.token(ptEqual, "=", start)
  of '!':
    discard lex.advance()
    if lex.peek() == '=':
      discard lex.advance()
      return lex.token(ptNotEqual, "!=", start)
    if lex.peek() == '~':
      discard lex.advance()
      return lex.token(ptNotRegex, "!~", start)
    lex.fail(start, "!", "expected '=' or '~'")
  else:
    discard lex.advance()
    lex.fail(start, $c, "unexpected character")
