module [tokenize, Token, TokenKind]

## SQL Lexer for Alopex SQL parser (Roc implementation)
## Tokenizes SQL input into a list of tokens.

TokenKind : [
    # Literals
    TkIdent, TkString, TkInteger, TkFloat,
    # Keywords
    TkSelect, TkFrom, TkWhere, TkAnd, TkOr, TkNot,
    TkInsert, TkInto, TkValues, TkUpdate, TkSet, TkDelete,
    TkCreate, TkDrop, TkTable,
    TkJoin, TkInner, TkLeft, TkRight, TkFull, TkOuter, TkCross, TkOn,
    TkAs, TkNull, TkTrue, TkFalse,
    TkOrder, TkBy, TkAsc, TkDesc,
    TkGroup, TkHaving, TkLimit, TkOffset,
    TkLike, TkIn, TkBetween, TkIs, TkExists,
    TkDistinct, TkPrimary, TkKey, TkNot_, TkUnique, TkDefault,
    TkInt, TkVarchar, TkText, TkBoolean,
    TkIf,
    # Symbols
    TkStar, TkComma, TkDot, TkSemicolon,
    TkLParen, TkRParen,
    TkEq, TkNeq, TkLt, TkLe, TkGt, TkGe,
    TkPlus, TkMinus, TkSlash, TkPercent,
    # Special
    TkEof,
]

Token : { kind : TokenKind, value : Str, line : U64, col : U64 }

## Resolve keyword from lowercase string
resolveKeyword : Str -> [Keyword TokenKind, NotKeyword]
resolveKeyword = |word|
    when word is
        "select" -> Keyword(TkSelect)
        "from" -> Keyword(TkFrom)
        "where" -> Keyword(TkWhere)
        "and" -> Keyword(TkAnd)
        "or" -> Keyword(TkOr)
        "not" -> Keyword(TkNot)
        "insert" -> Keyword(TkInsert)
        "into" -> Keyword(TkInto)
        "values" -> Keyword(TkValues)
        "update" -> Keyword(TkUpdate)
        "set" -> Keyword(TkSet)
        "delete" -> Keyword(TkDelete)
        "create" -> Keyword(TkCreate)
        "drop" -> Keyword(TkDrop)
        "table" -> Keyword(TkTable)
        "join" -> Keyword(TkJoin)
        "inner" -> Keyword(TkInner)
        "left" -> Keyword(TkLeft)
        "right" -> Keyword(TkRight)
        "full" -> Keyword(TkFull)
        "outer" -> Keyword(TkOuter)
        "cross" -> Keyword(TkCross)
        "on" -> Keyword(TkOn)
        "as" -> Keyword(TkAs)
        "null" -> Keyword(TkNull)
        "true" -> Keyword(TkTrue)
        "false" -> Keyword(TkFalse)
        "order" -> Keyword(TkOrder)
        "by" -> Keyword(TkBy)
        "asc" -> Keyword(TkAsc)
        "desc" -> Keyword(TkDesc)
        "group" -> Keyword(TkGroup)
        "having" -> Keyword(TkHaving)
        "limit" -> Keyword(TkLimit)
        "offset" -> Keyword(TkOffset)
        "like" -> Keyword(TkLike)
        "in" -> Keyword(TkIn)
        "between" -> Keyword(TkBetween)
        "is" -> Keyword(TkIs)
        "exists" -> Keyword(TkExists)
        "distinct" -> Keyword(TkDistinct)
        "primary" -> Keyword(TkPrimary)
        "key" -> Keyword(TkKey)
        "unique" -> Keyword(TkUnique)
        "default" -> Keyword(TkDefault)
        "int" -> Keyword(TkInt)
        "integer" -> Keyword(TkInt)
        "varchar" -> Keyword(TkVarchar)
        "text" -> Keyword(TkText)
        "boolean" -> Keyword(TkBoolean)
        "if" -> Keyword(TkIf)
        _ -> NotKeyword

## Tokenize SQL input string into a list of tokens
tokenize : Str -> Result (List Token) Str
tokenize = |input|
    bytes = Str.to_utf8(input)
    tokenizeHelper(bytes, 0, 1, 1, [])

tokenizeHelper : List U8, U64, U64, U64, List Token -> Result (List Token) Str
tokenizeHelper = |bytes, pos, line, col, tokens|
    if pos >= List.len(bytes) then
        Ok(List.append(tokens, { kind: TkEof, value: "", line, col }))
    else
        when List.get(bytes, pos) is
            Ok(byte) ->
                if byte == ' ' || byte == '\t' || byte == '\r' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, tokens)
                else if byte == '\n' then
                    tokenizeHelper(bytes, pos + 1, line + 1, 1, tokens)
                else if byte == '(' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkLParen, value: "(", line, col }))
                else if byte == ')' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkRParen, value: ")", line, col }))
                else if byte == ',' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkComma, value: ",", line, col }))
                else if byte == '.' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkDot, value: ".", line, col }))
                else if byte == ';' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkSemicolon, value: ";", line, col }))
                else if byte == '*' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkStar, value: "*", line, col }))
                else if byte == '+' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkPlus, value: "+", line, col }))
                else if byte == '-' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkMinus, value: "-", line, col }))
                else if byte == '/' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkSlash, value: "/", line, col }))
                else if byte == '%' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkPercent, value: "%", line, col }))
                else if byte == '=' then
                    tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkEq, value: "=", line, col }))
                else if byte == '<' then
                    nextByte = List.get(bytes, pos + 1) |> Result.with_default(0)
                    if nextByte == '=' then
                        tokenizeHelper(bytes, pos + 2, line, col + 2, List.append(tokens, { kind: TkLe, value: "<=", line, col }))
                    else if nextByte == '>' then
                        tokenizeHelper(bytes, pos + 2, line, col + 2, List.append(tokens, { kind: TkNeq, value: "<>", line, col }))
                    else
                        tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkLt, value: "<", line, col }))
                else if byte == '>' then
                    nextByte = List.get(bytes, pos + 1) |> Result.with_default(0)
                    if nextByte == '=' then
                        tokenizeHelper(bytes, pos + 2, line, col + 2, List.append(tokens, { kind: TkGe, value: ">=", line, col }))
                    else
                        tokenizeHelper(bytes, pos + 1, line, col + 1, List.append(tokens, { kind: TkGt, value: ">", line, col }))
                else if byte == '!' then
                    nextByte = List.get(bytes, pos + 1) |> Result.with_default(0)
                    if nextByte == '=' then
                        tokenizeHelper(bytes, pos + 2, line, col + 2, List.append(tokens, { kind: TkNeq, value: "!=", line, col }))
                    else
                        Err("Unexpected character '!' at line ${Num.to_str(line)}, col ${Num.to_str(col)}")
                else if byte == '\'' then
                    readString(bytes, pos + 1, line, col, tokens)
                else if isDigit(byte) then
                    readNumber(bytes, pos, line, col, tokens)
                else if isAlpha(byte) || byte == '_' then
                    readIdentOrKeyword(bytes, pos, line, col, tokens)
                else
                    Err("Unexpected character at line ${Num.to_str(line)}, col ${Num.to_str(col)}")
            Err(_) ->
                Ok(List.append(tokens, { kind: TkEof, value: "", line, col }))

isDigit : U8 -> Bool
isDigit = |b| b >= '0' && b <= '9'

isAlpha : U8 -> Bool
isAlpha = |b| (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')

isAlphaNum : U8 -> Bool
isAlphaNum = |b| isAlpha(b) || isDigit(b) || b == '_'

readString : List U8, U64, U64, U64, List Token -> Result (List Token) Str
readString = |bytes, startPos, line, col, tokens|
    readStringHelper(bytes, startPos, line, col, tokens, [])

readStringHelper : List U8, U64, U64, U64, List Token, List U8 -> Result (List Token) Str
readStringHelper = |bytes, pos, line, col, tokens, acc|
    if pos >= List.len(bytes) then
        Err("Unterminated string literal")
    else
        when List.get(bytes, pos) is
            Ok(byte) ->
                if byte == '\'' then
                    # Check for escaped single quote: '' -> '
                    nextByte = List.get(bytes, pos + 1) |> Result.with_default(0)
                    if nextByte == '\'' then
                        readStringHelper(bytes, pos + 2, line, col, tokens, List.append(acc, '\''))
                    else
                        value = acc |> Str.from_utf8 |> Result.with_default("")
                        newTokens = List.append(tokens, { kind: TkString, value, line, col })
                        tokenizeHelper(bytes, pos + 1, line, col + (pos - col) + 2, newTokens)
                else
                    readStringHelper(bytes, pos + 1, line, col, tokens, List.append(acc, byte))
            Err(_) ->
                Err("Unterminated string literal")

readNumber : List U8, U64, U64, U64, List Token -> Result (List Token) Str
readNumber = |bytes, startPos, line, col, tokens|
    { endPos, isFloat } = scanNumber(bytes, startPos, Bool.false)
    numBytes = List.sublist(bytes, { start: startPos, len: endPos - startPos })
    value = numBytes |> Str.from_utf8 |> Result.with_default("0")
    kind = if isFloat then TkFloat else TkInteger
    newTokens = List.append(tokens, { kind, value, line, col })
    tokenizeHelper(bytes, endPos, line, col + (endPos - startPos), newTokens)

scanNumber : List U8, U64, Bool -> { endPos : U64, isFloat : Bool }
scanNumber = |bytes, pos, isFloat|
    if pos >= List.len(bytes) then
        { endPos: pos, isFloat }
    else
        when List.get(bytes, pos) is
            Ok(byte) ->
                if isDigit(byte) then
                    scanNumber(bytes, pos + 1, isFloat)
                else if byte == '.' && !isFloat then
                    scanNumber(bytes, pos + 1, Bool.true)
                else
                    { endPos: pos, isFloat }
            Err(_) ->
                { endPos: pos, isFloat }

readIdentOrKeyword : List U8, U64, U64, U64, List Token -> Result (List Token) Str
readIdentOrKeyword = |bytes, startPos, line, col, tokens|
    endPos = scanIdent(bytes, startPos)
    identBytes = List.sublist(bytes, { start: startPos, len: endPos - startPos })
    value = identBytes |> Str.from_utf8 |> Result.with_default("")
    lower = Str.with_ascii_lowercased(value)
    kind =
        when resolveKeyword(lower) is
            Keyword(k) -> k
            NotKeyword -> TkIdent
    newTokens = List.append(tokens, { kind, value, line, col })
    tokenizeHelper(bytes, endPos, line, col + (endPos - startPos), newTokens)

scanIdent : List U8, U64 -> U64
scanIdent = |bytes, pos|
    if pos >= List.len(bytes) then
        pos
    else
        when List.get(bytes, pos) is
            Ok(byte) ->
                if isAlphaNum(byte) then
                    scanIdent(bytes, pos + 1)
                else
                    pos
            Err(_) -> pos
