module [parse]

import Lexer exposing [Token]
import Ast exposing [SqlNode]

## SQL Parser for Alopex DB (Roc implementation)
## Recursive-descent Pratt parser that converts a token list into an AST.

parse : List Token -> Result SqlNode Str
parse = |tokens|
    when parseStatement(tokens, 0) is
        Ok({ node, pos: _ }) -> Ok(node)
        Err(msg) -> Err(msg)

ParseResult : { node : SqlNode, pos : U64 }

parseStatement : List Token, U64 -> Result ParseResult Str
parseStatement = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            when tok.kind is
                TkSelect -> parseSelect(tokens, pos + 1)
                TkInsert -> parseInsert(tokens, pos + 1)
                TkUpdate -> parseUpdate(tokens, pos + 1)
                TkDelete -> parseDelete(tokens, pos + 1)
                _ -> Err("Expected SQL statement, got '${tok.value}'")
        Err(_) -> Err("Unexpected end of input")

getToken : List Token, U64 -> Result Token [OutOfBounds]
getToken = |tokens, pos|
    List.get(tokens, pos)

# ---------------------------------------------------------------------------
# Expression parsing (Pratt / precedence climbing)
# ---------------------------------------------------------------------------

# Precedence levels (higher = tighter binding)
# 1  OR
# 2  AND
# 3  NOT
# 4  IS, IS NOT, IN, BETWEEN, LIKE, comparison (=, <>, <, <=, >, >=)
# 5  addition, subtraction
# 6  multiplication, division, modulo
# 7  unary minus
# 8  primary

parseExpr : List Token, U64 -> Result ParseResult Str
parseExpr = |tokens, pos|
    parseOr(tokens, pos)

parseOr : List Token, U64 -> Result ParseResult Str
parseOr = |tokens, pos|
    when parseAnd(tokens, pos) is
        Ok({ node: left, pos: pos2 }) ->
            parseOrTail(tokens, pos2, left)
        Err(msg) -> Err(msg)

parseOrTail : List Token, U64, SqlNode -> Result ParseResult Str
parseOrTail = |tokens, pos, left|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkOr then
                when parseAnd(tokens, pos + 1) is
                    Ok({ node: right, pos: pos2 }) ->
                        parseOrTail(tokens, pos2, BinOp({ op: Or, left, right }))
                    Err(msg) -> Err(msg)
            else
                Ok({ node: left, pos })
        Err(_) -> Ok({ node: left, pos })

parseAnd : List Token, U64 -> Result ParseResult Str
parseAnd = |tokens, pos|
    when parseNot(tokens, pos) is
        Ok({ node: left, pos: pos2 }) ->
            parseAndTail(tokens, pos2, left)
        Err(msg) -> Err(msg)

parseAndTail : List Token, U64, SqlNode -> Result ParseResult Str
parseAndTail = |tokens, pos, left|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkAnd then
                when parseNot(tokens, pos + 1) is
                    Ok({ node: right, pos: pos2 }) ->
                        parseAndTail(tokens, pos2, BinOp({ op: And, left, right }))
                    Err(msg) -> Err(msg)
            else
                Ok({ node: left, pos })
        Err(_) -> Ok({ node: left, pos })

parseNot : List Token, U64 -> Result ParseResult Str
parseNot = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkNot then
                when parseComparison(tokens, pos + 1) is
                    Ok({ node: operand, pos: pos2 }) ->
                        Ok({ node: UnOp({ op: Not, operand }), pos: pos2 })
                    Err(msg) -> Err(msg)
            else
                parseComparison(tokens, pos)
        Err(_) -> Err("Unexpected end of input in NOT")

parseComparison : List Token, U64 -> Result ParseResult Str
parseComparison = |tokens, pos|
    when parseAddSub(tokens, pos) is
        Ok({ node: left, pos: pos2 }) ->
            when getToken(tokens, pos2) is
                Ok(tok) ->
                    when tok.kind is
                        TkEq ->
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: right, pos: pos3 }) ->
                                    Ok({ node: BinOp({ op: Eq, left, right }), pos: pos3 })
                                Err(msg) -> Err(msg)
                        TkNeq ->
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: right, pos: pos3 }) ->
                                    Ok({ node: BinOp({ op: Neq, left, right }), pos: pos3 })
                                Err(msg) -> Err(msg)
                        TkLt ->
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: right, pos: pos3 }) ->
                                    Ok({ node: BinOp({ op: Lt, left, right }), pos: pos3 })
                                Err(msg) -> Err(msg)
                        TkLe ->
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: right, pos: pos3 }) ->
                                    Ok({ node: BinOp({ op: Le, left, right }), pos: pos3 })
                                Err(msg) -> Err(msg)
                        TkGt ->
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: right, pos: pos3 }) ->
                                    Ok({ node: BinOp({ op: Gt, left, right }), pos: pos3 })
                                Err(msg) -> Err(msg)
                        TkGe ->
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: right, pos: pos3 }) ->
                                    Ok({ node: BinOp({ op: Ge, left, right }), pos: pos3 })
                                Err(msg) -> Err(msg)
                        TkLike ->
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: right, pos: pos3 }) ->
                                    Ok({ node: BinOp({ op: Like, left, right }), pos: pos3 })
                                Err(msg) -> Err(msg)
                        TkIs ->
                            # IS NULL or IS NOT NULL
                            when getToken(tokens, pos2 + 1) is
                                Ok(next) ->
                                    if next.kind == TkNot then
                                        when getToken(tokens, pos2 + 2) is
                                            Ok(nullTok) ->
                                                if nullTok.kind == TkNull then
                                                    Ok({ node: UnOp({ op: IsNotNull, operand: left }), pos: pos2 + 3 })
                                                else
                                                    Err("Expected NULL after IS NOT")
                                            Err(_) -> Err("Expected NULL after IS NOT")
                                    else if next.kind == TkNull then
                                        Ok({ node: UnOp({ op: IsNull, operand: left }), pos: pos2 + 2 })
                                    else
                                        Err("Expected NULL or NOT NULL after IS")
                                Err(_) -> Err("Expected NULL or NOT NULL after IS")
                        TkIn ->
                            # IN ( expr, expr, ... )
                            when getToken(tokens, pos2 + 1) is
                                Ok(lparen) ->
                                    if lparen.kind == TkLParen then
                                        when parseExprList(tokens, pos2 + 2) is
                                            Ok({ nodes: items, pos: pos3 }) ->
                                                when getToken(tokens, pos3) is
                                                    Ok(rparen) ->
                                                        if rparen.kind == TkRParen then
                                                            inList = NodeList(items)
                                                            Ok({ node: BinOp({ op: In, left, right: inList }), pos: pos3 + 1 })
                                                        else
                                                            Err("Expected ')' after IN list")
                                                    Err(_) -> Err("Expected ')' after IN list")
                                            Err(msg) -> Err(msg)
                                    else
                                        Err("Expected '(' after IN")
                                Err(_) -> Err("Expected '(' after IN")
                        TkBetween ->
                            # BETWEEN low AND high
                            when parseAddSub(tokens, pos2 + 1) is
                                Ok({ node: low, pos: pos3 }) ->
                                    when getToken(tokens, pos3) is
                                        Ok(andTok) ->
                                            if andTok.kind == TkAnd then
                                                when parseAddSub(tokens, pos3 + 1) is
                                                    Ok({ node: high, pos: pos4 }) ->
                                                        range = NodeList([low, high])
                                                        Ok({ node: BinOp({ op: Between, left, right: range }), pos: pos4 })
                                                    Err(msg) -> Err(msg)
                                            else
                                                Err("Expected AND in BETWEEN expression")
                                        Err(_) -> Err("Expected AND in BETWEEN expression")
                                Err(msg) -> Err(msg)
                        _ -> Ok({ node: left, pos: pos2 })
                Err(_) -> Ok({ node: left, pos: pos2 })
        Err(msg) -> Err(msg)

parseAddSub : List Token, U64 -> Result ParseResult Str
parseAddSub = |tokens, pos|
    when parseMulDiv(tokens, pos) is
        Ok({ node: left, pos: pos2 }) ->
            parseAddSubTail(tokens, pos2, left)
        Err(msg) -> Err(msg)

parseAddSubTail : List Token, U64, SqlNode -> Result ParseResult Str
parseAddSubTail = |tokens, pos, left|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkPlus then
                when parseMulDiv(tokens, pos + 1) is
                    Ok({ node: right, pos: pos2 }) ->
                        parseAddSubTail(tokens, pos2, BinOp({ op: Add, left, right }))
                    Err(msg) -> Err(msg)
            else if tok.kind == TkMinus then
                when parseMulDiv(tokens, pos + 1) is
                    Ok({ node: right, pos: pos2 }) ->
                        parseAddSubTail(tokens, pos2, BinOp({ op: Sub, left, right }))
                    Err(msg) -> Err(msg)
            else
                Ok({ node: left, pos })
        Err(_) -> Ok({ node: left, pos })

parseMulDiv : List Token, U64 -> Result ParseResult Str
parseMulDiv = |tokens, pos|
    when parseUnary(tokens, pos) is
        Ok({ node: left, pos: pos2 }) ->
            parseMulDivTail(tokens, pos2, left)
        Err(msg) -> Err(msg)

parseMulDivTail : List Token, U64, SqlNode -> Result ParseResult Str
parseMulDivTail = |tokens, pos, left|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkStar then
                when parseUnary(tokens, pos + 1) is
                    Ok({ node: right, pos: pos2 }) ->
                        parseMulDivTail(tokens, pos2, BinOp({ op: Mul, left, right }))
                    Err(msg) -> Err(msg)
            else if tok.kind == TkSlash then
                when parseUnary(tokens, pos + 1) is
                    Ok({ node: right, pos: pos2 }) ->
                        parseMulDivTail(tokens, pos2, BinOp({ op: Div, left, right }))
                    Err(msg) -> Err(msg)
            else if tok.kind == TkPercent then
                when parseUnary(tokens, pos + 1) is
                    Ok({ node: right, pos: pos2 }) ->
                        parseMulDivTail(tokens, pos2, BinOp({ op: Mod, left, right }))
                    Err(msg) -> Err(msg)
            else
                Ok({ node: left, pos })
        Err(_) -> Ok({ node: left, pos })

parseUnary : List Token, U64 -> Result ParseResult Str
parseUnary = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkMinus then
                when parsePrimary(tokens, pos + 1) is
                    Ok({ node: operand, pos: pos2 }) ->
                        Ok({ node: UnOp({ op: Neg, operand }), pos: pos2 })
                    Err(msg) -> Err(msg)
            else
                parsePrimary(tokens, pos)
        Err(_) -> Err("Unexpected end of input")

parsePrimary : List Token, U64 -> Result ParseResult Str
parsePrimary = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            when tok.kind is
                TkInteger ->
                    val = Str.to_i64(tok.value) |> Result.with_default(0)
                    Ok({ node: IntLit(val), pos: pos + 1 })
                TkFloat ->
                    val = Str.to_f64(tok.value) |> Result.with_default(0.0)
                    Ok({ node: FloatLit(val), pos: pos + 1 })
                TkString ->
                    Ok({ node: StrLit(tok.value), pos: pos + 1 })
                TkTrue ->
                    Ok({ node: BoolLit(Bool.true), pos: pos + 1 })
                TkFalse ->
                    Ok({ node: BoolLit(Bool.false), pos: pos + 1 })
                TkNull ->
                    Ok({ node: NullLit, pos: pos + 1 })
                TkStar ->
                    Ok({ node: StarLit, pos: pos + 1 })
                TkIdent ->
                    # Peek ahead: table.column or function call
                    when getToken(tokens, pos + 1) is
                        Ok(nextTok) ->
                            if nextTok.kind == TkDot then
                                # table.column
                                when getToken(tokens, pos + 2) is
                                    Ok(colTok) ->
                                        if colTok.kind == TkIdent then
                                            Ok({ node: ColRef({ table: tok.value, column: colTok.value }), pos: pos + 3 })
                                        else
                                            Ok({ node: Ident(tok.value), pos: pos + 1 })
                                    Err(_) -> Ok({ node: Ident(tok.value), pos: pos + 1 })
                            else if nextTok.kind == TkLParen then
                                # function call: name(args)
                                when parseFunctionArgs(tokens, pos + 2) is
                                    Ok({ nodes: args, pos: pos2 }) ->
                                        Ok({ node: FnCall({ name: tok.value, args }), pos: pos2 })
                                    Err(msg) -> Err(msg)
                            else
                                Ok({ node: Ident(tok.value), pos: pos + 1 })
                        Err(_) -> Ok({ node: Ident(tok.value), pos: pos + 1 })
                TkLParen ->
                    when parseExpr(tokens, pos + 1) is
                        Ok({ node, pos: pos2 }) ->
                            when getToken(tokens, pos2) is
                                Ok(closeTok) ->
                                    if closeTok.kind == TkRParen then
                                        Ok({ node, pos: pos2 + 1 })
                                    else
                                        Err("Expected ')' at line ${Num.to_str(closeTok.line)}")
                                Err(_) -> Err("Expected ')'")
                        Err(msg) -> Err(msg)
                _ ->
                    Err("Unexpected token '${tok.value}' at line ${Num.to_str(tok.line)}, col ${Num.to_str(tok.col)}")
        Err(_) -> Err("Unexpected end of input")

# Parse function arguments: already consumed '(', reads until ')'
parseFunctionArgs : List Token, U64 -> Result { nodes : List SqlNode, pos : U64 } Str
parseFunctionArgs = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkRParen then
                Ok({ nodes: [], pos: pos + 1 })
            else if tok.kind == TkStar then
                # COUNT(*) style
                when getToken(tokens, pos + 1) is
                    Ok(rparen) ->
                        if rparen.kind == TkRParen then
                            Ok({ nodes: [StarLit], pos: pos + 2 })
                        else
                            Err("Expected ')' after * in function call")
                    Err(_) -> Err("Expected ')' after * in function call")
            else
                when parseExprList(tokens, pos) is
                    Ok({ nodes, pos: pos2 }) ->
                        when getToken(tokens, pos2) is
                            Ok(rparen) ->
                                if rparen.kind == TkRParen then
                                    Ok({ nodes, pos: pos2 + 1 })
                                else
                                    Err("Expected ')' after function arguments")
                            Err(_) -> Err("Expected ')' after function arguments")
                    Err(msg) -> Err(msg)
        Err(_) -> Err("Expected ')' or arguments in function call")

# ---------------------------------------------------------------------------
# Comma-separated expression list (no trailing comma)
# ---------------------------------------------------------------------------

parseExprList : List Token, U64 -> Result { nodes : List SqlNode, pos : U64 } Str
parseExprList = |tokens, pos|
    when parseExpr(tokens, pos) is
        Ok({ node, pos: pos2 }) ->
            parseExprListTail(tokens, pos2, [node])
        Err(msg) -> Err(msg)

parseExprListTail : List Token, U64, List SqlNode -> Result { nodes : List SqlNode, pos : U64 } Str
parseExprListTail = |tokens, pos, acc|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkComma then
                when parseExpr(tokens, pos + 1) is
                    Ok({ node, pos: pos2 }) ->
                        parseExprListTail(tokens, pos2, List.append(acc, node))
                    Err(msg) -> Err(msg)
            else
                Ok({ nodes: acc, pos })
        Err(_) -> Ok({ nodes: acc, pos })

# ---------------------------------------------------------------------------
# Table list parsing
# ---------------------------------------------------------------------------

parseTableList : List Token, U64 -> Result { nodes : List SqlNode, pos : U64 } Str
parseTableList = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkIdent then
                parseExprListTail(tokens, pos + 1, [Ident(tok.value)])
            else
                Err("Expected table name, got '${tok.value}'")
        Err(_) -> Err("Expected table name")

# ---------------------------------------------------------------------------
# Optional WHERE clause
# ---------------------------------------------------------------------------

parseOptionalWhere : List Token, U64 -> { whereClause : [Some SqlNode, None], pos : U64 }
parseOptionalWhere = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkWhere then
                when parseExpr(tokens, pos + 1) is
                    Ok({ node, pos: pos2 }) ->
                        { whereClause: Some(node), pos: pos2 }
                    Err(_) ->
                        { whereClause: None, pos }
            else
                { whereClause: None, pos }
        Err(_) -> { whereClause: None, pos }

# ---------------------------------------------------------------------------
# Optional ORDER BY clause
# ---------------------------------------------------------------------------

parseOptionalOrderBy : List Token, U64 -> { orderBy : List SqlNode, pos : U64 }
parseOptionalOrderBy = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkOrder then
                when getToken(tokens, pos + 1) is
                    Ok(byTok) ->
                        if byTok.kind == TkBy then
                            when parseExprList(tokens, pos + 2) is
                                Ok({ nodes, pos: pos2 }) ->
                                    { orderBy: nodes, pos: pos2 }
                                Err(_) ->
                                    { orderBy: [], pos }
                        else
                            { orderBy: [], pos }
                    Err(_) -> { orderBy: [], pos }
            else
                { orderBy: [], pos }
        Err(_) -> { orderBy: [], pos }

# ---------------------------------------------------------------------------
# Optional LIMIT clause
# ---------------------------------------------------------------------------

parseOptionalLimit : List Token, U64 -> { limitClause : [Some SqlNode, None], pos : U64 }
parseOptionalLimit = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkLimit then
                when parseExpr(tokens, pos + 1) is
                    Ok({ node, pos: pos2 }) ->
                        { limitClause: Some(node), pos: pos2 }
                    Err(_) ->
                        { limitClause: None, pos }
            else
                { limitClause: None, pos }
        Err(_) -> { limitClause: None, pos }

# ---------------------------------------------------------------------------
# SELECT statement
# ---------------------------------------------------------------------------

parseSelect : List Token, U64 -> Result ParseResult Str
parseSelect = |tokens, pos|
    when parseExprList(tokens, pos) is
        Ok({ nodes: columns, pos: pos2 }) ->
            when getToken(tokens, pos2) is
                Ok(tok) ->
                    if tok.kind == TkFrom then
                        when parseTableList(tokens, pos2 + 1) is
                            Ok({ nodes: tables, pos: pos3 }) ->
                                { whereClause, pos: pos4 } = parseOptionalWhere(tokens, pos3)
                                { orderBy, pos: pos5 } = parseOptionalOrderBy(tokens, pos4)
                                { limitClause, pos: pos6 } = parseOptionalLimit(tokens, pos5)
                                node = SelectStmt({
                                    columns,
                                    from: tables,
                                    where: whereClause,
                                    orderBy,
                                    groupBy: [],
                                    having: None,
                                    limit: limitClause,
                                })
                                Ok({ node, pos: pos6 })
                            Err(msg) -> Err(msg)
                    else
                        node = SelectStmt({
                            columns,
                            from: [],
                            where: None,
                            orderBy: [],
                            groupBy: [],
                            having: None,
                            limit: None,
                        })
                        Ok({ node, pos: pos2 })
                Err(_) ->
                    node = SelectStmt({
                        columns,
                        from: [],
                        where: None,
                        orderBy: [],
                        groupBy: [],
                        having: None,
                        limit: None,
                    })
                    Ok({ node, pos: pos2 })
        Err(msg) -> Err(msg)

# ---------------------------------------------------------------------------
# INSERT statement
# INSERT INTO table (col, col, ...) VALUES (val, val, ...)
# ---------------------------------------------------------------------------

parseInsert : List Token, U64 -> Result ParseResult Str
parseInsert = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkInto then
                when getToken(tokens, pos + 1) is
                    Ok(tableTok) ->
                        if tableTok.kind == TkIdent then
                            # Check for optional column list: (col, col, ...)
                            when getToken(tokens, pos + 2) is
                                Ok(maybeLParen) ->
                                    if maybeLParen.kind == TkLParen then
                                        when parseIdentList(tokens, pos + 3) is
                                            Ok({ names: cols, pos: pos3 }) ->
                                                # Expect closing paren
                                                when getToken(tokens, pos3) is
                                                    Ok(rparen) ->
                                                        if rparen.kind == TkRParen then
                                                            parseInsertValues(tokens, pos3 + 1, tableTok.value, cols)
                                                        else
                                                            Err("Expected ')' after column list")
                                                    Err(_) -> Err("Expected ')' after column list")
                                            Err(msg) -> Err(msg)
                                    else
                                        # No column list, go straight to VALUES
                                        parseInsertValues(tokens, pos + 2, tableTok.value, [])
                                Err(_) ->
                                    node = InsertStmt({ table: tableTok.value, columns: [], values: [] })
                                    Ok({ node, pos: pos + 2 })
                        else
                            Err("Expected table name after INTO")
                    Err(_) -> Err("Expected table name after INTO")
            else
                Err("Expected INTO after INSERT")
        Err(_) -> Err("Expected INTO after INSERT")

parseInsertValues : List Token, U64, Str, List Str -> Result ParseResult Str
parseInsertValues = |tokens, pos, table, cols|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkValues then
                when getToken(tokens, pos + 1) is
                    Ok(lparen) ->
                        if lparen.kind == TkLParen then
                            when parseExprList(tokens, pos + 2) is
                                Ok({ nodes: values, pos: pos2 }) ->
                                    when getToken(tokens, pos2) is
                                        Ok(rparen) ->
                                            if rparen.kind == TkRParen then
                                                node = InsertStmt({ table, columns: cols, values })
                                                Ok({ node, pos: pos2 + 1 })
                                            else
                                                Err("Expected ')' after VALUES list")
                                        Err(_) -> Err("Expected ')' after VALUES list")
                                Err(msg) -> Err(msg)
                        else
                            Err("Expected '(' after VALUES")
                    Err(_) -> Err("Expected '(' after VALUES")
            else
                Err("Expected VALUES keyword")
        Err(_) -> Err("Expected VALUES keyword")

# Parse a comma-separated list of identifiers (used for column lists)
parseIdentList : List Token, U64 -> Result { names : List Str, pos : U64 } Str
parseIdentList = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkIdent then
                parseIdentListTail(tokens, pos + 1, [tok.value])
            else
                Err("Expected identifier in list")
        Err(_) -> Err("Expected identifier in list")

parseIdentListTail : List Token, U64, List Str -> Result { names : List Str, pos : U64 } Str
parseIdentListTail = |tokens, pos, acc|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkComma then
                when getToken(tokens, pos + 1) is
                    Ok(identTok) ->
                        if identTok.kind == TkIdent then
                            parseIdentListTail(tokens, pos + 2, List.append(acc, identTok.value))
                        else
                            Err("Expected identifier after ','")
                    Err(_) -> Err("Expected identifier after ','")
            else
                Ok({ names: acc, pos })
        Err(_) -> Ok({ names: acc, pos })

# ---------------------------------------------------------------------------
# UPDATE statement
# UPDATE table SET col = val, col = val WHERE ...
# ---------------------------------------------------------------------------

parseUpdate : List Token, U64 -> Result ParseResult Str
parseUpdate = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tableTok) ->
            if tableTok.kind == TkIdent then
                when getToken(tokens, pos + 1) is
                    Ok(setTok) ->
                        if setTok.kind == TkSet then
                            when parseSetList(tokens, pos + 2) is
                                Ok({ sets, pos: pos2 }) ->
                                    { whereClause, pos: pos3 } = parseOptionalWhere(tokens, pos2)
                                    node = UpdateStmt({ table: tableTok.value, sets, where: whereClause })
                                    Ok({ node, pos: pos3 })
                                Err(msg) -> Err(msg)
                        else
                            Err("Expected SET after table name in UPDATE")
                    Err(_) -> Err("Expected SET after table name in UPDATE")
            else
                Err("Expected table name after UPDATE")
        Err(_) -> Err("Expected table name after UPDATE")

# Parse SET col = val, col = val ...
parseSetList : List Token, U64 -> Result { sets : List { col : Str, val : SqlNode }, pos : U64 } Str
parseSetList = |tokens, pos|
    when parseSetItem(tokens, pos) is
        Ok({ item, pos: pos2 }) ->
            parseSetListTail(tokens, pos2, [item])
        Err(msg) -> Err(msg)

parseSetListTail : List Token, U64, List { col : Str, val : SqlNode } -> Result { sets : List { col : Str, val : SqlNode }, pos : U64 } Str
parseSetListTail = |tokens, pos, acc|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkComma then
                when parseSetItem(tokens, pos + 1) is
                    Ok({ item, pos: pos2 }) ->
                        parseSetListTail(tokens, pos2, List.append(acc, item))
                    Err(msg) -> Err(msg)
            else
                Ok({ sets: acc, pos })
        Err(_) -> Ok({ sets: acc, pos })

parseSetItem : List Token, U64 -> Result { item : { col : Str, val : SqlNode }, pos : U64 } Str
parseSetItem = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(colTok) ->
            if colTok.kind == TkIdent then
                when getToken(tokens, pos + 1) is
                    Ok(eqTok) ->
                        if eqTok.kind == TkEq then
                            when parseExpr(tokens, pos + 2) is
                                Ok({ node: valNode, pos: pos2 }) ->
                                    Ok({ item: { col: colTok.value, val: valNode }, pos: pos2 })
                                Err(msg) -> Err(msg)
                        else
                            Err("Expected '=' in SET clause")
                    Err(_) -> Err("Expected '=' in SET clause")
            else
                Err("Expected column name in SET clause")
        Err(_) -> Err("Expected column name in SET clause")

# ---------------------------------------------------------------------------
# DELETE statement
# ---------------------------------------------------------------------------

parseDelete : List Token, U64 -> Result ParseResult Str
parseDelete = |tokens, pos|
    when getToken(tokens, pos) is
        Ok(tok) ->
            if tok.kind == TkFrom then
                when getToken(tokens, pos + 1) is
                    Ok(tableTok) ->
                        if tableTok.kind == TkIdent then
                            { whereClause, pos: pos3 } = parseOptionalWhere(tokens, pos + 2)
                            node = DeleteStmt({ table: tableTok.value, where: whereClause })
                            Ok({ node, pos: pos3 })
                        else
                            Err("Expected table name after FROM")
                    Err(_) -> Err("Expected table name after FROM")
            else
                Err("Expected FROM after DELETE")
        Err(_) -> Err("Expected FROM after DELETE")
