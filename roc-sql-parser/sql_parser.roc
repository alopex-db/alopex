app [main!] { pf: platform "https://github.com/roc-lang/basic-cli/releases/download/0.20.0/X73hGh05nNTkDHU06FHC0YfFaQB1pimX7gncRcao5mU.tar.br" }

import pf.Stdout

import Lexer exposing [tokenize]
import Parser exposing [parse]
import Ast exposing [toStr]

main! = |_args|
    # Trial implementation: parse from command line argument or use demo SQL
    demoSql = "SELECT id, name FROM users WHERE age > 18"

    Stdout.line!("Alopex SQL Parser (Roc) v0.1.0")?
    Stdout.line!("Parsing: ${demoSql}")?

    when tokenize(demoSql) is
        Ok(tokens) ->
            Stdout.line!("Tokens: ${Num.to_str(List.len(tokens))}")?
            when parse(tokens) is
                Ok(ast) ->
                    Stdout.line!("AST: ${toStr(ast)}")?
                    Ok({})
                Err(msg) ->
                    Stdout.line!("Parse error: ${msg}")?
                    Ok({})
        Err(msg) ->
            Stdout.line!("Lex error: ${msg}")?
            Ok({})
