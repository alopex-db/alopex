module [SqlNode, SqlNodeKind, BinaryOp, UnaryOp, JoinKind, toStr]

## AST definitions for Alopex SQL parser (Roc implementation)

SqlNodeKind : [
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    Identifier,
    StringLit,
    IntLit,
    FloatLit,
    BoolLit,
    Null,
    Star,
    ColumnRef,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    Alias,
    FromClause,
    WhereClause,
    OrderByClause,
    GroupByClause,
    HavingClause,
    LimitClause,
    JoinNode,
    ColumnDef,
    TypeName,
    Constraint,
    ExprList,
]

BinaryOp : [
    Eq, Neq, Lt, Le, Gt, Ge,
    Add, Sub, Mul, Div, Mod,
    And, Or,
    Like, In, Between, Is,
]

UnaryOp : [Not, Neg, IsNull, IsNotNull]

JoinKind : [Inner, Left, Right, Full, Cross]

SqlNode : [
    Ident Str,
    StrLit Str,
    IntLit I64,
    FloatLit F64,
    BoolLit Bool,
    NullLit,
    StarLit,
    BinOp { op : BinaryOp, left : SqlNode, right : SqlNode },
    UnOp { op : UnaryOp, operand : SqlNode },
    FnCall { name : Str, args : List SqlNode },
    ColRef { table : Str, column : Str },
    AliasNode { expr : SqlNode, name : Str },
    JoinExpr { kind : JoinKind, left : SqlNode, right : SqlNode, cond : SqlNode },
    SelectStmt { columns : List SqlNode, from : List SqlNode, where : [Some SqlNode, None], orderBy : List SqlNode, groupBy : List SqlNode, having : [Some SqlNode, None], limit : [Some SqlNode, None] },
    InsertStmt { table : Str, columns : List Str, values : List SqlNode },
    UpdateStmt { table : Str, sets : List { col : Str, val : SqlNode }, where : [Some SqlNode, None] },
    DeleteStmt { table : Str, where : [Some SqlNode, None] },
    CreateTableStmt { table : Str, columns : List SqlNode, ifNotExists : Bool },
    DropTableStmt { table : Str, ifExists : Bool },
    NodeList (List SqlNode),
]

toStr : SqlNode -> Str
toStr = |node|
    when node is
        Ident(name) -> "Ident(${name})"
        StrLit(val) -> "Str('${val}')"
        IntLit(val) -> "Int(${Num.to_str(val)})"
        FloatLit(_val) -> "Float(...)"
        BoolLit(val) -> if val then "Bool(true)" else "Bool(false)"
        NullLit -> "NULL"
        StarLit -> "*"
        BinOp({ op: _, left, right }) -> "BinOp(${toStr(left)}, ${toStr(right)})"
        UnOp({ op: _, operand }) -> "UnOp(${toStr(operand)})"
        FnCall({ name, args: _ }) -> "Fn(${name})"
        ColRef({ table, column }) -> "ColRef(${table}.${column})"
        AliasNode({ expr, name }) -> "Alias(${toStr(expr)} AS ${name})"
        SelectStmt(_) -> "SELECT(...)"
        InsertStmt({ table, columns: _, values: _ }) -> "INSERT INTO ${table}"
        UpdateStmt({ table, sets: _, where: _ }) -> "UPDATE ${table}"
        DeleteStmt({ table, where: _ }) -> "DELETE FROM ${table}"
        CreateTableStmt({ table, columns: _, ifNotExists: _ }) -> "CREATE TABLE ${table}"
        DropTableStmt({ table, ifExists: _ }) -> "DROP TABLE ${table}"
        JoinExpr(_) -> "JOIN(...)"
        NodeList(_) -> "NodeList(...)"
