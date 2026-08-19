# alopex-sql

alopex-sql は Alopex の SQL 解析・計画・実行レイヤーを提供します。

## Quick Start

```rust
use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;

let sql = r#"
    CREATE TABLE sales (region TEXT, amount INT);
    INSERT INTO sales (region, amount) VALUES ('us', 10), ('us', 20), ('eu', 7);
    SELECT region, SUM(amount) FROM sales GROUP BY region;
"#;

let dialect = AlopexDialect;
let statements = Parser::parse_sql(&dialect, sql).unwrap();

let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
let store = Arc::new(MemoryKV::new());
let mut executor = Executor::new(store, catalog.clone());

for stmt in statements {
    let plan = {
        let guard = catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    };
    let _ = executor.execute(plan).unwrap();
}
```

## GROUP BY / HAVING

```sql
SELECT category, COUNT(*) FROM products GROUP BY category;
SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 10;
SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY COUNT(*) DESC;
```

## Supported Aggregate Functions

- `COUNT(*)`
- `COUNT(column)`
- `COUNT(DISTINCT column)`
- `SUM(column)` (numeric)
- `TOTAL(column)` (numeric, returns 0.0 for all-NULL)
- `AVG(column)` (numeric)
- `MIN(column)` / `MAX(column)` (comparable types)
- `GROUP_CONCAT(column)` / `GROUP_CONCAT(column, separator)`
- `STRING_AGG(column, separator)`

## Recursive CTE

直接自己再帰する単一 CTE を `UNION` または `UNION ALL` で実行できます。列名リストを省略した場合は、anchor term の出力名が公開スキーマになります。

```sql
WITH RECURSIVE counter(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM counter WHERE n < 5
)
SELECT n FROM counter ORDER BY n;
```

実行は delta working table による固定点反復です。既定上限は 1,000 iterations、100,000 accumulated rows で、保持する working/result/accumulated/dedup key は query memory policy の対象です。

## Recursive CTE Limitations

- 相互再帰、複数の直接自己参照、recursive term 内の subquery・nested `WITH`・set operation は fail-closed で拒否します。
- 再帰 CTE のリモート分散 read は未対応です。
- 各 inner operator の独自 buffer は同じ memory policy を検査しますが、現在の policy API は retained recursive sets と transient buffer の合算 high-water tracker を共有しません。

## Error Scenarios

| Code | Contract |
| --- | --- |
| `ALOPEX-E001` | Transaction conflict during execution. |
| `ALOPEX-E002` | A write was attempted through a read-only transaction. |
| `ALOPEX-E003` | 実行時の resource exhaustion。再帰 CTE の iteration/row/memory limit と既存の aggregate/memory limit が対象。 |
| `ALOPEX-E004` | CAST conversion failure。source/target type と bounded reason を公開し、内部 AST/MessagePack 名は公開しない。 |

- `ColumnNotFound`: 存在しないカラムを `GROUP BY` で参照した場合。
- `TypeMismatch`: `SUM/AVG` に非数値型、`GROUP_CONCAT` に非 TEXT を指定した場合。
- `InvalidExpression`: `HAVING` や `SELECT` で GROUP BY に含まれない非集約列を参照した場合。
- `ResourceExhausted`: resource limit を超えた場合。公開 SQL error code は `ALOPEX-E003`。
- `CastFailed`: CAST の変換不能。TRY_CAST はこの失敗だけを NULL に変換し、source expression の失敗は保持する。公開 SQL error code は `ALOPEX-E004`。
