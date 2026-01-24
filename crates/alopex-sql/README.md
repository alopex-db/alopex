# alopex-sql

alopex-sql は Alopex の SQL レイヤーを提供します。v0.5.0 では GROUP BY/HAVING と集約関数をサポートします。

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
- `AVG(column)` (numeric)
- `MIN(column)` / `MAX(column)` (comparable types)
- `GROUP_CONCAT(column)` / `GROUP_CONCAT(column, separator)`

## Limitations

- JOIN/Subquery/Window 関数は未対応。
- `SUM/AVG/MIN/MAX` の `DISTINCT` は未対応。
- `GROUP BY` の式は現状カラム参照のみを想定。
```
