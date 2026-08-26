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

## Pagination (LIMIT / OFFSET / FETCH)

```sql
SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 20;
SELECT id FROM t ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY;
SELECT id, score FROM t ORDER BY score FETCH FIRST 2 ROWS WITH TIES;
```

`WITH TIES` は境界行と ORDER BY キーが等しい peer 行を追加で返す(ORDER BY 必須、
PostgreSQL 準拠)。count は定数式を plan 時に畳み込み、`LIMIT NULL` / `LIMIT ALL`
は無制限、負値はエラー。詳細は
[docs/sql-fetch-pagination.md](../../docs/sql-fetch-pagination.md)。

## DISTINCT ON

```sql
SELECT DISTINCT ON (region) region, amount FROM sales ORDER BY region, amount;
```

キー群ごとに先頭 1 行を返す(PostgreSQL 準拠の ORDER BY prefix 制約、42P10 相当
は `ALOPEX-T014`)。ORDER BY で決まらない同順位は全入力列の schema 順比較で
決定的に解決するため、結果は物理挿入順に依存しない。詳細は
[docs/sql-distinct-on.md](../../docs/sql-distinct-on.md)。

## GROUPING SETS / ROLLUP / CUBE

```sql
SELECT region, product, SUM(amount), GROUPING(region, product)
FROM sales GROUP BY CUBE(region, product);
SELECT region, COUNT(*) FROM sales GROUP BY GROUPING SETS ((region), ());
```

単一パスのハッシュ集約で複数 grouping set を同時評価する(PostgreSQL 準拠)。
subtotal 行の placeholder NULL と実データ NULL はどちらも SQL NULL で出力され、
区別は `GROUPING`/`GROUPING_ID`(BIGINT ビットマスク、左端引数が最上位ビット)
のみで行う。通常キーとの混在はクロス積、重複 set は重複行を出力。展開後 set 数
は 4096、union キー数は 63 が上限。詳細は
[docs/sql-grouping-sets.md](../../docs/sql-grouping-sets.md)。

## LATERAL / table functions / relation alias column lists

```sql
SELECT p.id, top.val
FROM parent AS p
CROSS JOIN LATERAL (SELECT c.val FROM child AS c
                    WHERE c.parent_id = p.id ORDER BY c.val DESC LIMIT 1) AS top;
SELECT p.id, u.unnest FROM parent AS p, UNNEST(p.emb) AS u;
SELECT n.generate_series FROM GENERATE_SERIES(1, 5) AS n;
SELECT r.a, r.b FROM child AS r(a, b, c);
```

`LATERAL` 派生表は左側の FROM item を参照でき、左行ごとに再評価される
(`CROSS JOIN LATERAL` / `[INNER] JOIN LATERAL ... ON` /
`LEFT [OUTER] JOIN LATERAL ... ON`。`RIGHT`/`FULL` は `ALOPEX-T015`)。
`LATERAL` を書かない派生表は従来どおり外側 FROM を参照できない。FROM 句の
table function は `UNNEST(VECTOR)` と整数版 `GENERATE_SERIES(start, stop [, step])`
で、`LATERAL` なしでも左側を参照する(implicit lateral)。
`AS t(c1, ..., cn)` は base table / CTE 参照 / 派生表 / table function すべてで
使え、列数完全一致(`ALOPEX-T012`)を要求する。`LATERAL` は文脈キーワードなの
で `lateral` という名前の関係は引き続き使える。詳細は
[docs/sql-lateral-table-functions.md](../../docs/sql-lateral-table-functions.md)。
v0.8.9 の portable SQL 関数一覧と境界条件は
[docs/sql-portable-functions-v0.8.9.md](../../docs/sql-portable-functions-v0.8.9.md)。

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
- `PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)` (ordered-set)
- `PERCENTILE_CONT` / `QUANTILE_CONT` / `MEDIAN` / `MODE`
- `VARIANCE` / `VAR_SAMP` / `VAR_POP` / `STDDEV` / `COVAR_*` / `CORR`
- `REGR_COUNT` / `REGR_AVGX` / `REGR_AVGY` / `REGR_SXX` / `REGR_SYY` /
  `REGR_SXY` / `REGR_SLOPE` / `REGR_INTERCEPT` / `REGR_R2`
- `ANY_VALUE` / `FIRST` / `LAST` / `ARG_MIN` / `ARG_MAX` (`MIN_BY`/`MAX_BY`)
- `BIT_AND` / `BIT_OR` / `BIT_XOR` / `BOOL_AND` / `BOOL_OR`

## Aggregate FILTER / ordered aggregates / WITHIN GROUP

```sql
SELECT g, COUNT(*) FILTER (WHERE v > 10) FROM t GROUP BY g;
SELECT STRING_AGG(name, ',' ORDER BY v DESC, name ASC) FROM t;
SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) FROM t;
```

`FILTER` は述語が TRUE の行だけを(DISTINCT より前に)集約へ渡す。集約内
`ORDER BY` は順序鋭敏な `GROUP_CONCAT`/`STRING_AGG` の連結順を決め、順序不感な
集約では検証後に破棄される。`PERCENTILE_DISC` は PostgreSQL 準拠の離散選択
(空グループは NULL、NULL ソート値は除外)。`OVER` との併用は明示エラー。詳細は
[docs/sql-aggregate-filter-within-group.md](../../docs/sql-aggregate-filter-within-group.md)。

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
