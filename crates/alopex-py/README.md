# Alopex Python バインディング

Python から AlopexDB を操作するためのバインディングです。  
Database/Transaction の基本機能に加え、ベクトル検索（numpy）と Unity Catalog 互換の Catalog API（polars）を提供します。

## インストール

```bash
pip install alopex
```

Catalog API (polars) を使う場合:

```bash
pip install alopex[polars]
```

開発中は maturin を利用できます。

```bash
maturin develop -m crates/alopex-py/pyproject.toml
```

オプション依存:

- numpy を使う場合: `pip install alopex[numpy]`
- polars を使う場合: `pip install alopex[polars]`

## 対応バージョン

| 依存関係 | 対応バージョン |
| --- | --- |
| Python | 3.8+ |
| Polars | 0.20+ (Catalog API) |
| NumPy | 1.20+ (Vector API) |

## 基本的な使い方

### Database / Transaction

```python
from alopex import Database, TxnMode

db = Database.new()

with db.begin(TxnMode.READ_WRITE) as txn:
    txn.put(b"user:1", b"alice")
    txn.commit()

with db.begin(TxnMode.READ_ONLY) as txn:
    value = txn.get(b"user:1")
    print(value)

db.close()
```

## v0.8 embedded-local stream / DataFrame API

v0.8 の stream API は embedded-local database 専用です。Client、endpoint、remote session、remote
DataFrame execution は提供しません。`Database` は既定で `thread_mode="multi"` であり、
`thread_mode="single"` を選ぶと database、transaction、stream、DataFrame、LazyFrame は作成した
Python thread だけで使用できます。

### 同期 SQL / scan stream

`execute_sql_stream()` は、単一 table の local `SELECT`、row-local `WHERE`、projection、`LIMIT` /
`OFFSET` の documented subset を一行ずつ返します。`query_stream()` は `LocalScan.table`、`csv`、
`parquet`、`columnar_segment`、`lazyframe` の五つだけを受け取ります。非対応 SQL、callback、remote
source は stream を開く前に `AlopexError` となります。

```python
from alopex import Database, LocalScan

db = Database.new()
db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'one'), (2, 'two')")

with db.execute_sql_stream(
    "SELECT id, name FROM users WHERE id >= 1",
    resource_limit_bytes=64 * 1024 * 1024,
    timeout=5.0,
) as stream:
    for row in stream:
        print(row)
    print(stream.status)
    # {"terminal": "exhausted", "rows_delivered": 2,
    #  "resource_limit_bytes": ..., "resource_scope": "sql_row",
    #  "transaction_effect": "none"}
```

`DataFrameStream.status` は同じ構造で、`rows_delivered` の代わりに
`batches_delivered`、`resource_scope="dataframe_batch"` を返します。normal exhaustion は以後
end-of-stream、close/cancel/timeout/failure は以後も同じ識別可能な terminal error を返します。

transaction 内の stream は暗黙 commit しません。正常に exhaustion した read stream は commit 可能であり、
early close、cancel、failure は `transaction.status["stream_effect"]` と stream の
`transaction_effect` で確認した後に rollback してください。active stream 中の commit は拒否されます。

### Python DataFrame streaming / expressions

`LazyFrame.collect(streaming=True)` は Phase 3 と同じ source、row order、schema、NULL、resource
contract で有限 `DataFrame` batch を返します。`concat`、`concat_str`、`select`、`filter`、
`with_columns` は同じ expression semantics を使用します。

```python
from alopex import DataFrame, col, concat_str, lit

plan = (
    DataFrame({"id": [1, 2, 3], "left": ["a", "b", "c"], "right": ["x", "y", "z"]})
    .lazy()
    .filter(col("id").gt(lit(1)))
    .select([
        col("id").add(lit(10)).alias("next_id"),
        concat_str([col("left"), col("right")], "-").alias("label"),
    ])
)

with plan.collect(streaming=True, batch_rows=1) as batches:
    for batch in batches:
        print(batch.to_dict())
```

### asyncio

`alopex.asyncio` は Python 3.8 以上の標準 `asyncio` loop をサポートし、caller に Rust/Tokio runtime を
要求しません。`prefetch_batches` は read ahead の上限、`max_buffered_batches` は ready result
buffer の上限です。両者は `0 <= prefetch_batches <= max_buffered_batches`、かつ
`max_buffered_batches >= 1` でなければなりません。`consumer_idle_timeout` と `timeout` の単位は秒です。
native worker は bounded Rust payload だけを保持し、Python の row / DataFrame 変換は asyncio consumer 側で
行われます。Python の producer queue や callback は使用しません。

```python
import asyncio
from alopex.asyncio import AsyncDatabase

async def main() -> None:
    async with await AsyncDatabase.new() as db:
        await db.execute_sql("CREATE TABLE events (id INTEGER PRIMARY KEY)")
        await db.execute_sql("INSERT INTO events (id) VALUES (1), (2)")
        stream = await db.execute_sql_stream(
            "SELECT id FROM events",
            prefetch_batches=1,
            max_buffered_batches=1,
            consumer_idle_timeout=5.0,
        )
        async with stream:
            async for row in stream:
                print(row)

asyncio.run(main())
```

同じ stream で同時に二つの `anext()` を実行すると `stream_busy` です。task cancellation、`aclose()`、
`cancel()`、idle timeout は stream の native source を終端させ、後続の独立した database operation を継続できます。

### ベクトル検索（numpy 必須）

```python
import numpy as np
from alopex import Database, Metric, TxnMode

db = Database.new()
with db.begin(TxnMode.READ_WRITE) as txn:
    vec = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    txn.upsert_vector(b"k1", None, vec, Metric.COSINE)
    results = txn.search_similar(vec, Metric.COSINE, 1, return_vectors=True)
    print(results[0].key, results[0].score)
    if results[0].vector is not None:
        print(results[0].vector.dtype, results[0].vector.shape)
```

#### NumPy 入出力とゼロコピー条件（v0.3.5）

入力（Python → Rust）:

- dtype: `float32` が優先。`float64` は `float32` に変換して処理します。
- layout: C-contiguous が優先。非連続（strided/Fortran order 等）は C-contiguous に変換して処理します。
- **ゼロコピー入力**: `float32` かつ C-contiguous の場合は Rust 側でコピーなしに参照します。

出力（Rust → Python）:

- `Transaction.search_similar(..., return_vectors=True)` の場合、`SearchResult.vector` に `numpy.ndarray[float32]` を含められます。
- `Transaction.search_similar(..., zero_copy_return=True)` / `Transaction.get_vector(..., zero_copy_return=True)` の場合、可能なら所有権移譲によるゼロコピー返却を行います（`False` の場合はコピー）。

GIL:

- `upsert_vector` / `search_similar` / `search_hnsw` は重い処理中に GIL を解放します。

### HNSW インデックス（numpy 必須）

```python
import numpy as np
from alopex import Database, HnswConfig, TxnMode

db = Database.new()
db.create_hnsw_index("idx", HnswConfig(2))

with db.begin(TxnMode.READ_WRITE) as txn:
    vec = np.array([1.0, 0.0], dtype=np.float32)
    txn.upsert_to_hnsw("idx", b"k1", vec, None)
    txn.commit()

results, stats = db.search_hnsw("idx", np.array([1.0, 0.0], dtype=np.float32), 1)
print(stats.node_count)
```

### Catalog API（polars 必須）

```python
import polars as pl
from alopex import Catalog, ColumnInfo

Catalog.create_catalog("main")
Catalog.create_namespace("main", "default")

columns = [ColumnInfo("id", "int", 0, False)]
Catalog.create_table("main", "default", "users", columns, "/tmp/users.parquet")

df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
Catalog.write_table(
    df,
    "main",
    "default",
    "users",
    delta_mode="overwrite",
    storage_location="/tmp/users.parquet",
)

lazy_frame = Catalog.scan_table("main", "default", "users")
print(lazy_frame.collect())
```

## 注意事項

- numpy / polars が未インストールの場合、対応 API は AlopexError を返します。
- Phase 1 では Parquet のみ対応しています。
