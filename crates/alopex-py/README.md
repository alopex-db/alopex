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
