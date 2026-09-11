# Polars and ANN comparison

Codexは、GloVe `train[:10000]`（100次元）と `test[:200]` を同一入力として、3回のquery実行中央値を比較しました。PolarsはベクトルDBではないため、DataFrame保持後のNumPy exact scanを基準として分類しています。

| engine | build ms | 200-query ms (median) | qps | classification |
|---|---:|---:|---:|---|
| Polars exact scan | 112.284 | 622.859 | 321 | dataframe exact baseline |
| FAISS Flat | 4.816 | 1,142.016 | 175 | exact vector index |
| FAISS HNSW | 480.872 | 31.099 | 6,431 | ANN vector index |
| hnswlib | 1,881.494 | 32.595 | 6,136 | ANN vector index |

この比較は短時間の位置づけ確認であり、Issue #298の10,000-query・recall・scale受入の代替ではありません。Polarsのquery計算は `numpy-via-polars-dataframe` として明示し、Polars自体のANN性能とは主張しません。Alopexは別artifactのRust core build-only測定で管理します。
