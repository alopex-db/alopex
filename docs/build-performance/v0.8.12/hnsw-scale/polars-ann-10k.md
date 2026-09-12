# Polars and ANN comparison

Codexは、GloVe `train[:10000]`（100次元）と `test[:200]` を同一入力、スレッド数1、3回の中央値で測定しました。構築時間と検索性能は別責務・別表に分離しています。

## Build responsibility

| engine | build ms | classification |
|---|---:|---|
| Polars exact scan | 112.284 | dataframe exact baseline |
| FAISS Flat | 4.816 | exact vector index |
| FAISS HNSW | 480.872 | ANN vector index |
| hnswlib | 1,881.494 | ANN vector index |

## Search responsibility

| engine | 200-query ms (median) | qps | classification |
|---|---:|---:|---|
| Polars exact scan | 622.859 | 321 | dataframe exact baseline |
| FAISS Flat | 1,142.016 | 175 | exact vector index |
| FAISS HNSW | 31.099 | 6,431 | ANN vector index |
| hnswlib | 32.595 | 6,136 | ANN vector index |

このartifactは短時間の位置づけ確認であり、受入判定ではありません。検索のrecallとAlopexの同一ユーザーAPI検索ケースが未取得のため、Codexは順位付けや性能同等性を主張しません。Polarsの検索計算は `numpy-via-polars-dataframe` と明示し、Polars自体のANN性能とは主張しません。Alopex Rust coreの構築値は別artifactで管理し、検索QPSとは比較しません。
