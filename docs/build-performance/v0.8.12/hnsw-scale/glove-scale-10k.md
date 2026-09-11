# GloVe scale: 10k

Codexは、GloVe `train[:10000]`（100次元）と `test[:200]` を同一入力、スレッド数1、3run中央値、10,000-query条件で測定しました。構築と検索は別責務・別CSVです。

## Build

| engine | build ms | index memory bytes | nodes |
|---|---:|---:|---:|
| Alopex HNSW | 94,149.296 | 13,594,624 | 10,000 |
| FAISS HNSW | 8,795.166 | 5,440,442 | 10,000 |
| hnswlib | 15,985.137 | 5,486,948 | 10,000 |
| FAISS Flat exact | 3.470 | 4,000,045 | 10,000 |

## Search

| engine | ef_search | QPS | recall@10 |
|---|---:|---:|---:|
| Alopex HNSW | 128 | 329.593 | 0.9825 |
| FAISS HNSW | 128 | 622.531 | 0.9805 |
| hnswlib | 128 | 926.543 | 0.9630 |
| FAISS Flat exact | 10,000 | 504.978 | 1.0000 |

このartifactは、Alopex Python APIと参照実装を同一入力・同一単位で比較する10kセルの完走証跡です。50k/200k/1Mセルは別ケースで、未完了・未実行状態を成功扱いにしません。Alopexの10k検索QPSは、Rust core build-only値とは比較していません。
