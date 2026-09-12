# GloVe scale fixed-query 10k

Codexはデータブランチ`data/issue-298-glove-v2`（commit `8735a7fe`）から再構成し、SHA-256 `544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4`を検証した入力で測定しました。各設定は固定10,000 query×3 runです。

| N | engine | QPS at recall>=0.95 | recall | ef |
|---:|---|---:|---:|---:|
| 10000 | alopex-hnsw | 167.331 | 0.9825 | 128 |
| 10000 | faiss-hnsw | 577.945 | 0.9804999999999999 | 128 |
| 10000 | hnswlib | 490.389 | 0.9555 | 128 |
| 10000 | flat | 490.562 | 1.0 | 10000 |
