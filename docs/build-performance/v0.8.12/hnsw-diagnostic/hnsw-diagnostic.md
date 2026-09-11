# HNSW diagnostic

Release: `local`. Dataset: `9171 x 128`; minimum queries/run: `10000`; seed: `42`.

Codexはこのartifactを旧「時間下限 + query下限」契約の履歴証跡として保持します。固定query数契約へ移行したため、Codexはこのartifactをv0.8.12の製品間受入比較には使用せず、同じケースを新契約で再生成します。

| engine | ef_search | recall@10 | tie-aware recall@10 | QPS | us/query |
|---|---:|---:|---:|---:|---:|
| alopex-hnsw | 16 | 0.9965 | 0.9970 | 2293.9 | 435.9 |
| alopex-hnsw | 32 | 0.9990 | 0.9995 | 1105.2 | 904.8 |
| alopex-hnsw | 64 | 0.9995 | 1.0000 | 725.3 | 1378.7 |
| alopex-hnsw | 128 | 0.9995 | 1.0000 | 760.7 | 1314.6 |
| alopex-hnsw | 256 | 0.9995 | 1.0000 | 203.1 | 4924.2 |
| faiss-flat-exact | 9171 | 1.0000 | 1.0000 | 586.1 | 1706.3 |
| faiss-hnsw | 16 | 0.9965 | 0.9970 | 3986.0 | 250.9 |
| faiss-hnsw | 32 | 0.9985 | 0.9990 | 2556.7 | 391.1 |
| faiss-hnsw | 64 | 0.9995 | 1.0000 | 1553.6 | 643.7 |
| faiss-hnsw | 128 | 0.9995 | 1.0000 | 1014.4 | 985.8 |
| faiss-hnsw | 256 | 0.9995 | 1.0000 | 645.7 | 1548.7 |
| hnswlib | 16 | 0.9980 | 0.9980 | 4428.4 | 225.8 |
| hnswlib | 32 | 0.9990 | 0.9995 | 2647.8 | 377.7 |
| hnswlib | 64 | 0.9995 | 1.0000 | 1708.1 | 585.4 |
| hnswlib | 128 | 0.9995 | 1.0000 | 1042.3 | 959.4 |
| hnswlib | 256 | 0.9995 | 1.0000 | 623.0 | 1605.2 |

## Recall ceiling conclusion

strict top-k boundary tie

## Hybrid

Alopex advantageous selectivities: `[]`. Filter-aware traversal: `false`.

## Scale

Brute-force crossovers: `{}`. Limits: `[]`.
