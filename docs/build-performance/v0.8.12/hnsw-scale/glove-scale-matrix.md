# GloVe scale matrix (fixed-query)

Input: data/issue-298-glove-v2@8735a7fe; SHA-256: 544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4. Each cell uses fixed 10,000 queries × 3 runs; 50,000-node cells use independent engine×N workers, while 10,000-node rows reuse the fixed-query canonical artifact.

| N | engine | build s | QPS at recall>=0.95 | recall | ef |
|---:|---|---:|---:|---:|---:|
| 10000 | alopex-hnsw | 113.711 | 167.331 | 0.9825 | 128 |
| 10000 | faiss-hnsw | 17.105 | 577.945 | 0.9804999999999999 | 128 |
| 10000 | flat | 0.011 | 490.562 | 1.0 | 10000 |
| 10000 | hnswlib | 15.165 | 490.389 | 0.9555 | 128 |
| 50000 | alopex-hnsw | 1204.081 | 96.748 | 0.9745 | 256 |
| 50000 | faiss-hnsw | 68.687 | 281.317 | 0.9740000000000001 | 256 |
| 50000 | flat | 0.034 | 117.494 | 1.0 | 50000 |
| 50000 | hnswlib | 95.345 | 350.988 | 0.958 | 256 |

Pending sizes: 200000, 1000000. These cells have no result until all engines complete the same worker contract.
