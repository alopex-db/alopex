# v0.8.11 HNSW diagnostics

## Status

The public Kaggle Version 4 notebook demonstrates the Alopex vector API. Its
timings are not release performance claims. The canonical v0.8.11 performance
evidence is the `hnsw-diagnostic-*` artifact produced after publication by
[`post-release-hnsw.yml`](../.github/workflows/post-release-hnsw.yml).

## What the artifact answers

The artifact uses the issue #298 Amazon attachment for recall, latency, and
hybrid-search diagnostics. It also uses checksum-pinned `glove-100-angular` for
scale measurements.

- Recall: index/input counts, all-vector self-match, `ef_search` ceiling,
  `ef_construction` 50/200/800, Alopex M=32 versus competitor M=16, and exact
  normalized L2/inner-product ordering.
- Latency: the 10-vector/k=1/ef=1 call floor plus actual, fixed, and residual
  exploration time for each engine and `ef_search`. Alopex `SearchStats`
  supplies the Rust-internal search time, so the report also isolates the
  Python binding residual on the identical query/index path.
- Hybrid search: Alopex SQL + HNSW post-filter, stdlib SQLite + hnswlib, and
  filtered exact search at 0.1%, 1%, 5%, 20%, and 100% selectivity.
- Scale: build time, index bytes, memory, recall/QPS curves, fastest QPS at
  recall >= 0.95, brute-force crossover, and gap trend at 10k and 50k rows.
  The hosted runner records 200k and 1M as explicit execution-limit rows.

Every timed setting warms one full query cycle and records three runs that each
last at least two seconds and execute at least 10,000 queries. JSON contains the
full result, raw JSON preserves runs, CSV files expose each matrix, and Markdown
provides the release summary.

## Reproduce

```bash
python scripts/performance/hnsw_v0811_contract.py \
  --dataset /path/to/amazon-products.csv \
  --glove-dataset /path/to/glove-100-angular.hdf5 \
  --max-scale-n 50000 \
  --output artifacts/hnsw-diagnostic \
  --release-version 0.8.11
```

The workflow pins Alopex to the exact released wheel and pins FAISS, hnswlib,
NumPy, pandas, scikit-learn, and h5py. It also validates both source dataset
checksums before measuring.
