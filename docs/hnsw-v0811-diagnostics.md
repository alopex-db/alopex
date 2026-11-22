# v0.8.11 HNSW diagnostics

## Status

The public Kaggle Version 4 notebook demonstrates the Alopex vector API. Its
timings are not release performance claims. For v0.8.17 Extended Verification,
the canonical performance evidence is `hnsw-diagnostic.json` inside the
exact-commit `parity-performance-public-*` artifact produced by Extended Verification
in [`parity-performance.yml`](../.github/workflows/parity-performance.yml).
The public artifact contains only the typed JSON/Markdown measurement pair and
the fixed JSON/Markdown run-status pair. It records the checked-out commit,
run ID/attempt, package version, and numeric measurements. Local environment,
raw observations, CSV views, and arbitrary diagnostic text stay in the separate
debug path and are not uploaded. Historical v3 artifacts remain diagnostic
provenance and are not accepted as v4 public evidence. After the
same commit is published under an `alopex-py-vX.Y.Z` tag, Stable Delivery only
checks exact-wheel reachability, validates this retained evidence against that
tag, and copies the JSON/Markdown pair into the public versioned report path.

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
last at least two seconds and execute at least 10,000 queries. The public JSON
contains typed numeric run rows and summary matrices; Markdown presents the
release summary and fastest settings at recall 0.95 and 0.99 (including explicit
`not met` cells). It contains no CPU identity, affinity or dependency inventory.

The allowlisted public benchmark artifact is retained for 30 days. A separate hosted job
retains the exact SHA, run ID/attempt, and terminal performance-job outcome for
90 days even when the self-hosted job times out or loses its runner. The public
versioned JSON/Markdown pair remains the durable successful release snapshot.

## Reproduce

```bash
python scripts/performance/hnsw_v0811_contract.py \
  --dataset /path/to/amazon-products.csv \
  --glove-dataset /path/to/glove-100-angular.hdf5 \
  --max-scale-n 50000 \
  --output artifacts/hnsw-diagnostic \
  --mode debug \
  --release-version 0.8.11
```

Extended Verification builds Alopex from the exact checked-out commit and pins
FAISS, hnswlib, NumPy, pandas, scikit-learn, and h5py. It also validates both
source dataset checksums before measuring. Stable Delivery never reruns this
benchmark against the PyPI wheel; the exact wheel is checked only for public
availability and identity because functionality and performance must be known
before release.
