# VECTOR Parquet interchange in v0.8.17

`COPY ... TO/FROM ... WITH (FORMAT PARQUET)` supports explicitly declared
`VECTOR(dimension, metric)` columns in row and columnar tables. Parquet has no
dedicated VECTOR type: Alopex exports standard LIST values containing FLOAT
elements (Arrow `List<Float32>`).

## Representation and validation

| Property | Contract |
| --- | --- |
| Output | Nullable outer `List<Float32>`; the length of each non-NULL vector equals its declared dimension |
| Input | Arrow List, LargeList, or FixedSizeList with Float32 or Float64 elements |
| Float64 input | Round to Float32; reject NaN, infinity, or overflow to nonfinite Float32 |
| NULL | Whole vectors may be NULL if the destination allows it; NULL elements are rejected |
| Dimension | Exact destination dimension required, including for empty input lists |
| Other elements | Integer, string, and nested lists are not implicitly converted to VECTOR |
| Metadata | `alopex.vector.dimension` and `alopex.vector.metric` (`L2`, `COSINE`, `INNER`) are advisory field metadata |

Create the destination table explicitly with the intended dimension, metric,
constraints, and storage format. Its DDL governs validation even if external
metadata is missing or conflicts with it. Standard Parquet readers can read
the list values without understanding Alopex metadata. `READ_PARQUET` infers
floating arrays, not VECTOR columns from metadata; use COPY into a declared
VECTOR column for this conversion. `INSERT ... SELECT read_parquet(...)` is
not an additional implicit ARRAY-to-VECTOR conversion path.

Invalid input fails transactional COPY without leaving a partially imported
table. COPY TO retains temporary-file replacement semantics. Existing
materialization behavior is unchanged; this feature does not establish a
bounded-memory export or large-data performance guarantee.

## Export, import, and recreate the index

```sql
-- On the source database, using v0.8.17:
COPY vectors TO '/absolute/path/vectors.parquet' WITH (FORMAT PARQUET);

-- On a fresh destination database:
CREATE TABLE vectors (id INTEGER PRIMARY KEY, e VECTOR(3, L2));
COPY vectors FROM '/absolute/path/vectors.parquet' WITH (FORMAT PARQUET);
CREATE INDEX ih ON vectors(e) USING HNSW;
SELECT id FROM vectors
ORDER BY vector_distance(e, [1,2,3], 'l2') LIMIT 1 WITH (enable_hnsw=true);
```

The Parquet file contains column values and advisory metadata, not the full
DDL, constraints, or HNSW index. Recreate those separately. HNSW SQL search
currently requires row storage. Columnar VECTOR export/import is supported;
to search that exported data with HNSW, import it into an explicitly created
row table before creating the index. Flush, close, reopen, and compare every
value in the destination before treating a transfer as verified.

## Exporting from a published older binary

Published v0.8.15 and v0.8.16 still reject native VECTOR `COPY TO PARQUET`.
Updating a reader does not add export support to those binaries. An explicit
interchange path is to SELECT source values and use PyArrow in the old-version
process:

```python
import pyarrow as pa
import pyarrow.parquet as pq

rows = old_db.execute_sql("SELECT id,e FROM vectors ORDER BY id")
table = pa.table({
    "id": pa.array([row["id"] for row in rows], pa.int32()),
    "e": pa.array([row["e"] for row in rows], pa.list_(pa.float32())),
})
pq.write_table(table, "vectors.parquet")
assert pq.read_table("vectors.parquet").to_pylist() == rows
```

In a separate v0.8.17 process, create matching DDL, COPY FROM the file, reopen
the destination, compare all values, and recreate HNSW. Keep the source until
that comparison succeeds. This is an optional external interchange workflow;
it does not mean every existing database must be migrated through Parquet
before a newer binary can open it. Direct database compatibility is a separate
contract. The example SELECT materializes the source; the measured acceptance
below covers a tiny fixture, not a large-database migration.

## Development evidence and reproduction

The regression tests use at most three rows, with 3- and 384-dimensional
vectors. They run as functional tests in the existing Rust/Python workflows;
no throughput, load, or timing threshold is added to normal CI.

- Rust converter: eight tests for list routes, rounding, NULL, dimensions,
  finite values, and output metadata; mutation check caught all 12 compilable
  mutants (four default-value replacements cannot compile).
- Rust public Database: four row/columnar × dimension cases with disk reopen,
  complete element comparison, and HNSW recreation in row storage.
- Python: 34 VECTOR cases, including independent PyArrow reads, external list
  formats and Float32/Float64, atomic rejection, DDL authority, and NOT NULL.
  Four existing Parquet tests also passed.
- Manual legacy comparison: published PyPI 0.8.15 and 0.8.16, separate import
  processes, macOS arm64 / Python 3.13.16 / PyArrow 26.0.0. Each preserved two
  3-dimensional vectors and one NULL vector through disk reopen, independent
  PyArrow reading, v0.8.17 development import, and HNSW recreation. Both old
  native COPY calls returned the expected unsupported-format error.

The manual harness records imported version/module identity, Parquet SHA-256,
all values, and the old COPY error. Run each mode with its respective isolated
Python environment; `--directory` must initially be absent for export:

```sh
old-python scripts/compatibility/vector_parquet_legacy.py export \
  --directory /tmp/vector-transfer --expected-version 0.8.16
new-python scripts/compatibility/vector_parquet_legacy.py import \
  --directory /tmp/vector-transfer --expected-version 0.8.17
```

These are development results. Candidate CI and released-wheel qualification
must be recorded against their own source/artifact identities in
[#690](https://github.com/alopex-db/alopex/issues/690) before accepting
[#693](https://github.com/alopex-db/alopex/issues/693) for release.
