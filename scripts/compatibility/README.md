# Old writer → candidate reader regression checks

These are the reusable versions of the v0.8.15 → v0.8.16 checks originally run
outside the repository. They test **data written by old software and read by new
software**. They do not require old software to read a later format.

The candidate Python wheel and Rust crates must come from the checkout under test.
The baseline is the published Python wheel and exact Rust crates at **0.8.15**
(source tag `d3917e1fa21d41097742a971e14094c0d6ae397e`). `provenance.json` records
the scope, source, generation command and limitations. These are Alopex local
regressions, not external-product conformance tests.

## Coverage

| Contract | Inputs and assertions |
| --- | --- |
| Direct same-DB upgrade | Old process writes, flushes, closes and reopens; candidate checks the original DB file hashes before opening the same path. SQL VECTOR(3/384), raw 3D vector, NULL, empty/NULL/Unicode/quoted/newline TEXT, old HNSW indexes without recreation; second reopen. |
| Legacy columnar | 2,000 TEXT rows in two 1,000-row groups with different encodings. Record the old reader's result independently; candidate must read every expected row. |
| Direct cross-language upgrade | Copies of DBs written by old Python and old Rust, opened by candidate Python and Rust. Typed scalar data, vectors, binary KV, existing HNSW with forced `HnswSearch`, second reopen. |
| Logical export/import | Python→Python, Rust→Rust, Python→Rust, Rust→Python. Explicit schema/index recreation; typed SQL bundle, raw KV hex and columnar SELECT→CSV. Reopen, query, insert and update. Scalar Parquet must preserve values. |
| External PyArrow | All five representations (List/FixedSizeList Float32/Float64 and JSON UTF8), both 3 and 384 dimensions. Exact original float32 bits, NULL and TEXT, reopen, exact nearest-neighbor oracle and forced HNSW. All ten cases must pass. |

Native CSV NULL/empty handling (#691), columnar Parquet export (#692) and VECTOR
Parquet COPY (#693) are separate probes in the report. A successful alternate route
**does not certify those native APIs**. Their existing defects are not silently
converted into successful compatibility results. No blanket export/reimport
requirement follows from this matrix. The samples do not cover every historical
DB/segment layout or performance.

## Run locally

Python 3.11+ is needed for the harness. Prepare two isolated virtual environments
with `numpy==2.2.1` and `pyarrow==26.0.0`; install `alopex==0.8.15` from a binary wheel
in the old environment and the checkout's built wheel in the candidate environment.
Do not install the published candidate as a substitute for a source-build CI check.

Before building, measure the entire host's Alopex working storage, including other
worktrees, `/tmp`, environments and Cargo caches. Keep one shared Cargo target and
serialize builds. At 40 GB, stop and clean regenerable outputs; 50 GB is the hard
per-host maximum. An inventory failure stops execution. `storage.py` discovers
linked worktrees and usual local roots; pass `--root` for any additional location.
It records the inventory and monitors child commands every 15 seconds.

```bash
export CARGO_TARGET_DIR="$PWD/target"
python scripts/compatibility/storage.py --log /tmp/alopex-compat-storage.jsonl \
  --additional-bytes 20000000000 -- bash scripts/compatibility/build-rust.sh
python -m unittest scripts.compatibility.test_runner
python scripts/compatibility/storage.py --log /tmp/alopex-compat-storage.jsonl \
  --additional-bytes 2000000000 -- \
  python scripts/compatibility/run.py \
    --old-python /path/to/old-env/bin/python \
    --new-python /path/to/candidate-env/bin/python \
    --candidate-wheel /path/to/checkout-built-candidate.whl \
    --old-rust target/debug/alopex-compat-baseline \
    --new-rust target/debug/alopex-compat-candidate \
    --output /tmp/alopex-compat-results
```

The output directory must be empty. `--python-only` explicitly excludes Rust for a
focused check; CI always uses the full matrix. Missing Rust binaries, a missing
case, an unsuccessful Arrow representation, or a failed child process cannot
produce a full-matrix pass. Reports record native module/binary hashes, dependency
versions, source HEAD, script hashes and each process's exit code. Generated DBs
are removed even on failure. Keep the compact reports/logs/CSV/Parquet evidence;
delete the dedicated venvs and wheels after use, and remove build outputs once no
active check needs them. Never delete another task's files.

## CI ownership

The existing Linux/NumPy `alopex-py` job runs this matrix using its already-built
release wheel and shared Cargo target. It builds the baseline and candidate Rust
harnesses serially, creates an isolated old Python environment, and compares old
writers against the **PR checkout**, not just two published packages. Evidence is
uploaded on failure as well as success. The workflow's ordinary `ci-success`
aggregator includes that job. No extra release tag, release run or local runner is
needed. Tests and workflow changes trigger this lane through its path filters.

## Updating the candidate

The old Rust lockfile is a frozen registry baseline. The candidate Rust manifest
uses local path dependencies; when the workspace version or dependencies change,
regenerate its lockfile (`cargo generate-lockfile --manifest-path
scripts/compatibility/rust-current/Cargo.toml`) in the same change and review the
diff. CI builds both with `--locked` so dependency drift cannot change a passing
result silently. Do not replace the old writer with the candidate writer.
