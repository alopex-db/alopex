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
| Inherited Python API behavior | Replay an explicit case-ID allowlist from 16 v0.8.15 files against both isolated packages: SQL/DDL/DML/types/bindings, KV and transaction lifecycle, CSV streams, DataFrame and pinned Polars, catalog/metadata/errors, connection dispatch, async wrappers, credentials and remote-client local HTTP stubs. Every old-passing test must pass in the candidate; skipped/failed baseline cases are listed separately. |
| Public Python surface | Compare all discovered public symbols from `alopex`, `alopex.asyncio`, `alopex.remote`, `alopex.protocols`, including classes/members, callable signatures where metadata exists, defaults and stable error-code preservation. Optional additions are allowed. Presence/signature coverage does not replace behavior assertions. |
| Rust API source and behavior | Compile the same old client against frozen registry crates and candidate local-path crates. Ten groups exercise KV CRUD/scan, read-only/commit/rollback, SQL CRUD/constraints/metadata, prepared bind/reset/finalize, SQL sessions, CSV reader/writer, catalog hierarchy, DataFrame/SQL streaming, memory/config/clone/snapshot/clear, chunked Blob/Typed values and cancel. |
| Direct same-DB upgrade | Old process writes, flushes and closes; candidate checks the original DB file hashes before opening the same path. SQL VECTOR(3/384), raw 3D vector, NULL, empty/NULL/Unicode/quoted/newline TEXT, old HNSW indexes without recreation; second reopen. |
| Legacy columnar | Four TEXT rows (`apple`, `apricot`, `zebra`, `yak`), SQL row-group size 1000 (the minimum accepted by the public API). Candidate must read all rows and reopen. No old-reader self-read diagnostic. This public SQL fixture does not certify a particular on-disk encoding; an independent Rust case writes two 2-row groups with the published old codec, asserts their different encodings, and reads those exact bytes with the candidate. |
| Direct cross-language upgrade | Copies of DBs written by old Python and old Rust, opened by candidate Python and Rust. Typed scalar data, vectors, binary KV, existing HNSW with forced `HnswSearch`, second reopen. |
| Logical export/import | Python→Python, Rust→Rust, Python→Rust, Rust→Python. Explicit schema/index recreation; typed SQL bundle, raw KV hex and columnar SELECT→CSV. Reopen, query, insert and update. Scalar Parquet must preserve values. |
| External PyArrow | All five representations (List/FixedSizeList Float32/Float64 and JSON UTF8), both 3 and 384 dimensions. Exact original float32 bits, NULL and TEXT, reopen, exact nearest-neighbor oracle and forced HNSW. All ten cases must pass. |

Native CSV NULL/empty handling (#691), columnar Parquet export (#692) and VECTOR
Parquet COPY (#693) have separate issue acceptance. Logical reports retain their existing native probes; the external Arrow cases no longer invoke unrelated native COPY diagnostics. A successful alternate route
**does not certify those native APIs**. Their existing defects are not silently
converted into successful compatibility results. No blanket export/reimport
requirement follows from this matrix. The samples do not cover every historical
DB/segment layout or performance.

## Run locally

Python 3.11+ is needed for the harness. Prepare two isolated virtual environments
with `numpy==2.2.1` and `pyarrow==26.0.0`; install `alopex==0.8.15` from a binary wheel
in the old environment and the checkout's built wheel in the candidate environment.
Both environments also need `pytest==9.1.1` and `polars==1.43.2` for the frozen API
corpus. The pinned v0.8.15 Git commit must exist locally (CI fetches it once if missing).
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
python -m unittest scripts.compatibility.test_runner scripts.compatibility.test_api
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

`api-cases.json` is the machine-readable behavior corpus inventory, provenance,
required dependency set and explicit exclusions. The archived historical test
files remain unchanged. Only explicit case IDs run, with declared replacements for
the same-process ownership prose assertion and two configurable streaming cases.
The 34,250-row case is excluded; fixed internal Arrow boundary cases retain
1023/1024/1025 rows, while configurable streaming uses five rows and chunk size two. Its replacement preserves the
exception class/code, second-open rejection and reopening after handle release;
`api-cases.json` pins the implementation change and exact old/new case IDs.
A missing family, failed collection or skipped case
that used to pass cannot silently reduce coverage. The driver records the test
source hashes and old/new per-case setup/call/teardown outcomes. It also records
baseline failures and skips without counting them as verified compatibility.
The remote client is exercised against local HTTP stubs. Real old/new server
wire compatibility and the whole public Rust API are not certified by these
curated cases; existing server CI remains separate.

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

## Independently produced live WAL (manual #682 acceptance)

`live_wal.rs` is compiled into two core-only clients: `wal-v0816` pins the
published registry crate at `=0.8.16`; `wal-current` uses this checkout. Each has
its own committed dependency lock, but both builds use the host's one shared
Cargo target. They need no SQL parser, Python extension, or performance workload.

After the host inventory and a bounded build plan (the example caps additional
storage at 2 GB for core-only development builds), build serially:

```bash
export CARGO_TARGET_DIR="$PWD/target"
python scripts/compatibility/storage.py --log /tmp/alopex-live-wal-storage.jsonl \
  --additional-bytes 2000000000 -- \
  cargo build --locked --manifest-path scripts/compatibility/wal-v0816/Cargo.toml
python scripts/compatibility/storage.py --log /tmp/alopex-live-wal-storage.jsonl \
  --additional-bytes 2000000000 -- \
  cargo build --locked --manifest-path scripts/compatibility/wal-current/Cargo.toml
python scripts/compatibility/storage.py --log /tmp/alopex-live-wal-storage.jsonl \
  --additional-bytes 1000000 -- \
  python scripts/compatibility/live_wal.py \
    --legacy target/debug/alopex-wal-v0816 \
    --candidate target/debug/alopex-wal-current \
    --output /tmp/alopex-live-wal-result
```

Use a new output directory. The six serial subprocesses, each limited to 30
seconds, check:

1. Published 0.8.16 commits `old=ok` with `EveryWrite`, a single 64-byte ring
   payload and automatic checkpoint disabled, then exits without destructors.
   The parent asserts that the only data file is a 112-byte v2 WAL, with no SST
   or checkpoint. This is not a closed-container fixture or a rewritten header.
2. The candidate's default public LSM open recovers exactly one entry with the
   LSN recorded by the old writer, no checkpoint, warning or premature stop,
   and the expected value. The file bytes remain unchanged before activation.
3. The candidate's explicit overflow open independently recovers that same old
   WAL and commits a new 65-byte value. The old value remains intact.
4. The actual old reader rejects WAL v3 with the precise unsupported-version
   reason wrapped in its `CorruptedSegment` error and leaves all data file
   bytes unchanged.
5. Two separate candidate reopens each recover the committed reference and read
   both expected values. The new value depends on overflow recovery.

All fixture files together must stay below 64 KiB. The JSON receipt records
the registry crate checksum, both binary hashes, manifest/lock/client hashes,
checkout HEAD, file sizes/hashes and each process result. Keep build logs and
the exact tested source identity alongside it; the receipt alone does not prove
how an arbitrary supplied binary was built. Retain compact evidence and remove
the owned fixture (`output/store`) and regenerable build outputs after the run.
Do not add this manual acceptance driver to ordinary CI.

This covers native LSM process-exit recovery on supported platforms, not a kill
during spool/reference publication, power loss, Windows public overflow support,
or public SQL/Python integration. Those are distinct #682 acceptance items.

## Explicit overflow upgrade from a published container

`wal_overflow.py` is a separate, bounded public-API check for #682. Run each phase
in its own process, in this order, using an empty evidence directory and a fresh
`.alopex` database path:

1. Published `alopex==0.8.16` Python: `legacy-write` (one key, `old=ok`, then close).
2. Checkout-built Python: `upgrade` (verify old file hashes, opt in to a 64-byte
   ring, read the old value, commit one new 65-byte value, close).
3. The same published Python: `legacy-reject` (require the explicit incompatible
   version error and unchanged database/sidecar bytes).
4. The same checkout-built Python: `reopen` (verify both values).

Each invocation takes `--db <fresh-path.alopex> --evidence <directory>`. Candidate
phases additionally require `--candidate-native-sha256 <sha256-of-loaded-extension>`;
record that binary's build command and source commit separately. Old phases check
the installed distribution version (default `--legacy-version 0.8.16`). Each phase
writes a JSON receipt with native-module hash, database-file hashes and outcome;
missing or failed phases do not constitute acceptance. Keep failures separately
when rerunning with a new fixture. The ordinary historical API matrix remains
pinned to 0.8.15; this check does not relabel those results as 0.8.16 evidence.

Use the per-host storage inventory above, serialize builds through the shared
Cargo target, and set a 30-second timeout per phase in the invoking process
(for example, `subprocess.run(command, check=True, timeout=30)`). Closed files
recorded by each phase may not exceed 1 MiB. The reference fixture's closed old
container is only
468 bytes; its default old WAL is discarded by normal close, not retained as
a live-WAL recovery fixture. Use existing isolated environments where source
identity is established; otherwise install binary wheels only for the baseline.
Delete owned environments, generated DBs and build outputs after saving receipts.

This check is currently a manual acceptance command on supported Unix platforms,
not a load/performance job or Windows qualification. It certifies closed-container
upgrade and reader fencing, not every legacy WAL/segment layout, crash recovery,
or a downgrade path. Development parser overrides do not qualify release assets.

## Updating the candidate

The old Rust lockfile is a frozen registry baseline. The candidate Rust manifest
uses local path dependencies; when the workspace version or dependencies change,
regenerate its lockfile (`cargo generate-lockfile --manifest-path
scripts/compatibility/rust-current/Cargo.toml`) in the same change and review the
diff. CI builds both with `--locked` so dependency drift cannot change a passing
result silently. Do not replace the old writer with the candidate writer.

## Known Rust source incompatibility: #696

The runtime cases above do **not** mean that every old Rust source program builds.
The unchanged public-struct client in `legacy_config_literal.rs` builds against
0.8.15 but fails against 0.8.16 with `E0063`: `EmbeddedConfig` gained the public
`hnsw_cache_bytes` and `hnsw_cache_entries` fields. The candidate preserves the
six-field shape published in 0.8.16. This is tracked in
https://github.com/alopex-db/alopex/issues/696 (v0.8.17).

With the same host budget guard and shared target used above, reproduce it with:

```bash
# Historical writer/client: compiles (unset candidate parser overrides first).
env -u NIM_SQL_PARSER_LIB_DIR -u ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD \
  cargo check --locked --manifest-path scripts/compatibility/rust-old/Cargo.toml \
  --features legacy-config-literal
# Candidate: currently fails with E0063; this is not a passing compatibility case.
cargo check --locked --manifest-path scripts/compatibility/rust-current/Cargo.toml \
  --features legacy-config-literal
```

The failing compile witness remains separate from the passing runtime matrix.
It must not be reported as qualified or silently replaced with a constructor.
The [v0.8.17 source policy](../../docs/rust-config-compatibility.md) preserves
the published six-field shape and existing constructors, and routes new settings
through the private-field `DatabaseOptions` builder. Four-field clients require
the documented migration.

`source_config.py --output <directory>` records nine compile-only cases using
the same shared target and storage guard as above. Its locked `rust-v0816`
registry graph adds the second published version; `rust-old` stays frozen at
0.8.15. The report distinguishes seven supported cases from two
`known_incompatible` four-field cases (exact E0063, exit 101). The overall
`declared_contracts_match` status does not mean all old source is compatible.
Ordinary Linux CI retains this report alongside, but separately from, the
runtime matrix. No database data is created by these clients.

## Case size and execution budgets

These are correctness checks, with one execution per writer/reader and no warmup,
throughput, latency, scale, or load acceptance. Every native and external Arrow
case has its own process, DB path, result, and log. Logical directions already
have independent reader processes and databases. Python API behavior retains
pytest case IDs and per-case outcomes; filenames cannot implicitly expand coverage.

| Case IDs | Fixture budget | Reason |
| --- | --- | --- |
| native/sql-3, native/sql-384 | 5 rows each, 4 non-NULL + 1 NULL | Exact float32 bits, text variants, retained HNSW and NULL contract |
| rust-columnar | 4 rows, 2 groups of 2, <16 KiB | Old codec V2 bytes plus metadata, asserted mixed encodings, candidate read and reopen |
| native/raw-3 | 1 key, 3 floats | Raw vector API persistence |
| native/columnar | 4 rows, SQL row-group size 1000 | TEXT persistence through public SQL, candidate read and reopen |
| arrow/{list_f32,list_f64,fixed_f32,fixed_f64,json_utf8}-{3,384} | 5 rows each | Representation, precision, dimension, NULL and recreated HNSW contract |
| logical/{Python,Rust}→{Python,Rust} | scalar 3, typed 1, vectors 2, columnar 2, KV 2 | Types and directional interoperability, not volume |
| API configurable batches | 5 rows, chunk size 2 | Two full batches plus a tail |
| API fixed Arrow batches | 1023 / 1024 / 1025 rows | Internal 1024-row boundary cannot be configured by this API |

The driver runs children serially, with a 300-second fail-closed timeout per
phase, and no retries. The CI wrapper caps additional storage at 2 GB and stops
at the host's 40 GB threshold. Timeouts, aborts and missing receipts fail the run.
Native logs flush before and after create/write/flush/close/open/read/reopen.
DBs are removed in the driver's finally block; compact logs and receipts remain.
Builds use the existing wheel and one shared Cargo target, not one build per case.

To investigate a single contract after preparing environments, set
`ALOPEX_COMPAT_OUTPUT` to a new owned directory and `ALOPEX_COMPAT_TARGET_VERSION`
to the candidate version, then run `native.py old sql-3` with the old interpreter
and `native.py new sql-3` with the candidate. Arrow accepts
`arrow.py export list_f32 3` / `arrow.py import list_f32 3` similarly. A focused
case is not a full-matrix pass. Remove its owned DB after retaining the receipt.

Normal Python CI explicitly ignores `tests/benchmarks` and `test_benchmark_nfr13.py`.
Performance workflows are manual only, require an owning issue, and must have
separate input/resource/cleanup budgets before dispatch. Their results do not
qualify ordinary correctness CI.

## Public API WAL publication boundaries (manual development acceptance)

`crates/alopex-embedded/tests/wal_publication.rs` and `publication_python.py`
each exercise 24 independent cases: Database/KV and SQL, six persistence
boundaries, error return and immediate process exit. They use one prior and two
pending keys/rows, a largest value of 65 bytes and a 64-byte WAL ring payload.
Python also caps each physical fixture at 1 MiB. Child processes have a 30-second
deadline; there are no retries, throughput assertions or load loops.

These tests require an explicit **debug instrumented build** with
`RUSTFLAGS='--cfg alopex_wal_test_hooks'`. Hooks are thread-local and consumed on
first hit. This is deliberately not a Cargo feature: ordinary builds, including
`--all-features`, contain neither the controls nor the private Python
`_arm_wal_test_fault` entrypoint. Instrumented compilation without debug
assertions fails at compile time. Do not ship an instrumented extension.
These commands are manual; no ordinary CI workflow enables the cfg.

Before building, measure the host, reserve bounded additional space, and use
the same shared target and `storage.py` guard described above. Serialize these
commands, retain source/tree and binary hashes with the logs, then delete the
owned extension copies and generated target outputs. Use the configured parser
toolchain and the same interpreter for the extension and Python runner:

```bash
RUSTFLAGS='--cfg alopex_wal_test_hooks' cargo test --locked \
  -p alopex-embedded --test wal_publication
RUSTFLAGS='--cfg alopex_wal_test_hooks' PYO3_BUILD_EXTENSION_MODULE=1 \
  PYO3_PYTHON="$PYTHON" cargo build --locked -p alopex-py
# Install/copy this debug extension alongside crates/alopex-py/python/alopex
# in an owned temporary package directory, then point PYTHONPATH there.
"$PYTHON" scripts/compatibility/publication_python.py --output "$NEW_OUTPUT"
```

The Rust result is 24 passed plus one ignored child helper; each parent invokes
that helper explicitly in its own process. Python fails if the instrumented
entrypoint is absent and writes a per-case report plus native/driver SHA-256.
Run Python without `-O` so its assertions remain enabled.

The boundaries are after spool write, after spool sync, after spool publication,
before reference append, after reference append/sync, and after forced reference
sync. With `EveryWrite`, the last two recover the entire pending transaction
even when commit returned an error. Earlier boundaries recover only the prior
transaction. Error-return cases also verify that the live database has no pending
rows and that uncertain reference outcomes reject further writes until reopen.
Every case checks two open/close cycles, exact values, and independent SQL primary
and unique key enforcement. Forced exit avoids destructor-driven convergence.

Afterward rebuild the ordinary extension without `RUSTFLAGS`, import that exact
copy in a fresh process and assert
`not hasattr(alopex._alopex, '_arm_wal_test_fault')`. Preserve its separate hash.
The existing **Compatibility** workflow has an explicit manual
`acceptance_suite=windows-overflow` selection. Dispatch it on the exact reviewed
branch, without adding a workflow to main. It runs only the hosted Windows
public overflow acceptance job; ordinary PR/push and scheduled selection is
unchanged. The manual job builds a normal extension (15 small API cases and
hook absence), runs the instrumented Rust24/Python24 cases, then checks the
published 0.8.16 wheel/container and independently built registry-core/live-WAL
clients. It retains source/tree/native identities and per-phase receipts.

`windows_overflow_acceptance.py prepare` measures the isolated runner before
toolchain/build work and declares at most 28 GB additional storage. All product
builds use one target, two jobs, no incremental/debug symbols, and the existing
storage watchdog. A measurement error, declared growth exhaustion, or 40 GB
host usage stops work. The always-run cleanup removes target, owned packages,
venvs, fixtures and parser binaries after saving compact receipts. The workflow
uploads only JSON/JSONL/log/XML, never the built DLLs or test databases.

The four local guard tests need only the standard library:
`python -m unittest discover -s scripts/compatibility -p test_windows_overflow_acceptance.py`.
They reject nonmanual/self-hosted execution, missing/duplicate cases, hash
mismatches, failure/skip receipts and a Rust summary without actual case names.
They do not execute a native build or establish Windows acceptance.

The harnesses support native Unix and Windows. Existing non-Windows results
do not qualify Windows: run the same Rust24/Python24 cases against the exact
Windows source/build and retain their identities separately. These controlled
process exit/error checks do not simulate power loss, partial OS writes or
failing hardware fsync, and do not qualify a release wheel. Windows package
setup must also retain the package-local `native` DLL directory.

### Real OS spool write refusal and termination

`spool_os_write.py` uses the **ordinary extension without test hooks** on
Linux/macOS. Run it manually under the same host storage guard after preparing
an owned package directory with the normal extension:

```bash
"$PYTHON" scripts/compatibility/spool_os_write.py --output "$NEW_OUTPUT"
```

Eight separate child processes cover KV/SQL × file-size limit 0/32 bytes ×
returned EFBIG/SIGXFSZ termination. Each child disables core dumps and applies
`RLIMIT_FSIZE` only after opening the database and staging two pending records.
A 33-byte probe confirms the OS's short-write and EFBIG behavior in that same
child. stdout/stderr use pipes so the limit cannot break receipt output.
The signal cases leave an actual 0/32-byte temporary spool; the parent verifies
its size and, for the partial header, the spool magic before recovery. Error
cases require the public Python I/O error (not the configured capacity error)
and immediate temporary-file cleanup.

Every case verifies two recovery cycles with the old data only, independent SQL
primary/unique keys and no leftover spool, then a successful normal commit and
reopen. Fixtures have three records at most, largest value 65 bytes, a 1 MiB
physical cap, and a 30-second child timeout. DB fixtures are removed in `finally`;
logs and source/native hashes remain. Run without Python `-O`; unsupported hosts
and instrumented extensions fail before creating fixtures. No ordinary CI
workflow invokes this manual OS-limit test.

This verifies real spool write refusal and partial-header recovery. It does not
prove reference short-write handling, OS fsync-failure behavior, storage power
loss, the Windows public overflow API, or release-wheel qualification.

### Public API recovery from damaged reference/spool bytes

`publication_corruption.py` adds eight manual KV/SQL cases using the ordinary
extension. A bounded child commits two pending records and exits without close,
preserving the live WAL. After that child has exited, the parent modifies only
owned fixture bytes: reference CRC, a zeroed second half of the reference,
spool seal CRC, or a one-byte-truncated seal. The fixture checks the WAL's
20-byte section header, 28-byte segment header, v3 version and reference CRC
before damage; a format change fails the fixture explicitly.

```bash
"$PYTHON" scripts/compatibility/publication_corruption.py --output "$NEW_OUTPUT"
```

Bad reference tails recover only the prior transaction, retain the uncertain
spool evidence on the first open, and allow a subsequent complete commit.
Referenced bad spools instead reject open twice, preserving the WAL and damaged
spool bytes. Restoring the saved exact original spool then recovers the entire
transaction across two open/close cycles. Exact values and independent primary
and unique keys guard against partial or duplicate replay.

The same three-record/65-byte-value, 1 MiB fixture and 30-second child bounds
apply. Use the storage guard, shared target and cleanup policy above. Reports
record source/native hashes; ordinary CI does not invoke this runner. These
are **offline damaged-byte recovery** checks. Zeroing a reference tail does not
simulate an actual short-write syscall; none of these cases qualifies fsync
failure, power loss, the Windows public API or release assets.

## Actual reference short writes (manual Linux acceptance)

`reference_os_write.py` uses the explicit debug-only `alopex_wal_test_hooks`
configuration described above. Its additional private Python hook pauses once
**after** the complete spool has been synced and published, but **before** the
WAL reference write. The parent verifies that boundary, uses Linux `prlimit`
to lower only that child's `RLIMIT_FSIZE`, and acknowledges the pause over a
pipe. The hook supplies no synthetic I/O error: the kernel rejects the write
with EFBIG or terminates the child with SIGXFSZ.

```bash
"$PYTHON" scripts/compatibility/reference_os_write.py --output "$NEW_OUTPUT"
```

Eight separate cases cover KV/SQL, returned error/OS termination, and zero or
26 reference bytes written. The WAL is already allocated to 112 bytes; limits
of 48/74 bytes target the reference inside it. The test compares its exact
persisted prefix with the sealed spool identity, checks that bytes beyond the
limit and the sealed spool are unchanged, and verifies the poisoned writer on
returned errors. Two reopens preserve only the prior transaction and reclaim
the unreferenced spool; a later normal commit recovers both pending records and
SQL primary/unique keys. Each child has a total 30-second deadline, prior1 +
pending2 records, a 65-byte value and a 1 MiB fixture cap. Reports bind results
to native/source hashes. Retain failed logs and clean generated outputs using
the per-host storage policy above.

This runner is manual, Linux-only, and requires the instrumented **debug**
extension. Normal builds (including all features) expose neither private hook;
release builds reject the custom cfg. No ordinary CI enables it. These cases
qualify actual reference write failure, not OS fsync failure, power loss,
Windows support, performance, or release package identity.

## Sync syscall failures (manual Linux acceptance)

`sync_syscall.py` uses an **ordinary** Python extension and Linux `strace`
syscall injection. It rejects extensions with either private debug hook.
The host must permit tracing its own child processes and provide a strace
version supporting `-yy` and `inject=...:error=EIO:when=...`.

```bash
"$PYTHON" scripts/compatibility/sync_syscall.py --output "$NEW_OUTPUT"
# Run just one independent case while investigating a failure:
"$PYTHON" scripts/compatibility/sync_syscall.py --case kv-spool --output "$ONE_OUTPUT"
```

Eight cases cover KV/SQL and four sync boundaries: spool `fsync`, publication
directory `fsync`, reference append `fdatasync`, and forced `fdatasync`.
Each case first calibrates a successful commit on a separate three-record
fixture. Its trace identifies the syscall ordinal and decoded file path inside
the marked commit interval. The fault trace must contain exactly one injected
EIO at that same boundary; an error during database open is not accepted.

The caller must receive EIO without seeing the pending transaction. Failures
before reference publication preserve only the prior transaction on reopen;
failures after writing a complete reference poison the writer and replay the
whole transaction on reopen. The latter result relies on retained page-cache
contents and does not claim durability after failed sync. Two reopens check
values, SQL primary/unique keys and spool reclamation. Pre-reference failures
also verify a subsequent successful commit. Each child is limited to 30 seconds,
each case's combined fixtures to 1 MiB, and each trace to 1 MiB.

Reports retain per-case syscall/ordinal, native/source hashes, strace version
and separate baseline/failure traces. These are **syscall-boundary injected
errors**, not actual device errors or power-loss simulations. No product Rust
change, debug hook, ordinary CI job, or performance test is required. Apply the
same per-host budget and output cleanup policy as the other manual runners.

## Small inline sync failures and exit (manual Linux acceptance)

`inline_sync.py` qualifies the small inline path introduced by #737 through
the ordinary Python `Database` KV and SQL APIs. It requires Linux `strace`
with EIO and signal injection and rejects instrumented extensions and Python
`-O`. Run this separately from ordinary CI:

```bash
"$PYTHON" scripts/compatibility/inline_sync.py --case kv-eio --output "$ONE_OUTPUT"
"$PYTHON" scripts/compatibility/inline_sync.py --output "$NEW_OUTPUT"
```

The four independent cases are KV/SQL × EIO/SIGKILL at the inline WAL sync.
Each case first runs a normal synchronous commit followed by process exit,
then calibrates the exact syscall ordinal from its trace. The fault process
must hit that same WAL path inside the marked commit interval. There must be
exactly one `fdatasync` and no spool files, so silently exercising the older
spool path cannot pass. The kill case configures EIO and SIGKILL together;
SIGKILL prevents an observed syscall return (`?` in strace), so this case
requires the exact path/ordinal and termination signal rather than an EIO
return annotation. It does not represent a completed successful sync.

Fixtures contain one prior key/row and two pending keys/rows. KV uses one-byte
values; SQL uses two integer columns and a primary key. The ring is 4096 bytes,
inline budget 1024 bytes, and the remaining explicit overflow budgets are
bounded in `open_db`. EveryWrite synchronization remains enabled. Each child
has a 30-second timeout, and combined fixtures and each trace are capped at
1 MiB. No throughput, load, or performance measurement is included.

On EIO, the live caller retains prior data and cannot reuse the uncertain
writer. After EIO or SIGKILL, two reopens must recover the whole written inline
transaction, including SQL primary-key constraints and the absence of retry
data. This observation relies on retained page cache; it does not claim that
data survives power loss after a failed sync. Reports preserve each outcome,
native/script hashes, syscall ordinal and separate calibration/fault traces.
Use the same per-host storage budget, serialized build and cleanup rules.

## Bounded KV cost comparison (manual Linux measurement)

`kv_cost_probe.rs` and `kv_cost.py` compare the same public `LsmKV` transaction
against a pinned baseline, the candidate's legacy path, and its opt-in overflow
path. They are never invoked by ordinary CI. Measure the physical host's complete
Alopex storage before building, serialize builds with one shared Cargo target,
monitor storage during execution, and remove owned binaries/build outputs after
retaining compact evidence. Follow the repository's 40 GB stop / 50 GB maximum.

Build the **same probe source** in an isolated temporary Cargo package. Its
`Cargo.toml` must contain `[workspace]`, `[features] overflow = []`, and an
`alopex-core` path dependency pointing to a clean, dedicated source worktree.
Copy the probe to `src/main.rs`. Start its lockfile from the baseline's
`Cargo.lock`; retain each resolved lockfile and its hash. For each of baseline,
candidate legacy, and candidate overflow, check out the exact source SHA, set
`ALOPEX_SOURCE_SHA` to that SHA, and run:

```bash
# Set CARGO_TARGET_DIR to the host's shared target and TMPDIR to owned storage.
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_BUILD_JOBS=2
ALOPEX_SOURCE_SHA="$SOURCE_SHA" cargo build --release --manifest-path "$PROBE_MANIFEST"
# Add --features overflow only for the candidate overflow build.
# Preserve each executable under a distinct name before building the next one.
python3 scripts/compatibility/kv_cost.py \
  --baseline "$BASELINE_BIN" --candidate-legacy "$LEGACY_BIN" \
  --candidate-overflow "$OVERFLOW_BIN" \
  --baseline-sha "$BASELINE_SHA" --candidate-sha "$CANDIDATE_SHA" \
  --output "$NEW_OUTPUT"
```

Use the same release profile/toolchain/environment for all three builds. Record
source/tree, executable, probe, driver and lockfile hashes with the results. The
embedded SHA prevents accidentally running a binary built for another source.
The driver pins one CPU, rotates engine order, and runs one warmup plus five
measured rounds per cell. Each cell uses a fresh database, warm OS cache, one
prior key and two pending keys with 16- or 2048-byte values. EveryWrite sync,
4096-byte WAL ring and disabled automatic checkpointing are identical. Each
write/reopen subprocess has a 30-second timeout; fixtures are removed after
verification. The probe exits without destructor checkpointing so that the next
process actually replays the WAL. This is not a power-loss simulation.

Input preparation, transaction staging, commit and reopen times are distinct.
`peak_rss_kib` is the **whole write process** VmHWM (including database open and
the prior commit), not incremental transaction allocation. WAL/spool bytes are
logical file lengths before close; fixture allocated bytes include filesystem
blocks. No index is built. Capacity refusals are reported as capability gaps;
their failure latency must never be compared with successful commit latency.

The predeclared small-case median ratio above 1.20 is an investigation flag,
not an automatic acceptance rule or a noise exemption. Five tiny samples do not
qualify SQL/Python performance, throughput, cold-cache behavior, large datasets,
or release readiness. Keep #682's overall performance acceptance open until its
remaining coverage and any observed overhead have an explicit disposition.

For a direct optimization comparison, add `--previous-overflow "$BEFORE_BIN"`
and `--previous-sha "$BEFORE_SHA"` together. Build that fourth binary with the
same probe and overflow feature against the pre-change source. The runner uses
the same six rounds for all four modes (48 tiny cases total), verifies the
embedded source/feature identities, and reports the candidate/previous overflow
commit ratio as well as the baseline ratios. This provides a contemporaneous
control when changed production code requires a new measurement.
