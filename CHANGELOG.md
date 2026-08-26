# Changelog

All notable changes to this project will be documented in this file.

## [0.8.9] — Unreleased

### Added

- SQL adds portable UTC timestamp functions, math/string/regex functions,
  statistics/regression/value/bitwise/boolean aggregates, integer
  `GENERATE_SERIES`, and integer bitwise operators (issues #153-#157). The
  timestamp/interval series overload remains blocked on native `INTERVAL`
  support in issue #159.
- The Nim parser wire contract is `0.15.0`.
- KV transactions add bounded explicit byte-glob and byte-regex key search
  (issue #192), with deterministic bytewise ordering, binary cursors, resource
  limits, invalid-pattern errors, and cooperative cancellation. Embedded,
  async, HTTP (`POST /kv/search`), and CLI (`kv search --mode glob|regex`)
  surfaces share the additive contract; exact, prefix, and range APIs remain
  unchanged.

## [0.8.8] — 2026-08-26

### Added

- SQL `FROM` supports `LATERAL` derived tables, table functions, and relation
  alias column lists (issue #151). `CROSS JOIN LATERAL`, `[INNER] JOIN
  LATERAL … ON`, `LEFT [OUTER] JOIN LATERAL … ON`, and comma-separated
  `LATERAL` items evaluate the correlated right side once per left row, with a
  `LEFT JOIN LATERAL` padding unmatched left rows; `RIGHT`/`FULL JOIN LATERAL`
  is a stable planner error (`ALOPEX-T015`). A derived table without `LATERAL`
  still cannot see the enclosing FROM items. `UNNEST(vector)` is the first
  FROM-clause table function — one `FLOAT` column named `unnest`, zero rows for
  `NULL` or an empty vector — and its argument may reference the preceding FROM
  items without writing `LATERAL`; `GENERATE_SERIES` is reserved and reports
  issue #157, any other name is `ALOPEX-C007`. `AS t(c1, …, cn)` now applies to
  base tables, CTE references, derived tables, and table functions with exact
  arity (`ALOPEX-T012`) and no repeated names. Columnar filter fusion no longer
  applies to a correlated predicate: the `Filter` operator evaluates it over a
  columnar scan widened to the local columns it reads, which keeps the
  correlation boundary intact and also fixes subquery predicates over columnar
  storage. The v0.8 remote-read catalog rejects lateral joins before transport
  (`lateral_join_not_supported_remote`) and classifies table functions
  local-only (`table_function_not_supported_remote`). Covered on Rust, CLI,
  Embedded, and Python surfaces (docs/sql-lateral-table-functions.md). Parser
  FFI contract bumped 0.13.0 → 0.14.0: `FromItem.Table` gains `columns`,
  `FromItem.Derived` gains `lateral`, and a new `FromItem.Function` variant
  carries `{name, args, alias, columns, lateral}`, while the staged
  continuous-aggregate payload keeps its frozen FROM-item maps. `LATERAL`
  stays a contextual identifier, so a relation named `lateral` keeps working.

- The Python package can connect to a running `alopex-server` (issue #182).
  `alopex.connect(target)` selects the surface from the target alone:
  `http(s)://host:port` returns the new `alopex.RemoteDatabase`, while
  `:memory:`, `file:///path`, and bare paths open the embedded `Database`.
  `RemoteDatabase.execute_sql(sql, params)` returns what the embedded
  `Database` returns for the same statement — `list[dict]` in column order for
  SELECT, `int` for DML, `None` for DDL — by normalizing the server's
  `columns`/`rows` (`SqlValue` in serde's externally tagged form) in one place;
  `Float` values are re-narrowed through binary32 so they match the embedded
  `f64::from(f32)` exactly. `?` placeholders are expanded by the embedded binder
  itself (exported as the private `_alopex._bind_sql_params`), so parameter
  semantics cannot drift between the two surfaces. `begin()` opens a server
  session (`/session/begin` … `/commit` … `/rollback`) as `RemoteTransaction`.
  `alopex.DatabaseLike` and `alopex.TransactionLike` are runtime-checkable
  protocols describing the shared surface. The client is pure Python on
  `http.client`, adds no dependency to the wheel, opens no socket until the
  first call, never resends a request that was already sent, and requires
  `insecure=True` for plaintext `http://` to a non-loopback host.
  Operations with no server equivalent — stream APIs, HNSW, transactional KV
  and vector calls, `flush`, `memory_usage`, `routing_diagnostics`,
  `thread_mode`, and `begin(TxnMode.READ_ONLY)` — raise `NotImplementedError`
  with a reason instead of failing as a missing attribute. New stable codes:
  `ALOPEX-PY201` (connect failed), `ALOPEX-PY202` (client timeout),
  `ALOPEX-PY203` (protocol violation), `ALOPEX-PY204` (no server equivalent),
  `ALOPEX-PY205` (unusable target/option); the server's own codes
  (`UNAUTHORIZED`, `QUERY_TIMEOUT`, `SESSION_EXPIRED`, `SERVER_BACKPRESSURE`,
  and the rest) are forwarded verbatim onto `AlopexError.code` alongside
  `.correlation_id` and `.http_status`. Known asymmetry: the server's routing
  pre-pass flattens parse/catalog/type-check codes to `ALOPEX-E999` on the wire
  (documented as D20 in `docs/python-server-client.md`, same on the CLI's HTTP
  path), and a short list of statement-level value divergences — `PRAGMA`,
  `clear_cache()`, and a bare `;` — is pinned by a test and listed under
  "Known value divergences" (D24). Dropping a `RemoteTransaction` or calling
  `RemoteDatabase.close()` mid-transaction rolls the server session back
  instead of leaving it committable until the `session_ttl` sweep (D21);
  `RemoteTransaction.status` reports the embedded `stream_effect` value
  (`"committable"` while active) (D22); blank SQL returns `None` without a
  request like the embedded surface (D23); an explicit `:0` port and a `bool`
  `timeout` are rejected with `ALOPEX-PY205` instead of being read as port 80
  and a 1-second deadline. See `docs/python-server-client.md` and
  `docs/server-guide.md`.
- SQL `GROUP BY` supports `GROUPING SETS`, `ROLLUP`, `CUBE`, the empty
  grouping set `()`, and the `GROUPING`/`GROUPING_ID` functions (issue #149).
  Ordinary keys cross-product with the modifiers, duplicate sets emit
  duplicate rows (PostgreSQL semantics), and placeholder NULLs are
  distinguished from data NULLs only through `GROUPING` (BIGINT bitmask,
  leftmost argument = most significant bit, at most 63 arguments).
  `HAVING`/`ORDER BY`/window functions compose over a hidden `__grouping_id`
  aggregate column. Expansion is bounded at 4096 grouping sets and 63 union
  keys with stable planner errors; execution is single-pass and
  single-threaded (parallel/spill aggregation bypassed) with the existing
  1,000,000-group limit applied across all sets, and the v0.8 remote-read
  catalog classifies the forms local-only (`grouping_sets_local_only`).
  Covered on Rust, CLI, Embedded, and Python surfaces
  (docs/sql-grouping-sets.md). Parser FFI contract bumped 0.12.0 → 0.13.0:
  `Select.group_by` now carries tagged `GroupByItem` values
  (`Expr`/`Rollup`/`Cube`/`GroupingSets`) instead of bare expressions, while
  the staged continuous-aggregate payload keeps its frozen `[Expr]` shape and
  rejects the modifiers. `ROLLUP`/`CUBE`/`GROUPING`/`SETS` stay contextual
  identifiers.

- SQL aggregates support `FILTER (WHERE predicate)` on every aggregate
  (predicate rows that are not TRUE are excluded before `DISTINCT`),
  aggregate-local `ORDER BY` for `GROUP_CONCAT`/`STRING_AGG` (validated then
  discarded on order-insensitive aggregates), and the first ordered-set
  aggregate `PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)` with
  PostgreSQL selection semantics. `HAVING` recognizes filtered/ordered
  aggregates, aggregates differing only in FILTER/ORDER BY occupy distinct
  slots, ordered aggregates force single-threaded execution, and the v0.8
  remote-read catalog classifies the new forms local-only
  (`aggregate_filter_local_only`/`ordered_aggregate_local_only`). Combining
  the clauses with `OVER` is a stable planner error. Covered on Rust, CLI,
  Embedded, and Python surfaces (docs/sql-aggregate-filter-within-group.md).
  Breaking note: `filter`/`within` remain valid identifiers, but an implicit
  alias named `filter` directly followed by `(`, or `within` directly
  followed by `GROUP`, now parses as a clause — write `AS filter`/`AS within`
  instead. Parser FFI contract bumped 0.11.0 → 0.12.0: the `FunctionCall`
  wire map gains `order_by`/`within_group`/`filter` keys when any aggregate
  clause is present and keeps its historical 6-key shape otherwise.
- SQL supports PostgreSQL-style `SELECT DISTINCT ON (expr, ...)`: one row per
  key group with the PostgreSQL `ORDER BY` prefix contract (42P10-equivalent
  `ALOPEX-T014` on mismatch), NULL keys grouped as equal, select-list aliases
  in keys, and a documented determinism extension — remaining ties resolve by
  every input column in schema order, so results never depend on physical row
  order. `LIMIT`/`OFFSET`/`FETCH` — including `FETCH ... WITH TIES`, whose
  peer groups come from the user's `ORDER BY` — apply after deduplication.
  Covered on Rust, CLI, Embedded, and Python surfaces; v1 rejects
  combining it with `DISTINCT`, `GROUP BY`/aggregates/`HAVING`, window
  functions/`QUALIFY`, and trailing set operations (docs/sql-distinct-on.md).
- SQL supports standard pagination: `OFFSET n [ROW|ROWS]` without `LIMIT`,
  `FETCH {FIRST|NEXT} [count] {ROW|ROWS} {ONLY|WITH TIES}` with PostgreSQL
  peer semantics for `WITH TIES`, constant-expression and NULL/`ALL` counts
  folded at plan time, and negative/typed-count rejection across Rust, CLI,
  Embedded, and Python surfaces. A `?` bind placeholder now fails with a
  dedicated "bind parameters are not yet supported" parse error (prepared
  statements are tracked by issue #166) instead of a column-not-found.
  Breaking note: `FETCH`, `NEXT`, `TIES`, `ONLY` and `ROW` became lexer
  keywords. They remain valid identifiers wherever the grammar requires a
  name — expressions, `CREATE TABLE` column names, `INSERT`/CTE/alias column
  lists, `UPDATE SET` targets, table/index/window/CTE names, `USING` columns
  and `AS <name>` aliases — but an implicit alias named `fetch` now starts the
  pagination tail, so write `AS fetch` instead (`next`/`ties`/`only`/`row`
  still work as implicit aliases).
- SQL supports `TRY_CAST`, returning NULL only for conversion failures while
  preserving source-expression errors; CAST and TRY_CAST share bounded
  numeric, text, boolean, timestamp, BLOB, and vector conversion rules.
- SQL supports `IS [NOT] TRUE/FALSE/UNKNOWN`, null-safe
  `IS [NOT] DISTINCT FROM`, and row-value equality, ordering, `IN`, and
  `BETWEEN` with deterministic arity/type diagnostics.
- SQL `VALUES` works as a top-level query, derived table, CTE body, and set
  operand, with table/column aliases, `ORDER BY`/`LIMIT`, common-type
  inference, and exact-row coverage on Rust, CLI, Embedded, and Python.
- SQL windows support frame-aware `FIRST_VALUE`, `LAST_VALUE`, and
  `NTH_VALUE`, plus peer-aware `PERCENT_RANK` and `CUME_DIST` and deterministic
  `NTILE` bucket allocation across Rust, CLI, Embedded, and Python surfaces.

### Fixed

- Opening one data directory from two places is now rejected instead of
  silently corrupting it (issue #181). Two processes sharing a `data_dir` each
  kept their own WAL ring offset and their own SSTable id counter, so they
  overwrote each other's WAL bytes and each other's `sst/<id>.sst`, and each
  one's drop-time prune could delete the other's live sidecar — all with no
  error or warning. `LsmKV::open_with_config` now takes an OS-level exclusive
  lock (`flock` on Unix, `LockFileEx` on Windows) before it touches anything,
  including before the container rehydrate path, and fails with
  `Error::AlreadyOpen` — rendered with the stable string `already open by
  another process` — when someone already holds it. The lock is released only
  when the handle is dropped (not by `close()`, which leaves the store
  writable), and because it is an OS lock the kernel releases it on any
  abnormal exit, so a `SIGKILL`ed process leaves nothing stale behind. The lock
  file is `X.alopex.lock` beside the container for the sidecar shape and
  `<data_dir>/.alopex.lock` for a plain directory; it is excluded from backup,
  restore, and S3 sync. To share one database across processes, run
  `alopex-server` and connect over HTTP/gRPC. In-memory databases are
  unaffected (docs/single-process-lock.md).
  **Breaking**: pointing `alopex --data-dir` at a running server's `data_dir`,
  or opening one embedded database twice, now returns an error.
- A disk database opened through an `X.alopex` path now actually converges into
  that single file (issue #178). Previously all data stayed in the
  `X.alopex.d/` sidecar directory and `X.alopex` was either absent or a
  zero-byte existence marker, so copying `X.alopex` on its own restored
  nothing — the documented "安定後は `.alopex` 単体で完全状態" contract was
  unmet. `flush()`, the new `converge()`/`close()`, and dropping the handle now
  write every live SSTable plus an `LsmManifest` into the existing unified
  container format (`ALPX` header, per-section CRC32, `XPLA` footer carrying
  the converged LSN) via a temp file and atomic rename, and dropping the handle
  additionally prunes the sidecar. Opening a path whose sidecar WAL is absent
  rehydrates the working directory from the container. Copying only `X.alopex`
  to another directory and reopening it now restores every committed row,
  including tombstones and the MVCC clock (docs/single-file-convergence.md).
- `Database::persist_to_disk()` no longer leaves a zero-byte `.alopex` marker
  next to the data directory; it writes a real, self-contained container at the
  given path. Its staging directory moved from `X.alopex.tmp` to
  `X.alopex.d.tmp` because the container writer already claims the former for
  its own atomic-rename staging file.
- `LsmKV` no longer silently discards unpersisted immutable MemTables. Once
  more than `memtable.max_immutable_count` tables piled up, the oldest was
  dropped with `pop_front()` and its rows were lost; each table is now written
  to an SSTable *before* it leaves the queue, and a failed write propagates
  instead of losing data.
- A point lookup no longer costs the total size of every live SSTable.
  `SSTableReader::open` re-reads and CRC32s the whole file, and `get`/conflict
  detection opened one per SSTable *per key*, so a database with real SSTables
  spent all its time re-validating them. Lookups now skip SSTables whose key
  range cannot contain the key and share cached, already-validated readers
  (bounded at 256 open files). This was latent until `flush()` began producing
  SSTables in this release.
- Opening a data directory whose WAL is missing but whose `checkpoint.meta`
  survives no longer rewinds the timestamp oracle to 1. Committing after such
  an open used to issue commit timestamps below those already stored in
  SSTables, surfacing as spurious `TxnConflict`s and stale reads.
- Rust consumers of the published Embedded/SQL crates no longer require the
  Nim parser shared library at runtime: target-specific static parser archives
  are bundled into `alopex-sql`, while shared libraries remain Python-wheel
  assets. The release verifier now runs a published dependency smoke without
  `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH`.
- Internal Alopex crate dependencies use exact `=0.8.8` patch requirements so
  pinning `alopex-embedded = "=0.8.8"` cannot resolve sibling Alopex crates to a
  newer patch line. The public verifier checks the generated Cargo.lock.
- An explicit default RANGE frame for a value window uses the same linear
  peer-group path as its implicit form, avoiding a size-dependent resource
  failure for semantically identical queries.
- A correlated reference from a `LATERAL` item or a correlated subquery works
  in every operator, not only `WHERE` and the projection: aggregate `FILTER`
  predicates, aggregate-local `ORDER BY`, group keys, plain `ORDER BY`,
  `DISTINCT ON`, `FETCH ... WITH TIES` peer keys and window
  `PARTITION BY`/`ORDER BY` used to escape as an internal `ALOPEX-E999`
  "invalid column reference".
- Columnar projection pushdown collects the columns an aggregate `FILTER`
  predicate, an aggregate-local `ORDER BY`, or an `OVER (...)` partition/order
  key reads. They were previously materialized as `NULL`, so
  `SUM(v) FILTER (WHERE flag > 0)` silently returned `NULL` on a columnar
  table.
- `scripts/build-nim-parser.sh --backend host` — the path the release workflow
  uses for all four parser archives — compiles the static archive with
  `-fPIC`, matching the `staticlib` nimble task. The published Linux archive
  otherwise carried `R_X86_64_TPOFF32` relocations and could not be linked
  into the Python extension module. The script now fails closed when a Linux
  archive still carries them.
- Windows Embedded/Python convergence no longer reports `Access is denied`
  after atomically replacing the `.alopex` container: the file body is still
  synced before rename, while the unsupported standard-library directory sync
  is skipped on Windows.
- Python `file://` targets use the platform URL-to-path converter, so valid
  Windows file URIs open the intended embedded database.

### Changed

- `Database::flush()` now costs more than it used to: it freezes the MemTable,
  writes it to an SSTable, and converges the database into its single
  `.alopex` file. Previously it only froze the active MemTable and wrote
  nothing to disk, contradicting its own documentation. This propagates to
  Python's `db.flush()`. `Database::converge()`, `Database::close()`, and
  `Database::container_path()` are new; `db.close()` on the Python surface now
  reports convergence failures instead of leaving them to the best-effort drop
  path.
- Convergence is scoped by `LsmKVConfig::converge`, whose default
  `ConvergePolicy::SidecarOnly` only applies to `X.alopex.d` sidecar
  directories. Plain-directory disk databases — including every `alopex-server`
  deployment — keep the classic multi-file layout byte for byte, and no
  migration or data conversion is required for existing 0.8.x data
  (docs/single-file-convergence.md).
- The Nim parser wire contract is `0.14.0`. It widens `FromItem` for LATERAL,
  FROM-clause table functions, and relation alias column lists: `Table` gains
  `columns` (always written, empty when absent), `Derived` gains `lateral`,
  and a new `Function` variant carries
  `{name, args, alias, columns, lateral}`. Contract `0.13.0` producers are
  rejected before decode. The byte-frozen staged continuous-aggregate payload
  is unchanged and rejects LATERAL, FROM table functions, and a table alias
  column list before staging. This remains internal metadata of the unified
  Alopex v0.8.8 release, not a separate parser release lane.
- The Nim parser wire contract `0.13.0` changed `Select.group_by` from
  `[Expr]?` to tagged `[GroupByItem]?` values
  (`Expr`/`Rollup`/`Cube`/`GroupingSets`); contract `0.12.0` producers are
  rejected before decode. The byte-frozen staged continuous-aggregate payload
  keeps its `[Expr]` shape and rejects `ROLLUP`/`CUBE`/`GROUPING SETS` before
  staging. This remains internal metadata of the unified Alopex v0.8.8
  release, not a separate parser release lane.
- The Nim parser wire contract `0.12.0` added `order_by`, `within_group`, and
  `filter` keys to the `FunctionCall` map when any aggregate clause is present
  (the historical 6-key shape is kept otherwise); contract `0.11.0` producers
  are rejected before decode. The byte-frozen staged continuous-aggregate
  payload is unchanged and rejects aggregate `FILTER`, `WITHIN GROUP`, and
  aggregate-local `ORDER BY` before staging. This remains internal metadata of
  the unified Alopex v0.8.8 release, not a separate parser release lane.
- The Nim parser wire contract `0.11.0` added the always-written
  `distinct_on` expression list to `Select` (empty when the clause is absent);
  contract `0.10.0` producers are rejected before decode. The byte-frozen
  staged continuous-aggregate payload is unchanged and rejects `DISTINCT ON`
  before staging. This remains internal metadata of the unified Alopex v0.8.8
  release, not a separate parser release lane.
- The Nim parser wire contract `0.10.0` added the always-written
  `limit_with_ties` field to `Select`/`Values` and detached `OFFSET` from
  `LIMIT` on the wire; contract `0.9.0` producers are rejected before decode.
  The byte-frozen staged continuous-aggregate payload is unchanged and rejects
  `WITH TIES` before staging. This remains internal metadata of the unified
  Alopex v0.8.8 release, not a separate parser release lane.
- The Nim parser wire contract `0.9.0` added a dedicated `TryCast`
  expression variant; contract `0.8.0` producers are rejected before decode.
  This remains internal metadata of the unified Alopex v0.8.8 release, not a
  separate parser release lane.

### Release verification

- Python and Rust v0.8 demos verify `TRY_CAST`, `VALUES` query composition, and all six
  window functions with exact frame, peer, NULL, bucket, rank, and
  cumulative-distribution results.
- Python and Rust v0.8 demos verify `DISTINCT ON` deterministic tie
  resolution and the `ALOPEX-T014` ORDER BY prefix rejection.

## [0.8.7] — 2026-08-18

### Added

- Common table expressions accept ordered column-name lists and preserve their
  names across parser, planner, executor, CLI, Embedded, and Python surfaces.
- Direct self-recursive single CTEs execute `UNION` or `UNION ALL` to a bounded
  fixed point across Rust, embedded, CLI, and Python SQL surfaces. Anchor
  output names are used when the CTE column-name list is omitted.
- `LAG` and `LEAD` support offsets, defaults, partition boundaries, NULLs, and
  stable peer ordering.
- Aggregate window functions support explicit `ROWS` and `RANGE` frames with
  bounded work and memory accounting.
- Grouped aggregation and `HAVING` compose with window evaluation, projection,
  `DISTINCT`, and outer `ORDER BY` in SQL evaluation order.

### Changed

- Execution resource exhaustion now maps to stable public code `ALOPEX-E003`
  instead of the generic `ALOPEX-E999`. This applies to existing aggregate and
  memory limits as well as recursive CTE iteration, row, and memory limits.
- Unsupported mutual recursion, multiple self-references, recursive-term
  subqueries, nested `WITH`, and nested set operations fail closed.
- The Nim parser wire contract is `0.5.0`. It remains compatibility metadata
  inside the unified Alopex release; it is not a separate parser release lane.

### Fixed

- Implicit ordered aggregate windows include the complete peer group.
- Grouped-window expression rewriting preserves explicit frame metadata.
- The locked HTTP/2 stack uses `h2` 0.4.16, which fixes
  `RUSTSEC-2026-0258` (unbounded empty DATA frames).

### Release verification

- Python and Rust demos verify recursive CTEs, positional windows, explicit
  frames, and grouped-window composition with exact results.
- Publication remains blocked until all four contract-0.5 parser targets are
  rebuilt with the pinned Nim/Nimble toolchain, bound to the immutable v0.8.7
  tag, published, and accepted by the post-release verifier.

## [0.8.6] — 2026-08-17

### Added

- SQL projection aliases resolve in `ORDER BY` and `HAVING` with explicit
  scope errors in `WHERE` and `GROUP BY`.
- `REAL` is accepted as a SQL type and is preserved through Rust, Arrow, and
  Python value surfaces.
- Searched and simple `CASE` expressions support numeric promotion, implicit
  `NULL`, and typed Boolean results.
- `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` implement duplicate,
  precedence, associativity, NULL, and type/column-count contracts.
- Non-recursive common table expressions support multiple definitions, joins,
  aggregation, and statement-local table shadowing.
- Aggregate and ranking window functions support partition-wide and implicit
  cumulative frames; unsupported positional functions and explicit
  `ROWS`/`RANGE` frames fail closed.

### Release verification

- Python and Rust public demos compare complete row values for the v0.8.6 SQL
  contracts, including duplicate rows, NULLs, asymmetric set operands, CTE join
  multiplicity, and window ordering.
- CI builds and links the just-built Nim parser for the v0.8.6 gate and checks
  that test binaries do not resolve the historical vendored library.
- Parser release assets retain FFI contract `0.4.0` while being rebuilt from
  the v0.8.6 source tag for every supported native target.

## [0.8.5] — 2026-08-14

### Fixed

- Python wheels expose and execute the vector, HNSW, synchronous SQL-stream,
  and local-scan stream APIs without a release-only Cargo feature switch.
- `HnswSearchResult.distance` now follows one lower-is-closer public contract:
  Euclidean L2 distance, cosine distance, or negated inner product.
- Public-package parity accepts both the current `SqlResultSet` envelope and
  the legacy row stream, and removes inherited parser loader paths in released
  mode.
- Demo output distinguishes a missing API, a skipped scenario, and a typed
  execution error instead of attributing every exception to the SQL input.

### Security

- Updated the released Chirps dependency family and `object_store`/`quick-xml`
  chain to remove the reachable RustSec vulnerabilities found by the v0.8.5
  audit, and made the GitHub Actions audit fail closed.
- The lockfile-only `rkyv 0.7.46` exception is guarded by an all-features,
  all-target dependency-tree check. Non-vulnerability soundness and
  unmaintained warnings remain tracked in issue #95 for coordinated major
  dependency migrations.

### Release verification

- v0.8 SQL, Python streaming, Vector/HNSW, five-surface, and Rust embedded
  demos are mandatory after publication. The release remains incomplete until
  the successful report is present byte-for-byte on `alopex-db/docs@main`.
- Verification writes reusable JSON evidence and a reviewable Markdown report.
  Failed scheduled runs retain both as Actions artifacts and notify through an
  issue, but do not automatically publish a public guarantee.
- A weekly workflow exercises the latest PyPI/crates.io release, and release
  contract tests prevent target-version, demo-list, or dependency-wiring drift.

## [0.8.4] — release candidate

This entry records the reviewed release intent and compatibility contract. It
does not claim that public crates, wheels, or GitHub assets have been
published; publication evidence is recorded only after the core and Python
release gates complete.

### Compatibility

- SQL parser contract `0.4.0` remains Nim-owned and is staged as reviewed,
  deterministic native assets for Linux x86_64, macOS x86_64, macOS arm64,
  and Windows x86_64.
- Python wheels consume the Alopex core parser envelope and use package-local
  native libraries with relative loader paths; arbitrary external DLL
  directories are not part of the release interface.
- WebAssembly remains outside this release and is deferred to v1.0+.

### Release verification

- Core release candidates are checked from the peeled `v0.8.4` tag, including
  archive extraction and native loader smoke tests before upload.
- Python packaging consumes the public core assets only after the core
  manifest and envelope pass identity verification. Any later publication
  remains repair-forward and must not rewrite the immutable v0.8.2/v0.8.3
  history.

## [0.8.3]

Follow-up to the v0.8.2 name-resolution work. The remaining findings from the
reference-implementation review are addressed here.

### Fixed
- An equi-join keys rows on the numeric value rather than on its debug
  rendering, so `Integer(1)` and `Double(1.0)` meet in the same bucket. Joining
  columns of different numeric types through `USING` or `NATURAL` returned an
  empty result with no error, while the identical predicate in a `WHERE` clause
  matched; a `FULL` join additionally reported the shared key twice.
- A duplicate range-variable name, as in `FROM t AS x JOIN t AS x`, is rejected
  as ambiguous. It previously resolved to the first table, silently reading the
  wrong column.
- A column merged by one `USING` or `NATURAL` join stays a single key when it
  feeds the next one. Chaining three or more tables reported the merged name as
  an ambiguous pair of input keys.
- Unquoted identifiers fold to lower case and quoted identifiers keep theirs,
  which is the PostgreSQL-compatible reading v0.8.2 adopted for double-quoted
  names but did not fully implement.
- Diagnostics point at the position the caller wrote. Quoted identifiers are
  normalised before parsing, and removing the quote characters shifted every
  later span two columns per identifier.
- A mismatch between the `NATURAL` markers the parser supplies over FFI and the
  joins found in the AST is now a parse error. The surplus joins previously kept
  their default, turning a `NATURAL JOIN` into a cross product with no
  diagnostic.
- A derived table no longer resolves names from the query that encloses it.
  Standard SQL evaluates `FROM (SELECT ...) AS d` independently of the rest of
  the statement, and only `LATERAL` makes the surrounding scope visible; Alopex
  does not accept `LATERAL`. The outer scope was being passed into the derived
  subquery, so a name from an enclosing `SELECT` resolved into a correlated
  reference that was never written. Scalar subqueries still correlate.
- The Python test suite no longer leaves a stand-in `polars` module in
  `sys.modules`, which made a later test in a full-suite run fail against the
  wrong `scan_parquet` signature.

### Changed
- Python benchmark tests record binding overhead through `record_property`
  instead of asserting on it. A timing threshold makes a test's outcome depend
  on the machine it runs on; measurement on a controlled environment is tracked
  separately in #76. These tests still assert correctness: that a scan is
  usable, that a read returns every row, and that a write persists every row.

### Performance
Name resolution was superlinear in both schema width and subquery nesting. The
earlier reading that called it linear timed parsing and planning together, and
the parser's own linear cost hid the growth curve.

- Scoped tables share their `TableMetadata` through `Arc` rather than copying it
  into every nested scope. Twelve levels of correlated nesting: 76.1 µs → 36.5 µs.
- Tables wider than 32 columns carry a column-name index, so resolving a name no
  longer scans the column list. Narrow tables keep the scan, because indexing
  every table unconditionally cost more than it saved. An 800-column projection:
  2.41 ms → 633 µs, and its growth from 400 columns falls from 3.5× to 2.1×.
- `NATURAL JOIN` hashes the right schema when finding common columns instead of
  pairing every column against every other. A 200-column join: 573 µs → 482 µs.

`crates/alopex-sql/benches/name_resolution_bench.rs` holds these measurements.

## [0.8.2]

SQL correctness release. Every fix below addresses a documented feature that did
not work, or a documented plan that did not match the implementation.

### Fixed
- `TIMESTAMP` columns accept the literal form the dialect specification defines
  (`'2025-01-15 10:30:00'`, with optional fractional seconds) and Python
  `datetime` parameters. Previously no literal or bound parameter of any type
  could be written to a `TIMESTAMP` column. Time-zone-aware `datetime` values are
  rejected rather than silently reinterpreted, because the dialect is UTC-only.
- Arithmetic between `DOUBLE` and `INTEGER` promotes to `DOUBLE`. Comparison
  already promoted, so `v > 2` worked while `v * 2` failed.
- `SUM(INTEGER)` returns `BIGINT` instead of a double. A 32-bit accumulator
  overflows on ordinary data, so the sum is widened the way PostgreSQL sums
  int4 into int8. `TOTAL` remains floating point.
- `INTEGER` mixed with `FLOAT` in arithmetic promotes to `DOUBLE`. Promoting to
  `FLOAT` silently lost magnitude because a 24-bit mantissa cannot hold the
  whole `INTEGER` range.
- A decimal literal can be assigned to a `FLOAT` column. Every decimal literal
  is typed `DOUBLE`, and the narrowing was rejected, so `FLOAT` columns could
  not be populated at all.
- `USING` and `NATURAL` common columns are merged. An unqualified reference
  bound to the left input, so under `RIGHT` and `FULL` joins it returned the
  left side's NULL instead of the key present on the right, with no error.
- `CAST(expr AS type)` executes for every target type in the grammar. The syntax
  parsed but only the `TIMESTAMP` coercion was wired to the evaluator, so all
  other casts failed at runtime.
- Subquery column resolution follows lexical scope. Scalar, `IN`, `NOT IN`,
  `ANY`, and `ALL` subqueries all failed with an ambiguous-column error whenever
  the inner and outer relations shared a column name.
- `NATURAL JOIN` coalesces its common columns instead of reporting them as
  ambiguous.
- Double-quoted names resolve as identifiers per the SQL standard. They were
  parsed as string literals, so `SELECT "col"` silently returned the column name
  instead of the column value.
- `IN` lists and `BETWEEN` evaluate on the row-scan path. Both parsed and
  type-checked but had no evaluator arm outside the subquery and columnar paths.
- `INSERT INTO ... SELECT` and table-qualified wildcards (`t.*`) are accepted.
- `NOW()` is implemented and `DEFAULT NOW()` is evaluated on insert. The value is
  fixed for the duration of a statement, so every row and subquery in one
  statement observes the same timestamp.

### Changed
- The SQL dialect specification and milestone document now describe the shipped
  implementation: `JOIN`, subqueries, and `GROUP BY`/`HAVING` were still listed
  as unsupported despite shipping in v0.7.3 and v0.7.4, the `SELECT` grammar and
  BNF omitted them entirely, and parameter binding was undocumented. Window
  functions and `UNION` are consistently recorded as v0.9+.

### Breaking Changes
- `SUM` over an `INTEGER` column returns `BIGINT` rather than a double.
  Consumers that relied on the previous floating-point result should use
  `TOTAL`.

## [0.8.1]

Parser and release-gate reliability release.

### Added
- PromQL and SQL-TS parser contracts through the Nim parser ABI 0.2.0, including
  MessagePack FFI coverage and host-side Nim tests.

### Fixed
- Python DataFrame streaming tests now use portable temporary paths on Windows.
- Python async type stubs import extension types directly, including compatibility
  with mypy 2.3.
- Cross-surface server tests serialize child-server startup, verify gRPC readiness,
  and bound network waits so Windows release checks fail diagnostically instead of
  hanging.

### Changed
- The v0.8 CI and tag release gates now run the full supported surface suite on
  both Ubuntu and Windows, including the native Nim parser and Python bindings.

### Breaking Changes
- None intended.

## [0.8.0]

Cluster-aware and streaming release.

### Added
- Cluster metadata, lifecycle, routing diagnostics, and authenticated distributed-read contracts.
- HTTP/gRPC/CLI multi-statement and streaming SQL surfaces with explicit unsupported-operation outcomes.
- Bounded and incremental DataFrame streaming for CSV/Parquet with expression, projection, concat, and resource-lifecycle contracts.
- Synchronous and asynchronous Python local APIs, SQL streams, transactions, local scans, and DataFrame bindings.
- Offline, no-network/no-write candidate readiness verification and v0.7.4-to-v0.8 upgrade guidance.

### Compatibility
- Single-node behavior remains the default when cluster prerequisites are unavailable.
- Remote execution, distributed transactions, and client/connection-pool APIs remain outside the v0.8 supported scope.

## [0.7.6]

Additive cluster-surface release.

### Added
- gRPC cluster administration RPCs: `ClusterStatus`, `ClusterJoin`, and
  `ClusterLeave` (#38). Their `cluster_json` payload uses the same canonical
  cluster status schema as the HTTP admin `cluster` field, preserving exact
  integer values and cross-surface parity.

### Breaking Changes
- None intended.

## [0.7.5]

Bugfix and packaging release. No breaking changes intended.

### Fixed
- HTTP multi-statement execution now returns every statement result, with
  matching gRPC and CLI behavior (#31).
- Embedded cluster status accessors now reflect the engine's live catalog and
  routing state (#35).
- Unsupported compaction is reported consistently as `501 Not Implemented`
  across the server and CLI surfaces (#39).
- Windows Python wheels now bundle the Nim SQL parser DLL and load it from the
  package without requiring an external DLL-directory override (#33).
- Backup/restore completion tests now observe Coordinator state directly and
  verify the final HTTP status without relying on a fixed polling interval
  (#34).

### Breaking Changes
- None intended.

## [0.7.4]

### Added
- Registry-backed scalar functions covering the v0.5.3 catalog, including
  numeric, string, conditional, regex, pattern, and type-information functions.
- SHA256, MD5, SIMHASH, HAMMING_DISTANCE, UUID v4/v7, HEX, UNHEX, ENCODE,
  and DECODE SQL functions. Hash and encoding inputs are capped at 16 MiB.
- `memory_stats()`, `io_stats()`, `clear_cache()`, and the `PRAGMA
  cache_size`, `PRAGMA memory_limit`, and `PRAGMA io_stats` controls.
- Reproducible Nim parser build and test scripts using host Nim or a pinned
  Docker image.

### Changed
- Added SQL standard forms for `SUBSTRING ... FROM ... FOR`, `POSITION ...
  IN`, and `TRIM ... FROM`, plus LIKE-family pattern operators.
- System statistics remain backend-specific: unsupported statistics return
  `NULL` instead of being synthesized.

### Security
- MD5 is provided for compatibility and fingerprinting only; it must not be
  used for password storage, signatures, or other collision-sensitive security
  purposes.

### Breaking Changes
- None intended. Existing SQL value variants and Python result mappings are
  preserved.

## [0.7.3]

### Added
- `alopex-sql` aggregate accumulators now expose `state()` and `merge()`
  partial-state contracts. `AVG` uses `(sum, count)` state, ordered string
  aggregates preserve partition order, and invalid state arity/type returns
  an error instead of panicking.
- Single-process partial-to-final aggregate execution for non-DISTINCT
  aggregates, including GROUP BY and global aggregation. DISTINCT aggregates
  intentionally remain Single mode because two-stage DISTINCT is not
  mathematically correct without a future repartition/dedup stage.
- `SUM/AVG/MIN/MAX/GROUP_CONCAT/STRING_AGG(DISTINCT ...)` support. DISTINCT
  equality now follows the same `encode_group_key` semantics used by GROUP BY.

### Changed
- `COUNT(DISTINCT)` now uses the shared GROUP BY key encoder for duplicate
  detection, keeping NULL exclusion and type/bit-level equality behavior
  consistent across aggregate and grouping paths.
- `scripts/release/verify-release/run.sh` now defaults to v0.7.3 and includes
  a post-release artifact-only aggregate behavior guarantee step for
  state/merge, DISTINCT aggregates, and single-process parallel aggregation.

### Breaking Changes
- External implementations of the `Accumulator` trait must implement
  `state()` and `merge()` in addition to `update()`, `finalize()`, and
  `clone_box()`.

## [0.7.2]

Emergency bugfix release for v0.7.1. No new features. Both bugs were
discovered by a new release-verification container that installs the
published crates.io/PyPI artifacts (rather than building from source) and
runs the mode-parity and v0.7 feature demos against them.

### Fixed
- **`alopex-cli`/`alopex-server`/`alopex-py` failed to start when installed
  from crates.io/PyPI without manually setting `LD_LIBRARY_PATH`.** The Nim
  SQL parser's shared library rpath, emitted from `alopex-sql`'s build
  script via `cargo:rustc-link-arg`, does not propagate to a dependent
  crate's final binary — this is a documented cargo limitation, not a bug
  in the flag itself. `alopex-sql` now publishes its library directory via
  `links` + `cargo::metadata`, and `alopex-cli`/`alopex-server`/`alopex-py`
  each read it via `DEP_ALOPEX_SQL_PARSER_LIBDIR` in their own build
  scripts and set their own rpath (the standard `*-sys` crate pattern).
- **`cargo install alopex-sql` (and anything depending on it) failed on
  machines without a Nim toolchain.** The published crate contained the Nim
  source but no way to build it. Prebuilt shared libraries for the four
  released platforms (x86_64/aarch64 macOS, x86_64 Linux, x86_64 Windows)
  are now vendored into the published crate under
  `nim-sql-parser/vendor/<target-triple>/`, generated by CI at release time;
  end users no longer need a Nim toolchain to build or install `alopex-sql`.
- Unified the `tonic`/`axum`/`tower`/`tower-http`/`prost` dependency chain
  (0.10/0.7/0.4/0.5/0.12 → 0.14/0.8/0.5/0.6/0.14) to eliminate a duplicate
  hyper/rustls/tokio network-stack subtree (`tonic` 0.10 required `axum`
  0.6, while `alopex-server` directly required `axum` 0.7) — the same class
  of disk-exhaustion risk that caused the v0.7.1 release CI failure.

## [0.7.1]

Bugfix release. All fixes were discovered by the new mode-parity verification
suite, which is included in this release.

### Fixed
- `alopex-cli`'s `reqwest` dependency is now unified on 0.13 (it was pinned
  at 0.12, incompatible with `object_store` 0.14's `reqwest ^0.13`
  requirement). The two independent, semver-incompatible dependency trees
  this produced duplicated the entire hyper/rustls/tokio stack and could
  exhaust disk space during release builds.
- Subqueries now execute on the CLI/streaming query path. Previously,
  WHERE-clause subqueries failed with `ALOPEX-E999` (unsupported expression)
  and scalar subqueries silently returned empty results on the streaming
  path, while the embedded API and HTTP returned correct results (#23, #24).
- `NOT IN (subquery)` now follows SQL three-valued logic: when the subquery
  result contains NULL (or the probe value is NULL), the predicate evaluates
  to UNKNOWN instead of raising a type error, on both streaming and
  non-streaming paths.
- gRPC `ExecuteSql` now delegates to the same non-streaming SQL execution
  path as HTTP, so both server surfaces return identical results and errors
  for the same SQL, including DML/DDL execution and routing gates (#25).
  Response buffering was trimmed with incremental response-size checks.
- CLI `sql` now emits one result block per statement for multi-statement
  input instead of silently dropping all but the last result (#26). All
  statements run in one auto-commit transaction; a mid-batch failure rolls
  back the whole batch and exits non-zero.
- CLI streaming JSON output no longer leaves invalid partial JSON on stdout
  when a local or remote stream fails mid-result.
- Multi-row `INSERT ... VALUES (...), (...)` without an explicit column list
  no longer fails with `ALOPEX-P001` (Nim parser AST ambiguity between the
  column list and the first value row) and no longer desyncs the Nim FFI
  boundary for subsequent statements on the same thread (#40).
- Windows: the Nim SQL parser DLL no longer fails to load for `alopex-py`
  (`ImportError: DLL load failed`) — the MinGW runtime is now statically
  linked into the DLL, and the package-local native directory resolves the dependency
  explicitly instead of relying on PATH search (Python 3.8+ does not use
  PATH for extension-module dependency resolution).

### Security
- Updated dependencies to resolve 15 of 17 RustSec advisories found in the
  v0.7.0 dependency tree: `pyo3` 0.24.2 → 0.29.0 (RUSTSEC-2026-0176,
  RUSTSEC-2026-0177), `rustls` 0.21 → 0.23 chain including `axum`/`axum-server`
  and `alopex-cli`'s `reqwest` client (RUSTSEC-2026-0104, RUSTSEC-2026-0099,
  RUSTSEC-2026-0098; the native-tls/openssl chain was also removed from
  `alopex-cli`, which had been silently linked despite the `rustls-tls`
  feature being requested), plus semver-compatible patch bumps for `bytes`,
  `crossbeam-epoch`, `lz4_flex`, `quinn-proto`, `rustls-webpki`, and `time`.
  `object_store` was updated 0.11 → 0.14, but the two remaining advisories
  (RUSTSEC-2026-0194, RUSTSEC-2026-0195, both in the transitive `quick-xml`
  dependency, not used directly by alopex code) are blocked on an upstream
  `object_store` release not yet on crates.io; tracked in #42 and
  suppressed in CI via `rustsec/audit-check`'s `ignore` input until then.

### Added
- `Database.execute_sql` / `Transaction.execute_sql` in `alopex-py`:
  SELECT returns `list[dict]` (column order preserved), DML returns the
  affected-row count, DDL returns `None`. Positional `?` parameters are
  bound client-side with quote/comment-aware substitution (#27).
- `Database::execute_sql_multi` in `alopex-embedded`: executes all statements
  in one transaction and returns one `ExecutionResult` per statement.
- Mode-parity verification & demo suite (`scripts/parity/`): a shared SQL
  corpus with hand-calculated expected results, executed across the
  embedded API (in-memory / file), CLI, HTTP, gRPC, and now cluster-aware
  server surfaces to verify the mode-parity invariants (same data
  directory, same SQL, same results), plus a pinned verification container
  and a five-act demo (SF-CLUSTER, the fifth act, is no longer skipped).
- v0.7 feature demo suite (`scripts/demo/v07/`): cluster status
  cross-surface verification (HTTP/CLI, membership lifecycle, degraded
  fallback), routing transparency (live `local_only` decisions plus the
  simulated scatter-gather harness contract), and DataFrame P3
  (`str`/`dt`/`list` namespaces, `explode`/`implode`) with hand-calculated
  expected results and determinism checks.
- `_alopex.pyi` now declares `Database.cluster_status()` and
  `Database.routing_diagnostics()`, which were implemented but had no type
  stubs.

### Changed
- CLI `sql --output json` always emits an array of per-statement result sets;
  a single statement yields a 1-element array. DDL/DML statements contribute a
  `status`/`message` result set (omitted with `--quiet`). Remote (`--server`)
  output uses the same array-of-result-sets shape.

### Breaking Changes
- CLI `sql --output json` output shape changed: previously a single result set
  was emitted as an array of row objects; it is now always an array of
  per-statement result sets (one extra level of nesting, even for a single
  statement). Consumers parsing the old shape must unwrap the outer array.

## [0.7.0]

### Added
- Cluster-aware foundation through the `alopex-cluster` boundary, including
  stable node identity, membership lifecycle, placement metadata, routing
  diagnostics, and status schema contracts.
- Server admin cluster status surfaces for status, health, metrics, join, and
  leave operations while preserving `single_node` as the default mode.
- CLI and Python cluster status projections with cross-surface fixture checks
  against the Server status schema.
- Query Router planning foundation for `local_only`,
  `future_distributed_execution_required`, and simulated scatter-gather routing
  decisions with retry/backoff, idempotency, cancellation, and diagnostics
  coverage in the release gate.
- DataFrame P3 namespace primitives for string, datetime, and list operations
  in `alopex-dataframe`.
- Python `DataFrame` wrapper APIs for P3 namespace operations, `explode`,
  `implode`, default constructor/from_columns compatibility, and schema aliases
  for default DataFrame contracts.
- v0.7 release gate (`scripts/release/v07_gate.sh`) covering the v0.6
  baseline, cluster-aware tests, Server/CLI/Python status consistency,
  DataFrame P3, compatibility regressions, workflow contracts, and release
  binary smoke build.

### Changed
- Default Embedded and Server behavior remains single-node compatible with
  v0.6. Cluster-aware metadata and routing behavior are opt-in through
  cluster-aware configuration.
- Live Server SQL routing now records local routing diagnostics. Queries that
  would require production distributed execution return a stable future-work
  diagnostic instead of attempting partial remote execution.
- Release readiness is separated from release completion: the release branch is
  merged to `main` before tagging, the `v0.7.0` tag points to that merged `main`
  commit, and the release is complete only after artifacts and branch cleanup
  are verified.

### Compatibility
- Existing v0.6 Embedded, Server, SQL, DataFrame, and Python default behavior is
  covered by compatibility regressions.
- v0.7 metadata initialization and upgrade paths are designed to be idempotent
  and safe to retry.
- The v0.7 cluster status schema and routing diagnostics are forward
  compatibility contracts for v0.8 Metadata Raft and v0.9 Multi-Raft,
  distributed transaction, and Changefeed work.

### Not Included
- Production remote scatter-gather execution.
- Raft-backed metadata consensus or Raft DDL distribution.
- Distributed transactions, Multi-Raft placement, and Changefeed execution.
- The alopex-py Client/Transaction/ConnectionPool API, which remains on the
  independent alopex-py client release track.

## [0.4.0]

### Added
- Polars Unity Catalog API for Python bindings.
- Catalog API entry point: `Catalog`.
- Catalog metadata classes: `CatalogInfo`, `NamespaceInfo`, `TableInfo`, `ColumnInfo`.
- Catalog methods:
  - `list_catalogs`, `list_namespaces`, `list_tables`, `get_table_info`
  - `create_catalog`, `delete_catalog`, `create_namespace`, `delete_namespace`
  - `create_table`, `delete_table`, `scan_table`, `write_table`

### Breaking Changes
- None.

## [0.4.2]

### Added
- TUI as default mode for interactive terminals; automatically activates in TTY without `--tui`.
- TUI support for all non-SQL commands (KV, Vector, HNSW, Columnar, Server, Profile).
- Unified TUI renderer for consistent Column/Row display across commands.
- Admin TUI navigation with data lifecycle operations (create/update/delete/archive/restore/backup/export).
- Lifecycle subcommands (`archive`, `restore`, `backup`, `export`) with placeholder implementations.
- mTLS certificate validation: expiration and CN/SAN host matching enforcement.
- `--insecure` flag for explicit HTTP opt-in with warning.
- SELECT literal-only queries support (e.g., `SELECT 1`, `SELECT 1, 'ok'`) without FROM clause.

### Changed
- Default UI mode is now TUI in TTY; `--batch` or `--output` forces batch mode.
- Non-TTY environments fall back to batch output with warning message.
- Admin TUI uses rainfrog-inspired layout with left navigation and main panel.

### Fixed
- HNSW persistence after inserts.
- Columnar compression defaults to zstd.

## [0.4.1]

### Added
- Streaming SQL over HTTP with JSON array output and backpressure-aware buffering.
- Server connection profiles with HTTPS enforcement and authentication (token/basic/mTLS).
- TUI status bar showing connection, row count, and processing status; improved keybindings.
- Server admin commands for status/metrics/health/compaction trigger with updated schema fields.

### Changed
- SQL server endpoint updated to `/api/sql/query`.
- Output formats aligned to table/json/csv/tsv/quiet (jsonl removed from documented support).
- TUI paging behavior now uses half-screen paging for Ctrl+U/Ctrl+D.

## [0.3.4]

### Added
- CLI profile management commands (create/list/show/delete/set-default).
- Batch mode support (`--batch`, TTY detection, `ALOPEX_MODE=batch`).
- KVS transaction commands (begin/get/put/delete/commit/rollback).
- Columnar ingest (Parquet/CSV) and index management (minmax/bloom).
- Streaming-friendly output for jsonl/csv/tsv.
- File format version compatibility checks.
- Shell completions (bash/zsh/fish/pwsh).
