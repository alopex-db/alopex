# Python server client

Alopex v0.8.8 adds a server client to the `alopex` Python package. The same
calling convention now runs either in-process or against a running
`alopex-server`, and the connection target is the only thing that changes.

```python
import alopex

embedded = alopex.connect("/var/lib/alopex/db")          # embedded Database
remote = alopex.connect("https://alopex.internal:8080",  # RemoteDatabase
                        api_key="secret")

for db in (embedded, remote):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute_sql("INSERT INTO items (id, name) VALUES (?, ?)", [1, "alpha"])
    assert db.execute_sql("SELECT id, name FROM items") == [{"id": 1, "name": "alpha"}]
```

## Connection targets

| Target | Result |
| --- | --- |
| `http://host:port`, `https://host:port` | `alopex.RemoteDatabase` |
| `:memory:` (the default) | `Database.open_in_memory()` |
| `file:///path/db`, `/path/db`, `db` | `Database.open(path)` |
| `s3://bucket/prefix` | `NotImplementedError` (`ALOPEX-PY204`) |
| anything else | `ValueError` (`ALOPEX-PY205`) |

A URL path becomes the API prefix, so `http://host:8080/api/v1` posts to
`/api/v1/sql` and `/api/v1/session/...`.

Options passed to `connect()` belong to the surface the target selects. Server
options (`api_key`, `timeout`, ...) on an embedded target are a `ValueError`;
embedded targets accept only `thread_mode`.

## Return values

`RemoteDatabase.execute_sql` returns exactly what `Database.execute_sql`
returns for the same statement:

| Statement | Return |
| --- | --- |
| SELECT | `list[dict[str, Any]]`, keys in column order |
| INSERT / UPDATE / DELETE | `int` rows affected |
| DDL | `None` |

The server sends results as `columns` / `rows` / `affected_rows`, with each
value in serde's externally tagged `SqlValue` form (`"Null"`, `{"BigInt": 6}`,
`{"Blob": [1, 2]}`, ...). The client normalizes them in one place, matching the
embedded conversion value for value: `Blob` becomes `bytes`, `Vector` becomes
`list[float]`, `Timestamp` stays epoch microseconds as `int`.

## Not available over the server client

Each of these raises `NotImplementedError` with `code == "ALOPEX-PY204"` and a
reason, rather than failing as a missing attribute:

| API | Reason |
| --- | --- |
| `execute_sql_stream`, `query_stream` | The server's JSONL stream carries no column metadata (`StreamItem` is `{row, error, done}`), so it cannot produce the dicts the embedded stream API yields. Stream APIs stay embedded-local in v0.8. |
| `create_hnsw_index`, `search_hnsw`, `drop_hnsw_index`, `get_hnsw_stats`, `upsert_to_hnsw`, `delete_from_hnsw` | `/hnsw/*` accepts neither `HnswConfig` (`m`, `ef_construction`) nor `ef_search`, and never returns `HnswStats` with the results. |
| `get`, `put`, `delete`, `upsert_vector`, `search_similar`, `get_vector` | `/kv/txn/*` uses a transaction id space separate from SQL sessions. |
| `flush`, `memory_usage`, `routing_diagnostics`, `thread_mode` | Process-local engine state a server does not expose to its caller. Use `RemoteDatabase.last_routing_diagnostics` for what the server attached to the last `/sql` response. |
| `begin(TxnMode.READ_ONLY)` | Server sessions are always read-write. |

`RemoteDatabase.cluster_status()` is implemented and returns the same
`ClusterStatusSnapshot` schema as the embedded accessor and gRPC `cluster_json`.

## Transactions

`begin()` opens a server session (`POST /session/begin`), attaches its
`session_id` to every `/sql` request, and finishes with
`/session/{id}/commit` or `/session/{id}/rollback`. `RemoteTransaction` is a
context manager and rolls back on an incomplete exit, like the embedded
`Transaction`. Server sessions expire after the server's `session_ttl`
(300 seconds by default); the expiry surfaces as `AlopexError` with the server's
`SESSION_EXPIRED` code, which the embedded transaction never raises.

## Errors

Server error codes are forwarded verbatim onto `AlopexError.code`. Remote errors
also carry `.correlation_id` and `.http_status`.

Both surfaces raise `AlopexError` for the same invalid statement, and errors
raised **during execution** (failed `CAST` → `ALOPEX-E004`, division by zero,
constraint violations) carry the identical stable code on both. Errors raised in
the server's **routing pre-pass** currently do not — see D20 below.

| Code | Meaning |
| --- | --- |
| `ALOPEX-PY201` | Connection failed (DNS, refused, TLS, reset after send) |
| `ALOPEX-PY202` | Client-side timeout |
| `ALOPEX-PY203` | Protocol violation (non-JSON body, unknown `SqlValue` tag, row/column arity mismatch) |
| `ALOPEX-PY204` | The operation has no server equivalent |
| `ALOPEX-PY205` | Unusable connection target or client option |
| `ALOPEX-PY999` | Closed handle (same code and message as the embedded bindings) |
| `INVALID_REQUEST`, `UNAUTHORIZED`, `NOT_FOUND`, `CONFLICT`, `PAYLOAD_TOO_LARGE`, `QUERY_TIMEOUT`, `SESSION_EXPIRED`, `CAPABILITY_UNAVAILABLE`, `NOT_IMPLEMENTED`, `INTERNAL`, `SERVER_BACKPRESSURE`, ... | Forwarded from the server |

## Client options

| Option | Default | Notes |
| --- | --- | --- |
| `api_key` | `None` | Sent as `x-api-key`; `Authorization: Bearer` works through `headers` |
| `timeout` | `60.0` | Twice the server's `query_timeout` default so the server's classified `QUERY_TIMEOUT` wins |
| `sql_path` | `<prefix>/sql` | `/api/sql/query` is the same handler |
| `api_prefix` | from the URL path | |
| `headers` | `None` | Extra request headers |
| `ssl_context` | `None` | Replaces `ssl.create_default_context()`; mTLS goes here |
| `insecure` | `False` | Required for plaintext `http://` to a non-loopback host |
| `retries` | `0` | Connection establishment only; a sent request is never resent |
| `keep_alive` | `True` | One reused connection per instance |
| `idle_reconnect_seconds` | `5.0` | Reconnect before sending after this much idle time |

`RemoteDatabase` holds one connection guarded by a lock. Build one instance per
thread when you need concurrency; connection pooling is out of scope for v0.8.8.
The constructor opens no socket, so building a client never blocks.

## Decisions

- **D1** — The client is pure Python on `http.client`. Rust with `reqwest` would
  pull tokio/hyper/rustls into the `_alopex` cdylib and inflate the abi3 wheels
  built for three operating systems, for a code path where the GIL is released
  during socket I/O anyway.
- **D2** — `?` placeholder expansion is not reimplemented. The embedded binder is
  exported as `_alopex._bind_sql_params` and reused, because a drift in quoting
  or comment handling would be an injection-shaped defect.
- **D3** — A new `RemoteDatabase` class plus a single `alopex.connect(target)`
  entry point. `Database` is a pyo3 `#[pyclass]`, so neither monkeypatching nor a
  Rust-side `connect` returning a Python object is sound.
- **D4** — The shared surface is `typing.Protocol` (`DatabaseLike`,
  `TransactionLike`), not a base class: a pyo3 pyclass cannot inherit one.
- **D5** — Wire-value normalization lives in one place and refuses unknown tags
  instead of passing them through.
- **D6** — The three-way return shape is decided in the CLI's order: non-empty
  `columns`, then `affected_rows`, then `None`.
- **D7/D8/D9** — Operations without a server equivalent raise
  `NotImplementedError` with a reason, never `AttributeError`. A
  surface-completeness test enumerates `dir(Database)` and `dir(Transaction)`, so
  a new embedded method fails the build until it is implemented or refused.
- **D10** — `begin()` is implemented; `TxnMode.READ_ONLY` is refused rather than
  silently upgraded to read-write.
- **D11** — Server codes are forwarded verbatim; `.correlation_id` and
  `.http_status` are attached.
- **D12** — Only five client codes are added. A closed handle reuses the embedded
  `AlopexError("database is closed")` with `ALOPEX-PY999`.
- **D13** — Retries default to off and never resend a request already on the
  wire, because `/sql` carries non-idempotent writes. Stale keep-alive
  connections are prevented (`idle_reconnect_seconds`) rather than retried.
- **D14** — The default client timeout is 60s so the server's own 30s
  `query_timeout` classification wins the race.
- **D15** — One connection per instance, serialized by a lock.
- **D16** — Plaintext `http://` off loopback requires `insecure=True`, mirroring
  the CLI's `validate_base_url`.
- **D17** — Non-finite floats arrive as JSON `null` because serde_json cannot
  encode them; the client fails with `ALOPEX-PY203` rather than inventing a
  value. Fixing the server-side encoding is follow-up work.
- **D18** — End-to-end tests start the server binary from a pytest fixture and
  skip when it is missing, unless `ALOPEX_REQUIRE_SERVER_E2E=1` (set in CI)
  turns the skip into a failure.
- **D19** — `Float` (f32) values are re-narrowed through IEEE-754 binary32 before
  widening to a Python float. serde_json writes the shortest text that
  round-trips as f32, so narrowing recovers the exact f32 and the widened result
  matches the embedded `f64::from(f32)` bit for bit. Verified on the wire: a
  FLOAT column holding `0.1` is sent as `{"Float":0.1}`, which without this step
  would read `0.1` remotely and `0.10000000149011612` embedded.
- **D20** — Planning-time error codes are reported as the server sends them, not
  repaired on the client. Before running a statement the server plans it for
  routing, and `async_plan_for_routing`
  (`crates/alopex-sql/src/storage/async_storage.rs`) stringifies any failure into
  `ExecutorError::InvalidOperation`. Parse (`ALOPEX-P###`), catalog
  (`ALOPEX-C###`), and type-check (`ALOPEX-T###`) errors therefore reach the wire
  as `ALOPEX-E999`, with the original code surviving only inside the message.
  This is a server-side defect that predates the Python client and affects the
  CLI's HTTP path identically; recovering the code by scraping the message text
  would be fragile and would hide it.
  `test_planning_errors_lose_their_stable_code_on_the_server` pins the current
  behavior so a server-side fix makes the asymmetry visible instead of silently
  changing the client's contract. Follow-up: make `async_plan_for_routing` carry
  the `SqlError` instead of flattening it, then re-verify the parity corpus
  (`scripts/parity/runner/normalize.py` extracts the first `ALOPEX-[A-Z]###` from
  the message, so the message change matters there).

## Testing

- `crates/alopex-py/tests/test_remote_client.py` — stub HTTP and raw-socket
  servers; no `alopex-server` binary required.
- `crates/alopex-py/tests/test_connect_target.py` — target routing and protocol
  conformance.
- `crates/alopex-py/tests/test_remote_e2e.py` — marked `requires_server`; runs
  one SQL script on both surfaces and compares the whole result list.

```bash
cargo build -p alopex-server
ALOPEX_REQUIRE_SERVER_E2E=1 \
  ALOPEX_SERVER_BIN=target/debug/alopex-server \
  pytest crates/alopex-py/tests -m requires_server
```
