# alopex-server

Alopex DB server providing HTTP and gRPC APIs for SQL execution, vector search, and session-aware transactions.

## Features

- HTTP JSON API for SQL, vector search/upsert, and session control
- gRPC API with streaming SQL results
- Prometheus-compatible metrics and health checks
- TLS and optional API key authentication
- Audit logging for DDL and config changes

## Quick start

```bash
cargo run -p alopex-server
```

Default bindings:
- HTTP: `127.0.0.1:8080`
- Admin: `127.0.0.1:8081`
- gRPC: `127.0.0.1:9090`

### SQL over HTTP

```bash
curl -X POST http://127.0.0.1:8080/sql \
  -H 'content-type: application/json' \
  -d '{"sql":"CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));"}'

curl -X POST http://127.0.0.1:8080/sql \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM items;"}'
```

### Vector search

```bash
curl -X POST http://127.0.0.1:8080/vector/upsert \
  -H 'content-type: application/json' \
  -d '{"table":"items","id":1,"vector":[0.0,0.0]}'

curl -X POST http://127.0.0.1:8080/vector/search \
  -H 'content-type: application/json' \
  -d '{"table":"items","vector":[0.8,0.0],"k":2}'
```

## Configuration

The server loads configuration from `alopex.toml` (current directory) or an explicit path.
Environment variables use the `ALOPEX__` prefix.

Example `alopex.toml`:

```toml
http_bind = "127.0.0.1:8080"
grpc_bind = "127.0.0.1:9090"
admin_bind = "127.0.0.1:8081"
api_prefix = ""
query_timeout = "30s"
max_request_size = 104857600
max_response_size = 104857600
max_connections = 1000
session_ttl = "300s"
metrics_enabled = true
tracing_enabled = true
audit_log_enabled = true

auth_mode = { type = "none" }
# auth_mode = { type = "dev", api_key = "secret" }

# tls = { cert_path = "./server.crt", key_path = "./server.key", ca_path = "./ca.crt", min_version = "tls13" }
# audit_log_output = { type = "file", path = "./audit.log" }
```

## Admin endpoints

- `GET /healthz` - health check
- `GET /status` - runtime status
- `GET /metrics` - Prometheus metrics

For a full configuration guide and API reference, see `docs/server-guide.md`.

## Functional tests and manual load measurements

`stream_deadline` checks the HTTP timeout response with one constant query and
a virtual clock. HTTP SQL unit tests separately check producer cancellation,
channel closure and cleanup. These functional checks run in ordinary CI.

The two `load_test` cases are always ignored, including with `lane_ci` and
`--all-features`. They measure 1,000 concurrent health requests or a slow consumer
over 2,000 rows; throughput and real-time scheduling are not CI acceptance gates.
Run them only as a manual experiment with an owning issue, source identity,
measured per-host storage, a time/memory budget and an output cleanup plan:

```bash
cargo test -p alopex-server --test load_test load_test_concurrent_requests -- --ignored --exact
cargo test -p alopex-server --test load_test load_test_backpressure_with_slow_client -- --ignored --exact
```

Run one case at a time, outside CI, without competing builds or measurements.
Retain failures and raw observations; they do not qualify a release on their own.
