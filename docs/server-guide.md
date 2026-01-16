# Alopex Server Guide

This guide describes how to run `alopex-server`, configure it, and interact with the HTTP/gRPC APIs.

## Running the server

```bash
cargo run -p alopex-server
```

By default, the server binds to:
- HTTP: `127.0.0.1:8080`
- Admin: `127.0.0.1:8081`
- gRPC: `127.0.0.1:9090`

Configuration is loaded from `alopex.toml` in the current directory, or a path provided by the launcher.
Environment variables can override settings using the `ALOPEX__` prefix (double underscore for nesting).

## Configuration reference

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `http_bind` | string | `127.0.0.1:8080` | HTTP API bind address |
| `grpc_bind` | string | `127.0.0.1:9090` | gRPC bind address |
| `admin_bind` | string | `127.0.0.1:8081` | Admin bind address |
| `admin_allowlist` | list[string] | `[]` | Allowlist for non-loopback admin access |
| `data_dir` | string | `./data` | Storage directory |
| `api_prefix` | string | `""` | Optional HTTP API prefix (`/api/v1`) |
| `auth_mode` | object | `{ type = "none" }` | Auth mode (`none` or `dev`) |
| `tls` | object | `null` | TLS settings (see below) |
| `query_timeout` | duration | `30s` | Query timeout |
| `max_request_size` | integer | `104857600` | Max request body size (bytes) |
| `max_response_size` | integer | `104857600` | Max response size (bytes) |
| `max_connections` | integer | `1000` | Concurrency limit |
| `session_ttl` | duration | `300s` | Session TTL |
| `metrics_enabled` | bool | `true` | Enable metrics |
| `tracing_enabled` | bool | `true` | Enable tracing |
| `audit_log_enabled` | bool | `true` | Enable audit logs |
| `audit_log_output` | object | `{ type = "stdout" }` | Audit log output (`stdout` or `file`) |

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

### TLS configuration

```toml
tls = {
  cert_path = "./server.crt",
  key_path = "./server.key",
  ca_path = "./ca.crt", # optional, enable mTLS
  min_version = "tls13"
}
```

Supported `min_version` values: `tls12`, `tls13`.

### Auth configuration

Use `auth_mode` to enable API key authentication:

```toml
auth_mode = { type = "dev", api_key = "secret" }
```

Clients must send `x-api-key: secret` or `Authorization: Bearer secret`.

## HTTP API reference

All HTTP endpoints accept JSON and return JSON. Correlation IDs are returned in `x-correlation-id` headers.

### SQL execution

`POST /sql`

Request:

```json
{
  "sql": "SELECT * FROM items;",
  "session_id": null,
  "streaming": false
}
```

Responses:

- Non-streaming (default):

```json
{
  "columns": [{"name": "id", "data_type": "INTEGER"}],
  "rows": [[1]],
  "affected_rows": null
}
```

- Streaming (`streaming: true`): JSONL stream, one object per line:

```json
{"row":[1],"error":null,"done":false}
{"row":null,"error":null,"done":true}
```

### Vector search

`POST /vector/search`

```json
{
  "table": "items",
  "vector": [0.8, 0.0],
  "k": 5,
  "index": null,
  "column": null
}
```

### Vector upsert

`POST /vector/upsert`

```json
{
  "table": "items",
  "id": 1,
  "vector": [0.0, 0.0],
  "column": null
}
```

### Sessions

- `POST /session/begin`
- `POST /session/{id}/commit`
- `POST /session/{id}/rollback`

## gRPC API reference

Service: `alopex.v0.AlopexService`

- `ExecuteSql` (stream `Row`)
- `ExecuteDdl`
- `ExecuteDml`
- `BeginTransaction`
- `CommitTransaction`
- `RollbackTransaction`
- `VectorSearch`
- `VectorUpsert`
- `Health`

See `crates/alopex-server/proto/alopex.proto` for message definitions.

## Admin endpoints

- `GET /healthz` - health check
- `GET /status` - runtime status
- `GET /metrics` - Prometheus metrics

## CLI access

Use profiles to connect to a running server:

```toml
[profiles.prod]
connection_type = "server"

[profiles.prod.server]
url = "https://127.0.0.1:8080"
auth = "token"
token = "secret"
```

```bash
alopex --profile prod sql "SELECT 1"
alopex --profile prod sql --tui "SELECT * FROM items"
```

Server management commands:

```bash
alopex --profile prod server status
alopex --profile prod server metrics
alopex --profile prod server health
alopex --profile prod server compaction trigger
```
