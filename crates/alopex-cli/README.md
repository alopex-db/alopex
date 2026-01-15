# Alopex CLI

Alopex CLI provides access to embedded and server-backed Alopex DB features.
It supports SQL, KV, Vector, and Columnar workflows with streaming output and
an optional TUI preview for SELECT queries.

## Quick start

Local (embedded) SQL:

```bash
alopex --data-dir ./data sql "SELECT 1"
```

Server profile (HTTPS required):

```bash
alopex --profile prod sql "SELECT * FROM users" --fetch-size 500 --max-rows 1000
```

## Output formats

Use `--output` to control formatting:

- `table` (default)
- `json`
- `csv`
- `tsv`

## Streaming flags

For SELECT queries, streaming output supports:

- `--fetch-size <n>`: server batch size
- `--max-rows <n>`: stop after N rows
- `--deadline <duration>`: timeout (examples: `60s`, `5m`, `1h`)

Example:

```bash
alopex sql "SELECT * FROM events" --fetch-size 1000 --max-rows 10000 --deadline 90s --output json
```

## TUI preview

Launch the TUI for SELECT results:

```bash
alopex sql --tui "SELECT id, name FROM items"
```

Keybindings (TUI):

- `q` / `Esc`: quit
- `?`: help
- `/`: search
- `n` / `N`: next/previous match
- `hjkl` or arrow keys: move selection
- `g` / `G`: jump top/bottom
- `Ctrl+d` / `Ctrl+u`: page down/up
- `Enter`: toggle detail panel
- `J` / `K`: scroll detail panel

Note: `--tui` requires a TTY. When running in non-interactive mode, the CLI
falls back to batch output and preserves `--output` formatting.

## Profiles and server connections

Profiles are stored in `~/.alopex/config` (TOML). Example with a local profile:

```toml
[profiles.local]
connection_type = "local"

[profiles.local.local]
path = "/var/lib/alopex"
```

Server profile with fallback to local data directory:

```toml
[profiles.prod]
connection_type = "server"

[profiles.prod.server]
url = "https://db.example.com"

# Optional local fallback when the server is unavailable
[profiles.prod.local]
path = "/var/lib/alopex"
```

Set a default profile:

```toml
default_profile = "prod"
```

## Authentication

### Token

```toml
[profiles.prod]
connection_type = "server"

[profiles.prod.server]
url = "https://db.example.com"
auth = "token"
token = "YOUR_TOKEN"
```

### Basic (password_command)

```toml
[profiles.prod]
connection_type = "server"

[profiles.prod.server]
url = "https://db.example.com"
auth = "basic"
username = "alice"
password_command = "security find-generic-password -w -s alopex"
```

### mTLS

```toml
[profiles.prod]
connection_type = "server"

[profiles.prod.server]
url = "https://db.example.com"
auth = "mtls"
cert_path = "/etc/alopex/client.pem"
key_path = "/etc/alopex/client-key.pem"
```

## Server management commands

All server commands require a server profile. Commands:

```bash
alopex --profile prod server status
alopex --profile prod server metrics
alopex --profile prod server health
alopex --profile prod server compaction trigger
```

## Examples

SQL streaming to CSV:

```bash
alopex sql "SELECT * FROM events" --output csv --fetch-size 1000 --max-rows 5000
```

KV get:

```bash
alopex kv get my-key
```

Vector search:

```bash
alopex vector search --index my_index --query "[0.1,0.2,0.3]" --k 10
```
