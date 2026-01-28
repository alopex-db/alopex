#!/usr/bin/env bash
set -euo pipefail

ROOT="$(pwd)"
DATA_DIR="$(mktemp -d)"
SERVER_DATA_DIR="$(mktemp -d)"
DEMO_HOME="$(mktemp -d)"
COLUMNAR_FILE="$DATA_DIR/columnar.csv"
SERVER_PID=""
trap 'if [[ -n "$SERVER_PID" ]]; then kill "$SERVER_PID" >/dev/null 2>&1 || true; wait "$SERVER_PID" >/dev/null 2>&1 || true; fi; rm -rf "$DATA_DIR" "$SERVER_DATA_DIR" "$DEMO_HOME"' EXIT

cd "$ROOT"

echo "Preparing demo data..."
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" sql "CREATE TABLE items (id INT, name TEXT)"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" sql "INSERT INTO items VALUES (1,'alice'),(2,'bob'),(3,'carol')"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" kv put demo:1 alice
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" kv put demo:2 bob

echo
echo "1) Default TUI for SQL (press q to exit)"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" sql "SELECT id, name FROM items ORDER BY id"

echo
echo "2) Default TUI for non-SQL (KV list)"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" kv list --prefix demo:

echo
echo "3) Vector + HNSW (non-SQL TUI)"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" hnsw create demo_hnsw --dim 2 --metric cosine
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" vector upsert --index demo_hnsw --key item1 --vector "[0.1, 0.2]"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" vector upsert --index demo_hnsw --key item2 --vector "[0.2, 0.1]"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" vector search --index demo_hnsw --query "[0.1, 0.2]" -k 2
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" hnsw stats demo_hnsw

echo
echo "4) Columnar ingest + list + scan (non-SQL TUI)"
cat > "$COLUMNAR_FILE" <<'CSV'
id,name
1,alpha
2,beta
3,gamma
CSV

cargo run -p alopex-cli -- --data-dir "$DATA_DIR" columnar ingest --file "$COLUMNAR_FILE" --table demo_columnar --compression zstd
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" columnar list

segment_id=""
if command -v python3 >/dev/null 2>&1; then
  segment_id="$(cargo run -p alopex-cli -- --data-dir "$DATA_DIR" --output json columnar list \
    | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data[0]["segment_id"] if data else "")')"
elif command -v python >/dev/null 2>&1; then
  segment_id="$(cargo run -p alopex-cli -- --data-dir "$DATA_DIR" --output json columnar list \
    | python -c 'import json,sys; data=json.load(sys.stdin); print(data[0]["segment_id"] if data else "")')"
else
  echo "python not found; skipping columnar stats/scan/index demo."
fi

if [[ -n "$segment_id" ]]; then
  cargo run -p alopex-cli -- --data-dir "$DATA_DIR" columnar stats --segment "$segment_id"
echo "   Decoding scan output for display:"
scan_json="$DATA_DIR/columnar-scan.json"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" --output json columnar scan --segment "$segment_id" > "$scan_json"
if command -v python3 >/dev/null 2>&1; then
  python3 - <<'PY' "$scan_json"
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    rows = json.load(fh)
def decode(value):
    if isinstance(value, list):
        try:
            return bytes(value).decode("utf-8")
        except Exception:
            return value
    return value
print("id\tname")
for row in rows:
    values = list(row.values())
    if len(values) >= 2:
        print(f"{decode(values[0])}\t{decode(values[1])}")
PY
elif command -v python >/dev/null 2>&1; then
  python - <<'PY' "$scan_json"
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    rows = json.load(fh)
def decode(value):
    if isinstance(value, list):
        try:
            return bytes(value).decode("utf-8")
        except Exception:
            return value
    return value
print("id\tname")
for row in rows:
    values = list(row.values())
    if len(values) >= 2:
        print(f"{decode(values[0])}\t{decode(values[1])}")
PY
else
  cargo run -p alopex-cli -- --data-dir "$DATA_DIR" columnar scan --segment "$segment_id"
fi
  cargo run -p alopex-cli -- --data-dir "$DATA_DIR" columnar index create --segment "$segment_id" --column name --type minmax
  cargo run -p alopex-cli -- --data-dir "$DATA_DIR" columnar index list --segment "$segment_id"
fi

echo
echo "5) Batch override via --output (no TUI)"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" --output json kv list --prefix demo:

echo
echo "6) Admin console (Lifecycle panel)"
echo "   Use arrow keys to navigate, Enter to execute, ? for help."
cargo run -p alopex-cli -- --data-dir "$DATA_DIR"

echo
echo "7) Server-backed TUI demo"
HTTP_PORT=8080
ADMIN_PORT=8081
GRPC_PORT=9090
if command -v python3 >/dev/null 2>&1; then
  ports="$(python3 - <<'PY' 2>/dev/null || true
import socket
def pick():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
print(pick(), pick(), pick())
PY
)"
  if [[ -n "$ports" ]]; then
    read -r HTTP_PORT ADMIN_PORT GRPC_PORT <<<"$ports"
  fi
elif command -v python >/dev/null 2>&1; then
  ports="$(python - <<'PY' 2>/dev/null || true
import socket
def pick():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
print(pick(), pick(), pick())
PY
)"
  if [[ -n "$ports" ]]; then
    read -r HTTP_PORT ADMIN_PORT GRPC_PORT <<<"$ports"
  fi
fi

if [[ -z "${HTTP_PORT}" || -z "${ADMIN_PORT}" || -z "${GRPC_PORT}" ]]; then
  echo "   Warning: failed to pick random ports; falling back to defaults."
  HTTP_PORT=8080
  ADMIN_PORT=8081
  GRPC_PORT=9090
fi

SERVER_CONFIG="$SERVER_DATA_DIR/alopex.toml"
cat > "$SERVER_CONFIG" <<EOF
http_bind = "127.0.0.1:${HTTP_PORT}"
grpc_bind = "127.0.0.1:${GRPC_PORT}"
admin_bind = "127.0.0.1:${ADMIN_PORT}"
data_dir = "${SERVER_DATA_DIR}"
metrics_enabled = true
tracing_enabled = false
audit_log_enabled = false
EOF

SERVER_LOG="$SERVER_DATA_DIR/server.log"
echo "   Starting alopex-server on http://127.0.0.1:${HTTP_PORT}..."
cargo run -p alopex-server -- --config "$SERVER_CONFIG" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

check_url() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fs "$url" >/dev/null 2>&1
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- "$url" >/dev/null 2>&1
  elif command -v python3 >/dev/null 2>&1; then
    python3 - <<PY >/dev/null 2>&1
import urllib.request
urllib.request.urlopen("${url}", timeout=1).close()
PY
  elif command -v python >/dev/null 2>&1; then
    python - <<PY >/dev/null 2>&1
import urllib.request
urllib.request.urlopen("${url}", timeout=1).close()
PY
  else
    return 1
  fi
}

ready=0
url="http://127.0.0.1:${HTTP_PORT}/api/admin/health"
deadline=$((SECONDS + 90))
while [[ $SECONDS -lt $deadline ]]; do
  if check_url "$url"; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "   alopex-server exited before becoming ready."
    echo "   --- server log ---"
    tail -n 200 "$SERVER_LOG" || true
    exit 1
  fi
  sleep 0.2
done

if [[ $ready -ne 1 ]]; then
  echo "   server did not become ready"
  echo "   --- server log ---"
  tail -n 200 "$SERVER_LOG" || true
  exit 1
fi

mkdir -p "$DEMO_HOME/.alopex"
cat > "$DEMO_HOME/.alopex/config" <<EOF
[profiles.demo]
connection_type = "server"

[profiles.demo.server]
url = "http://127.0.0.1:${HTTP_PORT}"
insecure = true
EOF
chmod 600 "$DEMO_HOME/.alopex/config" 2>/dev/null || true

ALOPEX_BIN="$ROOT/target/debug/alopex"
if [[ ! -x "$ALOPEX_BIN" ]]; then
  cargo build -p alopex-cli
fi

echo "   Preparing data on server..."
HOME="$DEMO_HOME" "$ALOPEX_BIN" --profile demo --batch sql \
  "CREATE TABLE server_items (id INT PRIMARY KEY, name TEXT, embedding VECTOR(2, L2));"
HOME="$DEMO_HOME" "$ALOPEX_BIN" --profile demo --batch sql \
  "INSERT INTO server_items VALUES (1,'alpha',[0.1,0.2]),(2,'beta',[0.2,0.1]);"

echo "   Server SQL TUI (press q to exit)"
HOME="$DEMO_HOME" "$ALOPEX_BIN" --profile demo sql \
  "SELECT id, name FROM server_items ORDER BY id"

echo "   Server admin console (press q to exit)"
HOME="$DEMO_HOME" "$ALOPEX_BIN" --profile demo
