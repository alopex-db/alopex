#!/usr/bin/env bash
set -euo pipefail

ROOT="$(pwd)"
DATA_DIR="$(mktemp -d)"
COLUMNAR_FILE="$DATA_DIR/columnar.csv"
trap 'rm -rf "$DATA_DIR"' EXIT

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
