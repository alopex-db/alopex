#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/roomtv/works/alopex-db/alopex"
DATA_DIR="$(mktemp -d)"
trap 'rm -rf "$DATA_DIR"' EXIT

cd "$ROOT"

cargo run -p alopex-cli -- --data-dir "$DATA_DIR" sql "CREATE TABLE items (id INT, name TEXT)"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" sql "INSERT INTO items VALUES (1,'alice'),(2,'bob'),(3,'carol')"
cargo run -p alopex-cli -- --data-dir "$DATA_DIR" sql --tui "SELECT id, name FROM items ORDER BY id"
