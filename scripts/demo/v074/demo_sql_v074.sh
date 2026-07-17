#!/usr/bin/env bash
set -euo pipefail

# This demo can use a locally built CLI or the released CLI supplied by
# verify-release/run.sh through ALOPEX_CLI.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DATA_DIR="${ALOPEX_DATA_DIR:-$(mktemp -d)}"
KEEP_DATA_DIR=0
if [ -n "${ALOPEX_DATA_DIR:-}" ]; then
    KEEP_DATA_DIR=1
fi
cleanup() {
    if [ "${KEEP_DATA_DIR}" -eq 0 ]; then
        rm -rf "${DATA_DIR}"
    fi
}
trap cleanup EXIT

run_sql() {
    local title="$1"
    local sql="$2"
    echo "== ${title} =="
    if [ -n "${ALOPEX_CLI:-}" ]; then
        "${ALOPEX_CLI}" --data-dir "${DATA_DIR}" sql "${sql}"
    else
        (cd "${ROOT}" && cargo run -p alopex-cli -- --data-dir "${DATA_DIR}" sql "${sql}")
    fi
}

run_sql "Hash and encoding functions" \
    "SELECT md5('abc') AS md5, hex(unhex('A1B2')) AS hex_roundtrip, encode(unhex('A1B2'), 'base64') AS encoded;"

run_sql "UUID functions" \
    "SELECT gen_random_uuid() AS uuid_v4, uuidv7() AS uuid_v7;"

run_sql "Registry scalar functions" \
    "SELECT upper('alopex') AS upper_name, length('sql') AS name_length, coalesce(NULL, 'ok') AS fallback;"

run_sql "Numeric, regex, and pattern functions" \
    "SELECT abs(-3) AS absolute, round(1.234, 2) AS rounded, sqrt(9) AS root, regexp_match('id=42', '[0-9]+') AS match, 'alopex' LIKE 'alo%';"

run_sql "SQL standard string forms" \
    "SELECT substring('alopex' FROM 2 FOR 3) AS substring_value, position('pex' IN 'alopex') AS position_value;"

run_sql "Memory statistics function" "SELECT memory_stats() AS memory_stats;"
run_sql "I/O statistics function" "SELECT io_stats() AS io_stats;"
run_sql "Cache control function" "SELECT clear_cache() AS cleared_bytes;"

run_sql "PRAGMA controls" "PRAGMA cache_size = 8;"
run_sql "PRAGMA memory limit" "PRAGMA memory_limit = '64MiB';"
run_sql "PRAGMA I/O stats" "PRAGMA io_stats;"

echo "v0.7.4 SQL scalar and PRAGMA demo completed."
