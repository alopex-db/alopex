#!/usr/bin/env bash
set -euo pipefail

# Build the Nim FFI library used by alopex-sql. CI and release workflows use
# exact host Nim 2.2.10. Docker is an explicit local backend, or an auto-mode
# fallback only when nim or nimble is absent. A present wrong Nim version is
# selected as host and rejected rather than falling back to Docker.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
PARSER_DIR="${ROOT_DIR}/crates/alopex-sql/nim-sql-parser"
BACKEND="${NIM_PARSER_BACKEND:-auto}"
NIM_IMAGE="${NIM_IMAGE:-nimlang/nim:2.2@sha256:62428daa4a39baeb6f5e429a9c2ca3cee27a80ef880fe6e1bf3e29cc2296ac1b}"

case "$(uname -s)" in
  Darwin) OUTPUT="${PARSER_DIR}/libalopex_sql_parser.dylib" ;;
  MINGW*|MSYS*|CYGWIN*) OUTPUT="${PARSER_DIR}/alopex_sql_parser.dll" ;;
  *) OUTPUT="${PARSER_DIR}/libalopex_sql_parser.so" ;;
esac

usage() {
  cat <<'EOF'
Usage: scripts/build-nim-parser.sh [--backend auto|host|docker]

Builds crates/alopex-sql/nim-sql-parser/libalopex_sql_parser.so (or the
platform equivalent). CI and release use exact host Nim 2.2.10. Locally,
Docker is used when explicitly requested, or by auto only when nim or nimble
is absent. If both commands exist but Nim has the wrong version, host is
selected and the build is rejected without a Docker fallback.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == "--backend" ]]; then
  BACKEND="${2:?missing backend after --backend}"
fi
case "${BACKEND}" in
  auto|host|docker) ;;
  *) echo "invalid Nim parser backend: ${BACKEND}" >&2; exit 2 ;;
esac

build_host() {
  command -v nimble >/dev/null 2>&1 || {
    echo "nimble is required for the host backend" >&2
    return 1
  }
  local version
  version="$(nim --version | sed -n 's/.*Version \([0-9][0-9.]*\).*/\1/p' | head -n 1)"
  [[ -n "${version}" ]] || { echo "could not determine Nim version" >&2; return 1; }
  if [[ "${version}" != "2.2.10" ]]; then
    echo "Nim 2.2.10 is required, found ${version}" >&2
    return 1
  fi
  (cd "${PARSER_DIR}" && nimble install -y "npeg@1.3.0" "msgpack4nim@0.4.4" && nimble lib)
}

build_docker() {
  command -v docker >/dev/null 2>&1 || {
    echo "docker is required for the Docker backend" >&2
    return 1
  }
  docker run --rm \
    --entrypoint /bin/bash \
    -v "${PARSER_DIR}:/workspace" \
    -w "/workspace" \
    -e HOME=/tmp \
    --user "$(id -u):$(id -g)" \
    "${NIM_IMAGE}" \
    -c 'export PATH=/opt/nim/bin:/usr/local/bin:/usr/bin:/bin; nimble install -y "npeg@1.3.0" "msgpack4nim@0.4.4" && nimble lib'
}

if [[ "${BACKEND}" == "auto" ]]; then
  if command -v nim >/dev/null 2>&1 && command -v nimble >/dev/null 2>&1; then
    BACKEND=host
  else
    BACKEND=docker
  fi
fi

echo "Building Nim SQL parser (${BACKEND} backend)"
rm -f "${OUTPUT}"
if [[ "${BACKEND}" == "host" ]]; then
  build_host
else
  build_docker
fi

[[ -f "${OUTPUT}" ]] || { echo "Nim parser output not found: ${OUTPUT}" >&2; exit 1; }
echo "Built ${OUTPUT}"
