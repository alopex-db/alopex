#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
PARSER_DIR="${ROOT_DIR}/crates/alopex-sql/nim-sql-parser"
BACKEND="${NIM_PARSER_BACKEND:-auto}"
NIM_IMAGE="${NIM_IMAGE:-nimlang/nim:2.2@sha256:62428daa4a39baeb6f5e429a9c2ca3cee27a80ef880fe6e1bf3e29cc2296ac1b}"

if [[ "${1:-}" == "--backend" ]]; then
  BACKEND="${2:?missing backend after --backend}"
fi
case "${BACKEND}" in
  auto|host|docker) ;;
  *) echo "invalid Nim parser backend: ${BACKEND}" >&2; exit 2 ;;
esac
if [[ "${BACKEND}" == "auto" ]]; then
  if command -v nim >/dev/null 2>&1 && command -v nimble >/dev/null 2>&1; then BACKEND=host; else BACKEND=docker; fi
fi

echo "Testing Nim SQL parser (${BACKEND} backend)"
if [[ "${BACKEND}" == "host" ]]; then
  version="$(nim --version | sed -n 's/.*Version \([0-9][0-9.]*\).*/\1/p' | head -n 1)"
  [[ -n "${version}" ]] || { echo "could not determine Nim version" >&2; exit 1; }
  if [[ "$(printf '%s\n' "2.2.0" "${version}" | sort -V | head -n 1)" != "2.2.0" ]]; then
    echo "Nim >= 2.2 is required, found ${version}" >&2
    exit 1
  fi
  (cd "${PARSER_DIR}" && nimble install -y npeg msgpack4nim && nimble test)
else
  docker run --rm \
    --entrypoint /bin/bash \
    -v "${PARSER_DIR}:/workspace" \
    -w "/workspace" \
    -e HOME=/tmp \
    --user "$(id -u):$(id -g)" \
    "${NIM_IMAGE}" \
    -c 'export PATH=/opt/nim/bin:/usr/local/bin:/usr/bin:/bin; nimble install -y npeg msgpack4nim && nimble test'
fi
