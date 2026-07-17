#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_DIR="${ALOPEX_BENCH_REPORT_DIR:-${ROOT}/target/bench-reports/v0.7.4}"
mkdir -p "${REPORT_DIR}"

cd "${ROOT}"
cargo bench -p alopex-sql --bench scalar_registry_bench -- --noplot \
    2>&1 | tee "${REPORT_DIR}/scalar_registry_bench.txt"

if [ -d "${ROOT}/target/criterion/scalar_registry" ]; then
    rm -rf "${REPORT_DIR}/criterion"
    cp -R "${ROOT}/target/criterion/scalar_registry" "${REPORT_DIR}/criterion"
fi

printf 'v0.7.4 scalar comparison benchmark completed.\n'
printf 'text report: %s\n' "${REPORT_DIR}/scalar_registry_bench.txt"
