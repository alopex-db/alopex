#!/usr/bin/env bash
set -euo pipefail

# Demo script:
# 1. Runs the flat-search benchmark (10k x 128) to show p50/p95.
# 2. Executes the vector E2E tests (upsert/search and checksum corruption detection).
#
# Usage:
#   ./scripts/demo_vector.sh

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "== Step 1: Flat search benchmark =="
(
  cd "$WORKDIR"
  cargo bench -p alopex-core --bench vector_flat -- --warm-up-time 1 --measurement-time 3 --sample-size 10
)

echo
echo "== Step 2: Vector E2E tests (embedded API + corruption detection) =="
(
  cd "$WORKDIR"
  cargo test -p alopex-embedded --test vector_e2e
)

echo
echo "Demo complete."
