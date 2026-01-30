#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <asan|tsan|lsan|msan>" >&2
  exit 2
fi

SANITIZER="$1"
case "${SANITIZER}" in
  asan) SAN_FLAG="address";;
  tsan) SAN_FLAG="thread";;
  lsan) SAN_FLAG="leak";;
  msan) SAN_FLAG="memory";;
  *)
    echo "unknown sanitizer: ${SANITIZER}" >&2
    exit 2
    ;;
esac

REPORT_DIR="${STRESS_REPORT_DIR:-target/sanitizer-reports/${SANITIZER}}"
ARTIFACT_DIR="${STRESS_ARTIFACTS_DIR:-target/sanitizer-artifacts/${SANITIZER}}"

export STRESS_REPORT_DIR="${REPORT_DIR}"
export STRESS_ARTIFACTS_DIR="${ARTIFACT_DIR}"
export RUSTFLAGS="-Zsanitizer=${SAN_FLAG} ${RUSTFLAGS:-}"
export RUSTDOCFLAGS="-Zsanitizer=${SAN_FLAG} ${RUSTDOCFLAGS:-}"

cargo +nightly test --workspace --features lane_sanitizer -- --test-threads=1
