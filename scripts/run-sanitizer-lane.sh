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
TARGET="${SANITIZER_TARGET:-$(rustc +nightly -vV | awk '/host:/ {print $2}')}"
TARGET_ENV="$(printf '%s' "${TARGET}" | tr '[:lower:]-' '[:upper:]_')"
TARGET_RUSTFLAGS_VAR="CARGO_TARGET_${TARGET_ENV}_RUSTFLAGS"
TARGET_RUSTFLAGS="-Zsanitizer=${SAN_FLAG} -Cforce-frame-pointers=yes ${RUSTFLAGS:-}"

if [[ "${SANITIZER}" == "asan" ]]; then
  export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"
fi

CARGO_UNSTABLE_ARGS=()
if [[ "${SANITIZER}" == "tsan" || "${SANITIZER}" == "msan" ]]; then
  CARGO_UNSTABLE_ARGS=(-Zbuild-std)
fi

export STRESS_REPORT_DIR="${REPORT_DIR}"
export STRESS_ARTIFACTS_DIR="${ARTIFACT_DIR}"
export "${TARGET_RUSTFLAGS_VAR}=${TARGET_RUSTFLAGS}"
unset RUSTFLAGS
unset RUSTDOCFLAGS

cargo +nightly test \
  "${CARGO_UNSTABLE_ARGS[@]}" \
  -p alopex-core \
  --target "${TARGET}" \
  --features lane_sanitizer \
  --test stress_sanitizer \
  -- --test-threads=1
