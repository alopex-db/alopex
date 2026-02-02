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

OS_NAME="$(uname -s)"
SAN_FLAGS="-Zsanitizer=${SAN_FLAG} -Cforce-frame-pointers=yes -Cdebuginfo=1 -Cunsafe-allow-abi-mismatch=sanitizer"
HOST_TRIPLE="$(rustc -vV | awk '/^host: /{print $2}')"
HOST_ENV="$(echo "${HOST_TRIPLE}" | tr '[:lower:]-' '[:upper:]_')"
TARGET_RUSTFLAGS_VAR="CARGO_TARGET_${HOST_ENV}_RUSTFLAGS"
TARGET_RUSTDOCFLAGS_VAR="CARGO_TARGET_${HOST_ENV}_RUSTDOCFLAGS"
EXTRA_RUSTFLAGS="${RUSTFLAGS:-}"
EXTRA_RUSTDOCFLAGS="${RUSTDOCFLAGS:-}"

export STRESS_REPORT_DIR="${REPORT_DIR}"
export STRESS_ARTIFACTS_DIR="${ARTIFACT_DIR}"
export RUSTFLAGS=""
export RUSTDOCFLAGS="${SAN_FLAGS} ${EXTRA_RUSTDOCFLAGS}"
export "${TARGET_RUSTFLAGS_VAR}=${SAN_FLAGS} ${EXTRA_RUSTFLAGS} ${!TARGET_RUSTFLAGS_VAR:-}"
export "${TARGET_RUSTDOCFLAGS_VAR}=${SAN_FLAGS} ${EXTRA_RUSTDOCFLAGS} ${!TARGET_RUSTDOCFLAGS_VAR:-}"

if [[ "${OS_NAME}" == "Darwin" ]]; then
  if command -v xcrun >/dev/null 2>&1; then
    SDK_PATH="$(xcrun --show-sdk-path)"
    if [[ "${SANITIZER}" == "asan" ]]; then
      RUNTIME_PATH="${SDK_PATH}/usr/lib/clang/"*/lib/darwin/libclang_rt.asan_osx_dynamic.dylib
    elif [[ "${SANITIZER}" == "tsan" ]]; then
      RUNTIME_PATH="${SDK_PATH}/usr/lib/clang/"*/lib/darwin/libclang_rt.tsan_osx_dynamic.dylib
    fi
    if [[ -n "${RUNTIME_PATH:-}" ]]; then
      export DYLD_INSERT_LIBRARIES="${RUNTIME_PATH}"
      export DYLD_FORCE_FLAT_NAMESPACE=1
    fi
  fi
fi

if [[ "${OS_NAME}" == "Linux" ]] && command -v python3 >/dev/null 2>&1; then
  PY_LIBDIR="$(python3 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
  if [[ -n "${PY_LIBDIR}" ]] && [[ -d "${PY_LIBDIR}" ]]; then
    export LD_LIBRARY_PATH="${PY_LIBDIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  fi
fi

cargo +nightly test -Zbuild-std -Zbuild-std-features=panic-unwind \
  --workspace --features lane_sanitizer,test-hooks -- --test-threads=1
