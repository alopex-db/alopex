#!/usr/bin/env bash
set -euo pipefail

# The operator owns the exact-SHA admission hook and the continuous storage
# watchdog. A missing operator configuration must fail before any build.
[[ "${RUNNER_OS:-}" == 'macOS' ]] || exit 1
[[ "${GITHUB_EVENT_NAME:-}" == 'push' ]] || exit 1
[[ "${GITHUB_REF:-}" == 'refs/heads/integration/v0.8.16-candidate' ]] || exit 1
: "${ALOPEX_RUNNER_SHARED_TARGET:?operator must configure the shared target}"
: "${ALOPEX_RUNNER_ADMITTED_SHA:?operator admission hook must approve the job}"
[[ "${GITHUB_SHA}" == "${ALOPEX_RUNNER_ADMITTED_SHA}" ]] || exit 1
[[ "${ALOPEX_RUNNER_SHARED_TARGET}" == /* ]] || exit 1
[[ "${ALOPEX_RUNNER_SHARED_TARGET}" != '/' ]] || exit 1

mkdir -p "${ALOPEX_RUNNER_SHARED_TARGET}"
if [[ -L target ]]; then
  [[ "$(readlink target)" == "${ALOPEX_RUNNER_SHARED_TARGET}" ]] || exit 1
elif [[ -e target ]]; then
  echo 'Refusing a second Cargo target directory on this host' >&2
  exit 1
else
  ln -s "${ALOPEX_RUNNER_SHARED_TARGET}" target
fi
{
  echo "CARGO_TARGET_DIR=${ALOPEX_RUNNER_SHARED_TARGET}"
  echo 'CARGO_INCREMENTAL=0'
  echo 'CARGO_PROFILE_DEV_DEBUG=0'
  echo 'CARGO_PROFILE_TEST_DEBUG=0'
  echo 'CARGO_BUILD_JOBS=2'
  echo 'PIP_NO_CACHE_DIR=1'
} >> "${GITHUB_ENV}"
