#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
PARSER_DIR="${ROOT_DIR}/crates/alopex-sql/nim-sql-parser"
BACKEND="${NIM_PARSER_BACKEND:-host}"
REQUIRED_NIM_VERSION="2.2.10"
REQUIRED_NIMBLE_VERSION="0.22.3"
REQUIRED_NIMBLE_SHA="42ef70c2102a942c46f13eb76872326edd525cec"
FAILURE_INJECTION="${ALOPEX_NIM_PARSER_INJECT_FAILURE:-0}"
SEED_DIR="${ALOPEX_NIMBLE_SEED_DIR:-${ALOPEX_NIMBLE_DIR:-}}"

usage() {
  cat <<'EOF'
Usage: scripts/test-nim-parser.sh [--backend host]

Runs the Nim SQL parser tests with the exact host Nim 2.2.10 and Nimble
0.22.3 toolchain. ALOPEX_NIMBLE_SEED_DIR or ALOPEX_NIMBLE_DIR must point to a
job-owned dependency seed containing npeg 1.3.0 and msgpack4nim 0.4.4.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == "--backend" ]]; then
  [[ "$#" == "2" ]] || { echo "--backend requires exactly one value" >&2; exit 2; }
  BACKEND="${2}"
elif [[ "$#" != "0" ]]; then
  echo "unexpected argument: ${1}" >&2
  exit 2
fi
if [[ "${BACKEND}" != "host" ]]; then
  echo "only the host Nim parser test backend is supported" >&2
  exit 2
fi
case "${FAILURE_INJECTION}" in
  0|1) ;;
  *) echo "ALOPEX_NIM_PARSER_INJECT_FAILURE must be 0 or 1" >&2; exit 2 ;;
esac
if [[ -z "${SEED_DIR}" ]]; then
  echo "ALOPEX_NIMBLE_SEED_DIR or ALOPEX_NIMBLE_DIR is required; NIMBLE_DIR is not accepted as a dependency seed" >&2
  exit 2
fi

RUNNER_IS_WINDOWS=0
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) RUNNER_IS_WINDOWS=1 ;;
esac

to_posix_path() {
  local candidate="$1"
  if [[ "${RUNNER_IS_WINDOWS}" == "1" ]]; then
    candidate="$(cygpath -u "${candidate}")"
  fi
  printf '%s\n' "${candidate}"
}

resolve_executable() {
  local candidate
  local link_target
  local candidate_dir
  candidate="$(to_posix_path "$1")"
  case "${candidate}" in
    */*) ;;
    *) candidate="$(command -v "${candidate}")" ;;
  esac
  while [[ -L "${candidate}" ]]; do
    link_target="$(readlink "${candidate}")"
    case "${link_target}" in
      /*) candidate="${link_target}" ;;
      *) candidate="$(dirname "${candidate}")/${link_target}" ;;
    esac
  done
  candidate_dir="$(cd "$(dirname "${candidate}")" && pwd -P)"
  printf '%s/%s\n' "${candidate_dir}" "$(basename "${candidate}")"
}

NIM_CANDIDATE="${ALOPEX_NIM_BIN:-}"
if [[ -z "${NIM_CANDIDATE}" ]]; then
  NIM_CANDIDATE="$(command -v nim || true)"
fi
[[ -n "${NIM_CANDIDATE}" ]] || { echo "nim is required" >&2; exit 1; }
NIM_CANDIDATE="$(to_posix_path "${NIM_CANDIDATE}")"
case "${NIM_CANDIDATE}" in
  */.asdf/shims/nim|*/.asdf/shims/nim.exe)
    command -v asdf >/dev/null 2>&1 || {
      echo "cannot resolve the asdf Nim shim" >&2
      exit 1
    }
    NIM_CANDIDATE="$(asdf which nim)"
    ;;
esac
NIM_BIN="$(resolve_executable "${NIM_CANDIDATE}")"
[[ -x "${NIM_BIN}" ]] || { echo "resolved Nim is not executable: ${NIM_BIN}" >&2; exit 1; }

NIMBLE_CANDIDATE="${ALOPEX_NIMBLE_BIN:-}"
if [[ -z "${NIMBLE_CANDIDATE}" ]]; then
  NIMBLE_CANDIDATE="$(command -v nimble || true)"
fi
[[ -n "${NIMBLE_CANDIDATE}" ]] || { echo "nimble is required" >&2; exit 1; }
NIMBLE_BIN="$(resolve_executable "${NIMBLE_CANDIDATE}")"
NIMBLE_BIN_DIR="$(dirname "${NIMBLE_BIN}")"
if [[ "$(basename "${NIMBLE_BIN_DIR}")" == "shim" ]]; then
  NIMBLE_NAME="nimble"
  if [[ "${RUNNER_IS_WINDOWS}" == "1" ]]; then
    NIMBLE_NAME="nimble.exe"
  fi
  NIMBLE_SIBLING="$(cd "${NIMBLE_BIN_DIR}/.." && pwd -P)/bin/${NIMBLE_NAME}"
  if [[ -x "${NIMBLE_SIBLING}" ]]; then
    NIMBLE_BIN="${NIMBLE_SIBLING}"
  fi
fi
[[ -x "${NIMBLE_BIN}" ]] || { echo "resolved Nimble is not executable: ${NIMBLE_BIN}" >&2; exit 1; }

SEED_DIR="$(to_posix_path "${SEED_DIR}")"
SEED_DIR="$(cd "${SEED_DIR}" && pwd -P)"
TEST_BUILD_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/alopex-nim-parser-tests.XXXXXX")"
cleanup() {
  local original_status=$?
  local cleanup_status=0
  trap - EXIT
  set +e
  rm -rf -- "${TEST_BUILD_ROOT}"
  cleanup_status=$?
  if [[ -e "${TEST_BUILD_ROOT}" ]]; then
    echo "failed to remove invocation-owned test root: ${TEST_BUILD_ROOT}" >&2
    cleanup_status=1
  elif [[ "${cleanup_status}" != "0" ]]; then
    echo "cleanup command failed for invocation-owned test root: ${TEST_BUILD_ROOT}" >&2
  fi
  if [[ "${original_status}" != "0" ]]; then
    exit "${original_status}"
  fi
  exit "${cleanup_status}"
}
trap cleanup EXIT
BIN_DIR="${TEST_BUILD_ROOT}/bin"
NIMCACHE_DIR="${TEST_BUILD_ROOT}/nimcache"
NIMBLE_DIR_OWNED="${TEST_BUILD_ROOT}/nimble-dir"
HOME_OWNED="${TEST_BUILD_ROOT}/home"
XDG_CACHE_OWNED="${TEST_BUILD_ROOT}/xdg-cache"
TMP_OWNED="${TEST_BUILD_ROOT}/tmp"
mkdir -p "${BIN_DIR}" "${NIMCACHE_DIR}" "${NIMBLE_DIR_OWNED}/pkgs2" \
  "${HOME_OWNED}" "${XDG_CACHE_OWNED}" "${TMP_OWNED}"

find_seeded_package() {
  local package_name="$1"
  local package_pattern="$2"
  local package_candidate
  local -a package_matches=()
  for package_candidate in "${SEED_DIR}/pkgs2"/${package_pattern}; do
    if [[ -d "${package_candidate}" ]]; then
      package_matches+=("${package_candidate}")
    fi
  done
  if [[ "${#package_matches[@]}" != "1" ]]; then
    echo "expected exactly one seeded ${package_name}, found ${#package_matches[@]}" >&2
    exit 2
  fi
  printf '%s\n' "${package_matches[0]}"
}

NPEG_SEED="$(find_seeded_package "npeg 1.3.0" "npeg-1.3.0-*")"
MSGPACK_SEED="$(find_seeded_package "msgpack4nim 0.4.4" "msgpack4nim-0.4.4-*")"
cp -R "${NPEG_SEED}" "${NIMBLE_DIR_OWNED}/pkgs2/"
cp -R "${MSGPACK_SEED}" "${NIMBLE_DIR_OWNED}/pkgs2/"
for metadata_name in packages_official.json packages_temp.json \
  official-nim-releases.json; do
  if [[ ! -f "${SEED_DIR}/${metadata_name}" ]]; then
    echo "dependency seed is missing ${metadata_name}" >&2
    exit 2
  fi
  cp "${SEED_DIR}/${metadata_name}" "${NIMBLE_DIR_OWNED}/"
done
cat >"${NIMBLE_DIR_OWNED}/nimbledata2.json" <<'EOF'
{
  "version": 1,
  "reverseDeps": {}
}
EOF

HOME_ENV="${HOME_OWNED}"
XDG_CACHE_ENV="${XDG_CACHE_OWNED}"
TMP_ENV="${TMP_OWNED}"
BUILD_ROOT_ENV="${TEST_BUILD_ROOT}"
NIMBLE_DIR_ARG="${NIMBLE_DIR_OWNED}"
NIM_BIN_ARG="${NIM_BIN}"
if [[ "${RUNNER_IS_WINDOWS}" == "1" ]]; then
  HOME_ENV="$(cygpath -w "${HOME_OWNED}")"
  XDG_CACHE_ENV="$(cygpath -w "${XDG_CACHE_OWNED}")"
  TMP_ENV="$(cygpath -w "${TMP_OWNED}")"
  BUILD_ROOT_ENV="$(cygpath -w "${TEST_BUILD_ROOT}")"
  NIMBLE_DIR_ARG="$(cygpath -w "${NIMBLE_DIR_OWNED}")"
  NIM_BIN_ARG="$(cygpath -w "${NIM_BIN}")"
  export USERPROFILE="${HOME_ENV}"
  export MSYS2_ARG_CONV_EXCL='*'
fi
export HOME="${HOME_ENV}"
export XDG_CACHE_HOME="${XDG_CACHE_ENV}"
export TEMP="${TMP_ENV}"
export TMP="${TMP_ENV}"
export TMPDIR="${TMP_ENV}"
export PATH="$(dirname "${NIM_BIN}"):${PATH}"
unset NIMBLE_DIR
export ALOPEX_NIM_BIN="${NIM_BIN_ARG}"
export ALOPEX_NIM_TEST_BUILD_ROOT="${BUILD_ROOT_ENV}"
export ALOPEX_NIM_PARSER_INJECT_FAILURE="${FAILURE_INJECTION}"

NIM_VERSION="$("${NIM_BIN}" --version | sed -n \
  's/.*Version \([0-9][0-9.]*\).*/\1/p' | head -n 1)"
if [[ "${NIM_VERSION}" != "${REQUIRED_NIM_VERSION}" ]]; then
  echo "Nim ${REQUIRED_NIM_VERSION} is required, found ${NIM_VERSION:-unknown}" >&2
  exit 2
fi

NIMBLE_IDENTITY="$("${NIMBLE_BIN}" --nimbleDir:"${NIMBLE_DIR_ARG}" \
  --nim:"${NIM_BIN_ARG}" --useSystemNim --offline --version)"
NIMBLE_VERSION="$(sed -n 's/^nimble v\([0-9][0-9.]*\).*/\1/p' \
  <<<"${NIMBLE_IDENTITY}" | head -n 1)"
if [[ "${NIMBLE_VERSION}" != "${REQUIRED_NIMBLE_VERSION}" ]]; then
  echo "Nimble ${REQUIRED_NIMBLE_VERSION} is required, found ${NIMBLE_VERSION:-unknown}" >&2
  exit 2
fi
if ! grep -Fqx "git hash: ${REQUIRED_NIMBLE_SHA}" <<<"${NIMBLE_IDENTITY}"; then
  echo "Nimble must be built from ${REQUIRED_NIMBLE_SHA}" >&2
  exit 2
fi

echo "Testing Nim SQL parser (host backend)"
(
  cd "${PARSER_DIR}"
  "${NIMBLE_BIN}" --nimbleDir:"${NIMBLE_DIR_ARG}" \
    --nim:"${NIM_BIN_ARG}" --useSystemNim --offline test
)
