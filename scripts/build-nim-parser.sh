#!/usr/bin/env bash
set -euo pipefail

# Build the Nim FFI library used by alopex-sql. Release archives are available
# only from an exact, native host build with an isolated offline dependency
# store. Docker remains a non-release local fallback.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
PARSER_DIR="${ROOT_DIR}/crates/alopex-sql/nim-sql-parser"
MANIFEST_TOOL="${ROOT_DIR}/scripts/release/parser_asset_manifest.py"
BACKEND="${NIM_PARSER_BACKEND:-auto}"
TARGET=""
ARCHIVE_DIR=""
REQUIRED_ALOPEX_VERSION="$(python3 - "${ROOT_DIR}/Cargo.toml" <<'PY'
import pathlib
import sys
import tomllib

with pathlib.Path(sys.argv[1]).open("rb") as stream:
    print(tomllib.load(stream)["workspace"]["package"]["version"])
PY
)"
REQUIRED_CONTRACT_VERSION="0.19.0"
REQUIRED_NIM_VERSION="2.2.10"
REQUIRED_NIMBLE_VERSION="0.22.3"
REQUIRED_NIMBLE_SHA="42ef70c2102a942c46f13eb76872326edd525cec"
REQUIRED_BUILD_PROFILE="nim-release-dual-library-v2"
NIM_IMAGE="${NIM_IMAGE:-nimlang/nim:2.2@sha256:62428daa4a39baeb6f5e429a9c2ca3cee27a80ef880fe6e1bf3e29cc2296ac1b}"
SEED_DIR="${ALOPEX_NIMBLE_SEED_DIR:-${ALOPEX_NIMBLE_DIR:-}}"

RUNNER_IS_WINDOWS=0
case "$(uname -s)" in
  Darwin)
    OUTPUT="${PARSER_DIR}/libalopex_sql_parser.dylib"
    STATIC_OUTPUT="${PARSER_DIR}/libalopex_sql_parser.a"
    case "$(uname -m)" in
      arm64|aarch64) HOST_TARGET="aarch64-apple-darwin" ;;
      x86_64|amd64) HOST_TARGET="x86_64-apple-darwin" ;;
      *) HOST_TARGET="" ;;
    esac
    ;;
  MINGW*|MSYS*|CYGWIN*)
    RUNNER_IS_WINDOWS=1
    OUTPUT="${PARSER_DIR}/alopex_sql_parser.dll"
    STATIC_OUTPUT="${PARSER_DIR}/alopex_sql_parser.lib"
    case "$(uname -m)" in
      x86_64|amd64) HOST_TARGET="x86_64-pc-windows-msvc" ;;
      *) HOST_TARGET="" ;;
    esac
    ;;
  Linux)
    OUTPUT="${PARSER_DIR}/libalopex_sql_parser.so"
    STATIC_OUTPUT="${PARSER_DIR}/libalopex_sql_parser.a"
    case "$(uname -m)" in
      x86_64|amd64) HOST_TARGET="x86_64-unknown-linux-gnu" ;;
      *) HOST_TARGET="" ;;
    esac
    ;;
  *)
    OUTPUT="${PARSER_DIR}/libalopex_sql_parser.so"
    STATIC_OUTPUT="${PARSER_DIR}/libalopex_sql_parser.a"
    HOST_TARGET=""
    ;;
esac
DEFAULT_OUTPUT="${OUTPUT}"
DEFAULT_STATIC_OUTPUT="${STATIC_OUTPUT}"

usage() {
  cat <<'EOF'
Usage: scripts/build-nim-parser.sh [options]

Options:
  --backend auto|host|docker  Select the build backend (default: auto).
  --target TARGET            Declare the native release target triple.
  --archive-dir DIRECTORY    Emit a deterministic target record and tar.gz.
  --output FILE              Write the target library and sidecars to FILE.

Host builds require exact Nim 2.2.10, Nimble 0.22.3 from release commit
42ef70c2102a942c46f13eb76872326edd525cec, and ALOPEX_NIMBLE_SEED_DIR or
ALOPEX_NIMBLE_DIR containing the exact offline dependencies and metadata.
Deterministic archives require an explicit native --target and the host backend.
EOF
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --help|-h)
      usage
      exit 0
      ;;
    --backend)
      [[ "$#" -ge 2 ]] || { echo "--backend requires a value" >&2; exit 2; }
      BACKEND="$2"
      shift 2
      ;;
    --target)
      [[ "$#" -ge 2 ]] || { echo "--target requires a value" >&2; exit 2; }
      TARGET="$2"
      shift 2
      ;;
    --archive-dir)
      [[ "$#" -ge 2 ]] || { echo "--archive-dir requires a value" >&2; exit 2; }
      ARCHIVE_DIR="$2"
      shift 2
      ;;
    --output)
      [[ "$#" -ge 2 ]] || { echo "--output requires a value" >&2; exit 2; }
      OUTPUT="$2"
      shift 2
      ;;
    *)
      echo "unexpected argument: $1" >&2
      exit 2
      ;;
  esac
done

case "${BACKEND}" in
  auto|host|docker) ;;
  *) echo "invalid Nim parser backend: ${BACKEND}" >&2; exit 2 ;;
esac

if [[ -n "${ARCHIVE_DIR}" ]]; then
  [[ -n "${TARGET}" ]] || {
    echo "--archive-dir requires an explicit native --target" >&2
    exit 2
  }
  if [[ "${BACKEND}" == "docker" ]]; then
    echo "deterministic parser archives require the host backend" >&2
    exit 2
  fi
fi

to_posix_path() {
  local candidate="$1"
  if [[ "${RUNNER_IS_WINDOWS}" == "1" ]]; then
    candidate="$(cygpath -u "${candidate}")"
  fi
  printf '%s\n' "${candidate}"
}

to_native_path() {
  local candidate="$1"
  if [[ "${RUNNER_IS_WINDOWS}" == "1" ]]; then
    candidate="$(cygpath -w "${candidate}")"
  fi
  printf '%s\n' "${candidate}"
}

validate_output_path() {
  local expected_name
  local output_name
  local output_parent
  local canonical_parent
  local static_output_name

  OUTPUT="$(to_posix_path "${OUTPUT}")"
  expected_name="$(basename "${DEFAULT_OUTPUT}")"
  output_name="$(basename "${OUTPUT}")"
  static_output_name="$(basename "${STATIC_OUTPUT}")"
  if [[ "${output_name}" != "${expected_name}" ]]; then
    echo "--output basename must be ${expected_name}" >&2
    exit 2
  fi

  output_parent="$(dirname "${OUTPUT}")"
  if [[ ! -d "${output_parent}" || -L "${output_parent}" ]]; then
    echo "--output parent must be an existing real directory: ${output_parent}" >&2
    exit 2
  fi
  canonical_parent="$(cd "${output_parent}" && pwd -P)"
  OUTPUT="${canonical_parent}/${output_name}"
  static_output_name="$(basename "${DEFAULT_STATIC_OUTPUT}")"
  STATIC_OUTPUT="${canonical_parent}/${static_output_name}"
  if [[ -L "${OUTPUT}" || ( -e "${OUTPUT}" && ! -f "${OUTPUT}" ) ]]; then
    echo "--output must be absent or a regular file: ${OUTPUT}" >&2
    exit 2
  fi
  if [[ -L "${STATIC_OUTPUT}" || ( -e "${STATIC_OUTPUT}" && ! -f "${STATIC_OUTPUT}" ) ]]; then
    echo "static parser output must be absent or a regular file: ${STATIC_OUTPUT}" >&2
    exit 2
  fi
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

resolve_host_tools() {
  local nim_candidate="${ALOPEX_NIM_BIN:-}"
  local nimble_candidate="${ALOPEX_NIMBLE_BIN:-}"
  local nimble_bin_dir
  local nimble_name
  local nimble_sibling
  local python_candidate

  if [[ -z "${nim_candidate}" ]]; then
    nim_candidate="$(command -v nim || true)"
  fi
  [[ -n "${nim_candidate}" ]] || { echo "nim is required" >&2; exit 1; }
  nim_candidate="$(to_posix_path "${nim_candidate}")"
  case "${nim_candidate}" in
    */.asdf/shims/nim|*/.asdf/shims/nim.exe)
      command -v asdf >/dev/null 2>&1 || {
        echo "cannot resolve the asdf Nim shim" >&2
        exit 1
      }
      nim_candidate="$(asdf which nim)"
      ;;
  esac
  NIM_BIN="$(resolve_executable "${nim_candidate}")"
  [[ -x "${NIM_BIN}" ]] || {
    echo "resolved Nim is not executable: ${NIM_BIN}" >&2
    exit 1
  }

  if [[ -z "${nimble_candidate}" ]]; then
    nimble_candidate="$(command -v nimble || true)"
  fi
  [[ -n "${nimble_candidate}" ]] || { echo "nimble is required" >&2; exit 1; }
  NIMBLE_BIN="$(resolve_executable "${nimble_candidate}")"
  nimble_bin_dir="$(dirname "${NIMBLE_BIN}")"
  if [[ "$(basename "${nimble_bin_dir}")" == "shim" ]]; then
    nimble_name="nimble"
    if [[ "${RUNNER_IS_WINDOWS}" == "1" ]]; then
      nimble_name="nimble.exe"
    fi
    nimble_sibling="$(cd "${nimble_bin_dir}/.." && pwd -P)/bin/${nimble_name}"
    if [[ -x "${nimble_sibling}" ]]; then
      NIMBLE_BIN="${nimble_sibling}"
    fi
  fi
  [[ -x "${NIMBLE_BIN}" ]] || {
    echo "resolved Nimble is not executable: ${NIMBLE_BIN}" >&2
    exit 1
  }

  python_candidate="$(command -v python3 || command -v python || true)"
  [[ -n "${python_candidate}" ]] || {
    echo "Python 3 is required to produce parser archives" >&2
    exit 1
  }
  python_candidate="$(to_posix_path "${python_candidate}")"
  case "${python_candidate}" in
    */.asdf/shims/python|*/.asdf/shims/python.exe|*/.asdf/shims/python3|*/.asdf/shims/python3.exe)
      command -v asdf >/dev/null 2>&1 || {
        echo "cannot resolve the asdf Python shim" >&2
        exit 1
      }
      python_candidate="$(asdf which "$(basename "${python_candidate}" .exe)")"
      ;;
  esac
  PYTHON_BIN="$(resolve_executable "${python_candidate}")"
  [[ -x "${PYTHON_BIN}" ]] || {
    echo "resolved Python is not executable: ${PYTHON_BIN}" >&2
    exit 1
  }
}

if [[ "${BACKEND}" == "auto" ]]; then
  if command -v nim >/dev/null 2>&1 && command -v nimble >/dev/null 2>&1; then
    BACKEND="host"
  else
    BACKEND="docker"
  fi
fi

if [[ -n "${ARCHIVE_DIR}" && "${BACKEND}" != "host" ]]; then
  echo "deterministic parser archives require the host backend" >&2
  exit 2
fi

build_host() (
  local build_root
  local build_lock
  local build_root_created=0
  local build_lock_created=0
  local nimble_dir_owned
  local home_owned
  local xdg_cache_owned
  local tmp_owned
  local npeg_seed
  local msgpack_seed
  local npeg_owned
  local msgpack_owned
  local npeg_resolved
  local msgpack_resolved
  local metadata_name
  local nim_version
  local nimble_identity
  local nimble_version
  local nim_bin_arg
  local nimble_dir_arg
  local home_env
  local xdg_cache_env
  local tmp_env
  local archive_dir_arg
  local parser_dir_arg
  local output_arg
  local nimble_bin_arg
  local npeg_arg
  local msgpack_arg
  local manifest_tool_arg
  local packages_official_arg
  local packages_temp_arg
  local python_bin
  local original_status
  local cleanup_status
  local build_succeeded=0

  resolve_host_tools
  [[ -n "${SEED_DIR}" ]] || {
    echo "ALOPEX_NIMBLE_SEED_DIR or ALOPEX_NIMBLE_DIR is required for host builds" >&2
    exit 2
  }
  SEED_DIR="$(to_posix_path "${SEED_DIR}")"
  SEED_DIR="$(cd "${SEED_DIR}" && pwd -P)"

  if [[ -z "${HOST_TARGET}" ]]; then
    echo "unsupported native host for exact parser builds" >&2
    exit 2
  fi
  if [[ -n "${TARGET}" && "${TARGET}" != "${HOST_TARGET}" ]]; then
    echo "target ${TARGET} does not match native host ${HOST_TARGET}" >&2
    exit 2
  fi

  # Nim derives private RTTI identities from dependency source paths. A random
  # build root therefore changes otherwise identical native bytes. Use one
  # target-qualified canonical path, with an atomic lock, and remove only paths
  # created by this invocation.
  build_root="/tmp/alopex-nim-parser-v${REQUIRED_ALOPEX_VERSION}-contract-${REQUIRED_CONTRACT_VERSION}-${HOST_TARGET}"
  build_lock="${build_root}.lock"
  cleanup_host_build() {
    original_status=$?
    trap - EXIT
    set +e
    cleanup_status=0
    if [[ "${build_root_created}" == "1" ]]; then
      rm -rf -- "${build_root}"
      cleanup_status=$?
    fi
    if [[ "${build_lock_created}" == "1" ]]; then
      rmdir -- "${build_lock}" || cleanup_status=1
    fi
    if [[ "${build_succeeded}" != "1" ]]; then
      rm -f -- "${OUTPUT}"
    fi
    if [[ "${build_root_created}" == "1" && -e "${build_root}" ]]; then
      echo "failed to remove invocation-owned build root: ${build_root}" >&2
      cleanup_status=1
    fi
    if [[ "${build_lock_created}" == "1" && -e "${build_lock}" ]]; then
      echo "failed to remove invocation-owned build lock: ${build_lock}" >&2
      cleanup_status=1
    fi
    if [[ "${original_status}" != "0" ]]; then
      exit "${original_status}"
    fi
    exit "${cleanup_status}"
  }

  if ! mkdir "${build_lock}" 2>/dev/null; then
    echo "canonical parser build root is already locked: ${build_lock}" >&2
    exit 2
  fi
  build_lock_created=1
  trap cleanup_host_build EXIT
  if [[ -e "${build_root}" ]]; then
    echo "canonical parser build root already exists: ${build_root}" >&2
    exit 2
  fi
  mkdir "${build_root}"
  build_root_created=1

  nimble_dir_owned="${build_root}/nimble-dir"
  home_owned="${build_root}/home"
  xdg_cache_owned="${build_root}/xdg-cache"
  tmp_owned="${build_root}/tmp"
  mkdir -p "${nimble_dir_owned}/pkgs2" "${build_root}/nimcache" \
    "${home_owned}" "${xdg_cache_owned}" "${tmp_owned}"

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

  npeg_seed="$(find_seeded_package "npeg 1.3.0" "npeg-1.3.0-*")"
  msgpack_seed="$(find_seeded_package "msgpack4nim 0.4.4" "msgpack4nim-0.4.4-*")"
  cp -R "${npeg_seed}" "${nimble_dir_owned}/pkgs2/"
  cp -R "${msgpack_seed}" "${nimble_dir_owned}/pkgs2/"
  npeg_owned="${nimble_dir_owned}/pkgs2/$(basename "${npeg_seed}")"
  msgpack_owned="${nimble_dir_owned}/pkgs2/$(basename "${msgpack_seed}")"
  for metadata_name in packages_official.json packages_temp.json; do
    if [[ ! -f "${SEED_DIR}/${metadata_name}" ]]; then
      echo "dependency seed is missing ${metadata_name}" >&2
      exit 2
    fi
    cp "${SEED_DIR}/${metadata_name}" "${nimble_dir_owned}/"
  done
  cat >"${nimble_dir_owned}/nimbledata2.json" <<'EOF'
{
  "version": 1,
  "reverseDeps": {}
}
EOF

  nim_bin_arg="$(to_native_path "${NIM_BIN}")"
  nimble_dir_arg="$(to_native_path "${nimble_dir_owned}")"
  home_env="$(to_native_path "${home_owned}")"
  xdg_cache_env="$(to_native_path "${xdg_cache_owned}")"
  tmp_env="$(to_native_path "${tmp_owned}")"
  export HOME="${home_env}"
  export XDG_CACHE_HOME="${xdg_cache_env}"
  export TEMP="${tmp_env}"
  export TMP="${tmp_env}"
  export TMPDIR="${tmp_env}"
  export LANG=C
  export LC_ALL=C
  export SOURCE_DATE_EPOCH=0
  export TZ=UTC
  export ZERO_AR_DATE=1
  export PATH="$(dirname "${NIM_BIN}"):${PATH}"
  unset NIMBLE_DIR
  export ALOPEX_NIM_BIN="${nim_bin_arg}"
  if [[ "${RUNNER_IS_WINDOWS}" == "1" ]]; then
    export USERPROFILE="${home_env}"
    export MSYS2_ARG_CONV_EXCL='*'
  fi

  nim_version="$("${NIM_BIN}" --version | sed -n \
    's/.*Version \([0-9][0-9.]*\).*/\1/p' | head -n 1)"
  if [[ "${nim_version}" != "${REQUIRED_NIM_VERSION}" ]]; then
    echo "Nim ${REQUIRED_NIM_VERSION} is required, found ${nim_version:-unknown}" >&2
    exit 2
  fi

  nimble_identity="$("${NIMBLE_BIN}" --nimbleDir:"${nimble_dir_arg}" \
    --nim:"${nim_bin_arg}" --useSystemNim --offline --version)"
  nimble_version="$(sed -n 's/^nimble v\([0-9][0-9.]*\).*/\1/p' \
    <<<"${nimble_identity}" | head -n 1)"
  if [[ "${nimble_version}" != "${REQUIRED_NIMBLE_VERSION}" ]]; then
    echo "Nimble ${REQUIRED_NIMBLE_VERSION} is required, found ${nimble_version:-unknown}" >&2
    exit 2
  fi
  if ! grep -Fqx "git hash: ${REQUIRED_NIMBLE_SHA}" <<<"${nimble_identity}"; then
    echo "Nimble must be built from ${REQUIRED_NIMBLE_SHA}" >&2
    exit 2
  fi

  npeg_resolved="$("${NIMBLE_BIN}" --nimbleDir:"${nimble_dir_arg}" \
    --nim:"${nim_bin_arg}" --useSystemNim --offline path npeg)"
  msgpack_resolved="$("${NIMBLE_BIN}" --nimbleDir:"${nimble_dir_arg}" \
    --nim:"${nim_bin_arg}" --useSystemNim --offline path msgpack4nim)"
  npeg_resolved="$(to_posix_path "${npeg_resolved}")"
  msgpack_resolved="$(to_posix_path "${msgpack_resolved}")"
  if [[ "$(cd "${npeg_resolved}" && pwd -P)" != "$(cd "${npeg_owned}" && pwd -P)" ]]; then
    echo "Nimble resolved an unexpected npeg package: ${npeg_resolved}" >&2
    exit 2
  fi
  if [[ "$(cd "${msgpack_resolved}" && pwd -P)" != "$(cd "${msgpack_owned}" && pwd -P)" ]]; then
    echo "Nimble resolved an unexpected msgpack4nim package: ${msgpack_resolved}" >&2
    exit 2
  fi

  python_bin="${PYTHON_BIN}"
  manifest_tool_arg="$(to_native_path "${MANIFEST_TOOL}")"
  npeg_arg="$(to_native_path "${npeg_owned}")"
  msgpack_arg="$(to_native_path "${msgpack_owned}")"
  packages_official_arg="$(to_native_path "${nimble_dir_owned}/packages_official.json")"
  packages_temp_arg="$(to_native_path "${nimble_dir_owned}/packages_temp.json")"
  "${python_bin}" "${manifest_tool_arg}" verify-inputs \
    --package "npeg=1.3.0=${npeg_arg}" \
    --package "msgpack4nim=0.4.4=${msgpack_arg}" \
    --registry-metadata "packages_official.json=${packages_official_arg}" \
    --registry-metadata "packages_temp.json=${packages_temp_arg}"

  rm -f -- "${OUTPUT}" "${STATIC_OUTPUT}"
  (
    cd "${PARSER_DIR}"
    nimcache_arg="$(to_native_path "${build_root}/nimcache")"
    output_build_arg="$(to_native_path "${OUTPUT}")"
    npeg_source_arg="$(to_native_path "${npeg_resolved}")"
    msgpack_source_arg="$(to_native_path "${msgpack_resolved}")"
    static_output_build_arg="$(to_native_path "${STATIC_OUTPUT}")"
    static_flags=()
    # The static archive is linked into cdylib consumers (the Python
    # extension module), so its objects must be position independent with a
    # TLS model usable from a dlopen'd library. Without this the Linux
    # archive carries R_X86_64_TPOFF32 relocations and `maturin build` fails
    # with "relocation ... cannot be used when making a shared object". This
    # mirrors the `staticlib` nimble task, which the --backend docker path
    # uses; the host path must produce byte-comparable inputs (issue #179).
    static_cc_flags=(--passC:-fPIC)
    case "${HOST_TARGET}" in
      x86_64-unknown-linux-gnu)
        static_flags+=(--passL:-s)
        ;;
      x86_64-apple-darwin|aarch64-apple-darwin)
        static_flags+=(--passL:-Wl,-x)
        ;;
      x86_64-pc-windows-msvc)
        static_flags+=(
          --passL:-static
          --passL:-static-libgcc
          --passL:-s
          --passL:-Wl,--no-insert-timestamp
        )
        # MSVC has no -fPIC (all code is position independent there) and
        # rejects the flag, so the Windows branch replaces it outright.
        static_cc_flags=(--cc:vcc)
        ;;
    esac
    "${NIM_BIN}" c -d:release --app:lib --mm:orc --opt:speed \
      "${static_flags[@]}" \
      --nimcache:"${nimcache_arg}" \
      --path:"${npeg_source_arg}" \
      --path:"${msgpack_source_arg}" \
      -o:"${output_build_arg}" src/alopex_sql_parser.nim
    static_nimcache_arg="$(to_native_path "${build_root}/nimcache-static")"
    "${NIM_BIN}" c -d:release --app:staticlib --mm:orc --opt:speed \
      "${static_cc_flags[@]}" \
      --nimcache:"${static_nimcache_arg}" \
      --path:"${npeg_source_arg}" \
      --path:"${msgpack_source_arg}" \
      -o:"${static_output_build_arg}" src/alopex_sql_parser.nim
  )
  [[ -f "${OUTPUT}" ]] || {
    echo "Nim parser output not found: ${OUTPUT}" >&2
    exit 1
  }
  [[ -f "${STATIC_OUTPUT}" ]] || {
    echo "Nim parser static output not found: ${STATIC_OUTPUT}" >&2
    exit 1
  }

  if [[ -n "${ARCHIVE_DIR}" ]]; then
    archive_dir_arg="$(to_native_path "$(to_posix_path "${ARCHIVE_DIR}")")"
    parser_dir_arg="$(to_native_path "${PARSER_DIR}")"
    output_arg="$(to_native_path "${OUTPUT}")"
    nimble_bin_arg="$(to_native_path "${NIMBLE_BIN}")"
    "${python_bin}" "${manifest_tool_arg}" pack-target \
      --alopex-version "${REQUIRED_ALOPEX_VERSION}" \
      --contract-version "${REQUIRED_CONTRACT_VERSION}" \
      --target "${TARGET}" \
      --library "${output_arg}" \
      --static-library "$(to_native_path "${STATIC_OUTPUT}")" \
      --source-root "${parser_dir_arg}" \
      --nim-version "${REQUIRED_NIM_VERSION}" \
      --nim-binary "${nim_bin_arg}" \
      --nimble-version "${REQUIRED_NIMBLE_VERSION}" \
      --nimble-sha "${REQUIRED_NIMBLE_SHA}" \
      --nimble-binary "${nimble_bin_arg}" \
      --build-profile "${REQUIRED_BUILD_PROFILE}" \
      --package "npeg=1.3.0=${npeg_arg}" \
      --package "msgpack4nim=0.4.4=${msgpack_arg}" \
      --registry-metadata "packages_official.json=${packages_official_arg}" \
      --registry-metadata "packages_temp.json=${packages_temp_arg}" \
      --output-dir "${archive_dir_arg}"
  fi
  build_succeeded=1
)

build_docker() {
  local container_output
  local output_dir
  local output_name
  local static_output_name
  local -a output_mount=()
  local -a user_args=(--user "$(id -u):$(id -g)")
  command -v docker >/dev/null 2>&1 || {
    echo "docker is required for the Docker backend" >&2
    return 1
  }
  if command -v podman >/dev/null 2>&1 && [[ "$(command -v docker)" -ef "$(command -v podman)" ]]; then
    user_args=(--userns=keep-id --user "$(id -u):$(id -g)")
  fi
  output_dir="$(dirname "${OUTPUT}")"
  output_name="$(basename "${OUTPUT}")"
  static_output_name="$(basename "${DEFAULT_STATIC_OUTPUT}")"
  if [[ "${output_dir}" == "${PARSER_DIR}" ]]; then
    container_output="/workspace/${output_name}"
  else
    container_output="/output/${output_name}"
    output_mount=(-v "${output_dir}:/output")
  fi
  docker run --rm \
    --entrypoint /bin/bash \
    -v "${PARSER_DIR}:/workspace" \
    "${output_mount[@]}" \
    -w "/workspace" \
    -e HOME=/tmp \
    -e "ALOPEX_NIM_PARSER_OUTPUT=${container_output}" \
    -e "ALOPEX_NIM_PARSER_STATIC_OUTPUT=$(dirname "${container_output}")/${static_output_name}" \
    "${user_args[@]}" \
    "${NIM_IMAGE}" \
    -c 'export PATH=/opt/nim/bin:/usr/local/bin:/usr/bin:/bin; nimble install -y "npeg@1.3.0" "msgpack4nim@0.4.4" && nimble lib && nimble staticlib'
}

validate_docker_target() {
  if [[ "${HOST_TARGET}" != "x86_64-unknown-linux-gnu" || \
        ( -n "${TARGET}" && "${TARGET}" != "x86_64-unknown-linux-gnu" ) ]]; then
    echo "Docker backend only supports a native x86_64 Linux parser build" >&2
    exit 2
  fi
}

# Fail closed when a Linux static archive is not position independent.
#
# The archive is linked into the Python extension module, a cdylib. A non-PIC
# build carries R_X86_64_TPOFF32 TLS relocations that the linker refuses in a
# shared object, and the failure only surfaces in the wheel job — long after
# the release assets were staged. Both backends run this check, so the host
# and nimble/docker paths cannot silently diverge again (issue #179).
assert_static_archive_is_pic() {
  local effective_target
  effective_target="${TARGET:-${HOST_TARGET}}"
  case "${effective_target}" in
    *-linux-gnu|*-linux-musl) ;;
    *) return 0 ;;
  esac
  command -v readelf >/dev/null 2>&1 || {
    echo "readelf not found; skipping the position-independence check for ${STATIC_OUTPUT}" >&2
    return 0
  }
  if readelf --relocs -- "${STATIC_OUTPUT}" 2>/dev/null | grep -q "R_X86_64_TPOFF32"; then
    echo "static parser archive is not position independent: ${STATIC_OUTPUT}" >&2
    echo "it carries R_X86_64_TPOFF32 relocations and cannot link into a cdylib" >&2
    echo "build it with --passC:-fPIC" >&2
    exit 1
  fi
}

write_identity_sidecars() (
  local output_dir
  local output_name
  local output_sha
  local static_output_name
  local static_output_sha
  local contract_tmp
  local checksum_tmp
  output_dir="$(dirname "${OUTPUT}")"
  output_name="$(basename "${OUTPUT}")"
  static_output_name="$(basename "${STATIC_OUTPUT}")"
  contract_tmp="${output_dir}/.CONTRACT_VERSION.$$"
  checksum_tmp="${output_dir}/.SHA256SUMS.$$"
  trap 'rm -f -- "${contract_tmp}" "${checksum_tmp}"' EXIT

  if command -v sha256sum >/dev/null 2>&1; then
    output_sha="$(sha256sum "${OUTPUT}")"
    output_sha="${output_sha%% *}"
    static_output_sha="$(sha256sum "${STATIC_OUTPUT}")"
    static_output_sha="${static_output_sha%% *}"
  elif command -v shasum >/dev/null 2>&1; then
    output_sha="$(shasum -a 256 "${OUTPUT}")"
    output_sha="${output_sha%% *}"
    static_output_sha="$(shasum -a 256 "${STATIC_OUTPUT}")"
    static_output_sha="${static_output_sha%% *}"
  elif command -v openssl >/dev/null 2>&1; then
    output_sha="$(openssl dgst -sha256 "${OUTPUT}")"
    output_sha="${output_sha##*= }"
    static_output_sha="$(openssl dgst -sha256 "${STATIC_OUTPUT}")"
    static_output_sha="${static_output_sha##*= }"
  else
    echo "sha256sum, shasum, or openssl is required to identify the parser output" >&2
    exit 1
  fi
  [[ "${output_sha}" =~ ^[0-9a-f]{64}$ ]] || {
    echo "could not identify Nim parser output: ${OUTPUT}" >&2
    exit 1
  }
  [[ "${static_output_sha}" =~ ^[0-9a-f]{64}$ ]] || {
    echo "could not identify Nim parser static output: ${STATIC_OUTPUT}" >&2
    exit 1
  }

  printf '%s\n' "${REQUIRED_CONTRACT_VERSION}" >"${contract_tmp}"
  printf '%s  %s\n' "${output_sha}" "${output_name}" >"${checksum_tmp}"
  printf '%s  %s\n' "${static_output_sha}" "${static_output_name}" >>"${checksum_tmp}"
  chmod 0644 "${contract_tmp}" "${checksum_tmp}"
  mv -f -- "${contract_tmp}" "${output_dir}/CONTRACT_VERSION"
  mv -f -- "${checksum_tmp}" "${output_dir}/SHA256SUMS"
)

echo "Building Nim SQL parser (${BACKEND} backend)"
validate_output_path
if [[ "${BACKEND}" == "docker" ]]; then
  validate_docker_target
fi
rm -f -- "${OUTPUT}" "${STATIC_OUTPUT}" "$(dirname "${OUTPUT}")/CONTRACT_VERSION" \
  "$(dirname "${OUTPUT}")/SHA256SUMS"
if [[ "${BACKEND}" == "host" ]]; then
  build_host
else
  build_docker
fi

[[ -f "${OUTPUT}" ]] || { echo "Nim parser output not found: ${OUTPUT}" >&2; exit 1; }
[[ -f "${STATIC_OUTPUT}" ]] || { echo "Nim parser static output not found: ${STATIC_OUTPUT}" >&2; exit 1; }
assert_static_archive_is_pic
write_identity_sidecars
echo "Built ${OUTPUT}"
echo "Built ${STATIC_OUTPUT}"
