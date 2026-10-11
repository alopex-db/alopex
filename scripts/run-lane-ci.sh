#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3.11}"
venv_dir="${LANE_CI_VENV:-${project_root}/.venv-lane-ci}"
venv_python="${venv_dir}/bin/python"
parser_dir="${NIM_SQL_PARSER_LIB_DIR:-${project_root}/crates/alopex-sql/nim-sql-parser}"
parser_backend="${NIM_PARSER_BACKEND:-docker}"

python_without_numpy=0
if [[ "${1:-}" == "--python-test-no-numpy" ]]; then
    python_without_numpy=1
    if [[ -z "${LANE_CI_VENV:-}" ]]; then
        venv_dir="${project_root}/.venv-lane-ci-no-numpy"
        venv_python="${venv_dir}/bin/python"
    fi
fi

if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
    first_worktree=""
    while IFS= read -r line; do
        if [[ "${line}" == "worktree "* ]]; then
            first_worktree="${line#worktree }"
            break
        fi
    done < <(git -C "${project_root}" worktree list --porcelain)
    if [[ -z "${first_worktree}" ]]; then
        echo "error: unable to determine primary worktree for CARGO_TARGET_DIR" >&2
        exit 2
    fi
    export CARGO_TARGET_DIR="${first_worktree}/target"
fi

lib_test_mode=0
if [[ "${1:-}" == "--lib-test" ]]; then
    lib_test_mode=1
fi

case "$(uname -s)" in
    Darwin*) parser_shared="${parser_dir}/libalopex_sql_parser.dylib" ;;
    *) parser_shared="${parser_dir}/libalopex_sql_parser.so" ;;
esac
parser_static="${parser_dir}/libalopex_sql_parser.a"

python_libdir=""
if [[ "${lib_test_mode}" -eq 0 ]]; then
    if [[ ! -x "${venv_python}" ]]; then
        "${python}" -m venv "${venv_dir}"
    fi

    export VIRTUAL_ENV="${venv_dir}"
    export PATH="${venv_dir}/bin:${PATH}"

    if [[ "${python_without_numpy}" -eq 1 ]] &&
        "${venv_python}" -c 'import numpy' >/dev/null 2>&1; then
        echo "error: numpy is installed in ${venv_python}, but --python-test-no-numpy requires it to be absent" >&2
        exit 2
    elif [[ "${python_without_numpy}" -eq 0 ]] &&
        ! "${venv_python}" -c 'import numpy; assert int(numpy.__version__.split(".", 1)[0]) < 2' >/dev/null 2>&1; then
        "${venv_python}" -m pip install "numpy<2"
    fi

    python_libdir="$("${venv_python}" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
    python_site="$("${venv_python}" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"

    export PYO3_PYTHON="${venv_python}"
    export PYTHON_SYS_EXECUTABLE="${venv_python}"
    export PYTHONPATH="${python_site}${PYTHONPATH:+:${PYTHONPATH}}"
fi
export ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD=1
export NIM_SQL_PARSER_LIB_DIR="${parser_dir}"

case "$(uname -s)" in
    Linux*)
        runtime_library_path="${parser_dir}"
        if [[ -n "${python_libdir}" ]]; then
            runtime_library_path="${python_libdir}:${runtime_library_path}"
        fi
        export LD_LIBRARY_PATH="${runtime_library_path}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
        ;;
    Darwin*)
        runtime_library_path="${parser_dir}"
        if [[ -n "${python_libdir}" ]]; then
            runtime_library_path="${python_libdir}:${runtime_library_path}"
        fi
        export DYLD_FALLBACK_LIBRARY_PATH="${runtime_library_path}${DYLD_FALLBACK_LIBRARY_PATH:+:${DYLD_FALLBACK_LIBRARY_PATH}}"
        ;;
esac

cd "${project_root}"
parser_needs_build=0
if [[ ! -f "${parser_shared}" || ! -f "${parser_static}" || ! -f "${parser_dir}/CONTRACT_VERSION" || ! -f "${parser_dir}/SHA256SUMS" ]]; then
    parser_needs_build=1
elif [[ -d "${parser_dir}/src" && -f "${parser_dir}/nim_sql_parser.nimble" && -f "${parser_dir}/PARSER_CONTRACT_VERSION" ]] &&
    [[ -n "$(find "${parser_dir}/src" "${parser_dir}/nim_sql_parser.nimble" "${parser_dir}/PARSER_CONTRACT_VERSION" -type f -newer "${parser_shared}" -print -quit)" ]]; then
    parser_needs_build=1
fi
if [[ "${parser_needs_build}" -eq 1 ]]; then
    bash scripts/build-nim-parser.sh --backend "${parser_backend}"
fi

case "${1:-}" in
    --lib-test)
        shift
        if [[ "$#" -lt 2 ]]; then
            echo "usage: --lib-test <package> <exact-filter> [args...]" >&2
            exit 2
        fi
        package="$1"
        filter="$2"
        shift 2
        exec cargo test -p "${package}" --lib "${filter}" -- --exact "$@"
        ;;
    --python-test | --python-test-no-numpy)
        shift
        if [[ ! -x "${venv_dir}/bin/maturin" ]] ||
            ! "${venv_python}" -c 'import pytest' >/dev/null 2>&1; then
            "${venv_python}" -m pip install 'maturin>=1.4,<2.0' 'pytest>=7'
        fi
        "${venv_dir}/bin/maturin" develop -m "${project_root}/crates/alopex-py/Cargo.toml"
        exec "${venv_python}" -m pytest "$@"
        ;;
esac

if [[ "${1:-}" == "--" ]]; then
    shift
    exec "$@"
fi
if [[ $# -gt 0 ]]; then
    exec cargo test --features lane_ci "$@"
fi
exec cargo test --workspace --features lane_ci
