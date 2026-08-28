#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3.11}"
venv_dir="${LANE_CI_VENV:-${project_root}/.venv-lane-ci}"
venv_python="${venv_dir}/bin/python"
parser_dir="${NIM_SQL_PARSER_LIB_DIR:-${project_root}/crates/alopex-sql/nim-sql-parser}"
parser_backend="${NIM_PARSER_BACKEND:-docker}"

case "$(uname -s)" in
    Darwin*) parser_shared="${parser_dir}/libalopex_sql_parser.dylib" ;;
    *) parser_shared="${parser_dir}/libalopex_sql_parser.so" ;;
esac
parser_static="${parser_dir}/libalopex_sql_parser.a"

if [[ ! -x "${venv_python}" ]]; then
    "${python}" -m venv "${venv_dir}"
fi

if ! "${venv_python}" -c 'import numpy; assert int(numpy.__version__.split(".", 1)[0]) < 2' >/dev/null 2>&1; then
    "${venv_python}" -m pip install "numpy<2"
fi

mapfile -t python_paths < <("${venv_python}" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or ""); print(sysconfig.get_paths()["purelib"])')
python_libdir="${python_paths[0]}"
python_site="${python_paths[1]}"

export PYO3_PYTHON="${venv_python}"
export PYTHON_SYS_EXECUTABLE="${venv_python}"
export PYTHONPATH="${python_site}${PYTHONPATH:+:${PYTHONPATH}}"
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
if [[ ! -f "${parser_shared}" || ! -f "${parser_static}" || ! -f "${parser_dir}/CONTRACT_VERSION" || ! -f "${parser_dir}/SHA256SUMS" ]] ||
    find "${parser_dir}/src" "${parser_dir}/nim_sql_parser.nimble" "${parser_dir}/PARSER_CONTRACT_VERSION" -type f -newer "${parser_shared}" -print -quit | grep -q .; then
    bash scripts/build-nim-parser.sh --backend "${parser_backend}"
fi

if [[ $# -gt 0 ]]; then
    exec cargo test --features lane_ci "$@"
fi
exec cargo test --workspace --features lane_ci
