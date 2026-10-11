#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
requirements="${ALOPEX_PERFORMANCE_REQUIREMENTS:-${repo_root}/scripts/performance/requirements-v0811.txt}"
venv="${ALOPEX_PERFORMANCE_VENV:-${repo_root}/.venv-performance}"
python="${PYTHON:-python3.11}"

if [[ $# -eq 0 ]]; then
    echo "usage: scripts/run-performance-python.sh <command> [args...]" >&2
    exit 2
fi

if ! command -v "${python}" >/dev/null 2>&1; then
    echo "Python 3.11 is required; set PYTHON to a Python 3.11 executable" >&2
    exit 2
fi

if ! "${python}" -c 'import sys; raise SystemExit(sys.version_info[:2] != (3, 11))'; then
    echo "performance validation requires Python 3.11" >&2
    exit 2
fi

if [[ ! -x "${venv}/bin/python" ]] || ! "${venv}/bin/python" -c 'import sys; raise SystemExit(sys.version_info[:2] != (3, 11))'; then
    "${python}" -m venv --clear "${venv}"
fi

requirements_digest="$("${venv}/bin/python" -c \
    'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' \
    "${requirements}")"
stamp="${venv}/.alopex-performance-requirements.sha256"
if [[ ! -f "${stamp}" ]] || [[ "$(<"${stamp}")" != "${requirements_digest}" ]]; then
    "${venv}/bin/python" -m pip install --disable-pip-version-check -r "${requirements}"
    printf '%s\n' "${requirements_digest}" >"${stamp}"
fi

export VIRTUAL_ENV="${venv}"
export PATH="${venv}/bin:${PATH}"
exec "$@"
