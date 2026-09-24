#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
fake_bin="${scratch}/bin"
calls="${scratch}/docker-calls.log"
mkdir -p "${fake_bin}"

cat >"${fake_bin}/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "run" ]]; then
    exit 0
fi

user=""
target_mode=""
shift
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)
            user="$2"
            shift 2
            ;;
        -v)
            mount="$2"
            if [[ "${mount}" == *:/tools-target ]]; then
                target_mode="$(stat -c %a "${mount%:/tools-target}")"
            fi
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
printf 'user=%s target_mode=%s\n' "${user}" "${target_mode}" >>"${FAKE_DOCKER_CALLS}"
EOF
chmod +x "${fake_bin}/docker"

PATH="${fake_bin}:${PATH}" FAKE_DOCKER_CALLS="${calls}" \
    bash "${repo}/scripts/release/verify-release/run.sh" 0.8.13 \
        --no-report --report-dir "${scratch}/report"

root_calls="$(grep -Fc 'user=0:0 target_mode=755' "${calls}" || true)"
if [[ "${root_calls}" -ne 1 ]]; then
    echo "the helper build must use the rootless-compatible root mapping once" >&2
    cat "${calls}" >&2
    exit 1
fi

caller="$(id -u):$(id -g)"
non_root_calls="$(grep -Fc "user=${caller} target_mode=755" "${calls}" || true)"
if [[ "${non_root_calls}" -lt 1 ]]; then
    echo "public scenarios must keep the non-root execution boundary" >&2
    cat "${calls}" >&2
    exit 1
fi

echo "rootless helper mount wiring: ok"
