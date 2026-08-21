#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
BASE_REF="origin/main"
BASE_SHA="$(git -C "${ROOT}" rev-parse "${BASE_REF}")"
RUNTIME_DIR="/tmp/alopex-issue-196-runtime"
CACHE_DIR="/tmp/alopex-issue-196-cache"
SOURCE_DIR="${RUNTIME_DIR}/source"
SOCKET_PATH="${RUNTIME_DIR}/podman.sock"
SERVICE_LOG="${RUNTIME_DIR}/podman-service.log"
SOCKET_START_ATTEMPTS=600
RESOURCE_LABEL="alopex.issue196.experiment=true"
RESOURCE_PREFIX="act-Issue-196-local-responsibility-experiment-"
ACT_VERSION="0.2.88"
ACT_ASSET_ID="409583042"
ACT_SHA256="1eb9996682dfcc053ac8f3f90f2ec50376f0cdfc229712d82da03d673c63a2b3"
ACT_ARCHIVE="${CACHE_DIR}/act-v${ACT_VERSION}-Linux-x86_64.tar.gz"
ACT_ARCHIVE_PART="${ACT_ARCHIVE}.partial"
ACT_BINARY="${RUNTIME_DIR}/act"
RUNNER_IMAGE="${ISSUE196_RUNNER_IMAGE:-docker.io/library/rust:1.96-bookworm}"
CARGO_SEED_DIR="${ISSUE196_CARGO_SEED_DIR:-}"

if [[ -e "${RUNTIME_DIR}" ]]; then
    echo "experiment runtime already exists: ${RUNTIME_DIR}" >&2
    exit 2
fi
if [[ -z "${CARGO_SEED_DIR}" || ! -d "${CARGO_SEED_DIR}/registry" ]]; then
    echo "set ISSUE196_CARGO_SEED_DIR to a readable Cargo home with registry cache" >&2
    exit 2
fi
mkdir -p "${RUNTIME_DIR}"
mkdir -p "${CACHE_DIR}"
mkdir -p "${RUNTIME_DIR}/cargo-home/registry/cache"
mkdir -p "${RUNTIME_DIR}/cargo-home/registry/index"
# Archives are copied into task-owned state so integrity-checked missing
# archives can be added without mutating the seed. Index data stays read-only.
cp -a "${CARGO_SEED_DIR}/registry/cache/." \
    "${RUNTIME_DIR}/cargo-home/registry/cache/"

service_pid=""
had_act_toolcache=0
if podman volume exists act-toolcache; then
    had_act_toolcache=1
fi
cleanup() {
    if [[ -n "${service_pid}" ]]; then
        kill "${service_pid}" 2>/dev/null || true
        wait "${service_pid}" 2>/dev/null || true
    fi
    rm -f -- "${SOCKET_PATH}"
    rm -f -- "${ACT_ARCHIVE_PART}"
    rmdir "${CACHE_DIR}" 2>/dev/null || true
    while IFS= read -r container_id; do
        [[ -z "${container_id}" ]] && continue
        podman rm --force --time 1 "${container_id}" >/dev/null 2>&1 || true
    done < <(podman ps --all --quiet --filter "label=${RESOURCE_LABEL}")
    while IFS= read -r volume_name; do
        [[ -z "${volume_name}" ]] && continue
        podman volume rm "${volume_name}" >/dev/null 2>&1 || true
    done < <(podman volume ls --quiet --filter "name=${RESOURCE_PREFIX}")
    if [[ "${had_act_toolcache}" == "0" ]]; then
        podman volume rm act-toolcache >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

python3 "${ROOT}/scripts/issue196_experiment/verify_isolation.py" \
    --base-ref "${BASE_REF}"
podman image exists "${RUNNER_IMAGE}" || {
    echo "missing experiment runner image: ${RUNNER_IMAGE}" >&2
    exit 3
}
if [[ ! -f "${ACT_ARCHIVE}" ]]; then
    curl --fail --location --silent --show-error \
        --header "Accept: application/octet-stream" \
        "https://api.github.com/repos/nektos/act/releases/assets/${ACT_ASSET_ID}" \
        --output "${ACT_ARCHIVE_PART}"
    echo "${ACT_SHA256}  ${ACT_ARCHIVE_PART}" | sha256sum --check --status
    mv "${ACT_ARCHIVE_PART}" "${ACT_ARCHIVE}"
fi
echo "${ACT_SHA256}  ${ACT_ARCHIVE}" | sha256sum --check --status
tar -xzf "${ACT_ARCHIVE}" -C "${RUNTIME_DIR}" act
chmod 0555 "${ACT_BINARY}"

mkdir -p "${SOURCE_DIR}"
git -C "${ROOT}" archive --format=tar "${BASE_REF}" | tar -x -C "${SOURCE_DIR}"
tar -C "${ROOT}" -cf - \
    .github/workflows/issue-196-local-experiment.yml \
    docs/issue-196-local-experiment.md \
    scripts/issue196_experiment | tar -x -C "${SOURCE_DIR}"
git -C "${SOURCE_DIR}" init --quiet --initial-branch=issue-196-experiment
git -C "${SOURCE_DIR}" add --all
git -C "${SOURCE_DIR}" \
    -c user.name="Issue 196 local experiment" \
    -c user.email="issue-196-local@example.invalid" \
    -c commit.gpgsign=false \
    commit --quiet -m "test: materialize isolated issue 196 snapshot"

podman run --rm \
    --pull=never \
    --label "${RESOURCE_LABEL}" \
    --volume "${SOURCE_DIR}:${SOURCE_DIR}:ro" \
    --volume "${RUNTIME_DIR}:/experiment" \
    --workdir "${SOURCE_DIR}" \
    "${RUNNER_IMAGE}" python3 scripts/issue196_experiment/hydrate_cargo_seed.py \
    --lockfile Cargo.lock \
    --cache-dir /experiment/cargo-home/registry/cache

# Resolve the measured dependency closure before act starts. Public registry
# archives and index data are read-only submounts; extracted sources and Cargo
# bookkeeping remain below the task-owned runtime.
podman run --rm \
    --pull=never \
    --label "${RESOURCE_LABEL}" \
    --env CARGO_HOME=/experiment/cargo-home \
    --env CARGO_NET_OFFLINE=true \
    --volume "${SOURCE_DIR}:${SOURCE_DIR}:ro" \
    --volume "${RUNTIME_DIR}:/experiment" \
    --volume "${CARGO_SEED_DIR}/registry/index:/experiment/cargo-home/registry/index:ro" \
    --workdir "${SOURCE_DIR}" \
    "${RUNNER_IMAGE}" python3 scripts/issue196_experiment/verify_cargo_seed.py \
    --root . \
    --package alopex-dataframe \
    --features lane_ci \
    --cargo-home /experiment/cargo-home

podman system service --time=0 "unix://${SOCKET_PATH}" >"${SERVICE_LOG}" 2>&1 &
service_pid=$!
for _ in $(seq 1 "${SOCKET_START_ATTEMPTS}"); do
    [[ -S "${SOCKET_PATH}" ]] && break
    if ! kill -0 "${service_pid}" 2>/dev/null; then
        echo "experiment Podman API service exited before creating its socket" >&2
        exit 4
    fi
    sleep 0.1
done
[[ -S "${SOCKET_PATH}" ]] || {
    echo "experiment Podman API socket did not start" >&2
    exit 4
}

podman run --rm \
    --pull=never \
    --name alopex-issue-196-act-controller \
    --label "${RESOURCE_LABEL}" \
    --entrypoint /opt/issue196/act \
    --env DOCKER_HOST=unix:///var/run/podman.sock \
    --env ISSUE196_BASE_SHA="${BASE_SHA}" \
    --volume "${SOCKET_PATH}:/var/run/podman.sock" \
    --volume "${ACT_BINARY}:/opt/issue196/act:ro" \
    --volume "${SOURCE_DIR}:${SOURCE_DIR}:ro" \
    --workdir "${SOURCE_DIR}" \
    "${RUNNER_IMAGE}" workflow_dispatch \
    -W .github/workflows/issue-196-local-experiment.yml \
    --env ISSUE196_BASE_SHA="${BASE_SHA}" \
    --bind \
    --pull=false \
    --action-offline-mode \
    --container-daemon-socket - \
    --platform "issue-196-rust=${RUNNER_IMAGE}" \
    --container-options \
    "--volume ${RUNTIME_DIR}:/experiment --volume ${CARGO_SEED_DIR}/registry/index:/experiment/cargo-home/registry/index:ro --label ${RESOURCE_LABEL}"

python3 "${ROOT}/scripts/issue196_experiment/verify_isolation.py" \
    --base-ref "${BASE_REF}"
test -f "${RUNTIME_DIR}/summary.json"
python3 -m json.tool "${RUNTIME_DIR}/summary.json"
