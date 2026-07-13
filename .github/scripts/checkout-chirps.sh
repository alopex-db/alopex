#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${GITHUB_WORKSPACE:-}" ]]; then
    echo "GITHUB_WORKSPACE is not set" >&2
    exit 1
fi

workspace_parent="$(dirname "$(dirname "${GITHUB_WORKSPACE}")")"
chirps_dir="${workspace_parent}/chirps"

if [[ -d "${chirps_dir}/.git" ]]; then
    git -C "${chirps_dir}" fetch --depth 1 origin main
    git -C "${chirps_dir}" checkout FETCH_HEAD
    exit 0
fi

rm -rf "${chirps_dir}"
git clone --depth 1 https://github.com/alopex-db/alopex-chirps.git "${chirps_dir}"
