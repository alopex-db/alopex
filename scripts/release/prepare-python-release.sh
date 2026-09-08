#!/usr/bin/env bash
# Create the immutable Python release tag from a reviewed main commit.
# Required environment: CORE_RUN_ID, GH_TOKEN.

set -euo pipefail

: "${CORE_RUN_ID:?CORE_RUN_ID is required}"
: "${GH_TOKEN:?GH_TOKEN is required}"

version="$(sed -n 's/^version = "\([0-9][0-9.]*\)"/\1/p' Cargo.toml | head -1)"
[[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
core_tag="v${version}"
python_tag="alopex-py-v${version}"
candidate_sha="$(git rev-parse HEAD)"

git fetch --force origin main "refs/tags/${core_tag}:refs/tags/${core_tag}"
git merge-base --is-ancestor "${candidate_sha}" origin/main
core_sha="$(git rev-parse "${core_tag}^{commit}")"
git merge-base --is-ancestor "${core_sha}" HEAD

core_release="$(gh release view "${core_tag}" --json tagName,isDraft,isPrerelease)"
jq -e --arg tag "${core_tag}" \
  'select(.tagName == $tag and .isDraft == false and .isPrerelease == false)' \
  <<< "${core_release}" >/dev/null
gh run list --workflow ci.yml --commit "${candidate_sha}" \
  --json status,conclusion --limit 20 \
  --jq 'any(.[]; .status == "completed" and .conclusion == "success")' | grep -qx true
[[ "${CORE_RUN_ID}" =~ ^[1-9][0-9]*$ ]]
git ls-remote --exit-code --tags origin "refs/tags/${python_tag}" >/dev/null 2>&1 && {
  echo "Python release tag already exists: ${python_tag}" >&2
  exit 1
}

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git tag -a "${python_tag}" "${candidate_sha}" -m "Release ${python_tag}"
git push origin "${python_tag}"
printf 'PYTHON_TAG=%s\nPYTHON_TAG_SHA=%s\n' "${python_tag}" "${candidate_sha}" >> "${GITHUB_OUTPUT:-/dev/stdout}"
