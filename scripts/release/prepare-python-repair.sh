#!/usr/bin/env bash
# Create or verify an immutable Python tag for a historical repair release.
# Required environment: SOURCE_SHA, TARGET_SHA, RELEASE_TAG, GH_TOKEN.

set -euo pipefail

: "${SOURCE_SHA:?SOURCE_SHA is required}"
: "${TARGET_SHA:?TARGET_SHA is required}"
: "${RELEASE_TAG:?RELEASE_TAG is required}"
: "${GH_TOKEN:?GH_TOKEN is required}"

[[ "${SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]]
[[ "${TARGET_SHA}" =~ ^[0-9a-f]{40}$ ]]
[[ "${SOURCE_SHA}" == "${TARGET_SHA}" ]]
[[ "${RELEASE_TAG}" =~ ^alopex-py-v[0-9]+\.[0-9]+\.[0-9]+$ ]]

version="${RELEASE_TAG#alopex-py-v}"
core_tag="v${version}"

git fetch --force origin main:refs/remotes/origin/main
git cat-file -e "${SOURCE_SHA}^{commit}"
git merge-base --is-ancestor "${SOURCE_SHA}" origin/main
git fetch --force origin "refs/tags/${core_tag}:refs/tags/${core_tag}"
[[ "$(git rev-parse "${core_tag}^{commit}")" == "${SOURCE_SHA}" ]]

release_json="$(gh release view "${core_tag}" --json tagName,isDraft,isPrerelease)"
jq -e --arg tag "${core_tag}" \
  'select(.tagName == $tag and .isDraft == false and .isPrerelease == false)' \
  <<< "${release_json}" >/dev/null

if git ls-remote --exit-code --tags origin "refs/tags/${RELEASE_TAG}" >/dev/null 2>&1; then
  git fetch --force origin "refs/tags/${RELEASE_TAG}:refs/tags/${RELEASE_TAG}"
  [[ "$(git cat-file -t "refs/tags/${RELEASE_TAG}")" == "tag" ]]
  [[ "$(git rev-parse "${RELEASE_TAG}^{commit}")" == "${SOURCE_SHA}" ]]
else
  git config user.name "github-actions[bot]"
  git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
  git tag -a "${RELEASE_TAG}" "${SOURCE_SHA}" -m "Release ${RELEASE_TAG}"
  git push origin "${RELEASE_TAG}"
fi
