#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
fixture="$(mktemp -d)"
trap 'rm -rf "${fixture}"' EXIT

git -C "${fixture}" init --quiet
git -C "${fixture}" config user.email 'ci@example.invalid'
git -C "${fixture}" config user.name 'CI test'
mkdir -p "${fixture}/scripts/ci" "${fixture}/.github/workflows"
cp "${repo_root}/scripts/ci/classify-change-scope.sh" "${fixture}/scripts/ci/"
printf 'baseline\n' >"${fixture}/README.md"
git -C "${fixture}" add .
git -C "${fixture}" commit --quiet -m baseline
base_sha="$(git -C "${fixture}" rev-parse HEAD)"

run_classifier() {
  (
    cd "${fixture}"
    bash scripts/ci/classify-change-scope.sh "$1" "$2"
  )
}

printf 'name: release\n' >"${fixture}/.github/workflows/release.yml"
git -C "${fixture}" add .github/workflows/release.yml
git -C "${fixture}" commit --quiet -m release-process-change
release_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${base_sha}" "${release_sha}")" == 'production=false' ]]

printf 'production\n' >"${fixture}/README.md"
git -C "${fixture}" add README.md
git -C "${fixture}" commit --quiet -m production-change
production_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${release_sha}" "${production_sha}")" == 'production=true' ]]
[[ "$(run_classifier 0000000000000000000000000000000000000000 "${production_sha}")" == 'production=true' ]]
[[ "$(run_classifier "${production_sha}" "${production_sha}")" == 'production=true' ]]
