#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
fixture="$(mktemp -d)"
trap 'rm -rf "${fixture}"' EXIT

git -C "${fixture}" init --quiet
git -C "${fixture}" config user.email 'ci@example.invalid'
git -C "${fixture}" config user.name 'CI test'
mkdir -p "${fixture}/scripts/ci" "${fixture}/scripts/release" "${fixture}/.github/workflows" "${fixture}/.github/actions/cargo-deny"
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

printf 'release\n' >"${fixture}/scripts/release/process.sh"
git -C "${fixture}" add scripts/release/process.sh
git -C "${fixture}" commit --quiet -m release-process-change
release_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${base_sha}" "${release_sha}")" == $'production=false\nsecurity=false' ]]

printf 'name: release\n' >"${fixture}/.github/workflows/release.yml"
git -C "${fixture}" add .github/workflows/release.yml
git -C "${fixture}" commit --quiet -m other-workflow-change
workflow_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${release_sha}" "${workflow_sha}")" == $'production=false\nsecurity=true' ]]

printf 'name: ci\n' >"${fixture}/.github/workflows/ci.yml"
git -C "${fixture}" add .github/workflows/ci.yml
git -C "${fixture}" commit --quiet -m security-workflow-change
security_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${workflow_sha}" "${security_sha}")" == $'production=false\nsecurity=true' ]]

printf 'name: deny\n' >"${fixture}/.github/actions/cargo-deny/action.yml"
git -C "${fixture}" add .github/actions/cargo-deny/action.yml
git -C "${fixture}" commit --quiet -m audit-action-change
action_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${security_sha}" "${action_sha}")" == $'production=true\nsecurity=true' ]]

printf 'config\n' >"${fixture}/deny.toml"
git -C "${fixture}" add deny.toml
git -C "${fixture}" commit --quiet -m audit-policy-change
policy_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${action_sha}" "${policy_sha}")" == $'production=true\nsecurity=true' ]]

printf 'lock\n' >"${fixture}/Cargo.lock"
git -C "${fixture}" add Cargo.lock
git -C "${fixture}" commit --quiet -m lockfile-change
lock_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${policy_sha}" "${lock_sha}")" == $'production=true\nsecurity=true' ]]

printf '# classifier change\n' >>"${fixture}/scripts/ci/classify-change-scope.sh"
git -C "${fixture}" add scripts/ci/classify-change-scope.sh
git -C "${fixture}" commit --quiet -m ci-script-change
ci_script_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${lock_sha}" "${ci_script_sha}")" == $'production=true\nsecurity=true' ]]

printf 'production\n' >"${fixture}/README.md"
git -C "${fixture}" add README.md
git -C "${fixture}" commit --quiet -m production-change
production_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${ci_script_sha}" "${production_sha}")" == $'production=true\nsecurity=true' ]]
[[ "$(run_classifier 0000000000000000000000000000000000000000 "${production_sha}")" == $'production=true\nsecurity=true' ]]
[[ "$(run_classifier "${production_sha}" "${production_sha}")" == $'production=true\nsecurity=true' ]]

# A move from production into an excluded process path must still run production jobs.
mkdir -p "${fixture}/scripts/release"
git -C "${fixture}" mv README.md scripts/release/README.md
git -C "${fixture}" commit --quiet -m production-to-release-rename
rename_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${production_sha}" "${rename_sha}")" == $'production=true\nsecurity=true' ]]

# A workflow renamed into an excluded release path must still trigger the audit.
git -C "${fixture}" mv .github/workflows/release.yml scripts/release/release.yml
git -C "${fixture}" commit --quiet -m workflow-to-release-rename
workflow_rename_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${rename_sha}" "${workflow_rename_sha}")" == $'production=false\nsecurity=true' ]]
