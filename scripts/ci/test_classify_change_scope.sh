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
[[ "$(run_classifier "${base_sha}" "${release_sha}")" == $'production=false\nsecurity=false\nhnsw_contract=false' ]]

printf 'name: release\n' >"${fixture}/.github/workflows/release.yml"
git -C "${fixture}" add .github/workflows/release.yml
git -C "${fixture}" commit --quiet -m other-workflow-change
workflow_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${release_sha}" "${workflow_sha}")" == $'production=false\nsecurity=true\nhnsw_contract=false' ]]

printf 'name: ci\n' >"${fixture}/.github/workflows/ci.yml"
git -C "${fixture}" add .github/workflows/ci.yml
git -C "${fixture}" commit --quiet -m security-workflow-change
security_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${workflow_sha}" "${security_sha}")" == $'production=false\nsecurity=true\nhnsw_contract=false' ]]

printf 'name: deny\n' >"${fixture}/.github/actions/cargo-deny/action.yml"
git -C "${fixture}" add .github/actions/cargo-deny/action.yml
git -C "${fixture}" commit --quiet -m audit-action-change
action_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${security_sha}" "${action_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=false' ]]

printf 'config\n' >"${fixture}/deny.toml"
git -C "${fixture}" add deny.toml
git -C "${fixture}" commit --quiet -m audit-policy-change
policy_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${action_sha}" "${policy_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=false' ]]

printf 'lock\n' >"${fixture}/Cargo.lock"
git -C "${fixture}" add Cargo.lock
git -C "${fixture}" commit --quiet -m lockfile-change
lock_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${policy_sha}" "${lock_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=false' ]]

printf '# classifier change\n' >>"${fixture}/scripts/ci/classify-change-scope.sh"
git -C "${fixture}" add scripts/ci/classify-change-scope.sh
git -C "${fixture}" commit --quiet -m ci-script-change
ci_script_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${lock_sha}" "${ci_script_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=false' ]]

printf 'production\n' >"${fixture}/README.md"
git -C "${fixture}" add README.md
git -C "${fixture}" commit --quiet -m production-change
production_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${ci_script_sha}" "${production_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=false' ]]
[[ "$(run_classifier 0000000000000000000000000000000000000000 "${production_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=true' ]]
[[ "$(run_classifier "${production_sha}" "${production_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=true' ]]

# A move from production into an excluded process path must still run production jobs.
mkdir -p "${fixture}/scripts/release"
git -C "${fixture}" mv README.md scripts/release/README.md
git -C "${fixture}" commit --quiet -m production-to-release-rename
rename_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${production_sha}" "${rename_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=false' ]]

# A workflow renamed into an excluded release path must still trigger the audit.
git -C "${fixture}" mv .github/workflows/release.yml scripts/release/release.yml
git -C "${fixture}" commit --quiet -m workflow-to-release-rename
workflow_rename_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${rename_sha}" "${workflow_rename_sha}")" == $'production=false\nsecurity=true\nhnsw_contract=false' ]]

# The public HNSW harness contract is a short CI check, not a Rust product build.
mkdir -p "${fixture}/scripts/performance/tests"
printf 'contract\n' >"${fixture}/scripts/performance/issue681_public_pair.py"
git -C "${fixture}" add scripts/performance/issue681_public_pair.py
git -C "${fixture}" commit --quiet -m hnsw-harness-change
hnsw_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${workflow_rename_sha}" "${hnsw_sha}")" == $'production=false\nsecurity=false\nhnsw_contract=true' ]]

printf 'name: probe\n' >"${fixture}/.github/workflows/issue681-hosted-qualify.yml"
git -C "${fixture}" add .github/workflows/issue681-hosted-qualify.yml
git -C "${fixture}" commit --quiet -m hnsw-workflow-change
hnsw_workflow_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${hnsw_sha}" "${hnsw_workflow_sha}")" == $'production=false\nsecurity=true\nhnsw_contract=true' ]]

printf 'test\n' >"${fixture}/scripts/performance/tests/test_issue681_public_pair.py"
printf 'product\n' >"${fixture}/README.md"
git -C "${fixture}" add README.md scripts/performance/tests/test_issue681_public_pair.py
git -C "${fixture}" commit --quiet -m mixed-hnsw-product-change
mixed_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${hnsw_workflow_sha}" "${mixed_sha}")" == $'production=true\nsecurity=true\nhnsw_contract=true' ]]

printf 'observer\n' >"${fixture}/scripts/performance/phase_rss.py"
git -C "${fixture}" add scripts/performance/phase_rss.py
git -C "${fixture}" commit --quiet -m shared-hnsw-observer-change
observer_sha="$(git -C "${fixture}" rev-parse HEAD)"
[[ "$(run_classifier "${mixed_sha}" "${observer_sha}")" == $'production=false\nsecurity=false\nhnsw_contract=true' ]]
