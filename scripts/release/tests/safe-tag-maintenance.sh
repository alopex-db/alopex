#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_SCRIPT="$(cd "${SCRIPT_DIR}/.." && pwd)/safe-tag.sh"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEST_ROOT}"' EXIT

git init --bare "${TEST_ROOT}/remote.git" >/dev/null
git init -b main "${TEST_ROOT}/repo" >/dev/null
cd "${TEST_ROOT}/repo"
git config user.name "release-test"
git config user.email "release-test@example.invalid"
mkdir -p scripts/release crates/alopex-py
cp "${SOURCE_SCRIPT}" scripts/release/safe-tag.sh
printf '[workspace.package]\nversion = "0.7.6"\n' > Cargo.toml
printf '[package]\nname = "alopex-py"\nversion = "0.7.6"\n' > crates/alopex-py/Cargo.toml
git add .
git commit -m "base release" >/dev/null
git tag -a v0.7.6 -m "Release v0.7.6"
git remote add origin "${TEST_ROOT}/remote.git"
git push -u origin main --tags >/dev/null

git switch -c release/v0.7.7 v0.7.6 >/dev/null
sed -i 's/0\.7\.6/0.7.7/g' Cargo.toml crates/alopex-py/Cargo.toml
git add Cargo.toml crates/alopex-py/Cargo.toml
git commit -m "prepare patch" >/dev/null
git push -u origin release/v0.7.7 >/dev/null

bash scripts/release/safe-tag.sh v0.7.7 --maintenance-base v0.7.6 >/dev/null
bash scripts/release/safe-tag.sh alopex-py-v0.7.7 --maintenance-base v0.7.6 >/dev/null

# The chained Python workflow runs after the Rust workflow has created v0.7.7.
git tag -a v0.7.7 -m "Release v0.7.7"
git push origin v0.7.7 >/dev/null
bash scripts/release/safe-tag.sh alopex-py-v0.7.7 --maintenance-base v0.7.6 >/dev/null

if bash scripts/release/safe-tag.sh v0.7.7 >/dev/null 2>&1; then
    echo "safe-tag unexpectedly accepted a maintenance release without --maintenance-base" >&2
    exit 1
fi
if bash scripts/release/safe-tag.sh v0.7.7 --maintenance-base v0.6.9 >/dev/null 2>&1; then
    echo "safe-tag unexpectedly accepted a base from a different release line" >&2
    exit 1
fi

echo "safe-tag maintenance release tests passed"
