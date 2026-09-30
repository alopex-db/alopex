#!/usr/bin/env bash
set -euo pipefail

base_sha="${1:?base commit is required}"
head_sha="${2:?head commit is required}"

for sha in "${base_sha}" "${head_sha}"; do
  if ! [[ "${sha}" =~ ^[0-9a-f]{40}$ ]] || ! git cat-file -e "${sha}^{commit}" 2>/dev/null; then
    echo 'production=true'
    exit 0
  fi
done

changed_paths="$(mktemp)"
trap 'rm -f "${changed_paths}"' EXIT
if ! git diff --name-only -z "${base_sha}" "${head_sha}" >"${changed_paths}"; then
  echo 'production=true'
  exit 0
fi

production=false
changed=0
while IFS= read -r -d '' path; do
  changed=$((changed + 1))
  case "${path}" in
    .github/workflows/*|scripts/release/*|scripts/parity/*|formal/release-report/*) ;;
    *) production=true ;;
  esac
done <"${changed_paths}"

if [[ "${changed}" -eq 0 ]]; then
  production=true
fi
echo "production=${production}"
