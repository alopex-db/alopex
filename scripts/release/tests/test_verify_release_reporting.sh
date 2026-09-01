#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
results="${scratch}/v0.8.5.json"
output="${scratch}/report"
log="${scratch}/step.log"

python3 "${repo}/scripts/release/verify-release/report.py" init \
  --results "${results}" --version 0.8.5 --rust 1.90.0 --nim nimlang/nim:2.2.4
{
  echo "SKIP early diagnostic"
  for index in $(seq 1 70); do echo "ordinary line ${index}"; done
  echo "ERROR final diagnostic"
} >"${log}"
python3 "${repo}/scripts/release/verify-release/report.py" record \
  --results "${results}" --name demo --status fail \
  --description "failure extraction" --log "${log}"

bash "${repo}/scripts/release/verify-release/run.sh" --report-only "${results}" \
  --report-dir "${output}"
report="${output}/v0.8.5.md"
grep -Fxq '> 総合結果: **❌ 失敗あり**' "${report}"
grep -Fq 'SKIP early diagnostic' "${report}"
grep -Fq 'ERROR final diagnostic' "${report}"
python3 "${repo}/scripts/release/verify-release/report.py" validate-report \
  --results "${results}"

complete="${scratch}/complete.json"
complete_log="${scratch}/complete.log"
python3 "${repo}/scripts/release/verify-release/report.py" init \
  --results "${complete}" --version 0.8.5 --rust 1.90.0 --nim nimlang/nim:2.2.4
echo '--- PASS=22 / FAIL=0 / SKIP=0 / ERROR=0' >"${complete_log}"
python3 "${repo}/scripts/release/verify-release/report.py" record \
  --results "${complete}" --name parity --status ok \
  --description "complete parity" --log "${complete_log}"
python3 "${repo}/scripts/release/verify-release/report.py" validate-public \
  --results "${complete}"

skipped="${scratch}/skipped.json"
skipped_log="${scratch}/skipped.log"
python3 "${repo}/scripts/release/verify-release/report.py" init \
  --results "${skipped}" --version 0.8.5 --rust 1.90.0 --nim nimlang/nim:2.2.4
printf '%s\n' 'SKIP   compat-fixtures' \
  '--- PASS=22 / FAIL=0 / SKIP=1 / ERROR=0' >"${skipped_log}"
python3 "${repo}/scripts/release/verify-release/report.py" record \
  --results "${skipped}" --name parity --status ok \
  --description "incomplete parity" --log "${skipped_log}"
if python3 "${repo}/scripts/release/verify-release/report.py" validate-public \
  --results "${skipped}"; then
  echo "public validation accepted an executed SKIP" >&2
  exit 1
fi

if grep -Eq 'git push|gh pr create' "${repo}/scripts/release/verify-release/run.sh"; then
  echo "run.sh must generate reports without publishing them" >&2
  exit 1
fi

echo "release report generation/publication separation: ok"
