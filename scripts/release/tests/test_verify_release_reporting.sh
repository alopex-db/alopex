#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
results="${scratch}/v0.8.5.json"
output="${scratch}/report"
log="${scratch}/step.log"

python3 "${repo}/scripts/release/verify-release/report.py" init \
  --results "${results}" --version 0.8.5 --rust 1.90.0 --nim nimlang/nim:2.2.4 \
  --commit deadbeef --tag v0.8.5 --run-url https://example.invalid/run/1 \
  --responsibility 'Stable Delivery / downstream availability'
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
grep -Fq '| Commit | `deadbeef` |' "${report}"
grep -Fq '| 実行 | https://example.invalid/run/1 |' "${report}"
grep -Fq '最小importが成功している' "${report}"
if grep -Fq 'サーバー・クラスタのすべて' "${report}"; then
  echo "public availability report must not claim functionality demos" >&2
  exit 1
fi
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
python3 "${repo}/scripts/release/verify-release/report.py" validate-report \
  --results "${complete}"

if grep -Eq 'git push|gh pr create' "${repo}/scripts/release/verify-release/run.sh"; then
  echo "run.sh must generate reports without publishing them" >&2
  exit 1
fi

echo "release report generation/publication separation: ok"
