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
  --run-id 123 --run-attempt 2 \
  --responsibility 'Stable Delivery / downstream availability'
{
  echo "SKIP early diagnostic"
  for index in $(seq 1 70); do echo "ordinary line ${index}"; done
  echo "ERROR final diagnostic"
} >"${log}"
python3 "${repo}/scripts/release/verify-release/report.py" record \
  --results "${results}" --name demo --status failure \
  --description "failure extraction" --log "${log}"
python3 "${repo}/scripts/release/verify-release/report.py" finalize \
  --results "${results}"

bash "${repo}/scripts/release/verify-release/run.sh" --report-only "${results}" \
  --report-dir "${output}"
report="${output}/v0.8.5.md"
grep -Fxq '> 総合結果: **❌ 失敗あり**' "${report}"
grep -Fq 'SKIP early diagnostic' "${report}"
grep -Fq 'ERROR final diagnostic' "${report}"
grep -Fq '| Commit | `deadbeef` |' "${report}"
grep -Fq '| Run | `123` / attempt `2` |' "${report}"
grep -Fq '| 失敗段階 | demo |' "${report}"
grep -Fq '| 実行 | https://example.invalid/run/1 |' "${report}"
if grep -Fq 'サーバー・クラスタのすべて' "${report}"; then
  echo "public availability report must not claim functionality demos" >&2
  exit 1
fi
python3 "${repo}/scripts/release/verify-release/report.py" validate-report \
  --results "${results}"
python3 - "${results}" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["outcome"] == "failure"
assert payload["failure_stage"] == "demo"
assert payload["started_at"].endswith("Z")
assert payload["completed_at"].endswith("Z")
assert payload["identity"]["run_id"] == "123"
assert payload["identity"]["run_attempt"] == 2
PY

complete="${scratch}/complete.json"
complete_log="${scratch}/complete.log"
python3 "${repo}/scripts/release/verify-release/report.py" init \
  --results "${complete}" --version 0.8.5 --rust 1.90.0 --nim nimlang/nim:2.2.4 \
  --run-id 124 --run-attempt 1
echo '--- PASS=22 / FAIL=0 / SKIP=0 / ERROR=0' >"${complete_log}"
python3 "${repo}/scripts/release/verify-release/report.py" record \
  --results "${complete}" --name parity --status success \
  --description "complete parity" --log "${complete_log}"
python3 "${repo}/scripts/release/verify-release/report.py" finalize \
  --results "${complete}"
python3 "${repo}/scripts/release/verify-release/report.py" validate-report \
  --results "${complete}"
python3 "${repo}/scripts/release/verify-release/report.py" render \
  --results "${complete}" --output-dir "${output}"
grep -Fq '最小importが成功している' "${output}/v0.8.5.md"
python3 - "${complete}" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["outcome"] == "success"
assert payload["failure_stage"] is None
PY

incomplete="${scratch}/incomplete.json"
incomplete_log="${scratch}/incomplete.log"
python3 "${repo}/scripts/release/verify-release/report.py" init \
  --results "${incomplete}" --version 0.8.5 --rust 1.90.0 --nim nimlang/nim:2.2.4 \
  --run-id 125 --run-attempt 1
printf '%s\n' 'SKIP   public-index-check' \
  '--- PASS=1 / FAIL=0 / SKIP=1 / ERROR=0' >"${incomplete_log}"
python3 "${repo}/scripts/release/verify-release/report.py" record \
  --results "${incomplete}" --name availability --status success \
  --description "workflow incomplete" --log "${incomplete_log}"
python3 "${repo}/scripts/release/verify-release/report.py" finalize \
  --results "${incomplete}"
python3 "${repo}/scripts/release/verify-release/report.py" render \
  --results "${incomplete}" --output-dir "${output}"
python3 "${repo}/scripts/release/verify-release/report.py" validate-report \
  --results "${incomplete}"
grep -Fxq '> 総合結果: **⚠️ 未完了**' "${output}/v0.8.5.md"
python3 - "${incomplete}" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["outcome"] == "incomplete"
assert payload["failure_stage"] == "availability"
PY

if grep -Eq 'git push|gh pr create' "${repo}/scripts/release/verify-release/run.sh"; then
  echo "run.sh must generate reports without publishing them" >&2
  exit 1
fi

echo "release report generation/publication separation: ok"
