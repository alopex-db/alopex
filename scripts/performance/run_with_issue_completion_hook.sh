#!/usr/bin/env bash
set -u

issue="${1:?issue number is required}"
repo="${2:?repository is required}"
shift 2
started=$(date +%s)
status=0
"$@" || status=$?
elapsed=$(( $(date +%s) - started ))

if [ "$status" -eq 0 ]; then
    body="Codexの実行が完了しました。issue #${issue} の測定処理は成功です。elapsed_seconds=${elapsed}。"
else
    body="Codexの実行が終了しました。issue #${issue} の測定処理は失敗です。exit_status=${status} elapsed_seconds=${elapsed}。途中値は受入値にしません。"
fi
gh issue comment "$issue" --repo "$repo" --body "$body" || true
exit "$status"
