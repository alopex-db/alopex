# Release report publication model

公開パッケージ検証の「結果保存」「レポート生成」「明示的公開」「失敗通知」を分離し、失敗結果が一般向け保証書として自動公開されないことを検査する。

```bash
cd formal/release-report
docker compose run --rm apalache typecheck ReleaseReport.tla
docker compose run --rm apalache check --config=ReleaseReport.cfg --length=12 ReleaseReport.tla
```

`RetryLimit = 2`、最大12遷移の有限モデルである。GitHub Actions のスケジューラ可用性、レジストリの indexing latency、無限実行の liveness、人間によるレビュー品質は証明しない。
