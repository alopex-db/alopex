# v0.8.13 引継ぎ

## 再開地点

- 作業ツリー: `/home/roomtv/works/alopex-db/alopex/.codex/worktrees/release-v0.8.13-rebuild`
- ローカルブランチ: `release/v0.8.13-rebuild-stage`（ローカル先端 `38914256`）
- リモート先端: `origin/release/v0.8.13-rebuild-stage` = `30751e432507ef2e2a11c582105743c6aa2d1c0d`
- 保全対象: `origin/release/v0.8.13` = `7ea0559be35fbc0922e9b09aef894a0ddc083e0c`。変更禁止。

Codexは、ローカル作業ツリーに未コミット差分があるため、PR #436 をローカルへ取り込んでいない。次セッションのCodexは、差分を退避・評価した後にだけ `30751e4` を別の安全な作業ツリーへ取り込むこと。

## 直近の確定事項

- PR #436 は 2026-09-16 10:37 JST に `release/v0.8.13-rebuild-stage` へマージ済み。CI ポリシーは同PRの `docs/ci-test-policy.md` が正本。
- `release/v0.8.13` は一切変更していない。
- タグ、公開、PR の新規作成はしていない。
- RC Qualification は一時ワークフローであり、安定リリース完了・リリース識別子記録後、post-release verification 前に削除する。恒久化しない。
- 受け入れ、リリース、リリース後検証は確実に直列な一つの Issue #428 とする。200k/1M 分離測定は受け入れではなくリリース後診断。

## 未コミット差分: **採用禁止、再評価または撤回が必要**

現在の9ファイルの差分は、50k `batch_existing` タイムアウトの途中調査で作成された。Codexはこれらをコミット・プッシュしていない。

1. `crates/alopex-sql/src/executor/dml/insert.rs`
   - `ON CONFLICT` 更新を一括で `apply_changes` へ渡し、`RowsAffected` を修正する候補。
2. `crates/alopex-sql/src/executor/dml/update.rs` / `executor/hnsw_bridge.rs`
   - HNSW 更新を一括化する候補。
3. `crates/alopex-core/src/vector/hnsw/{graph.rs,mod.rs,storage.rs,tests/mod.rs}`
   - HNSW 既存ノードを再配線せずトポロジー維持で更新する候補。これは hnswlib の局所修復方式とも Qdrant の mutable-segment/optimizer 方式とも一致しないため、現状では採用不可。
4. `crates/alopex-server/tests/http_test.rs`
   - `batch_existing` の50kシードを10k単位に分割する、測定前準備の変更。
5. `crates/alopex-sql/tests/on_conflict_upsert.rs`
   - RowsAffected の回帰アサーション。

次セッションのCodexは、まず `git diff` を保存してから上記差分を一つずつ根因に対して必要か判定すること。HNSW差分を性能値だけで正当化しないこと。

## 性能調査の事実

測定契約は HTTP / `batch_existing` / 50,000 rows / runtime thread 1 / warmup 1 / sample 1 / API timeout 30 seconds。実測は明示ローカル parser (`NIM_SQL_PARSER_LIB_DIR=/tmp/alopex-v0813-parser`) で行った。

| 実装状態 | 結果 |
| --- | --- |
| 変更前 | `QUERY_TIMEOUT`、テスト全体 108.24s |
| HNSW 一括再接続 | `QUERY_TIMEOUT`、89.28s |
| HNSW 局所再接続候補 | `QUERY_TIMEOUT`、100.45s |
| HNSW トポロジー保持＋増分保存候補 | `QUERY_TIMEOUT`、100.22s |

Codexは、これらの結果からHNSW仮説を未証明と分類した。`http_ingestion_measurement` が実際に HNSW index を作成するかを、次セッションのCodexが測定関数全体を再読して確認する必要がある。タイムアウトを緩和してはならない。

先行実装の調査結果:

- [hnswlib](https://github.com/nmslib/hnswlib/blob/master/hnswlib/hnswalg.h) は既存点を局所近傍の修復と再接続で更新する。
- [Qdrant](https://github.com/qdrant/qdrant) は mutable / append-only segment と optimizer のライフサイクルを持つ。
- [Faiss HNSW](https://github.com/facebookresearch/faiss/wiki/Faiss-indexes) は HNSW の削除・更新を一般には提供しない。

## 先に行う作業

1. Codexは未コミット差分を保全して、`30751e4` を基点に新しい作業ツリーを作る。
2. Codexは `http_ingestion_measurement` の実際の index 作成・SQL実行経路を確認し、50k遅延のREDを最小プロファイルで再現する。
3. Codexは参照実装と同じ責務境界の根因だけを修正し、変更済み経路の性能を再測定する。
4. Codexはローカルの機能・回帰・性能検査を通してから、コミットして `release/v0.8.13-rebuild-stage` へプッシュする。
5. Codexは RC Qualification を一度だけ実行し、成功後にのみリリース受け入れを再判定する。

## 既存のリモート履歴

`release/v0.8.13-rebuild-stage` には、少なくとも次のコミットが存在する。

- `463c4499` remote stdio / untrusted path denial
- `3f5709ae` batch uniqueness index validation
- `e325711e`, `9b52882e`, `c0808da5` RC parser qualification and batch/parser checks
- `38914256` `batch_existing` measurement harness
- `30751e43` PR #436: CI policy simplification

