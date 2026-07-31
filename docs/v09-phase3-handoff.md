# v0.9 Phase 3 completion-gate handoff

## 結論

**Phase 3 feature verdict: Blocked.**  Task 3.30 は完了ゲートを実行し、実装・回帰証跡が揃っていることと、機能を完了・出荷できないことを分けて記録した。
これはリリース判定ではなく、tag、publish、Docker image push、GitHub Release、デプロイを一切許可しない。

評価 checkout は `c5fa8f16f80d0bdd3536b22ffc78393330befab8` である。機械可読な結果は
[`evidence/v09-phase3-completion.json`](../evidence/v09-phase3-completion.json) にあり、requirements の 18 基準の対応は
[`evidence/v09-phase3-crosswalk.json`](../evidence/v09-phase3-crosswalk.json) にある。

## 独立に確認した入力

| 項目 | 確認結果 |
|---|---|
| 3.1–3.29 の実装ログ | 29 件すべて存在し、タスク表では完了 |
| public-surface manifest | 64 lifecycle 行、11 change-kind 行、20 source snapshot hash が一致 |
| inherited ledger | I-01–I-26（suffix を含む）と F2/F3/F4 の 33 行 |
| R1.1–R5.4 crosswalk | 18 基準すべてに主担当・3 検証観点・実在する証跡を設定 |
| parity fixture | embedded / HTTP / gRPC / CLI / Python の共通 fixture hash が handoff 入力と一致 |
| common-gate / verifier handoff の入力 hash | 両方の全 5 入力が一致 |

コード変更を伴わない evidence gate であるため、このタスクでは Cargo/Python の再ビルドを行わなかった。commit
`c5fa8f1` の pre-commit all-target Clippy と、3.20–3.25 の記録済み fixture 実行結果を再利用した。これは新しいテスト成功を主張するものではなく、各 fixture のログと参照先を静的に再照合したものである。

## 完了できない理由

1. Durable prerequisite は fail closed のまま。`service_available`、durable storage、range routing、retention、authenticated dispatcher の証跡がない。従って lifecycle は `prerequisite_missing` を返し、local WAL へ fallback してはならない。
2. `v09-phase3-verifier-input.json` は verifier の実行結果ではなく handoff 入力である。candidate Rust/Python artifact identity と digest、および target-version v0.9 common-gate report がまだない。
3. common-gate と verifier handoff は、その時点の source SHA を記録している。入力 hash は現在も一致するが、最終 gate は candidate checkout 自身から再ハッシュして実行しなければならない。
4. Phase 1 range contract、Phase 2 CRDT、Phase 4 distributed transaction の release-wide 状態を Phase 3 が代行して承認することはできない。旧 v0.7/v0.8 gate で代用することも禁止する。

## 証跡が揃っている範囲

event schema/range ordering、checkpoint/ack/resume/idempotency、retention/backpressure、SQL/DataFrame の pre-execution unsupported、公開 surface parity、inherited ledger、requirements crosswalk は、manifest・fixture・実装ログへ結び付いている。詳細は completion JSON の `completion_criteria` を参照する。

この結果は、現行の API 形状や failure mapping の証跡が存在することを示すだけである。互換 Durable profile が確認されるまで、利用者に operational Durable feed が available であると表示してはならない。

## 次の所有者と必要な結果

| 所有者 | 必要な結果 |
|---|---|
| cluster/Durable | 互換 Chirps Durable profile、認証済み dispatcher、storage/range-routing/retention/service の実証 |
| release-wide common gate / alopex-tools | 同一 candidate checkout で manifest・ledger・crosswalk・fixture を検証し、missing/unknown/duplicate を fail closed で報告。Rust/Python artifact identity も記録する |
| Phase 1 / Phase 2 / Phase 4 / release-wide owners | 各 phase の承認済み証跡を揃え、別の target-version v0.9 release gate を実行する |

上記がそろうまでは、この handoff の verdict は `Blocked` のままであり、リリース操作は開始しない。
