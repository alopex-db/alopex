# #712 LSMのactive範囲構築の削減

MemTableの走査はキー順に整列した可視版を返す。その結果を1件ずつBTreeMapへ挿入し直す2か所を、`collect()`による一括構築に変更した。公開SQLの修正前後比較では、34,250行のORDER BY/LIMITがembeddedで42.18%、serverで43.71%短縮した。

全範囲の観測、read_setの登録、未commitの上書き・削除、immutable/SSTの版選択は維持する。LIMITまでの遅延走査やphantom競合検出を追加した変更ではなく、#712全体の受入は引き続き未完了である。

## 識別情報と対象

- 親PR: #776、HEAD `830e2c0fc409322c8f852ae7dbb371a5f496a1f7`。
- 修正前build: `79b32c30a984eebcb8b7ef86fa023fff52f84f75`。親に回帰テストのみ追加し、製品の動作は同じ。
- 修正後build: `a9800355309aa1b4296225a13dc07c7aa7c24747`。
- 製品差分は `crates/alopex-core/src/lsm/mod.rs` の `scan_prefix_visible` / `scan_range_visible` に限定。永続形式、公開API、parser、Pythonクライアント、測定ハーネスは不変。
- PR #770のwrite journal before-image修正は含めていない。効果の加算や、統合後の性能は未確認。
- [全生値と識別情報](lsm-range-bulk-build.json)に、各buildのserver/extension/parser hash、同一Pythonソース・ハーネスhash、全試行の中央値/MAD、回収記録を保存した。supervisorのsource_shaはハーネスを実行したcheckoutを示し、修正前のnative build sourceは別のhash対応表で識別する。

## 正しさとテストの不足確認

修正前・修正後とも、追加した7テストが成功した。可視版の参照モデル検証は24ケース、各ケース最大6キー・19版である。

- snapshot取得後の更新と、トランザクション内の上書き・削除・復活。
- iteratorを0件／1件で破棄した後も、未消費キーや可視tombstoneへの更新で競合すること。
- 空prefix、NULと0xffを含むキー、半開区間と空区間。
- active / immutable / SSTをまたぐsnapshot読取。
- 同一timestampでのsequence優先、tombstone、完全同順位時の既存source優先。
- 小さな独立した版選択モデルとの値・timestamp・sequenceの完全一致。

Clippyは `cargo clippy --offline --locked -p alopex-core --lib -- -D warnings` が成功。fmtと差分の空白検査も実施した。

couplingはLSMソースツリーだけを `--changed-since main --impact-depth 4` で確認した。34 modules、既存のLSM/WAL/lockへの結合がある。main比較には以前のリリース変更も含まれるため、C評価や2件のHighをこの2か所の変更に帰属させない。新しい依存境界は導入していない。runtimeの順序・共有状態、暗黙のアルゴリズム重複、sync/async距離、macro/cfgは静的解析の盲点として記録した。

mutationは対象2関数の4件に限定した。版選択の `>` を `>=` にする2件を検出。空mapを返す2件は `-D warnings` による未使用変数エラーでunviableとなり、動作検出には数えない。補足としてactive結果だけを消す故障注入をprefix/range各1件行い、両方ともコンパイル後の回帰テストが検出した。故障注入後はソースhashの復元を確認した。

最初のmutation実行ではツールが名前フィルタに反して範囲外の設定変更8件を含めたため停止した。その実行の未検出結果は対象ロジックのsurvivorに数えず、ログを保持した。事前に完全一致を確認した0始まりのslice shardで4件だけを再実行した。最初のparser指定不足とテストコードの型修正によるコンパイル失敗も記録に残している。

新しいunsafe処理や独自の境界演算はなく、Miri/Kaniは対象外。通常CIに追加したのは小さな回帰テストだけで、性能・負荷計測は追加していない。

## 修正前後の公開SQL比較

同一Mac・同一test profileで、before / after / after / before / before / afterの順に直列実行した。各側3試行、各ケース事前確認1回・warmup3回・測定21回。60計時ケースで1,260測定＋180warmupを保存した。別途、修正後のORDER BYをembedded/server各1回プロファイルした。

読取は元の34,250行と元のSQL、完全なPython結果形式を維持した。3行のORDER BYと、prefixによる主キー重複確認を通る1行INSERT/rollbackを対照にした。後者は計測開始前に対照として追加した理由をplanに記録した。

各ケースの結果値・型・順序と、再接続後の全状態を検証した。全ケースの期待値hashと全測定を別途再計算して一致した。fixture準備・検証・ログ保存は計時外。profileの値は通常の計時に混ぜない。

以下は3試行の中央値の中央値、単位ms。悪化値も省略していない。

| 経路 / ケース | 修正前 | 修正後 | 変化 |
| --- | ---: | ---: | ---: |
| embedded / aggregate 34,250行 | 13.884375 | 10.819042 | -22.08% |
| server / aggregate 34,250行 | 14.374292 | 11.348167 | -21.05% |
| embedded / ordered 34,250行 | 6.477833 | 3.745500 | -42.18% |
| server / ordered 34,250行 | 6.606542 | 3.719125 | -43.71% |
| embedded / select-all 34,250行 | 14.669750 | 11.482209 | -21.73% |
| server / select-all 34,250行 | 35.279958 | 32.098125 | -9.02% |
| embedded / ordered 3行 | 0.036333 | 0.036500 | **+0.46%** |
| server / ordered 3行 | 0.307583 | 0.304958 | -0.85% |
| embedded / insert-rollback 1行 | 0.017875 | 0.017833 | -0.23% |
| server / insert-rollback 1行 | 0.443750 | 0.450084 | **+1.43%** |

増加した2ケースは、それぞれ0.000167 msと0.006334 ms。事前に宣言した診断用変動幅 `max(修正前中央値の5%, 修正前全測定のMAD×3)` の範囲内だった。これは増加の原因が雑音だと証明したものでも、リリースの許容値を設定したものでもない。追加試行は行っていない。

修正後profileでも既存の主キー経路とLSM範囲処理を確認し、`bulk_build_from_sorted_iter` のフレームを保存した。inclusive sample数は親子が重複するため加算せず、前回profileとの件数比も性能改善率に使わない。性能効果は上記の同条件比較に基づく。

## 資源、回収、残件

- 各ケース180秒、各プロセスRSS 2 GiB、fixture 2 GB、ホスト40 GBで停止するwatchdogを使用。ビルドは共有target・jobs2・incremental/debugなしで直列化した。
- 観測ホスト最大18,352,893,952 bytes。client RSS最大355,975,168 bytes、server 74,563,584 bytes。RSSとCPU時間は準備・検証を含み、短いケースの未観測RSSはゼロと扱わない。操作単位IOは未測定。
- 公開APIの62 fixtureをすべて回収し、記録したclient/server 93 PIDが終了済みであることを確認した。unit fixtureはRAIIで回収し、所有TMPDIRも削除した。
- native package・parser・テスト実行ファイル等40ファイル、345,842,514 logical bytesを削除し、ホスト18,006,937,600 bytesへ戻した。故障注入の実行ファイルは再利用しない。記録と共有依存キャッシュを保持する。
- 完全なログ・native stackは `/private/tmp/alopex-712-lsm-range` に保存。バイナリは回収済み。

この比較は内部の修正採用判断用である。公開0.8.16・修正版・比較製品を同一条件で測る正式受入、候補への統合、候補CI、他の採用修正との組合せは未完了。全範囲走査とread_set構築の費用も残る。#712で見えているPythonの全件変換費用は別の後続対象として維持する。
