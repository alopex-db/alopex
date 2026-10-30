# #712 公開SQLの4ケース別診断

集約・並び替え付きLIMIT・全件取得・INSERT後のrollbackを、embedded/serverそれぞれ独立したプロセスとデータで診断した。22ケースが成功し、全データを回収した。製品コードの変更はない。

`ORDER BY … LIMIT 16` は既存の主キー高速経路を使っているが、その下のLSM範囲走査が全範囲を構築している。まずこの処理を調査する。全件取得にはPython側の行・値変換という別の費用もある。

## ソースと検証範囲

- 実行ソース: `019afdb92da4765a829285272a5a2de9faf1396e`。
- 製品ソース: PR #774 の `8e596f2eece1c84ffd1cfa2f0f3eb7176a868598` と同一。候補 `e4d5f59e67b93d8e3f4a491dc725cb96a47639f8` にDELETE主キー修正を加えた状態で、PR #770のLSM journal修正は含まない。
- macOS arm64 / Python 3.13.16 / Rust 1.96.0。`cargo build --offline --locked --profile test -p alopex-server -p alopex-py`、共有target、jobs=2、incremental/debug=0、`RUSTFLAGS=-D warnings`。
- パーサーは公開v0.8.16、contract 0.28.0。アーカイブSHA256は `8ac1f708efdfdbf6c85a65aebd34a368de41e798d9441a2785abad011cec038d`。ローカルoverrideによる診断で、配布パッケージの認定ではない。
- 原SQL・データ生成は既存の `sql-v0.8.11-curated.json` と公開SQL測定に合わせた。read側は `bench(id INTEGER PRIMARY KEY, value DOUBLE, enabled BOOLEAN)`、idの連番・`float(id)`・偶数フラグ。100行単位のautocommitで準備する。
- 元のINSERT/rollbackケースは初期1行なので、読取ケースの34,250行と混同せず独立した1行ケースにした。
- HTTPはloopback、認証なし、keep-alive、timeout=30秒。Rayon/Tokio/OMP各2、OpenBLAS=1。macOSではCPU affinityを設定していない。

これは1試行の内部診断であり、公開0.8.16・修正版・比較製品を同じホストで比べる正式な性能受入は別途必要。元のLinux結果との比率や、改善率は計算しない。

## 計時と正しさ

最小データ8ケース（読取3行、rollback1行）、元のサイズの読取6ケース（34,250行）、別プロセスでのプロファイル8ケースを直列実行した。

- 各計時ケースは事前確認1回、warmup 3回、計時21回。14ケース、計時294件＋warmup42件の全生値をJSONに保存した。
- 読取はpublic `execute_sql` がPythonの全結果を返すまで。INSERTはbegin・execute・rollback・handle解放を分け、すべてをtotalに含める。SQL構築、検証、ログ保存は計時外。
- 全件取得だけはSQL上で順序未指定のため、検証時にidで整列する。ORDER BYの結果は返却順のまま比較する。値・Python型・列・行数を検証し、再接続／再open後に全データが変わっていないことも確認した。
- プロファイルは4秒の別ループ。native `sample` は3秒、全スレッドのスタックを採取し、検証処理とattach待機も含む。Python cProfileは公開API呼び出しだけを包む。どちらも計時データに混ぜない。
- ケースごとに180秒、各プロセスRSS 2 GiB、データ2 GB、ホスト40 GBで停止する。通常CIでは実行を拒否する。CIへ負荷・性能テストは追加していない。

## 観測値

21件の中央値 / MAD、単位ms。読取は34,250行、INSERT/rollbackは1行。

| 処理 | embedded 中央値 / MAD | server 中央値 / MAD |
| --- | ---: | ---: |
| COUNT、偶数フラグのfilter | 13.471 / 0.155 | 14.222 / 0.212 |
| 主キー下限、ORDER BY、LIMIT 16 | 6.228 / 0.042 | 6.439 / 0.051 |
| 偶数行のid/valueを全件取得 | 13.876 / 0.143 | 34.226 / 0.491 |
| INSERT RETURNING後rollback | 0.018 / 0.001 | 0.522 / 0.011 |

INSERT/rollbackのserver各相の中央値は、begin 0.140 ms、execute 0.233 ms、rollback 0.143 ms、解放0.000625 ms。各相の中央値の和とtotal中央値は別統計である。

## スタックから分かったこと

1. **ORDER BYの主キー経路は既に使われている。** `build_primary_key_ordered_limit → scan_primary_key_from → IndexStorage::range_scan → LsmTransaction::scan_range` がembedded/server両方で確認できた。index rangeの代表枝はそれぞれ1,350 / 1,171 inclusive samples、その下のLSM map構築の代表枝は745 / 703だった。親子を加算したり、経過時間の割合として扱ったりしない。
2. **LIMITの前で範囲全体が構築される。** ソースでは `scan_range_visible` のBTreeMapから別のmapを構築し、全キーをread_setに登録した後でiteratorを返す。SQL側の `.take(max_rows)` はその後に適用される。集約・全件取得にもTableStorageのrange scanが現れた。PR #770のwrite journal before-image修正とは別の箇所である。
3. **server全件取得にはPython変換の費用もある。** cProfileの60回では `_rows_to_dicts` が累積2.011秒、内包する `_unwrap_sql_value` は2,055,000呼び出し・累積1.387秒、JSON decodeは0.346秒だった。計測器の費用と親子の重複を含むため、通常実行の寄与率や改善率には換算しない。
4. **1行rollbackは3回のHTTP呼び出しを伴う。** 6,727回のprofile反復に対し `_request_json` は20,181回だった。begin／execute／rollbackを保つAPI契約を無視して往復を削る提案はしていない。

## 保存・回収・次の作業

[JSON記録](sql-query-baseline.json)に全計時値、ケース結果、ソースとバイナリのhash、選択したnativeスタック、Python profile、資源計測、削除記録を保存した。完全なnativeスタックとコンパクトな実行ログは `/private/tmp/alopex-712-query-profile` に残した。

- ハーネスの単体検証10件成功（既存DELETE 4件＋新規6件）。ネイティブビルド成功。Rust製品コードは変更しておらず、mutation/coupling/Miri/Kaniの追加実行は対象外。
- ホスト使用量の観測最大18,033,999,872 bytes、回収後17,778,679,808 bytes。所有するバイナリ・package・parser等25ファイル、255,533,728 logical bytesを削除した。共有依存キャッシュと記録を保持する。
- 観測RSS最大はclient 355,926,016 bytes、server 74,694,656 bytes。準備と検証も含む。短いケースでserver RSSを採取できなかった値は未観測であり、ゼロではない。プロセス全期間のCPU時間は保存したが、操作単位のCPU／IOは未測定。

次は `LsmTransaction::scan_range` / `scan_prefix` の全範囲再構築を対象にする。最小ケースでsnapshot可視性、read_setの競合検出、未commitの上書き・削除、iterator早期破棄を固定し、既存の観測範囲を勝手に狭めずにコピーを減らせるか検証する。Rustの実装変更時はこの境界に限定したcoupling・mutation検証を行う。Pythonの型厳密な行変換は別の後続項目として扱う。

このPRは診断結果の引き継ぎであり、#712全体の受入、候補への統合、リリース準備完了を意味しない。
