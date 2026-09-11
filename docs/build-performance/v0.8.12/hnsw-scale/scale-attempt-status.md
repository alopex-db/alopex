# HNSW scale attempt status

CodexはIssue添付のAmazon archive（SHA-256 `fb3c0bc51dbfaae83c8b72cf472a69fe4a894d4d9e8fa639ff22f4100da123ea`）と、GloVe固定データ（SHA-256 `544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4`）を確認しました。

Codexは既存の `run_scale_benchmark` を `max_n=50_000` で実行しましたが、per-vector Python 挿入を含むAlopex構築が約13分継続したため中断しました。Codexは続けて `max_n=10_000` を実行しましたが、同じ経路が10分近く経過しても完了せず、I/O待ちで中断しました。これはAlopexの構築時間ではなく、既存ハーネスの実行経路が計測に不適切であることを示す診断です。Codexは結果行を1件も成功値として採用していません。

| requested N | status | reason |
|---:|---|---|
| 10,000 | build-only complete; search scale incomplete | Rust core build-only: 49,930.979 ms; search/recall contract not run |
| 50,000 | incomplete | Rust core build-only exceeded 600-second per-case budget; no row emitted |
| 200,000 | not attempted | larger N is not justified before bounded path exists |
| 1,000,000 | not attempted | larger N is not justified before bounded path exists |

Rust coreのbuild-only probeでは、GloVe先頭1,000件（100次元、M=16、ef_construction=200）を15,115.205 msで構築し、1,000ノードを確認しました。この値はdebug計測用バイナリのprobeであり、リリース性能値には採用しません。同じcore経路の10,000件は300秒上限まで完了せず、結果行を出力しませんでした。

その後、Codexはcosine距離のdot計算を既存SIMD kernelへ切り替え、同じ入力の10,000件をrelease最適化（計測用にLTO無効）で単独実行しました。Codexは49,930.979 ms、10,000 nodes、6,651,852 bytesを得て、修正前88,166.685 msから約43%短縮しました。この行はbuild-only成果物として採用しますが、Issueの検索recall/QPS契約を満たすscale行ではありません。

Codexは同じRust core build-only経路で50,000件を単独実行しましたが、SIMD修正後も600秒のケース上限まで完了せず、出力行はありませんでした。Codexはこのケースを性能値として採用せず、50k以上を同じ経路で連続実行しません。

Codexは上記ケースを `scale-attempt-status.json`、`scale-attempt-status.csv`、本Markdownへ同一内容で保存しました。

Codexは次に、全体診断を呼び出さず、Alopexのbuild-only経路を上限付きで計測できる最小ハーネスへ切り分けます。

Codexは比較エンジン（FAISS flat/HNSW、hnswlib）のbuild-only実行も開始しましたが、複数エンジン・複数Nを同一プロセスで順に処理する経路が20分を超える前に最初の集計行を出力しなかったため中断しました。これは比較エンジンの性能値ではなく、集計責務を分離できていない実行経路の診断です。Codexは部分結果を出力していないため、比較エンジンの値も成功値として採用していません。Codexは次に、AlopexはRust-side build harness、比較エンジンはエンジン×Nごとの個別プロセスへ分離し、各ケースの結果を即時保存して再測定します。
