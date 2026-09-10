# HNSW scale attempt status

CodexはIssue添付のAmazon archive（SHA-256 `fb3c0bc51dbfaae83c8b72cf472a69fe4a894d4d9e8fa639ff22f4100da123ea`）と、GloVe固定データ（SHA-256 `544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4`）を確認しました。

Codexは既存の `run_scale_benchmark` を `max_n=50_000` で実行しましたが、50k構築が約13分継続したため中断しました。Codexは続けて `max_n=10_000` を実行しましたが、10分近く経過してもAlopex index構築が完了せず、I/O待ちで中断しました。Codexは結果行を1件も成功値として採用していません。

| requested N | status | reason |
|---:|---|---|
| 10,000 | incomplete | Alopex build exceeded controlled 10-minute budget |
| 50,000 | incomplete | Alopex build exceeded controlled execution budget |
| 200,000 | not attempted | larger N is not justified before bounded path exists |
| 1,000,000 | not attempted | larger N is not justified before bounded path exists |

Codexは次に、全体診断を呼び出さず、Alopexのbuild-only経路を上限付きで計測できる最小ハーネスへ切り分けます。
