# HNSW scale attempt status

CodexはIssue添付のAmazon archive（SHA-256 `fb3c0bc51dbfaae83c8b72cf472a69fe4a894d4d9e8fa639ff22f4100da123ea`）と、専用データブランチ`data/issue-298-glove-v2`（commit `8735a7fe`）のGloVe固定データ（SHA-256 `544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4`）を正準入力として確認しました。

Codexは既存の `run_scale_benchmark` を `max_n=50_000` で実行しましたが、per-vector Python 挿入を含むAlopex構築が約13分継続したため中断しました。Codexは続けて `max_n=10_000` を実行しましたが、同じ経路が10分近く経過しても完了せず、I/O待ちで中断しました。これはAlopexの構築時間ではなく、既存ハーネスの実行経路が計測に不適切であることを示す診断です。Codexは結果行を1件も成功値として採用していません。

| requested N | status | reason |
|---:|---|---|
| 10,000 | Python API complete; Rust core build-only complete | Python API build/search: see `glove-scale-10k.json`; Rust core build-only: 49,930.979 ms |
| 50,000 | complete | Alopex・FAISS HNSW・hnswlib・Flat exactを独立workerで完走。`glove-scale-matrix.*` |
| 200,000 | measurable; not run | worker accepts N=200,000; this acceptance cell has not been executed |
| 1,000,000 | measurable; not run | worker accepts N=1,000,000; this acceptance cell has not been executed |

Rust coreのbuild-only probeでは、GloVe先頭1,000件（100次元、M=16、ef_construction=200）を15,115.205 msで構築し、1,000ノードを確認しました。この値はdebug計測用バイナリのprobeであり、リリース性能値には採用しません。同じcore経路の10,000件は300秒上限まで完了せず、結果行を出力しませんでした。

その後、Codexはcosine距離のdot計算を既存SIMD kernelへ切り替え、同じ入力の10,000件をrelease最適化（計測用にLTO無効）で単独実行しました。Codexは49,930.979 ms、10,000 nodes、6,651,852 bytesを得て、修正前88,166.685 msから約43%短縮しました。この行はbuild-only成果物として採用しますが、Issueの検索recall/QPS契約を満たすscale行ではありません。

Codexは同じRust core build-only経路で50,000件を単独実行しましたが、SIMD修正後も自身が設定した600秒の安全中断に到達したため停止しました。これは製品の完了時間・失敗ではなく、Codexの運用中断です。Codexはこのケースを性能値として採用しません。

Codexは入力SHA-256 `a88c65b881c5461d7f793f75e20cb63ecf5bf58c3dda4adab16d1b99c2599449`を固定したfresh release単独ケースを実行し、自身が設定した900秒の安全中断で停止しました。これは製品の完了時間・失敗ではなく、Codexの運用中断です。Codexは600秒試行と900秒試行を履歴証跡として保持し、50kのbuild_msを推定・補間しません。

Codexは今後、製品ごとの任意のwall-clock上限を測定契約にしません。buildは対象N件の挿入完了を終了条件とし、searchは固定query数・run数の完走を終了条件とします。過去の時間打切り試行は測定値として採用せず、完了セルの根拠にも使用しません。

Codexは上記の試行履歴を `scale-attempt-status.json`、`scale-attempt-status.csv`、本Markdownへ保存し、完成した比較行は別責務の `glove-scale-matrix.*` へ保存しました。

CodexはGloVe 10k/50kについて、Alopex・FAISS HNSW・hnswlib・Flat exactを同一入力・同一query契約で測定し、`glove-scale-matrix.json`とbuild/search別CSV/Markdownへ保存しました。50kはengine×N独立worker、10kは既存fixed-query artifactです。200k/1Mは同じworkerで測定可能ですが、まだ実行していないため受入未達です。

Codexは比較エンジン（FAISS flat/HNSW、hnswlib）の複数N同一プロセス経路を採用せず、責務分離したmatrixへ切り替えました。過去の混在経路に由来する値は採用していません。50kはAlopexを含む全engineで同じ入力・設定・終了条件を完走しており、200k/1Mは比較結果を確定していません。
