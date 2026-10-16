# グラフDB機能・性能調査とAlopex Graph ETLの到達目標

調査日: 2026-10-11

## 結論

AlopexのGraph ETLは、ファイルを頂点表と辺表へコピーするだけでは競争力を持たない。v1.xでは、**Kùzuに近い組み込みやすさ、Neo4jに近い取込時の検証・再開性、Memgraphに近い増分反映、Neptuneに近いS3ジョブ運用、TigerGraphに近い宣言的mapping**を目指す。これらをAlopexのSQL・Arrow/DataFrame・HNSWと一つのtransaction contractで結ぶ。

調査した5製品のうち、Alopexと配置形態が最も近いのはKùzu、運用上の基準として最も成熟しているのはNeo4jである。NeptuneとTigerGraphはクラウド／分散取込、Memgraphはリアルタイム更新の設計材料になる。ただし、製品ごとにハードウェア、ライセンス、キャッシュ状態、データモデルが違うため、各社の公称性能値をそのまま順位付けには使えない。

したがって、v1.xでは機能実装と同時に比較可能なベンチマークを作る。CIでは10k頂点／50k辺、nightlyではLDBC SNB SF1、release候補ではSF10を基本レーンとし、同じデータ、同じCPU・メモリ制限、同じcold/warm条件でAlopex、Kùzu、Neo4jを測る。最初の再現可能な結果を基準値として承認するまでは、根拠のない「Neo4jより高速」といった数値目標を置かない。

## 調査範囲と方法

Property Graphの取込、更新、検索、ベクトル連携、API、運用性を調べた。対象はNeo4j、Kùzu、Memgraph、Amazon Neptune／Neptune Analytics、TigerGraphである。一次資料として各製品の公式ドキュメント、性能評価の共通枠組みとしてGraph Data CouncilのLDBC Social Network Benchmark（SNB）を使用した。

本調査では製品を実行していない。性能に関する記述は、実装方式と公式ベンチマーク方法の比較であり、Alopexとの実測比較ではない。TigerGraphには監査済みLDBC SNB BI結果があるが、大容量サーバーや分散構成の結果は組み込みDBとの直接比較の根拠にならない。そのため、数値はAlopex用ベンチマークの設計根拠としてのみ扱う。

## 製品ごとの強みは異なる

| 製品 | 主な配置形態 | Graph ETLで参考になる機能 | 検索・性能設計 | Alopexへの示唆 |
|---|---|---|---|---|
| Neo4j | server／cluster／managed | CSV・Parquetのfull／incremental import、複数node/relationship source、dry-run、resume、reject方針、schema同時作成 | 統計ベースplanner、`EXPLAIN`／`PROFILE`、pipelined／parallel runtime、property・full-text・vector index | 検証、staging、再開、index状態、実行計画までを一つの製品機能にする |
| Kùzu | embedded、on-disk／in-memory | CSV・Parquet・Pandas・Polars・Arrow、glob、厳密な型、node先行import、error row制御 | columnar storage、CSR adjacency、vectorized／factorized execution、multi-core、HNSW | Alopexの直接比較対象。Rust／Python組み込みAPIとParquet取込の使い勝手を下回らない |
| Memgraph | in-memory中心のserver、HA | CSV・JSON・Parquet、Kafka／Pulsar、S3、変換module、snapshot | Cypher、MAGE、HNSW vector searchとgraph traversalの同一query | 初回一括ロードだけでなく、継続的なevent取込とGraphRAGの鮮度をゴールに含める |
| Neptune／Neptune Analytics | AWS managed | S3 prefixからのbulk load、job API、並列度、retry、load status、property graph／RDF形式 | openCypher／Gremlin／SPARQL、Neptune Analyticsのgraph algorithmsとHNSW | Lake-Linkを非同期jobとして扱い、S3権限、status、cancel、resumeを公開契約にする |
| TigerGraph | distributed server／cloud | loading job、複数source、列mapping、式、filter、temporary table、S3 Parquet、配列展開、REST endpoint | GSQL、MPP、分散load／query、vector hybrid search | mappingを再利用可能なjob定義にし、複数入力からnode／edgeを組み立てる |

### Neo4jは「速いimport」より運用上の境界が参考になる

Neo4jの`neo4j-admin database import`は、空DBへのfull importと既存DBへのincremental importを分けている。CSVに加えてParquetを扱い、増分importはprepare、build、mergeへ分割できる。停止・失敗後のresume、off-heap memory budget、進捗ログも用意されている。一方、incremental mergeの失敗がDBを破損し得るためbackupを推奨しており、高速経路には明確な運用条件がある。

Alopexはこの境界を曖昧にしない。小規模ロードは既存transaction内で処理し、大規模ロードは新しい世代へbuildした後にatomic publishする。`dry_run`、検証件数、推定容量、途中状態、再開点をSQL、Rust、Pythonのどこから開始しても確認できるようにする。

Neo4jはnodeとrelationshipの双方にvector indexを持ち、graph patternとvector検索を同じCypher内で構成できる。AlopexでもHNSW buildをETL後の別作業にせず、Graph publishの整合性対象へ含める必要がある。

### KùzuはAlopexと同じ組み込み領域の直接比較対象になる

Kùzuは、columnar disk storage、compressed sparse row（CSR）のadjacency list、vectorized／factorized query processing、multi-core executionを特徴とする組み込みProperty Graph DBである。PythonとRustのAPIを持ち、Parquetの`COPY FROM`、複数ファイルのglob、Pandas／Polars DataFrameからの取込を提供する。

この構成はAlopexに近い。したがって、ローカルParquetからの初回ロード、Arrow連携、Rust／Python binding overhead、1〜3 hopのanchored traversalはKùzuを主な比較対象にする。Kùzuではrelationshipより先にnodeを存在させる必要があるため、Alopexも依存順をplannerで解決し、dangling edgeをpublish前に検出する。

### Memgraphは増分ETLとGraphRAGの鮮度基準になる

MemgraphはCSV、JSON、Parquetに加え、Kafka、Pulsar、S3などからの取込経路を案内し、PythonやCでmessageをCypherへ変換するstream transformationを持つ。vector searchは安定APIとして提供され、HNSWの近傍検索結果からgraph traversalへ続けるquery例も公開されている。

Alopexのv1.xでKafka connectorまで同梱する必要はない。ただし、Graph ETLの内部契約には、順序付きevent、checkpoint、idempotency key、deleteを最初から含める。これを後回しにすると、S3差分取込やCDCを追加する時にGraph catalogと隣接インデックスの更新方式を作り直すことになる。

### NeptuneはS3取込をjobとして設計している

Neptuneのbulk loaderはS3上のファイルまたはprefixを入力に取り、HTTPからjobを開始する。複数vertex／edge file、並列度、statusを扱い、vertexをedgeより先に処理する。Neptune AnalyticsはS3からgraphを作成し、HNSWによるvector similarityとgraph algorithmをopenCypher procedureから利用できる。

これはLake-Linkの公開面に近い。Alopexも`COPY`実行中の接続を保持し続ける方式だけでなく、永続job IDを返す非同期実行を用意する。S3 object listとETag、入力fingerprint、checkpoint、retry回数をmanifestへ残し、同じ入力の再実行を判定する。

注意点として、Neptune Analyticsのvector index updateはatomicではないと公式資料に明記されている。Alopexはここを差別化点にできる。少なくともGraph catalogを公開したのにHNSWだけ旧世代という状態を正常完了として扱ってはならない。

### TigerGraphはmapping languageと分散ロードの参考になる

TigerGraphのloading jobは、data sourceとnode／edgeへのmappingを分離し、列位置または列名、式、filter、temporary table、list／set／map変換を記述できる。S3上のParquetも扱い、REST endpointとしてjobを起動できる。監査済みLDBC SNB BI結果が公開されている点も、性能主張にhardware、software version、load方法、価格を添える手本になる。

Alopexのmappingは、保存、inspect、再実行できる定義にする。1つの入力から複数のnode／edgeを生成すること、配列をedgeへ展開すること、複合自然キーを解決することを段階的に扱う。

## Alopex Graph ETLへ追加する機能ゴール

### P0: publish前に正しさを証明する

- node、edge、label、property、embeddingを型付きmappingで宣言できる。
- node ID、edge ID、始点／終点に単一キーと複合自然キーを使える。
- node先行、edge後続の依存順を自動計画する。
- duplicate、dangling edge、null、cast失敗、vector次元不一致を`dry_run`で集計する。
- strict、skip、reject出力を選択でき、rejectにはsource file、row、column、reasonを残す。
- Graph catalog、基表、隣接インデックス、HNSWを同じgenerationとしてatomic publishする。
- crash、cancel、timeout後に旧generationが読み取り可能で、半端な新generationを公開しない。

### P1: mappingを再利用可能なjobにする

- CSV、Parquet、Arrow RecordBatch、Pandas／Polars DataFrameを同じmapping modelへ接続する。
- rename、cast、default、定数label、filter、split／explode、property選択を扱う。
- 複数ファイル、glob、S3 prefix、partition pruningを扱う。
- mappingとsourceを分離し、同じmappingを別のpath、bucket、partitionへ再利用できる。
- SQL、Rust、Pythonからjobを作成し、同じjob ID、状態、エラー分類を得られる。

### P2: 初回ロードと増分更新を同じ契約で扱う

- full、append、upsert、deleteを明示する。
- input fingerprintとidempotency keyにより、安全に再実行できる。
- checkpointからresumeできる。再開不能な場合は理由と必要なcleanupを返す。
- edge更新とadjacency index、embedding更新とHNSWを同じcommit境界で扱う。
- 将来のKafka／CDCを接続できるよう、順序、offset、tombstoneをmanifest modelに含める。

### P3: GraphRAGをETL完了判定に含める

- import直後にHNSW Top-Kからbounded graph expansionへ進める。
- vector候補にlabel／property filterを適用し、graph展開後にrerankできる。
- graph-only、vector-only、hybridの`EXPLAIN ANALYZE`を比較できる。
- source rowからnode／edge／embeddingまでのprovenanceを追跡できる。
- embedding欠損、HNSW build中、generation不一致を明示し、黙って古いindexを使わない。

### P4: 運用APIを製品機能として扱う

- `plan`、`dry_run`、`start`、`status`、`cancel`、`resume`、`rejects`、`metrics`を公開する。
- 読込件数、生成node／edge数、reject数、source bytes、各index build時間、peak memory、temporary diskを記録する。
- jobとGraph generationの関係をinspectできる。
- export／rehydrate時にmapping、catalog、index metadata、source fingerprintを保持する。

## 性能ゴールは同一条件の再現可能な証拠で管理する

### ベンチマークレーン

| レーン | データ | 実行頻度 | 比較対象 | 主な評価項目 |
|---|---|---|---|---|
| CI smoke | 10k node／50k edge、384次元vector | PR | Alopex内の前回基準 | correctness、rollback、wall time、peak RSS |
| Nightly embedded | LDBC SNB SF1 + Parquet変換 | nightly | Kùzu | initial load、index build、1〜3 hop、Rust/Python overhead |
| Nightly incremental | SF1の1%／10%差分 | nightly | Neo4j incrementalの意味契約 | upsert、delete、resume、read availability、freshness |
| Release | LDBC SNB SF10 | release候補 | Kùzu、Neo4j | p50/p95/p99、throughput、memory、disk、query plan |
| GraphRAG | 100k／1M node、384／768次元vector | nightly／release | Alopexのgraph-only／vector-only | recall@k、seed search、expand、filter、end-to-end latency |
| Lake-Link | localとS3に同一Parquet | scheduled | Alopex内比較 | range read、bytes read、retry、resume、full mirror不使用 |

LDBC SNBは、近傍を辿るtransactionalなInteractive workloadと、aggregation／join中心のBI workloadを共通データ上に定義している。AlopexはまずInteractiveのread queryとinsert/delete streamを採用し、列指向実行が整った段階でBI subsetを追加する。

### 必ず保存する測定値

- commit SHA、package version、CPU、core affinity、memory limit、storage、OS
- source datasetとchecksum、node／edge／property／vector件数
- cold／warm cache、worker数、batch size、transaction size
- parse、transform、validate、base write、adjacency build、HNSW build、publishの内訳
- entities/s、source MiB/s、wall time、CPU time、peak RSS、temporary bytes、final bytes
- queryのp50／p95／p99、timeout数、cancel latency
- HNSWのrecall@kとQPS。GraphRAGではend-to-end latencyと展開node／edge数も記録する
- failure injection後のrecovery time、再処理件数、重複件数、公開generation

### release gate

機能面では、期待node／edge checksum、参照整合性、再実行後の重複ゼロ、失敗後の部分公開ゼロを必須とする。性能面では、承認済みの同一Alopex baselineから主要指標が20%以上悪化した場合にstress laneを失敗させる。Kùzu／Neo4jとの比較は同一環境の比率として公開し、最初の測定PRでv1.xの数値目標を承認する。比較対象を上回ることは先に約束しない。差が生じたstageと理由を説明できない状態はrelease不可とする。

## Alopexの差別化ポイント

成熟製品の全機能をv1.xで追う必要はない。Alopexが優先すべき差は次の三つである。

第一に、通常テーブルを正本にし、SQL、Property Graph、DataFrame、vector検索を同じtransactionとcatalogで扱う。Graph専用DBへ複製する方式より、データの二重管理を減らせる。

第二に、Graph catalog、adjacency index、HNSWをgeneration単位で公開する。vector indexの更新がgraph更新とずれる製品に対し、GraphRAGの一貫性を明示的な契約にできる。

第三に、組み込みRust APIとPython／Arrow連携を第一級にする。server接続を必須にせず、ETLから検索までを同一processで実行できる点は、Kùzuと競争しつつAlopex固有のSQL／vector資産を生かせる。

## 今後の実装判断

Issue #596のG1で、mapping model、job state machine、generation publish protocol、benchmark manifestを先に固定する。G2〜G4はKùzuとNeo4jを実行比較できる最小E2Eを保ちながら進める。G5でMemgraph／Neptune Analytics相当のhybrid queryを追加し、G6でS3 sourceとresumeを完成させる。

次に分割する子Issueには、再現可能なcomparison harnessを含める。性能主張はrelease artifactにcommit、環境、raw resultを残せた場合だけREADMEへ掲載する。

## 参照資料

- [Neo4j Operations Manual: Import](https://neo4j.com/docs/operations-manual/current/import/)
- [Neo4j Operations Manual: Incremental import](https://neo4j.com/docs/operations-manual/current/import/incremental-import/)
- [Neo4j Cypher Manual: Vector indexes](https://neo4j.com/docs/cypher-manual/current/indexes/search-indexes/vector-indexes/)
- [Neo4j Cypher Manual: Execution plans](https://neo4j.com/docs/cypher-manual/25/planning-and-tuning/execution-plans/)
- [Kùzu Documentation](https://kuzudb.github.io/docs/)
- [Kùzu: Import Parquet](https://kuzudb.github.io/docs/import/parquet/)
- [Kùzu: Copy from dataframes](https://kuzudb.github.io/docs/import/copy-from-dataframe/)
- [Kùzu Rust crate](https://docs.rs/kuzu/latest/kuzu/)
- [Memgraph Documentation](https://memgraph.com/docs)
- [Memgraph Release Notes](https://memgraph.com/docs/release-notes)
- [Memgraph Vector Search](https://memgraph.com/vector-search)
- [Amazon Neptune: Using the bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html)
- [Amazon Neptune: Load data formats](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format.html)
- [Neptune Analytics: Vector indexing](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/vector-index.html)
- [Neptune Analytics algorithms](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/algorithms.html)
- [TigerGraph: Creating a Loading Job](https://docs.tigergraph.com/gsql-ref/current/ddl-and-loading/creating-a-loading-job)
- [TigerGraph: Load from cloud storage](https://www.tigergraph.com/docs/tigergraph-server/3.10/data-loading/load-from-cloud)
- [Graph Data Council: LDBC Social Network Benchmark](https://ldbcouncil.org/benchmarks/snb/)
- [Graph Data Council: SNB Interactive](https://ldbcouncil.org/benchmarks/snb/interactive/)
- [Graph Data Council: SNB Business Intelligence](https://ldbcouncil.org/benchmarks/snb/bi/)


## 共通Query IRへの設計接続（2026-10-16）

[#596](https://github.com/alopex-db/alopex/issues/596) のG1〜G6と [#636](https://github.com/alopex-db/alopex/issues/636) の共通実行基盤を[演算子・transaction・共通適合試験契約](query-graph-ir-contract.md)で接続する。Graph traversalとbounded pathは共通Logical IR、adjacency/HNSWはplannerが選択するphysical provider、Graph ETLは共通変換IRとatomic publish commandへ割り当てる。SQL/PGQ、将来Graph言語、Rust/Pythonのparity、Graph世代とsnapshot、EXPLAIN、分散reduceをC01〜C12で検証する。既存の競合調査、性能基準、P0〜P4とG1〜G6を維持する。
