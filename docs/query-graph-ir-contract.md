# 共通Query IRとGraph trackの接続契約

更新: 2026-10-16。設計契約（実装・適合試験は各Issueで追跡）。所有者は [#636](https://github.com/alopex-db/alopex/issues/636)（共通IR・planner・runtime）と [#596](https://github.com/alopex-db/alopex/issues/596)（Graph G1〜G6）。既存SQL互換性、G1〜G6の順序、#184のストレージ計画とv2.0分散統合を維持する。

## 現行コードと移行境界

確認基点: `92ce0b0ae4025b317561b83b6bf8e61a4e68867e`。

| 現行実装 | 接続方針 |
|---|---|
| `crates/alopex-sql/src/planner/logical_plan.rs` は AST の AlterTableAction / WindowFrame とSQL側の型・式を参照 | SQL意味解析の出力をversioned共通型・式・演算子へlowerするadapterを設ける。DDLの効果はcommand契約で保持する |
| `crates/alopex-sql/src/executor/mod.rs` はSQLのLogicalPlanを入力とする | 既存SQL経路との結果・エラー差分試験を通して共通physical operator入力へ移行する |
| `crates/alopex-dataframe/src/lazy/logical_plan.rs` は独自LogicalPlanを持つ | Graph ETLのscan/transform/explodeを共通IRへ接続する。既存DataFrame APIを維持する |
| `crates/alopex-cluster/src/distributed_read/planner.rs` はSQLのRemoteReadDescriptorを受け、ReadFenceを生成 | descriptor adapterを介して共通IRへ接続し、metadata version/read point/consistency fenceを保持する |

[既存分散SQL capability](distributed-read.md) を移行基準とする。現在のremote対応範囲の拡張は、演算子単位の適合試験とcapability version更新で行う。

## Frontend・API・plannerの責務

| 入口／層 | 所有する責務 | 共通境界へ渡すもの |
|---|---|---|
| SQL + SQL/PGQ | parse、name binding、SQL NULL/三値論理/cast/collation、PGQ subset検証、診断 | 型付きrelational/Graph IR、明示したnull/cast/path semantics、source map |
| 将来のGraph専用言語 | 言語固有構文、path mode/default、対応範囲の検証 | 同じGraph IRとsemantic options。GQL対応範囲は独立したconformance matrixで管理 |
| Elasticsearch Aggregation | bucket/metric/pipeline、missing/multivalue、bucket順序/size、response shape | 共通集約IRと明示したsemantic options。SQL parityは同じ意味に正規化したsubsetで判定 |
| Rust型付きAPI | Graph query/ETL builder、型検証、transaction handle | 同じ検証済みIRまたはcatalog/job command。SQL/PGQはこの意味モデルへ接続 |
| Python API | Rust binding、Arrow/DataFrame変換、例外整形 | Rustと同じquery/job ID、IR、transaction context |
| 共通planner | schema/type検証、authorization、capability/cost、rewrite、物理化、配置・execution mode | versioned physical plan、必要capability、snapshot fence、budget |
| runtime・provider | physical operator実行、MVCC可視性、cancel、metrics、index/connector I/O | 型付きbatch、実行状態、分類済みerror |

SQL/PGQの`GRAPH_TABLE`はGraph結果をrelationとしてJoin/Filter/Projection/Aggregateへ合流させる。Rust/Pythonのtyped planも共通検証・authorizationを通過する。`execute_plan`の公開方式とABIは#636の拡張設計で決める。

## Logical operator契約

以下の名称は設計上の識別子であり、公開ABI名はG1/#636でversion固定する。全nodeにoperator ID/version、入力・出力schema、semantic options、source mapを持たせる。plan contextはgraph ID、catalog/schema version、generation manifest、transaction/read point、authorization、execution mode、budgetを保持する。

| Operator | 入出力と意味契約 | physical選択・制約 |
|---|---|---|
| `GraphScan` | graph mappingから型付きvertex/edge ID、label、propertiesをrelationとして返す。IDはgraph・mappingと組み合わせて識別する | 通常表scan/index scan。catalogと基表の可視性を一致させる |
| `GraphTraverse` | seed binding + vertex ID → binding、src、edge ID、dst、要求properties。direction=out/in/both、label/predicate、bag multiplicityを明示する | adjacency lookupまたは端点join。self-loopのbothは同一binding/edge/orientationの正規化規則を固定し、parallel edgeは別edge IDで保持 |
| `BoundedPathExpand` | seed → end vertex、hop count、要求時はordered vertex/edge ID列。0 ≤ min_hops ≤ max_hops、有限max_hops必須。0-hopは各seedの空pathを返す | walk（再訪可）/trail（edge再利用制限）/simple（vertex再利用制限）を明示。G1でPGQ subsetとの対応を固定。列挙はbag、明示Distinctのみ集合化 |
| `VectorTopK` + `GraphSeed` | query vector、metric、k、filter semantics → vertex ID、distance/score、rank、seed ID。表rowからGraph IDへのmappingを検証 | HNSW/exact scan、dimension/metric、ef_search、candidate budget、approximationを宣言。HNSW内部node IDはprovider内で表IDへ解決 |
| `Filter` / `Projection` / `Join` / `TopK` | Graph relationを共通型・式で処理。seedごとのpath provenanceを保持し、rerankとglobal limit位置を固定 | predicate pushdownはpath multiplicity、null semantics、TopK前後のfilter意味を保存する場合に適用 |
| `Bucket` / `Metric` / `Pipeline` | Graph結果も通常relationと同じ集約入力。group key、missing、null、multivalue、exactnessを明示 | Partial → Exchange → Merge/Reduce → Finalize。AVG stateはsum/count。pipelineは必要なglobal finalize後に実行 |

探索上限はmax_hopsに加え、展開edge数、出力行数、時間、メモリを全planで管理する。超過時は分類済みresource-limit errorを返す。部分結果は明示的opt-inと`complete=false`、reason、実際の上限を必要とする。通常の結果LIMITと安全上限を別属性で表す。

隣接インデックスはphysical access providerとして、graph/mapping ID、direction、label、vertex ID batch、snapshot/generationを受け、edge ID/endpoint batchとordering・coverage・cost・visibility capabilityを返す。ready/building/invalid状態と世代をplannerが検証する。利用可能な同一snapshotの表joinをfallback候補にでき、選択理由をEXPLAINへ出す。

拡張Graph演算子も共通registryにversion、schema inference、required capability、determinism、ordering/multiplicity、cost、physical factory、snapshot/delta supportを登録する。分散実装はstate serialization、partitioning、merge law、retry identityを追加する。capability不足は実行前に分類する。

## Transaction、ETL publish、snapshot/incremental

Graph DDL/ETL job制御はcatalog/job commandを所有し、データ変換部分をSource → Cast/Filter/Explode/Projection → Validate → Stageへlowerする。`PublishGraphGeneration`はeffectful commandとしてread-only optimizerのrewrite境界を形成する。

公開manifestは基表の可視version、Graph catalog/schema、adjacency generation、設定されたHNSW generationを束ねる。小規模ロードはcaller transaction、大規模ロードはstaging + 最終atomic manifest publishを用いる。競合検出、端点検証、index readiness検証後にcommitし、失敗・cancelは旧manifestを維持する。retryはjob ID/input fingerprint/idempotency key/checkpointで同じ公開結果へ収束する。

query開始時にcatalogとmanifestを同じsnapshotへpinする。継続中queryは旧世代を読み切り、新queryはcommit後の世代を参照する。旧成果物のGCはreader pin解放後に行う。HNSWの必要世代が不一致ならgeneration mismatchとして失敗し、明示されたexact fallback capabilityがある場合は同一snapshotのexact scanを選択できる。

caller transaction内ではread-your-writesを維持する。indexが未commit更新を表現できる場合はoverlayを統合し、同等性が証明できる基表経路を選択できる。両経路のcapabilityが不足する場合は分類済みunsupportedを返す。SQL/Rust/Pythonでcommit/rollback/savepoint境界を共有する。

snapshotは標準実行経路。#481のowner分割に従い、#507が単一ノードCommitDelta/DerivedState/StateFrontier、#509がcost selection/state共有/residency/local migration、#481がdistributed propagationを所有する。増分ETLによるデータ更新とquery operatorのincremental maintenanceを別capabilityとして扱う。

incremental planは全対象operatorのinsert/delete/update、multiplicity、state versionとfrontier適合を検証する。bounded traversalやANNの増分維持は適合試験を満たしたoperatorから有効化する。autoは同じread pointのsnapshot recomputeを選択可能とし、incremental必須指定はcapability不足を分類する。rebuild/replay後も指定read pointとfrontierの一致を確認する。

## Distributed executionとEXPLAIN

Graph logical semanticsを保持して、seed/endpointのowner routing、frontier Exchange、path state移送、global budget管理をphysical planへ置く。cycle検査はpath単位、duplicate対策はquery/operator/partition/batch/attempt identityによる再送除去とし、bag multiplicityを保持する。全workerは同じread fenceとmanifestを使用し、failoverはそのfenceを満たすreplicaを選ぶ。

HNSWのshard候補をglobal mergeしてseed TopKを確定してからGraph expansionを開始する。candidate truncation/approximation/recall条件を報告する。exact指定はexact providerとglobal TopKを必要とする。seed TopKと展開後rerank TopKは独立したnodeで表す。

集約のpartial stateはversioned schema、associative merge、empty/null処理、overflow、ordering、floating-point許容誤差を宣言する。retryで同じpartial batchを二重適用することを防ぐidentityを使用する。ordered aggregateはorder-preserving mergeまたはraw-value finalizationを選択する。Graphの分散対応は#174 capability gateで段階的に公開する。

EXPLAINはfrontend/source span → stable logical node ID → physical node ID → fragment/workerの対応、snapshot/generation、選択providerとfallback理由、path/cycle policy、推定cardinality/cost、exact/approx、budget、snapshot/incremental選択理由を表示する。ANALYZEはseed数、展開edge/path数、実行行数、partial/reduce件数、通信量、retry、frontier、時間・メモリ、terminal completenessを追加する。

## 共通適合試験（実装時の受け入れゲート）

全caseでdataset、plan/semantic options、snapshot、capability、seed設定をartifactへ保存する。順序指定がある結果は列順で、通常結果はbagとして比較し、浮動小数点は宣言した許容誤差で比較する。以下はテスト仕様であり、実行済み結果は各実装PRに記録する。

| ID | 条件・期待結果 | 所有段階 |
|---|---|---|
| C01 | SQL/PGQ・Rust・Pythonで同じpatternをlowerし、正規化IR、型、bag、error class、transaction境界が一致。将来Graph frontendにも同じfixtureを適用 | #636 / G1,G3 |
| C02 | scalar・単一値group keyのSQL GROUP BY/AVGとElastic terms/avgを同じnull/missing/order/size条件で比較。完全bucket出力とsum/count reduceが一致。multivalueやmissing差はsemantic optionsで別case化 | #636 |
| C03 | A→Bを別IDの2辺、B→C、C→A、A→A、孤立Dを含むfixtureでout/in/both、0/1/2-hop、walk/trail/simple、parallel edge、null propertyを検証。独立path列挙oracleとrecursive CTEで比較 | G2,G3 |
| C04 | C03をadjacencyと表joinで比較。rollback/savepoint、delete/upsert、rebuild、schema変更、再起動後にも同じsnapshotで一致 | G2,G3 |
| C05 | exact seedのtieをvertex IDで安定化し、HNSWは固定index/configの候補とrecallを測る。共通の固定seed集合では各入口の展開結果が一致。seed filter/post-filter、dimension不一致、k=0、世代不一致を検証 | G5 |
| C06 | ETL validate/build/publish直前・直後の障害注入、reader pin中のpublish、resume/重複retryを検証。可視状態は旧manifestまたは完全な新manifest。graph/adjacency/HNSWとprovenanceが一致 | G4,G5 |
| C07 | 1/複数partition、cross-shard cycle、self-loop、multi-edge、worker retryでlocalと同じbag/aggregate。global seed TopK、path budget、read fence保持を検証 | G6 / v2.0 / #174 |
| C08 | max depth/expansions/rows/memory/timeout/cancelの境界直前・到達・超過で同じ分類。partial opt-in時のみcomplete=falseを返す。全APIでauthorization適用 | #636 / G3,G5 |
| C09 | delta対応operatorでinsert/delete/update、commit跨ぎ、checkpoint/replay/rebuild後を同じfrontierのsnapshot再計算と比較。unsupported mandatory modeとauto fallbackを別caseで検証 | #507,#509 / G5 |
| C10 | EXPLAIN node対応、provider、世代、limit、approx、fragmentを照合。ANALYZEの展開・reduce・retry・completenessをfixture実測と照合 | #167 / G3,G5,G6 |
| C11 | 既存SQLのNULL/三値論理/cast/collation/集約/エラー/transaction回帰と既存remote capability matrixを維持。operator/version不足はdispatch前に分類 | #636 / #174 |
| C12 | local/S3同一入力、export/rehydrate/tiering/schema migration後にID、manifestの対応、query結果を比較 | G6 |

## 実装順とowner

G1で本契約とPGQ/API conformance、IR versionを固定する。G2はmanifest/adjacency provider、G3はGraph lowering/physical operators、G4はETL command/publish、G5はseed pipelineと増分更新、G6はconnector/配置/分散capabilityへ接続する。G3/G4はG2上で既存計画どおり並走できる。

#636の共通schema/operator/context契約をG1と共同で固定し、G3/G5の受け入れ時には対応するIR adapterとcontract testを要求する。#636全体の完成をGraph各段階の開始条件にせず、必要な共通境界を段階ごとに実装する。SQL/Elasticの二入口parityとGraph/API適合を個別に追跡し、親Issueの完了はそれぞれの既存条件と追加ゲートで判断する。
