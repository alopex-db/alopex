# CI・テスト方針

この文書は、Alopex で「何をテストするか」「どの CI を作るか」「どの CI は作らないか」を
決める規範である。個々のジョブの所有者は
[CI build responsibility](ci-build-responsibility.md) と
[release pipeline responsibilities](release-pipeline-responsibilities.md) が記録し、
互換性を主張するテストの証跡は
[reference compatibility development policy](reference-compatibility-development-policy.md)
が定める。本文書はそれらの上位にある選定基準であり、矛盾する場合は本文書を優先して
下位文書を直す。

## 原則

1. **テストは現行リリースが出荷する挙動を守るために存在する。** 過去のリリース番号を
   名前に持つゲートや、過去の受け入れ手順を再現するだけのテストは持たない。
   旧形式データのオープンや single-node 既定値のような互換契約は、現行スイートの
   通常テストとして書く。
2. **1 つの署名に 1 つの所有者。** `(toolchain, target, profile, features, package set)`
   の組み合わせごとに実行ジョブは 1 つとする。同じ workspace スイートを別名で
   二度走らせない。
3. **PR CI は決定論的で、失敗が commit の修正を要求するものだけ。** 時間や
   ランナー性能に依存する判定は PR CI に入れない。
4. **性能は計測して記録する。PR や release を止めない。** 性能受け入れは所有
   issue が `workflow_dispatch` で実行して結果を issue に記録する。定期計測は
   advisory であり、必須チェックにも tag/publication の条件にもしない。
5. **追加するときに削除条件を決める。** 新しいジョブ・レーン・テストは、
   所有者、トリガー、想定実行時間、削除条件（Delete when）を同時に文書化する。
   削除条件を書けないものは追加しない。

## 何をテストするか

| 種別 | 内容 | 置き場所 |
| --- | --- | --- |
| 機能回帰 | 公開 surface（SQL、CLI、server、embedded、DataFrame、Python）の観測可能な挙動 | 各 crate の unit/integration テスト、`lane_ci` |
| 互換契約 | 旧形式 DB のオープン、既定値の維持、フォーマットと圧縮の互換 | 現行スイート内の通常テスト（例: `v07_compatibility.rs`, `format_compatibility_test`） |
| 参照互換 | ピン留めした参照実装との differential、公開 API 台帳との一致 | `docs/parity` 台帳と `scripts/reference_tests`。curated ケースは PR、全コーパスは dispatch |
| 実行経路契約 | 分散読み取りの拒否、fail-closed 境界、ロック、リカバリ | `v08-release-gate` が委譲する各 crate スイート |
| 配布物 | パッケージ構造、インストール可能性、manifest/digest、最小 smoke | RC Qualification（`rc-qualification.yml`）と Stable Delivery |
| 耐久・安全性 | stress、sanitizer、fuzz、soak | 週次 schedule と dispatch のみ |
| 性能 | Python overhead、HNSW/SQL/DataFrame parity、ingestion | schedule（advisory）または issue 所有 dispatch |

## 作るべき CI

- **PR/push ごと**: fmt、clippy（release toolchain）、workspace テスト（stable、
  Linux は `v08-release-gate`、macOS は `test`）、coverage、cross-platform build、
  security audit、境界付き形式モデル、path フィルタ付きの CLI/Python/parity/release
  契約レーン。いずれも決定論的で、通常 30 分以内に終わること。
- **週次 schedule または dispatch**: Windows フル workspace、stress/sanitizer/fuzz/
  perf レーン、公開パッケージ到達性、advisory 性能計測。
- **issue 所有 dispatch**: 性能受け入れ（`parity-performance.yml`）。issue 番号を
  入力に取り、結果 URL を issue に記録する。
- **release/tag 起動**: RC Qualification と Stable Delivery。既知の機能や性能を
  再テストせず、承認済み SHA の同一性と配布物だけを検証する。

## 作ってはいけない CI

- **過去リリース専用のゲート**（`v06_gate.sh`、`v07_gate.sh` のようなもの）。現行
  スイートが同じ挙動を検証しているなら重複であり、していないなら現行スイートに
  移す。
- **同じスイートの重複実行**。beta toolchain レーン、nextest による二重実行、
  cron の二重登録（日次と週次で同じジョブ）は持たない。
- **PR CI に置く時間依存テスト**。同じ workload を 2 回測って差を閾値判定する
  テストや、固定マージンの latency 判定は、perf レーン（`lane_perf`）に置くか
  削除する。
- **push ごとの advisory 計測**。advisory な結果しか出さないジョブは schedule と
  tag、dispatch に限る。
- **自己参照する契約チェック**。ワークフローが実行中に自分自身の内容を検証する
  ステップは意味がない。契約は `scripts/release/tests` の unit テストとして
  `release-process.yml` で検証する。
- **どこからも実行されないテスト**。unit テストを追加するときは、それを実行する
  ワークフローと path フィルタを同じ変更で配線する。配線できないなら追加しない。
- **常に skip されるテスト**。依存パッケージがなければ skip する benchmark を
  通常テストツリーに置かない。
- **利用できないランナーに依存する schedule**。self-hosted ランナーが安定して
  いない間は dispatch のみとする。

## 運用ルール

- **連続失敗を放置しない。** 週次レーンが 3 回連続で失敗したら、直すか削除する。
  失敗が続いているのに誰も見ないレーンは存在しないのと同じであり、コストだけが残る。
- **ジョブを追加・削除したら同じ変更で文書と契約テストを直す。**
  `docs/release-pipeline-responsibilities.md` の一覧は
  `test_release_identity_contract.py` が全ジョブについて 1 行ずつ要求する。
- **リリース手順から性能を外し続ける。** 性能結果、性能ランナーの可用性、定期
  計測の失敗は、いずれもリリースの前提条件にしない。
- **命名にバージョンを含めない。** テスト・スクリプト・ジョブ名は契約の内容で
  命名する（例: `v07_compatibility` ではなく「pre-cluster database open」）。
  既存の版名付きファイルは、内容を変えるときに改名する。

## 新しい CI ジョブ・テストの追加チェックリスト

- [ ] 検証する挙動が現行リリースの出荷範囲にあり、既存スイートで未検証である。
- [ ] 実行の署名（toolchain、target、features、package set）に既存の所有者がいない。
- [ ] トリガーは PR（決定論的）、週次 schedule、issue 所有 dispatch のいずれかで、
      理由が書ける。
- [ ] 性能値を PR や release の合否に使っていない。
- [ ] 削除条件と所有者を文書化した。
- [ ] `docs/release-pipeline-responsibilities.md` に 1 行追加し、契約テストが通る。
- [ ] 追加した unit テストを実行するワークフローを配線した。
