# LSM-Tree ファイルモード（LsmKV）ベンチマーク

## 目的

- Phase 6.3 のベンチマーク基盤として、`LsmKV` と `MemoryKV` の比較を行う。
- 点読み取り/点書き込みのレイテンシ目標（参考: Point Get ~1.2μs、Point Put ~2.5μs）に対して、現状のボトルネックを把握する。

## 実行方法

作業ディレクトリ:

```bash
cd /home/roomtv/works/alopex-db/alopex-core-worktrees/lsm-tree-file-mode
```

基本実行:

```bash
TMPDIR=/home/roomtv/works/alopex-db/.tmp cargo bench -p alopex-core --bench lsm_kv_bench -- --noplot
```

データ規模の調整:

- `ALOPEX_LSM_BENCH_N`: 書き込み/読み取りの件数（デフォルト 100_000）
- `ALOPEX_LSM_BENCH_SCAN_LEN`: scan_range の範囲長（デフォルト 1_000）

例（軽めに回す）:

```bash
TMPDIR=/home/roomtv/works/alopex-db/.tmp ALOPEX_LSM_BENCH_N=10000 cargo bench -p alopex-core --bench lsm_kv_bench -- --noplot
```

例（目標確認に近い負荷）:

```bash
TMPDIR=/home/roomtv/works/alopex-db/.tmp ALOPEX_LSM_BENCH_N=1000000 cargo bench -p alopex-core --bench lsm_kv_bench -- --noplot
```

## ベンチ内容

`crates/alopex-core/benches/lsm_kv_bench.rs` が以下を測定する。

- `batch_writes`: シーケンシャル/ランダムのバッチ書き込み（単一トランザクションで N 件）
- `point_put`: 1 件 put + commit のレイテンシ
- `point_get`: 1 件 get（ReadOnly トランザクションで begin + get）
- `scan_range`: `scan_range` の範囲読み取り（件数カウントまで）

## 現状の注意点（解釈）

- 現段階の `LsmKV` は SSTable への永続 flush を伴わないため、主に「WAL + MemTable + トランザクション管理」のコストが反映される。
- durable flush/compaction が入ると、I/O パスやエラー方針次第で結果が変わる。

## 計測結果（参考）

以下は本リポジトリ作業環境での参考値（`SyncMode::NoSync`）です。環境差・設定差が大きいので、目標確認は同一条件で継続計測してください。

- コマンド:
  - `TMPDIR=/home/roomtv/works/alopex-db/.tmp ALOPEX_LSM_BENCH_N=10000 ALOPEX_LSM_BENCH_SCAN_LEN=1000 cargo bench -p alopex-core --bench lsm_kv_bench -- --noplot`
- 結果抜粋（中央値付近）:
  - `lsm_kv/point_get/lsmkv/10000`: 約 `0.52µs`
  - `lsm_kv/point_put/lsmkv`: 約 `2.92µs`
  - `lsm_kv/scan_range/lsmkv/10000`（len=1000）: 約 `0.39ms`

## 目標とボトルネックの当たり所（メモ）

- Point Put は「トランザクション生成」「WAL 追記」「commit の OCC 検証」「MemTable 反映」が主な構成要素。
- Point Get は「トランザクション生成」「可視読み取りパス（MemTable / Immutable / SSTable）」が主な構成要素。
- 目標未達の場合は、まず `cargo bench --bench lsm_kv_bench` の出力（median）を添えて、`perf`/`flamegraph` 等でホットスポットを切り分ける。
