# v0.8.16 の修正（PR #673〜#676）の残課題の再現スクリプト

`integration/v0.8.16-candidate` = `d9ff8783`（PR #673 #674 #675 #676 をマージした候補）のレビューで見つかった残課題を再現するスクリプトです。
いずれも v0.8.16 のリリースブロックではなく、milestone `v0.8.17` で扱います。
各スクリプトは**不具合が再現すれば終了コード 1、直っていれば 0** を返します。

| スクリプト | 内容 |
|---|---|
| `f1_truncate_hnsw_txn.py` | `db.begin()` の中で `TRUNCATE` すると、HNSW の古いグラフがコミットで書き戻される（#676 で入った退行） |
| `f2_kv_large_values.py` | 既定の設定では、1ページが `max_bytes`（16 MiB）を超えるとき `scan_prefix` / `scan_range` / `*_page` がエラーになり、続きを読む cursor も返らない |
| `f3_explain_not_executed.py` | EXPLAIN ANALYZE で、実行されなかった B-tree の節点が `IndexScan` という名前のまま表示される |

## ビルドと実行

ビルドは `../v0816-gates/build_local.sh` を使います（手順は `../v0816-gates/README.md`）。

```bash
git checkout <検証したいコミット>
scripts/repro/v0816-gates/build_local.sh
PY=target/repro-work/venv/bin/python
cd scripts/repro/v0816-followups
$PY f1_truncate_hnsw_txn.py      # 約1分
$PY f2_kv_large_values.py        # 数秒
$PY f3_explain_not_executed.py   # 数秒
```

**注意：** `maturin develop` で入れた仮想環境は、ソースツリーにある `.so` を読み込みます。
同じソースツリーで別のコミットをビルドすると、古い仮想環境も新しい `.so` を使います。
2つのコミットを比べるときは、`git worktree` で別々のソースツリーを用意するか、`maturin build` で作った wheel を別々の仮想環境に入れてください。

## 実測した値（2026-10-19）

環境: Linux x86_64、4 vCPU、15 GiB RAM、Python 3.11.15、rustc 1.94.1（Claude Code のクラウドセッションのコンテナ）。
`69b9abd3` は `maturin build --release` で作った wheel を別の仮想環境に入れて実行しました。

### f1（3,000行 × 64次元に HNSW、TRUNCATE してから 500行を入れ直す）
```
# 69b9abd3
[txn] rows=500 node_count=500 memory_bytes=240452 node_count_after_reopen=500 knn_mismatch=0/40
[autocommit] rows=500 node_count=500 memory_bytes=240452 node_count_after_reopen=500 knn_mismatch=0/40
RESULT: OK
# d9ff8783
[txn] rows=500 node_count=3000 memory_bytes=1451680 node_count_after_reopen=3000 knn_mismatch=0/40
  -> DEFECT: HNSW graph does not match the table after TRUNCATE (txn)
[autocommit] rows=500 node_count=500 memory_bytes=240452 node_count_after_reopen=500 knn_mismatch=0/40
RESULT: REPRODUCED
```
kNN の結果は正しいままです（古いベクトルの近くを検索しても、消した行は返りません）。
間違っているのは、統計（`node_count`）とメモリ量です。

### f2（200 KB の値を100件、合計約 20 MB）
```
# d9ff8783
ERR len(list(scan_prefix(prefix)))  [defaults] -> PyAlopexError: search response size exceeded: limit=16777216, requested=16800588
ERR len(list(scan_range(start, end)))  [defaults] -> PyAlopexError: search response size exceeded: limit=16777216, requested=16800588
ERR scan_prefix_page() loop with defaults -> PyAlopexError: search response size exceeded: limit=16777216, requested=16800588
reference: scan_prefix(page_size=50) -> 100
RESULT: REPRODUCED
```

### f3
```
# d9ff8783
ExactKnnScan
Limit table=k
  Sort table=k
    Filter table=k
      IndexScan index=kg table=k actual_path=NotExecuted candidates=0 max_candidates=1024 max_bytes=8388608 loops=0 fallback=none fallbacks=0
  -> DEFECT: node is rendered as IndexScan although it did not run
RESULT: REPRODUCED
```
`69b9abd3` では `actual_path` 自体がなく、`IndexScan index=kg table=k` とだけ表示されます（これも再現扱い）。

## 判定の基準（修正後に終了コード 0 になる条件）
- **f1：** トランザクションの中で TRUNCATE したあとも、`node_count` がテーブルの行数と一致する（再オープン後も）。kNN の結果が厳密検索と一致する
- **f2：** 既定の設定で `list(scan_prefix)` / `list(scan_range)` が100件すべてを返す。`scan_prefix_page()` を cursor で回すと、100件すべてに届く
- **f3：** 実行されなかった節点を `IndexScan` と表示しない（`actual_path=IndexScan` の行だけが `IndexScan` を名乗る）
