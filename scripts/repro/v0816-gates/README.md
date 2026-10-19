# v0.8.16 リリースゲートの再現スクリプト（#669〜#672）

v0.8.16 の候補 `integration/v0.8.16-candidate` = `69b9abd3` で見つかった、リリースゲートの4件を再現するスクリプトです。
修正の確認にもそのまま使えます。各スクリプトは**不具合が再現すれば終了コード 1、直っていれば 0** を返します。

| スクリプト | issue | 内容 |
|---|---|---|
| `gate1_explain_cap.py` | #669 | B-tree 索引の候補が上限を超えて全件スキャンに切り替わっても、EXPLAIN / EXPLAIN ANALYZE が `IndexScan` と表示する |
| `gate2_kv_scan.py` | #670 | `Transaction.scan_prefix` / `scan_range` が既定100件で黙って打ち切られる。ちょうど `scan_budget` 件でもエラーになる |
| `gate3_hnsw_cache.py` | #672 | `db.begin()` の中の kNN が HNSW キャッシュを使わない。キャッシュの上限を Python から変えられない |
| `gate4_knn_option_bind.py` | #671 | `WITH (ef_search = ?, enable_hnsw = ?)` を束縛できない |

## 1. ビルド

`v0.8.16` はまだ PyPI にないため、ソースからビルドします。
`build_local.sh` が、次の手順を Docker なしで行います。
- GitHub から Nim 2.2.10 を取得してビルドする
- Nim SQL パーサー（`.so` / `.a` と、`CONTRACT_VERSION` / `SHA256SUMS`）をビルドする
- 仮想環境に `maturin develop --release` で Python バインディングを入れる

リポジトリにあるパーサーの設定ファイル（`nim-sql-parser/vendor/parser-vendor-manifest.json`）は v0.8.4 のままなので、`NIM_SQL_PARSER_LIB_DIR` と `ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD=1` で、ローカルでビルドしたパーサーを使います。

```bash
git checkout <検証したいコミット>          # 例: 69b9abd3（不具合あり）や、修正後の候補
scripts/repro/v0816-gates/build_local.sh   # 作業用ディレクトリの既定は target/repro-work（git の管理対象外）
PY=target/repro-work/venv/bin/python
```

必要なもの: git、C コンパイラ、Rust（cargo）、python3。
Nim の初回ビルドに数分、Rust の release ビルドに10〜15分かかります（4 vCPU の場合）。

## 2. 実行

```bash
cd scripts/repro/v0816-gates
$PY gate1_explain_cap.py          # 約1分
$PY gate2_kv_scan.py              # 数秒
$PY gate4_knn_option_bind.py      # 数秒
# gate3 は、索引を作る段階と計測する段階を分けている
$PY gate3_hnsw_cache.py build   /tmp/gate3.alopex 20000    # 2万行: 約70秒（10万行なら約11分）
$PY gate3_hnsw_cache.py measure /tmp/gate3.alopex          # 約10秒
```

各スクリプトの最後の `RESULT:` 行と終了コードで判定します。

## 3. 実測した値（`69b9abd3`、2026-10-19）

環境: Linux x86_64、4 vCPU、15 GiB RAM、Python 3.11.15、rustc 1.94.1。
Claude Code のクラウドセッションのコンテナで実行しました。

### gate1（#669）
```
[under_cap_1000_rows] rows=1000 no_index=82.9ms index=5.3ms
  EXPLAIN ANALYZE: Filter table=t |   IndexScan index=t_small table=t | elapsed_ns=2647506 | rows=1000 |
[over_cap_10000_rows] rows=10000 no_index=78.4ms index=95.4ms
  EXPLAIN        : Filter table=t |   IndexScan index=t_big table=t |
  EXPLAIN ANALYZE: Filter table=t |   IndexScan index=t_big table=t | elapsed_ns=68803752 | rows=10000 |
  -> DEFECT: ran as a full scan but EXPLAIN ANALYZE reports IndexScan without a fallback marker
RESULT: REPRODUCED (#669)
```

### gate2（#670）
```
scan_prefix returns: list_iterator public attrs: []
len(list(scan_prefix)) = 100 (250 keys stored)
len(list(scan_range))  = 100 (250 keys stored)
scan_prefix(limit=None) ERROR: TypeError 'NoneType' object cannot be interpreted as an integer
scan_prefix(scan_budget=250) with exactly 250 keys ERROR: search scan budget exceeded: limit=250
RESULT: REPRODUCED (#670)
```

### gate3（#672）
2万行 × 384次元（`m=16, ef_construction=100`）の場合:
```
HNSW memory_bytes: 36434368
Python cache API: none
EXPLAIN: Limit table=ra |   HnswSearch index=ra_h k=10 |
auto-commit SELECT #2..#5: 2.4-2.9 ms
inside db.begin(READ_ONLY) SELECT #1..#3: 119.8 / 105.2 / 130.4 ms
inside db.begin(READ_WRITE) SELECT #1..#3: 138.5 / 89.0 / 87.7 ms
RESULT: REPRODUCED (#672)
```
同じスクリプトを10万行 × 384次元で実行した場合:
- 投入 61.4 s、`CREATE INDEX` 614.8 s
- `memory_bytes` = 182,181,060
- 自動コミットは、初回 850 ms、2回目以降 1.8〜5.6 ms
- `db.begin()` の中は、毎回 906〜1096 ms

**未計測：** 10万行の索引を2本使ったときに、互いをキャッシュから追い出し合うこと（182 MB × 2 が上限 256 MiB を超えることは、コードと `memory_bytes` で確認済み）。

### gate4（#671）
```
literal ef_search: [{'id': 0}, {'id': 4}, {'id': 44}, {'id': 217}, {'id': 98}]
execute_sql WITH (ef_search = ?) ERROR: ... expected integer or boolean kNN query option value (got tkQuestion '?')
prepare WITH (ef_search = ?) ERROR: ... expected integer or boolean kNN query option value (got tkQuestion '?')
RESULT: REPRODUCED (#671)
```

## 判定の基準（修正後に終了コード 0 になる条件）
- **gate1：** 上限を超えて全件スキャンになった場合に、EXPLAIN ANALYZE が `IndexScan` のままにならない（`fallback` の表示、または `actual_path=Scan` を出す）
- **gate2：** `list(scan_prefix)` と `list(scan_range)` が250件すべてを返す。`limit=None` を受け付ける。ちょうど `scan_budget` 件でもエラーにならない
- **gate3：** `db.begin()` の中の kNN が、温まった自動コミットの20倍以内の時間で終わる。Python に HNSW キャッシュの API（名前に `cache` を含むメソッド）がある
- **gate4：** `WITH (ef_search = ?)` と `WITH (enable_hnsw = ?)` を `execute_sql` と `prepare` の両方で使える
