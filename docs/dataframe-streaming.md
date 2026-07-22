# DataFrame の bounded / streaming 実行（v0.8）

v0.8 の DataFrame streaming は、通常の eager 実行を小分けにした互換モードではない。対応する `LazyFrame` 計画だけを、ソース読み込み・デコード・演算・出力の各所有権を事前予約したうえで、有限の Arrow `RecordBatch` 単位で実行する。

## 公開 API

```rust
use std::num::NonZeroUsize;

use alopex_dataframe::{concat, LazyFrame, StreamOptions};

let options = StreamOptions::new(
    64 * 1024 * 1024,
    NonZeroUsize::new(2).unwrap(),
    NonZeroUsize::new(8_192).unwrap(),
);

let plan = LazyFrame::scan_csv("events.csv")?;
let eager = plan.clone().collect()?;
let bounded = plan.clone().collect_with_options(options)?;
let mut stream = plan.collect_streaming(options)?;
while let Some(batch) = stream.next_batch()? {
    // batch は consumer 所有の有限 DataFrame
}
```

- `LazyFrame::collect()` は従来の通常実行である。全ての既存演算が streaming 化されたことを意味しない。
- `LazyFrame::collect_with_options(options)` は、対応する streaming 計画を最後に `DataFrame` へ materialize する。保持する各出力 batch も予算に予約する。
- `LazyFrame::collect_streaming(options)` は `DataFrameStream` を返す。利用者は `next_batch()`、`close()`、または `cancel()` で終了させる。
- `DataFrame::concat` と crate root の `concat` は、同一 schema の二つ以上の eager frame を入力順に垂直結合する。`LazyFrame::concat` も二つ以上の lazy input を同じ規則で結合する。

`memory_limit_bytes` は pipeline が同時に所有できる最大バイト数、`max_in_flight_batches` は同時に pipeline が所有できる batch 数、`batch_rows` はソースに要求する最大行数である。`batch_rows` はメモリ上限を緩和しない。

## ソースと実行モード

| ソース | 通常 `collect` | bounded / incremental | v0.8 の契約・制限 |
| --- | --- | --- | --- |
| CSV file scan | 対応 | 対応 | 物理入力レコード順を保存する。schema は bounded open 時に決定し、concat は全 child の schema を最初の出力前に照合する。 |
| Parquet file scan | 対応 | 対応 | 選択した row group と行の順序を保存する。一つの選択 row group の宣言済み上限が予算に収まらないと、page decode 前に拒否する。page 単位への分割はしない。 |
| V08 columnar segment | embedded の直接 API | `Database::stream_columnar_segment_v08` の直接 API | provision 済みの range-addressable V08 segment だけが対象。DataFrame `LazyFrame` の scan source にはまだ接続していない。 |
| 旧 V2 columnar segment | 既存互換経路 | 非対応 | pseudo-streaming は行わない。streaming 要求は `requires_v08_chunked_layout` で拒否する。 |
| in-memory `DataFrame` scan | 対応 | 非対応 | 既に materialize 済みの legacy source なので `legacy_materialized_source` で事前拒否する。eager executor への fallback は行わない。 |

CSV/Parquet の schema はソース生成前には既知でない。したがって、file scan を含む `LazyFrame::concat` の不一致は bounded preflight で `concat_schema_mismatch` となる。既知 schema の input だけから成る concat は計画構築時に同じ不一致を報告する。どちらも暗黙の型変換・列並べ替えは行わない。

## 演算の対応範囲

| 計画 / 式 | bounded / incremental | 境界 |
| --- | --- | --- |
| `concat` | 対応 | child を宣言順に一つずつ開く。全 child schema は最初の結果より前に照合する。 |
| `select`, `with_columns`, `filter` | 対応 | column、literal、alias、単項/二項式、`concat_str`、projection 内 wildcard のみ。predicate は source reader ではなく budget 済み batch operator として実行する。 |
| `head` / 前方 `slice` | 対応 | 前方に進む有限 slice のみ。 |
| `concat_str` | 対応 | `Propagate`、`Ignore`、`Replace(value)` の null 方針を指定する。少なくとも二入力が必要で、入力は UTF-8 でなければならない。 |
| 同一式の再利用 | 対応 | 一 batch の評価 scope 内だけで reuse する。別 batch・別 query・別 stream には状態を保持しない。 |
| namespace `Expr::Function` | 非対応 | 個別の allocation upper bound が未検証のため `operator_not_installed` で事前拒否する。 |
| group-by / aggregate、join、sort、unique、tail、後方 slice、null count、implode | 非対応 | global materialization が必要なため `streaming_requires_materialization` として事前拒否する。 |
| fill/drop null、explode | 非対応 | v0.8 の batch-at-a-time operator が未実装のため事前拒否する。 |

対応外の計画は、ソースを開いたあとで eager 実行へ切り替えない。`collect_streaming` / `collect_with_options` は構造化エラーを返し、通常 `collect` を使うか、bounded algorithm が追加されるまで待つ必要がある。

## 所有権・終了・エラー

streaming path は allocation を所有する前に resource reservation を取る。source/decode reservation は演算時に、演算 reservation は出力 batch への引き渡し時に、それぞれ移譲または解放される。`DataFrameStream::next_batch()` が返した `DataFrame` は consumer 所有であり、その保持量は stream budget には含まれない。`collect_with_options` はこの保持も `MaterializedOutput` として予約する。

- 通常終端後は `next_batch()` が繰り返し `Ok(None)` を返し、状態は `Exhausted` のままである。
- `close()` は idempotent。以後の `next_batch()` は常に `StreamClosed` を返す。
- `cancel()` は idempotent。以後の `next_batch()` は常に `StreamCancelled` を返す。
- source/decode/schema/resource の失敗は最初の分類と code を terminal state に固定し、以後も同じ構造化失敗として再現する。

利用者が途中で処理を止める場合は `cancel()` または `close()` を呼ぶ。drop 時にも open source は閉じられるが、明示的な終了のほうがライフサイクルを明確にする。

## 検証範囲

`streaming_differential` integration test は CSV と Parquet で、通常 `collect`、`collect_with_options`、逐次 `collect_streaming` の filter / projection / `concat_str` 結果を比較する。また、deferred CSV concat の入力順を同じ三モードで比較する。`streaming_contract` integration test は terminal 再消費、close/cancel、予算解放、in-memory source の事前拒否、deferred concat schema mismatch を検証する。V08 と旧 V2 の source-specific 契約は `alopex-embedded` の columnar streaming test で検証する。
