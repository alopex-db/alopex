# 単一 `.alopex` ファイルへの収束（v0.8.8）

対象 issue: #178「storage: 安定後に `.alopex` 単体へ収束しない（単一最終ファイル方針の未達）」

## 1. 契約と、v0.8.7 までの実態

`docs-public/tech/file-format-comparison.md` の「Alopex 単一最終ファイル方針」は次を約束している。

| 行 | 契約 |
|---|---|
| L61 | ロールフォワード/再起動で不要になった `.wal` は truncate/削除して最終状態を単一 `.alopex` に回収する |
| L67 | 稼働中は WAL + 現行 `.alopex` の2本立て、**安定後は `.alopex` 単体で完全状態** |
| L69 | MANIFEST 相当を `.alopex` footer に包含する設計のため、別ファイルの MANIFEST は不要 |
| L75 | 安定状態の `.alopex` をそのままスナップショットとして配布 |

`docs-internal/specs/lsm-tree-file-mode-spec.md` §10.1 も「『単一ファイル』とは**最終状態**を指す」と定義している。

v0.8.7 までの実態は契約を満たしていなかった。

- `Database::open("mydb.alopex")` は `mydb.alopex.d/` サイドカー（`lsm.wal` + `sst/` + `checkpoint.meta`）だけを作り、`mydb.alopex` 本体は**書かれない**。
- `Database::persist_to_disk()` だけが `mydb.alopex` を作るが、それは `create_new` で開くだけの **0 バイトの存在確認マーカー**だった。
- 起動側も `.alopex` を読まない（ヘッダ 64 バイトをバージョン表示のために覗くだけ）。したがって `.alopex` 単体をコピーしても復元できなかった。
- `LsmKV::flush()` は Active MemTable を freeze して immutable キューに積むだけで、SSTable を 1 バイトも書いていなかった。SSTable を書くのは `checkpoint()` 経由の `persist_immutable_memtables()` だけで、それは admin API / backup coordinator からしか呼ばれず、embedded 経路では一度も走らなかった。

## 2. v0.8.8 の到達点

**収束点（converge）で作業領域の全 SSTable と manifest を統一コンテナ形式 `.alopex` へアトミックに書き出す。** 新フォーマットは発明していない。`storage::format` の `AlopexFileWriter` / `AlopexFileReader` / `FileHeader` / `SectionIndex` / `FileFooter` はすでに出荷済みで、そこに載せるだけで契約 L69 まで満たせる。

### 物理レイアウト

```text
[FileHeader 64B  : "ALPX" | FileVersion::CURRENT | Crc32 | Snappy | flags]
[Section 0..N-1  : SectionType::SSTable,  CompressionAlgorithm::None, `<id>.sst` の生バイト]
[Section N       : SectionType::Metadata, CompressionAlgorithm::None, LsmManifest(bincode)]
[SectionIndex    : 4B count + SectionEntry(40B) * count]
[FileFooter 64B  : ... | wal_sequence_number = converged_lsn | footer_crc32 | "XPLA"]
```

`LsmManifest`（`crates/alopex-core/src/lsm/container.rs`）は manifest 版数、`converged_lsn` / `next_timestamp`、SSTable/WAL の形式バージョン、および各 SSTable の `file_id` / `level` / `section_id` / `size_bytes` / `entry_count` / キー範囲を持つ。

### 収束の発火点

| 経路 | 挙動 |
|---|---|
| `Database::flush()` | MemTable → SSTable 化 → converge。稼働中なのでサイドカーは残す（契約 L67 前半の「2本立て」） |
| `Database::converge()` | 明示収束。エラーは呼び出し元に返る |
| `Database::close()` | 収束してハンドルを閉じる。冪等。エラー可視の正規経路 |
| ハンドルの `Drop` | ベストエフォート収束 + サイドカー削除。issue の再現手順（open → commit → 正常終了）がここしか通らないため必須 |

### 起動時の判定（1 行ルール）

```
X.alopex.d/lsm.wal が存在する → サイドカーが正（現行どおり WAL リプレイ）
存在しない かつ X.alopex が有効なコンテナ → コンテナから rehydrate
それ以外 → 新規/現行どおり
```

## 3. 設計裁定（D 番号）

- **D1**: 収束方式は「作業領域は `.alopex.d/` のまま、converge で既存の統一コンテナ形式へ SSTable 実体 + manifest を書き出す」。出荷済みの writer/reader（magic / version / section CRC / footer CRC / atomic rename）をそのまま使えるため、新フォーマットの設計・検証コストがゼロ。
- **D2**: v0.8.8 では「`.alopex` を常時正本にして in-place 書き込みする」方式は採らない。spec §10.4/§10.5 が要求する WAL のセクション内リングバッファ化・`FreeSpaceManager` による in-place compaction・SSTable のオフセット窓読み出しを同時に作ることになり、1 リリースに収まらない。契約の「安定後は `.alopex` 単体で完全状態」を先に満たし、残りを別 issue に切る。
- **D3**: SSTable セクションは `CompressionAlgorithm::None` で `<id>.sst` のバイト列をそのまま格納する。SSTable は既にブロック単位で圧縮済み（既定 LZ4）なので二重圧縮に意味がなく、バイト同一なら rehydrate が単純コピーで済み `SSTableReader` / `SSTableCursor` / `BufferPool` / `load_sstable_levels` を無改造で使える。加えてセクション先頭オフセットが実 SSTable 先頭と一致するため、将来の in-place 読みへ形式変更なしで移行できる。**キー範囲は writer に登録しない**（L0 SSTable は範囲が重なるのが正常で、`validate_key_range` が `KeyRangeOverlap` で弾いてしまう）。範囲は manifest 側だけが持つ。
- **D4**: `.alopex` 単体からの完全復元は 4 層検証で保証する。(1) `FileHeader` magic `ALPX` と `check_compatibility()` による前方互換拒否、(2) `FileFooter` の逆 magic `XPLA` + footer CRC32、(3) 各 `SectionEntry.checksum` を rehydrate 時に照合、(4) `LSM_MANIFEST_VERSION` を持つ Metadata セクション。**SSTable セクションが 1 つ以上あるのに manifest が読めない場合は空 DB として開かずエラーにする** —— 空 DB に化けるのが最悪の失敗モード（利用者にはデータ全消失に見える）だから。セクション 0 個のコンテナだけを「空 DB」として許容し、既存の v0.1 互換フィクスチャを保つ。
- **D5**: 復元時の MVCC クロックは manifest の `converged_lsn` から復元する。rehydrate は `sst/*.sst` を展開したあと `checkpoint.meta` を `converged_lsn` で書き、`WalWriter::create(..., converged_lsn + 1)` で空 WAL を作って既存の WAL 復旧ブランチに合流させる。あわせて WAL 非存在ブランチの `next_ts = 1` 決め打ちを `load_checkpoint_meta` 参照に修正した（既存の潜在バグ）。
- **D6**: 発火点は「明示 `converge()` / `close()` / `flush()` + `Drop` でのベストエフォート」の 4 点。`Drop` はエラーを返せないので `close()` / `converge()` をエラー可視の正規経路として用意し、`Drop` 側は `std::thread::panicking()` とロックの poisoned 判定で握り潰して二重パニックを絶対に起こさない。dirty フラグ（commit で立てる）で無変更時は完全 no-op。
- **D7**: クラッシュ時はサイドカーが常に正。converge 前に落ちた場合 `X.alopex` は古いか存在しないが `X.alopex.d/`（WAL + sst + checkpoint.meta）は無傷なので、復旧は現行と 1 バイトも変わらない。判定は `X.alopex.d/lsm.wal` の有無 1 点のみ。
- **D8**: サイドカーの削除（prune）は `Drop` でのみ行い、稼働中の `close()` では行わない。`get_visible_at` / `open_owned_sstable_cursors` は `sst/<id>.sst` を都度 open するため、生きているハンドルの背後で `sst/` を消すと読み出しが壊れる。削除順は `lsm.wal` → `checkpoint.meta` → `sst/` → `.d/` 本体に固定し、prune 途中でクラッシュしても D7 の判定で死んだサイドカーとして安全に破棄・再 rehydrate される。
- **D9**: converge の適用範囲は `ConvergePolicy::SidecarOnly`（既定）でスコープを絞る。data_dir が `X.alopex.d` 形式のときだけコンテナ `X.alopex` を導出して収束する。`alopex-server` は素のディレクトリを渡すので**サーバ挙動と既存 core テスト群は完全に無変更**。`Never` / `Always { container }` も用意した。
- **D10**: 既存 0.8.x レイアウトの読み取り互換は 100% 維持する。WAL / SSTable / checkpoint.meta のオンディスク形式は一切変更していない。移行スクリプトもデータ変換も不要。
- **D11**: `Database::persist_to_disk()` は 0 バイトマーカー生成をやめて本物のコンテナを書く。tmp データディレクトリに書いたあと `ConvergePolicy::Always` で `wal_path` にコンテナを生成し、その後 tmp → data_dir を rename する。tmp ディレクトリ名は `X.alopex.d.tmp` に変更した（`X.alopex.tmp` はコンテナ writer のステージングファイルが先に使うため衝突する）。
- **D12**: 先行して既存 2 バグを直した。(a) `flush_inner()` の `pop_front()` は未永続化の immutable MemTable を黙って捨てていた（`max_immutable_count` を超えると確定的にデータ消失）。converge が WAL を truncate するようになると致命的なので、「front を SSTable に永続化してから pop」に変更した。(b) `LsmKV::flush()` を freeze のみから「freeze + SSTable 永続化」に変更し、`Database::flush()` の doc comment（"Flushes the current in-memory data to an SSTable on disk"）と実装を一致させた。
- **D14**（実装中に判明したため計画に追加）: 点読み出しの SSTable アクセスを「キー範囲での枝刈り + リーダーのキャッシュ」に直した。`SSTableReader::open` は**ファイル全体を読み直して CRC32 を検証する**のに、`get_visible_at` / `owned_visible_at` は **キーごと・テーブルごとに開き直していた**。つまり 1 回の `get` が「生きている全 SSTable の総バイト数」に比例するコストだった。v0.8.7 まではこれが顕在化しなかった——`flush()` が SSTable を 1 つも作らなかったので `levels` が空のままだったからで、D12(b) で `flush()` が本当に SSTable を書くようになった瞬間に露呈した（`lsm::integration::crud::large_crud_roundtrip_with_reopen` が 1M キーの挿入フェーズだけで CPU 731 秒を使っても終わらなくなった）。対策は 2 つとも標準的な LSM の手法: (1) `SSTableMeta.key_range` に含まれないキーは開く前にスキップ、(2) `SSTableReaderCache`（file_id → `Arc<Mutex<SSTableReader>>`、上限 256 で fd を保護）で CRC 検証を 1 テーブル 1 回に減らす。SSTable は書き込み後は不変で file_id も再利用しないため、キャッシュしたリーダーが陳腐化することはない。修正後、同テストは 1M キーで完走し core lib 全体が 83 秒で緑になる。

- **D13**: スコープ外として follow-up に切り出す 4 件。(A) コンテナ内 SSTable セクションの in-place 読み出し（`SSTableReader::open_window(path, offset, len)` で rehydrate のコピーを廃止し、open/close の O(N) を消す）、(B) WAL を `.alopex` 内セクション化（spec §10.4 の完成形）、(C) `FreeSpaceManager` を使った in-place compaction とコンテナの増分更新、(D) WASM から `.alopex` を read-only で直接開く経路と多重オープン防止のロックファイル。

## 4. 後方互換マトリクス（データ変換なし）

| 既存レイアウト | v0.8.8 の挙動 |
|---|---|
| `X.alopex.d/` + 0 バイト `X.alopex`（v0.8.6） | コンテナは `HEADER_SIZE + FOOTER_SIZE` 未満なので「マーカー」と判定して無視 → サイドカー優先で現行どおり開く。最初のクリーン close で本物のコンテナに昇格する |
| `X.alopex.d/` のみ | 同上 |
| 素のディレクトリ（`Database::open("./mydb")`、`alopex-server`） | `ConvergePolicy::SidecarOnly` により converge 対象外。ディレクトリの中身は 1 バイトも変わらない |
| 有効な空コンテナ（v0.1 互換フィクスチャ） | セクション 0 個 → 空 DB として rehydrate |
| `X.alopex` のみ（新規・配布物） | rehydrate して開く ← **本 issue の目標** |

## 5. 既知のコストと制限

- **converge / rehydrate は O(N)**。converge は生きている SSTable の全バイトをコンテナへコピーし、rehydrate は逆方向に展開する。10 GB の DB では close も open も 10 GB のシーケンシャル I/O になり、converge 中は一時的にディスク使用量が 2 倍になる。`converge_count` / `converge_bytes_written` / `converge_duration_ms` / `rehydrate_bytes_read` メトリクスで可視化し、1 GiB 超の収束は `warn!` を出す。解消は D13(A)。
- **`Database::flush()` のコスト特性が変わった**。「MemTable の freeze」だけだったものが「freeze + SSTable 書き出し + converge」になる。Python の `db.flush()` にも波及する。
- **多重オープンはロックで拒否される**（D13(D) 解消済み / issue #181）。データディレクトリを開くとき `X.alopex.lock`（サイドカー形状）または `<data_dir>/.alopex.lock`（素のディレクトリ）に OS 排他ロックを取り、二重オープンは `Error::AlreadyOpen` で失敗する。ロックはサイドカーの**外**に置くため、`prune_sidecar()` の `remove_dir_all` と寿命が衝突しない。詳細と裁定は [docs/single-process-lock.md](single-process-lock.md)。
- **WASM では収束しない**。`storage::format::writer` が native 専用のため、WASM ターゲットではポリシー型だけを共有し `converge()` は MemTable の SSTable 化のみを行う。
