# 単一プロセス強制とデータディレクトリロック（v0.8.8）

Alopex のデータディレクトリは、**同時にただ 1 つのプロセスからしか開けない**。
v0.8.8 からは、この前提が規約ではなく OS レベルのロックで強制される。

対象 issue: #181。複数プロセスで書ける仕組みを作る方向は別 issue #183（v2.0）。

## 1. なぜ強制が要るのか

「単一プロセス前提」は v0.8.7 まで実装の暗黙の前提でしかなく、破っても
**エラーも警告も出なかった**。同じ `data_dir` を 2 プロセスで開くと次の 3 系統が同時に壊れる。

| 壊れる箇所 | 仕組み |
| --- | --- |
| WAL リング | `WalWriter` は固定長リングを `set_len(wal_section_size)` で先に確保し、**メモリ上の** `logical_offset` から `ring_logical_to_physical()` で求めた物理位置へ `seek` して書く。各プロセスが独立した `logical_offset` を持つため、同じ物理バイトへ書き込んで後勝ちで上書きする |
| SSTable 採番 | `next_sstable_id` はプロセス内 `AtomicU64`。別プロセスは同じ番号を再利用して既存 `sst/<id>.sst` を上書きする |
| サイドカー寿命 | #178 の `prune_sidecar()` / `discard_dead_sidecar()` は `remove_dir_all`。片方の Drop がもう片方の生きているサイドカーを丸ごと消す |

再現は「組み込み同士」「サーバー稼働中の `data_dir` を組み込みで開く」「CLI `--data-dir`
でサーバーの `data_dir` を指す」のいずれでも起きた。HTTP 経由で書いた行が同時に開いた組み込み側から
見え、片方の書き込みが失われる。

### WAL の append 化について

issue #181 本文は「WAL が `OpenOptions::append(true)` でないのが原因」と書いているが、これは不正確である。
`WalWriter::create` は固定長リングを事前確保し、`write_ring` はセグメント境界で `WalSegmentHeader` を
書きながら任意の物理位置へ `seek` する。`O_APPEND` はラップアラウンド・セグメントヘッダ・
`advance_start` / `truncate_tail_to` のすべてと原理的に両立しない。append 化は「まだやっていない改善」
ではなく「この形式では取れない選択肢」であり、本 issue では行わない（**D11**）。

## 2. 何が起きるか

同じデータベースを 2 つ目のハンドルで開くと、開く側が
`Error::AlreadyOpen`（Python では `AlopexError`、CLI では非ゼロ終了）で失敗する。
メッセージには安定した検索可能文字列 `already open by another process` が含まれる。

```
data directory ./data is already open by another process
(pid=48213 host=box exe=/usr/local/bin/alopex-server started_ms=1755...);
an Alopex database can only be opened by one process at a time —
share it through alopex-server instead (lock file: ./data/.alopex.lock)
```

保持者情報（`pid=... host=... exe=...`）はロックファイル本文のベストエフォート読み出しであり、
**排他の根拠ではない**。読めない場合は `holder=unknown` に degrade する（Windows では通常こうなる。§6）。

## 3. 共有したいとき: サーバー経由

1 つのデータベースを複数のプロセス・複数のマシンから使いたい場合は、`alopex-server`
を 1 つだけ起動し、そこへ HTTP / gRPC で接続する。サーバーがそのデータディレクトリの
唯一の書き手になる。

```bash
# データディレクトリを所有するのはこのプロセスだけ
alopex-server --config alopex.toml     # data_dir = "./data"

# 他のプロセスはサーバー経由で読み書きする
alopex --profile prod sql "SELECT * FROM users"
curl -X POST http://127.0.0.1:8080/v1/sql -d '{"sql":"SELECT 1"}'
```

やってはいけないのは、サーバーが握っている `data_dir` を CLI や組み込みから直接開くことである。

```bash
# NG: サーバーが ./data を開いている間はこれが失敗する（v0.8.7 までは黙って通り、壊れた）
alopex --data-dir ./data sql "SELECT 1"
```

CLI をローカルの組み込みモードで使いたいときは、サーバーとは**別の**ディレクトリを指すこと。

## 4. ロックファイルの位置

ロックファイルの位置は #178 の「収束後は `X.alopex` 単体」という契約と両立させる必要がある。

| データディレクトリ | ロックファイル |
| --- | --- |
| `mydb.alopex.d`（サイドカー形状） | `mydb.alopex.lock`（コンテナの**隣**。サイドカーの外） |
| `./data`（素のディレクトリ / サーバー） | `./data/.alopex.lock` |
| `ConvergePolicy::Always { container }` | `<container>.lock` |

サイドカー形状でロックを**外側**に置くのは必須の不変条件である（**D3**）。内側に置くと:

- Windows では自分が開いたままのロックファイルのせいで `prune_sidecar()` の
  `fs::remove_dir_all` が失敗し、#178 の「収束後は `.alopex` 単体」が壊れる。
- Unix では prune が `unlink` した後もこちらは古い inode をロックしたままになる。
  `flock` は名前ではなく inode に付くので、次のプロセスが同じパスに新しいファイルを作って
  ロックに成功してしまう。**二重ライタが素通りする穴**が開く。

素のディレクトリは core が中身を消さない（`ConvergePolicy::SidecarOnly` では
`container_path` が `None` になり `Drop` が即 return する）ので、内側で安全。

不変条件は 1 行で言える: **prune が走るのは `container_path` が `Some` のときだけで、
そのときロックは必ずサイドカーの外にある。** `lsm::lock` の対応表テストがこれを固定している。

## 5. 設計裁定（D 番号）

- **D1**: ロック方式は `std::fs::File::try_lock()`（Unix = `flock(LOCK_EX|LOCK_NB)`、
  Windows = `LockFileEx(LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY)`）をハンドルの
  生存期間ずっと保持する方式とし、PID ファイル + stale 判定は採らない。
  どちらもプロセス終了時にカーネルが fd/ハンドルを閉じてロックを解放するため、
  「異常終了でロックが残らない」を**ヒューリスティクスなしで無条件に**満たせる。
  PID 方式は PID 再利用・コンテナ内 PID 名前空間・`kill(pid,0)` の権限差で誤判定が避けられない。
- **D2**: 新規 crate 依存はゼロ。`try_lock` / `TryLockError` は
  Rust 1.89.0 で安定化済みで、CI がピンする 1.90.0 に存在する。fs2 / fs4 / libc / windows-sys は不要。
- **D3**: ロックの粒度は「論理データベース 1 個」。パスは §4 の表のとおり
  `lsm::lock::lock_path_for(data_dir, policy)` が決める。サイドカー形状は必ず外側。
- **D4**: `lock_path_for` は `ConvergePolicy::Never` でもサイドカー形状を解決する。
  同じ DB を `Never` で開いたプロセスと `SidecarOnly` で開いたプロセスが別々のロックファイルを
  掴んで排他が成立しない穴を塞ぐため。`Always { container }` だけは `container + ".lock"` を使う
  （`persist_to_disk` は `X.alopex.d.tmp` に書くが実体の宛先は `X.alopex` なので、宛先側をロックするのが正しい）。
- **D5**: ロック取得は `LsmKV::open_with_config` の最初期、`restore_from_container()` より**前**。
  rehydrate は `discard_dead_sidecar()`（`remove_dir_all`）と `container::rehydrate()`
  （sst/ 展開・`checkpoint.meta` 書き込み・空 WAL 作成）という破壊的操作で、
  ここが保護されないと「`X.alopex` 単体を 2 プロセスが同時に開いて互いの rehydrate 中間状態を消す」
  という #178 由来の新しい壊れ方が残る。
- **D6**: ロックは `close()` では解放せず、ハンドルの Drop でのみ解放する。`close()` は
  `closed` フラグを立てて converge するだけでストアは書き込み可能なままなので、ここで解放すると
  「close 済みハンドルがまだ書けるのに別プロセスも開ける」状態を作る。
  `DirectoryLock` は `LsmKV` の**最終フィールド**に置き、`Drop::drop` 本体（converge + prune）の
  実行中もロックが効いていることを保証する。
- **D7**: 読み取り専用の同時オープンは v0.8.8 では**許可しない**。取得は常に排他。
  根拠は現行コードに read-only オープン経路が存在しないこと: `WalWriter::open` は
  `read(true).write(true)`、WAL 不在なら `WalWriter::create` で新規作成、リカバリが途中停止すれば
  `truncate_tail_to` で書き込み、Drop で converge + `prune_sidecar`。
  つまり「読むだけのオープン」は今のところ無く、共有ロックを許すと読者が書き手のサイドカーを消す。
  到達不能な shared-lock 実装も先行して持たず、現時点のコードは排他取得だけに限定する。
  **再開条件**: (a) WAL を replay するが書き戻さない read-only オープン経路、(b) Drop で
  converge / prune しないハンドル、(c) 読者から見た SSTable の削除に対する保護、の 3 つが揃った時点で
  shared lock を read-only open と同時に設計・実装する。
- **D8**: ロックファイルは解放時も削除しない。削除すると「A が unlink → B が同名の新 inode を作って
  ロック → C も新 inode をロック」という取り違えレースが生じる。残置される
  `X.alopex.lock` / `.alopex.lock` は排他状態を持たない**不活性な痕跡**であり、
  配布物は従来どおり `X.alopex` 1 本だけコピーすればよい。
- **D9**: エラーは `alopex_core::Error::AlreadyOpen { path, lock_path, holder }`。Display に
  安定した検索可能文字列 `already open by another process` を含める。
  上位クレートへは既存の `#[from]` 連鎖で伝播する。
- **D10**: Windows では保持者診断が読めない。`std` の Windows 実装は
  `LockFileEx(handle, flags, 0, u32::MAX, u32::MAX, ...)` でファイル全域を強制ロックするため、
  敗者側の `ReadFile` が `ERROR_LOCK_VIOLATION` になる。診断は `holder=unknown` に degrade し、
  パスと単一プロセス規約の説明だけを出す。ロック取得手順は
  `create(true).read(true).write(true)` を **truncate なし**で開く → `try_lock()` →
  勝者のみ `set_len(0)` + 診断書き込み、とし、敗者が勝者のメタデータを潰さないようにする。
- **D11**: 本 issue のスコープは「ロックによる単一プロセス強制」のみ。WAL の append モード化は
  行わない（§1 の理由）。SSTable 採番のディレクトリ全体一意化も行わない —
  仮に一意化しても各プロセスが独立した `levels` と manifest を持ち、`converge()` が `X.alopex` を
  丸ごと上書きし、Drop の `prune_sidecar` が相手のサイドカーを消すので、多重ライタは成立しない。
  それは #183 / v2.0 の仕事。
- **D12**: ロックを無効化する設定は設けない。外部オーケストレータが単一プロセスを保証していても
  Alopex 自身のロックは無害であり、opt-out は issue #181 の破損を public API から再び有効化してしまう。
- **D13**: in-memory（`Database::open_in_memory` / `Database::new` / `AnyKV::Memory`）はパスを持たない
  のでロック対象外、挙動は完全に不変。WASM では `acquire` を no-op にする
  （`restore_from_container` / `Drop for LsmKV` がすでに同じ分岐をしている）。
- **D14**: 同一プロセス内の二重オープンも拒否される（`std` の doc:
  「Returns `Err(TryLockError::WouldBlock)` if a different lock is already held on this file
  (via another handle/descriptor)」）。これは副作用であって目的ではないが、
  ハンドルのリークを表面化させる有用な性質でもある。
- **D15**: ロックファイルは配布物・バックアップ・S3 同期から除外する。除外しないと
  `restore` がサーバー稼働中に自分のロックファイルを消し（Windows では逆に削除が失敗し）、
  S3 には他ホストの pid が書かれたゴミが上がる。
  `alopex-server` の `copy_dir_filtered` / `clear_data_dir` と
  `alopex-core` の `S3KV::collect_files_recursive` の 3 箇所で除外する。
- **D16**: サーバーの `RecoveryCoordinator::open_store` は初回 open 失敗時に WAL を隔離して
  再オープンするが、失敗が `AlreadyOpen` のときは**隔離せず即座に返す**。
  他プロセスが使用中の生きた WAL を `lsm.wal.bad.<ts>` へ rename してしまうと、
  ロックで防ごうとしていた破壊そのものを起動シーケンスが引き起こすため。

## 6. 制限

- **共有ファイルシステム（NFS / SMB）では保証しない**。Linux の NFS は 2.6.12 以降
  `flock` を fcntl レコードロックへエミュレートするが、`local_lock` マウントオプションや
  NLM 無効の環境ではノードローカルになり、**ホストをまたいだ二重オープンを止められない**。
  ネットワークファイルシステム上のデータディレクトリを複数ホストから開かないこと。
- **Windows では保持者情報が出ない**（D10）。エラーの本文は
  `already open by another process (unknown)` になる。パスと対処方法は表示される。
- **`close()` はロックを解放しない**（D6）。同一プロセスで同じパスを開き直すには、
  ハンドル（および `Arc` 等の全参照）を drop する必要がある。
  Python の `db.close()` も同様で、生きている `Transaction` / `Cursor` が 1 つでもあると
  ストアが解放されず再オープンに失敗する。
- **書き込み不可のディレクトリでは open が失敗する**。ロックファイルを作れないため。
  従来も WAL 作成で失敗していたので機能的な後退はないが、エラーの種類が変わる。
  なお、ロック作成の I/O エラーは `Error::AlreadyOpen` ではなく `Error::Io` として素通しする
  （原因の取り違えを防ぐため）。
- **新しいオンディスク成果物が増える**。`X.alopex.lock` / `.alopex.lock`。
  「ディレクトリの中身がちょうど `X.alopex` 1 個」を仮定するツールは更新が要る。

## 7. 回帰テスト

- `crates/alopex-core/src/lsm/lock.rs`（単体）: `lock_path_for` の対応表（D3/D4 の穴を固定）、
  二重 `acquire` が `AlreadyOpen` になること、drop で解放されること、
  敗者の open が勝者の診断を truncate しないこと（D10）、
  残置ロックファイル単体では排他されないこと。
- `crates/alopex-core/tests/single_process_lock.rs`: 素のディレクトリ / サイドカー形状 /
  `Never` ポリシー / `close()` 後 / rehydrate 前（D5）の各経路。
- `crates/alopex-embedded/tests/single_process_lock.rs`: 組み込み同士（同一プロセス・**別プロセス**）、
  **`SIGKILL` されたプロセスのロックが残らないこと**、in-memory が無関係であること。
- `crates/alopex-server/tests/single_process_lock.rs`: サーバー ↔ 組み込みの両方向、
  サーバー解放後の再オープン、`AlreadyOpen` 時に WAL が隔離されないこと（D16）。
- `crates/alopex-cli/tests/single_process_lock.rs`: CLI ↔ サーバー、CLI 同士。
