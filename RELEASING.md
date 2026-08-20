# Alopex リリース手順書

## 概要

このドキュメントは Alopex プロジェクトのリリース手順を説明します。

## バージョン管理

### Workspace 継承

全クレートのバージョンは `Cargo.toml` の `[workspace.package]` で一元管理されています。

```toml
[workspace.package]
version = "X.Y.Z"  # 唯一の release version 入力
```

### クレート一覧

| クレート | 説明 | 依存関係 |
|---------|------|---------|
| `alopex-core` | コアストレージエンジン | なし（最初に公開） |
| `alopex-sql` | SQL パーサー | alopex-core |
| `alopex-dataframe` | DataFrame エンジン | alopex-core |
| `alopex-embedded` | 組み込みDB インターフェース | alopex-core, alopex-sql, alopex-dataframe |
| `alopex-cluster` | クラスタメタデータ契約（cluster-aware foundation） | なし（chirps は optional feature） |
| `alopex-server` | サーバーコンポーネント | alopex-core, alopex-sql, alopex-cluster |
| `alopex-cli` | CLI ツール | alopex-core, alopex-embedded |
| `alopex-tools` | 開発ツール | （crates.io 非公開） |

### 公開順序

依存関係により、以下の順序で公開する必要があります（`.github/workflows/release.yml` の `publish-crates` ジョブと一致させること）：

```
alopex-core → alopex-sql → alopex-dataframe → alopex-cluster → alopex-embedded → alopex-server → alopex-cli
```

## 現行リリース契約

対象版は `Cargo.toml` の `[workspace.package].version` からのみ取得する。
main にレビュー済みの同一コミットを取り込み、そのコミットを annotated
tag `v${version}` で公開する。RC は `rc/v${version}` を使用し、
未公開のローカル生成物や合成コミットを公開入力にしてはならない。
個別の workflow や script に対象版を直書きしない。

### パーサー資産の不変条件

- Alopex SQL parser の FFI 契約は `0.12.0` とする。この識別子は Alopex
  リリース内の互換メタデータであり、独立した parser リリース lane ではない。
- Linux x86_64、macOS x86_64、macOS arm64、Windows x86_64 の4ターゲットを
  Nim 2.2.10 / Nimble 0.22.3 で個別にビルドし、各ターゲットの native smoke
  と SHA-256 検証を通過させてからアップロードする。
- `parser-vendor-manifest.json` は追跡済みの入力を記録し、公開後に
  `parser-assets-v${version}.json` がタグの peeled SHA、マニフェスト、アーカイブ、
  ライブラリ digest を同一性付きで束ねる。手作業コピーや別ビルドで補っては
  ならない。
- Python 配布物は公開済み Alopex parser 資産を取得して package-local
  `alopex/native/` に配置する。Python ジョブで Nim を再ビルドせず、任意の
  外部ライブラリディレクトリをローダー入力にしない。
- Rust向けcrate資産は各targetの静的parser archive（Linux/macOS `.a`、Windows
  MSVC `.lib`）を同じtarget recordとSHA-256で検証し、`#[link(kind = "static", modifiers = "+bundle")]`
  で`alopex-sql`へ取り込む。共有parser libraryはPython wheel専用であり、公開版
  `alopex-embedded`の実行に`LD_LIBRARY_PATH`や利用者側rpathを要求してはならない。
- 追跡済み v0.8.4 / contract-0.4.0 vendor bytes と sidecar は履歴資産として
  書き換えない。release staging だけが新規4ターゲットの contract-0.12.0
  archive から library/sidecar/manifest を展開し、`build_support.rs` の Alopex
  version と manifest SHA pin を同時に更新する。

### 公開順序と失敗時の扱い

1. clean な peeled tag から core crate と parser 資産を一度だけ生成する。
2. core crate を依存順に公開し、公開済みの同じ parser envelope を検証する。
3. core の公開成功を確認してから Python wheel/sdist を公開する。
4. 途中失敗時は既存のタグ・版・バイト列を削除・再作成せず、同じ識別子を
   使った修復を行う。

ゲートが不合格なら公開を開始しない。生成物、Cargo target、Nimble cache、
hook 用 target は invocation 所有範囲を記録し、完了後にその範囲だけを削除する。
Umbrella 全体と各 target/cache の合計は常に 50 GiB 以下であることを `du -sb`
で確認する。

## リリースワークフロー

### タグ形式

| プロジェクト | タグ形式 | 例 |
|-------------|---------|-----|
| alopex | `v{major}.{minor}.{patch}` | `vX.Y.Z` |

### v0.7.0 リリース契約

v0.7.0 は single-node compatible cluster-aware release として扱う。タグは
すべての実装タスク、release notes、v0.7 release gate が完了した後、release
branch から `main` へマージされた commit にだけ作成する。

安定仕様:

- 既定の Embedded / Server / SQL / DataFrame / Python surface は v0.6 互換の
  single-node behavior を維持する。
- `alopex-cluster` は node identity、membership lifecycle、placement metadata、
  routing diagnostics、cluster status schema を所有する。
- Server / CLI / Python は同じ cluster status schema を観測する。
- Query Router は live database では `local_only` または
  `future_distributed_execution_required` を返す。production remote execution は
  実行しない。
- DataFrame P3 は string / datetime / list namespace primitives を提供する。

Migration contract:

- cluster-aware mode は明示設定された場合だけ有効になる。
- v0.7 metadata initialization / upgrade は再実行しても安全であることを gate で
  検証する。
- v0.8 は v0.7 の metadata / status / routing contracts に Metadata Raft と
  Raft DDL を接続する。
- v0.9 は v0.7 の logical shard/range model と routing target contract に
  Multi-Raft、distributed transaction、Changefeed を接続する。

Out of scope for v0.7.0:

- production remote scatter-gather execution
- Raft-backed metadata consensus
- distributed transactions
- alopex-py Client / Transaction / ConnectionPool API

### v0.7.0 Taggable Readiness

以下を満たすまで `v0.7.0` tag を作成しない。

```bash
bash scripts/release/v07_gate.sh
```

`scripts/release/v07_gate.sh` は以下を集約する。

- v0.6 baseline release gate
- workspace fmt / clippy
- `alopex-cluster` cluster metadata / router / simulated harness tests
- Embedded and Server v0.6 compatibility regressions
- Server routing and cluster status cross-surface tests
- CLI cluster status fixture projection
- DataFrame P3 tests
- alopex-py Rust tests and Python pytest surface checks
- release workflow contract checks
- release CLI binary smoke build

ローカルで workflow wiring だけを確認する場合:

```bash
bash scripts/release/v07_gate.sh --workflow-contract-only
```

### v0.7.0 Release Completion

Taggable Readiness を満たした後、以下を完了した時点で v0.7.0 release を完了と
する。

1. release branch から `main` への PR が merge 済みである。
2. `main` の merged commit に annotated tag `v0.7.0` を作成し push 済みである。
3. GitHub Actions の release workflow が成功している。
4. GitHub Release に以下の CLI artifacts が存在する。
   - `alopex-linux-x86_64`
   - `alopex-macos-x86_64`
   - `alopex-macos-aarch64`
   - `alopex-windows-x86_64.exe`
5. release 後の branch cleanup を実行した、または保持理由を `CHANGELOG.md`、
   release PR、または release note に記録した。

Branch cleanup record の最小記録項目:

- release branch 名
- 削除したか、保持したか
- 保持した場合の理由と owner
- hotfix branch が残る場合は branch 名と終了条件

### 自動化される処理

タグをプッシュすると、GitHub Actions が以下を自動実行します：

1. **Approved source evidence**: tag の peeled SHA と、同一 SHA の成功済み main CI を照合
2. **Build Release**: マルチプラットフォームバイナリのビルド
3. **Create Release**: GitHub Release の作成
4. **Publish Crate**: crates.io への公開（依存順）
5. **Publish Python**: 同一 peeled SHA の Python tag を作成し、PyPI へ公開
6. **Public verification**: crates.io / PyPI の対象版だけでデモを実行
7. **Docs join**: 成功レポートが `alopex-db/docs@main` に同一バイトで反映されたことを確認

## リリース手順

### 0. RCブランチの作成（必須）

リリース作業は **必ずRCブランチと専用 worktree から開始**する。
project root は `main` のまま固定し、`Cargo.toml` の版から branch/worktree 名を導出する。

RCブランチの命名規則:

- 基本: `rc/<version>`（例: `rc/v0.5.0`）
- 追加RCが必要な場合: `rc/<version>-rcN`（例: `rc/v0.5.0-rc2`）
- プレフィックスは必ず `rc/` とし、`release/` などは使用しない

```bash
VERSION="$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["workspace"]["package"]["version"])')"
git pull --ff-only origin main
git worktree add ".wt/rc-v${VERSION}" -b "rc/v${VERSION}" main
git -C ".wt/rc-v${VERSION}" push -u origin "rc/v${VERSION}"
```

以後のリリース準備はこの RC ブランチで実施する。

### 1. 事前確認

```bash
# ビルド確認
cargo check --workspace

# テスト実行
cargo test --workspace

# clippy チェック
cargo clippy --all-targets --all-features -- -D warnings

# dry-run で公開可能か確認
cargo publish --dry-run -p alopex-core
```

### 2. バージョン更新

`Cargo.toml` の workspace バージョンを更新：

```bash
# 例: 0.3.0 → 0.4.0
vim Cargo.toml
```

```toml
[workspace.package]
version = "0.4.0"  # 新しいバージョン

[workspace.dependencies]
alopex-core = { version = "0.4.0", path = "crates/alopex-core" }
```

### 3. コミット

```bash
git add Cargo.toml
git commit -m "chore: bump version to 0.4.0"
```

### 4. プッシュ & CI 確認

```bash
git push origin rc/v0.5.0
```

GitHub Actions の CI が成功することを確認してください。

### 5. RCブランチから main へのマージ（必須）

**RCブランチからのマージを必須**とし、直接 `main` でリリース作業を行わない。

- RC ブランチ（例: `rc/v0.5.0`）から `main` への PR を作成
- すべての必須チェックが通ったらマージ

### 6. タグ作成 & プッシュ

```bash
# タグ作成
git tag -a v0.4.0 -m "Release v0.4.0"

# タグをプッシュ（リリースワークフロー発火）
git push origin v0.4.0
```

### 7. リリース確認

- [ ] GitHub Actions の Release ワークフローが成功
- [ ] GitHub Releases にバイナリがアップロードされている
- [ ] crates.io に各クレートが公開されている
  - https://crates.io/crates/alopex-core
  - https://crates.io/crates/alopex-sql
  - https://crates.io/crates/alopex-embedded

## 手動リリース（緊急時）

自動リリースが失敗した場合の手動手順：

```bash
# 1. alopex-core を公開
cargo publish -p alopex-core

# 2. crates.io index 更新待ち（約30秒）
sleep 30

# 3. alopex-sql を公開
cargo publish -p alopex-sql

# 4. 待機
sleep 30

# 5. alopex-embedded を公開
cargo publish -p alopex-embedded

# 6. 残りのクレートを公開（依存関係がある場合は待機を挟む）
cargo publish -p alopex-server
cargo publish -p alopex-cli
```

## トラブルシューティング

### "no matching package named `alopex-core` found"

原因: `alopex-core` がまだ crates.io にない状態で依存クレートを公開しようとした

対処:
1. `alopex-core` を先に公開
2. 30秒待機（crates.io index 更新）
3. 依存クレートを公開

### "crate version already exists"

原因: 同じバージョンが既に公開済み

対処: バージョン番号を上げて再リリース

### CI Gate 失敗

原因: fmt, clippy, test のいずれかが失敗

対処:
```bash
# ローカルで修正
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace

# 修正をコミット & プッシュ
git add -A
git commit -m "fix: resolve CI issues"
git push origin main

# 既存タグを削除して再作成（必要な場合）
git tag -d v0.4.0
git push origin :refs/tags/v0.4.0
git tag -a v0.4.0 -m "Release v0.4.0"
git push origin v0.4.0
```

## 関連ドキュメント

- [GitHub Actions ワークフロー](.github/workflows/release.yml)
- [CI ワークフロー](.github/workflows/ci.yml)
- [Pre-commit フック設定](scripts/setup-hooks.sh)
- [v0.7 Cluster-aware Foundation](docs/cluster-aware-foundation.md)

## 変更履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|---------|
| 2024-12-17 | v0.3.0 | 初回 crates.io リリース準備 |
