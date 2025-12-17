# Alopex リリース手順書

## 概要

このドキュメントは Alopex プロジェクトのリリース手順を説明します。

## バージョン管理

### Workspace 継承

全クレートのバージョンは `Cargo.toml` の `[workspace.package]` で一元管理されています。

```toml
[workspace.package]
version = "0.3.0"  # ← ここを変更すると全クレートに反映
```

### クレート一覧

| クレート | 説明 | 依存関係 |
|---------|------|---------|
| `alopex-core` | コアストレージエンジン | なし（最初に公開） |
| `alopex-sql` | SQL パーサー | alopex-core |
| `alopex-embedded` | 組み込みDB インターフェース | alopex-core |
| `alopex-server` | サーバーコンポーネント | （未実装） |
| `alopex-cli` | CLI ツール | （未実装） |
| `alopex-cluster` | 分散クラスター | （未実装） |
| `alopex-tools` | 開発ツール | （未実装） |

### 公開順序

依存関係により、以下の順序で公開する必要があります：

```
alopex-core → alopex-sql → alopex-embedded → alopex-server → alopex-cli
```

## リリースワークフロー

### タグ形式

| プロジェクト | タグ形式 | 例 |
|-------------|---------|-----|
| alopex | `v{major}.{minor}.{patch}` | `v0.3.0` |

### 自動化される処理

タグをプッシュすると、GitHub Actions が以下を自動実行します：

1. **CI Gate**: fmt, clippy, test の実行
2. **Build Release**: マルチプラットフォームバイナリのビルド
3. **Create Release**: GitHub Release の作成
4. **Publish Crate**: crates.io への公開（依存順）

## リリース手順

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
git push origin main
```

GitHub Actions の CI が成功することを確認してください。

### 5. タグ作成 & プッシュ

```bash
# タグ作成
git tag -a v0.4.0 -m "Release v0.4.0"

# タグをプッシュ（リリースワークフロー発火）
git push origin v0.4.0
```

### 6. リリース確認

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

## 変更履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|---------|
| 2024-12-17 | v0.3.0 | 初回 crates.io リリース準備 |
