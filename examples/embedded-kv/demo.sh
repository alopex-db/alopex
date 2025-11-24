#!/usr/bin/env bash
# AlopexDB 埋め込みAPIの動作を一通り体験するデモスクリプト。
# 実行例:
#   ./demo.sh            # 単にデモを流す
#   LOG_LEVEL=debug ./demo.sh  # ログを増やして見る

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXAMPLE_BIN="embedded-kv"

echo "== AlopexDB 埋め込みKVデモを開始します =="
echo "プロジェクト: ${ROOT_DIR}"
echo "例: cargo run --example ${EXAMPLE_BIN}"

pushd "${ROOT_DIR}" > /dev/null

# クリーンビルドよりデモを優先し、再ビルド時間を短縮するためreleaseを使わない。
echo ">>> サンプルをビルドして実行中..."
cargo run --example "${EXAMPLE_BIN}" --quiet

echo ">>> デモ完了。上記ログで CRUD/トランザクション/WAL リカバリの流れを確認できます。"
popd > /dev/null
