#!/usr/bin/env bash
# docker-entrypoint.sh - ALOPEX_EXTRA_PATH を PATH の先頭に足してから
# 本来のコマンドを exec する。
#
# ENV PATH はイメージビルド時にしか展開されないため、実行時にコンテナへ
# マウントするディレクトリ(crates/alopex-tools のビルド出力等)を PATH に
# 足すにはエントリポイントで処理する必要がある。
set -euo pipefail

if [ -n "${ALOPEX_EXTRA_PATH:-}" ]; then
    export PATH="${ALOPEX_EXTRA_PATH}:${PATH}"
fi

exec "$@"
