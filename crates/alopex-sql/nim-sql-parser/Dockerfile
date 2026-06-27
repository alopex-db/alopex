# ==============================================================
# Stage 1: Nim でパーサーを共有ライブラリとしてビルド
# ==============================================================
FROM nimlang/nim:2.2 AS nim-builder

WORKDIR /build

# 依存パッケージのインストール
COPY nim_sql_parser.nimble ./
RUN nimble install -y --depsOnly

# ソースコードのコピーとビルド
COPY src/ ./src/

# 共有ライブラリ (.so) としてビルド — Rust から FFI で呼び出す
RUN nim c \
  -d:release \
  --app:lib \
  --noMain \
  --mm:orc \
  --opt:speed \
  -o:/build/libalopex_sql_parser.so \
  src/alopex_sql_parser.nim

# ヘッダファイルの生成（Rust FFI 用）
RUN nim c \
  -d:release \
  --header:alopex_sql_parser.h \
  --noMain \
  --app:lib \
  src/alopex_sql_parser.nim

# ==============================================================
# Stage 2: テスト実行
# ==============================================================
FROM nim-builder AS nim-test

COPY tests/ ./tests/
RUN nim c -r tests/test_parser.nim

# ==============================================================
# Stage 3: Rust プロジェクトに統合
# ==============================================================
FROM rust:1.87-alpine AS rust-builder

RUN apk add --no-cache musl-dev

WORKDIR /app

# Nim でビルドした共有ライブラリをコピー
COPY --from=nim-builder /build/libalopex_sql_parser.so /usr/local/lib/
COPY --from=nim-builder /build/alopex_sql_parser.h /usr/local/include/

# Rust プロジェクトのビルド（alopex-sql クレートがある場合）
# COPY alopex/ ./
# RUN cargo build --release

# ==============================================================
# Stage 4: 最小ランタイムイメージ
# ==============================================================
FROM alpine:3.21

COPY --from=nim-builder /build/libalopex_sql_parser.so /usr/local/lib/
RUN ldconfig /usr/local/lib 2>/dev/null || true

ENTRYPOINT ["/bin/sh"]
