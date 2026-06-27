# ==============================================================
# Stage 1: Nim でパーサーを共有ライブラリとしてビルド
#   非 root ユーザーでビルドする（ビルドに root 権限は不要）
# ==============================================================
FROM nimlang/nim:2.2 AS nim-builder

# 非 root ビルドユーザーを作成（UID/GID は build-arg で上書き可能。
# マウントビルド時にホストの UID と一致させて生成物の所有者を揃えるため）
ARG NIM_UID=1000
ARG NIM_GID=1000
RUN groupadd -g "${NIM_GID}" builder 2>/dev/null || true \
  && useradd -m -u "${NIM_UID}" -g "${NIM_GID}" builder 2>/dev/null || true

USER builder
ENV HOME=/home/builder
WORKDIR /home/builder/build

# 依存パッケージのインストール
COPY --chown=builder:builder nim_sql_parser.nimble ./
RUN nimble install -y --depsOnly

# ソースコードのコピーとビルド
COPY --chown=builder:builder src/ ./src/

# 共有ライブラリ (.so) としてビルド — Rust から FFI で呼び出す
RUN nim c \
  -d:release \
  --app:lib \
  --noMain \
  --mm:orc \
  --opt:speed \
  -o:/home/builder/build/libalopex_sql_parser.so \
  src/alopex_sql_parser.nim

# ヘッダファイルの生成（Rust FFI 用）
RUN nim c \
  -d:release \
  --header:alopex_sql_parser.h \
  --noMain \
  --app:lib \
  src/alopex_sql_parser.nim

# ==============================================================
# Stage 2: テスト実行（非 root）
# ==============================================================
FROM nim-builder AS nim-test

COPY --chown=builder:builder tests/ ./tests/
RUN nim c -r tests/test_parser.nim

# ==============================================================
# Stage 3: Rust プロジェクトに統合
# ==============================================================
FROM rust:1.87-alpine AS rust-builder

RUN apk add --no-cache musl-dev

WORKDIR /app

# Nim でビルドした共有ライブラリをコピー
COPY --from=nim-builder /home/builder/build/libalopex_sql_parser.so /usr/local/lib/
COPY --from=nim-builder /home/builder/build/alopex_sql_parser.h /usr/local/include/

# Rust プロジェクトのビルド（alopex-sql クレートがある場合）
# COPY alopex/ ./
# RUN cargo build --release

# ==============================================================
# Stage 4: 最小ランタイムイメージ（非 root 実行）
# ==============================================================
FROM alpine:3.21

RUN addgroup -S alopex && adduser -S -G alopex alopex

COPY --from=nim-builder /home/builder/build/libalopex_sql_parser.so /usr/local/lib/
RUN ldconfig /usr/local/lib 2>/dev/null || true

USER alopex
ENTRYPOINT ["/bin/sh"]
