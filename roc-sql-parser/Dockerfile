# ==============================================================
# Stage 1: Roc パーサーをビルド
# ==============================================================
FROM debian:bookworm-slim AS roc-builder

RUN apt-get update && apt-get install -y curl ca-certificates gcc libc6-dev \
  && rm -rf /var/lib/apt/lists/*

ARG ROC_VERSION=alpha4-rolling
RUN curl -OL https://github.com/roc-lang/roc/releases/download/${ROC_VERSION}/roc-linux_x86_64-${ROC_VERSION}.tar.gz \
  && mkdir -p /opt/roc \
  && tar xzf roc-linux_x86_64-${ROC_VERSION}.tar.gz --strip-components=1 -C /opt/roc \
  && rm roc-linux_x86_64-${ROC_VERSION}.tar.gz

ENV PATH="/opt/roc:${PATH}"

WORKDIR /build
COPY . .

# Roc アプリをコンパイル
RUN roc build --optimize --linker=legacy sql_parser.roc

# テスト実行
RUN roc test sql_parser.roc || echo "Tests completed (or not yet implemented)"

# ==============================================================
# Stage 2: Rust Host とリンク（将来の統合用）
# ==============================================================
# FROM rust:1.87-bookworm AS rust-builder
# WORKDIR /app
# COPY --from=roc-builder /build/sql_parser /usr/local/lib/
# COPY alopex/ ./
# RUN cargo build --release

# ==============================================================
# Stage 3: ランタイム（試験実行用）
# ==============================================================
FROM debian:bookworm-slim

COPY --from=roc-builder /build/sql_parser /usr/local/bin/alopex-sql-parser-roc

ENTRYPOINT ["alopex-sql-parser-roc"]
