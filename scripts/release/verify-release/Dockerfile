# syntax=docker/dockerfile:1
# =============================================================================
# alopex リリース確認コンテナ
#
# scripts/parity/Dockerfile(検証コンテナ)との違い: あちらはリポジトリを
# マウントしてソースからビルドする。このコンテナは逆に、ソースを一切
# COPY/マウントせず、crates.io / PyPI に実際に公開された成果物だけを
# 取得して動かす。「ユーザーが手にする配布物が本当に動くか」を検証する
# ためのものであり、v0.7.1 リリースで発生した障害(crates.io publish の
# 順序バグ・依存重複によるディスク枯渇等)はいずれもこの観点の検証が
# 無かったために気づけなかった。
#
# 取得する成果物:
#   - alopex-cli / alopex-server: `cargo install --locked` で crates.io から
#     ソース取得してビルド(バイナリ配布は Linux/macOS/Windows の4種のみで
#     alopex-server を含まないため、cargo install に統一する)。
#   - alopex (Python): `pip install alopex==<version>` で PyPI の manylinux
#     wheel を取得(ビルドしない。ユーザーが実際にインストールする経路
#     そのもの)。
#   - crates/alopex-tools の verify-release-embedded: crates.io 公開版の
#     alopex-embedded に依存する独立クレート(alopex-tools/Cargo.toml の
#     [workspace] 空テーブルで親から分離済み)。ビルドには
#     デモスクリプト側から crates/alopex-sql/nim-sql-parser の Nim 共有
#     ライブラリが必要(ビルド時のみ、実行時は cargo install 版の
#     alopex-cli/alopex-server が libalopex_sql_parser.so を rpath で
#     解決する)。
#
# ビルド版のバージョン固定根拠は scripts/parity/Dockerfile と同じ
# (Rust stable、Nim 2.2、Python 3.11)。
#
# 使い方:
#   docker build -t alopex-verify-release \
#     --build-arg ALOPEX_VERSION=0.7.1 \
#     -f scripts/release/verify-release/Dockerfile \
#     scripts/release/verify-release
#   docker run --rm -it \
#     --user "$(id -u):$(id -g)" -e HOME=/home/verify \
#     -v "$PWD":/workspace:ro -v <path-to-chirps>:/chirps:ro \
#     -w /workspace \
#     -e ALOPEX_BINARY_SOURCE=released \
#     alopex-verify-release \
#     python3 scripts/parity/demo.py
#
# リポジトリは読み取り専用でマウントする(デモスクリプト自体とコーパス/
# 期待値の読み出しにのみ使う。製品コードは一切ビルドしない)。
# =============================================================================

ARG RUST_VERSION=1.96
ARG NIM_IMAGE=nimlang/nim:2.2@sha256:62428daa4a39baeb6f5e429a9c2ca3cee27a80ef880fe6e1bf3e29cc2296ac1b
ARG ALOPEX_VERSION=0.7.1

FROM ${NIM_IMAGE} AS nim-toolchain

FROM rust:${RUST_VERSION}-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 \
      python3-venv \
      python3-pip \
 && rm -rf /var/lib/apt/lists/*

COPY --from=nim-toolchain /opt/nim /opt/nim
ENV PATH=/opt/nim/bin:${PATH}

ARG VERIFY_UID=1000
ARG VERIFY_GID=1000
RUN groupadd -g "${VERIFY_GID}" verify \
 && useradd -m -u "${VERIFY_UID}" -g "${VERIFY_GID}" verify

# --- Python 依存: PyPI 公開版 alopex のみ(ソースビルドしない) ---
ARG ALOPEX_VERSION
COPY requirements.txt /opt/verify/requirements.txt
RUN python3 -m venv /opt/verify/venv \
 && /opt/verify/venv/bin/pip install --no-cache-dir -r /opt/verify/requirements.txt \
 && /opt/verify/venv/bin/pip install --no-cache-dir "alopex==${ALOPEX_VERSION}"
ENV PATH=/opt/verify/venv/bin:${PATH}

USER verify
ENV HOME=/home/verify

# --- Nim ビルド依存(verify-release-embedded と alopex-cli/alopex-server
#     のビルド時に必要。実行時には不要) ---
ENV NIMBLE_DIR=/home/verify/.nimble
RUN nimble install -y npeg msgpack4nim \
 && chmod -R a+rX /home/verify/.nimble
ENV XDG_CACHE_HOME=/tmp/verify-cache

# --- Rust 製品バイナリ: crates.io から `cargo install` (ソース取得+ビルド)。
#     GitHub Release のプリビルドバイナリは alopex-server を含まず
#     4 プラットフォームの固定ターゲットしかないため使わない。
#     alopex-sql の build.rs が要求する Nim 共有ライブラリを、この
#     ビルド専用に一時生成する(実行時に使う正式な rpath 解決対象とは
#     別。generated once, used only during `cargo install`)。 ---
ARG ALOPEX_VERSION
ENV CARGO_HOME=/home/verify/.cargo
RUN set -eux; \
    tmp_nim_dir="$(mktemp -d)"; \
    cd "${tmp_nim_dir}"; \
    curl -sSL -A "alopex-verify-release (https://github.com/alopex-db/alopex)" \
      -o alopex-sql.crate \
      "https://crates.io/api/v1/crates/alopex-sql/${ALOPEX_VERSION}/download"; \
    tar xzf alopex-sql.crate; \
    cd "alopex-sql-${ALOPEX_VERSION}/nim-sql-parser"; \
    nimble lib; \
    mkdir -p /home/verify/.local/lib; \
    cp libalopex_sql_parser.so /home/verify/.local/lib/; \
    rm -rf "${tmp_nim_dir}"
ENV NIM_SQL_PARSER_LIB_DIR=/home/verify/.local/lib
ENV LD_LIBRARY_PATH=/home/verify/.local/lib
RUN cargo install alopex-cli --version "=${ALOPEX_VERSION}" --locked \
 && cargo install alopex-server --version "=${ALOPEX_VERSION}" --locked
ENV PATH=/home/verify/.cargo/bin:${PATH}

# --- crates/alopex-tools の verify-release-embedded: リポジトリマウント後、
#     デモスクリプト実行前に呼び出し側(run.sh 相当)がビルドする契約。
#     このイメージにはビルド依存(cargo/nim)のみ用意し、ビルド自体は
#     実行時(リポジトリマウント後)に行う(alopex-tools はリリースごとに
#     依存バージョンを追随するため、イメージに焼き込むとビルド日と
#     リリース内容がずれるリスクがある)。 ---

# ALOPEX_EXTRA_PATH(実行時に run.sh が -e で渡す、通常
# /tools-target/release)が設定されていれば PATH の先頭に足す。ENV PATH は
# ビルド時にしか展開されないため、実行時の追加パスはエントリポイントで
# 処理する。
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
WORKDIR /workspace
CMD ["bash"]
