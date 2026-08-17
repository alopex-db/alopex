# syntax=docker/dockerfile:1
# =============================================================================
# alopex リリース確認コンテナ
#
# scripts/parity/Dockerfile(検証コンテナ)との違い: あちらはリポジトリを
# マウントしてソースからビルドする。このコンテナは逆に、ソースを一切
# COPY/マウントせず、crates.io / PyPI に実際に公開された成果物だけを
# 取得して動かす。「ユーザーが手にする配布物が本当に動くか」を検証する
# ためのものであり、v0.7.1 リリースで発生した障害(crates.io publish の
# 順序バグ・依存重複によるディスク枯渇・rpath 伝播バグ・Nim ツールチェーン
# 依存等)はいずれもこの観点の検証が無かったために気づけなかった。
#
# v0.7.2 (D2 是正) 以降、alopex-sql は crates.io 公開時に4プラットフォーム
# 分の事前ビルド済み Nim 共有ライブラリを nim-sql-parser/vendor/<target>/
# に同梱するようになった (build.rs の解決順序に vendor/ を追加)。これにより
# `cargo install alopex-cli` はホスト側で Nim ソースを別途ビルドする
# ワークアラウンド無しに動作する。Nim ツールチェーン(下記 nim-toolchain)は
# このイメージ自体には不要になったが、alopex-tools(verify-release-embedded)
# のビルド時に *ソース側* の Nim 共有ライブラリを使う経路が実行時に必要な
# ため、引き続き同梱する(run.sh が実行時にリポジトリをマウントしてビルド
# する契約)。
#
# 取得する成果物:
#   - alopex-cli / alopex-server: `cargo install --locked` で crates.io から
#     ソース取得してビルド(バイナリ配布は Linux/macOS/Windows の4種のみで
#     alopex-server を含まないため、cargo install に統一する)。Nim
#     ツールチェーンは不要(vendored ライブラリを alopex-sql の build.rs が
#     自動解決する)。
#   - alopex (Python): `pip install alopex==<version>` で PyPI の manylinux
#     wheel を取得(ビルドしない。ユーザーが実際にインストールする経路
#     そのもの)。
#   - crates/alopex-tools の verify-release-embedded: crates.io 公開版の
#     alopex-embedded に依存する独立クレート(alopex-tools/Cargo.toml の
#     [workspace] 空テーブルで親から分離済み)。alopex-embedded 経由で
#     alopex-sql に依存するため、同じく vendored ライブラリで解決される。
#
# ビルド版のバージョン固定根拠は scripts/parity/Dockerfile と同じ
# (Rust stable、Nim 2.2、Python 3.11)。
#
# 使い方:
#   docker build -t alopex-verify-release \
#     --build-arg ALOPEX_VERSION=<Cargo.toml で検証済みの版> \
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
ARG ALOPEX_VERSION

FROM ${NIM_IMAGE} AS nim-toolchain

FROM rust:${RUST_VERSION}-bookworm

ARG ALOPEX_VERSION
RUN test -n "${ALOPEX_VERSION}"

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
#     v0.7.2 (D2 是正) 以降、alopex-sql の build.rs が
#     nim-sql-parser/vendor/<target-triple>/ の事前ビルド済み共有ライブラリを
#     自動解決するため、Nim ソースを別途取得・ビルドする手順は不要になった。
#     実行時の共有ライブラリ解決も rpath (RUNPATH) 経由でビルド時に完結する
#     ため、LD_LIBRARY_PATH の設定も不要 (rpath 伝播バグの是正を実機で
#     検証する意図もあり、あえて設定しない)。 ---
ARG ALOPEX_VERSION
ENV CARGO_HOME=/home/verify/.cargo
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
