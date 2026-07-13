# syntax=docker/dockerfile:1
# =============================================================================
# alopex mode-parity 検証コンテナ
# 仕様: docs-public/specs/alopex-mode-parity-spec.md「検証コンテナ」
#
# 構成物とバージョン固定の根拠:
#   - Rust toolchain: CI (.github/workflows/ci.yml) は dtolnay/rust-toolchain@stable
#     を使うため、コンテナは CI が現在解決している stable = 1.96 に固定する。
#     CI の stable が更新されたら RUST_VERSION を同時に更新し、
#     コンテナと CI の版ずれを作らない。
#   - Nim: CI の Nim ビルドステップと同一イメージ nimlang/nim:2.2 から
#     /opt/nim をそのまま取り込む(ツールチェーンのバイナリ一致)。
#     タグは可動のため digest で固定する(下記 NIM_IMAGE。この digest は
#     NIM_VERSION=2.2.10 の CI 実使用イメージ)。Nim 2.2.x の更新に追随する
#     際は CI と同時に digest を更新する。
#     nimble 依存 (npeg, msgpack4nim) も CI と同一コマンドでプリインストールする。
#   - Python: Debian bookworm の python3 (3.11 >= 3.10) + venv。
#     requirements.txt の依存(バージョン固定)をイメージ内にインストールする。
#     pip はベースイメージ(python3-pip)同梱のものをそのまま使う
#     (アップグレードすると pip 自体が非固定の可動要素になるため)。
#   - apt パッケージは意図的に unpinned とする: バージョンの再現性は
#     ベースイメージのタグ/digest 側で担保し、apt は Debian bookworm の
#     セキュリティ追随を優先する(個別ピンは bookworm のポイントリリースで
#     壊れやすく、保守コストに見合わない)。
#
# リポジトリはビルド時に COPY せず、実行時にマウントして使う。
# v0.7.0 以降、alopex-cluster が chirps リポジトリへのパス依存
# (../../../../chirps、optional でも manifest 読込は必須)を持つため、
# chirps の checkout を /chirps に読み取り専用でマウントする:
#
#   docker build -t alopex-parity -f scripts/parity/Dockerfile scripts/parity
#   docker run --rm -it \
#     --user "$(id -u):$(id -g)" -e HOME=/home/parity \
#     -v "$PWD":/workspace -v <path-to-chirps>:/chirps:ro \
#     -w /workspace \
#     alopex-parity \
#     python3 scripts/parity/verify.py \
#       --corpus scripts/parity/corpus --expected scripts/parity/expected
#
# ホスト UID/GID が 1000 以外の場合(HOME 配下書き込みの検証結果):
#   - cargo / rustup: CARGO_HOME=/usr/local/cargo, RUSTUP_HOME=/usr/local/rustup
#     (rust 公式イメージが全ユーザー書き込み可に設定)→ HOME 非依存で動く。
#   - Python venv (/opt/parity/venv): 実行時は読み取りのみ → HOME 非依存。
#   - nimble パッケージ: 下記 ENV NIMBLE_DIR で /home/parity/.nimble に固定し、
#     chmod a+rX 済みのため任意 UID から参照できる(HOME 非依存)。
#   - Nim コンパイラキャッシュ(nimble lib 実行時): XDG_CACHE_HOME
#     (未設定時は $HOME/.cache)に書く。下記 ENV XDG_CACHE_HOME で
#     誰でも書ける /tmp 配下へ向けているため任意 UID で動く。
#   残る HOME 直下への書き込みは既知ツールには無いが、万一に備えて
#   UID/GID 1000 以外で使う場合は次のどちらかを推奨する:
#     (a) --build-arg PARITY_UID/PARITY_GID をホストに合わせて再ビルド(推奨)
#     (b) 実行時に -e HOME=/tmp/parity-home を追加し、書き込み可能な HOME を与える
#   いずれの場合もホスト側マウントに root 所有物は作られない(USER が非 root)。
#
# コンテナ内での cargo build / cargo test は逐次実行とする(多重起動禁止)。
# 逐次性は runner/surfaces.py の run_cargo() がロックで保証する。
# =============================================================================

ARG RUST_VERSION=1.96
# nimlang/nim:2.2 (NIM_VERSION=2.2.10) を digest 固定。CI 実使用イメージと同一。
ARG NIM_IMAGE=nimlang/nim:2.2@sha256:62428daa4a39baeb6f5e429a9c2ca3cee27a80ef880fe6e1bf3e29cc2296ac1b

# --- Nim ツールチェーン取り込み元(CI と同一イメージ) ---
FROM ${NIM_IMAGE} AS nim-toolchain

# --- 本体: Rust(CI と同一版)+ Nim + Python ---
FROM rust:${RUST_VERSION}-bookworm

# Python 3.11 (bookworm 標準) — 仕様の Python 3.10+ を満たす
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 \
      python3-venv \
      python3-pip \
 && rm -rf /var/lib/apt/lists/*

# Nim ツールチェーン(nimlang/nim:2.2 の /opt/nim をそのまま使用)
COPY --from=nim-toolchain /opt/nim /opt/nim
ENV PATH=/opt/nim/bin:${PATH}

# 専用非 root ユーザー(マウント運用時はホスト UID/GID に合わせて build-arg で上書き)
ARG PARITY_UID=1000
ARG PARITY_GID=1000
RUN groupadd -g "${PARITY_GID}" parity \
 && useradd -m -u "${PARITY_UID}" -g "${PARITY_GID}" parity

# Python 依存(requirements.txt でバージョン固定)を専用 venv にインストール。
# pip はベース同梱版を使う(アップグレードしない)。
COPY requirements.txt /opt/parity/requirements.txt
RUN python3 -m venv /opt/parity/venv \
 && /opt/parity/venv/bin/pip install --no-cache-dir -r /opt/parity/requirements.txt
ENV PATH=/opt/parity/venv/bin:${PATH}

# 以降は非 root で実行
USER parity
ENV HOME=/home/parity

# nimble 依存: CI (ci.yml) の `nimble install -y npeg msgpack4nim` と同一。
# NIMBLE_DIR でパッケージ置き場を HOME 非依存に固定し、任意 UID から
# 参照できるよう読み取り権限を開く(冒頭コメント「HOME 配下書き込みの
# 検証結果」参照)。
ENV NIMBLE_DIR=/home/parity/.nimble
RUN nimble install -y npeg msgpack4nim \
 && chmod -R a+rX /home/parity/.nimble

# Nim コンパイラキャッシュを誰でも書ける場所へ(--user 任意 UID 対応)。
# 検証コンテナは単一タスク実行前提のためキャッシュ共有の衝突は許容する。
ENV XDG_CACHE_HOME=/tmp/parity-cache

# リポジトリのマウント先。製品バイナリ実行時に Nim 共有ライブラリ
# (`nimble lib` の生成物) を解決できるよう、CI と同じディレクトリを
# LD_LIBRARY_PATH に載せる。
ENV LD_LIBRARY_PATH=/workspace/crates/alopex-sql/nim-sql-parser
WORKDIR /workspace

CMD ["bash"]
