#!/usr/bin/env bash
# v0.8.5 の embedded-local 公開能力を、Rust の alopex-embedded API から
# 一続きで実演する。公開リリース検証では crates.io の完全一致依存だけで
# ビルド済みの demo-v085-embedded を PATH から実行する。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

if [[ "${ALOPEX_BINARY_SOURCE:-}" == "released" ]]; then
    exec demo-v085-embedded
fi

exec cargo run --quiet \
    --manifest-path "${ROOT}/crates/alopex-tools/Cargo.toml" \
    --bin demo-v085-embedded
