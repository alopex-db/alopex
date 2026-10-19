#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
: "${CARGO_TARGET_DIR:=$repo/target}"
export CARGO_TARGET_DIR CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0 CARGO_BUILD_JOBS=2
# The 0.8.15 package owns its exact historical parser contract. Do not pass the
# candidate's explicit parser library directory into the historical build.
env -u NIM_SQL_PARSER_LIB_DIR -u ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD \
  cargo build --locked --manifest-path "$repo/scripts/compatibility/rust-old/Cargo.toml"
cargo build --locked --manifest-path "$repo/scripts/compatibility/rust-current/Cargo.toml"
