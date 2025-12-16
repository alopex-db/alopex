#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="${WORKSPACE_DIR:-$(cd -- "${ROOT_DIR}/.." && pwd)}"

LSM_TREE_DIR="${LSM_TREE_DIR:-${WORKSPACE_DIR}/alopex-core-worktrees/lsm-tree-file-mode}"
TMPDIR="${TMPDIR:-${WORKSPACE_DIR}/.tmp}"

if [[ ! -d "${LSM_TREE_DIR}" ]]; then
  echo "lsm-tree-file-mode worktree not found: ${LSM_TREE_DIR}" >&2
  echo "Set LSM_TREE_DIR=/path/to/lsm-tree-file-mode and retry." >&2
  exit 1
fi

mkdir -p "${TMPDIR}"

mode="${1:-min}"
case "${mode}" in
  min)
    (cd "${LSM_TREE_DIR}" && TMPDIR="${TMPDIR}" cargo test -p alopex-core)
    (cd "${LSM_TREE_DIR}" && TMPDIR="${TMPDIR}" cargo test -p alopex-sql)
    ;;
  focus)
    (cd "${LSM_TREE_DIR}" && TMPDIR="${TMPDIR}" cargo test -p alopex-core lsm::integration::crud)
    (cd "${LSM_TREE_DIR}" && TMPDIR="${TMPDIR}" cargo test -p alopex-core lsm::integration::recovery)
    (cd "${LSM_TREE_DIR}" && cargo test -p alopex-core columnar::integration::disk)
    (cd "${LSM_TREE_DIR}" && cargo test -p alopex-core vector::integration::disk)
    (cd "${LSM_TREE_DIR}" && cargo test -p alopex-sql storage::disk)
    (cd "${LSM_TREE_DIR}" && cargo test -p alopex-sql integration::disk)
    ;;
  all)
    "${0}" min
    "${0}" focus
    ;;
  *)
    echo "Usage: $0 {min|focus|all}" >&2
    echo "  min   : cargo test -p alopex-core/alopex-sql" >&2
    echo "  focus : disk-mode integration subsets" >&2
    echo "  all   : min + focus" >&2
    exit 2
    ;;
esac

