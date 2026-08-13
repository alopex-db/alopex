#!/usr/bin/env bash
set -euo pipefail

# Kept at the legacy path for workflow compatibility. Alopex consumes the
# published package and must not depend on a sibling checkout.
repo_root="${GITHUB_WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
manifest="${repo_root}/crates/alopex-cluster/Cargo.toml"

grep -Eq 'alopex-chirps-gossip-swim = \{ version = "=0\.5\.1", optional = true \}' "${manifest}"
if grep -Eq 'alopex-chirps-gossip-swim.*path[[:space:]]*=' "${manifest}"; then
    echo "alopex-chirps-gossip-swim must resolve from crates.io, not a local path" >&2
    exit 1
fi

echo "Chirps dependency contract OK: crates.io alopex-chirps-gossip-swim =0.5.1"
