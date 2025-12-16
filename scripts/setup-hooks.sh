#!/bin/bash
# setup-hooks.sh
# Configure git to use project-specific hooks
#
# Usage: ./scripts/setup-hooks.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Setting up git hooks for alopex..."

# Configure git to use .githooks directory
git -C "$REPO_ROOT" config core.hooksPath .githooks

echo "Git hooks configured successfully!"
echo ""
echo "The following hooks are now active:"
echo "  - pre-commit: runs 'cargo fmt --check' and 'cargo clippy'"
echo ""
echo "To disable hooks temporarily, use:"
echo "  git commit --no-verify"
