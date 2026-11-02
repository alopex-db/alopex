"""Remove only regenerable outputs owned by an isolated RC build job."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil


def cleanup(workspace: Path, temporary: Path, target: Path, kind: str) -> None:
    if kind not in {"core", "crates"}:
        raise ValueError("unknown RC build kind")
    # A malformed target must never turn cleanup into deletion of source or a
    # shared host cache. Workflows declare one target directory for this host.
    if target != workspace / "target":
        raise ValueError("RC target must be the workspace target directory")
    owned = [target]
    if kind == "core":
        owned.append(workspace / "artifacts/parser/native-smoke")
    else:
        owned.extend(temporary / name for name in (
            "alopex-release-source", "alopex-release-parser-assets",
        ))
        # Qualified crate bytes and publish metadata remain available even if
        # a remote upload failed. Only the disposable local index is removed.
        owned.append(temporary / "alopex-crate-assets/.candidate-index")
    # Check all parent components before removing anything. Do not follow an
    # attacker-controlled symlink to outputs outside the isolated job roots.
    for path in owned:
        boundary = workspace if path.is_relative_to(workspace) else temporary
        for parent in path.parents:
            if parent == boundary:
                break
            if parent.is_symlink():
                raise ValueError(f"symlink cleanup parent refused: {parent}")
    for path in owned:
        if path.is_symlink():
            path.unlink()
        elif path.exists():
            if not path.is_dir():
                raise ValueError(f"expected owned output directory: {path}")
            shutil.rmtree(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=("core", "crates"), required=True)
    args = parser.parse_args()
    if os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise SystemExit("RC cleanup requires an isolated GitHub-hosted runner")
    workspace = Path(os.environ["GITHUB_WORKSPACE"])
    temporary = Path(os.environ["RUNNER_TEMP"])
    if not workspace.is_absolute() or not temporary.is_absolute():
        raise SystemExit("RC cleanup requires absolute runner roots")
    cleanup(workspace, temporary, Path(os.environ["CARGO_TARGET_DIR"]), args.kind)


if __name__ == "__main__":
    main()
