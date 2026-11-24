"""Verify the exact candidate-side SQL harness dependency closure."""
from __future__ import annotations

import hashlib
from pathlib import Path
import stat
import subprocess

COMMIT = "426418099c4337e73f5ced4444215a640898ed5a"
TREE = "84f2b38c5a1e0aa9becd1527a2de9cb5356e4b93"
FILES = {
    "parity_v0811_measure.py": (30611,
        "e6b9f9544763f482ff4236f11d044a63cd058a6fdd51fb07cca37ff959c7bab3"),
    "parity_v0811_gate.py": (7977,
        "2165e030159f66ef2e7203d6e8500d84e6e062f2ef26fa8fc6a195341b0c1b8c"),
    "fixtures/sql-v0.8.11-curated.json": (515,
        "23147656b2336db08e37cb876d26490bac40ef5b3d84077f8a76397e3232b3f3"),
}


def require(ok):
    if not ok:
        raise ValueError("PRODUCT_HARNESS")


def _git(checkout, expression):
    result = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "--verify", expression],
        stdin=subprocess.DEVNULL, capture_output=True, timeout=10, check=False)
    require(result.returncode == 0 and not result.stderr)
    return result.stdout.decode("ascii").strip()


def verify(checkout):
    """Return fixed relative file pins only after checkout and byte checks."""
    try:
        require(isinstance(checkout, Path) and checkout.is_absolute() and
                checkout.is_dir() and not checkout.is_symlink() and
                _git(checkout, "HEAD") == COMMIT and
                _git(checkout, "HEAD^{tree}") == TREE)
        root = checkout / "scripts/performance"
        require(stat.S_ISDIR((checkout / "scripts").lstat().st_mode) and
                stat.S_ISDIR(root.lstat().st_mode))
        for name, (size, digest) in FILES.items():
            path = root / name
            require(all(stat.S_ISDIR(parent.lstat().st_mode)
                        for parent in (path.parent, root)))
            info = path.lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                    info.st_size == size and size < 1_000_000)
            data = path.read_bytes()
            require(len(data) == size and hashlib.sha256(data).hexdigest() == digest)
        return root, {name: {"size": size, "sha256": digest}
                      for name, (size, digest) in FILES.items()}
    except (OSError, TypeError, ValueError, UnicodeError, subprocess.TimeoutExpired):
        raise ValueError("PRODUCT_HARNESS") from None
