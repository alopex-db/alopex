#!/usr/bin/env python3
"""Select native parser checks from their inputs, defaulting to checks on doubt."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PARSER_FILES = {
    "Cargo.toml",  # The archive identity includes the workspace version.
    ".github/workflows/release-process.yml",
    "scripts/build-nim-parser.sh",
    "scripts/release/parser_asset_manifest.py",
    "scripts/release/tests/test_parser_asset_manifest.py",
    "scripts/release/tests/host_tools.py",
    "scripts/release/classify_parser_changes.py",
    "scripts/release/tests/test_classify_parser_changes.py",
}
PARSER_PREFIXES = (
    "crates/alopex-sql/nim-sql-parser/",
    ".github/actions/setup-nim-parser-toolchain/",
)


def needs_native_parser(base: str, head: str, repository: Path = ROOT) -> bool:
    """Use both rename sides; unavailable or empty comparisons run all hosts."""
    try:
        for commit in (base, head):
            if not re.fullmatch(r"[0-9a-f]{40}", commit):
                return True
            subprocess.run(
                ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
                cwd=repository,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        paths = subprocess.check_output(
            ["git", "diff", "--no-renames", "--name-only", "-z", base, head],
            cwd=repository,
            stderr=subprocess.DEVNULL,
        ).split(b"\0")[:-1]
    except (OSError, subprocess.CalledProcessError):
        return True
    return not paths or any(
        path.decode("utf-8", errors="surrogateescape") in PARSER_FILES
        or path.decode("utf-8", errors="surrogateescape").startswith(PARSER_PREFIXES)
        for path in paths
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base")
    parser.add_argument("head")
    args = parser.parse_args()
    required = needs_native_parser(args.base, args.head)
    print(f"parser={str(required).lower()}")
    print(
        "Native parser checks: "
        + (
            "required (changed inputs or unavailable comparison)"
            if required
            else "skipped (inputs unchanged)"
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
