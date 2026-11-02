#!/usr/bin/env python3
"""Validate the required Python publication inventory without waiting for Intel."""
import argparse
from pathlib import Path
import re


def validate(names, version):
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
        raise ValueError("invalid release version")
    if len(names) != len(set(names)):
        raise ValueError("duplicate Python artifact")
    required = {"linux", "windows", "arm", "sdist"}
    if tuple(map(int, version.split("."))) < (0, 8, 17):
        required.add("intel")
    present = set()
    for name in names:
        if name == f"alopex-{version}.tar.gz":
            present.add("sdist")
            continue
        match = re.fullmatch(rf"alopex-{re.escape(version)}-[A-Za-z0-9_.]+-[A-Za-z0-9_.]+-([A-Za-z0-9_.]+)\.whl", name)
        if match is None:
            raise ValueError(f"unexpected Python distribution: {name}")
        platforms = match[1].split(".")
        category = None
        for key, pattern in (
            ("linux", r"manylinux[0-9_]+_x86_64"),
            ("windows", r"win_amd64"),
            ("arm", r"macosx_[0-9_]+_arm64"),
            ("intel", r"macosx_[0-9_]+_x86_64"),
        ):
            if all(re.fullmatch(pattern, platform) for platform in platforms):
                category = key
                break
        if category is None:
            raise ValueError(f"unsupported wheel platforms: {name}")
        present.add(category)
    if not required.issubset(present):
        raise ValueError(f"missing primary Python distributions: {sorted(required - present)}")


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--version", required=True)
    cli.add_argument("--directory", type=Path, required=True)
    args = cli.parse_args()
    paths = list(args.directory.iterdir())
    if any(p.is_symlink() or not p.is_file() or p.stat().st_size == 0 for p in paths):
        raise ValueError("Python artifacts must be nonempty regular files")
    validate([p.name for p in paths], args.version)
