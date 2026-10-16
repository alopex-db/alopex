#!/usr/bin/env python3
"""Check the Python extension's Mach-O string table in a macOS wheel."""

import argparse
import subprocess
import tempfile
import zipfile
from pathlib import Path


def symtab_string_offsets(otool_output: str) -> list[int]:
    offsets = []
    in_symtab = False
    for line in otool_output.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] == "cmd":
            in_symtab = fields[1] == "LC_SYMTAB"
        elif in_symtab and len(fields) == 2 and fields[0] == "stroff":
            offsets.append(int(fields[1]))
            in_symtab = False
    return offsets


def verify(wheel: Path) -> None:
    with zipfile.ZipFile(wheel) as archive:
        extensions = [
            name
            for name in archive.namelist()
            if name.startswith("alopex/_alopex") and name.endswith(".so")
        ]
        if len(extensions) != 1:
            raise ValueError(f"expected one alopex extension in {wheel}, found {extensions}")
        with tempfile.TemporaryDirectory() as directory:
            extension = Path(directory) / Path(extensions[0]).name
            extension.write_bytes(archive.read(extensions[0]))
            output = subprocess.run(
                ["otool", "-l", str(extension)],
                check=True,
                capture_output=True,
                text=True,
            ).stdout

    offsets = symtab_string_offsets(output)
    if not offsets:
        raise ValueError(f"no LC_SYMTAB string table in {wheel}")
    if any(offset % 8 for offset in offsets):
        raise ValueError(f"misaligned LC_SYMTAB string table in {wheel}: {offsets}")
    print(f"{wheel}: LC_SYMTAB.stroff={offsets} (8-byte aligned)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    verify(args.wheel)


if __name__ == "__main__":
    main()
