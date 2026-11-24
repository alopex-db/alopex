"""Check the exact existing Rust toolchain without installing or logging it."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess


CASE = "HNSW_TOOLCHAIN"
VERSION = "1.90.0"
HOST = "x86_64-unknown-linux-gnu"
LIMIT = 4096


def require(value):
    if not value:
        raise ValueError("TOOLCHAIN")


def _binary(name, path):
    found = shutil.which(name, path=path)
    require(type(found) is str)
    selected = Path(found)
    require(selected.is_absolute() and selected.is_file())
    resolved = selected.resolve(strict=True)
    info = resolved.stat()
    require(stat.S_ISREG(info.st_mode) and
            info.st_uid in (0, os.geteuid()) and
            not (info.st_mode & 0o022))
    return selected


def _run(binary, environment, *args):
    result = subprocess.run([str(binary), *args],
                            env=environment, stdin=subprocess.DEVNULL,
                            capture_output=True, timeout=10)
    require(result.returncode == 0 and not result.stderr and
            0 < len(result.stdout) <= LIMIT)
    return result.stdout.decode("ascii")


def check():
    home = Path.home()
    require(home.is_absolute() and home.is_dir() and not home.is_symlink() and
            home.stat().st_uid == os.geteuid())
    search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
    environment = {"HOME": str(home), "PATH": search, "LANG": "C",
                   "RUSTUP_AUTO_INSTALL": "0", "RUSTUP_TOOLCHAIN": VERSION}
    rustc = _binary("rustc", search)
    cargo = _binary("cargo", search)
    compiler = _run(rustc, environment, "-Vv").splitlines()
    require(compiler.count("release: " + VERSION) == 1 and
            compiler.count("host: " + HOST) == 1)
    package = _run(cargo, environment, "-V").strip()
    require(re.fullmatch(r"cargo " + re.escape(VERSION) +
                         r" \([0-9a-f]{9} [0-9]{4}-[0-9]{2}-[0-9]{2}\)",
                         package))


def run():
    try:
        check()
        return {"case": CASE, "outcome": "passed"}, 0
    except BaseException:
        return {"case": CASE, "outcome": "failed",
                "error_code": "TOOLCHAIN"}, 1


def main():
    outcome, status = run()
    print(json.dumps(outcome, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
