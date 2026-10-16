"""Resolve host tools without accidentally invoking a different shell runtime."""

from __future__ import annotations

import platform
import shutil
from pathlib import Path


def git_bash() -> str:
    """Use Git for Windows' real shell, not a PATH launcher or WSL shim."""
    if platform.system() != "Windows":
        return "bash"
    git = shutil.which("git")
    if git is not None:
        for root in Path(git).resolve().parents:
            shell = root / "usr" / "bin" / "bash.exe"
            if shell.is_file():
                return str(shell)
    raise FileNotFoundError(
        "Git for Windows usr/bin/bash.exe is required for parser host tests; "
        "install Git for Windows and put its git.exe on PATH"
    )
