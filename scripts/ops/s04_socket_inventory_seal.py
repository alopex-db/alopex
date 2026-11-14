#!/usr/bin/env python3
"""Seal socket observations and export only fixed public status plus CMS."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sys

from s04_inspect_seal import _seal_inner, owner_only


def main() -> int:
    private = Path(os.environ["RUNNER_TEMP"]) / "alopex-s04-socket-inventory"
    export = Path("native-s04-socket-inventory-export")
    outcome = os.environ.get("COLLECT_OUTCOME", "failure")
    public, code = _seal_inner(private, export, outcome)
    public.update(schema="alopex.s04-socket-inventory-public/v1", case="S04_SOCKET_INVENTORY")
    if code == 0:
        public["outcome"] = "collected_not_qualified"
    try:
        if private.is_dir() and not private.is_symlink() and private.lstat().st_uid == os.getuid():
            shutil.rmtree(private)
    except OSError:
        public.update(outcome="failed", error_code="CLEANUP_FAILED")
        code = 1
    try:
        export.mkdir(mode=0o700, exist_ok=True)
        if not owner_only(export):
            raise OSError("export mode")
        fd = os.open(export / "public.json", os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write((json.dumps(public, sort_keys=True) + "\n").encode())
    except OSError:
        print('{"case":"S04_SOCKET_INVENTORY","outcome":"evidence_failed"}')
        return 1
    print(json.dumps({key: public[key] for key in ("case", "outcome", "error_code")}, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
