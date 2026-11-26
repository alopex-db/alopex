"""Private selected-runner entry for one #681 public measurement mode."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys

import issue681_public_admission as admission
import issue681_public_pair as pair
import issue681_public_run as runner

SCHEMA = "alopex.issue681-selected-entry/v1"
FIELDS = {"schema", "finish", "profile", "pins", "assets", "work",
          "export", "admission", "private_roots", "extra_roots"}
MAX_CONFIG_BYTES = 16_384


def require(value):
    if not value:
        raise ValueError("PUBLIC_SELECTED")


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def directory(value, *, empty=False, private=True):
    require(type(value) is str and value.startswith("/") and
            "\x00" not in value)
    path = Path(value)
    require(str(path) == value and path.is_dir() and
            path.resolve(strict=True) == path and not path.is_symlink())
    if private:
        require(path.stat().st_uid == os.geteuid() and
                stat.S_IMODE(path.stat().st_mode) == 0o700)
    if empty:
        require(not any(path.iterdir()))
    return path


def load(path):
    require(isinstance(path, Path) and path.is_absolute() and
            path.is_file() and path.resolve(strict=True) == path and
            not path.is_symlink())
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o600 and
                0 < info.st_size <= MAX_CONFIG_BYTES)
        raw = os.read(fd, MAX_CONFIG_BYTES + 1)
        require(len(raw) == info.st_size)
    finally:
        os.close(fd)
    value = json.loads(raw, object_pairs_hook=unique)
    require(type(value) is dict and set(value) == FIELDS and
            value["schema"] == SCHEMA and
            value["finish"] in ("commit", "rollback") and
            type(value["assets"]) is dict and
            set(value["assets"]) == set(pair.ARMS) and
            type(value["private_roots"]) is list and
            bool(value["private_roots"]) and
            type(value["extra_roots"]) is list)
    pair._pins(value["pins"])
    require(type(value["profile"]) is dict and
            set(value["profile"]) == {
                "schema", "runner_kind", "cpu_count", "additional_bytes"} and
            value["profile"]["schema"] == admission.SCHEMA)
    assets = {arm: directory(value["assets"][arm]) for arm in pair.ARMS}
    work = directory(value["work"], empty=True)
    export = directory(value["export"], empty=True)
    admitted = directory(value["admission"], empty=True)
    require(len({work, export, admitted, *assets.values()}) == 5 and
            all(left not in right.parents and right not in left.parents
                for left in (work, export, admitted)
                for right in (work, export, admitted) if left != right))
    private_roots = tuple(directory(item, private=False)
                          for item in value["private_roots"])
    extra_roots = tuple(directory(item, private=False)
                        for item in value["extra_roots"])
    protected = tuple(dict.fromkeys((str(path.parent),
                                     *(str(item) for item in private_roots),
                                     *(str(item) for item in assets.values()),
                                     str(work), str(export), str(admitted))))
    inventory = tuple(dict.fromkeys((*private_roots, *extra_roots,
                                     path.parent)))
    return {"profile": value["profile"], "pins": value["pins"],
            "asset_locations": assets, "work": work, "export": export,
            "admission_dir": admitted, "finish": value["finish"],
            "private_roots": protected, "extra_roots": inventory}


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    try:
        require(len(args) == 1)
        inputs = load(Path(args[0]))
        runner.run_selected_mode(**inputs)
        print("PUBLIC_SELECTED_OK")
        return 0
    except Exception:
        print("PUBLIC_SELECTED")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
