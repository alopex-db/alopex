"""Admit a six-file, public-only #681 example bundle before measurement."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

import issue681_public_pair as pair

SCHEMA = "alopex.hnsw-public-assets/v1"
RESULT_SCHEMA = "alopex.hnsw-public-build-result/v1"
RECEIPT_SCHEMA = "alopex.hnsw-public-build/v1"
FILES = {"binary": "bin/issue681_hnsw_first_write",
         "library": "lib/libalopex_sql_parser.so",
         "example": "example.rs",
         "receipt": "receipts/build.json"}
MEMBERS = {"result.json", "assets/assets.json"} | {
    "assets/" + name for name in FILES.values()}
COMMAND = ["cargo", "build", "--locked", "--offline", "--release", "-p",
           "alopex-embedded", "--example", "issue681_hnsw_first_write"]
FIXED_PRIVATE_PREFIXES = (b"/Users/", b"/home/runner/", b"/private/var/folders/",
                          b"/tmp/alopex", b"C:\\Users\\", b"/_work/")


def require(value):
    if not value:
        raise ValueError("PUBLIC_ASSET")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def _read_file(path, maximum):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and 0 < info.st_size <= maximum)
        raw = stream.read(maximum + 1)
        require(len(raw) == info.st_size)
    return raw


def _json(path, maximum):
    raw = _read_file(path, maximum)
    try:
        value = json.loads(raw, object_pairs_hook=_unique)
    except (ValueError, UnicodeError):
        raise ValueError("PUBLIC_ASSET") from None
    require(type(value) is dict and raw == (json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode())
    return value, raw


def _elf_amd64(path):
    header = _read_file(path, 268_435_456)[:20]
    require(len(header) == 20 and header[:4] == b"\x7fELF" and
            header[4:6] == b"\x02\x01" and header[18:20] == b"\x3e\x00")


def _smoke(value, mode=None):
    expected = {"status", "rows", "dimensions", "phases", "events"}
    if mode is not None:
        expected |= {"mode", "result_validated", "elapsed_ns"}
    require(type(value) is dict and set(value) == expected and
            value["status"] == "passed" and value["rows"] == 2 and
            value["dimensions"] == 4 and value["phases"] == 9 and
            value["events"] == 19)
    if mode is not None:
        require(value["mode"] == mode and value["result_validated"] is True and
                type(value["elapsed_ns"]) is int and
                0 < value["elapsed_ns"] <= 3_600_000_000_000)


def _verify(root, pin, private_roots, *, diag=None):
    pair._pin(pin)
    require(isinstance(root, Path) and root.is_absolute() and
            root.is_dir() and not root.is_symlink() and
            root.stat().st_uid == os.geteuid() and
            stat.S_IMODE(root.stat().st_mode) == 0o700)
    actual = set()
    for path in root.rglob("*"):
        info = path.lstat()
        require(not stat.S_ISLNK(info.st_mode) and
                info.st_uid == os.geteuid())
        if stat.S_ISREG(info.st_mode):
            require(info.st_nlink == 1)
            actual.add(path.relative_to(root).as_posix())
        else:
            require(stat.S_ISDIR(info.st_mode))
    require(actual == MEMBERS)
    source = {"commit": pin["source_commit"], "tree": pin["source_tree"],
              "profile": "release"}
    manifest, manifest_raw = _json(root / "assets/assets.json", 16_384)
    require(hashlib.sha256(manifest_raw).hexdigest() == pin["assets_digest"] and
            set(manifest) == {"schema", "source", "platform", "files",
                              "expanded_limit_bytes"} and
            manifest["schema"] == SCHEMA and manifest["source"] == source and
            manifest["platform"] == "linux/amd64" and
            type(manifest["expanded_limit_bytes"]) is int and
            0 < manifest["expanded_limit_bytes"] <= 268_435_456 and
            type(manifest["files"]) is list and len(manifest["files"]) == 4)
    seen = set()
    total = 0
    selected = {}
    for item in manifest["files"]:
        require(type(item) is dict and set(item) == {"role", "path", "size", "sha256"})
        role = item["role"]
        require(role in FILES and role not in seen and item["path"] == FILES[role] and
                type(item["size"]) is int and
                0 < item["size"] <= manifest["expanded_limit_bytes"] and
                type(item["sha256"]) is str and len(item["sha256"]) == 64)
        path = root / "assets" / item["path"]
        raw = _read_file(path, manifest["expanded_limit_bytes"])
        require(len(raw) == item["size"] and
                hashlib.sha256(raw).hexdigest() == item["sha256"])
        selected[role] = (path, item["sha256"])
        seen.add(role)
        total += item["size"]
    require(seen == set(FILES) and total <= manifest["expanded_limit_bytes"] and
            selected["binary"][1] == pin["binary_sha256"] and
            selected["library"][1] == pin["parser_sha256"] and
            selected["example"][1] == pin["example_sha256"])
    _elf_amd64(selected["binary"][0])
    _elf_amd64(selected["library"][0])
    require(type(private_roots) is tuple and bool(private_roots) and
            all(type(value) is str and value.startswith("/") and len(value) >= 8
                for value in private_roots))
    forbidden = (*FIXED_PRIVATE_PREFIXES,
                 *(value.encode() for value in private_roots))
    for role, (path, _) in selected.items():
        data = _read_file(path, manifest["expanded_limit_bytes"])
        if any(prefix in data for prefix in forbidden):
            if isinstance(diag, dict):
                diag["code"] = f"BUILD_ASSETS_PREFIX_{role.upper()}"
            require(False)
    result, _ = _json(root / "result.json", 4_096)
    require(set(result) == {"schema", "status", "source", "assets_digest",
                            "performance_qualification"} and
            result["schema"] == RESULT_SCHEMA and result["status"] == "passed" and
            result["source"] == source and
            result["assets_digest"] == pin["assets_digest"] and
            result["performance_qualification"] is False)
    receipt, _ = _json(selected["receipt"][0], 16_384)
    require(set(receipt) == {"schema", "source", "version", "binary_sha256",
                             "parser_sha256", "example_source_sha256",
                             "parser_record_sha256", "cargo_lock_sha256",
                             "rust_version", "parser_contract", "build", "smoke",
                             "completion_smoke", "qualification"} and
            receipt["schema"] == RECEIPT_SCHEMA and receipt["source"] == source and
            receipt["version"] == pin["version"] and
            receipt["binary_sha256"] == pin["binary_sha256"] and
            receipt["parser_sha256"] == pin["parser_sha256"] and
            receipt["example_source_sha256"] == pin["example_sha256"] and
            receipt["parser_record_sha256"] == pin["parser_record_sha256"] and
            receipt["cargo_lock_sha256"] == pin["cargo_lock_sha256"] and
            receipt["rust_version"] == pin["rust_version"] and
            receipt["parser_contract"] == pin["parser_contract"] and
            receipt["qualification"] == "asset_only")
    build = receipt["build"]
    require(type(build) is dict and set(build) == {"command", "incremental",
                                                  "release_debug", "path_remap_applied"} and
            build["command"] == COMMAND and build["incremental"] is False and
            build["release_debug"] is False and
            build["path_remap_applied"] is True)
    _smoke(receipt["smoke"])
    completion = receipt["completion_smoke"]
    require(type(completion) is dict and set(completion) == {"commit", "rollback"})
    for mode in ("commit", "rollback"):
        _smoke(completion[mode], mode)
    return {role: path for role, (path, _) in selected.items()}


def verify(root, pin, *, private_roots, diag=None):
    """Return four exact local member paths or one fixed asset failure."""
    try:
        return _verify(root, pin, private_roots, diag=diag)
    except Exception:
        raise ValueError("PUBLIC_ASSET") from None
