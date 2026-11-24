"""Reject nonpublic files before uploading a native subject asset bundle."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import zipfile

SCHEMA = "alopex.native-assets/v1"
ROLES = {
    "server": "bin/alopex-server",
    "library": "lib/libalopex_sql_parser.so",
    "receipt": "receipts/build.json",
    "harness": "harness.tar",
}
SUPPORTED_VERSIONS = {"0.8.16", "0.8.17"}
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def require(value):
    if not value:
        raise ValueError("PUBLIC_OUTPUT")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1_048_576), b""):
            value.update(chunk)
    return value.hexdigest()


def private_markers():
    markers = {b"/home/runner/work/", b"/Users/", b"/private/var/",
               b"C:\\Users\\", b"path+file:///"}
    for key in ("GITHUB_WORKSPACE", "RUNNER_TEMP", "HOME"):
        value = os.environ.get(key)
        if value and Path(value).is_absolute() and len(value) > 4:
            markers.add(os.fsencode(value))
    return tuple(sorted(markers))


def scan_stream(stream, markers):
    overlap = max(map(len, markers)) - 1
    tail = b""
    while chunk := stream.read(1_048_576):
        data = tail + chunk
        require(not any(marker in data for marker in markers))
        tail = data[-overlap:]


def scan_public_assets(export, manifest):
    markers = private_markers()
    for item in manifest["files"]:
        path = export / "assets" / item["path"]
        with path.open("rb") as stream:
            scan_stream(stream, markers)
        if item["role"] == "wheel":
            with zipfile.ZipFile(path) as package:
                entries = package.infolist()
                require(0 < len(entries) <= 64 and
                        sum(entry.file_size for entry in entries) <= 268_435_456)
                for entry in entries:
                    if not entry.is_dir():
                        with package.open(entry) as stream:
                            scan_stream(stream, markers)


def read_canonical(path, limit):
    require(path.is_file() and not path.is_symlink() and
            0 < path.stat().st_size <= limit)
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
        expected = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    except (ValueError, UnicodeError, TypeError) as error:
        raise ValueError("PUBLIC_OUTPUT") from error
    require(raw == expected and type(value) is dict)
    return value


def verify(export, *, asset_verifier=None):
    """Validate the exact success or failure export and all file identities."""
    export = Path(export)
    require(export.is_dir() and not export.is_symlink() and
            export.stat().st_uid == os.geteuid() and
            stat.S_IMODE(export.stat().st_mode) == 0o700)
    result = read_canonical(export / "result.json", 4096)
    require(result.get("schema") == SCHEMA)
    if result.get("status") == "failed":
        require(set(result) == {"schema", "status"})
        expected_files = {"result.json"}
        expected_dirs = set()
    else:
        require(result.get("status") == "passed" and
                set(result) == {"schema", "status", "source", "assets_digest",
                                "performance_qualification"} and
                result["performance_qualification"] is False and
                type(result["source"]) is dict and
                set(result["source"]) == {"commit", "tree", "profile"} and
                result["source"]["profile"] == "release" and
                type(result["source"]["commit"]) is str and
                HEX40.fullmatch(result["source"]["commit"]) and
                type(result["source"]["tree"]) is str and
                HEX40.fullmatch(result["source"]["tree"]) and
                type(result["assets_digest"]) is str and
                HEX64.fullmatch(result["assets_digest"]))
        manifest_path = export / "assets/assets.json"
        manifest = read_canonical(manifest_path, 16384)
        require(set(manifest) == {"schema", "platform", "source_commit",
                                 "harness_commit", "expanded_limit_bytes", "files"} and
                manifest["schema"] == "alopex.test-env-assets/v1" and
                manifest["platform"] == "linux/amd64" and
                manifest["source_commit"] == result["source"]["commit"] and
                type(manifest["harness_commit"]) is str and
                HEX40.fullmatch(manifest["harness_commit"]) and
                type(manifest["expanded_limit_bytes"]) is int and
                0 < manifest["expanded_limit_bytes"] <= 268_435_456 and
                type(manifest["files"]) is list and len(manifest["files"]) == 5 and
                digest(manifest_path) == result["assets_digest"])
        roles = {}
        expected_files = {"result.json", "assets/assets.json"}
        total = 0
        for item in manifest["files"]:
            require(type(item) is dict and set(item) == {"role", "path", "size", "sha256"})
            role, name = item["role"], item["path"]
            require(type(role) is str and role not in roles and
                    type(name) is str and
                    (name == ROLES.get(role) or role == "wheel" and
                     name in {f"wheels/alopex-{version}-cp38-abi3-linux_x86_64.whl"
                              for version in SUPPORTED_VERSIONS}) and
                    type(item["size"]) is int and 0 < item["size"] <= 268_435_456 and
                    type(item["sha256"]) is str and HEX64.fullmatch(item["sha256"]))
            roles[role] = name
            path = export / "assets" / name
            require(path.is_file() and not path.is_symlink() and
                    path.stat().st_size == item["size"] and digest(path) == item["sha256"])
            expected_files.add("assets/" + name)
            total += item["size"]
        require(set(roles) == set(ROLES) | {"wheel"} and total <= 268_435_456)
        receipt = read_canonical(export / "assets" / roles["receipt"], 16384)
        require(receipt.get("schema") == "alopex.performance-build/v1" and
                type(receipt.get("version")) is str and
                receipt["version"] in SUPPORTED_VERSIONS and
                receipt.get("source") == result["source"] and
                manifest["source_commit"] == receipt["source"]["commit"] and
                roles["wheel"] ==
                f"wheels/alopex-{receipt['version']}-cp38-abi3-linux_x86_64.whl" and
                receipt.get("server_sha256") == next(item["sha256"] for item in manifest["files"]
                                                     if item["role"] == "server") and
                receipt.get("python_wheel_sha256") == next(item["sha256"] for item in manifest["files"]
                                                           if item["role"] == "wheel") and
                receipt.get("parser_sha256") == next(item["sha256"] for item in manifest["files"]
                                                     if item["role"] == "library"))
        expected_dirs = {"assets", "assets/bin", "assets/lib", "assets/wheels",
                         "assets/receipts"}
        if asset_verifier is None:
            sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test-env"))
            from common import verify_assets
            asset_verifier = verify_assets
        asset_verifier(manifest_path, "linux/amd64")
        scan_public_assets(export, manifest)
    seen_files, seen_dirs = set(), set()
    for path in export.rglob("*"):
        relative = path.relative_to(export).as_posix()
        info = path.lstat()
        require(info.st_uid == os.geteuid() and not stat.S_ISLNK(info.st_mode))
        if stat.S_ISDIR(info.st_mode):
            require(stat.S_IMODE(info.st_mode) == 0o700)
            seen_dirs.add(relative)
        else:
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                    stat.S_IMODE(info.st_mode) ==
                    (0o700 if relative == "assets/bin/alopex-server" else 0o600))
            seen_files.add(relative)
    require(seen_files == expected_files and seen_dirs == expected_dirs)
    return result["status"]


def main():
    try:
        root = Path(os.environ["GITHUB_WORKSPACE"]).resolve(strict=True)
        status = verify(root / "native-assets-export")
        print(json.dumps({"case": "NATIVE_PUBLIC_EXPORT", "outcome": status}, sort_keys=True))
        return 0
    except BaseException:
        print('{"case":"NATIVE_PUBLIC_EXPORT","outcome":"failed","error_code":"PUBLIC_OUTPUT"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
