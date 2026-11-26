"""Offline admission of two independently pinned public native references.

The caller obtains run/artifact metadata and wrapper ZIPs separately. This
module performs no network access and never treats those values as pins.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import zipfile

SUBJECTS = ("before", "published")
SOURCE_COMMIT = {
    "before": "34ce28d5c15377837cb78b15a261cfa2c05757a8",
    "published": "d9ff8783b8d18a63d167204e55902ea6cd90dcd1",
}
VERSION = {"before": "0.8.17", "published": "0.8.16"}
ROLES = {"server": "bin/alopex-server", "library": "lib/libalopex_sql_parser.so",
         "receipt": "receipts/build.json", "harness": "harness.tar"}
PIN_FIELDS = {"run_id", "head_sha", "repository_id", "workflow_id", "artifact_id",
              "artifact_name", "zip_sha256", "zip_size_bytes", "manifest_sha256",
              "result_sha256", "source_commit", "source_tree", "harness_commit",
              "version", "platform", "files"}
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MAX_ZIP = 300_000_000
MAX_EXPANDED = 268_435_456
MAX_FILE = 268_435_456
CHUNK = 1_048_576
STATIC_MARKERS = (b"/home/runner/work/", b"/Users/", b"/private/var/",
                  b"C:\\Users\\", b"path+file:///")


def require(ok, code):
    if not ok:
        raise ValueError(code)


def hex_digest(value, length):
    return type(value) is str and bool((HEX40 if length == 40 else HEX64).fullmatch(value))


def _json(data, code):
    require(type(data) is bytes and 0 < len(data) <= 65_536, code)
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, code)
            result[key] = value
        return result
    try:
        result = json.loads(data, object_pairs_hook=unique)
    except (UnicodeError, ValueError) as error:
        raise ValueError(code) from error
    require(type(result) is dict, code)
    return result


def _wheel_path(version):
    return f"wheels/alopex-{version}-cp38-abi3-linux_x86_64.whl"


def _files(pin):
    records = pin["files"]
    require(type(records) is list and len(records) == 5, "PUBLIC_PIN")
    by_role = {}
    for item in records:
        require(type(item) is dict and set(item) == {"role", "path", "size", "sha256"},
                "PUBLIC_PIN")
        role = item["role"]
        require(type(role) is str and role not in by_role and
                role in set(ROLES) | {"wheel"} and
                item["path"] == (ROLES.get(role) if role != "wheel" else
                                 _wheel_path(pin["version"])) and
                type(item["size"]) is int and 0 < item["size"] <= MAX_FILE and
                hex_digest(item["sha256"], 64), "PUBLIC_PIN")
        by_role[role] = item
    require(set(by_role) == set(ROLES) | {"wheel"} and
            sum(item["size"] for item in records) <= MAX_EXPANDED, "PUBLIC_PIN")
    return by_role


def _pin(subject, pin):
    require(type(pin) is dict and set(pin) == PIN_FIELDS and
            type(pin["run_id"]) is int and pin["run_id"] > 0 and
            type(pin["repository_id"]) is int and pin["repository_id"] > 0 and
            type(pin["workflow_id"]) is int and pin["workflow_id"] > 0 and
            type(pin["artifact_id"]) is int and pin["artifact_id"] > 0 and
            pin["artifact_name"] == "native-product-assets" and
            hex_digest(pin["head_sha"], 40) and
            hex_digest(pin["source_commit"], 40) and
            pin["source_commit"] == SOURCE_COMMIT[subject] and
            hex_digest(pin["source_tree"], 40) and
            hex_digest(pin["harness_commit"], 40) and
            pin["version"] == VERSION[subject] and
            pin["platform"] == "linux/amd64" and
            hex_digest(pin["zip_sha256"], 64) and
            type(pin["zip_size_bytes"]) is int and
            0 < pin["zip_size_bytes"] <= MAX_ZIP and
            hex_digest(pin["manifest_sha256"], 64) and
            hex_digest(pin["result_sha256"], 64), "PUBLIC_PIN")
    return _files(pin)


def _remote(pin, run, artifact):
    require(type(run) is dict and run.get("id") == pin["run_id"] and
            run.get("head_sha") == pin["head_sha"] and
            type(run.get("repository")) is dict and
            run["repository"].get("id") == pin["repository_id"] and
            run.get("workflow_id") == pin["workflow_id"] and
            type(run.get("run_attempt")) is int and run["run_attempt"] == 1 and
            run.get("event") == "workflow_dispatch" and
            run.get("status") == "completed" and run.get("conclusion") == "success",
            "PUBLIC_RUN")
    require(type(artifact) is dict and artifact.get("id") == pin["artifact_id"] and
            artifact.get("name") == pin["artifact_name"] and
            artifact.get("digest") == "sha256:" + pin["zip_sha256"] and
            type(artifact.get("size_in_bytes")) is int and
            artifact["size_in_bytes"] == pin["zip_size_bytes"] and
            artifact.get("expired") is False and
            type(artifact.get("workflow_run")) is dict and
            artifact["workflow_run"].get("id") == pin["run_id"] and
            artifact["workflow_run"].get("head_sha") == pin["head_sha"],
            "PUBLIC_ARTIFACT")


def _private_directory(path):
    require(isinstance(path, Path) and path.is_absolute(), "PUBLIC_DESTINATION")
    for component in (*reversed(path.parents), path):
        info = component.lstat()
        require(stat.S_ISDIR(info.st_mode), "PUBLIC_DESTINATION")
    info = path.lstat()
    require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700 and
            not any(path.iterdir()), "PUBLIC_DESTINATION")


def _regular_wrapper(path, pin):
    require(isinstance(path, Path) and path.is_absolute(), "PUBLIC_WRAPPER")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600 and
                info.st_size == pin["zip_size_bytes"],
                "PUBLIC_WRAPPER")
        digest = hashlib.sha256()
        while block := stream.read(CHUNK):
            digest.update(block)
        require(digest.hexdigest() == pin["zip_sha256"], "PUBLIC_ZIP_BYTES")


def _markers(private_roots):
    require(type(private_roots) in (tuple, list) and private_roots and
            all(type(root) is str and len(root) > 4 and Path(root).is_absolute()
                for root in private_roots), "PUBLIC_PRIVACY_ROOTS")
    values = set(STATIC_MARKERS)
    for root in private_roots:
        values.add(os.fsencode(root))
    for key in ("GITHUB_WORKSPACE", "RUNNER_TEMP", "HOME"):
        root = os.environ.get(key)
        if root and len(root) > 4 and Path(root).is_absolute():
            values.add(os.fsencode(root))
    return tuple(sorted(values))


def _scan(stream, markers):
    tail = b""
    overlap = max(map(len, markers)) - 1
    while block := stream.read(CHUNK):
        data = tail + block
        require(not any(marker in data for marker in markers), "PUBLIC_PRIVACY")
        tail = data[-overlap:]


def _scan_wheel(path, markers):
    try:
        with zipfile.ZipFile(path) as wheel:
            entries = wheel.infolist()
            require(0 < len(entries) <= 64 and
                    sum(item.file_size for item in entries) <= MAX_FILE,
                    "PUBLIC_WHEEL")
            seen = set()
            for item in entries:
                name = item.filename
                mode = item.external_attr >> 16
                parts = name.rstrip("/").split("/")
                require(name not in seen and name and not name.startswith("/") and
                        "\\" not in name and all(part not in ("", ".", "..") for part in parts) and
                        not item.flag_bits & 1 and
                        stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR) and
                        (item.is_dir() or stat.S_IFMT(mode) != stat.S_IFDIR),
                        "PUBLIC_WHEEL")
                seen.add(name)
                if not item.is_dir():
                    with wheel.open(item) as stream:
                        _scan(stream, markers)
    except (zipfile.BadZipFile, EOFError) as error:
        raise ValueError("PUBLIC_WHEEL") from error


def _extract(wrapper, target, pin, files, markers):
    names = {"result.json", "assets/assets.json"} | {
        "assets/" + item["path"] for item in files.values()}
    hashes = {"result.json": pin["result_sha256"],
              "assets/assets.json": pin["manifest_sha256"]}
    hashes.update({"assets/" + item["path"]: item["sha256"] for item in files.values()})
    sizes = {"result.json": 4096, "assets/assets.json": 16_384}
    sizes.update({"assets/" + item["path"]: item["size"] for item in files.values()})
    try:
        fd = os.open(wrapper, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as source:
            info = os.fstat(source.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                    info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600 and
                    info.st_size == pin["zip_size_bytes"],
                    "PUBLIC_WRAPPER")
            digest = hashlib.sha256()
            while block := source.read(CHUNK):
                digest.update(block)
            require(digest.hexdigest() == pin["zip_sha256"], "PUBLIC_ZIP_BYTES")
            source.seek(0)
            with zipfile.ZipFile(source) as archive:
                _extract_members(archive, target, pin, files, markers, names, hashes, sizes)
        _scan_wheel(target / "assets" / files["wheel"]["path"], markers)
    except (zipfile.BadZipFile, EOFError) as error:
        raise ValueError("PUBLIC_ZIP") from error


def _extract_members(archive, target, pin, files, markers, names, hashes, sizes):
    entries = archive.infolist()
    require(len(entries) == 7 and {item.filename for item in entries} == names,
            "PUBLIC_MEMBERS")
    for item in entries:
        mode = item.external_attr >> 16
        require(not item.is_dir() and not item.flag_bits & 1 and
                stat.S_IFMT(mode) in (0, stat.S_IFREG) and
                0 < item.file_size <= sizes[item.filename] and
                (item.filename in ("result.json", "assets/assets.json") or
                 item.file_size == sizes[item.filename]) and
                item.filename != "private-evidence.cms", "PUBLIC_MEMBERS")
    for item in entries:
        path = target.joinpath(*item.filename.split("/"))
        current = target
        for component in path.relative_to(target).parts[:-1]:
            current = current / component
            current.mkdir(mode=0o700, exist_ok=True)
            info = current.lstat()
            require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                    stat.S_IMODE(info.st_mode) == 0o700, "PUBLIC_DESTINATION")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0),
                     0o700 if item.filename == "assets/bin/alopex-server" else 0o600)
        digest = hashlib.sha256()
        count = 0
        with os.fdopen(fd, "wb") as output, archive.open(item) as source:
            while block := source.read(CHUNK):
                count += len(block)
                require(count <= sizes[item.filename], "PUBLIC_SIZE")
                digest.update(block)
                output.write(block)
            output.flush()
            os.fsync(output.fileno())
        require(count == item.file_size and digest.hexdigest() == hashes[item.filename],
                "PUBLIC_FILE_BYTES")
        with path.open("rb") as stream:
            _scan(stream, markers)


def _product(target, pin, files):
    source = {"commit": pin["source_commit"], "tree": pin["source_tree"],
              "profile": "release"}
    result = _json((target / "result.json").read_bytes(), "PUBLIC_RESULT")
    require(result == {"schema": "alopex.native-assets/v1", "status": "passed",
                       "source": source, "assets_digest": pin["manifest_sha256"],
                       "performance_qualification": False}, "PUBLIC_RESULT")
    manifest = _json((target / "assets/assets.json").read_bytes(), "PUBLIC_MANIFEST")
    require(set(manifest) == {"schema", "platform", "source_commit", "harness_commit",
                              "expanded_limit_bytes", "files"} and
            manifest["schema"] == "alopex.test-env-assets/v1" and
            manifest["platform"] == pin["platform"] and
            manifest["source_commit"] == pin["source_commit"] and
            manifest["harness_commit"] == pin["harness_commit"] and
            type(manifest["expanded_limit_bytes"]) is int and
            0 < manifest["expanded_limit_bytes"] <= MAX_EXPANDED and
            manifest["files"] == pin["files"], "PUBLIC_MANIFEST")
    receipt = _json((target / "assets" / files["receipt"]["path"]).read_bytes(),
                    "PUBLIC_RECEIPT")
    require(receipt.get("schema") == "alopex.performance-build/v1" and
            receipt.get("source") == source and
            receipt.get("version") == pin["version"] and
            receipt.get("server_sha256") == files["server"]["sha256"] and
            receipt.get("python_wheel_sha256") == files["wheel"]["sha256"] and
            receipt.get("parser_sha256") == files["library"]["sha256"],
            "PUBLIC_RECEIPT")


def _admit_public_references(*, pins, runs, artifacts, wrappers, destination,
                             private_roots):
    """Verify and extract exactly before/published into an empty private root.

    Caller provides independently reviewed pins and separately obtained remote
    metadata and ZIP paths. Return local manifest paths for matrix.prepare_cell.
    """
    require(all(type(value) is dict and set(value) == set(SUBJECTS) for value in
                (pins, runs, artifacts, wrappers)), "PUBLIC_INVENTORY")
    _private_directory(destination)
    markers = _markers(private_roots)
    reviewed = {}
    run_ids, artifact_ids = set(), set()
    for subject in SUBJECTS:
        pin = pins[subject]
        files = _pin(subject, pin)
        _remote(pin, runs[subject], artifacts[subject])
        require(pin["run_id"] not in run_ids and pin["artifact_id"] not in artifact_ids,
                "PUBLIC_DUPLICATE")
        run_ids.add(pin["run_id"])
        artifact_ids.add(pin["artifact_id"])
        _regular_wrapper(wrappers[subject], pin)
        reviewed[subject] = files
    created = []
    try:
        for subject in SUBJECTS:
            target = destination / subject
            target.mkdir(mode=0o700)
            created.append(target)
            _extract(wrappers[subject], target, pins[subject], reviewed[subject], markers)
            _product(target, pins[subject], reviewed[subject])
        return {subject: destination / subject / "assets/assets.json"
                for subject in SUBJECTS}
    except BaseException:
        for path in created:
            shutil.rmtree(path)
        raise


def admit_public_references(*, pins, runs, artifacts, wrappers, destination,
                            private_roots):
    """Admit reviewed public references; never expose filesystem exception text."""
    try:
        return _admit_public_references(
            pins=pins, runs=runs, artifacts=artifacts, wrappers=wrappers,
            destination=destination, private_roots=private_roots)
    except (OSError, zipfile.BadZipFile, EOFError):
        raise ValueError("PUBLIC_FILESYSTEM") from None
