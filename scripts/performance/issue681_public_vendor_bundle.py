"""Create and independently verify a public-purpose offline Cargo vendor ZIP."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tomllib
import zipfile

import issue681_hosted_acquire_source as source

SCHEMA = "alopex.issue681-public-cargo-vendor/v1"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
MAX_ZIP = 1_000_000_000
MAX_EXPANDED = 1_200_000_000
MAX_ARCHIVE = 16_000_000
MAX_MEMBER = 80_000_000
DATE = (1980, 1, 1, 0, 0, 0)
FILE_MODE = stat.S_IFREG | 0o644
CHECKSUM_COMMENT = ("This file only protects against accidental modifications. "
                    "It is not a security mechanism and does not protect "
                    "against malicious changes.")
MANIFEST_KEYS = {"schema", "release_version", "sources",
                 "common_example_sha256", "package_count", "packages",
                 "vendor_file_count", "vendor_content_bytes",
                 "vendor_tree_sha256"}


def require(value):
    if not value:
        raise ValueError("PUBLIC_VENDOR")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _unique(items):
    value = {}
    for key, item in items:
        require(key not in value)
        value[key] = item
    return value


def _json(raw, *, canonical=True):
    require(type(raw) is bytes and len(raw) <= 1_000_000)
    value = json.loads(raw, object_pairs_hook=_unique)
    require(type(value) is dict)
    if canonical:
        require(raw == (json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode())
    return value


def _locked_packages(locks):
    require(type(locks) is dict and set(locks) == {"before", "candidate"})
    packages = {}
    for raw in locks.values():
        require(type(raw) is bytes and 0 < len(raw) <= 1_000_000)
        data = tomllib.loads(raw.decode("utf-8"))
        require(data.get("version") == 4 and type(data.get("package")) is list)
        for item in data["package"]:
            registry = item.get("source")
            if registry is None:
                continue
            require(registry == REGISTRY and
                    type(item.get("name")) is str and
                    re.fullmatch(r"[A-Za-z0-9_-]+", item["name"]) and
                    type(item.get("version")) is str and
                    re.fullmatch(r"[0-9A-Za-z.+-]+", item["version"]) and
                    type(item.get("checksum")) is str and
                    re.fullmatch(r"[0-9a-f]{64}", item["checksum"]))
            key = (item["name"], item["version"])
            require(key not in packages or packages[key] == item["checksum"])
            packages[key] = item["checksum"]
    require(1 <= len(packages) <= 1000)
    return packages


def _manifest(raw, locks):
    value = _json(raw)
    require(set(value) == MANIFEST_KEYS and value["schema"] == SCHEMA and
            value["release_version"] == "0.8.17" and
            type(value["sources"]) is dict and
            set(value["sources"]) == {"before", "candidate"} and
            type(value["common_example_sha256"]) is str and
            re.fullmatch(r"[0-9a-f]{64}", value["common_example_sha256"]) and
            type(value["vendor_tree_sha256"]) is str and
            re.fullmatch(r"[0-9a-f]{64}", value["vendor_tree_sha256"]) and
            type(value["vendor_file_count"]) is int and
            1 <= value["vendor_file_count"] <= 100_000 and
            type(value["vendor_content_bytes"]) is int and
            1 <= value["vendor_content_bytes"] <= MAX_EXPANDED)
    for arm in ("before", "candidate"):
        pin = source.PINS[arm]
        observed = value["sources"][arm]
        require(type(observed) is dict and
                set(observed) == {"commit", "tree", "cargo_lock_sha256"} and
                observed["commit"] == pin["commit"] and
                observed["tree"] == pin["tree"] and
                digest(locks[arm]) == pin["lock"] and
                observed["cargo_lock_sha256"] == digest(locks[arm]) and
                value["common_example_sha256"] == pin["example"])
    packages = _locked_packages(locks)
    require(type(value["package_count"]) is int and
            value["package_count"] == len(packages) and
            type(value["packages"]) is list and
            len(value["packages"]) == len(packages))
    observed_packages = {}
    for item in value["packages"]:
        require(type(item) is dict and
                set(item) == {"crate", "version", "archive_sha256"})
        key = (item["crate"], item["version"])
        require(key in packages and key not in observed_packages and
                item["archive_sha256"] == packages[key])
        observed_packages[key] = item["archive_sha256"]
    require(set(observed_packages) == set(packages) and
            value["packages"] == sorted(value["packages"],
                                         key=lambda x: (x["crate"], x["version"])))
    return value, packages


def _safe_name(name):
    require(type(name) is str and name and "\\" not in name and
            not name.startswith("/") and not name.endswith("/"))
    parts = PurePosixPath(name).parts
    require(all(part not in ("", ".", "..") for part in parts) and
            PurePosixPath(*parts).as_posix() == name)


def _archive_files(raw, package):
    require(0 < len(raw) <= MAX_ARCHIVE)
    files = {}
    total = 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as archive:
        for member in archive:
            require(member.isfile() and 0 <= member.size <= MAX_MEMBER)
            _safe_name(member.name)
            parts = PurePosixPath(member.name).parts
            require(len(parts) >= 2 and parts[0] == package)
            name = PurePosixPath(*parts[1:]).as_posix()
            require(name not in files)
            total += member.size
            require(total <= MAX_MEMBER)
            stream = archive.extractfile(member)
            require(stream is not None)
            data = stream.read(MAX_MEMBER + 1)
            require(len(data) == member.size)
            files[name] = data
    return files


def _zip_info(name):
    info = zipfile.ZipInfo(name, DATE)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = FILE_MODE << 16
    return info


def _member(raw_zip, name, limit):
    info = raw_zip.getinfo(name)
    require(0 <= info.file_size <= limit)
    data = raw_zip.read(info)
    require(len(data) == info.file_size)
    return data


def verify_bundle(path, locks, *, forbidden_roots):
    """Independently authenticate one complete ZIP or raise fixed failure."""
    try:
        require(type(forbidden_roots) is tuple and bool(forbidden_roots) and
                all(type(root) is bytes and len(root) >= 8
                    for root in forbidden_roots))
        require(isinstance(path, Path) and path.is_file() and
                not path.is_symlink() and 0 < path.stat().st_size <= MAX_ZIP)
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            require(len(names) == len(set(names)) and names and
                    names[0] == "manifest.json" and
                    all(info.date_time == DATE and info.create_system == 3 and
                        info.compress_type == zipfile.ZIP_DEFLATED and
                        info.external_attr >> 16 == FILE_MODE and
                        not info.extra and not info.comment and
                        0 <= info.file_size <= MAX_MEMBER
                        for info in infos) and
                    sum(info.file_size for info in infos) <= MAX_EXPANDED)
            for name in names:
                _safe_name(name)
            manifest_raw = _member(archive, "manifest.json", 1_000_000)
            require(not any(root in manifest_raw for root in forbidden_roots))
            manifest, packages = _manifest(manifest_raw, locks)
            expected = {"manifest.json"}
            digest_rows = {}
            for (crate, version), checksum in sorted(packages.items()):
                package = crate + "-" + version
                archive_name = "archives/" + package + ".crate"
                metadata_name = "vendor/" + package + "/.cargo-checksum.json"
                expected.update((archive_name, metadata_name))
                raw = _member(archive, archive_name, MAX_ARCHIVE)
                require(digest(raw) == checksum)
                original = _archive_files(raw, package)
                metadata_raw = _member(archive, metadata_name, 1_000_000)
                metadata = _json(metadata_raw, canonical=False)
                require(set(metadata) == {"$comment", "files", "package"} and
                        metadata["$comment"] == CHECKSUM_COMMENT and
                        metadata["package"] == checksum and
                        type(metadata["files"]) is dict and
                        not any(root in metadata_raw for root in forbidden_roots))
                digest_rows[metadata_name[7:]] = (len(metadata_raw),
                                                  digest(metadata_raw))
                for relative, file_digest in metadata["files"].items():
                    _safe_name(relative)
                    require(relative in original and
                            type(file_digest) is str and
                            re.fullmatch(r"[0-9a-f]{64}", file_digest))
                    name = "vendor/" + package + "/" + relative
                    expected.add(name)
                    data = _member(archive, name, MAX_MEMBER)
                    require(digest(data) == file_digest and
                            data == original[relative] and
                            not any(root in data for root in forbidden_roots))
                    digest_rows[name[7:]] = (len(data), file_digest)
                omitted = set(original) - set(metadata["files"])
                require(all(PurePosixPath(name).name in
                            (".gitignore", ".gitattributes") for name in omitted))
            ordered = (["manifest.json"] +
                       sorted(name for name in expected if name.startswith("archives/")) +
                       sorted(name for name in expected if name.startswith("vendor/")))
            require(names == ordered and
                    len(digest_rows) == manifest["vendor_file_count"] and
                    sum(size for size, _ in digest_rows.values()) ==
                    manifest["vendor_content_bytes"])
            tree = hashlib.sha256()
            for name, (size, file_digest) in sorted(digest_rows.items()):
                tree.update(json.dumps([name, size, file_digest],
                                       separators=(",", ":")).encode() + b"\n")
            require(tree.hexdigest() == manifest["vendor_tree_sha256"])
            return manifest
    except BaseException:
        raise ValueError("PUBLIC_VENDOR") from None


def write_bundle(vendor, manifest_path, cache_roots, output, locks, *,
                 forbidden_roots):
    """Write a deterministic candidate, reopen it, then publish locally."""
    partial = output.with_name(output.name + ".partial")
    try:
        require(isinstance(vendor, Path) and vendor.is_dir() and
                not vendor.is_symlink() and isinstance(output, Path) and
                output.is_absolute() and not output.exists() and
                not partial.exists() and output.parent.is_dir() and
                not output.parent.is_symlink() and
                output.parent.stat().st_uid == os.geteuid() and
                stat.S_IMODE(output.parent.stat().st_mode) == 0o700 and
                type(cache_roots) is tuple and bool(cache_roots))
        manifest_raw = manifest_path.read_bytes()
        _, packages = _manifest(manifest_raw, locks)
        archives = {}
        for crate, version in packages:
            name = crate + "-" + version + ".crate"
            matches = [root / name for root in cache_roots
                       if (root / name).is_file()]
            require(len(matches) == 1 and not matches[0].is_symlink())
            archives[name] = matches[0]
        fd = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "wb") as stream, zipfile.ZipFile(
                stream, mode="w", compression=zipfile.ZIP_DEFLATED,
                compresslevel=6, allowZip64=True) as archive:
            items = [("manifest.json", manifest_path)]
            items += [("archives/" + name, path)
                      for name, path in sorted(archives.items())]
            for path in sorted(vendor.rglob("*"),
                               key=lambda item: item.relative_to(vendor).as_posix()):
                info = path.lstat()
                require(info.st_uid == os.geteuid() and
                        not stat.S_ISLNK(info.st_mode))
                if stat.S_ISDIR(info.st_mode):
                    continue
                require(stat.S_ISREG(info.st_mode))
                items.append(("vendor/" + path.relative_to(vendor).as_posix(), path))
            for name, path in items:
                _safe_name(name)
                info = path.lstat()
                require(stat.S_ISREG(info.st_mode) and
                        info.st_uid == os.geteuid() and
                        not path.is_symlink())
                with archive.open(_zip_info(name), "w") as member, path.open("rb") as input_file:
                    while block := input_file.read(1_048_576):
                        member.write(block)
                        require(stream.tell() <= MAX_ZIP)
        verify_bundle(partial, locks, forbidden_roots=forbidden_roots)
        total = hashlib.sha256()
        with partial.open("rb") as input_file:
            while block := input_file.read(1_048_576):
                total.update(block)
        os.replace(partial, output)
        return total.hexdigest()
    except BaseException:
        try:
            partial.unlink(missing_ok=True)
        except Exception:
            pass
        raise ValueError("PUBLIC_VENDOR") from None
