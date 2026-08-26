#!/usr/bin/env python3
"""Build and verify deterministic Alopex Nim parser release archives."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import stat
import sys
import tarfile
import tempfile
import tomllib
from typing import Any
import zlib


TARGET_RECORD_SCHEMA = "alopex-parser-target-record-v2"
BUILD_IDENTITY_SCHEMA = "alopex-parser-build-identity-v2"
VENDOR_MANIFEST_SCHEMA = "alopex-parser-vendor-manifest-v2"
RELEASE_ENVELOPE_SCHEMA = "alopex-parser-release-envelope-v1"


def _workspace_version() -> str:
    """Read the release version from the workspace's canonical manifest."""
    manifest = Path(__file__).resolve().parents[2] / "Cargo.toml"
    with manifest.open("rb") as stream:
        value = tomllib.load(stream)["workspace"]["package"]["version"]
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value) is None:
        raise RuntimeError(f"invalid workspace release version: {value!r}")
    return value


REQUIRED_ALOPEX_VERSION = _workspace_version()
REQUIRED_CONTRACT_VERSION = "0.15.0"
REQUIRED_NIM_VERSION = "2.2.10"
REQUIRED_NIMBLE_VERSION = "0.22.3"
REQUIRED_NIMBLE_SHA = "42ef70c2102a942c46f13eb76872326edd525cec"
REQUIRED_BUILD_PROFILE = "nim-release-dual-library-v2"

TARGET_LIBRARIES = {
    "aarch64-apple-darwin": "libalopex_sql_parser.dylib",
    "x86_64-apple-darwin": "libalopex_sql_parser.dylib",
    "x86_64-pc-windows-msvc": "alopex_sql_parser.dll",
    "x86_64-unknown-linux-gnu": "libalopex_sql_parser.so",
}
TARGET_STATIC_LIBRARIES = {
    "aarch64-apple-darwin": "libalopex_sql_parser.a",
    "x86_64-apple-darwin": "libalopex_sql_parser.a",
    "x86_64-pc-windows-msvc": "alopex_sql_parser.lib",
    "x86_64-unknown-linux-gnu": "libalopex_sql_parser.a",
}
REQUIRED_PACKAGES = {
    "msgpack4nim": {
        "file_count": 5,
        "manifest": "msgpack4nim.nimble",
        "tree_sha256": (
            "462002b97d57683173c49a0110182f0edb4bfb74d523cb15f569f79bcf88f4fc"
        ),
        "vcs_revision": "f4cc097ca9694f17feced9f82994f583ef7911fe",
        "version": "0.4.4",
    },
    "npeg": {
        "file_count": 16,
        "manifest": "npeg.nimble",
        "tree_sha256": (
            "83cd5c1fd9ee21e81306b5e15a5080c7aac132984ef87d5c804a870b55959b0b"
        ),
        "vcs_revision": "409f6796d0e880b3f0222c964d1da7de6e450811",
        "version": "1.3.0",
    },
}
REQUIRED_PACKAGE_LIST_REVISION = "b79eaaa3fc65fc473bc9e803445f8f7aef7112a2"
REQUIRED_REGISTRY_METADATA = {
    "packages_official.json": {
        "sha256": "e42c394337184b2fe52f65538ea3dc93fb8840efb77ec775261ebd1f7169d329",
        "size": 977981,
        "source_revision": REQUIRED_PACKAGE_LIST_REVISION,
    },
    "packages_temp.json": {
        "sha256": "e42c394337184b2fe52f65538ea3dc93fb8840efb77ec775261ebd1f7169d329",
        "size": 977981,
        "source_revision": REQUIRED_PACKAGE_LIST_REVISION,
    },
}

MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_ARCHIVE_MEMBER_BYTES = 8 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 8
MAX_CANONICAL_JSON_BYTES = 1024 * 1024
MAX_DECOMPRESSED_ARCHIVE_BYTES = 16 * 1024 * 1024
MAX_TREE_FILE_BYTES = 8 * 1024 * 1024
MAX_TOOL_BINARY_BYTES = 32 * 1024 * 1024

PACKING_IDENTITY = {
    "archive_size_limit": MAX_ARCHIVE_BYTES,
    "decompressed_size_limit": MAX_DECOMPRESSED_ARCHIVE_BYTES,
    "format": "tar.gz",
    "gzip_compresslevel": 9,
    "gzip_mtime": 0,
    "member_count_limit": MAX_ARCHIVE_MEMBERS,
    "member_size_limit": MAX_ARCHIVE_MEMBER_BYTES,
    "tar_format": "gnu",
    "tar_gid": 0,
    "tar_mode": "0644",
    "tar_mtime": 0,
    "tar_uid": 0,
}
TARGET_LINKER_FLAGS = {
    "aarch64-apple-darwin": ["-Wl,-x"],
    "x86_64-apple-darwin": ["-Wl,-x"],
    "x86_64-pc-windows-msvc": [
        "-static",
        "-static-libgcc",
        "-s",
        "-Wl,--no-insert-timestamp",
    ],
    "x86_64-unknown-linux-gnu": ["-s"],
}
GZIP_HEADER = bytes.fromhex("1f8b08000000000002ff")
HEX_40 = re.compile(r"^[0-9a-f]{40}$")
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
VERSION_PATTERN = re.compile(
    r'^\s*version\s*=\s*["\']([^"\']+)["\']\s*$', re.MULTILINE
)


class ParserAssetError(ValueError):
    """An input or generated parser asset violated the release contract."""


@dataclass(frozen=True)
class PackageInput:
    name: str
    version: str
    root: Path


@dataclass(frozen=True)
class MetadataInput:
    name: str
    path: Path


def canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def read_regular_file(
    path: Path,
    description: str,
    *,
    nonempty: bool = True,
    max_bytes: int | None = None,
) -> bytes:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ParserAssetError(f"missing {description}: {path}") from error
    if stat.S_ISLNK(metadata.st_mode):
        raise ParserAssetError(f"{description} must not be a symlink: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise ParserAssetError(f"{description} must be a regular file: {path}")
    if max_bytes is not None and metadata.st_size > max_bytes:
        raise ParserAssetError(
            f"{description} exceeds the {max_bytes}-byte size limit"
        )
    try:
        content = path.read_bytes()
    except OSError as error:
        raise ParserAssetError(f"could not read {description}: {path}") from error
    if nonempty and not content:
        raise ParserAssetError(f"{description} must not be empty: {path}")
    if max_bytes is not None and len(content) > max_bytes:
        raise ParserAssetError(
            f"{description} exceeds the {max_bytes}-byte size limit"
        )
    return content


def reject_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ParserAssetError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def parse_json_bytes(content: bytes, description: str) -> Any:
    try:
        return json.loads(
            content.decode("utf-8"), object_pairs_hook=reject_duplicate_object
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ParserAssetError(f"invalid JSON in {description}") from error


def load_canonical_json(path: Path, description: str) -> dict[str, Any]:
    content = read_regular_file(
        path, description, max_bytes=MAX_CANONICAL_JSON_BYTES
    )
    parsed = parse_json_bytes(content, description)
    if not isinstance(parsed, dict):
        raise ParserAssetError(f"{description} must contain a JSON object")
    if content != canonical_json_bytes(parsed):
        raise ParserAssetError(f"{description} is not canonical JSON: {path}")
    return parsed


def require_exact_keys(
    value: dict[str, Any], expected: set[str], description: str
) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ParserAssetError(
            f"{description} keys differ; missing={missing}, extra={extra}"
        )


def require_string(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value:
        raise ParserAssetError(f"{description} must be a non-empty string")
    return value


def require_positive_int(value: Any, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ParserAssetError(f"{description} must be a positive integer")
    return value


def require_digest(value: Any, description: str) -> str:
    digest = require_string(value, description)
    if HEX_64.fullmatch(digest) is None:
        raise ParserAssetError(f"{description} must be a lowercase SHA-256")
    return digest


def safe_archive_path(value: str, description: str) -> str:
    if not value or "\\" in value or "\x00" in value:
        raise ParserAssetError(f"unsafe {description}: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ParserAssetError(f"unsafe {description}: {value!r}")
    if path.as_posix() != value:
        raise ParserAssetError(f"unsafe {description}: {value!r}")
    return value


def source_path_ignored(relative: PurePosixPath, is_dir: bool) -> bool:
    ignored_directories = {".git", ".nimble", "__pycache__", "nimcache", "vendor"}
    if any(part in ignored_directories for part in relative.parts):
        return True
    if relative.parts[:2] == ("tests", "fixtures"):
        return True
    if is_dir:
        return False
    if len(relative.parts) == 1 and relative.name in {
        "CONTRACT_VERSION",
        "SHA256SUMS",
    }:
        return True
    ignored_names = {
        "alopex_sql_parser.dll",
        "libalopex_sql_parser.dylib",
        "libalopex_sql_parser.so",
        "libalopex_sql_parser.a",
        "alopex_sql_parser.lib",
        "poc_msgpack",
        "test_ffi_boundary",
        "test_harness_failure",
        "test_msgpack_output",
        "test_parser",
        "test_promql_parser",
        "test_security_limits",
    }
    return relative.name in ignored_names or relative.suffix in {
        ".dll",
        ".dylib",
        ".exe",
        ".pdb",
        ".so",
        ".a",
        ".lib",
    }


def package_path_ignored(relative: PurePosixPath, is_dir: bool) -> bool:
    del is_dir
    return relative == PurePosixPath("nimblemeta.json") or any(
        part in {".git", "__pycache__", "nimcache"} for part in relative.parts
    )


def normalize_nimble_metadata_path(value: str, description: str) -> str:
    normalized = value.replace("\\", "/")
    if not normalized.startswith("/"):
        raise ParserAssetError(f"{description} must be package-root-relative")
    relative = safe_archive_path(normalized[1:], description)
    return f"/{relative}"


def validate_nimble_metadata(
    metadata: Any,
    *,
    name: str,
    version: str,
    vcs_revision: str,
    source_root: Path,
) -> None:
    if not isinstance(metadata, dict):
        raise ParserAssetError(f"{name} Nimble metadata must be an object")
    require_exact_keys(
        metadata, {"metaData", "version"}, f"{name} Nimble metadata"
    )
    if isinstance(metadata["version"], bool) or metadata["version"] != 1:
        raise ParserAssetError(f"{name} Nimble metadata version must be 1")
    details = metadata["metaData"]
    if not isinstance(details, dict):
        raise ParserAssetError(f"{name} Nimble metaData must be an object")
    require_exact_keys(
        details,
        {
            "binaries",
            "downloadMethod",
            "files",
            "specialVersions",
            "url",
            "vcsRevision",
        },
        f"{name} Nimble metaData",
    )
    if details["downloadMethod"] != "git":
        raise ParserAssetError(f"{name} Nimble download method must be git")
    if details["url"] != "":
        raise ParserAssetError(f"{name} Nimble metadata URL must be empty")
    if details["binaries"] != []:
        raise ParserAssetError(f"{name} Nimble package must declare no binaries")
    if details["specialVersions"] != [version]:
        raise ParserAssetError(
            f"{name} Nimble metadata must declare only version {version}"
        )
    if details["vcsRevision"] != vcs_revision:
        raise ParserAssetError(
            f"{name} VCS revision must be {vcs_revision}, "
            f"found {details['vcsRevision']}"
        )
    files = details["files"]
    if not isinstance(files, list) or not all(
        isinstance(item, str) for item in files
    ):
        raise ParserAssetError(f"{name} Nimble file list must contain strings")
    normalized_files = [
        normalize_nimble_metadata_path(item, f"{name} Nimble file path")
        for item in files
    ]
    if len(normalized_files) != len(set(normalized_files)):
        raise ParserAssetError(f"{name} Nimble file list contains duplicates")
    expected_files: list[str] = []
    for path in sorted(source_root.rglob("*")):
        if not path.is_file():
            continue
        relative = PurePosixPath(path.relative_to(source_root).as_posix())
        if package_path_ignored(relative, False):
            continue
        expected_files.append(f"/{relative.as_posix()}")
    if sorted(normalized_files) != sorted(expected_files):
        raise ParserAssetError(
            f"{name} Nimble file list does not match package content"
        )


def tree_identity(
    root: Path,
    description: str,
    ignored: Callable[[PurePosixPath, bool], bool],
) -> dict[str, Any]:
    try:
        root_metadata = root.lstat()
    except OSError as error:
        raise ParserAssetError(f"missing {description}: {root}") from error
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise ParserAssetError(f"{description} must be a real directory: {root}")

    entries: list[dict[str, Any]] = []
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        retained_directories: list[str] = []
        for name in sorted(directory_names):
            child = directory_path / name
            relative = PurePosixPath(child.relative_to(root).as_posix())
            if ignored(relative, True):
                continue
            if child.is_symlink():
                raise ParserAssetError(
                    f"{description} contains a symlinked directory: {relative}"
                )
            retained_directories.append(name)
        directory_names[:] = retained_directories

        for name in sorted(file_names):
            child = directory_path / name
            relative = PurePosixPath(child.relative_to(root).as_posix())
            if ignored(relative, False):
                continue
            content = read_regular_file(
                child,
                f"{description} member {relative}",
                nonempty=False,
                max_bytes=MAX_TREE_FILE_BYTES,
            )
            entries.append(
                {
                    "path": relative.as_posix(),
                    "sha256": sha256_bytes(content),
                    "size": len(content),
                }
            )
    entries.sort(key=lambda entry: entry["path"])
    if not entries:
        raise ParserAssetError(f"{description} contains no identity files")
    return {
        "file_count": len(entries),
        "tree_sha256": sha256_bytes(canonical_json_bytes(entries)),
    }


def parse_package(value: str) -> PackageInput:
    fields = value.split("=", 2)
    if len(fields) != 3 or not all(fields):
        raise ParserAssetError(
            "package identity must use name=version=directory syntax"
        )
    return PackageInput(fields[0], fields[1], Path(fields[2]))


def parse_metadata(value: str) -> MetadataInput:
    fields = value.split("=", 1)
    if len(fields) != 2 or not all(fields):
        raise ParserAssetError(
            "registry metadata must use canonical-name=file syntax"
        )
    return MetadataInput(fields[0], Path(fields[1]))


def validate_version_inputs(args: argparse.Namespace) -> None:
    expected = {
        "Alopex": (args.alopex_version, REQUIRED_ALOPEX_VERSION),
        "contract": (args.contract_version, REQUIRED_CONTRACT_VERSION),
        "Nim": (args.nim_version, REQUIRED_NIM_VERSION),
        "Nimble": (args.nimble_version, REQUIRED_NIMBLE_VERSION),
    }
    for name, (actual, required) in expected.items():
        if actual != required:
            raise ParserAssetError(f"{name} {required} is required, found {actual}")
    if args.nimble_sha != REQUIRED_NIMBLE_SHA:
        raise ParserAssetError(
            f"Nimble must be built from {REQUIRED_NIMBLE_SHA}, "
            f"found {args.nimble_sha}"
        )
    if args.build_profile != REQUIRED_BUILD_PROFILE:
        raise ParserAssetError(
            f"build profile {REQUIRED_BUILD_PROFILE} is required, "
            f"found {args.build_profile}"
        )


def compile_identity(target: str) -> dict[str, Any]:
    return {
        "arguments": [
            "-d:release",
            "--app:lib",
            "--app:staticlib",
            "--mm:orc",
            "--opt:speed",
        ],
        "dependency_path_policy": "fixed-/tmp-target-qualified-v1",
        "environment": {
            "LANG": "C",
            "LC_ALL": "C",
            "SOURCE_DATE_EPOCH": "0",
            "TZ": "UTC",
            "ZERO_AR_DATE": "1",
        },
        "linker_flags": TARGET_LINKER_FLAGS[target],
        "profile": REQUIRED_BUILD_PROFILE,
        "static_c_compiler": (
            "vcc" if target == "x86_64-pc-windows-msvc" else "default"
        ),
    }


def package_identities(values: Iterable[str]) -> list[dict[str, Any]]:
    packages = [parse_package(value) for value in values]
    by_name: dict[str, PackageInput] = {}
    for package in packages:
        if package.name in by_name:
            raise ParserAssetError(f"duplicate package identity: {package.name}")
        by_name[package.name] = package
    if set(by_name) != set(REQUIRED_PACKAGES):
        raise ParserAssetError(
            "package identities must contain exactly msgpack4nim and npeg"
        )

    identities: list[dict[str, Any]] = []
    for name in sorted(by_name):
        package = by_name[name]
        requirement = REQUIRED_PACKAGES[name]
        required_version = requirement["version"]
        manifest_name = requirement["manifest"]
        if package.version != required_version:
            raise ParserAssetError(
                f"package {name} {required_version} is required, "
                f"found {package.version}"
            )
        manifest_content = read_regular_file(
            package.root / manifest_name, f"{name} package manifest"
        )
        try:
            manifest_text = manifest_content.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ParserAssetError(f"{name} package manifest is not UTF-8") from error
        version_match = VERSION_PATTERN.search(manifest_text)
        if version_match is None or version_match.group(1) != required_version:
            raise ParserAssetError(
                f"{name} package manifest does not declare {required_version}"
            )
        metadata_content = read_regular_file(
            package.root / "nimblemeta.json", f"{name} Nimble metadata"
        )
        metadata = parse_json_bytes(metadata_content, f"{name} Nimble metadata")
        vcs_revision = requirement["vcs_revision"]
        validate_nimble_metadata(
            metadata,
            name=name,
            version=required_version,
            vcs_revision=vcs_revision,
            source_root=package.root,
        )
        identity = tree_identity(
            package.root, f"{name} package", package_path_ignored
        )
        expected_identity = {
            "file_count": requirement["file_count"],
            "tree_sha256": requirement["tree_sha256"],
        }
        if identity != expected_identity:
            raise ParserAssetError(
                f"{name} package content does not match the locked identity; "
                f"expected={expected_identity}, actual={identity}"
            )
        identities.append(
            {
                "name": name,
                "vcs_revision": vcs_revision,
                "version": required_version,
                **identity,
            }
        )
    return identities


def metadata_identities(values: Iterable[str]) -> list[dict[str, Any]]:
    metadata = [parse_metadata(value) for value in values]
    by_name: dict[str, MetadataInput] = {}
    for item in metadata:
        if item.name in by_name:
            raise ParserAssetError(f"duplicate registry metadata: {item.name}")
        by_name[item.name] = item
    if set(by_name) != set(REQUIRED_REGISTRY_METADATA):
        raise ParserAssetError(
            "registry metadata must contain exactly packages_official.json "
            "and packages_temp.json"
        )

    identities: list[dict[str, Any]] = []
    for name in sorted(by_name):
        requirement = REQUIRED_REGISTRY_METADATA[name]
        content = read_regular_file(
            by_name[name].path,
            f"registry metadata {name}",
            max_bytes=requirement["size"],
        )
        parse_json_bytes(content, f"registry metadata {name}")
        if len(content) != requirement["size"]:
            raise ParserAssetError(f"registry metadata size drift: {name}")
        digest = sha256_bytes(content)
        if digest != requirement["sha256"]:
            raise ParserAssetError(f"registry metadata digest drift: {name}")
        identities.append(
            {
                "name": name,
                "sha256": digest,
                "size": len(content),
                "source_revision": requirement["source_revision"],
            }
        )
    return identities


def file_identity(path: Path, description: str) -> dict[str, Any]:
    content = read_regular_file(
        path, description, max_bytes=MAX_TOOL_BINARY_BYTES
    )
    return {"binary_sha256": sha256_bytes(content), "binary_size": len(content)}


def packer_identity() -> dict[str, Any]:
    manifest_tool = Path(__file__).resolve(strict=True)
    build_script = manifest_tool.parents[1] / "build-nim-parser.sh"
    python_binary = Path(sys.executable).resolve(strict=True)
    return {
        "build_script": file_identity(build_script, "parser build script"),
        "manifest_tool": file_identity(manifest_tool, "parser manifest tool"),
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            **file_identity(python_binary, "Python interpreter binary"),
        },
        "zlib": {
            "compile_version": zlib.ZLIB_VERSION,
            "runtime_version": zlib.ZLIB_RUNTIME_VERSION,
        },
    }


def archive_stem(alopex_version: str, contract_version: str, target: str) -> str:
    return (
        f"alopex-parser-v{alopex_version}-contract-{contract_version}-{target}"
    )


def internal_paths(target: str) -> tuple[str, str, str]:
    base = f"alopex-sql-parser/{target}"
    return (
        f"{base}/{TARGET_LIBRARIES[target]}",
        f"{base}/{TARGET_STATIC_LIBRARIES[target]}",
        f"{base}/BUILD_IDENTITY.json",
    )


def build_identity_from_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "alopex_version": record["alopex_version"],
        "builder": record["builder"],
        "contract_version": record["contract_version"],
        "library": record["library"],
        "static_library": record["static_library"],
        "packages": record["packages"],
        "packing": record["packing"],
        "parser_source": record["parser_source"],
        "registry_metadata": record["registry_metadata"],
        "schema": BUILD_IDENTITY_SCHEMA,
        "target": record["target"],
    }


def normalized_archive_bytes(members: dict[str, bytes]) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0
    ) as compressed:
        with tarfile.open(
            fileobj=compressed, mode="w", format=tarfile.GNU_FORMAT
        ) as archive:
            for name in sorted(members):
                safe_archive_path(name, "archive member path")
                content = members[name]
                info = tarfile.TarInfo(name=name)
                info.size = len(content)
                info.mode = 0o644
                info.mtime = 0
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                archive.addfile(info, io.BytesIO(content))
    return raw.getvalue()


def validate_tar_termination(content: bytes, members: list[tarfile.TarInfo]) -> None:
    if not content or len(content) % 512 != 0:
        raise ParserAssetError("archive tar payload is not block aligned")
    data_end = max(
        (member.offset_data + ((member.size + 511) // 512) * 512 for member in members),
        default=0,
    )
    trailer = content[data_end:]
    if len(trailer) < 1024 or any(trailer):
        raise ParserAssetError("archive has a noncanonical or nonzero tar trailer")


def read_normalized_archive(content: bytes) -> dict[str, bytes]:
    if len(content) < len(GZIP_HEADER) or content[:10] != GZIP_HEADER:
        raise ParserAssetError("archive gzip header is not deterministic")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content), mode="rb") as compressed:
            tar_content = compressed.read(MAX_DECOMPRESSED_ARCHIVE_BYTES + 1)
    except (OSError, EOFError) as error:
        raise ParserAssetError("archive gzip stream is invalid") from error
    if len(tar_content) > MAX_DECOMPRESSED_ARCHIVE_BYTES:
        raise ParserAssetError("archive exceeds the decompressed size limit")

    try:
        with tarfile.open(fileobj=io.BytesIO(tar_content), mode="r:") as archive:
            members = archive.getmembers()
            if len(members) > MAX_ARCHIVE_MEMBERS:
                raise ParserAssetError("archive contains too many members")
            validate_tar_termination(tar_content, members)
            result: dict[str, bytes] = {}
            for member in members:
                name = safe_archive_path(member.name, "archive member path")
                if name in result:
                    raise ParserAssetError(f"duplicate archive member: {name}")
                if not member.isreg():
                    raise ParserAssetError(f"archive member is not regular: {name}")
                if member.size > MAX_ARCHIVE_MEMBER_BYTES:
                    raise ParserAssetError(
                        f"archive member exceeds the size limit: {name}"
                    )
                if (
                    member.mode != 0o644
                    or member.mtime != 0
                    or member.uid != 0
                    or member.gid != 0
                    or member.uname != ""
                    or member.gname != ""
                    or member.pax_headers
                ):
                    raise ParserAssetError(
                        f"archive member metadata is not normalized: {name}"
                    )
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise ParserAssetError(f"could not read archive member: {name}")
                result[name] = extracted.read()
            return result
    except (tarfile.TarError, OSError) as error:
        raise ParserAssetError("archive tar stream is invalid") from error


def validate_identity_list(record: dict[str, Any]) -> None:
    parser_source = record["parser_source"]
    if not isinstance(parser_source, dict):
        raise ParserAssetError("parser_source must be an object")
    require_exact_keys(
        parser_source, {"file_count", "tree_sha256"}, "parser_source"
    )
    require_positive_int(parser_source["file_count"], "parser_source.file_count")
    require_digest(parser_source["tree_sha256"], "parser_source.tree_sha256")

    packages = record["packages"]
    if not isinstance(packages, list):
        raise ParserAssetError("packages must be a list")
    package_names: list[str] = []
    for index, package in enumerate(packages):
        if not isinstance(package, dict):
            raise ParserAssetError(f"packages[{index}] must be an object")
        require_exact_keys(
            package,
            {"file_count", "name", "tree_sha256", "vcs_revision", "version"},
            f"packages[{index}]",
        )
        name = require_string(package["name"], f"packages[{index}].name")
        package_names.append(name)
        if name not in REQUIRED_PACKAGES:
            raise ParserAssetError(f"unexpected package identity: {name}")
        requirement = REQUIRED_PACKAGES[name]
        if package["version"] != requirement["version"]:
            raise ParserAssetError(f"wrong package version for {name}")
        if package["vcs_revision"] != requirement["vcs_revision"]:
            raise ParserAssetError(f"wrong package VCS revision for {name}")
        require_positive_int(package["file_count"], f"packages[{index}].file_count")
        require_digest(package["tree_sha256"], f"packages[{index}].tree_sha256")
        if package["file_count"] != requirement["file_count"]:
            raise ParserAssetError(f"wrong package file count for {name}")
        if package["tree_sha256"] != requirement["tree_sha256"]:
            raise ParserAssetError(f"wrong package tree digest for {name}")
    if package_names != sorted(REQUIRED_PACKAGES):
        raise ParserAssetError("package identities are missing, duplicate, or unsorted")

    metadata = record["registry_metadata"]
    if not isinstance(metadata, list):
        raise ParserAssetError("registry_metadata must be a list")
    metadata_names: list[str] = []
    for index, item in enumerate(metadata):
        if not isinstance(item, dict):
            raise ParserAssetError(f"registry_metadata[{index}] must be an object")
        require_exact_keys(
            item,
            {"name", "sha256", "size", "source_revision"},
            f"registry_metadata[{index}]",
        )
        name = require_string(
            item["name"], f"registry_metadata[{index}].name"
        )
        metadata_names.append(name)
        if name not in REQUIRED_REGISTRY_METADATA:
            raise ParserAssetError(f"unexpected registry metadata: {name}")
        requirement = REQUIRED_REGISTRY_METADATA[name]
        require_positive_int(item["size"], f"registry_metadata[{index}].size")
        require_digest(item["sha256"], f"registry_metadata[{index}].sha256")
        if item["size"] != requirement["size"]:
            raise ParserAssetError(f"wrong registry metadata size for {name}")
        if item["sha256"] != requirement["sha256"]:
            raise ParserAssetError(f"wrong registry metadata digest for {name}")
        if item["source_revision"] != requirement["source_revision"]:
            raise ParserAssetError(
                f"wrong registry metadata source revision for {name}"
            )
    if metadata_names != sorted(REQUIRED_REGISTRY_METADATA):
        raise ParserAssetError(
            "registry metadata identities are missing, duplicate, or unsorted"
        )


def validate_builder(builder: Any, target: str) -> None:
    if not isinstance(builder, dict):
        raise ParserAssetError("builder must be an object")
    require_exact_keys(
        builder, {"compile", "nim", "nimble", "packer"}, "builder"
    )
    if builder["compile"] != compile_identity(target):
        raise ParserAssetError("builder compile identity is not exact")
    nim = builder["nim"]
    nimble = builder["nimble"]
    if not isinstance(nim, dict) or not isinstance(nimble, dict):
        raise ParserAssetError("builder tool identities must be objects")
    require_exact_keys(
        nim, {"binary_sha256", "binary_size", "version"}, "builder.nim"
    )
    require_exact_keys(
        nimble,
        {"binary_sha256", "binary_size", "release_sha", "version"},
        "builder.nimble",
    )
    if nim["version"] != REQUIRED_NIM_VERSION:
        raise ParserAssetError("builder Nim version is not exact")
    if nimble["version"] != REQUIRED_NIMBLE_VERSION:
        raise ParserAssetError("builder Nimble version is not exact")
    if nimble["release_sha"] != REQUIRED_NIMBLE_SHA:
        raise ParserAssetError("builder Nimble release SHA is not exact")
    for name, identity in (("nim", nim), ("nimble", nimble)):
        require_positive_int(identity["binary_size"], f"builder.{name}.binary_size")
        require_digest(identity["binary_sha256"], f"builder.{name}.binary_sha256")

    packer = builder["packer"]
    if not isinstance(packer, dict):
        raise ParserAssetError("builder.packer must be an object")
    require_exact_keys(
        packer,
        {"build_script", "manifest_tool", "python", "zlib"},
        "builder.packer",
    )
    for name in ("build_script", "manifest_tool"):
        identity = packer[name]
        if not isinstance(identity, dict):
            raise ParserAssetError(f"builder.packer.{name} must be an object")
        require_exact_keys(
            identity,
            {"binary_sha256", "binary_size"},
            f"builder.packer.{name}",
        )
        require_positive_int(
            identity["binary_size"], f"builder.packer.{name}.binary_size"
        )
        require_digest(
            identity["binary_sha256"], f"builder.packer.{name}.binary_sha256"
        )
    python = packer["python"]
    if not isinstance(python, dict):
        raise ParserAssetError("builder.packer.python must be an object")
    require_exact_keys(
        python,
        {"binary_sha256", "binary_size", "implementation", "version"},
        "builder.packer.python",
    )
    require_positive_int(
        python["binary_size"], "builder.packer.python.binary_size"
    )
    require_digest(
        python["binary_sha256"], "builder.packer.python.binary_sha256"
    )
    require_string(
        python["implementation"], "builder.packer.python.implementation"
    )
    require_string(python["version"], "builder.packer.python.version")
    zlib_identity = packer["zlib"]
    if not isinstance(zlib_identity, dict):
        raise ParserAssetError("builder.packer.zlib must be an object")
    require_exact_keys(
        zlib_identity,
        {"compile_version", "runtime_version"},
        "builder.packer.zlib",
    )
    require_string(
        zlib_identity["compile_version"],
        "builder.packer.zlib.compile_version",
    )
    require_string(
        zlib_identity["runtime_version"],
        "builder.packer.zlib.runtime_version",
    )


def validate_sized_digest(value: Any, description: str, *, include_path: bool) -> None:
    if not isinstance(value, dict):
        raise ParserAssetError(f"{description} must be an object")
    expected = {"sha256", "size"}
    if include_path:
        expected.add("path")
    else:
        expected.add("filename")
    require_exact_keys(value, expected, description)
    require_positive_int(value["size"], f"{description}.size")
    require_digest(value["sha256"], f"{description}.sha256")


def reject_commit_keys(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if "commit" in key.lower():
                raise ParserAssetError("self-referential commit fields are forbidden")
            reject_commit_keys(child)
    elif isinstance(value, list):
        for child in value:
            reject_commit_keys(child)


def validate_record_shape(record: dict[str, Any]) -> None:
    require_exact_keys(
        record,
        {
            "alopex_version",
            "archive",
            "build_identity",
            "builder",
            "contract_version",
            "library",
            "packages",
            "packing",
            "parser_source",
            "registry_metadata",
            "schema",
            "static_library",
            "target",
        },
        "target record",
    )
    reject_commit_keys(record)
    if record["schema"] != TARGET_RECORD_SCHEMA:
        raise ParserAssetError("wrong target record schema")
    if record["alopex_version"] != REQUIRED_ALOPEX_VERSION:
        raise ParserAssetError("wrong Alopex version in target record")
    if record["contract_version"] != REQUIRED_CONTRACT_VERSION:
        raise ParserAssetError("wrong parser contract in target record")
    target = require_string(record["target"], "target")
    if target not in TARGET_LIBRARIES:
        raise ParserAssetError(f"unsupported parser target: {target}")
    if record["packing"] != PACKING_IDENTITY:
        raise ParserAssetError("wrong deterministic packing identity")
    validate_identity_list(record)
    validate_builder(record["builder"], target)
    validate_sized_digest(record["archive"], "archive", include_path=False)
    validate_sized_digest(record["library"], "library", include_path=True)
    validate_sized_digest(
        record["static_library"], "static_library", include_path=True
    )
    validate_sized_digest(
        record["build_identity"], "build_identity", include_path=True
    )

    stem = archive_stem(
        record["alopex_version"], record["contract_version"], target
    )
    if record["archive"]["filename"] != f"{stem}.tar.gz":
        raise ParserAssetError("archive filename does not match target identity")
    library_path, static_library_path, identity_path = internal_paths(target)
    if record["library"]["path"] != library_path:
        raise ParserAssetError("internal library path does not match target")
    if record["static_library"]["path"] != static_library_path:
        raise ParserAssetError("internal static library path does not match target")
    if record["build_identity"]["path"] != identity_path:
        raise ParserAssetError("internal build identity path does not match target")
    safe_archive_path(record["library"]["path"], "library path")
    safe_archive_path(record["static_library"]["path"], "static library path")
    safe_archive_path(record["build_identity"]["path"], "build identity path")


def verify_record_archive(record: dict[str, Any], archive_path: Path) -> None:
    validate_record_shape(record)
    archive_content = read_regular_file(
        archive_path, "target archive", max_bytes=MAX_ARCHIVE_BYTES
    )
    if len(archive_content) != record["archive"]["size"]:
        raise ParserAssetError("outer archive size mismatch")
    if sha256_bytes(archive_content) != record["archive"]["sha256"]:
        raise ParserAssetError("outer archive digest mismatch")

    members = read_normalized_archive(archive_content)
    if normalized_archive_bytes(members) != archive_content:
        raise ParserAssetError("archive bytes are not in canonical packing form")
    expected_paths = {
        record["build_identity"]["path"],
        record["library"]["path"],
        record["static_library"]["path"],
    }
    if set(members) != expected_paths:
        raise ParserAssetError(
            "archive members differ from the declared internal paths"
        )
    library_content = members[record["library"]["path"]]
    if len(library_content) != record["library"]["size"]:
        raise ParserAssetError("internal library size mismatch")
    if sha256_bytes(library_content) != record["library"]["sha256"]:
        raise ParserAssetError("internal library digest mismatch")

    static_library_content = members[record["static_library"]["path"]]
    if len(static_library_content) != record["static_library"]["size"]:
        raise ParserAssetError("internal static library size mismatch")
    if sha256_bytes(static_library_content) != record["static_library"]["sha256"]:
        raise ParserAssetError("internal static library digest mismatch")

    identity_content = members[record["build_identity"]["path"]]
    if len(identity_content) != record["build_identity"]["size"]:
        raise ParserAssetError("internal build identity size mismatch")
    if sha256_bytes(identity_content) != record["build_identity"]["sha256"]:
        raise ParserAssetError("internal build identity digest mismatch")
    identity = parse_json_bytes(identity_content, "internal build identity")
    if not isinstance(identity, dict):
        raise ParserAssetError("internal build identity must be an object")
    if identity_content != canonical_json_bytes(identity):
        raise ParserAssetError("internal build identity is not canonical JSON")
    if identity != build_identity_from_record(record):
        raise ParserAssetError("internal and outer identities do not agree")


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary_name = handle.name
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_name, 0o644)
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def pack_target(args: argparse.Namespace) -> None:
    validate_version_inputs(args)
    target = args.target
    if target not in TARGET_LIBRARIES:
        raise ParserAssetError(f"unsupported parser target: {target}")
    library_path = Path(args.library)
    if library_path.name != TARGET_LIBRARIES[target]:
        raise ParserAssetError(
            f"library name for target {target} must be {TARGET_LIBRARIES[target]}"
        )
    library_content = read_regular_file(
        library_path,
        "native parser library",
        max_bytes=MAX_ARCHIVE_MEMBER_BYTES,
    )
    static_library_path = Path(args.static_library)
    if static_library_path.name != TARGET_STATIC_LIBRARIES[target]:
        raise ParserAssetError(
            f"static library name for target {target} must be "
            f"{TARGET_STATIC_LIBRARIES[target]}"
        )
    static_library_content = read_regular_file(
        static_library_path,
        "native parser static library",
        max_bytes=MAX_ARCHIVE_MEMBER_BYTES,
    )

    source_root = Path(args.source_root)
    contract_content = read_regular_file(
        source_root / "PARSER_CONTRACT_VERSION", "parser contract descriptor"
    )
    try:
        source_contract = contract_content.decode("ascii").strip()
    except UnicodeDecodeError as error:
        raise ParserAssetError("parser contract descriptor is not ASCII") from error
    if source_contract != args.contract_version:
        raise ParserAssetError(
            "parser source contract does not match the requested contract"
        )

    parser_source = tree_identity(
        source_root, "parser source tree", source_path_ignored
    )
    packages = package_identities(args.package)
    registry_metadata = metadata_identities(args.registry_metadata)
    builder = {
        "compile": compile_identity(target),
        "nim": {
            "version": args.nim_version,
            **file_identity(Path(args.nim_binary), "Nim compiler binary"),
        },
        "nimble": {
            "release_sha": args.nimble_sha,
            "version": args.nimble_version,
            **file_identity(Path(args.nimble_binary), "Nimble binary"),
        },
        "packer": packer_identity(),
    }
    library_member, static_library_member, identity_member = internal_paths(target)
    library = {
        "path": library_member,
        "sha256": sha256_bytes(library_content),
        "size": len(library_content),
    }
    static_library = {
        "path": static_library_member,
        "sha256": sha256_bytes(static_library_content),
        "size": len(static_library_content),
    }
    record: dict[str, Any] = {
        "alopex_version": args.alopex_version,
        "archive": {},
        "build_identity": {},
        "builder": builder,
        "contract_version": args.contract_version,
        "library": library,
        "packages": packages,
        "packing": PACKING_IDENTITY,
        "parser_source": parser_source,
        "registry_metadata": registry_metadata,
        "schema": TARGET_RECORD_SCHEMA,
        "static_library": static_library,
        "target": target,
    }
    identity = build_identity_from_record(record)
    identity_content = canonical_json_bytes(identity)
    record["build_identity"] = {
        "path": identity_member,
        "sha256": sha256_bytes(identity_content),
        "size": len(identity_content),
    }
    archive_content = normalized_archive_bytes(
        {
            identity_member: identity_content,
            library_member: library_content,
            static_library_member: static_library_content,
        }
    )
    stem = archive_stem(args.alopex_version, args.contract_version, target)
    archive_filename = f"{stem}.tar.gz"
    record["archive"] = {
        "filename": archive_filename,
        "sha256": sha256_bytes(archive_content),
        "size": len(archive_content),
    }
    validate_record_shape(record)
    if read_normalized_archive(archive_content) != {
        identity_member: identity_content,
        library_member: library_content,
        static_library_member: static_library_content,
    }:
        raise ParserAssetError("generated archive did not replay byte-identically")

    output_dir = Path(args.output_dir)
    archive_output = output_dir / archive_filename
    record_output = output_dir / f"{stem}.json"
    atomic_write(archive_output, archive_content)
    atomic_write(record_output, canonical_json_bytes(record))
    print(record_output)
    print(archive_output)


def verify_inputs(args: argparse.Namespace) -> None:
    package_identities(args.package)
    metadata_identities(args.registry_metadata)
    print("locked parser dependency inputs verified")


def verify_target(args: argparse.Namespace) -> None:
    record_path = Path(args.record)
    record = load_canonical_json(record_path, "target record")
    validate_record_shape(record)
    archive_path = Path(args.asset_dir) / record["archive"]["filename"]
    verify_record_archive(record, archive_path)
    print(record_path)


def record_common_identity(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "alopex_version": record["alopex_version"],
        "builder_requirements": {
            "build_profile": record["builder"]["compile"]["profile"],
            "nim_version": record["builder"]["nim"]["version"],
            "nimble_release_sha": record["builder"]["nimble"]["release_sha"],
            "nimble_version": record["builder"]["nimble"]["version"],
        },
        "contract_version": record["contract_version"],
        "packages": record["packages"],
        "packing": record["packing"],
        "parser_source": record["parser_source"],
        "registry_metadata": record["registry_metadata"],
    }


def manifest_asset(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "archive": record["archive"],
        "build_identity": record["build_identity"],
        "builder": record["builder"],
        "library": record["library"],
        "static_library": record["static_library"],
        "target": record["target"],
    }


def record_from_manifest(
    manifest: dict[str, Any], asset: dict[str, Any]
) -> dict[str, Any]:
    return {
        "alopex_version": manifest["alopex_version"],
        "archive": asset["archive"],
        "build_identity": asset["build_identity"],
        "builder": asset["builder"],
        "contract_version": manifest["contract_version"],
        "library": asset["library"],
        "packages": manifest["packages"],
        "packing": manifest["packing"],
        "parser_source": manifest["parser_source"],
        "registry_metadata": manifest["registry_metadata"],
        "schema": TARGET_RECORD_SCHEMA,
        "static_library": asset["static_library"],
        "target": asset["target"],
    }


def validate_manifest_shape(manifest: dict[str, Any]) -> None:
    require_exact_keys(
        manifest,
        {
            "alopex_version",
            "assets",
            "builder_requirements",
            "contract_version",
            "packages",
            "packing",
            "parser_source",
            "registry_metadata",
            "schema",
        },
        "vendor manifest",
    )
    reject_commit_keys(manifest)
    if manifest["schema"] != VENDOR_MANIFEST_SCHEMA:
        raise ParserAssetError("wrong vendor manifest schema")
    if manifest["alopex_version"] != REQUIRED_ALOPEX_VERSION:
        raise ParserAssetError("wrong Alopex version in vendor manifest")
    if manifest["contract_version"] != REQUIRED_CONTRACT_VERSION:
        raise ParserAssetError("wrong contract version in vendor manifest")
    if manifest["packing"] != PACKING_IDENTITY:
        raise ParserAssetError("wrong packing identity in vendor manifest")
    requirements = manifest["builder_requirements"]
    if not isinstance(requirements, dict):
        raise ParserAssetError("builder_requirements must be an object")
    require_exact_keys(
        requirements,
        {
            "build_profile",
            "nim_version",
            "nimble_release_sha",
            "nimble_version",
        },
        "builder_requirements",
    )
    if requirements != {
        "build_profile": REQUIRED_BUILD_PROFILE,
        "nim_version": REQUIRED_NIM_VERSION,
        "nimble_release_sha": REQUIRED_NIMBLE_SHA,
        "nimble_version": REQUIRED_NIMBLE_VERSION,
    }:
        raise ParserAssetError("vendor manifest builder requirements are not exact")

    identity_record = {
        "packages": manifest["packages"],
        "parser_source": manifest["parser_source"],
        "registry_metadata": manifest["registry_metadata"],
    }
    validate_identity_list(identity_record)
    assets = manifest["assets"]
    if not isinstance(assets, list) or not assets:
        raise ParserAssetError("vendor manifest assets must be a non-empty list")
    targets: list[str] = []
    archive_names: list[str] = []
    for index, asset in enumerate(assets):
        if not isinstance(asset, dict):
            raise ParserAssetError(f"assets[{index}] must be an object")
        require_exact_keys(
            asset,
            {
                "archive",
                "build_identity",
                "builder",
                "library",
                "static_library",
                "target",
            },
            f"assets[{index}]",
        )
        record = record_from_manifest(manifest, asset)
        validate_record_shape(record)
        targets.append(record["target"])
        archive_names.append(record["archive"]["filename"])
    if targets != sorted(targets) or len(targets) != len(set(targets)):
        raise ParserAssetError("vendor manifest targets are duplicate or unsorted")
    if set(targets) != set(TARGET_LIBRARIES):
        raise ParserAssetError(
            "vendor manifest target matrix must contain every supported target"
        )
    if len(archive_names) != len(set(archive_names)):
        raise ParserAssetError("vendor manifest has duplicate archive filenames")


def assemble_manifest(args: argparse.Namespace) -> None:
    if not args.record:
        raise ParserAssetError("at least one target record is required")
    records: list[dict[str, Any]] = []
    targets: set[str] = set()
    archive_names: set[str] = set()
    common_identity: dict[str, Any] | None = None
    asset_dir = Path(args.asset_dir)
    for record_name in args.record:
        record = load_canonical_json(Path(record_name), "target record")
        validate_record_shape(record)
        target = record["target"]
        archive_name = record["archive"]["filename"]
        if target in targets:
            raise ParserAssetError(f"duplicate target record: {target}")
        if archive_name in archive_names:
            raise ParserAssetError(f"duplicate archive filename: {archive_name}")
        targets.add(target)
        archive_names.add(archive_name)
        verify_record_archive(record, asset_dir / archive_name)
        candidate_common = record_common_identity(record)
        if common_identity is None:
            common_identity = candidate_common
        elif candidate_common != common_identity:
            raise ParserAssetError("target records have inconsistent common identity")
        records.append(record)
    assert common_identity is not None
    manifest = {
        **common_identity,
        "assets": [
            manifest_asset(record)
            for record in sorted(records, key=lambda record: record["target"])
        ],
        "schema": VENDOR_MANIFEST_SCHEMA,
    }
    validate_manifest_shape(manifest)
    atomic_write(Path(args.output), canonical_json_bytes(manifest))
    print(args.output)


def verify_manifest(args: argparse.Namespace) -> None:
    manifest_path = Path(args.manifest)
    manifest = load_canonical_json(manifest_path, "vendor manifest")
    validate_manifest_shape(manifest)
    asset_dir = Path(args.asset_dir)
    for asset in manifest["assets"]:
        record = record_from_manifest(manifest, asset)
        verify_record_archive(record, asset_dir / record["archive"]["filename"])
    print(manifest_path)


def release_envelope(args: argparse.Namespace) -> None:
    """Emit the post-tag publication envelope without changing source state."""
    manifest_path = Path(args.manifest)
    manifest = load_canonical_json(manifest_path, "vendor manifest")
    validate_manifest_shape(manifest)
    tag = require_string(args.tag, "tag")
    tag_sha = require_string(args.tag_sha, "tag SHA")
    if not re.fullmatch(r"[0-9a-f]{40}", tag_sha):
        raise ParserAssetError("tag SHA must be a 40-character lowercase hex value")
    if tag != f"v{REQUIRED_ALOPEX_VERSION}":
        raise ParserAssetError("release envelope tag does not match Alopex version")
    asset_dir = Path(args.asset_dir)
    assets: list[dict[str, Any]] = []
    for asset in manifest["assets"]:
        archive_name = asset["archive"]["filename"]
        archive_path = asset_dir / archive_name
        content = read_regular_file(
            archive_path, "release archive", max_bytes=MAX_ARCHIVE_BYTES
        )
        assets.append(
            {
                "filename": archive_name,
                "sha256": sha256_bytes(content),
                "size": len(content),
                "target": asset["target"],
            }
        )
    envelope = {
        "alopex_version": REQUIRED_ALOPEX_VERSION,
        "assets": assets,
        "contract_version": REQUIRED_CONTRACT_VERSION,
        "manifest": {
            "filename": manifest_path.name,
            "sha256": sha256_bytes(manifest_path.read_bytes()),
            "size": manifest_path.stat().st_size,
        },
        "schema": RELEASE_ENVELOPE_SCHEMA,
        "source": {"tag": tag, "tag_sha": tag_sha},
    }
    atomic_write(Path(args.output), canonical_json_bytes(envelope))
    print(args.output)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description="Build and verify deterministic Alopex parser assets"
    )
    commands = root.add_subparsers(dest="command", required=True)

    pack = commands.add_parser("pack-target")
    pack.add_argument("--alopex-version", required=True)
    pack.add_argument("--contract-version", required=True)
    pack.add_argument("--target", required=True)
    pack.add_argument("--library", required=True)
    pack.add_argument("--static-library", required=True)
    pack.add_argument("--source-root", required=True)
    pack.add_argument("--nim-version", required=True)
    pack.add_argument("--nim-binary", required=True)
    pack.add_argument("--nimble-version", required=True)
    pack.add_argument("--nimble-sha", required=True)
    pack.add_argument("--nimble-binary", required=True)
    pack.add_argument("--build-profile", required=True)
    pack.add_argument("--package", action="append", default=[])
    pack.add_argument("--registry-metadata", action="append", default=[])
    pack.add_argument("--output-dir", required=True)
    pack.set_defaults(handler=pack_target)

    inputs = commands.add_parser("verify-inputs")
    inputs.add_argument("--package", action="append", default=[])
    inputs.add_argument("--registry-metadata", action="append", default=[])
    inputs.set_defaults(handler=verify_inputs)

    target = commands.add_parser("verify-target")
    target.add_argument("--record", required=True)
    target.add_argument("--asset-dir", required=True)
    target.set_defaults(handler=verify_target)

    assemble = commands.add_parser("assemble-manifest")
    assemble.add_argument("--record", action="append", default=[])
    assemble.add_argument("--asset-dir", required=True)
    assemble.add_argument("--output", required=True)
    assemble.set_defaults(handler=assemble_manifest)

    manifest = commands.add_parser("verify-manifest")
    manifest.add_argument("--manifest", required=True)
    manifest.add_argument("--asset-dir", required=True)
    manifest.set_defaults(handler=verify_manifest)

    envelope = commands.add_parser("release-envelope")
    envelope.add_argument("--manifest", required=True)
    envelope.add_argument("--asset-dir", required=True)
    envelope.add_argument("--tag", required=True)
    envelope.add_argument("--tag-sha", required=True)
    envelope.add_argument("--output", required=True)
    envelope.set_defaults(handler=release_envelope)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        args.handler(args)
    except ParserAssetError as error:
        print(f"parser asset error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
