"""Stage one verified parser target archive for both public build arms."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat
import sys
import tempfile

import issue681_hosted_nim_archive as nim_archive
import issue681_public_assets_produce as producer

RELEASE_DIR = Path(__file__).resolve().parents[1] / "release"
if str(RELEASE_DIR) not in sys.path:
    sys.path.insert(0, str(RELEASE_DIR))
import parser_asset_manifest as manifest


TARGET = "x86_64-unknown-linux-gnu"
STEM = "alopex-parser-v0.8.17-contract-0.28.0-" + TARGET
ROOT_NAME = "parser-input"


def require(value):
    if not value:
        raise ValueError("PUBLIC_PARSER_INPUT")


def _owned_file(path, limit):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                0 < info.st_size <= limit)
        content = stream.read(limit + 1)
        require(0 < len(content) <= limit and len(content) == info.st_size)
        return content


def _write_file(path, raw, guard):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        for start in range(0, len(raw), 1_048_576):
            stream.write(raw[start:start + 1_048_576])
            guard()


def _same_directory(path, identity):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and
            (info.st_dev, info.st_ino) == identity)


def stage_parser(archive_dir, destination_parent, guard):
    """Return an exact private parser input, or fixed PUBLIC_PARSER_INPUT."""
    partial = output = None
    owned_partial = owned_output = None
    stage = "parser-stage-admission"
    try:
        nim_archive._owned_directory(archive_dir)
        nim_archive._owned_directory(destination_parent)
        require(callable(guard) and archive_dir != destination_parent and
                archive_dir not in destination_parent.parents and
                destination_parent not in archive_dir.parents)
        record_path = archive_dir / (STEM + ".json")
        asset_path = archive_dir / (STEM + ".tar.gz")
        output = destination_parent / ROOT_NAME
        require(not output.exists() and not output.is_symlink())
        guard()
        stage = "parser-stage-verify"
        record_raw = _owned_file(record_path, manifest.MAX_CANONICAL_JSON_BYTES)
        record = manifest.parse_json_bytes(record_raw, "parser target record")
        require(type(record) is dict and
                manifest.canonical_json_bytes(record) == record_raw)
        manifest.validate_record_shape(record)
        expected_record_digest = hashlib.sha256(record_raw).hexdigest()
        require(record["target"] == TARGET and
                record["alopex_version"] == "0.8.17" and
                record["contract_version"] == "0.28.0")
        raw = _owned_file(asset_path, manifest.MAX_ARCHIVE_BYTES)
        require(len(raw) == record["archive"]["size"] and
                hashlib.sha256(raw).hexdigest() == record["archive"]["sha256"])
        manifest.verify_record_archive_bytes(record, raw)
        guard()
        members = manifest.read_normalized_archive(raw)
        require(manifest.normalized_archive_bytes(members) == raw)
        expected = {record[role]["path"]
                    for role in ("library", "static_library", "build_identity")}
        require(set(members) == expected)
        for role in ("library", "static_library", "build_identity"):
            item = record[role]
            content = members[item["path"]]
            require(len(content) == item["size"] and
                    hashlib.sha256(content).hexdigest() == item["sha256"])
        guard()
        stage = "parser-stage-write"
        partial = Path(tempfile.mkdtemp(prefix=".parser-input.",
                                        dir=destination_parent))
        fd = os.open(partial, os.O_RDONLY | os.O_DIRECTORY |
                     getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(fd)
        finally:
            os.close(fd)
        owned_partial = (info.st_dev, info.st_ino)

        def guarded():
            _same_directory(partial, owned_partial)
            guard()

        for name, content in sorted(members.items()):
            guarded()
            _write_file(partial / name, content, guarded)
        library_name = record["library"]["path"]
        library_parent = (partial / library_name).parent
        _write_file(library_parent / "CONTRACT_VERSION", b"0.28.0\n", guarded)
        sums = "".join(
            record[role]["sha256"] + "  " +
            Path(record[role]["path"]).name + "\n"
            for role in ("library", "static_library"))
        _write_file(library_parent / "SHA256SUMS", sums.encode(), guarded)
        guarded()
        require(producer.verify_parser(
            record_path, partial, partial / library_name)
            == expected_record_digest)
        guarded()
        stage = "parser-stage-promotion"
        _same_directory(partial, owned_partial)
        nim_archive._promote_noreplace(partial, output)
        owned_output = owned_partial
        owned_partial = None
        _same_directory(output, owned_output)
        require(producer.verify_parser(
            record_path, output, output / library_name) == expected_record_digest)
        guard()
        return {"parser_record": record_path,
                "parser_root": output,
                "parser_library": output / library_name,
                "parser_record_sha256": expected_record_digest,
                "parser_sha256": record["library"]["sha256"]}
    except BaseException as error:
        if isinstance(destination_parent, Path):
            nim_archive._private_reason(destination_parent, stage, error)
        for path, identity in ((output, owned_output), (partial, owned_partial)):
            if path is not None and identity is not None:
                try:
                    nim_archive._remove_owned_tree(path, identity)
                except Exception:
                    pass
        raise ValueError("PUBLIC_PARSER_INPUT") from None
