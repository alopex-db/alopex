"""Authenticate and stage one offline Cargo vendor source for both arms."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tarfile
import tempfile
import zipfile

import issue681_hosted_acquire_source as acquire
import issue681_hosted_build_guard as capacity
import issue681_hosted_nim_archive as archive_tools
import issue681_public_vendor_bundle as bundle


ZIP_BYTES = 301_598_193
ZIP_SHA256 = "dd6b618ad9d45a5703f32bafb30fb936e119c827fed2ac6d649cda22b5489a3c"
CHUNK = 1_048_576


def require(value):
    if not value:
        raise ValueError("PUBLIC_VENDOR")


def _owned_file(path, limit):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                0 < info.st_size <= limit)
        raw = stream.read(limit + 1)
        require(len(raw) == info.st_size)
        return raw


def _directory_identity(path):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700)
    return info.st_dev, info.st_ino


def _same_directory(path, identity):
    require(_directory_identity(path) == identity)


def _zip_digest(stream, guard):
    stream.seek(0)
    info = os.fstat(stream.fileno())
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
            info.st_size == ZIP_BYTES and info.st_size <= bundle.MAX_ZIP)
    digest = hashlib.sha256()
    total = 0
    for block in iter(lambda: stream.read(CHUNK), b""):
        total += len(block)
        require(total <= ZIP_BYTES)
        digest.update(block)
        guard()
    require(total == ZIP_BYTES and digest.hexdigest() == ZIP_SHA256)


def _archive_modes(source_zip):
    modes = {}
    for info in source_zip.infolist():
        if not info.filename.startswith("archives/"):
            continue
        package = info.filename[9:-6]
        require(info.filename == "archives/" + package + ".crate")
        raw = source_zip.read(info)
        require(len(raw) <= bundle.MAX_ARCHIVE)
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as source:
            for member in source:
                require(member.isfile())
                bundle._safe_name(member.name)
                parts = member.name.split("/")
                require(len(parts) >= 2 and parts[0] == package)
                relative = "/".join(parts[1:])
                name = "vendor/" + package + "/" + relative
                require(name not in modes)
                modes[name] = 0o700 if member.mode & 0o111 else 0o600
    return modes


def _write_member(root_fd, relative, source_zip, info, mode, guard, limit):
    parts = relative.split("/")
    directory_fd = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            try:
                os.mkdir(part, mode=0o700, dir_fd=directory_fd)
            except FileExistsError:
                pass
            child_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY |
                               getattr(os, "O_NOFOLLOW", 0),
                               dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = child_fd
        fd = os.open(parts[-1], os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600,
                     dir_fd=directory_fd)
        count = 0
        digest = hashlib.sha256()
        with os.fdopen(fd, "wb") as output, source_zip.open(info) as input_file:
            while block := input_file.read(CHUNK):
                count += len(block)
                require(count <= info.file_size and count <= limit)
                output.write(block)
                digest.update(block)
                guard()
            os.fchmod(output.fileno(), mode)
        require(count == info.file_size)
        return count, digest.hexdigest()
    finally:
        os.close(directory_fd)


def _locks(sources):
    require(type(sources) is dict and set(sources) == {"before", "candidate"})
    locks = {}
    for name in ("before", "candidate"):
        source = sources[name]
        archive_tools._owned_directory(source)
        require(source.resolve(strict=True) == source)
        raw = _owned_file(source / "Cargo.lock", 1_000_000)
        require(hashlib.sha256(raw).hexdigest() == acquire.PINS[name]["lock"])
        locks[name] = raw
    return locks


def _remove_file(path, identity):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    require(stat.S_ISREG(info.st_mode) and
            (info.st_dev, info.st_ino) == identity)
    path.unlink()


def install_vendor(zip_path, sources, cargo_home, guard):
    """Return one private Cargo home, or only fixed PUBLIC_VENDOR failure."""
    partial = vendor = config = None
    owned_partial = owned_vendor = owned_config = None
    try:
        require(type(guard) is capacity.HostStorageGuard)
        archive_tools._owned_directory(zip_path.parent)
        archive_tools._owned_directory(cargo_home)
        require(zip_path.is_absolute() and not zip_path.is_symlink() and
                cargo_home.resolve(strict=True) == cargo_home and
                Path.home() in cargo_home.parents and
                not any(cargo_home.iterdir()))
        locks = _locks(sources)
        guard()
        forbidden = tuple(dict.fromkeys(
            str(root).encode() for root in
            (Path.home(), zip_path.parent, cargo_home,
             sources["before"], sources["candidate"])))
        manifest = bundle.verify_bundle(zip_path, locks,
                                        forbidden_roots=forbidden)
        guard()
        bundle_fd = os.open(zip_path, os.O_RDONLY |
                            getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(bundle_fd, "rb") as bundle_stream:
            _zip_digest(bundle_stream, guard)
            partial = Path(tempfile.mkdtemp(prefix=".vendor.", dir=cargo_home))
            owned_partial = _directory_identity(partial)
            root_fd = os.open(partial, os.O_RDONLY | os.O_DIRECTORY |
                              getattr(os, "O_NOFOLLOW", 0))
            try:
                rows = {}
                mode_rows = {}
                total = 0
                with zipfile.ZipFile(bundle_stream) as source_zip:
                    modes = _archive_modes(source_zip)
                    for info in source_zip.infolist():
                        if not info.filename.startswith("vendor/"):
                            continue
                        relative = info.filename[7:]
                        bundle._safe_name(relative)
                        require(0 <= info.file_size <= bundle.MAX_MEMBER and
                                relative not in rows)
                        mode = modes.get(info.filename)
                        if relative.endswith("/.cargo-checksum.json"):
                            require(mode is None)
                            mode = 0o600
                        else:
                            require(mode in (0o600, 0o700))
                        _same_directory(partial, owned_partial)
                        size, file_digest = _write_member(
                            root_fd, relative, source_zip, info, mode,
                            guard, manifest["vendor_content_bytes"] - total)
                        total += size
                        rows[relative] = (size, file_digest)
                        mode_rows[relative] = mode
                        guard()
            finally:
                os.close(root_fd)
            _zip_digest(bundle_stream, guard)
        tree = hashlib.sha256()
        for name, (size, sha256) in sorted(rows.items()):
            tree.update(json.dumps([name, size, sha256],
                                   separators=(",", ":")).encode() + b"\n")
        require(len(rows) == manifest["vendor_file_count"] and
                total == manifest["vendor_content_bytes"] and
                tree.hexdigest() == manifest["vendor_tree_sha256"])
        mode_tree = hashlib.sha256()
        for name, mode in sorted(mode_rows.items()):
            mode_tree.update(json.dumps([name, mode],
                                        separators=(",", ":")).encode() + b"\n")
        guard()
        vendor = cargo_home / "vendor"
        archive_tools._promote_noreplace(partial, vendor)
        owned_vendor = owned_partial
        owned_partial = None
        _same_directory(vendor, owned_vendor)
        config = cargo_home / "config.toml"
        content = ("[source.crates-io]\nreplace-with = \"vendored-sources\"\n\n"
                   "[source.vendored-sources]\ndirectory = " +
                   json.dumps(str(vendor)) + "\n").encode()
        fd = os.open(config, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as output:
            info = os.fstat(output.fileno())
            owned_config = (info.st_dev, info.st_ino)
            output.write(content)
        guard()
        return {"cargo_home": cargo_home,
                "vendor_root": vendor,
                "bundle_sha256": ZIP_SHA256,
                "vendor_tree_sha256": manifest["vendor_tree_sha256"],
                "vendor_mode_sha256": mode_tree.hexdigest()}
    except BaseException:
        if config is not None and owned_config is not None:
            try:
                _remove_file(config, owned_config)
            except Exception:
                pass
        for path, identity in ((vendor, owned_vendor), (partial, owned_partial)):
            if path is not None and identity is not None:
                try:
                    archive_tools._remove_owned_tree(path, identity)
                except Exception:
                    pass
        raise ValueError("PUBLIC_VENDOR") from None
