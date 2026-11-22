"""Build one exact #681 source and produce a public-only example bundle.

The selected controller supplies an already qualified Linux job, parser
sidecars, one shared target directory and a measured storage allowance.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import subprocess
import sys
import time
import tomllib

import issue681_public as case
import issue681_public_assets as assets
import issue681_public_assets_build as pack

EXAMPLE = "crates/alopex-embedded/examples/issue681_hnsw_first_write.rs"
CONTRACT = "crates/alopex-sql/nim-sql-parser/PARSER_CONTRACT_VERSION"
HOST = "x86_64-unknown-linux-gnu"
LIMIT_SECONDS = 7_200


def require(value):
    if not value:
        raise ValueError("PUBLIC_BUILD")


def digest(path):
    return hashlib.sha256(assets._read_file(path, 268_435_456)).hexdigest()


def owned_private_directory(path):
    require(isinstance(path, Path) and path.is_absolute() and path.is_dir() and
            not path.is_symlink() and path.stat().st_uid == os.geteuid() and
            stat.S_IMODE(path.stat().st_mode) == 0o700)


def _run(args, *, cwd=None, env=None):
    result = subprocess.run(args, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            timeout=30, check=False)
    require(result.returncode == 0 and len(result.stdout) <= 4096)
    return result.stdout.decode("ascii").strip()


def verify_source(source, commit, tree, example_sha256, lock_sha256):
    owned_private_directory(source)
    require(re.fullmatch(r"[0-9a-f]{40}", commit) and
            re.fullmatch(r"[0-9a-f]{40}", tree))
    require(_run(["git", "rev-parse", "HEAD"], cwd=source) == commit and
            _run(["git", "rev-parse", "HEAD^{tree}"], cwd=source) == tree and
            _run(["git", "status", "--porcelain", "--untracked-files=all"],
                 cwd=source) == "")
    require(digest(source / EXAMPLE) == example_sha256 and
            digest(source / "Cargo.lock") == lock_sha256 and
            assets._read_file(source / CONTRACT, 64) == b"0.28.0\n")
    version = tomllib.loads(assets._read_file(source / "Cargo.toml", 65_536).decode())
    require(version["workspace"]["package"]["version"] == "0.8.17")


def verify_parser(record_path, parser_root, library):
    owned_private_directory(parser_root)
    require(record_path.is_absolute() and library.is_absolute() and
            record_path.is_file() and library.is_file() and
            not record_path.is_symlink() and not library.is_symlink())
    record_raw = assets._read_file(record_path, 65_536)
    try:
        record = json.loads(record_raw)
    except (ValueError, UnicodeError):
        raise ValueError("PUBLIC_BUILD") from None
    require(type(record) is dict and record.get("contract_version") == "0.28.0" and
            record.get("target") == HOST and type(record.get("library")) is dict and
            type(record.get("static_library")) is dict and
            type(record.get("build_identity")) is dict)
    for role in ("library", "static_library", "build_identity"):
        item = record[role]
        require(type(item.get("path")) is str and
                not Path(item["path"]).is_absolute() and
                all(part not in ("", ".", "..")
                    for part in Path(item["path"]).parts) and
                type(item.get("size")) is int and item["size"] > 0 and
                type(item.get("sha256")) is str and
                re.fullmatch(r"[0-9a-f]{64}", item["sha256"]))
        path = parser_root / item["path"]
        require(not any(parent.is_symlink() for parent in path.parents
                        if parent == parser_root or parser_root in parent.parents))
        require(assets._read_file(path, 268_435_456) and
                path.stat().st_size == item["size"] and
                digest(path) == item["sha256"])
    require(library == parser_root / record["library"]["path"] and
            library.parent == (parser_root / record["static_library"]["path"]).parent)
    contract = assets._read_file(library.parent / "CONTRACT_VERSION", 64)
    sums = assets._read_file(library.parent / "SHA256SUMS", 512)
    expected_sums = "".join(
        record[role]["sha256"] + "  " + Path(record[role]["path"]).name + "\n"
        for role in ("library", "static_library"))
    require(contract == b"0.28.0\n" and sums == expected_sums.encode())
    return hashlib.sha256(record_raw).hexdigest()


def _storage(extra, additional):
    require(type(additional) is int and 0 < additional < case.STOP_BYTES)
    measured = case.storage.inventory(extra)
    used = measured["allocated_bytes"]
    require(type(used) is int and 0 <= used < case.STOP_BYTES and
            used + additional <= case.STOP_BYTES)
    return used


def _build(source, target, work, parser_library, inventory_roots, limit,
           rust_version, env):
    log = work / "cargo-build.private.log"
    flags = " ".join(
        f"--remap-path-prefix={path}=/alopex-build/{label}"
        for path, label in ((source, "control"), (work, "work"),
                            (target, "target"),
                            (parser_library.parent, "parser"),
                            (Path.home(), "home")))
    build_env = dict(env, CARGO_INCREMENTAL="0", CARGO_PROFILE_RELEASE_DEBUG="0",
                     CARGO_BUILD_JOBS="2", CARGO_TARGET_DIR=str(target),
                     RUSTUP_TOOLCHAIN=rust_version,
                     NIM_SQL_PARSER_LIB_DIR=str(parser_library.parent),
                     ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD="1", RUSTFLAGS=flags)
    build_env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    with log.open("xb") as output:
        os.chmod(log, 0o600)
        process = subprocess.Popen(assets.COMMAND, cwd=source, env=build_env,
                                   stdin=subprocess.DEVNULL, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + LIMIT_SECONDS
        try:
            while process.poll() is None:
                require(time.monotonic() < deadline)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    pass
                used = case.storage.inventory(inventory_roots)["allocated_bytes"]
                require(used < case.STOP_BYTES and used <= limit)
            require(process.returncode == 0)
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            raise


def produce(*, source, target, work, output, parser_record, parser_root,
            parser_library,
            source_commit, source_tree, example_sha256, cargo_lock_sha256,
            parser_record_sha256, parser_sha256, rust_version,
            additional_storage_bytes, private_roots, build_lock):
    """Return exact public pins, or one fixed failure without a promoted bundle."""
    try:
        require(sys.platform == "linux" and platform.machine() == "x86_64")
        for path in (source, target, work, output.parent, build_lock.parent,
                     parser_root):
            owned_private_directory(path)
        require(target != source and source not in target.parents and
                work != source and source not in work.parents)
        require(all(" " not in str(path) for path in
                    (source, target, work, parser_root, Path.home())))
        require(not output.exists() and isinstance(private_roots, tuple) and
                all(type(item) is str and item.startswith("/") and len(item) >= 8
                    for item in private_roots))
        require(re.fullmatch(r"1\.[0-9]{1,3}\.[0-9]{1,3}", rust_version) and
                re.fullmatch(r"[0-9a-f]{64}", parser_record_sha256) and
                re.fullmatch(r"[0-9a-f]{64}", parser_sha256))
        compiler = _run(["rustc", "-Vv"])
        require("release: " + rust_version in compiler.splitlines() and
                "host: " + HOST in compiler.splitlines())
        verify_source(source, source_commit, source_tree, example_sha256,
                      cargo_lock_sha256)
        require(verify_parser(parser_record, parser_root, parser_library) ==
                parser_record_sha256 and digest(parser_library) == parser_sha256)
        inventory_roots = [source, target, work, parser_root,
                           parser_record.parent, output.parent]
        _storage(inventory_roots, additional_storage_bytes)
        fd = os.open(build_lock, os.O_CREAT | os.O_RDWR |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            info = os.fstat(fd)
            require(info.st_uid == os.geteuid() and
                    stat.S_IMODE(info.st_mode) == 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            baseline = _storage(inventory_roots, additional_storage_bytes)
            _build(source, target, work, parser_library, inventory_roots,
                   baseline + additional_storage_bytes, rust_version, os.environ)
            verify_source(source, source_commit, source_tree, example_sha256,
                          cargo_lock_sha256)
            binary = target / "release/examples/issue681_hnsw_first_write"
            require(binary.is_file() and not binary.is_symlink() and
                    binary.stat().st_uid == os.geteuid())
            protected_roots = tuple(dict.fromkeys((*private_roots, str(source),
                                                  str(target), str(work),
                                                  str(parser_root),
                                                  str(parser_record.parent),
                                                  str(output.parent),
                                                  str(Path.home()))))
            pin = pack._package(output, binary, parser_library, source / EXAMPLE,
                                source_commit=source_commit,
                                source_tree=source_tree,
                                parser_record_sha256=parser_record_sha256,
                                cargo_lock_sha256=cargo_lock_sha256,
                                rust_version=rust_version,
                                private_roots=protected_roots,
                                smoke_runner=pack.smoke)
            return pin
        finally:
            os.close(fd)
    except Exception:
        raise ValueError("PUBLIC_BUILD") from None
