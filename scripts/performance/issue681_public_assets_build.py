"""Internal #681 smoke and packaging primitives for a verified build.

This module has no production entry point until exact-source build evidence,
capacity admission and a selected-host controller are implemented.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import tempfile
import time

import issue681_public as case
import issue681_public_assets as assets
import issue681_public_pair as pair


def require(value):
    if not value:
        raise ValueError("PUBLIC_BUILD")


def _write(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def _canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode()


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _read_event(selector, process, deadline):
    buffer = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        require(remaining > 0)
        require(selector.select(remaining))
        chunk = os.read(process.stdout.fileno(), 4096)
        require(chunk)
        buffer.extend(chunk)
        require(len(buffer) <= case.MAX_EVENT_BYTES)
        if buffer.endswith(b"\n"):
            try:
                value = json.loads(buffer)
            except (ValueError, UnicodeError):
                raise ValueError("PUBLIC_BUILD") from None
            require(type(value) is dict and set(value) == {"phase", "data"} and
                    type(value["data"]) is dict)
            return value


def smoke(binary, mode, *, timeout=30):
    """Check the exact two-row completion protocol without recording raw output."""
    require(mode in ("commit", "rollback") and type(timeout) is int and
            1 <= timeout <= 60 and isinstance(binary, Path) and
            binary.is_absolute() and binary.is_file() and not binary.is_symlink())
    env = dict(os.environ, ALOPEX_MANUAL_681="1", RAYON_NUM_THREADS="2",
               OMP_NUM_THREADS="2")
    process = subprocess.Popen([str(binary), "2", "4", mode],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL, env=env,
                               start_new_session=True)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout
    phases = (*case.PHASES, mode)
    plan = tuple((phase, kind) for phase in phases
                 for kind in ("start", "end")) + (("complete", None),)
    elapsed = None
    try:
        for phase, kind in plan:
            event = _read_event(selector, process, deadline)
            require(event["phase"] == phase)
            data = event["data"]
            if phase == "complete":
                require(data.get("finish") == mode and
                        data.get("result_validated") is True)
            else:
                require(data.get("event") == kind)
                if phase == mode and kind == "end":
                    elapsed = data.get("elapsed_ns")
                    require(type(elapsed) is int and
                            0 < elapsed <= 3_600_000_000_000)
            process.stdin.write(b"continue\n")
            process.stdin.flush()
        remaining = deadline - time.monotonic()
        require(remaining > 0 and process.wait(timeout=remaining) == 0)
        require(process.stdout.read(1) == b"")
        return {"status": "passed", "rows": 2, "dimensions": 4,
                "phases": 9, "events": 19, "mode": mode,
                "result_validated": True, "elapsed_ns": elapsed}
    except Exception:
        raise ValueError("PUBLIC_BUILD") from None
    finally:
        selector.close()
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
        process.stdin.close()
        process.stdout.close()


def _package(output, binary, library, example, *, source_commit, source_tree,
             parser_record_sha256, cargo_lock_sha256, rust_version,
             private_roots, smoke_runner, diag=None):
    require(isinstance(output, Path) and output.is_absolute() and
            not output.exists() and output.parent.is_dir() and
            output.parent.stat().st_uid == os.geteuid() and
            stat.S_IMODE(output.parent.stat().st_mode) == 0o700)
    require(type(private_roots) is tuple and bool(private_roots) and
            all(type(value) is str and value.startswith("/") for value in private_roots))
    for path in (binary, library, example):
        require(isinstance(path, Path) and path.is_absolute() and
                path.is_file() and not path.is_symlink() and
                path.stat().st_uid == os.geteuid())
    require(callable(smoke_runner))
    try:
        results = {}
        for mode in ("commit", "rollback"):
            if isinstance(diag, dict):
                diag["code"] = f"BUILD_SMOKE_{mode.upper()}"
            result = smoke_runner(binary, mode)
            assets._smoke(result, mode)
            results[mode] = result
    except Exception:
        raise ValueError("PUBLIC_BUILD") from None
    source = {"commit": source_commit, "tree": source_tree,
              "profile": "release"}
    staged = Path(tempfile.mkdtemp(prefix=".hnsw-public-", dir=output.parent))
    promoted = False
    try:
        destination = staged / "assets"
        destination.mkdir(mode=0o700)
        copied = {}
        for role, original in (("binary", binary), ("library", library),
                               ("example", example)):
            target = destination / assets.FILES[role]
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            content = assets._read_file(original, 268_435_456)
            _write(target, content)
            copied[role] = _sha(content)
        receipt = {"schema": assets.RECEIPT_SCHEMA, "source": source,
                   "version": "0.8.17", "binary_sha256": copied["binary"],
                   "parser_sha256": copied["library"],
                   "example_source_sha256": copied["example"],
                   "parser_record_sha256": parser_record_sha256,
                   "cargo_lock_sha256": cargo_lock_sha256,
                   "rust_version": rust_version, "parser_contract": "0.28.0",
                   "build": {"command": assets.COMMAND, "incremental": False,
                             "release_debug": False, "path_remap_applied": True},
                   "smoke": {"status": "passed", "rows": 2,
                             "dimensions": 4, "phases": 9, "events": 19},
                   "completion_smoke": results,
                   "qualification": "asset_only"}
        receipt_path = destination / assets.FILES["receipt"]
        receipt_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _write(receipt_path, _canonical(receipt))
        manifest_files = []
        for role, name in assets.FILES.items():
            data = assets._read_file(destination / name, 268_435_456)
            manifest_files.append({"role": role, "path": name,
                                   "size": len(data), "sha256": _sha(data)})
        require(sum(row["size"] for row in manifest_files) <= 268_435_456)
        manifest = {"schema": assets.SCHEMA, "source": source,
                    "platform": "linux/amd64", "files": manifest_files,
                    "expanded_limit_bytes": 268_435_456}
        manifest_raw = _canonical(manifest)
        _write(destination / "assets.json", manifest_raw)
        pin = {"source_commit": source_commit, "source_tree": source_tree,
               "binary_sha256": copied["binary"],
               "parser_sha256": copied["library"],
               "example_sha256": copied["example"],
               "assets_digest": _sha(manifest_raw),
               "parser_record_sha256": parser_record_sha256,
               "cargo_lock_sha256": cargo_lock_sha256,
               "rust_version": rust_version, "version": "0.8.17",
               "profile": "release", "platform": "linux/amd64",
               "parser_contract": "0.28.0"}
        pair._pin(pin)
        _write(staged / "result.json", _canonical({
            "schema": assets.RESULT_SCHEMA, "status": "passed",
            "source": source, "assets_digest": pin["assets_digest"],
            "performance_qualification": False}))
        verify_kwargs = {"private_roots": private_roots}
        if diag is not None:
            if isinstance(diag, dict):
                diag["code"] = "BUILD_ASSETS_VERIFY"
            verify_kwargs["diag"] = diag
        assets.verify(staged, pin, **verify_kwargs)
        staged.rename(output)
        promoted = True
        assets.verify(output, pin, **verify_kwargs)
        return pin
    except Exception:
        if promoted:
            shutil.rmtree(output)
        raise ValueError("PUBLIC_BUILD") from None
    finally:
        if staged.exists():
            shutil.rmtree(staged)
