"""Offline-preparable four-case lock, acquisition, and attestation helpers.

The #804 controller must qualify the four-case assets in this GitHub job before
``attest_current_job`` is called. Its 1951-pinned CLI is not a substitute for
that step. This CLI validates the reviewed lock only; it does not stage a
candidate for #804 or dispatch a pair.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import selectors
import stat
import subprocess
import sys
import time

from . import acquire, attestor, qualification


def _require(value, code="MISSING_PIN"):
    if not value:
        raise ValueError(code)


def load_reviewed_lock(path, expected_sha256):
    """Bind exact lock bytes to a separately reviewed digest, never to self-report."""
    _require(type(expected_sha256) is str and
             qualification.HEX64.fullmatch(expected_sha256))
    path = Path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        _require(stat.S_ISREG(info.st_mode) and info.st_size in range(1, 16_385))
        raw = os.read(fd, 16_385)
        after = os.fstat(fd)
        _require(len(raw) == info.st_size and
                 (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) ==
                 (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns))
    finally:
        os.close(fd)
    _require(hashlib.sha256(raw).hexdigest() == expected_sha256)
    try:
        lock = json.loads(raw, object_pairs_hook=acquire.acquisition._unique_pairs)
    except (ValueError, UnicodeError) as error:
        raise ValueError("MISSING_PIN") from error
    _require(type(lock) is dict and set(lock) == {"schema", "pin"} and
             lock["schema"] == "alopex.sql-four-case-pins/v1" and
             type(lock["pin"]) is dict and
             set(lock["pin"]) == qualification.PIN_FIELDS)
    return lock


class GitHubFetcher:
    """Bounded GitHub API reader; gh owns authentication and artifact redirect."""

    def __init__(self, *, executable="/usr/bin/gh", repository="alopex-db/alopex"):
        self.executable = Path(executable)
        self.repository = repository

    def fetch(self, path, *, binary=False, timeout=30, max_bytes=None):
        _require(self.repository == "alopex-db/alopex" and
                 type(path) is str and path.startswith("/actions/") and
                 ".." not in path and "\n" not in path and
                 type(timeout) in (int, float) and 0 < timeout <= 180 and
                 (not binary or type(max_bytes) is int and 0 < max_bytes < 60_000_000) and
                 bool(os.environ.get("GH_TOKEN")) and
                 os.environ.get("GITHUB_REPOSITORY") == self.repository,
                 "PREREQUISITE")
        info = self.executable.lstat()
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
                 not info.st_mode & 0o022 and info.st_mode & 0o111,
                 "PREREQUISITE")
        limit = max_bytes if binary else 2_000_000
        process = subprocess.Popen(
            [str(self.executable), "api", "repos/" + self.repository + path],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL)
        chunks, size = [], 0
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + timeout
                while True:
                    ready = selector.select(max(0, deadline - time.monotonic()))
                    _require(ready, "PREREQUISITE")
                    chunk = os.read(process.stdout.fileno(), min(65_536, limit + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    _require(size <= limit, "PREREQUISITE")
                    chunks.append(chunk)
            _require(process.wait(timeout=max(0, deadline - time.monotonic())) == 0,
                     "PREREQUISITE")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
        raw = b"".join(chunks)
        if binary:
            return raw
        try:
            return json.loads(raw, object_pairs_hook=acquire.acquisition._unique_pairs)
        except (ValueError, UnicodeError) as error:
            raise ValueError("PREREQUISITE") from error


def controller_operations(repo, lock):
    """Load #804 APIs only with a pinned, dedicated four-case controller."""
    repo = Path(repo).resolve(strict=True)
    env_dir = repo / "scripts/test-env"
    ops_dir = repo / "scripts/ops"
    _require(env_dir.is_dir() and ops_dir.is_dir(), "QUALIFICATION")
    controller_name = "hosted_test_env_qualification_four_case.py"
    controller = ops_dir / controller_name
    expected = lock.get("pin", {}).get("operator_sha256", {}).get(controller_name)
    _require(type(expected) is str and qualification.HEX64.fullmatch(expected) and
             controller.is_file() and not controller.is_symlink() and
             controller.stat().st_size in range(1, 1_000_001) and
             hashlib.sha256(controller.read_bytes()).hexdigest() == expected,
             "OPERATOR_IDENTITY")
    sys.path.insert(0, str(env_dir))
    sys.path.insert(0, str(ops_dir))
    common = importlib.import_module("common")
    backend = importlib.import_module("docker_backend")
    envctl = importlib.import_module("envctl")
    lifecycle = importlib.import_module("lifecycle")
    spec = importlib.util.spec_from_file_location(
        "alopex_pinned_four_case_controller", controller)
    _require(spec is not None and spec.loader is not None, "OPERATOR_IDENTITY")
    controller_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controller_module)
    return {"read_json": common.read_json, "load_profile": common.load_profile,
            "verify_assets": common.verify_assets,
            "source_identity": envctl.source_identity,
            "docker_factory": backend.Docker,
            "verified_image": envctl.verified_image,
            "run_factory": lifecycle.Run,
            "verify_temp_snapshot": controller_module.verify_temp_snapshot}


def acquire_current_job(lock, *, job_id, directory, fetch=None):
    return acquire.acquire(lock, job_id=job_id, directory=directory,
                           fetch=GitHubFetcher().fetch if fetch is None else fetch)


def attest_current_job(lock, *, job_id, repo, runner_temp, environment,
                       operations=None):
    return attestor.attest_current_job(
        lock, job_id, repo=repo, runner_temp=runner_temp,
        environment=environment,
        operations=controller_operations(repo, lock) if operations is None else operations)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--lock-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        load_reviewed_lock(args.lock, args.lock_sha256)
        print(json.dumps({"status": "lock_verified", "qualification": False},
                         sort_keys=True))
        return 0
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        code = error.args[0] if type(error) is ValueError and error.args else "PREREQUISITE"
        print(json.dumps({"status": "failed", "error_code": code}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
