"""Source-side adapter for one selected private #681 release finish.

The caller must select and budget the job separately. This module has no CLI,
dispatcher, uploader, or public log writer.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

import issue681_hosted_full_job as job
import issue681_hosted_qualify as qualify
import issue681_public_admission as admission
import issue681_public_transport as transport

ERROR = "PUBLIC_JOB"
GIT = Path("/usr/bin/git")
PROFILE_KEYS = {"schema", "runner_kind", "cpu_count", "additional_bytes"}


def require(value):
    if not value:
        raise ValueError(ERROR)


def _profile_digest(profile):
    require(type(profile) is dict and set(profile) == PROFILE_KEYS and
            profile["schema"] == admission.SCHEMA and
            type(profile["runner_kind"]) is str and
            re.fullmatch(r"[a-z][a-z0-9_-]{2,31}",
                         profile["runner_kind"]) is not None and
            type(profile["cpu_count"]) is int and profile["cpu_count"] == 4 and
            type(profile["additional_bytes"]) is int and
            0 < profile["additional_bytes"] <= qualify.BUILD_BYTES)
    raw = json.dumps(profile, sort_keys=True, separators=(",", ":"),
                     allow_nan=False).encode("ascii")
    return hashlib.sha256(raw).hexdigest()


def _source(workspace, source_sha):
    require(isinstance(workspace, Path) and workspace == qualify.REPO and
            workspace.is_absolute() and workspace.is_dir() and
            not workspace.is_symlink() and
            workspace.resolve(strict=True) == workspace and
            type(source_sha) is str and
            re.fullmatch(r"[0-9a-f]{40}", source_sha) is not None)
    info = GIT.stat()
    require(GIT.is_file() and not GIT.is_symlink() and info.st_uid == 0 and
            not (info.st_mode & 0o022))
    env = {"HOME": "/dev/null", "XDG_CONFIG_HOME": "/dev/null",
           "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8",
           "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
           "GIT_TERMINAL_PROMPT": "0"}
    def read(*args):
        result = subprocess.run([str(GIT), "-C", str(workspace), *args],
                                env=env, stdin=subprocess.DEVNULL,
                                capture_output=True, timeout=30)
        require(result.returncode == 0 and len(result.stdout) <= 4096 and
                len(result.stderr) <= 4096)
        return result.stdout.strip()
    require(read("rev-parse", "HEAD") == source_sha.encode() and
            read("status", "--porcelain", "--untracked-files=all") == b"")


def _private_qualification(_unused, workspace, task_parent, home_parent,
                           source_sha):
    _source(workspace, source_sha)
    qualify.platform_security()
    for root in (workspace, task_parent, home_parent):
        require(root.is_absolute() and root.is_dir() and not root.is_symlink() and
                root.resolve(strict=True) == root and
                root.stat().st_uid == os.geteuid())
    manifest = qualify.scoped_storage.make_manifest(
        workspace, task_parent, task_parent=task_parent,
        home_parent=home_parent)
    used, free = qualify.mounted_allocation(manifest)
    require(used + qualify.BUILD_BYTES + qualify.RESERVE_BYTES <
            qualify.STOP_BYTES and
            used + qualify.BUILD_BYTES + qualify.RESERVE_BYTES <
            qualify.CEILING_BYTES and
            all(value > qualify.BUILD_BYTES + qualify.RESERVE_BYTES
                for value in free.values()))
    return {"case": qualify.CASE, "outcome": "passed"}, 0


def execute(*, workspace: Path, task_parent: Path, home_parent: Path,
            profile: dict, finish: str, source_sha: str, run_id: str):
    """Run exactly one finish and retain its result only in the private job."""
    try:
        require(type(run_id) is str and
                re.fullmatch(r"[0-9a-f]{64}", run_id) is not None and
                finish in ("commit", "rollback"))
        profile_sha256 = _profile_digest(profile)
        require(os.environ.get("ALOPEX_SELECTED_RUN_ID") == run_id and
                os.environ.get("RUNNER_KIND") == profile["runner_kind"])
        def preflight(_env):
            return _private_qualification(_env, workspace, task_parent,
                                          home_parent, source_sha)
        result = job._execute({}, workspace, task_parent, home_parent,
                              profile, finish, preflight)
        require(result["case"] == job.CASE and result["outcome"] == "passed")
        return {**result, "run_id": run_id, "source_sha": source_sha,
                "finish": finish, "profile_sha256": profile_sha256}
    except BaseException:
        raise ValueError(ERROR) from None


def handoff(result, *, run_id: str, source_sha: str, finish: str,
            profile_sha256: str, expected_digest: str, expected_pins: dict):
    """Return only revalidated public bytes and digest, without a path."""
    try:
        require(type(result) is dict and
                set(result) == {"case", "outcome", "public", "public_sha256",
                                "pins", "task_root", "home_root", "run_id",
                                "source_sha", "finish", "profile_sha256"} and
                result["case"] == job.CASE and result["outcome"] == "passed" and
                type(run_id) is str and re.fullmatch(r"[0-9a-f]{64}", run_id) and
                type(source_sha) is str and
                re.fullmatch(r"[0-9a-f]{40}", source_sha) and
                finish in ("commit", "rollback") and
                type(profile_sha256) is str and
                re.fullmatch(r"[0-9a-f]{64}", profile_sha256) and
                type(expected_digest) is str and
                re.fullmatch(r"[0-9a-f]{64}", expected_digest) and
                result["run_id"] == run_id and
                result["source_sha"] == source_sha and
                result["finish"] == finish and
                result["profile_sha256"] == profile_sha256 and
                result["public_sha256"] == expected_digest and
                result["pins"] == expected_pins and
                isinstance(result["public"], Path) and
                result["public"].is_relative_to(result["task_root"]))
        raw = job._public_snapshot(result["public"])
        require(transport.validate(raw, expected_sha256=expected_digest,
                                   expected_pins=expected_pins,
                                   finish=finish) == raw)
        return {"public_bytes": raw, "sha256": expected_digest}
    except BaseException:
        raise ValueError(transport.ERROR) from None
