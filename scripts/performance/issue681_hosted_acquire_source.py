"""Acquire exact #681 product sources with private Git logs and one host budget."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import time

import issue681_hosted_build_guard as capacity
import issue681_public_assets_produce as producer

REPOSITORY = "https://github.com/alopex-db/alopex.git"
GIT = Path("/usr/bin/git")
TIME_LIMIT = 300
PINS = {
    "before": {
        "commit": "987d72ddca3adfb84794ad395a395ad6608e5c63",
        "tree": "04b09be4f843a8d3735d8a681aa71ae8a35fe6c7",
        "example": "a41bcaccd6143d942bd7064afa444f388d1bcb5788cd8f458737fddf7aed6758",
        "lock": "007e0f00d2652cec726fd0f1cd364945ed6ed8c7901fb03c195c1ce8bc1b6658",
    },
    "candidate": {
        "commit": "a2b15a9cb4ec80c7686c40f679038901d4b51347",
        "tree": "cbed275910e40155c4a3abd16dbf8bb7f4777dc1",
        "example": "a41bcaccd6143d942bd7064afa444f388d1bcb5788cd8f458737fddf7aed6758",
        "lock": "c1a1abdd274a633f7ff1f8e47eed70fdc05a032c745ba1645bf000df711b6103",
    },
}


def require(value):
    if not value:
        raise ValueError("PUBLIC_ACQUIRE")


def _trusted_git():
    info = GIT.stat()
    require(GIT.is_file() and not GIT.is_symlink() and info.st_uid == 0 and
            not (info.st_mode & 0o022))


def _git_env(home):
    return {"HOME": str(home), "XDG_CONFIG_HOME": str(home),
            "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8",
            "GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1", "GIT_ASKPASS": "/bin/false"}


def _stop_git(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=20)
    else:
        process.wait()


def _git(args, cwd, guard, output, env):
    argv = [str(GIT), "-c", "credential.helper=", "-c",
            "http.followRedirects=false", "-c", "core.hooksPath=/dev/null",
            *args]
    guard()
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                               stdout=output, stderr=subprocess.STDOUT,
                               start_new_session=True)
    deadline = time.monotonic() + TIME_LIMIT
    try:
        while process.poll() is None:
            require(time.monotonic() < deadline)
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
            guard()
        require(process.returncode == 0)
        guard()
    except BaseException:
        _stop_git(process)
        raise


def acquire_sources(workspace, parent, *, manifest):
    """Return the pre-download guard and two verified private checkouts."""
    created = []
    try:
        require(isinstance(parent, Path) and parent.is_absolute() and
                parent.is_dir() and not parent.is_symlink() and
                parent.resolve(strict=True) == parent and
                parent.stat().st_uid == os.geteuid() and
                stat.S_IMODE(parent.stat().st_mode) == 0o700 and
                not any(parent.iterdir()) and
                isinstance(workspace, Path) and workspace.is_absolute() and
                parent != workspace and parent not in workspace.parents and
                workspace not in parent.parents)
        _trusted_git()
        require(manifest.workspace == workspace and
                parent.is_relative_to(manifest.task_parent))
        guard = capacity.HostStorageGuard(workspace, manifest=manifest)
        git_home = parent / "git-home"
        git_home.mkdir(mode=0o700)
        created.append(git_home)
        git_env = _git_env(git_home)
        log_fd = os.open(parent / "git-acquire.private.log",
                         os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                         getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(log_fd, "wb") as log:
            for name in ("before", "candidate"):
                source = parent / name
                source.mkdir(mode=0o700)
                created.append(source)
                _git(["init", "-q"], source, guard, log, git_env)
                _git(["remote", "add", "origin", REPOSITORY], source,
                     guard, log, git_env)
                _git(["fetch", "-q", "--no-tags", "--depth=1", "origin",
                      PINS[name]["commit"]], source, guard, log, git_env)
                _git(["checkout", "-q", "--detach", "FETCH_HEAD"], source,
                     guard, log, git_env)
                pin = PINS[name]
                producer.verify_source(source, pin["commit"], pin["tree"],
                                       pin["example"], pin["lock"],
                                       git_binary=GIT, git_env=git_env)
                guard()
        return {"guard": guard, "git_binary": GIT, "git_env": git_env,
                "sources": {name: parent / name
                            for name in ("before", "candidate")}}
    except BaseException:
        for source in reversed(created):
            try:
                shutil.rmtree(source)
            except Exception:
                pass
        raise ValueError("PUBLIC_ACQUIRE") from None
