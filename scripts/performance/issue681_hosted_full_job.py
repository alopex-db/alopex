"""Join one qualified hosted job to one audited public finish."""

from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import stat
import tempfile

import issue681_hosted_acquire_all as acquire
import issue681_hosted_build_join as build_join
import issue681_hosted_measure_join as measure_join
import issue681_hosted_qualify as qualify
import issue681_hosted_rust_preflight as rust
import issue681_public_transport as transport

ERROR = "PUBLIC_JOB"
CASE = "HNSW_PUBLIC_JOB"


def require(value):
    if not value:
        raise ValueError(ERROR)


def _empty_private(path):
    build_join._private_dir(path)
    require(not any(path.iterdir()))


def _owned(path, parent, prefix, identity):
    require(path.parent == parent and path.name.startswith(prefix))
    if identity is None:
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o700)
        identity = (info.st_dev, info.st_ino)
    acquire._remove_owned(path, identity)


def _public_snapshot(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o600 and
                0 < info.st_size <= transport.MAX_BYTES)
        raw = stream.read(transport.MAX_BYTES + 1)
        require(len(raw) == info.st_size)
        return raw


def execute(env, workspace: Path, task_parent: Path, home_parent: Path,
            profile: dict, finish: str):
    """Return a private, audited public-file handle; never print host data."""
    owned = []
    try:
        require(type(env) is dict and isinstance(workspace, Path) and
                workspace.is_absolute() and workspace.is_dir() and
                not workspace.is_symlink() and
                workspace.resolve(strict=True) == workspace and
                workspace == qualify.REPO and
                isinstance(task_parent, Path) and
                isinstance(home_parent, Path) and
                finish in ("commit", "rollback") and
                build_join._disjoint((workspace, task_parent,
                                      home_parent)) and
                Path.home() in home_parent.parents)
        _empty_private(task_parent)
        _empty_private(home_parent)
        outcome, status = qualify.run(env)
        require(status == 0 and outcome ==
                {"case": qualify.CASE, "outcome": "passed"})
        outcome, status = rust.run()
        require(status == 0 and outcome ==
                {"case": rust.CASE, "outcome": "passed"})

        task = Path(tempfile.mkdtemp(prefix="issue681-job-", dir=task_parent))
        owned.append((task, task_parent, "issue681-job-", None))
        owned[-1] = (*owned[-1][:3], acquire._identity(task))
        home = Path(tempfile.mkdtemp(prefix="issue681-home-", dir=home_parent))
        owned.append((home, home_parent, "issue681-home-", None))
        owned[-1] = (*owned[-1][:3], acquire._identity(home))
        for name in ("acquire", "build", "measure"):
            (task / name).mkdir(mode=0o700)
            _empty_private(task / name)
        acquired = acquire.acquire_all(workspace, task / "acquire", home)
        built = build_join.build(acquired, workspace, task / "build")
        result = measure_join.measure(acquired, built, workspace,
                                      task / "measure", profile, finish)
        public = result["public"]
        require(type(result) is dict and result["finish"] == finish and
                result["guard"] is acquired["guard"] and
                isinstance(public, Path) and public.is_relative_to(task) and
                public.is_file() and not public.is_symlink() and
                public.stat().st_uid == os.geteuid())
        raw = _public_snapshot(public)
        digest = hashlib.sha256(raw).hexdigest()
        pins = built["pair"]["pins"]
        require(transport.validate(raw, expected_sha256=digest,
                                   expected_pins=pins, finish=finish) == raw)
        acquired["guard"]()
        return {"case": CASE, "outcome": "passed", "public": public,
                "public_sha256": digest, "pins": copy.deepcopy(pins),
                "task_root": task, "home_root": home}
    except BaseException:
        for path, parent, prefix, identity in reversed(owned):
            try:
                _owned(path, parent, prefix, identity)
            except BaseException:
                pass
        raise ValueError(ERROR) from None
