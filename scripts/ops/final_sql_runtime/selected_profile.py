"""Select private final SQL profile and storage from live #804 proof."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat

from . import container, same_job


def require(ok):
    if not ok:
        raise ValueError("SELECTED_PROFILE")


def from_live(qualification_args):
    """Return one proof-bound profile path, hash and Docker backing root."""
    try:
        require(type(qualification_args) is dict and
                set(qualification_args) == {"lock_path", "expected", "repo",
                                            "runner_temp", "environment"})
        proof = same_job.attest_live(**qualification_args)
        runner_temp = Path(qualification_args["runner_temp"])
        require(runner_temp.is_absolute() and runner_temp.is_dir() and
                not runner_temp.is_symlink())
        private = runner_temp / "alopex-804-qualification"
        info = private.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o700)
        profile = private / "profile.json"
        info = profile.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600 and
                0 < info.st_size < 1_000_000 and
                hashlib.sha256(profile.read_bytes()).hexdigest() ==
                proof.get("profile_sha256"))
        storage = proof.get("storage")
        require(type(storage) is dict and
                type(storage.get("runtime_storage_roots")) is list and
                len(storage["runtime_storage_roots"]) == 1)
        backing = Path(storage["runtime_storage_roots"][0])
        require(backing.is_absolute() and backing.is_dir() and
                backing.resolve(strict=True) == backing)
        common, *_ = container._runtime()
        loaded = common.load_profile(profile)
        require(type(loaded) is dict and loaded.get("storage") == storage and
                loaded.get("private_output") == str(private / "runs"))
        return profile, proof["profile_sha256"], backing
    except (OSError, KeyError, TypeError, ValueError, UnicodeError):
        raise ValueError("SELECTED_PROFILE") from None
