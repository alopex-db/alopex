"""Preseal the 52 private SQL profiles from one qualified #804 profile.

Admission runs before each cell is staged.  Therefore the assets pathname is
reserved and sealed up front; its contents are checked when staging completes.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import stat

from . import collection_attestor, container, matrix


def _require(value, code="PROFILE_PIN"):
    if not value:
        raise ValueError(code)


def _private_file(path):
    path = Path(path)
    _require(path.is_absolute() and not path.is_symlink() and path.is_file())
    info = path.stat()
    _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and
             not info.st_mode & 0o077)
    return path.read_bytes()


def _private_directory(path):
    path = Path(path)
    _require(path.is_absolute() and not path.is_symlink() and path.is_dir())
    info = path.stat()
    _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and
             not info.st_mode & 0o077)
    return path


class ProfileFactory:
    """Own immutable profile paths and their reviewed byte hashes.

    ``staging_root`` is the private root also passed to ``destination_for``.
    ``source_pins`` are the four reviewed subject records used for staging.
    Existing profile files are never overwritten, including on retry.
    """

    def __init__(self, qualified_profile, *, expected_profile_sha256,
                 profile_dir, staging_root, source_pins,
                 case_seconds=300, run_seconds=420):
        from . import container as runtime
        common, *_ = runtime._runtime()
        _require(type(expected_profile_sha256) is str and
                 len(expected_profile_sha256) == 64 and
                 all(char in "0123456789abcdef" for char in expected_profile_sha256))
        raw = _private_file(qualified_profile)
        _require(hashlib.sha256(raw).hexdigest() == expected_profile_sha256)
        base = common.load_profile(Path(qualified_profile))
        _require(type(source_pins) is dict and set(source_pins) == set(matrix.SUBJECTS) and
                 all(type(pin) is dict and type(pin.get("source")) is dict and
                     set(pin["source"]) == {"commit", "tree", "profile"} and
                     pin["source"]["profile"] == "release" and
                     all(type(pin["source"][key]) is str and
                         len(pin["source"][key]) == 40 and
                         all(ch in "0123456789abcdef" for ch in pin["source"][key])
                         for key in ("commit", "tree")) and
                     pin.get("platform") == base["platform"] and
                     matrix._hex(pin.get("assets_digest"), 64) and
                     type(pin.get("files")) is list and
                     {item.get("role") for item in pin["files"] if type(item) is dict} ==
                     {"server", "wheel", "library", "receipt"}
                     for pin in source_pins.values()))
        _require(type(case_seconds) is int and 0 < case_seconds <= 300 and
                 type(run_seconds) is int and 0 < run_seconds <= 420 and
                 case_seconds <= run_seconds)
        self.profile_dir = _private_directory(profile_dir)
        self.staging_root = _private_directory(staging_root)
        _require(self.profile_dir != self.staging_root and
                 not self.profile_dir.is_relative_to(self.staging_root) and
                 not self.staging_root.is_relative_to(self.profile_dir))
        self.qualified_profile = Path(qualified_profile)
        self.qualified_sha256 = expected_profile_sha256
        self.source_pins = copy.deepcopy(source_pins)
        self.common = common
        self.paths = {}
        self.profile_hashes = {}
        runs = collection_attestor.expected_runs()
        _require(len(runs) == 52)
        for kind, row in runs:
            run_id = container.owned_run_id(kind, row)
            destination = self.staging_root / run_id
            path = self.profile_dir / (run_id + ".json")
            _require(not path.exists() and not path.is_symlink() and
                     not destination.exists() and not destination.is_symlink(),
                     "PROFILE_EXISTS")
            profile = copy.deepcopy(base)
            profile["run_id"] = run_id
            profile["assets"] = str(destination / "assets.json")
            profile["limits"]["case_seconds"] = case_seconds
            profile["limits"]["run_seconds"] = run_seconds
            data = (json.dumps(profile, ensure_ascii=False, sort_keys=True,
                               indent=2) + "\n").encode()
            # O_EXCL prevents a concurrent constructor from replacing a seal.
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            self.paths[run_id] = path
            self.profile_hashes[run_id] = hashlib.sha256(data).hexdigest()
        _require(len(self.paths) == len(self.profile_hashes) == 52)

    def _id(self, kind, row):
        _require(type(row) is dict, "PROFILE_COORDINATE")
        coordinate = {key: row.get(key) for key in
                      ("id", "case", "trial", "subject", "engine")}
        _require(coordinate in matrix.plan(), "PROFILE_COORDINATE")
        _require(kind in ("preflight", "cell") and
                 (kind != "preflight" or
                  coordinate["case"] == "select-all" and coordinate["trial"] == 0),
                 "PROFILE_COORDINATE")
        return container.owned_run_id(kind, coordinate)

    def destination_for(self, kind, row):
        """Return the reserved, phase-qualified staging directory."""
        return self.staging_root / self._id(kind, row)

    def profile_for(self, kind, row):
        """Lookup for controller execution, admission, and the attestor."""
        run_id = self._id(kind, row)
        path = self.paths[run_id]
        raw = _private_file(path)
        _require(hashlib.sha256(raw).hexdigest() == self.profile_hashes[run_id])
        _require(hashlib.sha256(_private_file(self.qualified_profile)).hexdigest() ==
                 self.qualified_sha256)
        profile = self.common.load_profile(path)
        _require(profile["run_id"] == run_id and
                 profile["assets"] == str(self.destination_for(kind, row) / "assets.json"))
        staged = Path(profile["assets"])
        if staged.exists() or staged.is_symlink():
            self.verify_staged(kind, row)
        return path

    def verify_staged(self, kind, row):
        """Verify the materialized manifest and its pinned source before use."""
        run_id = self._id(kind, row)
        path = self.paths[run_id]
        _require(hashlib.sha256(_private_file(path)).hexdigest() ==
                 self.profile_hashes[run_id])
        profile = self.common.load_profile(path)
        staged = Path(profile["assets"])
        _require(staged == self.destination_for(kind, row) / "assets.json" and
                 staged.parent.is_dir() and not staged.parent.is_symlink(),
                 "STAGED_ASSETS")
        _private_directory(staged.parent)
        _private_file(staged)
        manifest = self.common.verify_assets(staged, profile["platform"])
        pin = self.source_pins[row["subject"]]
        _require(manifest.get("source_commit") == pin["source"].get("commit") and
                 ("source_tree" not in manifest or
                  manifest["source_tree"] == pin["source"].get("tree")) and
                 manifest.get("platform") == pin["platform"], "STAGED_SOURCE")
        product_files = [item for item in manifest["files"] if item["role"] in
                         ("server", "wheel", "library", "receipt")]
        _require(sorted(product_files, key=lambda item: item["role"]) ==
                 sorted(pin["files"], key=lambda item: item["role"]),
                 "STAGED_PRODUCT")
        _private_file(staged.parent / "measurement.json")
        config = self.common.read_json(staged.parent / "measurement.json")
        _require(type(config) is dict and config.get("phase") == kind and
                 all(config.get(key) == row[key] for key in
                     ("id", "case", "trial", "subject", "engine")) and
                 config.get("source") == pin["source"], "STAGED_SOURCE")
        return staged
