"""Bind final SQL admission to one qualified host storage inventory.

The caller must qualify the private test-env profile and its local Docker
backing root before constructing this adapter. This module does not change the
runtime or security policy. Its two methods are controller.run callbacks.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path

from . import container

STOP_BYTES = 40_000_000_000
HARD_BYTES = 50_000_000_000
PLANNED_BYTES = 4_000_000_000
RESERVE_BYTES = 1_000_000_000


def _require(condition, code):
    if not condition:
        raise ValueError(code)


def _profile(path, loader, expected_sha256=None):
    _require(isinstance(path, Path) and path.is_absolute() and
             path.is_file() and not path.is_symlink(), "STORAGE_PROFILE")
    if expected_sha256 is not None:
        _require(type(expected_sha256) is str and len(expected_sha256) == 64 and
                 hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha256,
                 "STORAGE_PROFILE")
    return loader(path)


class StorageAdmission:
    """Admission before harness staging and capacity checks before each cell.

    ``cell_profile_for`` maps ``(kind, row)`` to the exact private profile
    selected for that cell. Its storage inventory and host/runtime identity
    must match the already qualified profile, even if run ID and assets differ.
    ``expected_backing_root`` is the independently qualified daemon backing
    root (or VM disk directory), never inferred from an untrusted cell profile.
    """

    def __init__(self, qualified_profile, *, expected_profile_sha256,
                 expected_backing_root, cell_profile_for, loader=None,
                 budget_factory=None):
        _require(callable(cell_profile_for), "STORAGE_PROFILE")
        if loader is None or budget_factory is None:
            common, capacity, *_ = container._runtime()
            loader = loader or common.load_profile
            budget_factory = budget_factory or capacity.Budget
        self.loader = loader
        self.cell_profile_for = cell_profile_for
        profile = _profile(qualified_profile, loader, expected_profile_sha256)
        self.qualified_profile = qualified_profile
        self.expected_profile_sha256 = expected_profile_sha256
        _require(type(profile) is dict and type(profile.get("storage")) is dict,
                 "STORAGE_PROFILE")
        storage = profile["storage"]
        roots = storage.get("roots")
        runtime_roots = storage.get("runtime_storage_roots")
        _require(type(roots) is list and roots and type(runtime_roots) is list and
                 runtime_roots and isinstance(expected_backing_root, Path) and
                 expected_backing_root.is_absolute() and
                 all(type(item) is str and Path(item).is_absolute() for item in
                     roots + runtime_roots), "STORAGE_PROFILE")
        try:
            resolved_roots = [Path(item).resolve(strict=True) for item in roots]
            resolved_runtime = [Path(item).resolve(strict=True) for item in runtime_roots]
            backing = expected_backing_root.resolve(strict=True)
        except OSError as error:
            raise ValueError("STORAGE_PROFILE") from error
        _require(backing in resolved_runtime and
                 all(any(item.is_relative_to(root) for root in resolved_roots)
                     for item in resolved_runtime) and
                 type(storage.get("stop_bytes")) is int and
                 0 < storage["stop_bytes"] <= STOP_BYTES and
                 type(storage.get("reserve_bytes")) is int and
                 storage["reserve_bytes"] >= RESERVE_BYTES,
                 "STORAGE_PROFILE")
        self.storage = storage
        self.host_identity = {key: profile.get(key) for key in
                              ("docker_context", "platform", "execution_mode",
                               "image_lock", "private_output", "cache", "cpu", "security")}
        self.resource_limits = {key: profile.get("limits", {}).get(key) for key in
                                ("memory_bytes", "swap_bytes", "pids", "stop_seconds")}
        self.budget = budget_factory(storage)
        self.initial = None
        self.last_used = None

    def _used(self):
        try:
            value = self.budget.used()
        except Exception as error:
            raise ValueError("STORAGE_MEASUREMENT") from error
        _require(type(value) is int and value >= 0, "STORAGE_MEASUREMENT")
        self.last_used = value
        _require(value < HARD_BYTES and value < STOP_BYTES and
                 value + RESERVE_BYTES < STOP_BYTES and
                 value + self.storage["reserve_bytes"] < self.storage["stop_bytes"],
                 "STORAGE_CAPACITY")
        return value

    def admit_storage(self, required_seconds):
        _require(type(required_seconds) in (int, float) and
                 math.isfinite(required_seconds) and
                 0 < required_seconds <= 7_200 and self.initial is None,
                 "STORAGE_ADMISSION")
        used = self._used()
        _require(used + PLANNED_BYTES + RESERVE_BYTES < STOP_BYTES and
                 used + PLANNED_BYTES + RESERVE_BYTES < HARD_BYTES and
                 used + PLANNED_BYTES + self.storage["reserve_bytes"] <
                 self.storage["stop_bytes"], "STORAGE_CAPACITY")
        self.initial = used
        return True

    def capacity(self, kind, row, future_seconds):
        _require(self.initial is not None and kind in ("preflight", "cell") and
                 type(row) is dict and type(future_seconds) in (int, float) and
                 math.isfinite(future_seconds) and
                 0 < future_seconds <= 7_200, "STORAGE_ADMISSION")
        _profile(self.qualified_profile, self.loader, self.expected_profile_sha256)
        path = self.cell_profile_for(kind, row)
        profile = _profile(path, self.loader)
        _require(type(profile) is dict and profile.get("storage") == self.storage and
                 {key: profile.get(key) for key in self.host_identity} == self.host_identity and
                 {key: profile.get("limits", {}).get(key) for key in self.resource_limits} ==
                 self.resource_limits and
                 type(profile.get("limits", {}).get("case_seconds")) is int and
                 0 < profile["limits"]["case_seconds"] <= 300 and
                 type(profile["limits"].get("run_seconds")) is int and
                 0 < profile["limits"]["run_seconds"] <= 420,
                 "STORAGE_PROFILE")
        used = self._used()
        _require(used - self.initial <= PLANNED_BYTES, "STORAGE_CAPACITY")
        return used
