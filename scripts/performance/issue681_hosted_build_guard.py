"""Bound one selected hosted #681 build to a complete mount-allocation budget."""

from __future__ import annotations

import os
from pathlib import Path
import time

import issue681_hosted_qualify as qualify


DEFAULT_REAL_SCAN_INTERVAL_SECONDS = 1.0


def require(value):
    if not value:
        raise ValueError("PUBLIC_CAPACITY")


class HostStorageGuard:
    """Recount admitted Alopex roots on each call; expose no host diagnostics."""

    def __init__(self, workspace, *, manifest=None, scanner=None,
                 stat_fn=os.stat, min_interval_seconds=None):
        try:
            require(isinstance(workspace, Path) and workspace.is_absolute() and
                    workspace.is_dir() and not workspace.is_symlink() and
                    workspace.resolve(strict=True) == workspace and
                    workspace.stat().st_uid == os.geteuid())
            self.device = stat_fn(workspace).st_dev
            require(manifest is not None or scanner is not None)
            if min_interval_seconds is None:
                interval = (DEFAULT_REAL_SCAN_INTERVAL_SECONDS
                            if scanner is None else 0.0)
            else:
                require(isinstance(min_interval_seconds, (int, float)) and
                        not isinstance(min_interval_seconds, bool) and
                        min_interval_seconds >= 0.0)
                interval = float(min_interval_seconds)
            self.min_interval_seconds = interval
            self.manifest = manifest
            self.scanner = scanner or (lambda: qualify.mounted_allocation(manifest))
            baseline, available = self._sample()
            self._last_sample = time.monotonic()
            self.devices = frozenset(available)
            require(baseline + qualify.BUILD_BYTES + qualify.RESERVE_BYTES <
                    qualify.STOP_BYTES and
                    baseline + qualify.BUILD_BYTES + qualify.RESERVE_BYTES <
                    qualify.CEILING_BYTES and
                    all(free > qualify.BUILD_BYTES + qualify.RESERVE_BYTES
                        for free in available.values()))
            self.baseline = baseline
        except Exception:
            raise ValueError("PUBLIC_CAPACITY") from None

    def _sample(self):
        used, available = self.scanner()
        require(type(used) is int and 0 <= used < qualify.CEILING_BYTES and
                type(available) is dict and
                self.device in available and
                all(type(device) is int and type(free) is int and free >= 0
                    for device, free in available.items()))
        return used, available

    def __call__(self, *, force: bool = False):
        try:
            if (not force and self.min_interval_seconds > 0.0 and
                    (time.monotonic() - self._last_sample) < self.min_interval_seconds):
                return
            used, available = self._sample()
            self._last_sample = time.monotonic()
            require(frozenset(available) == self.devices and
                    used < self.baseline + qualify.BUILD_BYTES and
                    used + qualify.RESERVE_BYTES < qualify.STOP_BYTES and
                    used + qualify.RESERVE_BYTES < qualify.CEILING_BYTES and
                    all(free > qualify.RESERVE_BYTES
                        for free in available.values()))
        except Exception:
            raise ValueError("PUBLIC_CAPACITY") from None
