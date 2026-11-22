"""Bound one selected hosted #681 build to a complete mount-allocation budget."""

from __future__ import annotations

import os
from pathlib import Path

import issue681_hosted_qualify as qualify


def require(value):
    if not value:
        raise ValueError("PUBLIC_CAPACITY")


class HostStorageGuard:
    """Reopen all visible mounts on each call; expose no host diagnostics."""

    def __init__(self, workspace, *, scanner=None, stat_fn=os.stat):
        try:
            require(isinstance(workspace, Path) and workspace.is_absolute() and
                    workspace.is_dir() and not workspace.is_symlink() and
                    workspace.resolve(strict=True) == workspace and
                    workspace.stat().st_uid == os.geteuid())
            self.device = stat_fn(workspace).st_dev
            self.scanner = scanner or qualify.mounted_allocation
            baseline, available = self._sample()
            self.devices = frozenset(available)
            require(baseline + qualify.BUILD_BYTES + qualify.RESERVE_BYTES <
                    qualify.STOP_BYTES and
                    baseline + qualify.BUILD_BYTES + qualify.RESERVE_BYTES <
                    qualify.CEILING_BYTES and
                    available[self.device] >
                    qualify.BUILD_BYTES + qualify.RESERVE_BYTES)
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

    def __call__(self):
        try:
            used, available = self._sample()
            require(frozenset(available) == self.devices and
                    used < self.baseline + qualify.BUILD_BYTES and
                    used + qualify.RESERVE_BYTES < qualify.STOP_BYTES and
                    used + qualify.RESERVE_BYTES < qualify.CEILING_BYTES and
                    available[self.device] > qualify.RESERVE_BYTES)
        except Exception:
            raise ValueError("PUBLIC_CAPACITY") from None
