"""Run builder-lock version checks through the selected Ubuntu base's dpkg.

The caller owns a live, already admitted resolver container and its host monitor.
This module neither creates a container nor changes its policy or network.
"""
from __future__ import annotations

import re

from offline_closure_lifecycle import inspect_offline_container
from resolve_builder_lock import LockError
import resolve_builder_lock

VERSION = re.compile(r"[0-9][0-9A-Za-z.+:~\-]*\Z")
OPERATORS = frozenset(("<<", "<=", "=", ">=", ">>"))


class ExactBaseDpkg:
    """Compare Debian versions only inside one verified, running base container."""

    def __init__(self, docker, monitor, identity: str, profile: dict,
                 run_id: str, token: str):
        if not re.fullmatch(r"[0-9a-f]{64}", identity):
            raise LockError("invalid resolver identity")
        self.docker = docker
        self.monitor = monitor
        self.identity = identity
        self.profile = profile
        self.run_id = run_id
        self.token = token
        self._verify_container()

    def _verify_container(self) -> None:
        inspect_offline_container(self.docker, self.identity, self.profile,
                                  self.run_id, self.token, running=True)

    def compare(self, actual: str, operator: str, required: str) -> bool:
        if (operator not in OPERATORS or not VERSION.fullmatch(actual) or
                not VERSION.fullmatch(required)):
            raise LockError("invalid dpkg comparison")
        # Recheck identity before each exec; never substitute the host's dpkg.
        self._verify_container()
        code, stdout, stderr = self.monitor.run(
            [*self.docker.prefix, "exec", self.identity,
             "/usr/bin/dpkg", "--compare-versions", actual, operator, required],
            timeout=10, capture=True, allow_nonzero=True)
        if code not in (0, 1) or stdout or stderr:
            raise LockError("selected base dpkg comparison failed")
        return code == 0


def make_lock_with_exact_base_dpkg(base: dict, authentication: dict,
                                   indexes: dict[str, bytes], apt_uris: str,
                                   resolver_sha256: str, comparator: ExactBaseDpkg) -> dict:
    """Keep the existing proof gate and package checks; replace only version ordering.

    The override is process-local and restored even when lock validation fails.
    Run this in a dedicated, serial process while its host monitor is active.
    """
    if not isinstance(comparator, ExactBaseDpkg):
        raise LockError("exact base comparator required")
    original = resolve_builder_lock.version_satisfies
    if original is not _HOST_VERSION_SATISFIES:
        raise LockError("version comparator already overridden")
    resolve_builder_lock.version_satisfies = comparator.compare
    try:
        return resolve_builder_lock.make_lock(
            base, authentication, indexes, apt_uris, resolver_sha256)
    finally:
        resolve_builder_lock.version_satisfies = original


_HOST_VERSION_SATISFIES = resolve_builder_lock.version_satisfies
